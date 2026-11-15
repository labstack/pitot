//go:build filebench

package engine

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// Exercise the real snapshot binder, scoped candidate repair, raw tail and
// completed-query kernel. This is an exploratory local benchmark, not an HTTP
// or mixed-ingest capacity gate. Run unchanged before and after a query edit.
func TestTraceCandidateWindowPlanComparisonBenchmark(t *testing.T) {
	const traces = 20000
	for _, boundaryPercent := range []int{1, 10} {
		t.Run(fmt.Sprintf("boundary_%d_percent", boundaryPercent), func(t *testing.T) {
			d, repo := newBatchCacheTest(t)
			base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
			for part := range 4 {
				batch := telemetrystore.Batch{ID: fmt.Sprintf("part_%d", part)}
				for i := range traces {
					// Permute identities independently of time and scope. Some
					// traces span services, so the scoped index must be repaired.
					id := uint64(i+1) * 0x9e3779b97f4a7c15
					service := "api"
					if i%7 == 0 && part == 0 {
						service = "db"
					}
					start := base.Add(time.Duration(i%1000+part) * time.Millisecond)
					if i%100 < boundaryPercent && part == 0 {
						start = base.Add(-time.Second)
						if (i/100)%2 == 0 {
							start = base.Add(3 * time.Second)
						}
					}
					status := "STATUS_CODE_OK"
					if i%13 == 0 && part == 0 {
						status = "STATUS_CODE_ERROR"
					}
					batch.Spans = append(batch.Spans, telemetry.Span{Namespace: "prod", ServiceName: service,
						TraceID: fmt.Sprintf("%032x", id), SpanID: fmt.Sprintf("%016x", part+1),
						StartUnixNanos: start.UnixNano(), EndUnixNanos: start.Add(time.Millisecond).UnixNano(),
						DurationMS: 1, StatusCode: status, IngestedAt: base.UnixNano()})
				}
				if err := repo.Commit(t.Context(), batch); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := d.RefreshReadCaches(t.Context()); err != nil {
				t.Fatal(err)
			}
			var version, memory string
			var threads int
			if err := d.DB.QueryRow("SELECT version(),current_setting('memory_limit'),current_setting('threads')").Scan(&version, &memory, &threads); err != nil {
				t.Fatal(err)
			}
			t.Logf("engine=%s memory=%s threads=%d traces=%d parts=%d boundary_percent=%d", version, memory, threads, traces, traces*4, boundaryPercent)
			for _, service := range []string{"", "api"} {
				w := queryrows.Window{Start: base, End: base.Add(2 * time.Second), Namespace: "prod", Service: service, Kind: queryrows.TraceCandidateRead}
				compareCompletedTraceWindowPlans(t, d, w)
			}
		})
	}
}

func compareCompletedTraceWindowPlans(t *testing.T, d *Duck, w queryrows.Window) {
	t.Helper()
	const samples = 100
	pin := d.repository.Parquet.Pin()
	defer d.repository.Parquet.Unpin(pin)
	tx, err := d.DB.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	compiled, err := d.compileSnapshot(t.Context(), tx, queryrows.CompletedTraceQuery)
	if err != nil {
		t.Fatal(err)
	}
	batches := d.repository.Parquet.BatchMetadata()
	sources := map[string]string{}
	if err := d.aggregateSources(t.Context(), tx, batches, w, false, sources); err != nil {
		t.Fatal(err)
	}
	markers, err := batchMarkers(t.Context(), tx)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, b := range batches {
		if markers[b.ID] && overlapping(b, "spans", w) {
			ids = append(ids, b.ID)
		}
	}
	scope := "batch_id IN (" + idsSQL(ids) + ") AND namespace=" + sqlLiteral(w.Namespace)
	if w.Service != "" {
		scope += " AND service=" + sqlLiteral(w.Service)
	}
	full := fmt.Sprintf("min_start>=%d AND max_start<%d", w.Start.UnixNano(), w.End.UnixNano())
	index, err := d.traceCandidateSource(t.Context(), tx, batches, markers, w)
	if err != nil {
		t.Fatal(err)
	}
	partial := "SELECT * FROM (" + index + ") WHERE NOT (" + full + ") AND " + fmt.Sprintf("max_start>=%d AND min_start<%d", w.Start.UnixNano(), w.End.UnixNano())
	// Keep both experimental expressions here so a negative result remains
	// reproducible without changing product code or its materialization policy.
	bodies := []string{
		`SELECT namespace,trace_id,min(min_start) AS min_start,max(max_end) AS max_end,max(has_error) AS has_error FROM read_trace_parts WHERE ` + scope + ` AND ` + full + ` GROUP BY namespace,trace_id`,
		"SELECT namespace,trace_id,min_start,max_end,has_error FROM (" + index + ") WHERE " + full +
			` UNION ALL SELECT namespace,trace_id,min(min_start),max(max_end),max(has_error) FROM read_trace_parts WHERE ` + scope + ` AND ` + full +
			` AND (namespace,trace_id) IN (SELECT namespace,trace_id FROM (` + partial + `)) GROUP BY namespace,trace_id`,
	}
	if sources["trace_candidates"] != bodies[0] {
		t.Fatal("benchmark baseline no longer matches the production candidate expression")
	}
	var queries, names []string
	for i, body := range bodies {
		sources["trace_candidates"] = body
		bound, err := bindCompiledSnapshot(compiled, sources)
		if err != nil {
			t.Fatal(err)
		}
		marker := compiled.names["trace_candidates"] + " AS NOT MATERIALIZED ("
		if strings.Count(bound, marker) != 1 {
			t.Fatal("benchmark requires the current candidate inlining policy")
		}
		queries = append(queries, bound, strings.Replace(bound, marker, compiled.names["trace_candidates"]+" AS MATERIALIZED (", 1))
		names = append(names, fmt.Sprintf("expression_%d_inline", i), fmt.Sprintf("expression_%d_shared", i))
	}
	args := []any{w.Start, w.End, w.Namespace, w.Namespace, w.Service, w.Service, 20}
	run := func(query string) []completedTraceWindowRow {
		rows, err := tx.QueryContext(t.Context(), query, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []completedTraceWindowRow
		for rows.Next() {
			var row completedTraceWindowRow
			if err := rows.Scan(&row.namespace, &row.trace, &row.hasError); err != nil {
				t.Fatal(err)
			}
			out = append(out, row)
		}
		if err := rows.Err(); err != nil || len(out) != 20 {
			t.Fatalf("completed rows=%d error=%v", len(out), err)
		}
		return out
	}
	rawWindow := w
	rawWindow.Kind = queryrows.RawRead
	want := completedTraceWindowRows(t, d, rawWindow, rawCompletedTraceWindowQuery, args)
	// The actual completed-query API projects identities/errors. The separate
	// lifecycle oracle compares its internal bounds as well.
	for i := range want {
		want[i].minStart, want[i].maxEnd = 0, 0
	}
	for _, query := range queries {
		for range 3 {
			if got := run(query); !reflect.DeepEqual(got, want) {
				t.Fatalf("experimental query changed ranking: got=%v want=%v", got, want)
			}
		}
	}
	timings := make([][]float64, len(queries))
	for sample := range samples {
		for offset := range queries {
			i := (sample + offset) % len(queries)
			started := time.Now()
			run(queries[i])
			timings[i] = append(timings[i], float64(time.Since(started))/float64(time.Millisecond))
		}
	}
	for i, values := range timings {
		sort.Float64s(values)
		t.Logf("scope_service=%q variant=%s bound_execution_only=true samples=%d median_ms=%.3f p95_ms=%.3f", w.Service, names[i], samples, (values[49]+values[50])/2, values[94])
	}
}
