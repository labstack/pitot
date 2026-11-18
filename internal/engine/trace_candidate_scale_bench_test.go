//go:build filebench

package engine

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// Time the completed-trace ranking over an unaligned 24-hour window as
// retained traces grow, with a raw tail of two uncached recent batches like
// live ingest, and the read-cache passes that build its rows.
// PITOT_SCALE_BATCHES sets the fully cached batches of 20,000 two-span traces
// each, spread over PITOT_SCALE_HOURS (default 24) so that retention can exceed
// the window; PITOT_SCALE_MIXED=1 puts each trace's spans in two services, as
// real request traces do. The window is the newest 24 hours.
func TestCompletedTraceQueryScaleBenchmark(t *testing.T) {
	mixed := os.Getenv("PITOT_SCALE_MIXED") == "1"
	batches := 20
	if v, err := strconv.Atoi(os.Getenv("PITOT_SCALE_BATCHES")); err == nil && v > 0 {
		batches = v
	}
	const traces = 20000
	d, repo := newBatchCacheTest(t)
	hours := 24
	if v, err := strconv.Atoi(os.Getenv("PITOT_SCALE_HOURS")); err == nil && v >= 24 {
		hours = v
	}
	retained := time.Duration(hours) * time.Hour
	base := time.Now().UTC().Add(-retained - time.Hour).Truncate(time.Second)
	spacing := retained / time.Duration(batches+2)
	commit := func(part int) {
		batch := telemetrystore.Batch{ID: fmt.Sprintf("part_%d", part)}
		for i := range traces {
			id := uint64(part*traces+i+1) * 0x9e3779b97f4a7c15
			start := base.Add(time.Duration(part)*spacing + time.Duration(i%1000)*time.Millisecond)
			status := "STATUS_CODE_OK"
			if i%97 == 0 {
				status = "STATUS_CODE_ERROR"
			}
			for span := range 2 {
				service := "api"
				if mixed && span == 1 {
					service = "db"
				}
				batch.Spans = append(batch.Spans, telemetry.Span{Namespace: "prod", ServiceName: service,
					TraceID: fmt.Sprintf("%032x", id), SpanID: fmt.Sprintf("%016x", span+1),
					StartUnixNanos: start.UnixNano(), EndUnixNanos: start.Add(time.Duration(1+i%50) * time.Millisecond).UnixNano(),
					DurationMS: float64(1 + i%50), StatusCode: status, IngestedAt: start.UnixNano()})
			}
		}
		if err := repo.Commit(t.Context(), batch); err != nil {
			t.Fatal(err)
		}
	}
	for part := range batches {
		commit(part)
	}
	// One pass stops at the row budget; repeat until every batch is cached.
	var passes []float64
	refresh := func() {
		for {
			started := time.Now()
			rows, err := d.RefreshReadCaches(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if rows == 0 {
				return
			}
			passes = append(passes, float64(time.Since(started))/float64(time.Millisecond))
		}
	}
	refresh()
	first, last := passes[:min(5, len(passes))], passes[max(0, len(passes)-5):]
	var exemplars int
	if err := d.DB.QueryRow(`SELECT count(*) FROM read_trace_exemplars`).Scan(&exemplars); err != nil {
		t.Fatal(err)
	}
	// The pass that caches two more batches over the whole retained history.
	for part := batches + 2; part < batches+4; part++ {
		commit(part)
	}
	passes = passes[:0]
	refresh()
	t.Logf("passes first_ms=%.0f last_ms=%.0f exemplar_rows=%d two_batch_pass_ms=%.0f", first, last, exemplars, passes)
	// Recent batches the read cache has not reached yet.
	for part := batches; part < batches+2; part++ {
		commit(part)
	}
	start := base.Add(retained - 24*time.Hour + 17*time.Second)
	w := queryrows.Window{Start: start, End: start.Add(24 * time.Hour), Namespace: "prod", Service: "api", Kind: queryrows.TraceCandidateRead}
	args := []any{w.Start, w.End, w.Namespace, w.Namespace, w.Service, w.Service, 21}
	run := func(query string) []string {
		rows, err := d.QueryContext(queryrows.WithWindow(t.Context(), w), query, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var namespace, trace string
			var hasError int
			if err := rows.Scan(&namespace, &trace, &hasError); err != nil {
				t.Fatal(err)
			}
			out = append(out, fmt.Sprint(namespace, trace, hasError))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got := run(queryrows.CompletedTraceQuery); len(got) != 21 {
		t.Fatalf("completed rows=%d", len(got))
	}
	const samples = 15
	var ms []float64
	for range samples {
		started := time.Now()
		run(queryrows.CompletedTraceQuery)
		ms = append(ms, float64(time.Since(started))/float64(time.Millisecond))
	}
	sort.Float64s(ms)
	// Split snapshot binding from execution of the bound statement.
	pin := d.repository.Parquet.Pin()
	defer d.repository.Parquet.Unpin(pin)
	tx, err := d.DB.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var bind, exec []float64
	var bound string
	for range samples {
		started := time.Now()
		bound, err = d.bindSnapshot(t.Context(), tx, queryrows.CompletedTraceQuery, w)
		if err != nil {
			t.Fatal(err)
		}
		bind = append(bind, float64(time.Since(started))/float64(time.Millisecond))
		started = time.Now()
		rows, err := tx.QueryContext(t.Context(), bound, args...)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
		}
		rows.Close()
		exec = append(exec, float64(time.Since(started))/float64(time.Millisecond))
	}
	sort.Float64s(bind)
	sort.Float64s(exec)
	if os.Getenv("PITOT_SCALE_EXPLAIN") == "1" {
		var key, plan string
		if err := tx.QueryRowContext(t.Context(), "EXPLAIN ANALYZE "+bound, args...).Scan(&key, &plan); err != nil {
			t.Fatal(err)
		}
		t.Log(plan)
	}
	t.Logf("retained_hours=%d cached_traces=%d tail_traces=%d mixed_services=%v median_ms=%.1f max_ms=%.1f bind_ms=%.1f exec_ms=%.1f", hours, batches*traces, 2*traces, mixed, ms[samples/2], ms[samples-1], bind[samples/2], exec[samples/2])
}
