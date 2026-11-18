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

// Time the completed-trace kernel as retained traces grow, with a raw tail of
// two uncached recent batches like live ingest. PITOT_SCALE_BATCHES sets the
// fully cached batches of 20,000 two-span traces each; PITOT_SCALE_MIXED=1 puts
// each trace's spans in two services, as real request traces do.
func TestCompletedTraceQueryScaleBenchmark(t *testing.T) {
	mixed := os.Getenv("PITOT_SCALE_MIXED") == "1"
	batches := 20
	if v, err := strconv.Atoi(os.Getenv("PITOT_SCALE_BATCHES")); err == nil && v > 0 {
		batches = v
	}
	const traces = 20000
	d, repo := newBatchCacheTest(t)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	commit := func(part int) {
		batch := telemetrystore.Batch{ID: fmt.Sprintf("part_%d", part)}
		for i := range traces {
			id := uint64(part*traces+i+1) * 0x9e3779b97f4a7c15
			start := base.Add(time.Duration(part)*time.Second + time.Duration(i%1000)*time.Millisecond)
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
	for {
		rows, err := d.RefreshReadCaches(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if rows == 0 {
			break
		}
	}
	// Recent batches the read cache has not reached yet.
	for part := batches; part < batches+2; part++ {
		commit(part)
	}
	w := queryrows.Window{Start: base.Add(-time.Hour), End: base.Add(2 * time.Hour), Namespace: "prod", Service: "api", Kind: queryrows.TraceCandidateRead}
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
	t.Logf("cached_traces=%d tail_traces=%d mixed_services=%v median_ms=%.1f max_ms=%.1f", batches*traces, 2*traces, mixed, ms[samples/2], ms[samples-1])
}
