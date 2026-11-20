//go:build readbench

package observability

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/config"
	"github.com/labstack/pitot/internal/engine"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// Time trace detail over a window holding many log rows: the service read,
// the indexed span read alone, and the repository's spans-and-logs read the
// service makes. PITOT_TRACE_BENCH_BATCHES sets
// the committed batches of 100,000 spans and 100,000 logs, each log carrying
// its own trace's ID as an instrumented service would.
func TestTraceDetailLogsBenchmark(t *testing.T) {
	batches := 10
	if v, err := strconv.Atoi(os.Getenv("PITOT_TRACE_BENCH_BATCHES")); err == nil && v > 0 {
		batches = v
	}
	const rows = 100000
	cfg := config.Config{DataDir: t.TempDir(), DuckDBMemory: "4GB", DuckDBThreads: 8, DuckDBMaxConns: 4, RollupInterval: time.Hour}
	repo, err := telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	at := time.Now().UTC().Truncate(time.Minute)
	// Rows spread evenly over the newest 20 hours, inside the 24-hour window.
	step := 20 * time.Hour / time.Duration(batches*rows)
	for batch := range batches {
		b := telemetrystore.Batch{ID: fmt.Sprint("b", batch)}
		for i := range rows {
			n := at.Add(-time.Duration(batch*rows+i) * step).UnixNano()
			trace := fmt.Sprintf("%016x%016x", uint64(batch*rows+i/2+1)*uint64(0x9e3779b97f4a7c15), uint64(i/2+1)*uint64(0xbf58476d1ce4e5b9))
			service := fmt.Sprint("service", i%4)
			b.Spans = append(b.Spans, telemetry.Span{Namespace: "prod", ServiceName: service, TraceID: trace, SpanID: fmt.Sprintf("%016x", i+1), StartUnixNanos: n, EndUnixNanos: n + 1e6, DurationMS: 1, StatusCode: "STATUS_CODE_OK", IngestedAt: n})
			b.Logs = append(b.Logs, telemetry.Log{Namespace: "prod", ServiceName: service, TraceID: trace, SpanID: fmt.Sprintf("%016x", i+1), TimeUnixNanos: n, Severity: "INFO", Body: "request complete", IngestedAt: n})
		}
		if err := repo.Commit(t.Context(), b); err != nil {
			t.Fatal(err)
		}
	}
	d, err := engine.NewDuck(t.Context(), cfg, repo)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	// PITOT_TRACE_BENCH_COMPACT=1 merges the batches as a long-running server
	// does, so lookups open few large files instead of many small ones.
	if os.Getenv("PITOT_TRACE_BENCH_COMPACT") == "1" {
		for {
			n, err := repo.CompactParquet(t.Context(), d, 64)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				break
			}
		}
	}
	svc := New(d, d, 30)
	scope := Scope{Start: at.Add(-24 * time.Hour), End: at.Add(time.Second), Namespace: "prod"}
	pick, row := uint64(batches/2*rows+rows/4+1), uint64(rows/4+1)
	trace := fmt.Sprintf("%016x%016x", pick*uint64(0x9e3779b97f4a7c15), row*uint64(0xbf58476d1ce4e5b9))
	// Drill-down reads the summaries of its 20 ranked traces over the panel
	// window; spread them over the batches as ranked traces are.
	var ids []telemetry.TraceIdentity
	for k := range 20 {
		b, i := uint64(k*batches/20), uint64(k*997%(rows/2))
		ids = append(ids, telemetry.TraceIdentity{Namespace: "prod", TraceID: fmt.Sprintf("%016x%016x", (b*rows+i+1)*uint64(0x9e3779b97f4a7c15), (i+1)*uint64(0xbf58476d1ce4e5b9))})
	}
	const samples = 7
	var total, spans, detail, summaries []float64
	for range samples {
		started := time.Now()
		got, err := svc.Trace(t.Context(), scope, trace, "", 100)
		if err != nil {
			t.Fatal(err)
		}
		total = append(total, ms(time.Since(started)))
		if got.Data.SpanCount != 2 || len(got.Data.Logs) != 2 {
			t.Fatalf("spans=%d logs=%d", got.Data.SpanCount, len(got.Data.Logs))
		}
		started = time.Now()
		if _, _, err := d.Trace(t.Context(), telemetry.TraceQuery{TraceID: trace, Namespace: "prod", StartNanos: scope.Start.UnixNano(), EndNanos: scope.End.UnixNano(), Limit: 100}); err != nil {
			t.Fatal(err)
		}
		spans = append(spans, ms(time.Since(started)))
		started = time.Now()
		if got, err := d.TraceDetail(t.Context(), telemetry.TraceQuery{TraceID: trace, Namespace: "prod", StartNanos: scope.Start.UnixNano(), EndNanos: scope.End.UnixNano(), Limit: 100}); err != nil || len(got.Spans) != 2 || len(got.Logs) != 2 {
			t.Fatalf("spans=%d logs=%d err=%v", len(got.Spans), len(got.Logs), err)
		}
		detail = append(detail, ms(time.Since(started)))
		started = time.Now()
		totals, err := d.TraceSummaries(t.Context(), ids, scope.Start.UnixNano(), scope.End.UnixNano())
		if err != nil {
			t.Fatal(err)
		}
		summaries = append(summaries, ms(time.Since(started)))
		for i, total := range totals {
			if total.Root == nil {
				t.Fatalf("summary %d has no root", i)
			}
		}
	}
	t.Logf("log_rows=%d trace_ms=%.1f spans_ms=%.1f detail_ms=%.1f summaries_ms=%.1f", batches*rows, median(total), median(spans), median(detail), median(summaries))
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func median(v []float64) float64 {
	sort.Float64s(v)
	return v[len(v)/2]
}
