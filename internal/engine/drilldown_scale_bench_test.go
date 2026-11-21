//go:build filebench

package engine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/config"
	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// Drill-down (indexedExemplars) cost at day scale. PITOT_DD_CACHED batches of
// PITOT_DD_SPANS telemetrygen-shaped spans (parent+child, one service) are
// spread over PITOT_DD_HOURS and fully cached, then uncached recent batches
// of PITOT_DD_SMALL spans (+ as many logs and metrics) are added in steps of
// PITOT_DD_STEPS, and optionally PITOT_DD_BIG uncached compacted outputs of
// PITOT_DD_BIG_SPANS spans. Each step times the ranking, its bind/exec split,
// TraceSummaries for the picked 20, and an EXPLAIN ANALYZE when
// PITOT_DD_EXPLAIN names a window. Finally it times the read-cache drain.
func TestDrilldownScaleBenchmark(t *testing.T) {
	env := func(name string, def int) int {
		if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v >= 0 {
			return v
		}
		return def
	}
	cached, spansPer, hours := env("PITOT_DD_CACHED", 500), env("PITOT_DD_SPANS", 100000), env("PITOT_DD_HOURS", 5)
	small, big, bigSpans := env("PITOT_DD_SMALL", 500), env("PITOT_DD_BIG", 0), env("PITOT_DD_BIG_SPANS", 1000000)
	steps := []int{0, 50, 200}
	if s := os.Getenv("PITOT_DD_STEPS"); s != "" {
		steps = nil
		for _, f := range strings.Split(s, ",") {
			n, _ := strconv.Atoi(f)
			steps = append(steps, n)
		}
	}
	dir := os.Getenv("PITOT_DD_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	cfg := config.Config{DataDir: filepath.Join(dir, "data"), DuckDBMemory: "16GB", DuckDBThreads: 8, DuckDBMaxConns: 4}
	repo, err := telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDuck(t.Context(), cfg, repo)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close(); _ = repo.Close() })

	now := time.Now().UTC().Truncate(time.Second)
	retained := time.Duration(hours) * time.Hour
	base := now.Add(-retained)
	rng := rand.New(rand.NewPCG(1, 2))
	seq := uint64(0)
	// spans of n/2 two-span traces evenly over [from, from+spread).
	spans := func(n int, from time.Time, spread time.Duration) []telemetry.Span {
		out := make([]telemetry.Span, 0, n)
		traces := max(1, n/2)
		for i := range traces {
			seq++
			trace := fmt.Sprintf("%016x%016x", seq*0x9e3779b97f4a7c15, rng.Uint64())
			start := from.Add(time.Duration(float64(spread) * float64(i) / float64(traces)))
			status := "STATUS_CODE_UNSET"
			if rng.IntN(1000) == 0 {
				status = "STATUS_CODE_ERROR"
			}
			durUS := 100 + rng.IntN(60)
			for s := range 2 {
				parent := ""
				if s == 1 {
					parent = "0000000000000001"
				}
				end := start.Add(time.Duration(durUS-s*20) * time.Microsecond)
				out = append(out, telemetry.Span{ServiceName: "telemetrygen", TraceID: trace, SpanID: fmt.Sprintf("%016x", s+1), ParentSpanID: parent,
					Name: "okey-dokey-" + strconv.Itoa(s), Kind: "SPAN_KIND_SERVER", StartUnixNanos: start.UnixNano(), EndUnixNanos: end.UnixNano(),
					DurationMS: float64(end.Sub(start)) / 1e6, StatusCode: status, Attributes: map[string]any{"net.peer.ip": "1.2.3.4", "peer.service": "telemetrygen-client"},
					Resource: map[string]any{"service.name": "telemetrygen"}, IngestedAt: start.UnixNano()})
			}
		}
		return out
	}
	mixed := func(n int, from time.Time, spread time.Duration) telemetrystore.Batch {
		b := telemetrystore.Batch{Spans: spans(n, from, spread)}
		for i := range n {
			at := from.Add(time.Duration(int64(spread) * int64(i) / int64(max(1, n)))).UnixNano()
			b.Logs = append(b.Logs, telemetry.Log{ServiceName: "telemetrygen", Body: "the message", Severity: "Info", TimeUnixNanos: at, EventUnixNanos: at, IngestedAt: at,
				Resource: map[string]any{"service.name": "telemetrygen"}})
			b.Metrics = append(b.Metrics, telemetry.Metric{ServiceName: "telemetrygen", Name: "gen", Type: "gauge", Value: float64(i), TimeUnixNanos: at, EventUnixNanos: at, IngestedAt: at,
				Resource: map[string]any{"service.name": "telemetrygen"}})
		}
		return b
	}
	commit := func(id string, b telemetrystore.Batch) {
		b.ID = id
		if err := repo.Commit(t.Context(), b); err != nil {
			t.Fatal(err)
		}
	}
	// Cached history. Leave the newest 2 minutes for the tail.
	historyEnd := now.Add(-2 * time.Minute)
	slice := historyEnd.Sub(base) / time.Duration(max(1, cached))
	started := time.Now()
	for part := range cached {
		commit(fmt.Sprintf("hist_%05d", part), telemetrystore.Batch{Spans: spans(spansPer, base.Add(time.Duration(part)*slice), slice)})
	}
	t.Logf("committed cached=%d spans=%d in %s", cached, cached*spansPer, time.Since(started).Round(time.Second))
	drain := func(label string) {
		var passes []float64
		var total int64
		started := time.Now()
		for {
			s := time.Now()
			rows, err := d.RefreshReadCaches(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if rows == 0 {
				break
			}
			total += rows
			passes = append(passes, float64(time.Since(s))/1e6)
		}
		sorted := append([]float64(nil), passes...)
		sort.Float64s(sorted)
		med := 0.0
		if len(sorted) > 0 {
			med = sorted[len(sorted)/2]
		}
		t.Logf("drain %s passes=%d total=%s median_pass_ms=%.0f rows_written=%d", label, len(passes), time.Since(started).Round(time.Millisecond), med, total)
	}
	drain("history")
	var exemplarRows int
	_ = d.DB.QueryRow(`SELECT count(*) FROM read_trace_exemplars`).Scan(&exemplarRows)
	t.Logf("read_trace_exemplars rows=%d", exemplarRows)

	windows := []struct {
		name string
		dur  time.Duration
	}{{"15m", 15 * time.Minute}, {"1h", time.Hour}, {"6h", 6 * time.Hour}, {"24h", 24 * time.Hour}}
	explain := os.Getenv("PITOT_DD_EXPLAIN")
	median := func(xs []float64) float64 { sort.Float64s(xs); return xs[len(xs)/2] }
	measure := func(label string) {
		end := now.Add(time.Second)
		for _, win := range windows {
			start := end.Add(-win.dur).Add(17 * time.Millisecond)
			w := queryrows.Window{Start: start, End: end, Service: "telemetrygen", Kind: queryrows.TraceCandidateRead}
			args := []any{w.Start, w.End, w.Namespace, w.Namespace, w.Service, w.Service, 21}
			rank := func() []telemetry.TraceIdentity {
				rows, err := d.QueryContext(queryrows.WithWindow(t.Context(), w), queryrows.CompletedTraceQuery, args...)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				var ids []telemetry.TraceIdentity
				for rows.Next() {
					var id telemetry.TraceIdentity
					var e int
					if err := rows.Scan(&id.Namespace, &id.TraceID, &e); err != nil {
						t.Fatal(err)
					}
					ids = append(ids, id)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				return ids
			}
			ids := rank()
			const samples = 5
			var rankMS, sumMS, bindMS, execMS []float64
			for range samples {
				s := time.Now()
				ids = rank()
				rankMS = append(rankMS, float64(time.Since(s))/1e6)
			}
			ids = ids[:min(20, len(ids))]
			for range samples {
				s := time.Now()
				if _, err := d.TraceSummaries(t.Context(), ids, start.UnixNano(), end.UnixNano()); err != nil {
					t.Fatal(err)
				}
				sumMS = append(sumMS, float64(time.Since(s))/1e6)
			}
			func() {
				pin := d.repository.Parquet.Pin()
				defer d.repository.Parquet.Unpin(pin)
				tx, err := d.DB.BeginTx(t.Context(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback() }()
				var bound string
				for range samples {
					s := time.Now()
					bound, err = d.bindSnapshot(t.Context(), tx, queryrows.CompletedTraceQuery, w)
					if err != nil {
						t.Fatal(err)
					}
					bindMS = append(bindMS, float64(time.Since(s))/1e6)
					s = time.Now()
					rows, err := tx.QueryContext(t.Context(), bound, args...)
					if err != nil {
						t.Fatal(err)
					}
					for rows.Next() {
					}
					err = rows.Err()
					rows.Close()
					if err != nil {
						t.Fatal(err)
					}
					execMS = append(execMS, float64(time.Since(s))/1e6)
				}
				if explain == win.name || explain == "all" {
					var key, plan string
					if err := tx.QueryRowContext(t.Context(), "EXPLAIN ANALYZE "+bound, args...).Scan(&key, &plan); err != nil {
						t.Fatal(err)
					}
					t.Logf("EXPLAIN %s %s\n%s", label, win.name, plan)
				}
			}()
			t.Logf("RESULT %s window=%s ids=%d rank_ms=%.1f bind_ms=%.1f exec_ms=%.1f summaries_ms=%.1f", label, win.name, len(ids), median(rankMS), median(bindMS), median(execMS), median(sumMS))
		}
	}
	added := 0
	for _, n := range steps {
		for ; added < n; added++ {
			// ~25 small batches per second of ingest, newest data.
			from := now.Add(-time.Duration(n-added) * 40 * time.Millisecond)
			commit(fmt.Sprintf("tail_%05d", added), mixed(small, from, 40*time.Millisecond))
		}
		measure(fmt.Sprintf("uncached_small=%d", n))
	}
	if big > 0 {
		for i := range big {
			// Compacted outputs of recent, still-uncached inputs.
			from := now.Add(-time.Duration(big-i) * 10 * time.Minute)
			commit(fmt.Sprintf("big_%03d", i), telemetrystore.Batch{Spans: spans(bigSpans, from, 10*time.Minute)})
		}
		measure(fmt.Sprintf("uncached_small=%d+big=%dx%d", added, big, bigSpans))
	}
	drain("tail")
	measure("drained")
	// Under write load: ingest goroutines commit small mixed batches at
	// PITOT_DD_RATE batches/s while the read-cache loop drains, and drills run
	// back to back (24h window) for PITOT_DD_LOAD seconds.
	if load := env("PITOT_DD_LOAD", 0); load > 0 {
		rate := env("PITOT_DD_RATE", 25)
		if rate <= 0 {
			t.Fatalf("PITOT_DD_RATE=%d must be positive", rate)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Duration(load)*time.Second)
		defer cancel()
		done := make(chan struct{}, 4)
		var committed, passes atomic.Int64
		var generate sync.Mutex // The generator's rng and sequence are not concurrency-safe.
		for g := range 4 {
			go func() {
				defer func() { done <- struct{}{} }()
				tick := time.NewTicker(time.Second * 4 / time.Duration(rate))
				defer tick.Stop()
				for i := 0; ; i++ {
					select {
					case <-ctx.Done():
						return
					case <-tick.C:
					}
					generate.Lock()
					b := mixed(small, time.Now().UTC(), 40*time.Millisecond)
					generate.Unlock()
					b.ID = fmt.Sprintf("load_%d_%06d", g, i)
					if err := repo.Commit(context.Background(), b); err != nil {
						t.Error(err)
						return
					}
					committed.Add(1)
				}
			}()
		}
		refreshed := make(chan struct{})
		go func() {
			defer close(refreshed)
			for ctx.Err() == nil {
				if _, err := d.RefreshReadCaches(ctx); err == nil {
					passes.Add(1)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()
		var rankMS, sumMS []float64
		for ctx.Err() == nil {
			end := time.Now().UTC()
			start := end.Add(-24 * time.Hour)
			w := queryrows.Window{Start: start, End: end, Service: "telemetrygen", Kind: queryrows.TraceCandidateRead}
			s := time.Now()
			rows, err := d.QueryContext(queryrows.WithWindow(context.Background(), w), queryrows.CompletedTraceQuery, w.Start, w.End, "", "", w.Service, w.Service, 21)
			if err != nil {
				t.Fatal(err)
			}
			var ids []telemetry.TraceIdentity
			for rows.Next() {
				var id telemetry.TraceIdentity
				var e int
				if err := rows.Scan(&id.Namespace, &id.TraceID, &e); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			rankMS = append(rankMS, float64(time.Since(s))/1e6)
			s = time.Now()
			if _, err := d.TraceSummaries(context.Background(), ids[:min(20, len(ids))], start.UnixNano(), end.UnixNano()); err != nil {
				t.Fatal(err)
			}
			sumMS = append(sumMS, float64(time.Since(s))/1e6)
		}
		for range 4 {
			<-done
		}
		<-refreshed
		var pending int
		markers, err := batchMarkers(context.Background(), d.writer())
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range d.repository.Parquet.BatchMetadata() {
			if !markers[b.ID] {
				pending++
			}
		}
		sort.Float64s(rankMS)
		sort.Float64s(sumMS)
		if len(rankMS) == 0 {
			t.Fatal("load phase ended before the first drill-down")
		}
		p := func(xs []float64, q float64) float64 { return xs[int(float64(len(xs)-1)*q)] }
		t.Logf("LOAD seconds=%d committed=%d cache_passes=%d pending_end=%d drills=%d rank_p50=%.1f p90=%.1f max=%.1f summaries_p50=%.1f p90=%.1f",
			load, committed.Load(), passes.Load(), pending, len(rankMS), p(rankMS, .5), p(rankMS, .9), p(rankMS, 1), p(sumMS, .5), p(sumMS, .9))
	}
}
