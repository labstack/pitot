package query

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/config"
	"github.com/labstack/pitot/internal/engine"
	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// Rate and gauge panels read their window bounds as bind parameters, so the
// same panel compiles to identical statement text across different time
// windows. That text stability is what lets the engine's snapshot-compile
// cache hit instead of reparsing a per-request string on every refresh.
func TestRatePanelCompiledTextIsWindowStable(t *testing.T) {
	const interval = time.Minute
	windowA := Scope{Start: fixtureStart, End: fixtureStart.Add(time.Hour), Interval: interval}
	windowB := Scope{Start: fixtureStart.Add(30 * time.Minute), End: fixtureStart.Add(90 * time.Minute), Interval: interval}
	// FreshTo slides with the window so the rollup freshness clamp is exercised
	// and proven to ride as a parameter rather than a literal.
	windowA.FreshTo = windowA.End.Add(-90 * time.Second)
	windowB.FreshTo = windowB.End.Add(-90 * time.Second)

	for _, tc := range []struct {
		name, from, measure, viz string
	}{
		{"rollup rate", "spans", "rate()", "timeseries"},
		{"metric rate", "metrics", "rate()", "timeseries"},
		{"metric gauge avg", "metrics", "avg(value)", "timeseries"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &Panel{Viz: tc.viz, Query: &Spec{From: tc.from}}
			measures := measuresOf(t, tc.from, tc.measure)
			c1, err := compileQuery(p, measures, nil, windowA)
			if err != nil {
				t.Fatal(err)
			}
			c2, err := compileQuery(p, measures, nil, windowB)
			if err != nil {
				t.Fatal(err)
			}
			if c1.SQL != c2.SQL {
				t.Fatalf("statement text changed with the window:\nA: %s\nB: %s", c1.SQL, c2.SQL)
			}
			// Every placeholder must have exactly one argument, or DuckDB would
			// reject the statement; a mismatch here means the window values were
			// not threaded in positional order.
			for _, c := range []Compiled{c1, c2} {
				if n := strings.Count(c.SQL, "?"); n != len(c.Args) {
					t.Fatalf("%d placeholders but %d args:\n%s", n, len(c.Args), c.SQL)
				}
			}
			if len(c1.Args) == 0 {
				t.Fatalf("expected window bounds to bind as parameters, got none:\n%s", c1.SQL)
			}
			if reflect.DeepEqual(c1.Args, c2.Args) {
				t.Fatalf("different windows must bind different args: %v", c1.Args)
			}
		})
	}
}

// A rate panel refreshed across sliding windows now reuses one compiled
// snapshot instead of recompiling each window. The window_stable arm issues the
// real compiled text; window_varying appends a unique per-window token, which
// faithfully reproduces the pre-fix behaviour where embedded nanosecond
// literals made every window's text unique. Compare their reported hit_rate_%
// and ns/op: the stable arm both hits the cache and runs faster.
func BenchmarkRatePanelSnapshotCompileCacheHitRate(b *testing.B) {
	duck := benchMetricEngine(b)
	p := &Panel{Viz: "timeseries", Query: &Spec{From: "metrics", Where: []string{"name='net'"}}}
	var problems Problems
	measures := parseMeasures(signals["metrics"], []string{"rate()"}, "m", &problems)
	if len(problems) > 0 {
		b.Fatal(problems)
	}
	// A window wide enough to pin every seeded file; the compiled statement's
	// own ? bounds do the time filtering.
	wide := queryrows.Window{Start: fixtureStart.Add(-time.Hour), End: fixtureStart.Add(6 * time.Hour)}

	run := func(b *testing.B, vary bool) {
		hits0, misses0 := duck.SnapshotCacheStats()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Each iteration is a different window, as an auto-refreshing
			// dashboard produces.
			shift := time.Duration(i) * time.Minute
			scope := Scope{Start: fixtureStart.Add(shift), End: fixtureStart.Add(shift + time.Hour), Interval: time.Minute}
			c, err := compileQuery(p, measures, nil, scope)
			if err != nil {
				b.Fatal(err)
			}
			sql := c.SQL
			if vary {
				sql += " /* window " + scope.Start.String() + " */"
			}
			rows, err := duck.QueryContext(queryrows.WithWindow(b.Context(), wide), sql, c.Args...)
			if err != nil {
				b.Fatal(err)
			}
			for rows.Next() {
			}
			rows.Close()
		}
		b.StopTimer()
		hits, misses := duck.SnapshotCacheStats()
		dh, dm := hits-hits0, misses-misses0
		rate := 0.0
		if dh+dm > 0 {
			rate = float64(dh) / float64(dh+dm)
		}
		b.ReportMetric(rate*100, "hit_rate_%")
	}
	b.Run("window_stable", func(b *testing.B) { run(b, false) })
	b.Run("window_varying", func(b *testing.B) { run(b, true) })
}

// benchMetricEngine seeds a single counter series so the rate panel returns
// rows while the benchmark measures the read path end to end.
func benchMetricEngine(b *testing.B) *engine.Duck {
	b.Helper()
	cfg := config.Config{DataDir: b.TempDir(), DuckDBMemory: "256MB", DuckDBThreads: 2, DuckDBMaxConns: 4, RollupInterval: time.Hour}
	repo, err := telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		b.Fatal(err)
	}
	d, err := engine.NewDuck(b.Context(), cfg, repo)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = d.Close(); _ = repo.Close() })
	var series []telemetry.Metric
	for i := -2; i < 300; i++ {
		series = append(series, metricCounter(fixtureStart.Add(time.Duration(i)*time.Minute), float64(100+60*(i+2))))
	}
	if err := repo.Commit(b.Context(), telemetrystore.Batch{ID: "bench-metrics", Metrics: series}); err != nil {
		b.Fatal(err)
	}
	return d
}
