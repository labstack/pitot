//go:build readbench

package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/engine"
	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// Compare equivalent native histogram expansion strategies through the complete
// Executor. Keep the previous lateral plan reproducible after the product change.
type histogramExpansionEngine struct {
	*engine.Duck
	project bool
	mu      sync.Mutex
	query   string
	args    []any
}

func (e *histogramExpansionEngine) QueryContext(ctx context.Context, query string, args ...any) (queryrows.Rows, error) {
	for _, source := range []string{"base", "folded"} {
		lateral := "FROM " + source + ",unnest(map_entries(hist)) entries(e)"
		projected := "FROM (SELECT *,unnest(map_entries(hist)) AS e FROM " + source + ") __hist_entries"
		if e.project {
			query = strings.ReplaceAll(query, lateral, projected)
		} else {
			query = strings.ReplaceAll(query, projected, lateral)
		}
	}
	if strings.Contains(query, "__panel_hist_bins") {
		e.mu.Lock()
		e.query, e.args = query, append([]any(nil), args...)
		e.mu.Unlock()
	}
	return e.Duck.QueryContext(ctx, query, args...)
}

func BenchmarkHistogramExpansionComparison(b *testing.B) {
	data := preparePanelRegression(b)
	// The base fixture has equal service/severity counts. Break those ties in
	// canonical telemetry so complete result ordering can be compared exactly.
	repo, err := telemetrystore.Open(filepath.Join(data.Dir, "telemetry"))
	if err != nil {
		b.Fatal(err)
	}
	extra := telemetrystore.Batch{ID: "distinct-counts"}
	for service := range 6 {
		for item := range service + 1 {
			at := data.End.Add(-time.Hour).Add(time.Duration(item) * time.Nanosecond).UnixNano()
			name, id := fmt.Sprintf("svc%d", service), fmt.Sprintf("distinct-%d-%d", service, item)
			extra.Spans = append(extra.Spans, telemetry.Span{Namespace: "regression", ServiceName: name, TraceID: id, SpanID: id, StartUnixNanos: at, EndUnixNanos: at + 1000000, DurationMS: 1, StatusCode: "STATUS_CODE_OK", IngestedAt: at})
			extra.Logs = append(extra.Logs, telemetry.Log{Namespace: "regression", ServiceName: name, TraceID: id, TimeUnixNanos: at, EventUnixNanos: at, IngestedAt: at, Severity: []string{"INFO", "WARN", "ERROR"}[service%3], Body: "comparison fixture"})
		}
	}
	if err := repo.Commit(b.Context(), extra); err != nil {
		b.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		b.Fatal(err)
	}
	data.Spans += int64(len(extra.Spans))
	data.Logs += int64(len(extra.Logs))
	d, cfg := panelBenchEngine(b, data)
	metadata, err := panelReplayMetadata(cfg.TelemetryDir())
	if err != nil {
		b.Fatal(err)
	}
	horizon := metadata[0].MaxIngestedNanos
	for _, batch := range metadata {
		horizon = max(horizon, batch.MaxIngestedNanos)
	}
	panelBenchWarmDashboard(b, d, horizon)
	var version, memory string
	var threads int
	if err := d.DB.QueryRow("SELECT version(),current_setting('memory_limit'),current_setting('threads')").Scan(&version, &memory, &threads); err != nil {
		b.Fatal(err)
	}
	run := func(b *testing.B, spec Dashboard) {
		b.Logf("engine=%s memory=%s threads=%d spans=%d logs=%d result_cache_ttl=0 analytical_rollups=warm", version, memory, threads, data.Spans, data.Logs)
		compareHistogramExpansion(b, d, spec, data.End)
	}
	dashboard := panelBenchmarkDashboard(data)
	panels := []Panel{
		{ID: "latency", Viz: "timeseries", Query: &Spec{From: "spans", Measures: []string{"p95(duration_ms)"}, Bucket: "auto"}},
		{ID: "quantile_totals", Viz: "stat", Query: &Spec{From: "spans", Measures: []string{"p95(duration_ms)"}}},
		{ID: "quantile_multiple", Viz: "timeseries", Query: &Spec{From: "spans", Measures: []string{"p50(duration_ms)", "p95(duration_ms)", "p99(duration_ms)", "avg(duration_ms)"}, Bucket: "auto"}},
		{ID: "quantile_rank", Viz: "timeseries", Query: &Spec{From: "spans", Measures: []string{"p95(duration_ms)"}, By: []string{"service"}, Bucket: "auto"}, Options: &Options{Top: 3}},
		{ID: "quantile_table", Viz: "table", Query: &Spec{From: "spans", Measures: []string{"p95(duration_ms)"}, By: []string{"service"}, Limit: 3}},
		{ID: "quantile_trends", Viz: "table", Query: &Spec{From: "spans", Measures: []string{"p95(duration_ms)"}, By: []string{"service"}, Limit: 3}, Options: &Options{Columns: []ColumnFormat{{Field: "p95", Format: "sparkline"}}}},
		{ID: "raw_trace_count", Viz: "table", Query: &Spec{From: "spans", Measures: []string{"count_distinct(trace_id)"}}},
	}
	for _, panel := range panels {
		panel.Title = panel.ID
		spec := dashboard
		spec.Panels, spec.Variables = []Panel{panel}, nil
		b.Run(panel.ID, func(b *testing.B) { run(b, spec) })
	}
	comparison := dashboard
	comparison.Panels, comparison.Variables = []Panel{panels[1]}, nil
	comparison.Panels[0].Title = "Previous period"
	comparisonStart := data.Start.Add(12 * time.Hour)
	comparison.Time.From, comparison.Time.Compare = &comparisonStart, "previous_period"
	b.Run("quantile_compare", func(b *testing.B) { run(b, comparison) })
	b.Run("dashboard_twelve", func(b *testing.B) { run(b, dashboard) })
}

func compareHistogramExpansion(b *testing.B, d *engine.Duck, spec Dashboard, end time.Time) {
	b.StopTimer()
	engines := []*histogramExpansionEngine{{Duck: d}, {Duck: d, project: true}}
	executors := make([]*Executor, len(engines))
	for i, engine := range engines {
		executors[i] = NewExecutor(engine, 30)
		executors[i].SetCacheTTL(0)
		executors[i].now = func() time.Time { return end }
		b.Cleanup(executors[i].Close)
	}
	run := func(i int) []Result {
		results, err := executors[i].Run(b.Context(), RunRequest{Dashboard: spec})
		if err != nil || len(results) != len(spec.Panels) {
			b.Fatalf("variant=%d results=%d error=%v", i, len(results), err)
		}
		for _, result := range results {
			if result.Status != StatusOK || result.Frame == nil || result.Frame.Rows == 0 || result.partial {
				b.Fatalf("variant=%d panel=%s status=%s error=%s partial=%t", i, result.ID, result.Status, result.Error, result.partial)
			}
		}
		return results
	}
	canonical := func(results []Result) []Result {
		for i := range results {
			results[i].ElapsedMS, results[i].SQL = 0, ""
		}
		return results
	}
	for range 3 {
		want, got := canonical(run(0)), canonical(run(1))
		if !reflect.DeepEqual(want, got) {
			for i := range want {
				if !reflect.DeepEqual(want[i], got[i]) {
					a, _ := json.Marshal(want[i])
					z, _ := json.Marshal(got[i])
					b.Fatalf("histogram expansion changed panel %s: baseline=%s projected=%s", want[i].ID, a, z)
				}
			}
		}
	}
	if engines[0].query != "" && engines[0].query == engines[1].query {
		b.Fatal("histogram comparison no longer produces distinct baseline and projected SQL")
	}
	timings := make([][]float64, 2)
	b.ResetTimer()
	b.StartTimer()
	for sample := range b.N {
		for offset := range 2 {
			i := (sample + offset) % 2
			started := time.Now()
			run(i)
			timings[i] = append(timings[i], float64(time.Since(started))/float64(time.Millisecond))
		}
	}
	b.StopTimer()
	for i, values := range timings {
		sort.Float64s(values)
		b.ReportMetric(values[(95*len(values)+99)/100-1], fmt.Sprintf("variant%d_p95_ms", i))
		b.Logf("variant=%d samples=%d p50_ms=%.3f p95_ms=%.3f cache_ttl=0 full_executor=true", i, len(values), values[(50*len(values)+99)/100-1], values[(95*len(values)+99)/100-1])
	}
	if b.N >= 100 && len(spec.Panels) == 1 && spec.Panels[0].ID == "latency" {
		for i, engine := range engines {
			var key, plan string
			if err := d.DB.QueryRowContext(b.Context(), "EXPLAIN (ANALYZE, FORMAT JSON) "+engine.query, engine.args...).Scan(&key, &plan); err != nil {
				b.Fatal(err)
			}
			var compact bytes.Buffer
			if err := json.Compact(&compact, []byte(plan)); err != nil {
				b.Fatal(err)
			}
			b.Logf("native_profile_variant=%d %s=%s", i, key, compact.String())
		}
	}
}
