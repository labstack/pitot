//go:build readbench

package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/config"
	"github.com/labstack/pitot/internal/engine"
	"github.com/labstack/pitot/internal/queryrows"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// This experiment rewrites only the compiler's known histogram-bin fragment.
// It is deliberately test-only: repeated-map compression is a hypothesis until
// both full-executor and mixed-load comparisons clear the observed regression.
var weightedHistogramFragment = regexp.MustCompile(`, ([a-zA-Z0-9_]+)_bins AS \(SELECT (.*?)e.key AS __hist_k,sum\(e.value\)::DOUBLE AS __hist_n FROM \(SELECT \*,unnest\(map_entries\(hist\)\) AS e FROM ([a-zA-Z0-9_]+)\) __hist_entries(.*?)\), ([a-zA-Z0-9_]+)_cdf AS`)

func histogramExperimentSQL(query, variant string) (string, error) {
	for _, source := range []string{"base", "folded"} {
		lateral := "FROM " + source + ",unnest(map_entries(hist)) entries(e)"
		projected := "FROM (SELECT *,unnest(map_entries(hist)) AS e FROM " + source + ") __hist_entries"
		if variant == "lateral" {
			query = strings.ReplaceAll(query, projected, lateral)
		} else {
			query = strings.ReplaceAll(query, lateral, projected)
		}
	}
	if variant == "lateral" || variant == "projection" {
		return query, nil
	}
	if variant != "weighted" {
		return "", fmt.Errorf("unknown histogram experiment %q", variant)
	}
	var rewriteErr error
	matched := 0
	query = weightedHistogramFragment.ReplaceAllStringFunc(query, func(fragment string) string {
		match := weightedHistogramFragment.FindStringSubmatch(fragment)
		prefix, hidden, source := match[1], match[2], match[3]
		if prefix != match[5] {
			rewriteErr = fmt.Errorf("histogram CTE prefix mismatch")
			return fragment
		}
		marker, rest, ok := strings.Cut(hidden, " AS __hist_group,")
		if !ok {
			rewriteErr = fmt.Errorf("unrecognized histogram group marker %q", hidden)
			return fragment
		}
		var groups, projected []string
		for i := 0; rest != ""; i++ {
			expression, remaining, ok := strings.Cut(rest, fmt.Sprintf(" AS __hist_key_%d,", i))
			if !ok {
				rewriteErr = fmt.Errorf("unrecognized histogram grouping expression %q", rest)
				return fragment
			}
			alias := fmt.Sprintf("__hist_input_%d", i)
			groups = append(groups, alias)
			projected = append(projected, expression+" AS "+alias)
			rest = remaining
		}
		totals := strings.HasPrefix(marker, "grouping(")
		if marker != "0" && !totals || totals && len(groups) == 0 {
			rewriteErr = fmt.Errorf("unrecognized histogram total marker %q", marker)
			return fragment
		}
		newHidden := []string{"0 AS __hist_group"}
		if totals {
			newHidden[0] = "grouping(" + groups[0] + ") AS __hist_group"
		}
		for i, group := range groups {
			newHidden = append(newHidden, group+fmt.Sprintf(" AS __hist_key_%d", i))
		}
		all := append(append([]string(nil), groups...), "e.key")
		grouping := " GROUP BY " + strings.Join(all, ",")
		if totals {
			rest := append(append([]string(nil), groups[1:]...), "e.key")
			grouping = " GROUP BY GROUPING SETS ((" + strings.Join(all, ",") + "),(" + strings.Join(rest, ",") + "))"
		}
		projected = append(projected, "hist", "count(*) AS __hist_mult")
		compressed := "SELECT " + strings.Join(projected, ",") + " FROM " + source + " GROUP BY ALL"
		matched++
		return fmt.Sprintf(", %s_bins AS (SELECT %s,e.key AS __hist_k,sum(e.value::HUGEINT*__hist_mult)::DOUBLE AS __hist_n FROM (SELECT *,unnest(map_entries(hist)) AS e FROM (%s) __hist_compressed) __hist_entries%s), %s_cdf AS", prefix, strings.Join(newHidden, ","), compressed, grouping, prefix)
	})
	if rewriteErr != nil {
		return "", rewriteErr
	}
	if strings.Contains(query, "__hist_k") && matched == 0 {
		return "", fmt.Errorf("histogram experiment did not match generated bins")
	}
	return query, nil
}

type weightedHistogramEngine struct {
	*engine.Duck
	variant string
	mu      sync.Mutex
	query   string
	args    []any
}

func (e *weightedHistogramEngine) QueryContext(ctx context.Context, query string, args ...any) (queryrows.Rows, error) {
	query, err := histogramExperimentSQL(query, e.variant)
	if err != nil {
		return nil, err
	}
	if strings.Contains(query, "__panel_hist_bins") {
		e.mu.Lock()
		e.query, e.args = query, append([]any(nil), args...)
		e.mu.Unlock()
	}
	return e.Duck.QueryContext(ctx, query, args...)
}

func TestWeightedHistogramExperimentPreservesBinsAndQuantiles(t *testing.T) {
	d, _ := newTestEngine(t)
	input := `WITH base(bucket,service,spans,hist) AS (VALUES
 (0,'empty',2::BIGINT,map([]::INTEGER[],[]::UBIGINT[])),
 (1,'empty',3::BIGINT,map([]::INTEGER[],[]::UBIGINT[])),
 (0,'null',4::BIGINT,NULL::MAP(INTEGER,UBIGINT)),
 (1,'null',5::BIGINT,NULL::MAP(INTEGER,UBIGINT)),
 (0,'zero',6::BIGINT,map([-2147483648],[6]::UBIGINT[])),
 (1,'zero',6::BIGINT,map([-2147483648],[6]::UBIGINT[])),
 (0,'positive',5::BIGINT,map([8,16],[2,3]::UBIGINT[])),
 (0,'positive',5::BIGINT,map([8,16],[2,3]::UBIGINT[])),
 (1,'positive',5::BIGINT,map([16,8],[3,2]::UBIGINT[])),
 (1,NULL,5::BIGINT,map([8,16],[2,3]::UBIGINT[])),
 (1,'',5::BIGINT,map([8,16],[2,3]::UBIGINT[])),
 (0,'huge',1::BIGINT,map([8],[9223372036854775808]::UBIGINT[])),
 (0,'huge',1::BIGINT,map([8],[9223372036854775808]::UBIGINT[])),
 (0,'huge',1::BIGINT,map([8],[9223372036854775808]::UBIGINT[])))`
	for _, shape := range []struct {
		name   string
		groups []string
		totals bool
	}{
		{"whole_window", nil, false},
		{"service", []string{"coalesce(service,'')"}, false},
		{"bucket_totals", []string{"bucket"}, true},
		{"bucket_service_totals", []string{"bucket", "coalesce(service,'')"}, true},
	} {
		t.Run(shape.name, func(t *testing.T) {
			var query strings.Builder
			query.WriteString(input)
			selects := []string{"sum(spans)::DOUBLE AS n", "NULL::DOUBLE AS p50", "NULL::DOUBLE AS p95"}
			aliases := []string{"n", "p50", "p95"}
			for i, group := range shape.groups {
				alias := fmt.Sprintf("g%d", i)
				selects, aliases = append(selects, group+" AS "+alias), append(aliases, alias)
			}
			histogramAggregate(&query, "checked", "base", selects, aliases, shape.groups, shape.totals, map[int]Measure{1: {Q: .5}, 2: {Q: .95}})
			for _, relation := range []string{"checked_bins", "checked"} {
				var want [][]any
				for _, variant := range []string{"lateral", "projection", "weighted"} {
					sql, err := histogramExperimentSQL(query.String()+" SELECT * FROM "+relation+" ORDER BY ALL", variant)
					if err != nil {
						t.Fatal(err)
					}
					rows, err := d.DB.QueryContext(t.Context(), sql)
					if err != nil {
						t.Fatalf("%s/%s: %v", relation, variant, err)
					}
					columns, err := rows.Columns()
					if err != nil {
						rows.Close()
						t.Fatal(err)
					}
					var got [][]any
					for rows.Next() {
						values, pointers := make([]any, len(columns)), make([]any, len(columns))
						for i := range values {
							pointers[i] = &values[i]
						}
						if err := rows.Scan(pointers...); err != nil {
							rows.Close()
							t.Fatal(err)
						}
						got = append(got, values)
					}
					err = rows.Err()
					closeErr := rows.Close()
					if err != nil || closeErr != nil {
						t.Fatalf("rows=%v close=%v", err, closeErr)
					}
					if variant == "lateral" {
						want = got
					} else if !reflect.DeepEqual(want, got) {
						t.Fatalf("%s/%s: bins/results changed: want=%v got=%v", relation, variant, want, got)
					}
				}
			}
		})
	}
}

func BenchmarkWeightedHistogramComparison(b *testing.B) {
	scale := 4
	if raw := os.Getenv("PITOT_HISTOGRAM_SCALE"); raw != "" {
		var err error
		scale, err = strconv.Atoi(raw)
		if err != nil || scale < 1 || scale > 4 {
			b.Fatal("PITOT_HISTOGRAM_SCALE must be 1..4")
		}
	}
	for _, diversity := range []string{"low", "high"} {
		b.Run(diversity, func(b *testing.B) {
			b.StopTimer()
			cfg := config.Config{DataDir: filepath.Join(panelBenchScratch(b), "state"), RetentionDays: 30, DuckDBMemory: "4GB", DuckDBThreads: 4, DuckDBMaxConns: 8}
			repo, err := telemetrystore.Open(cfg.TelemetryDir())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { repo.Close() })
			end := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
			start := end.Add(-24 * time.Hour)
			for index := range 96 * scale {
				at := start.Add(time.Minute + time.Duration(index)*(24*time.Hour-4*time.Minute)/time.Duration(96*scale))
				batch := mixedReadBatch(fmt.Sprintf("histogram-%06d", index), at, 2048)
				for i := range batch.Spans {
					n := at.Add(time.Duration(i) * (24*time.Hour - 4*time.Minute) / time.Duration(96*scale*2048)).UnixNano()
					duration := batch.Spans[i].DurationMS
					if diversity == "high" {
						duration = float64((i%6 + 1) * (1 + (i*7919+index*409)%1000))
					}
					batch.Spans[i].StartUnixNanos, batch.Spans[i].EndUnixNanos = n, n+int64(duration*1e6)
					batch.Spans[i].DurationMS = duration
					batch.Logs[i].EventUnixNanos, batch.Logs[i].TimeUnixNanos = n, n
				}
				if err := repo.Commit(b.Context(), batch); err != nil {
					b.Fatal(err)
				}
			}
			d, err := engine.NewDuck(b.Context(), cfg, repo)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { d.Close() })
			horizon, _, _ := repo.PublicationBounds()
			panelBenchWarmDashboard(b, d, horizon)
			var version, memory string
			var threads int
			if err := d.DB.QueryRowContext(b.Context(), "SELECT version(),current_setting('memory_limit'),current_setting('threads')").Scan(&version, &memory, &threads); err != nil {
				b.Fatal(err)
			}
			panels := []Panel{
				{ID: "service_p95", Viz: "table", Query: &Spec{From: "spans", Measures: []string{"p95(duration_ms)"}, By: []string{"service"}}},
				{ID: "latency", Viz: "timeseries", Query: &Spec{From: "spans", Measures: []string{"p95(duration_ms)"}, Bucket: "auto"}},
				{ID: "totals", Viz: "stat", Query: &Spec{From: "spans", Measures: []string{"p95(duration_ms)"}}},
				{ID: "multiple", Viz: "timeseries", Query: &Spec{From: "spans", Measures: []string{"p50(duration_ms)", "p95(duration_ms)", "p99(duration_ms)", "avg(duration_ms)"}, Bucket: "auto"}},
				{ID: "rank", Viz: "timeseries", Query: &Spec{From: "spans", Measures: []string{"p95(duration_ms)"}, By: []string{"service"}, Bucket: "auto"}, Options: &Options{Top: 3}},
				{ID: "trends", Viz: "table", Query: &Spec{From: "spans", Measures: []string{"p95(duration_ms)"}, By: []string{"service"}, Limit: 3}, Options: &Options{Columns: []ColumnFormat{{Field: "p95", Format: "sparkline"}}}},
			}
			for _, panel := range panels {
				panel.Title = panel.ID
				spec := Dashboard{Name: "Histogram compression experiment", Time: Time{From: &start, To: &end}, Panels: []Panel{panel}}
				b.Run(panel.ID, func(b *testing.B) {
					b.Logf("diversity=%s scale=%d spans=%d logs=%d batches=%d engine=%s memory=%s threads=%d connections=8 result_cache_ttl=0 analytical=warm background=off", diversity, scale, 96*scale*2048, 96*scale*2048, len(repo.Parquet.BatchMetadata()), version, memory, threads)
					compareWeightedHistograms(b, d, spec, end)
				})
			}
		})
	}
}

func compareWeightedHistograms(b *testing.B, d *engine.Duck, spec Dashboard, end time.Time) {
	b.StopTimer()
	names := []string{"lateral", "projection", "weighted"}
	engines, executors := make([]*weightedHistogramEngine, len(names)), make([]*Executor, len(names))
	for i, name := range names {
		engines[i] = &weightedHistogramEngine{Duck: d, variant: name}
		executors[i] = NewExecutor(engines[i], 30)
		executors[i].SetCacheTTL(0)
		executors[i].now = func() time.Time { return end }
		b.Cleanup(executors[i].Close)
	}
	run := func(i int) []Result {
		results, err := executors[i].Run(WithoutCachedResults(b.Context()), RunRequest{Dashboard: spec})
		if err != nil || len(results) != len(spec.Panels) {
			b.Fatalf("variant=%s results=%d error=%v", names[i], len(results), err)
		}
		for j := range results {
			result := &results[j]
			if result.Status != StatusOK || result.Frame == nil || result.Frame.Rows == 0 || result.partial || result.Cached || result.AnnotationError != "" {
				b.Fatalf("variant=%s panel=%s status=%s error=%s partial=%t cached=%t annotation=%s", names[i], result.ID, result.Status, result.Error, result.partial, result.Cached, result.AnnotationError)
			}
			result.ElapsedMS, result.SQL = 0, ""
		}
		return results
	}
	want := run(0)
	for range 3 {
		for i := range names {
			if got := run(i); !reflect.DeepEqual(want, got) {
				a, _ := json.Marshal(want)
				z, _ := json.Marshal(got)
				b.Fatalf("variant=%s changed complete result: want=%s got=%s", names[i], a, z)
			}
		}
	}
	for i := range names {
		if engines[i].query == "" || i > 0 && engines[i].query == engines[i-1].query {
			b.Fatal("histogram experiment did not capture distinct native queries")
		}
	}
	timings := make([][]float64, len(names))
	b.ResetTimer()
	b.StartTimer()
	for sample := range b.N {
		for offset := range names {
			i := (sample + offset) % len(names)
			started := time.Now()
			run(i)
			timings[i] = append(timings[i], float64(time.Since(started))/float64(time.Millisecond))
		}
	}
	b.StopTimer()
	for i, values := range timings {
		sort.Float64s(values)
		p95 := values[(95*len(values)+99)/100-1]
		b.ReportMetric(p95, names[i]+"_p95_ms")
		b.Logf("variant=%s samples=%d p50_ms=%.3f p95_ms=%.3f full_executor=true", names[i], len(values), values[(50*len(values)+99)/100-1], p95)
	}
	if b.N >= 100 && spec.Panels[0].ID == "service_p95" {
		for i, e := range engines {
			var key, plan string
			if err := d.DB.QueryRowContext(b.Context(), "EXPLAIN (ANALYZE, FORMAT JSON) "+e.query, e.args...).Scan(&key, &plan); err != nil {
				b.Fatal(err)
			}
			var compact bytes.Buffer
			if err := json.Compact(&compact, []byte(plan)); err != nil {
				b.Fatal(err)
			}
			b.Logf("native_profile_variant=%s %s=%s", names[i], key, compact.String())
		}
	}
}
