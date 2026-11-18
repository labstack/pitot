//go:build filebench

package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/config"
	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// Opt-in file-binder experiment. Run alone with the pinned native wrapper and
// -tags=filebench; preparation and cache backfill are outside measurement.
func TestSnapshotFileBindingBenchmark(t *testing.T) {
	cfg := config.Config{DataDir: t.TempDir(), DuckDBMemory: "2GB", DuckDBThreads: 4, DuckDBMaxConns: 4}
	repo, err := telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	at := time.Now().UTC().Truncate(time.Hour)
	for i := range 1000 {
		n := at.Add(-time.Duration(i) * time.Minute).UnixNano()
		if err := repo.Commit(t.Context(), telemetrystore.Batch{ID: fmt.Sprintf("binder-%032d", i), Logs: []telemetry.Log{{TimeUnixNanos: n, IngestedAt: n, Severity: "INFO", Body: "event"}}}); err != nil {
			t.Fatal(err)
		}
	}
	d, err := NewDuck(t.Context(), cfg, repo)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	active := repo.Parquet.BatchMetadata()
	for _, duration := range []time.Duration{24 * time.Hour, 10 * time.Minute} {
		w := queryrows.Window{Start: at.Add(-duration), End: at.Add(time.Nanosecond)}
		var selected []telemetry.BatchMetadata
		for _, b := range active {
			if overlapping(b, "logs", w) {
				selected = append(selected, b)
			}
		}
		for _, mode := range []string{"list", "snapshot_glob", "glob"} {
			source := d.physicalSource("logs", selected)
			if mode == "snapshot_glob" {
				source = d.snapshotSource("logs", selected, active)
			}
			if mode == "glob" {
				source = "SELECT * FROM read_parquet(" + sqlLiteral(repo.Parquet.Pattern("logs")) + ", union_by_name=false, hive_partitioning=false)"
			}
			stmt := "SELECT count(*) FROM (" + cleanSource("logs", source) + ") WHERE " + windowPredicate("time", w)
			var count int
			if err := d.DB.QueryRow(stmt).Scan(&count); err != nil || count != len(selected) {
				t.Fatalf("%s count=%d expected=%d: %v", mode, count, len(selected), err)
			}
			timings := make([]float64, 0, 10)
			for range 10 {
				started := time.Now()
				if err := d.DB.QueryRow(stmt).Scan(&count); err != nil {
					t.Fatal(err)
				}
				timings = append(timings, float64(time.Since(started))/float64(time.Millisecond))
			}
			sort.Float64s(timings)
			data, _ := json.Marshal(map[string]any{"mode": mode, "window_minutes": duration.Minutes(), "selected_files": len(selected), "statement_bytes": len(stmt), "median_ms": (timings[4] + timings[5]) / 2, "p95_ms": timings[9], "uses_glob": strings.Contains(source, "*.batch")})
			t.Log("RESULT", string(data))
		}
	}
}

// Synthetic retained-history experiment: append five disjoint 1,000-span
// batches without timing fixture creation. Existing exemplar rows model the
// retained history; a pass must not read them, so its cost stays flat. There is
// no stale cleanup or compaction in this interval.
func TestReadCacheAppendScalingBenchmark(t *testing.T) {
	for _, retained := range []int{200000, 800000, 2000000, 8000000} {
		t.Run(fmt.Sprint(retained), func(t *testing.T) {
			cfg := config.Config{DataDir: t.TempDir(), DuckDBMemory: "2GB", DuckDBThreads: 4, DuckDBMaxConns: 4}
			repo, err := telemetrystore.Open(cfg.TelemetryDir())
			if err != nil {
				t.Fatal(err)
			}
			defer repo.Close()
			for batch := range 5 {
				b := telemetrystore.Batch{ID: fmt.Sprint("append", batch)}
				for i := range 1000 {
					b.Spans = append(b.Spans, telemetry.Span{Namespace: "prod", ServiceName: "api", TraceID: fmt.Sprintf("new%d-%d", batch, i/2), SpanID: fmt.Sprint(i), StartUnixNanos: 1, EndUnixNanos: 2, IngestedAt: 1})
				}
				if err := repo.Commit(t.Context(), b); err != nil {
					t.Fatal(err)
				}
			}
			d, err := NewDuck(t.Context(), cfg, repo)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			rows := fmt.Sprintf(`INSERT INTO read_trace_exemplars SELECT 'retained','prod','api',(i//1000)*60000000000,md5(i::VARCHAR),(i//1000)*60000000000+i%%1000,1,0,i%%50=0,true,true FROM range(%d) t(i)`, retained)
			if _, err := d.DB.Exec(rows); err != nil {
				t.Fatal(err)
			}
			var timings []float64
			for _, b := range repo.Parquet.BatchMetadata() {
				started := time.Now()
				if _, err := d.cacheBatches(t.Context(), []telemetry.BatchMetadata{b}, nil); err != nil {
					t.Fatal(err)
				}
				timings = append(timings, float64(time.Since(started))/float64(time.Millisecond))
			}
			sort.Float64s(timings)
			data, _ := json.Marshal(map[string]any{"retained_exemplar_rows": retained, "added_spans_per_pass": 1000, "passes": 5, "median_ms": timings[2], "p95_ms": timings[4]})
			t.Log("RESULT", string(data))
		})
	}
}
