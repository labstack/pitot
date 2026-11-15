//go:build filebench

package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/config"
	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// This local diagnostic fixes selected rows/window while adding real published
// unrelated files. It does not modify runtime caching or measure HTTP, browser
// paint, cold-engine behavior, concurrent publication, or mixed-load capacity.
// Stage timings overlap: bind includes catalog/marker preparation where needed;
// never add their percentiles to derive an end-to-end percentile.
// Run alone, with other native timing workloads stopped:
// go test -tags=filebench ./internal/engine -run '^TestSnapshotPreparationRetentionScalingBenchmark$' -count=1 -v
func TestSnapshotPreparationRetentionScalingBenchmark(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	const selectedFiles, selectedRowsPerFile, samples = 4, 32, 20
	const wantedTraces = selectedFiles * selectedRowsPerFile / 2
	cfg := config.Config{DataDir: t.TempDir(), DuckDBMemory: "1GB", DuckDBThreads: 2, DuckDBMaxConns: 2}
	repo, err := telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	base := time.Date(2026, 11, 1, 12, 0, 0, 123, time.UTC)
	window := queryrows.Window{Start: base, End: base.Add(time.Minute), Namespace: "prod", Service: "api"}
	for file := range selectedFiles {
		batch := telemetrystore.Batch{ID: fmt.Sprintf("selected_%04d", file)}
		for row := range selectedRowsPerFile {
			n := base.Add(time.Duration(file*selectedRowsPerFile+row) * time.Millisecond).UnixNano()
			batch.Spans = append(batch.Spans, telemetry.Span{Namespace: "prod", ServiceName: "api", TraceID: fmt.Sprintf("selected_trace_%04d_%04d", file, row/2), SpanID: fmt.Sprint(row), StartUnixNanos: n, EndUnixNanos: n + int64(time.Millisecond), DurationMS: 1, IngestedAt: n})
		}
		if err := repo.Commit(ctx, batch); err != nil {
			t.Fatal(err)
		}
		if err := telemetry.ValidatePublishedBatch(filepath.Join(repo.Parquet.BatchesDir(), batch.ID+telemetry.BatchSuffix)); err != nil {
			t.Fatalf("selected published fixture verification: %v", err)
		}
	}
	d, err := NewDuck(ctx, cfg, repo)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var version, memory string
	var threads int
	if err := d.DB.QueryRowContext(ctx, "SELECT version(),current_setting('memory_limit'),current_setting('threads')").Scan(&version, &memory, &threads); err != nil {
		t.Fatal(err)
	}
	t.Logf("snapshot_preparation engine=%s memory=%s threads=%d read_connections=%d samples=%d selected_files=%d selected_rows=%d selected_traces=%d; warm engine/result cache bypassed (engine API), cold/warm SQL compilation distinguished", version, memory, threads, cfg.DuckDBMaxConns, samples, selectedFiles, selectedFiles*selectedRowsPerFile, wantedTraces)
	retained := 0
	for _, unrelated := range []int{256, 1024, 4096} {
		setup := time.Now()
		for ; retained < unrelated; retained++ {
			if err := ctx.Err(); err != nil {
				t.Fatal(err)
			}
			n := base.Add(-24*time.Hour - time.Duration(retained)*time.Second).UnixNano()
			batch := telemetrystore.Batch{ID: fmt.Sprintf("unrelated_%05d", retained), Spans: []telemetry.Span{{Namespace: "other", ServiceName: "other", TraceID: fmt.Sprintf("unrelated_trace_%05d", retained), SpanID: "root", StartUnixNanos: n, EndUnixNanos: n + int64(time.Millisecond), DurationMS: 1, IngestedAt: base.UnixNano() + int64(retained)}}}
			if err := repo.Commit(ctx, batch); err != nil {
				t.Fatal(err)
			}
		}
		for {
			_, backlog, err := d.refreshReadCaches(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !backlog {
				break
			}
		}
		batches := repo.Parquet.BatchMetadata()
		if len(batches) != unrelated+selectedFiles {
			t.Fatalf("active batch count=%d expected=%d", len(batches), unrelated+selectedFiles)
		}
		var physicalBytes int64
		selected, selectedRows := 0, 0
		for _, batch := range batches {
			info, err := os.Stat(filepath.Join(repo.Parquet.BatchesDir(), batch.ID+telemetry.BatchSuffix, "spans.parquet"))
			if err != nil || !info.Mode().IsRegular() || info.Size() == 0 || !batch.SpanTime.Known {
				t.Fatalf("fixture needs real published files with event footer bounds: batch=%s err=%v", batch.ID, err)
			}
			physicalBytes += info.Size()
			if overlapping(batch, "spans", window) {
				selected++
				selectedRows += batch.Spans
			}
		}
		if selected != selectedFiles || selectedRows != selectedFiles*selectedRowsPerFile {
			t.Fatalf("selected workload changed with retention: files=%d rows=%d", selected, selectedRows)
		}
		if err := telemetry.ValidatePublishedBatch(filepath.Join(repo.Parquet.BatchesDir(), fmt.Sprintf("unrelated_%05d", unrelated-1)+telemetry.BatchSuffix)); err != nil {
			t.Fatalf("unrelated published fixture verification: %v", err)
		}
		markers, err := batchMarkers(ctx, d.DB)
		if err != nil || len(markers) != len(batches) {
			t.Fatalf("cache backfill incomplete: markers=%d active=%d err=%v", len(markers), len(batches), err)
		}
		t.Logf("fixture unrelated_files=%d active_span_files=%d span_file_bytes=%d markers=%d setup=%s", unrelated, len(batches), physicalBytes, len(markers), time.Since(setup))
		for _, mode := range []struct {
			name  string
			query string
			kind  queryrows.ReadKind
			args  []any
		}{
			{"raw", `SELECT count(DISTINCT trace_id) FROM spans WHERE start_time>=?::TIMESTAMP_NS::TIMESTAMPTZ_NS AND start_time<?::TIMESTAMP_NS::TIMESTAMPTZ_NS AND namespace='prod' AND service='api'`, queryrows.RawRead, []any{window.Start, window.End}},
			{"trace_candidates", `SELECT count(*) FROM (SELECT namespace,trace_id FROM trace_candidates UNION SELECT namespace,trace_id FROM trace_tail)`, queryrows.TraceCandidateRead, nil},
		} {
			w := window
			w.Kind = mode.kind
			timings := map[string][]float64{}
			for sample := range samples {
				if err := ctx.Err(); err != nil {
					t.Fatal(err)
				}
				measureSnapshotPreparationStages(t, ctx, d, mode.query, mode.args, w, unrelated, sample, wantedTraces, timings)
			}
			stages := make([]string, 0, len(timings))
			for stage := range timings {
				stages = append(stages, stage)
			}
			sort.Strings(stages)
			for _, stage := range stages {
				values := timings[stage]
				sort.Float64s(values)
				var sum float64
				for _, value := range values {
					sum += value
				}
				result, err := json.Marshal(map[string]any{"experiment": "snapshot_preparation_retention", "mode": mode.name, "unrelated_files": unrelated, "active_span_files": len(batches), "selected_files": selected, "selected_spans": selectedRows, "expected_trace_count": wantedTraces, "stage": stage, "samples": len(values), "mean_ms": sum / float64(len(values)), "median_ms": (values[(len(values)-1)/2] + values[len(values)/2]) / 2, "p95_ms": values[(95*len(values)+99)/100-1], "max_ms": values[len(values)-1], "stages_overlap": true, "qualification": false})
				if err != nil {
					t.Fatal(err)
				}
				t.Log("RESULT", string(result))
			}
		}
	}
}

func measureSnapshotPreparationStages(t *testing.T, ctx context.Context, d *Duck, query string, args []any, w queryrows.Window, unrelated, sample, wanted int, timings map[string][]float64) {
	t.Helper()
	measure := func(stage string, run func() error) {
		started := time.Now()
		if err := run(); err != nil {
			t.Fatalf("stage=%s unrelated=%d: %v", stage, unrelated, err)
		}
		timings[stage] = append(timings[stage], float64(time.Since(started))/float64(time.Millisecond))
	}
	// Follow the real pin/connection/read-transaction order. Marker loads and
	// aggregate bindings below observe the same MVCC snapshot, without caching
	// marker results in Go or inventing metadata for absent files.
	conn, pin, err := d.readConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var tx *sql.Tx
	defer func() {
		if conn != nil {
			endRead(conn, tx)
			d.unpinParquet(pin)
		}
	}()
	tx, err = beginRead(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	measure("catalog_copy_sort", func() error {
		if batches := d.repository.Parquet.BatchMetadata(); len(batches) != unrelated+4 {
			return fmt.Errorf("catalog changed: %d", len(batches))
		}
		return nil
	})
	measure("marker_load_diagnostic", func() error {
		markers, err := batchMarkers(ctx, tx)
		if err == nil && len(markers) != unrelated+4 {
			return fmt.Errorf("marker snapshot changed: %d", len(markers))
		}
		return err
	})
	// A unique comment forces the production SQL-text cache miss. This is cold
	// SQL compilation only; the native engine, data and filesystem remain warm.
	measure("compile_cold_sql", func() error {
		_, err := d.compileSnapshot(ctx, tx, query+fmt.Sprintf(" /* snapshot preparation %d %d */", unrelated, sample))
		return err
	})
	if _, err := d.compileSnapshot(ctx, tx, query); err != nil {
		t.Fatal(err)
	}
	measure("compile_warm_sql", func() error {
		_, err := d.compileSnapshot(ctx, tx, query)
		return err
	})
	var bound string
	measure("bind_snapshot_warm_sql", func() error {
		var err error
		bound, err = d.bindSnapshot(ctx, tx, query, w)
		return err
	})
	measure("bound_execute_scan", func() error {
		var count int
		if err := tx.QueryRowContext(ctx, bound, args...).Scan(&count); err != nil {
			return err
		}
		if count != wanted {
			return fmt.Errorf("bound count=%d expected=%d", count, wanted)
		}
		return nil
	})
	endRead(conn, tx)
	d.unpinParquet(pin)
	conn, tx = nil, nil
	measure("query_context_complete_read", func() error {
		rows, err := d.QueryContext(queryrows.WithWindow(ctx, w), query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		if !rows.Next() {
			return fmt.Errorf("full query returned no scalar row: %v", rows.Err())
		}
		var count int
		if err := rows.Scan(&count); err != nil {
			return err
		}
		if count != wanted || rows.Next() {
			return fmt.Errorf("full query count=%d expected=%d or extra row", count, wanted)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	})
}
