//go:build filebench

package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/config"
	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// Protect the broad-selection case which the narrow four-file preparation
// experiment never reaches. Both SQL variants use the same canonical captured
// files and one pinned read transaction. Each timing includes native SQL
// preparation/binding, execution and result scanning; it excludes Go snapshot
// preparation, HTTP, browser paint, ingestion and concurrent maintenance.
// This is an exploratory control, not a capacity or latency qualification.
// Run alone after reserving the native timing slot:
// go test -tags=filebench ./internal/engine -run '^TestBroadSnapshotSourceComparisonBenchmark$' -count=1 -v
func TestBroadSnapshotSourceComparisonBenchmark(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	cfg := config.Config{DataDir: t.TempDir(), DuckDBMemory: "4GB", DuckDBThreads: 4, DuckDBMaxConns: 4}
	repo, err := telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
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
	t.Logf("broad_snapshot_source engine=%s memory=%s threads=%d read_connections=%d samples=100 warmups=3 rows_per_file=32; full canonical aggregate oracle; warm engine; not capacity evidence", version, memory, threads, cfg.DuckDBMaxConns)
	base := time.Date(2026, 11, 1, 12, 0, 0, 123, time.UTC)
	w := queryrows.Window{Start: base, End: base.Add(time.Minute)}
	var oracle []telemetry.Span
	published := 0
	for _, files := range []int{64, 384, 1024} {
		setup := time.Now()
		for ; published < files; published++ {
			batch := telemetrystore.Batch{ID: fmt.Sprintf("broad_%05d", published)}
			for row := range 32 {
				namespace := "prod"
				if (row/2)%4 == 0 {
					namespace = "other"
				}
				service := []string{"api", "worker", "db"}[row%3]
				n := base.Add(time.Duration(published%1000+row) * time.Millisecond).UnixNano()
				// Every file overlaps the queried window, but has rows outside
				// both half-open boundaries. Native predicates must still apply.
				if row == 0 {
					n = w.Start.UnixNano() - 1
				}
				if row == 31 {
					n = w.End.UnixNano()
				}
				status := "STATUS_CODE_OK"
				if row%7 == 0 {
					status = "STATUS_CODE_ERROR"
				}
				span := telemetry.Span{Namespace: namespace, ServiceName: service, TraceID: fmt.Sprintf("broad_trace_%05d_%02d", published, row/2), SpanID: fmt.Sprint(row), StartUnixNanos: n, EndUnixNanos: n + int64(row+1)*int64(time.Millisecond), DurationMS: float64(row + 1), StatusCode: status, IngestedAt: base.UnixNano() + int64(published)}
				batch.Spans = append(batch.Spans, span)
				oracle = append(oracle, span)
			}
			if err := repo.Commit(ctx, batch); err != nil {
				t.Fatal(err)
			}
		}
		batches := repo.Parquet.BatchMetadata()
		if len(batches) != files {
			t.Fatalf("captured active files=%d expected=%d", len(batches), files)
		}
		for _, batch := range batches {
			if batch.Spans != 32 || !batch.SpanTime.Known || !overlapping(batch, "spans", w) {
				t.Fatalf("broad published fixture has invalid row/footer bounds: %+v", batch)
			}
		}
		if err := telemetry.ValidatePublishedBatch(filepath.Join(repo.Parquet.BatchesDir(), batches[len(batches)-1].ID+telemetry.BatchSuffix)); err != nil {
			t.Fatal("canonical fixture verification:", err)
		}
		t.Logf("broad_snapshot_fixture files=%d canonical_spans=%d selected_file_ratio=1.0 setup=%s", files, len(oracle), time.Since(setup))
		compareBroadSnapshotSources(t, ctx, d, batches, w, oracle)
	}
}

type broadSnapshotRow struct {
	Namespace, Service                                      string
	Rows, Traces, MinStart, MaxStart, DurationNanos, Errors int64
}

func broadSnapshotOracle(spans []telemetry.Span, w queryrows.Window, scoped bool) []broadSnapshotRow {
	groups := map[[2]string]*broadSnapshotRow{}
	traces := map[[2]string]map[string]bool{}
	for _, span := range spans {
		if span.StartUnixNanos < w.Start.UnixNano() || span.StartUnixNanos >= w.End.UnixNano() || scoped && (span.Namespace != "prod" || span.ServiceName != "api") {
			continue
		}
		key := [2]string{span.Namespace, span.ServiceName}
		row := groups[key]
		if row == nil {
			row = &broadSnapshotRow{Namespace: span.Namespace, Service: span.ServiceName, MinStart: span.StartUnixNanos, MaxStart: span.StartUnixNanos}
			groups[key], traces[key] = row, map[string]bool{}
		}
		row.Rows++
		row.MinStart, row.MaxStart = min(row.MinStart, span.StartUnixNanos), max(row.MaxStart, span.StartUnixNanos)
		row.DurationNanos += span.EndUnixNanos - span.StartUnixNanos
		if span.StatusCode == "STATUS_CODE_ERROR" {
			row.Errors++
		}
		traces[key][span.TraceID] = true
	}
	result := make([]broadSnapshotRow, 0, len(groups))
	for key, row := range groups {
		row.Traces = int64(len(traces[key]))
		result = append(result, *row)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Namespace != result[j].Namespace {
			return result[i].Namespace < result[j].Namespace
		}
		return result[i].Service < result[j].Service
	})
	return result
}

func readBroadSnapshotRows(ctx context.Context, tx *sql.Tx, statement string, w queryrows.Window) ([]broadSnapshotRow, error) {
	rows, err := tx.QueryContext(ctx, statement, w.Start, w.End)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []broadSnapshotRow
	for rows.Next() {
		var row broadSnapshotRow
		if err := rows.Scan(&row.Namespace, &row.Service, &row.Rows, &row.Traces, &row.MinStart, &row.MaxStart, &row.DurationNanos, &row.Errors); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, rows.Close()
}

func compareBroadSnapshotSources(t *testing.T, ctx context.Context, d *Duck, batches []telemetry.BatchMetadata, w queryrows.Window, oracle []telemetry.Span) {
	t.Helper()
	conn, pin, err := d.readConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var tx *sql.Tx
	defer func() { endRead(conn, tx); d.unpinParquet(pin) }()
	tx, err = beginRead(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	// Freeze the previous broad-source expression explicitly so this control
	// remains useful if runtime snapshotSource later changes its selection policy.
	paths := make([]string, 0, len(batches))
	for _, batch := range batches {
		paths = append(paths, filepath.ToSlash(filepath.Join(d.repository.Parquet.BatchesDir(), batch.ID+telemetry.BatchSuffix, "spans.parquet")))
	}
	signal, _ := telemetry.LookupSignal("spans")
	glob := "SELECT " + rawProjection(signal) + " FROM read_parquet(" + parquetGlob(d.repository.Parquet.Dir(), "spans") + ", union_by_name=false, hive_partitioning=false) WHERE filename IN (" + idsSQL(paths) + ")"
	list := d.physicalSource("spans", append([]telemetry.BatchMetadata(nil), batches...))
	for _, scoped := range []bool{false, true} {
		scope := "all"
		predicate := ""
		if scoped {
			scope, predicate = "prod_api", " AND namespace='prod' AND service='api'"
		}
		want := broadSnapshotOracle(oracle, w, scoped)
		var statements [2]string
		for i, source := range []string{glob, list} {
			statements[i] = `SELECT namespace,service,count(*)::BIGINT,count(DISTINCT trace_id)::BIGINT,
 min(epoch_ns(start_time)),max(epoch_ns(start_time)),sum(end_unix_nano-start_unix_nano)::BIGINT,
 sum(CASE WHEN status='STATUS_CODE_ERROR' THEN 1 ELSE 0 END)::BIGINT
 FROM (` + cleanSource("spans", source) + `) WHERE start_time>=?::TIMESTAMP_NS::TIMESTAMPTZ_NS AND start_time<?::TIMESTAMP_NS::TIMESTAMPTZ_NS` + predicate + ` GROUP BY namespace,service ORDER BY namespace,service`
		}
		names := [2]string{"captured_glob_filename_in", "captured_explicit_list"}
		run := func(variant int) (float64, float64) {
			var before, after syscall.Rusage
			if err := syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
				t.Fatal("process CPU before read:", err)
			}
			started := time.Now()
			got, err := readBroadSnapshotRows(ctx, tx, statements[variant], w)
			elapsed := float64(time.Since(started)) / float64(time.Millisecond)
			if err := syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
				t.Fatal("process CPU after read:", err)
			}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("files=%d scope=%s variant=%s full oracle mismatch: got=%+v want=%+v error=%v", len(batches), scope, names[variant], got, want, err)
			}
			cpuMilliseconds := func(r syscall.Rusage) float64 {
				return float64(r.Utime.Sec+r.Stime.Sec)*1000 + float64(r.Utime.Usec+r.Stime.Usec)/1000
			}
			return elapsed, cpuMilliseconds(after) - cpuMilliseconds(before)
		}
		for range 3 {
			run(0)
			run(1)
		}
		var timings [2][]float64
		var cpuTimings [2][]float64
		for sample := range 100 {
			// AB/BA alternation balances warmed metadata, CPU and filesystem state.
			for offset := range 2 {
				variant := (sample + offset) % 2
				elapsed, cpu := run(variant)
				timings[variant] = append(timings[variant], elapsed)
				cpuTimings[variant] = append(cpuTimings[variant], cpu)
			}
		}
		for variant, values := range timings {
			sort.Float64s(values)
			var sum float64
			for _, value := range values {
				sum += value
			}
			cpuValues := cpuTimings[variant]
			sort.Float64s(cpuValues)
			var cpuSum float64
			for _, value := range cpuValues {
				cpuSum += value
			}
			result, err := json.Marshal(map[string]any{"experiment": "broad_snapshot_source", "variant": names[variant], "files": len(batches), "canonical_spans": len(oracle), "scope": scope, "oracle_groups": len(want), "samples": len(values), "warmups": 3, "statement_bytes": len(statements[variant]), "mean_ms": sum / float64(len(values)), "median_ms": (values[49] + values[50]) / 2, "p95_ms": values[94], "p99_ms": values[98], "max_ms": values[99], "process_cpu_total_ms": cpuSum, "process_cpu_mean_ms": cpuSum / float64(len(cpuValues)), "process_cpu_median_ms": (cpuValues[49] + cpuValues[50]) / 2, "cpu_method": "getrusage_self_user_plus_system_excludes_oracle_comparison", "unit": "native_prepare_execute_scan", "go_snapshot_preparation": false, "http": false, "qualification": false})
			if err != nil {
				t.Fatal(err)
			}
			t.Log("RESULT", string(result))
			var key, plan string
			if err := tx.QueryRowContext(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+statements[variant], w.Start, w.End).Scan(&key, &plan); err != nil {
				t.Logf("NATIVE_PROFILE_UNAVAILABLE files=%d scope=%s variant=%s error=%v", len(batches), scope, names[variant], err)
				continue
			}
			var profile map[string]any
			if err := json.Unmarshal([]byte(plan), &profile); err != nil {
				t.Logf("NATIVE_PROFILE_UNAVAILABLE files=%d scope=%s variant=%s JSON_error=%v", len(batches), scope, names[variant], err)
				continue
			}
			compact := compactBroadSnapshotProfile(profile)
			encoded, err := json.Marshal(compact)
			if err != nil {
				t.Fatal(err)
			}
			label := "NATIVE_PROFILE"
			if available, _ := compact["available"].(bool); !available {
				label = "NATIVE_PROFILE_UNAVAILABLE"
			}
			t.Logf("%s files=%d scope=%s variant=%s %s", label, len(batches), scope, names[variant], encoded)
		}
	}
}

// Keep native timing/operator evidence bounded and omit query text and local
// filenames. Raw EXPLAIN trees can include every captured filename.
func compactBroadSnapshotProfile(profile map[string]any) map[string]any {
	result := map[string]any{}
	for _, key := range []string{"latency", "cpu_time", "blocked_thread_time", "rows_returned", "cumulative_rows_scanned", "total_bytes_read", "system_peak_buffer_memory", "system_peak_temp_dir_size"} {
		if value, ok := profile[key]; ok {
			result[key] = value
		}
	}
	var operators []map[string]any
	var walk func(map[string]any, int)
	walk = func(node map[string]any, depth int) {
		if _, ok := node["operator_name"]; ok {
			operator := map[string]any{"depth": depth}
			for _, key := range []string{"operator_name", "operator_type", "operator_timing", "operator_cardinality", "operator_rows_scanned", "cumulative_rows_scanned", "cpu_time"} {
				if value, ok := node[key]; ok {
					operator[key] = value
				}
			}
			if info, ok := node["extra_info"].(map[string]any); ok {
				for _, key := range []string{"Total Files Read", "Scanning Files", "Function", "Estimated Cardinality"} {
					if value, ok := info[key]; ok {
						operator[key] = value
					}
				}
			}
			operators = append(operators, operator)
		}
		if children, ok := node["children"].([]any); ok {
			for _, child := range children {
				if next, ok := child.(map[string]any); ok {
					walk(next, depth+1)
				}
			}
		}
	}
	walk(profile, 0)
	result["operators"] = operators
	result["available"] = len(operators) > 0
	if len(operators) == 0 {
		keys := make([]string, 0, len(profile))
		for key := range profile {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		result["root_keys"] = keys
		result["reason"] = "native EXPLAIN JSON operator layout was not recognized; use measured process CPU instead"
	}
	return result
}
