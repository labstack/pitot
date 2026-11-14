//go:build filebench

package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/config"
	"github.com/labstack/pitot/internal/engine/writegate"
	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// This opt-in guard exercises cold startup correctness with one compacted file
// larger than the complete-batch read-cache budget. It is not capacity evidence.
func TestColdStartupCompactsOversizedCacheOutput(t *testing.T) {
	began := time.Now()
	defer func() {
		var usage syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
			t.Logf("cold_startup elapsed=%s process_peak_error=%v", time.Since(began), err)
			return
		}
		divisor := float64(1 << 20)
		if runtime.GOOS == "linux" {
			divisor = 1024
		}
		t.Logf("cold_startup elapsed=%s process_peak_mib=%.3f peak_scope=whole_test_process_including_fixture platform=%s/%s capacity_evidence=false", time.Since(began), float64(usage.Maxrss)/divisor, runtime.GOOS, runtime.GOARCH)
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cfg := config.Config{DataDir: filepath.Join(t.TempDir(), "cold-state"), DuckDBMemory: "4GB", DuckDBThreads: 4, DuckDBMaxConns: 4, RollupInterval: time.Second, MaintenanceInterval: time.Hour, RetentionDays: 30}
	repo, err := telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	// Two otherwise-equal resources retain distinct integer/string identities.
	resources := []map[string]any{
		{"generation": int64(1), "ready": true, "nested": map[string]any{"labels": []any{"a", int64(2)}}, "literal.dot": []byte{0, 255}},
		{"generation": "1", "ready": true, "nested": map[string]any{"labels": []any{"a", int64(2)}}, "literal.dot": []byte{0, 255}},
	}
	at := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour + time.Minute)
	const inputs = 8
	const rowsPerInput = 32768
	const historical = inputs * rowsPerInput
	const want = historical + 1
	if historical <= readCacheRowBudget {
		t.Fatalf("fixture no longer exceeds read-cache budget: rows=%d budget=%d", historical, readCacheRowBudget)
	}
	inputIDs := make([]string, 0, inputs)
	for batchIndex := range inputs {
		id := fmt.Sprintf("cold-input-%02d", batchIndex)
		inputIDs = append(inputIDs, id)
		batch := telemetrystore.Batch{ID: id, Spans: make([]telemetry.Span, rowsPerInput)}
		for i := range batch.Spans {
			n := at.Add(time.Duration(batchIndex)*time.Second + time.Duration(i)*time.Nanosecond).UnixNano()
			identity := fmt.Sprintf("cold-%02d-%05d", batchIndex, i)
			batch.Spans[i] = telemetry.Span{Namespace: "cold_startup", ServiceName: "api", TraceID: identity, SpanID: identity, StartUnixNanos: n, EndUnixNanos: n + 1_000_000, DurationMS: 1, Kind: "SPAN_KIND_SERVER", StatusCode: "STATUS_CODE_OK", IngestedAt: at.UnixNano(), Resource: resources[i%2]}
		}
		if err := commitRestoredFixture(repo, ctx, batch); err != nil {
			repo.Close()
			t.Fatal(err)
		}
	}
	sentinel := at.Add(time.Hour).UnixNano()
	if err := commitRestoredFixture(repo, ctx, telemetrystore.Batch{ID: "cold-newest", Spans: []telemetry.Span{{Namespace: "cold_startup", ServiceName: "api", TraceID: "cold-newest", SpanID: "sentinel", StartUnixNanos: sentinel, EndUnixNanos: sentinel + 1_000_000, DurationMS: 1, Kind: "SPAN_KIND_SERVER", StatusCode: "STATUS_CODE_OK", IngestedAt: sentinel, Resource: resources[0]}}}); err != nil {
		repo.Close()
		t.Fatal(err)
	}
	// Reopen the canonical published fixture as a real restored repository. No
	// query cache, synthetic DB rows or warmed complete-batch markers are supplied.
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	repo, err = telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDuck(ctx, cfg, repo)
	if err != nil {
		repo.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
		if err := repo.Close(); err != nil {
			t.Error(err)
		}
	})
	var marked int
	if err := d.DB.QueryRowContext(ctx, "SELECT count(*) FROM read_batches").Scan(&marked); err != nil || marked != 0 {
		t.Fatalf("fixture is not cold: markers=%d error=%v", marked, err)
	}
	horizon, _, _ := repo.PublicationBounds()
	window := queryrows.Window{Start: at.Add(-time.Second), End: at.Add(time.Hour + time.Second)}
	unlock := sync.OnceFunc(d.writeGate.Lock(writegate.RollupService))
	work, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	startupBegan := time.Now()
	go func() { defer close(done); d.RunRollups(work) }()
	join := func() {
		t.Helper()
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			t.Error("cold startup RunRollups failed to join after cancellation")
		}
	}
	t.Cleanup(func() { stop(); unlock(); join() })
	var output telemetry.BatchMetadata
	coldStartupAwait(t, ctx, "oversized compacted output before analytical gate release", func(context.Context) (bool, error) {
		active := repo.Parquet.BatchMetadata()
		for _, batch := range active {
			if batch.Generation > 0 && batch.Spans > readCacheRowBudget {
				output = batch
				return len(active) == 2, nil
			}
		}
		return false, nil
	})
	if output.Spans != historical {
		t.Fatalf("compacted output spans=%d want=%d", output.Spans, historical)
	}
	// Raw immutable reads must remain exact before any analytical write succeeds.
	coldStartupAssertRaw(t, ctx, d, want)
	t.Logf("cold_startup early_compaction=%s output_rows=%d cache_row_budget=%d analytical_gate_held=true", time.Since(startupBegan), output.Spans, readCacheRowBudget)
	unlock()
	coldStartupAwait(t, ctx, "active cache markers and service/edge watermarks", func(ctx context.Context) (bool, error) {
		markers, err := batchMarkers(ctx, d.DB)
		if err != nil {
			return false, err
		}
		active := repo.Parquet.BatchMetadata()
		if len(markers) != len(active) {
			return false, nil
		}
		for _, batch := range active {
			if !markers[batch.ID] {
				return false, nil
			}
		}
		var complete int
		err = d.DB.QueryRowContext(ctx, `SELECT count(*) FROM rollup_state WHERE cache_key IN (?,?) AND last_ingested_unix_nano>=?`, serviceRollupStateKey, edgeRollupStateKey, horizon).Scan(&complete)
		return complete == 2, err
	})
	coldStartupAssertRaw(t, ctx, d, want)
	traceWindow := window
	traceWindow.Kind = queryrows.TraceCandidateRead
	traceWindow.Namespace = "cold_startup"
	traceWindow.Service = "api"
	var candidates, tail int
	if err := d.QueryRowScan(queryrows.WithWindow(ctx, traceWindow), []any{&candidates, &tail}, `SELECT (SELECT count(DISTINCT trace_id) FROM (SELECT trace_id FROM trace_candidates UNION ALL SELECT trace_id FROM trace_tail)),(SELECT count(*) FROM trace_tail)`); err != nil || candidates != want || tail != 0 {
		t.Fatalf("candidate traces=%d want=%d uncached_tail=%d error=%v", candidates, want, tail, err)
	}
	assertInventoryOracle(t, d, window, 2)
	var integerResources, stringResources, readyResources int
	if err := d.QueryRowScan(inventoryContext(ctx, window), []any{&integerResources, &stringResources, &readyResources}, `SELECT count(*) FILTER (WHERE variant_type(resource,'generation')='INT64'),count(*) FILTER (WHERE variant_type(resource,'generation')='VARCHAR'),count(*) FILTER (WHERE resource['ready']::BOOLEAN) FROM resource_inventory`); err != nil || integerResources != 1 || stringResources != 1 || readyResources != 2 {
		t.Fatalf("typed resources integer=%d string=%d ready=%d error=%v", integerResources, stringResources, readyResources, err)
	}
	for _, table := range []string{"read_batches", "read_trace_parts", "read_resource_inventory", "read_resource_seconds", "read_metric_names"} {
		var retired int
		if err := d.DB.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE batch_id IN ("+idsSQL(inputIDs)+")").Scan(&retired); err != nil || retired != 0 {
			t.Fatalf("retired contributions table=%s rows=%d error=%v", table, retired, err)
		}
	}
	var candidateRows, rolledSpans int
	if err := d.DB.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM read_trace_candidates),(SELECT coalesce(sum(spans),0) FROM span_rollup)`).Scan(&candidateRows, &rolledSpans); err != nil || candidateRows != want || rolledSpans != want {
		t.Fatalf("cache candidates=%d rollup spans=%d want=%d error=%v", candidateRows, rolledSpans, want, err)
	}
	stop()
	join()
	t.Logf("cold_startup converged=%s active_batches=%d candidate_rows=%d raw_rows=%d cache_markers=complete watermarks=complete retired_contributions=0", time.Since(startupBegan), len(repo.Parquet.BatchMetadata()), candidateRows, want)
}

func coldStartupAwait(t *testing.T, ctx context.Context, label string, ready func(context.Context) (bool, error)) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := ready(ctx)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no progress: %s: %v", label, ctx.Err())
		case <-ticker.C:
		}
	}
}
func coldStartupAssertRaw(t *testing.T, ctx context.Context, d *Duck, want int) {
	t.Helper()
	var spans, traces int
	if err := d.QueryRowScan(ctx, []any{&spans, &traces}, `SELECT count(*),count(DISTINCT trace_id) FROM spans`); err != nil || spans != want || traces != want {
		t.Fatalf("raw spans=%d traces=%d want=%d error=%v", spans, traces, want, err)
	}
}
