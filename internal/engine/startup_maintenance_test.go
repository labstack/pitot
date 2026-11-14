package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/engine/writegate"
	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// Initial analytical work must not prevent independent Parquet maintenance
// from reducing the catalog. All production gates and budgets remain in use.
func TestStartupMaintenanceProgressesBeforeInitialRollup(t *testing.T) {
	for _, state := range []struct {
		name    string
		warm    bool
		expired bool
	}{
		{name: "cold"},
		{name: "warm", warm: true},
		{name: "cold_with_retention", expired: true},
	} {
		t.Run(state.name, func(t *testing.T) {
			d, repo, window, horizon := startupMaintenanceFixture(t, state.expired)
			if state.warm {
				if _, err := d.rollupOnce(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			unlock, stop, done := startMaintenanceBehindAnalyticalGate(t, d)
			startupMaintenanceAwait(t, "catalog compaction while initial analytical gate is held", func(context.Context) (bool, error) {
				active := repo.Parquet.BatchMetadata()
				return len(active) == 2 && (active[0].Generation > 0 || active[1].Generation > 0), nil
			})
			// Before any initial analytical write, raw reads already see exactly
			// the retained canonical rows through the new compacted catalog.
			assertStartupRawRows(t, d, 9)
			unlock()
			startupMaintenanceAwait(t, "complete active markers and analytical watermarks", func(ctx context.Context) (bool, error) {
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
			assertStartupRawRows(t, d, 9)
			var traces int
			window.Kind = queryrows.TraceCandidateRead
			window.Namespace, window.Service = "startup", "api"
			if err := d.QueryRowScan(queryrows.WithWindow(t.Context(), window), []any{&traces}, `SELECT count(DISTINCT trace_id) FROM (SELECT trace_id FROM trace_candidates UNION ALL SELECT trace_id FROM trace_tail)`); err != nil || traces != 9 {
				t.Fatalf("candidate traces=%d want9: %v", traces, err)
			}
			assertInventoryOracle(t, d, window, 3)
			var rolled int
			if err := d.DB.QueryRowContext(t.Context(), `SELECT coalesce(sum(spans),0) FROM span_rollup`).Scan(&rolled); err != nil || rolled != 9 {
				t.Fatalf("retained rollup spans=%d want9: %v", rolled, err)
			}
			stop()
			awaitStartupStopped(t, done)
		})
	}
}

func TestStartupMaintenanceCancellationDuringInitialGateWait(t *testing.T) {
	d, repo, _, _ := startupMaintenanceFixture(t, false)
	unlock, stop, done := startMaintenanceBehindAnalyticalGate(t, d)
	startupMaintenanceAwait(t, "maintenance starts independently of initial gate wait", func(context.Context) (bool, error) {
		active := repo.Parquet.BatchMetadata()
		return len(active) == 2 && (active[0].Generation > 0 || active[1].Generation > 0), nil
	})
	// Cancellation happens while the analytical gate is held. Its existing
	// non-contextual Lock still needs the owner to release it; after release,
	// RunRollups must join both background loops before returning.
	stop()
	unlock()
	awaitStartupStopped(t, done)
}

func startupMaintenanceFixture(t *testing.T, expired bool) (*Duck, *telemetrystore.Repository, queryrows.Window, int64) {
	t.Helper()
	d, repo := newBatchCacheTest(t) // 256MB, two native threads, two read connections.
	d.cfg.RollupInterval = time.Second
	d.cfg.MaintenanceInterval = time.Hour
	d.cfg.RetentionDays = 1
	at := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour + time.Minute)
	put := func(id string, stamp time.Time) {
		t.Helper()
		n := stamp.UnixNano()
		resource := map[string]any{"generation": int64(1), "ready": true}
		metric := telemetry.Metric{Namespace: "startup", ServiceName: "api", Name: "startup.gauge", Type: "gauge", Value: 1, TimeUnixNanos: n, EventUnixNanos: n, IngestedAt: n, Resource: resource}
		metric.SeriesID = telemetry.MetricSeriesID(metric)
		batch := telemetrystore.Batch{ID: id,
			Spans: []telemetry.Span{{Namespace: "startup", ServiceName: "api", TraceID: id, SpanID: id, StartUnixNanos: n, EndUnixNanos: n + 1_000_000, DurationMS: 1, Kind: "SPAN_KIND_SERVER", StatusCode: "STATUS_CODE_OK", IngestedAt: n, Resource: resource}},
			Logs:  []telemetry.Log{{Namespace: "startup", ServiceName: "api", TimeUnixNanos: n, EventUnixNanos: n, IngestedAt: n, Severity: "INFO", Body: id, Resource: resource}}, Metrics: []telemetry.Metric{metric}}
		if err := commitRestoredFixture(repo, t.Context(), batch); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 8 {
		put(fmt.Sprintf("startup-%d", i), at.Add(time.Duration(i)*time.Nanosecond))
	}
	// A partial newest-hour group waits for the production file ceiling.
	// This newer sentinel makes the eight historical inputs eligible unchanged.
	put("newest", at.Add(time.Hour))
	if expired {
		put("expired", at.Add(-48*time.Hour))
	}
	// Restored fixtures preserve a deterministic same-hour ingestion clock.
	d.publicationBounds = restoredPublicationBounds(repo)
	return d, repo, queryrows.Window{Start: at.Add(-time.Second), End: at.Add(time.Hour + time.Second)}, at.Add(time.Hour).UnixNano()
}

func startMaintenanceBehindAnalyticalGate(t *testing.T, d *Duck) (func(), context.CancelFunc, <-chan struct{}) {
	t.Helper()
	unlock := sync.OnceFunc(d.writeGate.Lock(writegate.RollupService))
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.RunRollups(ctx) }()
	t.Cleanup(func() {
		stop()
		unlock()
		awaitStartupStopped(t, done)
	})
	return unlock, stop, done
}

func startupMaintenanceAwait(t *testing.T, label string, ready func(context.Context) (bool, error)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
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

func awaitStartupStopped(t *testing.T, done <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		t.Error("RunRollups did not join its background loops after cancellation")
	}
}

func assertStartupRawRows(t *testing.T, d *Duck, want int) {
	t.Helper()
	var spans, logs, metrics int
	if err := d.QueryRowScan(t.Context(), []any{&spans, &logs, &metrics}, `SELECT (SELECT count(*) FROM spans),(SELECT count(*) FROM logs),(SELECT count(*) FROM metrics)`); err != nil || spans != want || logs != want || metrics != want {
		t.Fatalf("raw rows=%d/%d/%d want%d each: %v", spans, logs, metrics, want, err)
	}
}
