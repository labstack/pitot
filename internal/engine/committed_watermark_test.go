package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/config"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

func TestCommittedWatermarkDoesNotAcquireReadConnection(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	commitWatermarkSpan(t, repo, "committed")
	if _, err := d.refreshServiceRollup(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := assertCommittedWatermarkParity(t, d)
	// Hold every read connection. The getter must still return the committed
	// horizon; its deadline is only a deadlock watchdog, not a latency assertion.
	for range d.DB.Stats().MaxOpenConnections {
		conn, err := d.DB.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	got, err := d.RollupWatermark(ctx)
	if err != nil || !got.Equal(want) {
		t.Fatalf("getter waited for read pool: got=%v want=%v err=%v", got, want, err)
	}
	ctx, cancelNow := context.WithCancel(t.Context())
	cancelNow()
	if _, err := d.RollupWatermark(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled getter error=%v", err)
	}
}

func TestCommittedWatermarkPublishesOnlySuccessfulRefresh(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	if got := assertCommittedWatermarkParity(t, d); !got.IsZero() {
		t.Fatalf("cold watermark=%v", got)
	}
	commitWatermarkSpan(t, repo, "first")
	if _, err := d.refreshServiceRollup(t.Context()); err != nil {
		t.Fatal(err)
	}
	before := assertCommittedWatermarkParity(t, d)
	// Writing the durable cursor inside an uncommitted transaction must not
	// publish it to readers, and rolling that transaction back must be invisible.
	tx, err := d.writer().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := storeRollupWatermark(t.Context(), tx, serviceRollupStateKey, before.Add(time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	if got, err := d.RollupWatermark(t.Context()); err != nil || !got.Equal(before) {
		t.Fatalf("uncommitted watermark became visible: %v %v", got, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	commitWatermarkSpan(t, repo, "second")
	if _, err := d.DB.ExecContext(t.Context(), `ALTER TABLE span_rollup ADD COLUMN incompatible BOOLEAN`); err != nil {
		t.Fatal(err)
	}
	if _, err := d.refreshServiceRollup(t.Context()); err == nil {
		t.Fatal("incompatible refresh unexpectedly committed")
	}
	if got := assertCommittedWatermarkParity(t, d); !got.Equal(before) {
		t.Fatalf("failed refresh changed watermark: %v -> %v", before, got)
	}
	if _, err := d.DB.ExecContext(t.Context(), `ALTER TABLE span_rollup DROP COLUMN incompatible`); err != nil {
		t.Fatal(err)
	}
	if _, err := d.refreshServiceRollup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := assertCommittedWatermarkParity(t, d); !got.After(before) {
		t.Fatalf("successful retry did not publish new watermark: %v -> %v", before, got)
	}
}

func TestCommittedWatermarkRestoresRewoundStartupState(t *testing.T) {
	cfg := config.Config{DataDir: t.TempDir(), DuckDBMemory: "256MB", DuckDBThreads: 2, DuckDBMaxConns: 2}
	repo, err := telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDuck(t.Context(), cfg, repo)
	if err != nil {
		repo.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close(); _ = repo.Close() })
	commitWatermarkSpan(t, repo, "durable")
	if _, err := d.refreshServiceRollup(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := assertCommittedWatermarkParity(t, d)
	// A failed publication can advance the persisted empty tail beyond the
	// durable repository tip. Open rewinds that tail before exposing readers.
	if _, err := d.DB.ExecContext(t.Context(), `UPDATE rollup_state SET last_ingested_unix_nano=? WHERE cache_key=?`, want.Add(time.Hour).UnixNano(), serviceRollupStateKey); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	repo, err = telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err = NewDuck(t.Context(), cfg, repo)
	if err != nil {
		t.Fatal(err)
	}
	if got := assertCommittedWatermarkParity(t, d); !got.Equal(want) {
		t.Fatalf("restart exposed unrewound horizon: got=%v want=%v", got, want)
	}
}

func TestCommittedWatermarkInitializesSkipToLatest(t *testing.T) {
	cfg := config.Config{DataDir: t.TempDir(), DuckDBMemory: "256MB", DuckDBThreads: 2, DuckDBMaxConns: 2, RollupSkipToLatest: true}
	repo, err := telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	commitWatermarkSpan(t, repo, "skip")
	horizon, _, _ := repo.PublicationBounds()
	d, err := NewDuck(t.Context(), cfg, repo)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if got := assertCommittedWatermarkParity(t, d); got.UnixNano() != horizon {
		t.Fatalf("skip horizon=%v want=%d", got, horizon)
	}
	var rows int
	if err := d.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM span_rollup`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("skip unexpectedly aggregated rows=%d: %v", rows, err)
	}
}

func TestCommittedWatermarkConcurrentReadersAndRefreshes(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	stop := make(chan struct{})
	errs := make(chan error, 4)
	var ready, readers sync.WaitGroup
	ready.Add(4)
	readers.Add(4)
	for range 4 {
		go func() {
			defer readers.Done()
			ready.Done()
			var previous time.Time
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				got, err := d.RollupWatermark(t.Context())
				if err != nil || got.Before(previous) {
					errs <- fmt.Errorf("reader watermark=%v previous=%v err=%v", got, previous, err)
					return
				}
				previous = got
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
			}
		}()
	}
	finish := sync.OnceFunc(func() { close(stop); readers.Wait() })
	defer finish()
	ready.Wait()
	for i := range 4 {
		commitWatermarkSpan(t, repo, fmt.Sprintf("concurrent-%d", i))
		if _, err := d.refreshServiceRollup(t.Context()); err != nil {
			t.Fatal(err)
		}
		assertCommittedWatermarkParity(t, d)
	}
	finish()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func commitWatermarkSpan(t *testing.T, repo *telemetrystore.Repository, id string) {
	t.Helper()
	at := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	if err := repo.Commit(t.Context(), telemetrystore.Batch{ID: id, Spans: []telemetry.Span{{Namespace: "ns", ServiceName: "svc", TraceID: id, SpanID: id, StartUnixNanos: at.UnixNano(), EndUnixNanos: at.Add(time.Millisecond).UnixNano(), DurationMS: 1}}}); err != nil {
		t.Fatal(err)
	}
}

func assertCommittedWatermarkParity(t *testing.T, d *Duck) time.Time {
	t.Helper()
	var stored int64
	if err := d.DB.QueryRowContext(t.Context(), `SELECT coalesce((SELECT last_ingested_unix_nano FROM rollup_state WHERE cache_key=?),0)`, serviceRollupStateKey).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	got, err := d.RollupWatermark(t.Context())
	want := time.Time{}
	if stored != 0 {
		want = time.Unix(0, stored).UTC()
	}
	if err != nil || !got.Equal(want) {
		t.Fatalf("published watermark=%v persisted=%v: %v", got, want, err)
	}
	return got
}
