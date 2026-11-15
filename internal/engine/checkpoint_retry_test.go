package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/labstack/pitot/internal/engine/writegate"
	"github.com/labstack/pitot/internal/metrics"
	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func checkpointOldReader(t *testing.T, d *Duck, repo *telemetrystore.Repository) queryrows.Rows {
	t.Helper()
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	if err := repo.Commit(t.Context(), telemetrystore.Batch{ID: "checkpoint-reader", Logs: []telemetry.Log{{TimeUnixNanos: at.UnixNano(), EventUnixNanos: at.UnixNano(), IngestedAt: at.UnixNano(), Body: "checkpoint reader"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.writer().ExecContext(t.Context(), "CREATE TABLE checkpoint_progress(value INTEGER); INSERT INTO checkpoint_progress VALUES (0)"); err != nil {
		t.Fatal(err)
	}
	w := queryrows.Window{Start: at, End: at.Add(time.Minute)}
	rows, err := d.QueryContext(queryrows.WithWindow(t.Context(), w), "SELECT count(*) FROM logs WHERE time>=?::TIMESTAMP_NS::TIMESTAMPTZ_NS AND time<?::TIMESTAMP_NS::TIMESTAMPTZ_NS", w.Start, w.End)
	if err != nil {
		t.Fatal(err)
	}
	// New committed WAL work after the reader's snapshot makes the native
	// checkpoint refuse while that reader remains open. An idle WAL would let
	// CHECKPOINT succeed without doing work, hiding the mixed-load failure.
	if _, err := d.writer().ExecContext(t.Context(), "UPDATE checkpoint_progress SET value=value+1"); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	return rows
}

// Wait for the real maintenance goroutine to park inside checkpoint's retry,
// not a guessed delay before releasing the old reader. The old implementation
// returns the native refusal first, so this test is red on the observed bug.
func awaitCheckpointRetry(t *testing.T, ctx context.Context, result <-chan error) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	buffer := make([]byte, 1<<20)
	for {
		for stack := range strings.SplitSeq(string(buffer[:runtime.Stack(buffer, true)]), "\n\n") {
			if strings.Contains(stack, "[select]") && strings.Contains(stack, "engine.(*Duck).checkpoint") && strings.Contains(stack, "engine.(*Duck).runRepositoryMaintenance") {
				return
			}
		}
		select {
		case err := <-result:
			t.Fatalf("maintenance returned before old reader drained instead of retrying: %v", err)
		case <-ctx.Done():
			t.Fatalf("maintenance never reached checkpoint retry: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestMaintenanceCheckpointWaitsForOldReaderToDrain(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	rows := checkpointOldReader(t, d, repo)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	result, finished := make(chan error, 1), make(chan struct{})
	go func() { defer close(finished); result <- d.runRepositoryMaintenance(ctx) }()
	defer func() { rows.Close(); cancel(); <-finished }()
	awaitCheckpointRetry(t, ctx, result)
	if release := d.writeGate.TryLock(writegate.RollupService); release != nil {
		release()
		t.Fatal("checkpoint retry released the analytical write gate")
	}
	if release := d.cacheGate.TryLock(writegate.ReadCache); release != nil {
		release()
		t.Fatal("checkpoint retry released the completed-batch write gate")
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatalf("checkpoint failed after the old reader drained: %v", err)
	}
	lastSuccess, _, failures, err := d.MaintenanceHealth()
	if lastSuccess.IsZero() || failures != 0 || err != nil {
		t.Fatalf("successful retry reported maintenance failure: success=%s failures=%d err=%v", lastSuccess, failures, err)
	}
}

func TestMaintenanceCheckpointRetryHonorsCallerCancellation(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	rows := checkpointOldReader(t, d, repo)
	ctx, cancel := context.WithCancel(t.Context())
	result, finished := make(chan error, 1), make(chan struct{})
	go func() { defer close(finished); result <- d.runRepositoryMaintenance(ctx) }()
	defer func() { rows.Close(); cancel(); <-finished }()
	watch, stopWatch := context.WithTimeout(t.Context(), 2*time.Second)
	defer stopWatch()
	awaitCheckpointRetry(t, watch, result)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("caller cancellation was hidden: %v", err)
		}
	case <-watch.Done():
		t.Fatal("checkpoint retry ignored caller cancellation")
	}
}

func TestMaintenanceCheckpointRetryExhaustionIsExplicitAndBounded(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	rows := checkpointOldReader(t, d, repo)
	defer rows.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	before := testutil.ToFloat64(metrics.DuckDBCheckpointFailures)
	started := time.Now()
	err := d.runRepositoryMaintenance(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "checkpoint retry budget exhausted") || !strings.Contains(err.Error(), "other write transactions active") {
		t.Fatalf("exhausted checkpoint was hidden or mislabeled: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("250ms retry budget was not bounded: %s", elapsed)
	}
	if after := testutil.ToFloat64(metrics.DuckDBCheckpointFailures); after != before+1 {
		t.Fatalf("attempts counted as logical checkpoint failures: before=%v after=%v", before, after)
	}
	_, _, failures, healthErr := d.MaintenanceHealth()
	if failures != 1 || !errors.Is(healthErr, context.DeadlineExceeded) {
		t.Fatalf("checkpoint exhaustion not reflected in maintenance health: failures=%d error=%v", failures, healthErr)
	}
}

func TestCheckpointRetryMatchesOnlyPinnedNativeDiagnostic(t *testing.T) {
	busy := &duckdb.Error{Type: duckdb.ErrorTypeTransaction, Msg: checkpointBusyMessage}
	for _, check := range []struct {
		name string
		err  error
		want bool
	}{
		{"native", busy, true},
		{"wrapped native", fmt.Errorf("wrapped: %w", busy), true},
		{"untyped same text", errors.New(checkpointBusyMessage), false},
		{"different transaction failure", &duckdb.Error{Type: duckdb.ErrorTypeTransaction, Msg: "TransactionContext Error: unrelated failure"}, false},
		{"different native type", &duckdb.Error{Type: duckdb.ErrorTypeOutOfMemory, Msg: checkpointBusyMessage}, false},
		{"nil", nil, false},
	} {
		t.Run(check.name, func(t *testing.T) {
			if got := checkpointRetryable(check.err); got != check.want {
				t.Fatalf("retryable=%t want=%t for %v", got, check.want, check.err)
			}
		})
	}
}

func TestCheckpointRetriesConflictWithoutCountingTransientFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	d := &Duck{DB: db, writeDB: db}
	for range 2 {
		mock.ExpectExec("^CHECKPOINT$").WillReturnError(&duckdb.Error{Type: duckdb.ErrorTypeTransaction, Msg: checkpointBusyMessage})
	}
	mock.ExpectExec("^CHECKPOINT$").WillReturnResult(sqlmock.NewResult(0, 0))
	before := testutil.ToFloat64(metrics.DuckDBCheckpointFailures)
	if err := d.checkpoint(t.Context()); err != nil {
		t.Fatal(err)
	}
	if after := testutil.ToFloat64(metrics.DuckDBCheckpointFailures); after != before {
		t.Fatalf("successful logical checkpoint counted retry attempts as errors: before=%v after=%v", before, after)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointDoesNotRetryUnrelatedError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	d := &Duck{DB: db, writeDB: db}
	injected := errors.New("injected checkpoint I/O failure")
	mock.ExpectExec("^CHECKPOINT$").WillReturnError(injected)
	if err := d.checkpoint(t.Context()); !errors.Is(err, injected) {
		t.Fatalf("unrelated failure was retried or hidden: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointRetainsCallerExecutionBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	wantDeadline, _ := ctx.Deadline()
	db := sql.OpenDB(checkpointExecutionConnector{execute: func(actual context.Context) (driver.Result, error) {
		deadline, ok := actual.Deadline()
		if !ok || deadline != wantDeadline {
			return nil, fmt.Errorf("native checkpoint execution deadline=%s; want unchanged caller deadline=%s", deadline, wantDeadline)
		}
		return driver.RowsAffected(0), nil
	}})
	defer db.Close()
	d := &Duck{DB: db, writeDB: db}
	// Inspect the context at the driver seam, without sleeping for a slow I/O
	// operation: the 250ms retry budget must never cap native execution.
	if err := d.checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointNativeExecutionHonorsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	entered := make(chan struct{})
	db := sql.OpenDB(checkpointExecutionConnector{execute: func(actual context.Context) (driver.Result, error) {
		close(entered)
		<-actual.Done()
		return nil, actual.Err()
	}})
	defer func() { cancel(); db.Close() }()
	d := &Duck{DB: db, writeDB: db}
	result := make(chan error, 1)
	go func() { result <- d.checkpoint(ctx) }()
	select {
	case <-entered:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("native execution was not reached")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("native caller cancellation hidden: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("native execution ignored caller cancellation")
	}
}

type checkpointExecutionConnector struct {
	execute func(context.Context) (driver.Result, error)
}

func (c checkpointExecutionConnector) Connect(context.Context) (driver.Conn, error) {
	return checkpointExecutionConn(c), nil
}
func (checkpointExecutionConnector) Driver() driver.Driver { return checkpointExecutionDriver{} }

type checkpointExecutionDriver struct{}

func (checkpointExecutionDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("checkpoint test requires its connector")
}

type checkpointExecutionConn struct {
	execute func(context.Context) (driver.Result, error)
}

func (checkpointExecutionConn) Close() error { return nil }
func (checkpointExecutionConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("checkpoint test does not prepare statements")
}
func (checkpointExecutionConn) Begin() (driver.Tx, error) {
	return nil, errors.New("checkpoint test does not begin transactions")
}
func (c checkpointExecutionConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if query != "CHECKPOINT" || len(args) != 0 {
		return nil, fmt.Errorf("unexpected checkpoint execution: %s", query)
	}
	return c.execute(ctx)
}
