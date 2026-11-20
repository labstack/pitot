package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/labstack/pitot/internal/telemetry"
)

type testParquetCompactor struct {
	publishErr error
	afterSwap  func() error
}

type testParquetPublisherFunc func(context.Context, func(context.Context) error) error

func (f testParquetPublisherFunc) PublishParquet(ctx context.Context, publish func(context.Context) error) error {
	return f(ctx, publish)
}

func (c *testParquetCompactor) PublishParquet(ctx context.Context, publish func(context.Context) error) error {
	if c.publishErr != nil {
		return c.publishErr
	}
	if err := publish(ctx); err != nil {
		return err
	}
	if c.afterSwap != nil {
		return c.afterSwap()
	}
	return nil
}

func testBatch() Batch {
	return Batch{
		ID: "batch-test",
		Spans: []telemetry.Span{{
			TraceID: "trace-1", SpanID: "span-1", ServiceName: "api", Name: "GET /",
			StartUnixNanos: 100, EndUnixNanos: 200, DurationMS: 0.0001, StatusCode: "OK", IngestedAt: 300,
		}},
		Logs:    []telemetry.Log{{TimeUnixNanos: 110, Severity: "INFO", Body: "ready", ServiceName: "api", TraceID: "trace-1", IngestedAt: 300}},
		Metrics: []telemetry.Metric{{TimeUnixNanos: 120, Name: "requests", Type: "counter", Temporality: "cumulative", ServiceName: "api", Value: 1, IngestedAt: 300}},
	}
}

func TestRepositoryCommitIsIdempotentDurableAndQueryable(t *testing.T) {
	dir := t.TempDir()
	repository, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch()
	if err := repository.Commit(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := repository.Commit(context.Background(), batch); err != nil {
		t.Fatalf("idempotent commit: %v", err)
	}
	if got := repository.RowCount(); got != 3 {
		t.Fatalf("rows = %d, want 3", got)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.RowCount(); got != 3 {
		t.Fatalf("reopened rows = %d, want 3", got)
	}
	spans, err := traceAll(reopened, "trace-1")
	if err != nil || len(spans) != 1 || spans[0].Namespace != "default" {
		t.Fatalf("reopened trace = %#v, %v", spans, err)
	}

	db := openTestDuckDB(t)
	defer db.Close()
	for _, signal := range []string{"spans", "logs", "metrics"} {
		var count int
		if err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM read_parquet(?)", reopened.Parquet.Pattern(signal)).Scan(&count); err != nil {
			t.Fatalf("query %s: %v", signal, err)
		}
		if count != 1 {
			t.Fatalf("%s rows = %d, want 1", signal, count)
		}
	}
}

func TestRepositoryCommitsOversizedRequestAsOneBatch(t *testing.T) {
	repository, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	spans := make([]telemetry.Span, maxGroupBatchRows+1)
	for i := range spans {
		spans[i] = telemetry.Span{
			TraceID: "trace", SpanID: fmt.Sprintf("span-%d", i),
			StartUnixNanos: int64(i + 1), EndUnixNanos: int64(i + 2), IngestedAt: 1,
		}
	}
	if err := repository.Commit(context.Background(), Batch{ID: "oversized", Spans: spans}); err != nil {
		t.Fatal(err)
	}
	metadata := repository.Parquet.BatchMetadata()
	if len(metadata) != 1 || metadata[0].Spans != len(spans) {
		t.Fatalf("oversized batch metadata = %#v", metadata)
	}
}

func TestRepositoryHasOneAuthoritativeStorageLayout(t *testing.T) {
	dir := t.TempDir()
	repository, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if err := repository.Commit(context.Background(), testBatch()); err != nil {
		t.Fatal(err)
	}
	for _, removed := range []string{"wal", "hot", "MANIFEST.json", "MANIFEST.log", "ducklake.sqlite"} {
		if _, err := os.Stat(filepath.Join(dir, removed)); !os.IsNotExist(err) {
			t.Fatalf("removed storage artifact %s exists: %v", removed, err)
		}
	}
	entries, err := os.ReadDir(repository.Parquet.BatchPath("batch-test"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 6 {
		t.Fatalf("batch contains %d files, want Parquet signals, trace and log indexes, and metadata", len(entries))
	}
}

func TestRepositoryBootDiscardsCompactionStaging(t *testing.T) {
	dir := t.TempDir()
	staging := filepath.Join(dir, "compaction", "orphan")
	if err := os.MkdirAll(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	repository, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	// Discarding the staging directory is the whole boot-time recovery for an
	// interrupted compaction: a merge that never renamed its output into the
	// batch namespace left no authoritative rows behind.
	if _, err := os.Stat(filepath.Join(dir, "compaction")); !os.IsNotExist(err) {
		t.Fatalf("compaction staging remains after boot: %v", err)
	}
}

func TestRepositoryPrunesOnlyExpiredBatches(t *testing.T) {
	repository, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	old := testBatch()
	old.ID = "old"
	old.Spans[0].StartUnixNanos, old.Spans[0].IngestedAt = 100, 100
	old.Logs[0].TimeUnixNanos, old.Logs[0].IngestedAt = 100, 100
	old.Metrics[0].TimeUnixNanos, old.Metrics[0].IngestedAt = 100, 100
	newer := testBatch()
	newer.ID = "new"
	newer.Spans[0].StartUnixNanos, newer.Spans[0].IngestedAt = 1_000, 1_000
	newer.Logs[0].TimeUnixNanos, newer.Logs[0].IngestedAt = 1_000, 1_000
	newer.Metrics[0].TimeUnixNanos, newer.Metrics[0].IngestedAt = 1_000, 1_000
	if err := commitRestoredFixture(repository, context.Background(), old); err != nil {
		t.Fatal(err)
	}
	if err := commitRestoredFixture(repository, context.Background(), newer); err != nil {
		t.Fatal(err)
	}
	publisher := &testParquetCompactor{afterSwap: func() error {
		if repository.compactionMu.TryLock() {
			repository.compactionMu.Unlock()
			return errors.New("retention published without holding compaction lock")
		}
		// Readers that captured the old catalog may still open the expired
		// batch, so publication only removes it from the catalog.
		if _, err := os.Stat(repository.Parquet.BatchPath("old")); err != nil {
			return fmt.Errorf("expired batch deleted during publication: %w", err)
		}
		if len(repository.Parquet.BatchMetadata()) != 1 {
			return errors.New("expired batch remains in the catalog after publication")
		}
		return nil
	}}
	removed, err := repository.PruneParquet(context.Background(), publisher, 500, 64)
	if err != nil || removed != 1 {
		t.Fatalf("prune = %d, %v", removed, err)
	}
	if n, err := repository.CollectRetiredParquet(); err != nil || n != 1 {
		t.Fatalf("collected %d retired batches: %v", n, err)
	}
	if _, err := os.Stat(repository.Parquet.BatchPath("old")); !os.IsNotExist(err) {
		t.Fatalf("expired batch remains: %v", err)
	}
	if _, err := os.Stat(repository.Parquet.BatchPath("new")); err != nil {
		t.Fatalf("current batch missing: %v", err)
	}
	if got := repository.RowCount(); got != 3 {
		t.Fatalf("retained rows = %d, want 3", got)
	}
}

func TestRepositoryPrunePassDrainsWithinBudgetAndOldestFirst(t *testing.T) {
	repository, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	for i := range 5 {
		batch := testBatch()
		batch.ID = fmt.Sprintf("expired-%d", i)
		batch.Spans[0].IngestedAt = int64(i + 1)
		batch.Logs[0].IngestedAt = int64(i + 1)
		batch.Metrics[0].IngestedAt = int64(i + 1)
		if err := commitRestoredFixture(repository, context.Background(), batch); err != nil {
			t.Fatal(err)
		}
	}
	publications := 0
	publisher := &testParquetCompactor{afterSwap: func() error {
		publications++
		return nil
	}}
	removed, err := repository.PruneParquetPass(context.Background(), publisher, 10, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 5 || publications != 3 {
		t.Fatalf("removed=%d publications=%d, want 5 across 3 bounded swaps", removed, publications)
	}
	metadata := repository.Parquet.BatchMetadata()
	if len(metadata) != 0 {
		t.Fatalf("remaining batches = %#v, want the backlog drained", metadata)
	}
}

func TestRepositoryPrunePassFinishesPublicationAfterBudget(t *testing.T) {
	repository, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	for i := range 4 {
		batch := testBatch()
		batch.ID = fmt.Sprintf("expired-%d", i)
		batch.Spans[0].IngestedAt = int64(i + 1)
		batch.Logs[0].IngestedAt = int64(i + 1)
		batch.Metrics[0].IngestedAt = int64(i + 1)
		if err := commitRestoredFixture(repository, context.Background(), batch); err != nil {
			t.Fatal(err)
		}
	}
	publications := 0
	publisher := &testParquetCompactor{afterSwap: func() error {
		publications++
		time.Sleep(10 * time.Millisecond)
		return nil
	}}
	removed, err := repository.PruneParquetPass(context.Background(), publisher, 10, 2, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 || publications != 1 {
		t.Fatalf("removed=%d publications=%d, want the in-flight 2-batch publication completed once", removed, publications)
	}
}

func TestRepositoryCleanupOwnsRetiredDirectories(t *testing.T) {
	repository, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	retired := filepath.Join(repository.Parquet.BatchesDir(), "old.retired")
	if err := os.Mkdir(retired, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := repository.Commit(context.Background(), Batch{ID: "contains.retired", Spans: []telemetry.Span{{TraceID: "trace"}}}); err != nil {
		t.Fatal(err)
	}
	if err := repository.CleanupParquet(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(retired); !os.IsNotExist(err) {
		t.Fatalf("retired directory remains: %v", err)
	}
	if _, err := os.Stat(repository.Parquet.BatchPath("contains.retired")); err != nil {
		t.Fatalf("published batch with retired in its ID was removed: %v", err)
	}
}

func TestRepositoryRetentionUsesIngestTimeNotEventTime(t *testing.T) {
	repository, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	batch := testBatch()
	batch.ID = "future-events"
	batch.Spans[0].StartUnixNanos = 1 << 62
	batch.Logs[0].TimeUnixNanos = 1 << 62
	batch.Metrics[0].TimeUnixNanos = 1 << 62
	batch.Spans[0].IngestedAt = 100
	batch.Logs[0].IngestedAt = 100
	batch.Metrics[0].IngestedAt = 100
	if err := commitRestoredFixture(repository, context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	removed, err := repository.PruneParquet(context.Background(), &testParquetCompactor{}, 500, 64)
	if err != nil || removed != 1 {
		t.Fatalf("prune future-dated events = %d, %v; want one batch expired by ingest time", removed, err)
	}
}

func TestRepositoryCompactsParquetWithoutChangingRows(t *testing.T) {
	dir := t.TempDir()
	repository, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	for i := range minCompactionInputs {
		batch := testBatch()
		batch.ID = fmt.Sprintf("batch-%d", i)
		batch.Spans[0].SpanID = fmt.Sprintf("span-%d", i)
		batch.Spans[0].StartUnixNanos = int64(100 + i)
		if err := repository.Commit(context.Background(), batch); err != nil {
			t.Fatal(err)
		}
	}
	db := openTestDuckDB(t)
	defer db.Close()
	compactor := &testParquetCompactor{afterSwap: func() error {
		// Inputs stay in place for readers of the old catalog; the catalog
		// holds only the output.
		for i := range minCompactionInputs {
			if _, err := os.Stat(repository.Parquet.BatchPath(fmt.Sprintf("batch-%d", i))); err != nil {
				return fmt.Errorf("compaction input deleted during publication: %w", err)
			}
		}
		if n := len(repository.Parquet.BatchMetadata()); n != 1 {
			return fmt.Errorf("catalog holds %d batches after publication, want the output only", n)
		}
		return nil
	}}
	compactCtx, cancelCompact := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCompact()
	compacted, err := repository.CompactParquet(compactCtx, compactor, minCompactionInputs)
	if err != nil {
		t.Fatal(err)
	}
	if compacted != minCompactionInputs {
		t.Fatalf("compacted inputs = %d", compacted)
	}
	if n, err := repository.CollectRetiredParquet(); err != nil || n != minCompactionInputs {
		t.Fatalf("collected %d retired inputs: %v", n, err)
	}
	metadata := repository.Parquet.BatchMetadata()
	if len(metadata) != 1 || metadata[0].Generation != 1 || metadata[0].Spans != minCompactionInputs || metadata[0].Logs != minCompactionInputs || metadata[0].Metrics != minCompactionInputs {
		t.Fatalf("compacted metadata = %#v", metadata)
	}
	if got := repository.RowCount(); got != 3*minCompactionInputs {
		t.Fatalf("compacted rows = %d", got)
	}
	spans, err := traceAll(repository, "trace-1")
	if err != nil || len(spans) != minCompactionInputs {
		t.Fatalf("compacted trace spans = %d, %v", len(spans), err)
	}
	for _, signal := range []string{"spans", "logs", "metrics"} {
		var count int
		if err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM read_parquet(?)", repository.Parquet.Pattern(signal)).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != minCompactionInputs {
			t.Fatalf("compacted %s rows = %d", signal, count)
		}
	}
}

func TestSelectCompactionBatchesRequiresFullSmallFileGroup(t *testing.T) {
	const groupSize = 16
	batches := make([]telemetry.BatchMetadata, groupSize)
	for i := range batches {
		batches[i] = telemetry.BatchMetadata{ID: fmt.Sprintf("batch-%d", i), MaxIngestedNanos: 1, Spans: 1}
	}
	if selected := selectCompactionBatchesWithHigherCap(batches[:groupSize-1], groupSize, groupSize, 0); len(selected) != 0 {
		t.Fatalf("selected partial group of %d batches", len(selected))
	}
	if selected := selectCompactionBatchesWithHigherCap(batches, groupSize, groupSize, 0); len(selected) != groupSize {
		t.Fatalf("selected full group = %d, want %d", len(selected), groupSize)
	}
}

func TestSelectCompactionBatchesBuildsRowBoundedGroup(t *testing.T) {
	const maxBatches = 16
	batches := make([]telemetry.BatchMetadata, maxBatches)
	for i := range batches {
		batches[i] = telemetry.BatchMetadata{
			ID: fmt.Sprintf("batch-%d", i), MaxIngestedNanos: 1, Generation: 2,
			Spans: 2_000_000,
			// Sized well under the byte ceiling so the row ceiling stays the
			// constraint under test here; the byte ceiling has its own tests.
			Bytes: 1 << 20,
		}
	}
	selected := selectCompactionBatchesWithHigherCap(batches, maxBatches, maxBatches, 0)
	if len(selected) != 12 {
		t.Fatalf("selected %d batches, want 12 below row limit", len(selected))
	}
	var rows int
	for _, batch := range selected {
		rows += batch.Spans + batch.Logs + batch.Metrics
	}
	if rows > maxCompactionRows {
		t.Fatalf("selected %d rows above limit %d", rows, maxCompactionRows)
	}
}

func TestSelectCompactionBatchesCombinesSaturatedLargeFiles(t *testing.T) {
	const maxBatches = 16
	batches := make([]telemetry.BatchMetadata, 4)
	for i := range batches {
		batches[i] = telemetry.BatchMetadata{
			ID: fmt.Sprintf("large-%d", i), MaxIngestedNanos: 1, Generation: 3,
			Spans: 6_000_000,
			// Sized well under the byte ceiling so saturation here is the row
			// ceiling, which is what this test is about.
			Bytes: 1 << 20,
		}
	}
	selected := selectCompactionBatchesWithHigherCap(batches, maxBatches, maxBatches, 0)
	if len(selected) != len(batches) {
		t.Fatalf("selected %d saturated large batches, want %d", len(selected), len(batches))
	}
}

func TestSelectCompactionBatchesSkipsUncompactableOlderGroup(t *testing.T) {
	batches := make([]telemetry.BatchMetadata, 2*minCompactionInputs)
	for i := range minCompactionInputs {
		batches[i] = telemetry.BatchMetadata{
			ID: fmt.Sprintf("oversized-%d", i), MaxIngestedNanos: 1,
			Spans: maxCompactionRows + 1,
		}
	}
	for i := minCompactionInputs; i < len(batches); i++ {
		batches[i] = telemetry.BatchMetadata{
			ID: fmt.Sprintf("newer-%d", i), MaxIngestedNanos: int64(24*time.Hour) + 1,
			Spans: 1,
		}
	}
	selected := selectCompactionBatchesWithHigherCap(batches, minCompactionInputs, minCompactionInputs, 0)
	if len(selected) != minCompactionInputs || !strings.HasPrefix(selected[0].ID, "newer-") {
		t.Fatalf("selection did not skip uncompactable older group: %#v", selected)
	}
}

func openTestDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func traceAll(repository *Repository, traceID string) ([]telemetry.IndexedSpan, error) {
	spans, _, err := repository.Parquet.Trace(context.Background(), telemetry.TraceQuery{
		TraceID: traceID, StartNanos: -1 << 63, EndNanos: 1<<63 - 1, Limit: 500,
	})
	return spans, err
}

// A live repository owns its data directory exclusively: it deletes, renames,
// and rewrites files there with no cross-process coordination. A second process
// opened on the same directory must fail fast rather than race the first into
// corruption, and the hold must end at Close so the normal reopen path works.
func TestOpenRejectsSecondProcessOnSameDataDir(t *testing.T) {
	dir := t.TempDir()
	first, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(dir)
	if err == nil {
		_ = second.Close()
		_ = first.Close()
		t.Fatal("second Open succeeded while the first still held the data directory")
	}
	if !strings.Contains(err.Error(), dir) || !strings.Contains(err.Error(), "another process") {
		t.Fatalf("lock error must name the data directory and another process: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after Close: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
func (f testParquetPublisherFunc) PublishParquetReplacement(ctx context.Context, _ telemetry.BatchMetadata, _ []string, publish func(context.Context) error) error {
	return f.PublishParquet(ctx, publish)
}
func (c *testParquetCompactor) PublishParquetReplacement(ctx context.Context, _ telemetry.BatchMetadata, _ []string, publish func(context.Context) error) error {
	return c.PublishParquet(ctx, publish)
}

// commitRestoredFixture seeds already-stamped historical inputs, as recovery
// does. Normal ingest tests use Repository.Commit and its fresh attempt stamp.
func commitRestoredFixture(repo *Repository, ctx context.Context, batch Batch) error {
	var lo, hi int64
	include := func(n int64) {
		if n > 0 && (lo == 0 || n < lo) {
			lo = n
		}
		hi = max(hi, n)
	}
	for i := range batch.Spans {
		row := &batch.Spans[i]
		row.Namespace = telemetry.NormalizeNamespace(row.Namespace)
		include(row.IngestedAt)
		if row.StartUnixNanos == 0 {
			row.StartUnixNanos = row.IngestedAt
		}
	}
	for i := range batch.Logs {
		row := &batch.Logs[i]
		row.Namespace = telemetry.NormalizeNamespace(row.Namespace)
		include(row.IngestedAt)
		if row.EventUnixNanos == 0 {
			row.EventUnixNanos = telemetry.FirstPositiveNanos(row.TimeUnixNanos, row.ObservedTimeNanos, row.IngestedAt)
		}
	}
	for i := range batch.Metrics {
		row := &batch.Metrics[i]
		row.Namespace = telemetry.NormalizeNamespace(row.Namespace)
		include(row.IngestedAt)
		if row.EventUnixNanos == 0 {
			row.EventUnixNanos = telemetry.FirstPositiveNanos(row.TimeUnixNanos, row.IngestedAt)
		}
	}
	return repo.Parquet.CommitBatch(ctx, &telemetry.BatchMetadata{ID: batch.ID, MinIngestedNanos: lo, MaxIngestedNanos: hi}, batch.Spans, batch.Logs, batch.Metrics)
}
