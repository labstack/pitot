package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/labstack/pitot/internal/metrics"
	"github.com/labstack/pitot/internal/telemetry"
)

type Batch struct {
	ID      string
	Spans   []telemetry.Span
	Logs    []telemetry.Log
	Metrics []telemetry.Metric
}

// Repository publishes self-contained Parquet batch directories. The
// directory rename is the transaction and the filesystem is the catalog.
type Repository struct {
	releaseBacklogGauge func()
	lock                *dataDirLock
	root                string
	Parquet             *telemetry.ParquetStore
	compactionMu        sync.Mutex
	publicationMu       sync.Mutex
	lastStamp           int64
	lastPublished       int64
	minimumStamp        int64
	inFlight            map[int64]struct{}

	// Guarded by compactionMu; generation-0 completions must not reset this.
	preemptedCompactionInputs int
}

func Open(root string) (*Repository, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	// Take the directory lock before touching any batch file, so a second
	// process cannot delete this one's staging and compaction work mid-write.
	lock, err := acquireDataDirLock(root)
	if err != nil {
		return nil, err
	}
	parquetStore, err := telemetry.OpenParquetStore(filepath.Join(root, "parquet"))
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	r := &Repository{root: root, Parquet: parquetStore, lock: lock}
	// Compaction has no durable marker and no boot-time rollback. A compaction
	// is purely in-process: it merges into compaction/<output> staging and
	// commits by renaming that into the batch namespace, where the output's
	// replaces.json retires its inputs. So the entire recovery protocol is to
	// discard the staging directory and let replaces.json decide. A crash
	// before the rename leaves the inputs live as .batch and the staging is
	// dropped here; a crash after it leaves the output present with its
	// replaces.json, which loadBatches has already applied to retire the
	// inputs. Either way the on-disk state is consistent and nothing refuses
	// to start.
	if err := r.cleanupCompactionArtifacts(); err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("clean Parquet compaction staging: %w", err)
	}
	if _, err := parquetStore.CollectRetired(); err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("delete retired Parquet batches: %w", err)
	}
	if err := r.cleanupRetired(); err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("clean retired Parquet batches: %w", err)
	}
	for _, batch := range parquetStore.BatchMetadata() {
		r.lastStamp = max(r.lastStamp, batch.MaxIngestedNanos)
		r.lastPublished = max(r.lastPublished, batch.MaxIngestedNanos)
		if batch.MinIngestedNanos > 0 && (r.minimumStamp == 0 || batch.MinIngestedNanos < r.minimumStamp) {
			r.minimumStamp = batch.MinIngestedNanos
		}
	}
	r.releaseBacklogGauge = metrics.SetCompactionBacklogSource(parquetStore.Generation0BatchCount)
	return r, nil
}

// cleanupCompactionArtifacts discards the compaction staging directory on
// boot. A compaction that did not commit never renamed its staging into the
// batch namespace, so the staging holds no authoritative rows; one that did
// commit already left the output in the namespace, so the staging is spent.
// Dropping it is the whole boot-time recovery for compaction.
func (r *Repository) cleanupCompactionArtifacts() error {
	if err := os.RemoveAll(filepath.Join(r.root, "compaction")); err != nil {
		return err
	}
	return syncDirectory(r.root)
}

func (r *Repository) Close() error {
	if r.releaseBacklogGauge != nil {
		r.releaseBacklogGauge()
	}
	// Close the Parquet handles before releasing the lock, so the directory is
	// quiescent by the time another process may acquire it.
	err := r.Parquet.Close()
	if r.lock != nil {
		err = errors.Join(err, r.lock.Close())
		r.lock = nil
	}
	return err
}

func (r *Repository) CompactionDebt() int { return r.Parquet.CompactionDebt() }

// CleanupParquet removes retired inputs only while no retention or compaction
// transaction can still need them for rollback.
func (r *Repository) CleanupParquet() error {
	r.compactionMu.Lock()
	defer r.compactionMu.Unlock()
	_, err := r.Parquet.CollectRetired()
	return errors.Join(err, r.cleanupRetired())
}

// CollectRetiredParquet deletes batches that retention or compaction retired
// once no reader that captured them remains. Serialized with both, so a
// directory is never deleted while a publication or recovery inspects it.
func (r *Repository) CollectRetiredParquet() (int, error) {
	r.compactionMu.Lock()
	defer r.compactionMu.Unlock()
	return r.Parquet.CollectRetired()
}

// cleanupRetired reclaims retention's <id>.retired directories whose removal
// was interrupted after the rename. Compaction no longer produces any retired
// directory of its own: it retires inputs through each output's replaces.json,
// and CollectRetired deletes those input directories in place.
func (r *Repository) cleanupRetired() error {
	entries, err := os.ReadDir(r.Parquet.BatchesDir())
	if err != nil {
		return err
	}
	removed := false
	var cleanupErr error
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() || !strings.HasSuffix(name, ".retired") {
			continue
		}
		if err := os.RemoveAll(filepath.Join(r.Parquet.BatchesDir(), name)); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		} else {
			removed = true
		}
	}
	if removed {
		cleanupErr = errors.Join(cleanupErr, syncDirectory(r.Parquet.BatchesDir()))
	}
	return cleanupErr
}

// beginPublication reserves a strictly increasing attempt stamp before any row
// can become visible. The same mutex protects reservations and the horizon.
func (r *Repository) beginPublication(now int64) int64 {
	r.publicationMu.Lock()
	defer r.publicationMu.Unlock()
	r.lastStamp = max(now, r.lastStamp+1)
	// A renamed directory can remain visible after its final fsync fails.
	// Keep its stamp in the initial scan even before a retry adopts it.
	if r.minimumStamp == 0 {
		r.minimumStamp = r.lastStamp
	}
	if r.inFlight == nil {
		r.inFlight = make(map[int64]struct{})
	}
	r.inFlight[r.lastStamp] = struct{}{}
	return r.lastStamp
}

func (r *Repository) finishPublication(stamp int64, published telemetry.BatchMetadata) {
	r.publicationMu.Lock()
	defer r.publicationMu.Unlock()
	if published.MaxIngestedNanos > 0 {
		r.lastPublished = max(r.lastPublished, published.MaxIngestedNanos)
		if r.minimumStamp == 0 || published.MinIngestedNanos < r.minimumStamp {
			r.minimumStamp = published.MinIngestedNanos
		}
	}
	delete(r.inFlight, stamp)
}

// PublicationBounds returns the exact completed-attempt horizon, the largest
// published stamp (for lag), and the minimum opened or reserved stamp (for initial catch-up).
// Compaction/recovery only republish existing stamps and do not reserve attempts.
func (r *Repository) PublicationBounds() (horizon, published, minimum int64) {
	r.publicationMu.Lock()
	defer r.publicationMu.Unlock()
	horizon = r.lastStamp
	for stamp := range r.inFlight {
		horizon = min(horizon, stamp-1)
	}
	return horizon, r.lastPublished, r.minimumStamp
}

func (r *Repository) Commit(ctx context.Context, batch Batch) error {
	if err := validateBatch(batch); err != nil {
		return err
	}
	stamp := r.beginPublication(time.Now().UnixNano())
	var published telemetry.BatchMetadata
	defer func() { r.finishPublication(stamp, published) }()
	normalizeBatch(&batch, stamp)
	metadata := telemetry.BatchMetadata{
		ID: batch.ID, MinIngestedNanos: stamp, MaxIngestedNanos: stamp,
	}
	err := r.Parquet.CommitBatch(ctx, &metadata, batch.Spans, batch.Logs, batch.Metrics)
	if err == nil {
		// An idempotent retry can adopt a directory from a previous attempt
		// whose final fsync failed. Its durable rows retain that older stamp.
		published = metadata
	}
	return err
}

func (r *Repository) RowCount() uint64 { return r.Parquet.RowCount() }

func (r *Repository) PruneParquet(ctx context.Context, publisher ParquetPublisher, cutoff int64, maxBatches int) (int, error) {
	r.compactionMu.Lock()
	defer r.compactionMu.Unlock()
	return r.Parquet.PruneBefore(cutoff, maxBatches, func(prune func(context.Context) error) error {
		return publisher.PublishParquet(ctx, prune)
	})
}

// PruneParquetPass starts bounded publications until the phase budget expires.
// The budget is checked between publications so an atomic swap is never
// canceled halfway through.
func (r *Repository) PruneParquetPass(ctx context.Context, publisher ParquetPublisher, cutoff int64, maxBatches int, budget time.Duration) (int, error) {
	if maxBatches <= 0 || budget <= 0 {
		return 0, nil
	}
	deadline := time.Now().Add(budget)
	total := 0
	for {
		count, err := r.PruneParquet(ctx, publisher, cutoff, maxBatches)
		total += count
		if err != nil || count < maxBatches || !time.Now().Before(deadline) {
			return total, err
		}
	}
}

func validateBatch(batch Batch) error {
	if batch.ID == "" || strings.ContainsAny(batch.ID, `/\\`) {
		return errors.New("telemetry batch requires a safe ID")
	}
	if rows := batchRows(batch); rows == 0 {
		return errors.New("telemetry batch is empty")
	}
	return nil
}

func normalizeBatch(batch *Batch, ingestedAt int64) {
	for i := range batch.Spans {
		batch.Spans[i].Namespace = telemetry.NormalizeNamespace(batch.Spans[i].Namespace)
		batch.Spans[i].IngestedAt = ingestedAt
		if batch.Spans[i].StartUnixNanos == 0 {
			batch.Spans[i].StartUnixNanos = batch.Spans[i].IngestedAt
		}
	}
	for i := range batch.Logs {
		batch.Logs[i].Namespace = telemetry.NormalizeNamespace(batch.Logs[i].Namespace)
		batch.Logs[i].IngestedAt = ingestedAt
		if batch.Logs[i].EventUnixNanos == 0 {
			batch.Logs[i].EventUnixNanos = telemetry.FirstPositiveNanos(batch.Logs[i].TimeUnixNanos, batch.Logs[i].ObservedTimeNanos, batch.Logs[i].IngestedAt)
		}
	}
	for i := range batch.Metrics {
		batch.Metrics[i].Namespace = telemetry.NormalizeNamespace(batch.Metrics[i].Namespace)
		batch.Metrics[i].IngestedAt = ingestedAt
		if batch.Metrics[i].EventUnixNanos == 0 {
			batch.Metrics[i].EventUnixNanos = telemetry.FirstPositiveNanos(batch.Metrics[i].TimeUnixNanos, batch.Metrics[i].IngestedAt)
		}
	}
}
