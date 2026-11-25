package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/labstack/pitot/internal/metrics"
	"github.com/labstack/pitot/internal/telemetry"
	"golang.org/x/sync/errgroup"
)

const (
	maxCompactionRows = telemetry.CompactionMaxRows

	// maxCompactionBytes bounds a pass by the bytes it admits, because that is
	// what a merge costs: parquet-go opens a cursor per row group across every
	// input and holds each one's dictionaries at once. Measured on real batches,
	// peak RSS tracks the on-disk input bytes almost exactly -- 4 inputs
	// totalling 442 MiB on disk merged at +413 MiB RSS.
	//
	// Row counts cannot stand in for it: wide spans carrying large attribute
	// payloads and bare log lines differ by an order of magnitude at equal row
	// counts.
	//
	// The figure is a memory budget, so it also decides how fast compaction can
	// consolidate. Set too low it throttles the cascade: at 256 MiB a pass
	// admitted only ~16 of the 16 MB generation-2 batches instead of the 128 the
	// count ceiling allows, and 290 of them piled up unpromoted. 768 MiB keeps
	// merge peak well inside the headroom the process now has -- it sits at
	// 1.5 GiB against a 4 GB DuckDB budget -- while admitting enough inputs per
	// pass for the higher generations to drain.
	maxCompactionBytes = 768 << 20

	// assumedCompactionBytesPerRow prices a batch whose size was never
	// measured. Measured batches run well under this -- span rows with JSON
	// attributes compress to roughly 75 bytes on disk -- so the estimate is
	// deliberately several times pessimistic: over-charging costs smaller
	// groups and more passes, while under-charging costs the merge that the
	// byte ceiling exists to prevent.
	assumedCompactionBytesPerRow = 256
	minCompactionInputs          = 8

	// minCompactionMergeInputs is the smallest group worth merging, and the
	// floor the interrupted-pass halving stops at.
	minCompactionMergeInputs = 2

	// compactionSettledHours is how far an ingest hour trails the newest
	// before its files may merge across the day. Hourly files keep narrow
	// queries cheap, and a quiet install would otherwise keep one small file
	// per hour per signal for its whole retention, 720 at 30 days.
	compactionSettledHours = 24

	// maxDayCompactionRows bounds a file merged across hours. Span files sort
	// by trace, so a narrow query decodes the whole file, at about a million
	// rows a second on two threads for an attribute filter, while each extra
	// file costs a scan about a tenth of a millisecond. Merging busy hours
	// would trade cheap files for slower narrow queries: on a 72-hour pico
	// simulation, million-row day files cut 73 span files to 33 but slowed a
	// one-minute attribute filter on old data from 0.12 to 0.66 seconds and a
	// 24-hour one from 2.5 to 3.6. Only hours smaller than a busy one merge.
	maxDayCompactionRows int64 = 250_000
)

// compactionAttemptFile records that a merge was started. The completion
// marker is written only after PrepareReplacement, so a process killed during
// the merge -- which is the likely way a large merge ends -- leaves no trace
// that it ever ran. The next start reselects the same group by the same rules
// and dies identically, and nothing in the system observes the loop. This file
// is what a restart reads to know the last attempt was too big.
const compactionAttemptFile = "COMPACTION-ATTEMPT.json"

type compactionAttempt struct {
	Inputs int `json:"inputs"`
}

// compactionBatchCap halves the ceiling after an interrupted pass, so repeated
// kills walk the group down instead of retrying the same one. It never goes
// below a pair: selection always admits two however large they are, so a lower
// floor would stop compaction rather than shrink it. A merge that still dies
// at two inputs is a single file too large to merge, which is a different
// problem and not one a smaller group can solve.
func compactionBatchCap(maxBatches, lastAttempt int) int {
	if lastAttempt <= 0 {
		return maxBatches
	}
	capped := min(maxBatches, lastAttempt) / 2
	return max(capped, minCompactionMergeInputs)
}

// readCompactionAttempt reports the size of an interrupted pass, or zero when
// there was none. A missing, damaged or nonsensical file reads as zero: losing
// this record costs one oversized merge, while treating it as fatal would cost
// the ability to compact at all.
func readCompactionAttempt(root string) int {
	data, err := os.ReadFile(filepath.Join(root, compactionAttemptFile))
	if err != nil {
		return 0
	}
	var attempt compactionAttempt
	if err := json.Unmarshal(data, &attempt); err != nil || attempt.Inputs <= 0 {
		return 0
	}
	return attempt.Inputs
}

func writeCompactionAttempt(root string, inputs int) error {
	data, err := json.Marshal(compactionAttempt{Inputs: inputs})
	if err != nil {
		return err
	}
	return writeDurableFile(filepath.Join(root, compactionAttemptFile), data)
}

func clearCompactionAttempt(root string) error {
	if err := os.Remove(filepath.Join(root, compactionAttemptFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// A compactionKey names a group: an ingest hour and generation, or, for
// settled files, the first hour of their day.
type compactionKey struct {
	window     int64
	generation uint32
	day        bool
}

// ParquetPublisher lets the query layer wrap the atomic catalog swap; storage
// owns native merges, crash-safe replacement state and deferred deletion.
type ParquetPublisher interface {
	PublishParquet(context.Context, func(context.Context) error) error
	PublishParquetReplacement(context.Context, telemetry.BatchMetadata, []string, func(context.Context) error) error
}

// CompactParquet combines one same-window, same-generation group. The output is
// prepared outside the query gate and swapped as one batch directory.
func (r *Repository) CompactParquet(ctx context.Context, publisher ParquetPublisher, maxBatches int) (int, error) {
	if publisher == nil || maxBatches < minCompactionInputs {
		return 0, nil
	}
	r.compactionMu.Lock()
	defer r.compactionMu.Unlock()
	for {
		count, err := r.compactParquet(ctx, publisher, maxBatches)
		if !errors.Is(err, errCompactionPreempted) {
			return count, err
		}
		// The interrupted output was never prepared or marked. Its workers and
		// staging cleanup have finished. The higher-generation retry size is
		// retained separately while generation 0 retires debt and clears its
		// own attempt record. Reselect immediately, prioritizing generation 0.
		if err := clearCompactionAttempt(r.root); err != nil {
			return 0, err
		}
	}
}

func (r *Repository) compactParquet(ctx context.Context, publisher ParquetPublisher, maxBatches int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	// A pass that was killed mid-merge left a record of how much it tried to
	// take. Take less.
	batchCap := compactionBatchCap(maxBatches, readCompactionAttempt(r.root))
	selected := selectCompactionBatchesWithHigherCap(r.Parquet.BatchMetadata(), batchCap,
		compactionBatchCap(batchCap, r.preemptedCompactionInputs), r.Parquet.CompactionDebt())
	if len(selected) < minCompactionMergeInputs {
		return 0, nil
	}
	// Day groups mix generations; the output sits above all of them.
	var generation uint32
	for _, batch := range selected {
		generation = max(generation, batch.Generation)
	}
	output := telemetry.BatchMetadata{
		ID: fmt.Sprintf("compact-%d", time.Now().UnixNano()), MinIngestedNanos: math.MaxInt64,
		Generation: generation + 1,
	}
	inputs := make([]string, 0, len(selected))
	for _, batch := range selected {
		inputs = append(inputs, batch.ID)
		if batch.MinIngestedNanos > 0 {
			output.MinIngestedNanos = min(output.MinIngestedNanos, batch.MinIngestedNanos)
		}
		output.MaxIngestedNanos = max(output.MaxIngestedNanos, batch.MaxIngestedNanos)
		output.Spans += batch.Spans
		output.Logs += batch.Logs
		output.Metrics += batch.Metrics
	}
	if output.MinIngestedNanos == math.MaxInt64 {
		output.MinIngestedNanos = 0
	}
	stage := r.compactionStage(output.ID)
	if err := os.RemoveAll(stage); err != nil {
		return 0, err
	}
	if err := os.MkdirAll(stage, 0o755); err != nil {
		return 0, err
	}
	if err := errors.Join(telemetry.SyncDirectory(filepath.Dir(stage)), telemetry.SyncDirectory(r.root)); err != nil {
		return 0, err
	}
	// The staging directory is the whole crash-recovery story. If anything below
	// fails before PublishReplacement renames it into the batch namespace, it is
	// removed here and, equivalently, discarded on the next boot; the selected
	// inputs stay live and a later pass redoes the merge. Once the rename lands,
	// this path no longer exists and the removal is a no-op.
	defer func() { _ = os.RemoveAll(stage) }()
	type mergePlan struct {
		signal string
		inputs []string
		output string
	}
	plans := make([]mergePlan, 0, len(telemetry.Signals))
	for _, s := range telemetry.Signals {
		signal := s.Name
		inputs := make([]string, 0, len(selected))
		for _, batch := range selected {
			path := filepath.Join(r.Parquet.BatchPath(batch.ID), signal+".parquet")
			if _, err := os.Stat(path); err == nil {
				inputs = append(inputs, path)
			} else if !errors.Is(err, os.ErrNotExist) {
				return 0, err
			}
		}
		if len(inputs) == 0 {
			continue
		}
		plans = append(plans, mergePlan{signal: signal, inputs: inputs, output: filepath.Join(stage, signal+".parquet")})
	}
	// Written before the merge, on purpose: the attempt record is cleared only
	// after the merge returns, and the failure this guards against is the merge
	// not returning.
	if err := writeCompactionAttempt(r.root, len(selected)); err != nil {
		return 0, err
	}
	err := r.mergeCompaction(ctx, generation, len(selected), func(ctx context.Context) error {
		group, mergeCtx := errgroup.WithContext(ctx)
		for _, plan := range plans {
			group.Go(func() error {
				stats, err := r.Parquet.MergeParquet(mergeCtx, plan.signal, plan.inputs, plan.output)
				if err != nil {
					return fmt.Errorf("compact %s Parquet: %w", plan.signal, err)
				}
				if plan.signal == "spans" {
					if stats.Rows != int64(output.Spans) {
						return fmt.Errorf("compacted span count: got %d want %d", stats.Rows, output.Spans)
					}
					output.MinSpanStartNanos = stats.MinSpanStartNanos
					output.MaxSpanStartNanos = stats.MaxSpanStartNanos
				}
				if err := mergeCtx.Err(); err != nil {
					return err
				}
				if err := syncFile(plan.output); err != nil {
					return fmt.Errorf("sync compacted %s Parquet: %w", plan.signal, err)
				}
				return nil
			})
		}
		return group.Wait()
	})
	if err != nil {
		return 0, err
	}
	if err := r.Parquet.PrepareReplacement(stage, output); err != nil {
		return 0, err
	}
	// Completion is a single atomic step: PublishReplacement writes the output's
	// replaces.json into the stage, renames the stage into the batch namespace,
	// and retires the inputs. There is no separate durable marker to recover
	// from -- the output's presence plus its replaces.json is the whole record,
	// which boot's loadBatches reapplies, so a crash on either side of the
	// rename is consistent: before it the inputs are live and the stage is
	// discarded, after it the output is live and its replaces.json retires the
	// inputs.
	if err := r.Parquet.PublishReplacement(stage, output, inputs, func(swap func(context.Context) error) error {
		return publisher.PublishParquetReplacement(ctx, output, inputs, swap)
	}); err != nil {
		return 0, err
	}
	// A completed higher-generation output clears the debt-preemption size; a
	// generation-0 or minimum-pair completion must leave it, so an earlier
	// preempted higher generation still shrinks its next attempt.
	if output.Generation > 1 {
		r.preemptedCompactionInputs = 0
	}
	if err := clearCompactionAttempt(r.root); err != nil {
		return 0, err
	}
	return len(selected), nil
}

var errCompactionPreempted = errors.New("higher-generation compaction yielded to ingest debt")

// Only unprepared higher-generation work is cancellable by debt. Join the
// watcher and all merge workers before touching preparation or durable markers.
// Generation 0 and minimum pairs retain the caller context: debt can delay a
// higher generation only until its attempts halve to a pair. The retry size is
// process-local; after a restart the durable interrupted-attempt rule still
// shrinks unfinished work. Called with compactionMu held.
func (r *Repository) mergeCompaction(ctx context.Context, generation uint32, inputs int, merge func(context.Context) error) error {
	if generation == 0 || inputs <= minCompactionMergeInputs {
		return merge(ctx)
	}
	mergeCtx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if r.Parquet.WaitForCompactionLimit(mergeCtx) == nil {
			cancel(errCompactionPreempted)
		}
	}()
	err := merge(mergeCtx)
	cancel(nil)
	<-done
	if (err == nil || errors.Is(err, context.Canceled)) && errors.Is(context.Cause(mergeCtx), errCompactionPreempted) && ctx.Err() == nil {
		r.preemptedCompactionInputs = inputs
		return errCompactionPreempted
	}
	return err
}

func selectCompactionBatchesWithHigherCap(batches []telemetry.BatchMetadata, maxBatches, higherMaxBatches, debt int) []telemetry.BatchMetadata {
	// minCompactionInputs is a "worth the effort" policy and belongs to the
	// caller, which applies it to the ceiling it was asked for. Here the floor
	// is only what a merge needs: after an interrupted pass the ceiling is
	// deliberately halved below that policy, and refusing it would stop
	// compaction at exactly the moment it most needs to make smaller progress.
	if maxBatches < minCompactionMergeInputs {
		return nil
	}
	hour := int64(telemetry.CompactionWindow)
	groups := make(map[compactionKey][]telemetry.BatchMetadata)
	var spread []telemetry.BatchMetadata
	newest := int64(math.MinInt64)
	for _, batch := range batches {
		if batch.MaxIngestedNanos <= 0 {
			continue
		}
		newest = max(newest, batch.MaxIngestedNanos/hour)
		// A file spanning more than an hour came from a day merge; only its
		// day group may take it, under the day bound.
		if batch.MinIngestedNanos > 0 && batch.MaxIngestedNanos-batch.MinIngestedNanos >= hour {
			spread = append(spread, batch)
			continue
		}
		key := compactionKey{window: batch.MaxIngestedNanos / hour, generation: batch.Generation}
		groups[key] = append(groups[key], batch)
	}
	// Under writer pressure, reclaim generation-0 debt before starting a
	// potentially long higher-generation merge, even in an older window. Within
	// that priority keep the normal order: oldest window, then highest
	// generation. Below pressure, higher generations run first within each
	// window so continuous
	// ingest cannot starve them.
	pressure := debt >= telemetry.CompactionDebtLimit/2
	candidates := make(map[compactionKey][]telemetry.BatchMetadata, len(groups))
	days := make(map[int64][]telemetry.BatchMetadata)
	for _, batch := range spread {
		if window := batch.MaxIngestedNanos / hour; window < newest-compactionSettledHours {
			days[window/24] = append(days[window/24], batch)
		}
	}
	for key, group := range groups {
		batchCap := maxBatches
		if key.generation > 0 {
			batchCap = min(batchCap, higherMaxBatches)
		}
		rowLimit := int64(maxCompactionRows)
		if pressure && key.generation == 0 {
			rowLimit = telemetry.CompactionPressureMaxRows
		}
		candidate := selectBoundedCompactionGroup(group, batchCap, rowLimit, key.window < newest)
		if len(candidate) >= minCompactionMergeInputs {
			candidates[key] = candidate
		} else if key.window < newest-compactionSettledHours {
			// A settled hour has nothing left to merge within itself.
			days[key.window/24] = append(days[key.window/24], group...)
		}
	}
	for day, group := range days {
		if candidate := selectDayCompactionGroup(group, min(maxBatches, higherMaxBatches), maxDayCompactionRows); len(candidate) >= minCompactionMergeInputs {
			candidates[compactionKey{window: day * 24, day: true}] = candidate
		}
	}
	var chosen compactionKey
	var selected []telemetry.BatchMetadata
	found := false
	for key, candidate := range candidates {
		priority := pressure && key.generation == 0 && !key.day
		chosenPriority := pressure && chosen.generation == 0 && !chosen.day
		better := !found || priority && !chosenPriority ||
			priority == chosenPriority && (key.window < chosen.window ||
				key.window == chosen.window && key.generation > chosen.generation ||
				key.window == chosen.window && key.generation == chosen.generation && chosen.day)
		if better {
			chosen, selected, found = key, candidate, true
		}
	}
	if !found {
		return nil
	}
	return selected
}

// selectBoundedCompactionGroup keeps the high-reclaim full-group behavior for
// small files, but admits a smaller group when the row ceiling fills first.
// Without the latter, one successful generation can make every later group too
// large for the ceiling and permanently strand those files.
func selectBoundedCompactionGroup(group []telemetry.BatchMetadata, maxBatches int, rowLimit int64, finished bool) []telemetry.BatchMetadata {
	ordered := append([]telemetry.BatchMetadata(nil), group...)
	sort.Slice(ordered, func(i, j int) bool {
		left, right := compactionBatchRows(ordered[i]), compactionBatchRows(ordered[j])
		if left != right {
			return left < right
		}
		if ordered[i].MinIngestedNanos != ordered[j].MinIngestedNanos {
			return ordered[i].MinIngestedNanos < ordered[j].MinIngestedNanos
		}
		return ordered[i].ID < ordered[j].ID
	})
	candidate := make([]telemetry.BatchMetadata, 0, min(maxBatches, len(ordered)))
	var rows, bytes int64
	saturated := false
	var smallest int64
	for _, batch := range ordered {
		batchRows := compactionBatchRows(batch)
		if batchRows <= 0 || batchRows > rowLimit {
			continue
		}
		batchBytes := compactionBatchBytes(batch)
		if smallest == 0 {
			smallest = batchRows
		}
		if len(candidate) == maxBatches {
			saturated = true
			break
		}
		if rows > rowLimit-batchRows {
			saturated = true
			break
		}
		// A merge opens a cursor per row group across every input and holds
		// each one's dictionaries at once, so the live cost of a pass tracks
		// the bytes it admits. Rows cannot stand in for that: wide spans and
		// bare log lines differ by an order of magnitude at equal row counts.
		// A pair is always admitted, however large: a merge of two inputs is
		// bounded by those inputs, and refusing it would strand files that are
		// individually over budget instead of ever shrinking them. Past a pair
		// the ceiling binds.
		if len(candidate) >= 2 && bytes > maxCompactionBytes-batchBytes {
			saturated = true
			break
		}
		candidate = append(candidate, batch)
		rows += batchRows
		bytes += batchBytes
	}
	if rows == rowLimit || smallest > 0 && smallest > rowLimit-rows {
		saturated = true
	}
	if bytes == maxCompactionBytes {
		saturated = true
	}
	// A group is worth merging when it is full, when a ceiling stopped it, or
	// when it belongs to a window that has finished and holds at least a pair.
	//
	// Holding out for a full group is right while a window is still being
	// written: more batches are coming, and merging early wastes the work. It
	// is wrong once the window is over, because the group will never grow
	// again. A finished hour merges even a pair, so a quiet install keeps at
	// most one file per hour rather than up to seven. On the
	// live box that stranded almost everything -- 1,243 batches in 69
	// (day, generation) groups across 25 days, not one of them a candidate,
	// because only the current day ever reached 128 files. Every compaction
	// output was generation 1 and every older day was untouchable, which is why
	// the live file count held at an equilibrium that neither reordering nor a
	// larger byte budget could move.
	if len(candidate) == maxBatches || saturated && len(candidate) >= 2 {
		return candidate
	}
	if finished && len(candidate) >= minCompactionMergeInputs {
		return candidate
	}
	return nil
}

// selectDayCompactionGroup takes the first run of files adjacent in ingest
// time that fits the row, byte and count bounds, so each output covers one
// stretch of the day and logs and metrics, which keep input order, stay in
// time order. A file too large to join its neighbour ends the run.
func selectDayCompactionGroup(group []telemetry.BatchMetadata, maxBatches int, rowLimit int64) []telemetry.BatchMetadata {
	ordered := append([]telemetry.BatchMetadata(nil), group...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].MinIngestedNanos != ordered[j].MinIngestedNanos {
			return ordered[i].MinIngestedNanos < ordered[j].MinIngestedNanos
		}
		return ordered[i].ID < ordered[j].ID
	})
	var run []telemetry.BatchMetadata
	var rows, bytes int64
	for _, batch := range ordered {
		batchRows, batchBytes := compactionBatchRows(batch), compactionBatchBytes(batch)
		if batchRows <= 0 || batchRows >= rowLimit {
			if len(run) >= minCompactionMergeInputs {
				return run
			}
			run, rows, bytes = nil, 0, 0
			continue
		}
		if len(run) == maxBatches || rows > rowLimit-batchRows || bytes > maxCompactionBytes-batchBytes {
			if len(run) >= minCompactionMergeInputs {
				return run
			}
			run, rows, bytes = nil, 0, 0
		}
		run = append(run, batch)
		rows += batchRows
		bytes += batchBytes
	}
	if len(run) >= minCompactionMergeInputs {
		return run
	}
	return nil
}

// compactionBatchBytes prices a batch for the byte ceiling, falling back to a
// pessimistic per-row estimate when the batch was never measured.
func compactionBatchBytes(batch telemetry.BatchMetadata) int64 {
	if batch.Bytes > 0 {
		return batch.Bytes
	}
	rows := compactionBatchRows(batch)
	if rows <= 0 || rows > math.MaxInt64/assumedCompactionBytesPerRow {
		return math.MaxInt64
	}
	return rows * assumedCompactionBytesPerRow
}

func compactionBatchRows(batch telemetry.BatchMetadata) int64 {
	if batch.Spans < 0 {
		return math.MaxInt64
	}
	rows := int64(batch.Spans)
	for _, count := range [...]int{batch.Logs, batch.Metrics} {
		if count < 0 || rows > math.MaxInt64-int64(count) {
			return math.MaxInt64
		}
		rows += int64(count)
	}
	return rows
}

// CompactParquetPass starts complete compactions until the phase budget
// expires. The phase budget never cancels an in-flight merge; only blocking
// ingest debt preempts unprepared higher-generation work. Read pressure must not
// delay debt retirement: growing file counts hurts both reads and ingestion.
func (r *Repository) CompactParquetPass(ctx context.Context, publisher ParquetPublisher, maxBatches int, budget time.Duration) (int, error) {
	if maxBatches <= 0 || budget <= 0 {
		return 0, nil
	}
	started := time.Now()
	defer func() { metrics.CompactionPassSeconds.Observe(time.Since(started).Seconds()) }()
	deadline := started.Add(budget)
	total := 0
	for {
		count, err := r.CompactParquet(ctx, publisher, maxBatches)
		total += count
		if err != nil || count == 0 || !time.Now().Before(deadline) {
			return total, err
		}
	}
}

func (r *Repository) compactionStage(id string) string {
	return filepath.Join(r.root, "compaction", id)
}

func syncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func writeDurableFile(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
