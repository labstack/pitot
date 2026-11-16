package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/labstack/pitot/internal/telemetry"
)

// Compaction keeps no durable marker and performs no boot-time rollback. A
// compaction merges its inputs into compaction/<output> staging and commits by
// renaming that staging into the batch namespace, where the output's
// replaces.json retires the inputs. These tests pin that both sides of that
// single rename are crash-safe, by abandoning a repository in each window and
// reopening the same directory. Each one asserts exact row counts and catalog
// composition, so it fails if recovery stops masking inputs by replaces.json,
// completes a merge it should have discarded, or loses rows.

// seedCompactionInputs commits n single-row-per-signal batches that all belong
// to trace-1, so the trace query returns one span per surviving input.
func seedCompactionInputs(t *testing.T, repository *Repository, n int) []string {
	t.Helper()
	ids := make([]string, n)
	for i := range n {
		batch := testBatch()
		batch.ID = fmt.Sprintf("in-%d", i)
		batch.Spans[0].SpanID = fmt.Sprintf("span-%d", i)
		batch.Spans[0].StartUnixNanos = int64(100 + i)
		if err := repository.Commit(context.Background(), batch); err != nil {
			t.Fatal(err)
		}
		ids[i] = batch.ID
	}
	return ids
}

// stageCompactionMerge reproduces a compaction that merged a complete output
// into compaction/<outID> staging but has not yet renamed it into the batch
// namespace -- the exact on-disk state when a process dies after the merge and
// before the commit. It writes no marker, no replaces.json, and never renames.
func stageCompactionMerge(t *testing.T, repository *Repository, outID string, inputIDs []string) telemetry.BatchMetadata {
	t.Helper()
	byID := map[string]telemetry.BatchMetadata{}
	for _, m := range repository.Parquet.BatchMetadata() {
		byID[m.ID] = m
	}
	output := telemetry.BatchMetadata{ID: outID, Generation: 1, MinIngestedNanos: math.MaxInt64}
	for _, id := range inputIDs {
		m := byID[id]
		if m.MinIngestedNanos > 0 {
			output.MinIngestedNanos = min(output.MinIngestedNanos, m.MinIngestedNanos)
		}
		output.MaxIngestedNanos = max(output.MaxIngestedNanos, m.MaxIngestedNanos)
		output.Spans += m.Spans
		output.Logs += m.Logs
		output.Metrics += m.Metrics
	}
	if output.MinIngestedNanos == math.MaxInt64 {
		output.MinIngestedNanos = 0
	}
	stage := repository.compactionStage(outID)
	if err := os.MkdirAll(stage, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, s := range telemetry.Signals {
		var paths []string
		for _, id := range inputIDs {
			path := filepath.Join(repository.Parquet.BatchPath(id), s.Name+".parquet")
			if _, err := os.Stat(path); err == nil {
				paths = append(paths, path)
			} else if err != nil {
				t.Fatal(err)
			}
		}
		if len(paths) == 0 {
			continue
		}
		stats, err := repository.Parquet.MergeParquet(context.Background(), s.Name, paths, filepath.Join(stage, s.Name+".parquet"))
		if err != nil {
			t.Fatal(err)
		}
		if s.Name == "spans" {
			output.MinSpanStartNanos = stats.MinSpanStartNanos
			output.MaxSpanStartNanos = stats.MaxSpanStartNanos
		}
	}
	if err := repository.Parquet.PrepareReplacement(stage, output); err != nil {
		t.Fatal(err)
	}
	return output
}

// W1: a merge produced a complete compaction/<output> stage but the process
// died before the stage->final rename. No output reached the batch namespace,
// so the inputs are still the authoritative copy. Reopening must keep every
// input live and queryable, must not surface the output, and must discard the
// staging directory.
func TestInterruptedCompactionBeforeRenameKeepsInputsLive(t *testing.T) {
	dir := t.TempDir()
	repository, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	const inputs = minCompactionInputs
	ids := seedCompactionInputs(t, repository, inputs)
	output := stageCompactionMerge(t, repository, "compact-before-rename", ids)
	// Abandon the process with the stage prepared but never committed.
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	metadata := reopened.Parquet.BatchMetadata()
	if len(metadata) != inputs {
		t.Fatalf("catalog holds %d batches, want the %d live inputs", len(metadata), inputs)
	}
	for _, m := range metadata {
		if m.ID == output.ID {
			t.Fatalf("an unpublished compaction output became visible: %s", output.ID)
		}
	}
	for _, id := range ids {
		if _, err := os.Stat(reopened.Parquet.BatchPath(id)); err != nil {
			t.Fatalf("input %s was lost: %v", id, err)
		}
	}
	if got := reopened.RowCount(); got != uint64(3*inputs) {
		t.Fatalf("rows after discarding the stage = %d, want %d", got, 3*inputs)
	}
	spans, err := traceAll(reopened, "trace-1")
	if err != nil || len(spans) != inputs {
		t.Fatalf("trace spans after discarding the stage = %d, %v", len(spans), err)
	}
	if _, err := os.Stat(filepath.Join(dir, "compaction")); !os.IsNotExist(err) {
		t.Fatalf("compaction staging was not discarded: %v", err)
	}
}

// W2: the stage->final rename landed, so the output and its replaces.json are
// durable, but the process died before the retired input directories were
// deleted. Reopening must surface the output, retire the inputs named by
// replaces.json without double-counting them, and reclaim their directories.
func TestInterruptedCompactionAfterRenameRetiresInputsFromReplaces(t *testing.T) {
	dir := t.TempDir()
	repository, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	const inputs = minCompactionInputs
	ids := seedCompactionInputs(t, repository, inputs)
	// The real compaction path renames the output into place and writes its
	// replaces.json; deferred deletion (CollectRetiredParquet) is deliberately
	// not run, so the retired input directories are still on disk -- the window
	// this test reopens in.
	compacted, err := repository.CompactParquet(context.Background(), &testParquetCompactor{}, minCompactionInputs)
	if err != nil || compacted != inputs {
		t.Fatalf("compaction = %d, %v", compacted, err)
	}
	for _, id := range ids {
		if _, err := os.Stat(repository.Parquet.BatchPath(id)); err != nil {
			t.Fatalf("input %s should still be on disk before deferred deletion: %v", id, err)
		}
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	metadata := reopened.Parquet.BatchMetadata()
	if len(metadata) != 1 || metadata[0].Generation != 1 {
		t.Fatalf("catalog = %#v, want the single compacted output", metadata)
	}
	if got := reopened.RowCount(); got != uint64(3*inputs) {
		t.Fatalf("rows after recovery = %d, want %d with inputs masked once", got, 3*inputs)
	}
	spans, err := traceAll(reopened, "trace-1")
	if err != nil || len(spans) != inputs {
		t.Fatalf("trace spans after recovery = %d, %v", len(spans), err)
	}
	for _, id := range ids {
		if _, err := os.Stat(reopened.Parquet.BatchPath(id)); !os.IsNotExist(err) {
			t.Fatalf("retired input %s was not reclaimed: %v", id, err)
		}
	}
}

// W3: the scenario the deleted TestRepositoryRecoversInterruptedCompactionSwap
// covered, re-expressed against replaces.json recovery. The output is durable
// and deferred deletion got halfway: some retired input directories are gone,
// some remain. Reopening must not expose a half-old/half-new file set -- only
// the output is authoritative, every row survives exactly once, and the
// leftover input directories are reclaimed.
func TestInterruptedCompactionMidInputDeletionRetiresRemainder(t *testing.T) {
	dir := t.TempDir()
	repository, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	const inputs = minCompactionInputs
	ids := seedCompactionInputs(t, repository, inputs)
	compacted, err := repository.CompactParquet(context.Background(), &testParquetCompactor{}, minCompactionInputs)
	if err != nil || compacted != inputs {
		t.Fatalf("compaction = %d, %v", compacted, err)
	}
	// Reproduce a crash partway through deleting the retired inputs: half the
	// directories are already gone, half still linger beside the output.
	for _, id := range ids[:inputs/2] {
		if err := os.RemoveAll(repository.Parquet.BatchPath(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	metadata := reopened.Parquet.BatchMetadata()
	if len(metadata) != 1 || metadata[0].ID == "" || metadata[0].Generation != 1 {
		t.Fatalf("catalog = %#v, want only the compacted output", metadata)
	}
	if got := reopened.RowCount(); got != uint64(3*inputs) {
		t.Fatalf("rows after partial deletion recovery = %d, want %d", got, 3*inputs)
	}
	spans, err := traceAll(reopened, "trace-1")
	if err != nil || len(spans) != inputs {
		t.Fatalf("trace spans after partial deletion recovery = %d, %v", len(spans), err)
	}
	leftover, err := filepath.Glob(filepath.Join(reopened.Parquet.BatchesDir(), "in-*"))
	if err != nil || len(leftover) != 0 {
		t.Fatalf("retired inputs after recovery = %v, %v", leftover, err)
	}
}

// A compaction whose publish fails leaves no marker and no partial output: the
// staging is discarded and the inputs stay live, so no row is lost and a later
// pass simply redoes the merge.
func TestFailedCompactionPublishKeepsInputsLiveAndRetryPreservesRows(t *testing.T) {
	dir := t.TempDir()
	repository, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	const inputs = minCompactionInputs
	seedCompactionInputs(t, repository, inputs)

	compactor := &testParquetCompactor{publishErr: errors.New("publication unavailable")}
	if _, err := repository.CompactParquet(context.Background(), compactor, minCompactionInputs); err == nil {
		t.Fatal("compaction reported success despite a publication failure")
	}
	if metadata := repository.Parquet.BatchMetadata(); len(metadata) != inputs {
		t.Fatalf("catalog = %d batches after a failed publish, want the %d inputs untouched", len(metadata), inputs)
	}
	if got := repository.RowCount(); got != uint64(3*inputs) {
		t.Fatalf("rows after a failed publish = %d, want %d", got, 3*inputs)
	}
	spans, err := traceAll(repository, "trace-1")
	if err != nil || len(spans) != inputs {
		t.Fatalf("trace spans after a failed publish = %d, %v", len(spans), err)
	}

	compactor.publishErr = nil
	compacted, err := repository.CompactParquet(context.Background(), compactor, minCompactionInputs)
	if err != nil || compacted < minCompactionMergeInputs {
		t.Fatalf("retry compaction = %d, %v", compacted, err)
	}
	if got := repository.RowCount(); got != uint64(3*inputs) {
		t.Fatalf("rows after a successful retry = %d, want %d", got, 3*inputs)
	}
	spans, err = traceAll(repository, "trace-1")
	if err != nil || len(spans) != inputs {
		t.Fatalf("trace spans after a successful retry = %d, %v", len(spans), err)
	}
}
