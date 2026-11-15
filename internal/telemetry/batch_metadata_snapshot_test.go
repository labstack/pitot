package telemetry

import (
	"slices"
	"sort"
	"strings"
	"testing"
)

func orderedBatchIDs(batches []BatchMetadata) string {
	ids := make([]string, len(batches))
	for i, b := range batches {
		ids[i] = b.ID
	}
	return strings.Join(ids, ",")
}

// BatchMetadata publishes an immutable, pre-sorted snapshot rebuilt on each
// catalog mutation, rather than allocating and sorting the whole catalog on
// every call under the lock. The order is canonical (generation, then min
// ingest time, then ID), the same snapshot is returned until the catalog
// changes, callers must not mutate it, and a mutation shows on the next call.
func TestBatchMetadataSharesSortedSnapshot(t *testing.T) {
	p := &ParquetStore{batches: make(map[string]*storedBatch)}
	install := func(id string, generation uint32, minIngest int64) {
		p.installBatch(&storedBatch{metadata: BatchMetadata{ID: id, Generation: generation, MinIngestedNanos: minIngest, Spans: 1}})
	}
	// Installed out of canonical order.
	install("z", 1, 10)
	install("a", 0, 30)
	install("b", 0, 20)
	install("c", 0, 20)

	if got := orderedBatchIDs(p.BatchMetadata()); got != "b,c,a,z" {
		t.Fatalf("canonical order = %q, want \"b,c,a,z\"", got)
	}

	// No per-call allocation or sort: the same snapshot backs each call until
	// the catalog mutates.
	first, second := p.BatchMetadata(), p.BatchMetadata()
	if len(first) == 0 || &first[0] != &second[0] {
		t.Fatal("BatchMetadata reallocated the catalog without a mutation")
	}

	// The snapshot is shared and read-only: a caller sorting a clone must not
	// disturb what the next reader sees.
	clone := slices.Clone(first)
	sort.Slice(clone, func(i, j int) bool { return clone[i].ID > clone[j].ID })
	if got := orderedBatchIDs(p.BatchMetadata()); got != "b,c,a,z" {
		t.Fatalf("shared snapshot was mutated: %q", got)
	}

	// A prune and a commit are both reflected on the next call, re-sorted.
	p.mu.Lock()
	p.setBatchLocked("a", nil) // prune
	p.mu.Unlock()
	install("m", 0, 5) // commit
	if got := orderedBatchIDs(p.BatchMetadata()); got != "m,b,c,z" {
		t.Fatalf("after prune+commit = %q, want \"m,b,c,z\"", got)
	}
}
