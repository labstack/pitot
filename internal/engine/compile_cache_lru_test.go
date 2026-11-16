package engine

import (
	"fmt"
	"testing"
)

// The snapshot-compile cache is an LRU: reaching capacity evicts exactly one
// entry, the least recently used, rather than clearing the whole cache. A
// wholesale clear would throw away a dashboard's stable, repeatedly-run
// statements every time a burst of one-off queries crossed the bound.
func TestCompileCacheEvictsOneEntryNotAll(t *testing.T) {
	d, _ := newBatchCacheTest(t)
	ctx := t.Context()
	// Distinct statement text per index keys a distinct cache entry.
	query := func(i int) string { return fmt.Sprintf("SELECT %d FROM spans", i) }

	// Fill the cache to capacity; query(0) is the least recently used.
	for i := 0; i < snapshotCacheCapacity; i++ {
		if _, err := d.compileSnapshot(ctx, d.DB, query(i)); err != nil {
			t.Fatal(err)
		}
	}
	// One more entry overflows the cache, evicting exactly the one LRU entry.
	if _, err := d.compileSnapshot(ctx, d.DB, query(snapshotCacheCapacity)); err != nil {
		t.Fatal(err)
	}

	// Every entry except the evicted query(0) must still be cached. Re-reading
	// them is a hit and never evicts, so order among survivors is irrelevant.
	hits0, misses0 := d.SnapshotCacheStats()
	for i := 1; i <= snapshotCacheCapacity; i++ {
		if _, err := d.compileSnapshot(ctx, d.DB, query(i)); err != nil {
			t.Fatal(err)
		}
	}
	hits1, misses1 := d.SnapshotCacheStats()
	if misses1 != misses0 {
		t.Fatalf("surviving entries should all hit, but %d missed (wholesale clear?)", misses1-misses0)
	}
	if hits1-hits0 != int64(snapshotCacheCapacity) {
		t.Fatalf("expected %d hits from survivors, got %d", snapshotCacheCapacity, hits1-hits0)
	}

	// The single oldest entry, and only it, was evicted: a fresh miss.
	if _, err := d.compileSnapshot(ctx, d.DB, query(0)); err != nil {
		t.Fatal(err)
	}
	_, misses2 := d.SnapshotCacheStats()
	if misses2 != misses1+1 {
		t.Fatalf("query(0) should have been the one evicted; misses %d->%d", misses1, misses2)
	}
}
