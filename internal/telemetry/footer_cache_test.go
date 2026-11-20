package telemetry

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func mkdir(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

func newRand(seed int) *rand.Rand { return rand.New(rand.NewPCG(uint64(seed), 1)) }

// A dropped entry stays open for the reader that holds it and closes on its
// release; the byte bound evicts the least recently used entries.
func TestFooterCacheDropsAndEvictsWithoutClosingHeldFiles(t *testing.T) {
	dir := t.TempDir()
	var paths []string
	for i := range 3 {
		batch := filepath.Join(dir, fmt.Sprint(i)+BatchSuffix)
		paths = append(paths, writeTestLogs(t, mkdir(t, batch), randomTraceLogs(newRand(i), 50)))
	}
	cache := footerCache{}
	_, release, err := cache.open(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	held := cache.entries[paths[0]]
	cache.drop(filepath.Dir(paths[0]))
	if _, ok := cache.entries[paths[0]]; ok || !held.dropped {
		t.Fatal("drop kept the batch's file")
	}
	if _, err := held.file.Stat(); err != nil {
		t.Fatalf("drop closed a held file: %v", err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	if _, err := held.file.Stat(); err == nil {
		t.Fatal("release left a dropped file open")
	}

	cache = footerCache{}
	_, release, err = cache.open(paths[1])
	if err != nil {
		t.Fatal(err)
	}
	cache.limit = cache.bytes + 1
	if err := release(); err != nil {
		t.Fatal(err)
	}
	_, release, err = cache.open(paths[2])
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, ok := cache.entries[paths[1]]; ok || cache.recent.Len() != 1 {
		t.Fatalf("cache kept %d entries over its bound", cache.recent.Len())
	}
}

// Readers share one parsed file concurrently; run under -race.
func TestFooterCacheSharesParsedFilesAcrossReaders(t *testing.T) {
	dir := mkdir(t, filepath.Join(t.TempDir(), "shared"+BatchSuffix))
	rows := randomTraceLogs(newRand(9), 2_000)
	path := writeTestLogs(t, dir, rows)
	cache := footerCache{}
	var group sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			reader, closeReader, err := openCachedReader[traceLogRow](&cache, path)
			if err != nil {
				errs <- err
				return
			}
			buffer := make([]traceLogRow, 100)
			if err := reader.SeekToRow(1_000); err != nil {
				errs <- err
			} else if n, err := reader.Read(buffer); n != len(buffer) {
				errs <- fmt.Errorf("read %d rows: %v", n, err)
			}
			if err := closeReader(); err != nil {
				errs <- err
			}
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if entry := cache.entries[path]; entry == nil || entry.refs != 0 {
		t.Fatalf("shared entry = %+v", entry)
	}
}
