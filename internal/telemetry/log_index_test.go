package telemetry

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/zeebo/xxh3"
)

func writeTestLogs(t *testing.T, dir string, rows []Log) string {
	t.Helper()
	path := filepath.Join(dir, "logs.parquet")
	if err := writeParquet(path, Logs, rows, parquetPageSize); err != nil {
		t.Fatal(err)
	}
	return path
}

func randomTraceLogs(r *rand.Rand, n int) []Log {
	rows := make([]Log, n)
	for i := range rows {
		trace := ""
		if r.IntN(5) > 0 {
			trace = fmt.Sprintf("%032x", r.IntN(n/3+1))
		}
		rows[i] = Log{Namespace: "prod", ServiceName: fmt.Sprint("s", r.IntN(3)), TraceID: trace, TimeUnixNanos: int64(1 + r.IntN(1_000_000)), Body: "b"}
	}
	return rows
}

// Hash-range passes must write the same bytes as one in-memory sort, and
// every probe must return exactly the rows whose trace ID hashes to it.
func TestLogIndexPassesMatchOneSort(t *testing.T) {
	dir := t.TempDir()
	rows := randomTraceLogs(rand.New(rand.NewPCG(1, 2)), 5_000)
	logs := writeTestLogs(t, dir, rows)
	one, passes := filepath.Join(dir, "one.fidx"), filepath.Join(dir, "passes.fidx")
	if err := buildLogIndex(t.Context(), logs, one, len(rows), len(rows)); err != nil {
		t.Fatal(err)
	}
	if err := buildLogIndex(t.Context(), logs, passes, len(rows), 300); err != nil {
		t.Fatal(err)
	}
	a, _ := os.ReadFile(one)
	b, _ := os.ReadFile(passes)
	if !bytes.Equal(a, b) {
		t.Fatal("multi-pass log index differs from a single sort")
	}
	if _, err := os.Stat(passes + ".postings"); !os.IsNotExist(err) {
		t.Fatalf("postings spool left behind: %v", err)
	}
	index, err := loadLogIndex(one, len(rows))
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyLogIndex(t.Context(), logs, len(rows), index); err != nil {
		t.Fatal(err)
	}
	// Decode the written order, which differs from the input order.
	want := map[uint64][]uint32{}
	if err := scanLogTraceHashes(t.Context(), logs, len(rows), func(row uint32, hash uint64) { want[hash] = append(want[hash], row) }); err != nil {
		t.Fatal(err)
	}
	if uint64(len(want)) != index.entries || len(index.fences) != int((index.entries+indexBlockEntries-1)/indexBlockEntries) {
		t.Fatalf("entries=%d fences=%d distinct=%d", index.entries, len(index.fences), len(want))
	}
	for hash, rows := range want {
		got, err := index.Lookup(hash)
		if err != nil || !slices.Equal(got, rows) {
			t.Fatalf("lookup %x = %v, %v; want %v", hash, got, err, rows)
		}
	}
	for _, hash := range []uint64{0, xxh3.HashString("absent"), ^uint64(0)} {
		if got, err := index.Lookup(hash); err != nil || got != nil {
			t.Fatalf("absent lookup = %v, %v", got, err)
		}
	}
}

// Offline verification must reject an index that loads cleanly but misplaces
// a single posting.
func TestLogIndexVerificationRejectsWrongPostings(t *testing.T) {
	dir := t.TempDir()
	rows := randomTraceLogs(rand.New(rand.NewPCG(3, 4)), 600)
	logs := writeTestLogs(t, dir, rows)
	var pairs []logPosting
	if err := scanLogTraceHashes(t.Context(), logs, len(rows), func(row uint32, hash uint64) { pairs = append(pairs, logPosting{hash, row}) }); err != nil {
		t.Fatal(err)
	}
	sortLogPostings(pairs)
	// Two distinct hashes whose postings the cases rearrange.
	first, second := pairs[0], pairs[len(pairs)-1]
	cases := map[string]func([]logPosting) []logPosting{
		"missing": func(p []logPosting) []logPosting { return p[1:] },
		"duplicated": func(p []logPosting) []logPosting {
			return append(p, logPosting{second.hash + 1, first.row})
		},
		"misattributed": func(p []logPosting) []logPosting {
			p[0].hash = second.hash
			return p
		},
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			crafted := edit(slices.Clone(pairs))
			sortLogPostings(crafted)
			path := filepath.Join(t.TempDir(), "logs.fidx")
			writer, err := newLogIndexWriter(path, false)
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.Pass(crafted); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			index, err := loadLogIndex(path, len(rows))
			if err != nil {
				t.Fatal(err)
			}
			if err := verifyLogIndex(t.Context(), logs, len(rows), index); err == nil {
				t.Fatal("verification accepted a wrong posting")
			}
		})
	}
}

func TestLogIndexRejectsRowsBeyondU32(t *testing.T) {
	if err := buildLogIndex(t.Context(), "unused", filepath.Join(t.TempDir(), "logs.fidx"), 1<<32, logIndexPassRows); err == nil {
		t.Fatal("log index accepted a file beyond u32 rows")
	}
}

// A hash shared by two trace IDs posts both traces' rows under one entry. The
// read must keep only the exact trace ID; verification, which rebuilds the
// index from real hashes, must reject the crafted collision.
func TestTraceLogsResolveHashCollisionsByTraceID(t *testing.T) {
	root := t.TempDir()
	store, err := OpenParquetStore(root)
	if err != nil {
		t.Fatal(err)
	}
	var logs []Log
	for i := range 20 {
		trace := []string{"x", "y", ""}[i%3]
		logs = append(logs, Log{Namespace: "prod", ServiceName: "svc", TraceID: trace, SpanID: fmt.Sprint(i), Body: fmt.Sprint("body ", i), TimeUnixNanos: int64(100 + i)})
	}
	if err := store.CommitBatch(t.Context(), &BatchMetadata{ID: "collide"}, nil, logs, nil); err != nil {
		t.Fatal(err)
	}
	dir := store.BatchPath("collide")
	// Post every traced row under x's hash.
	hash := xxh3.HashString("x")
	var pairs []logPosting
	if err := scanLogTraceHashes(t.Context(), filepath.Join(dir, "logs.parquet"), len(logs), func(row uint32, _ uint64) { pairs = append(pairs, logPosting{hash, row}) }); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "logs.fidx")); err != nil {
		t.Fatal(err)
	}
	writer, err := newLogIndexWriter(filepath.Join(dir, "logs.fidx"), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Pass(pairs); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenParquetStore(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.TraceLogs(t.Context(), TraceQuery{TraceID: "x", StartNanos: 0, EndNanos: 1000, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 7 {
		t.Fatalf("x logs = %d, want 7", len(got))
	}
	for i, log := range got {
		if log.TraceID != "x" || (i > 0 && log.TimeUnixNanos <= got[i-1].TimeUnixNanos) {
			t.Fatalf("collision read %+v", got)
		}
	}
	if err := ValidatePublishedBatch(dir); !IsBatchCorrupt(err) {
		t.Fatalf("verification of a crafted collision: %v", err)
	}
}

// A canceled build stops at the next page instead of scanning the file.
func TestLogIndexBuildHonoursCancellation(t *testing.T) {
	dir := t.TempDir()
	rows := randomTraceLogs(newRand(3), 1_000)
	logs := writeTestLogs(t, dir, rows)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	path := filepath.Join(dir, "logs.fidx")
	if err := buildLogIndex(ctx, logs, path, len(rows), logIndexPassRows); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled build: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("canceled build left its index: %v", err)
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

// Short index content is corrupt; a failing read is operational and must not
// authorize quarantine.
func TestIndexReadErrorsKeepOperationalFailures(t *testing.T) {
	buf := make([]byte, 16)
	if err := readIndexBytes(bytes.NewReader(buf[:5]), buf, "log index entry"); err == nil || !IsBatchCorrupt(batchDataError(err)) {
		t.Fatalf("short index read: %v", err)
	}
	if err := readIndexBytes(bytes.NewReader(nil), buf, "log index header"); err == nil || !IsBatchCorrupt(batchDataError(err)) {
		t.Fatalf("empty index read: %v", err)
	}
	eio := &os.PathError{Op: "read", Path: "logs.fidx", Err: syscall.EIO}
	err := readIndexBytes(failingReader{eio}, buf, "log index header")
	if !errors.Is(err, syscall.EIO) || IsBatchCorrupt(batchDataError(err)) {
		t.Fatalf("failing index read: %v", err)
	}
	if err := readIndexBytes(bufio.NewReader(failingReader{eio}), buf, "log index entry"); IsBatchCorrupt(batchDataError(err)) {
		t.Fatalf("buffered failing index read: %v", err)
	}
}
