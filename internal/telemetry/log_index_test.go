package telemetry

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
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
	if err := buildLogIndex(logs, one, len(rows), len(rows)); err != nil {
		t.Fatal(err)
	}
	if err := buildLogIndex(logs, passes, len(rows), 300); err != nil {
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
	if err := verifyLogIndex(logs, len(rows), index); err != nil {
		t.Fatal(err)
	}
	// Decode the written order, which differs from the input order.
	want := map[uint64][]uint32{}
	if err := scanLogTraceHashes(logs, len(rows), func(row uint32, hash uint64) { want[hash] = append(want[hash], row) }); err != nil {
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
	if err := scanLogTraceHashes(logs, len(rows), func(row uint32, hash uint64) { pairs = append(pairs, logPosting{hash, row}) }); err != nil {
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
			if err := verifyLogIndex(logs, len(rows), index); err == nil {
				t.Fatal("verification accepted a wrong posting")
			}
		})
	}
}

func TestLogIndexRejectsRowsBeyondU32(t *testing.T) {
	if err := buildLogIndex("unused", filepath.Join(t.TempDir(), "logs.fidx"), 1<<32, logIndexPassRows); err == nil {
		t.Fatal("log index accepted a file beyond u32 rows")
	}
}
