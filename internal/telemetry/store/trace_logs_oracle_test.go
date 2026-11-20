package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/telemetry"
	"github.com/parquet-go/parquet-go"
)

type oracleLogRow struct {
	Namespace string `parquet:"namespace"`
	Time      int64  `parquet:"log_time,timestamp(nanosecond)"`
	Severity  string `parquet:"severity"`
	Body      string `parquet:"body"`
	Service   string `parquet:"service"`
	TraceID   string `parquet:"trace_id"`
	SpanID    string `parquet:"span_id"`
}

type oracleLog struct {
	log   telemetry.TraceLog
	batch string
	row   int
}

// oracleTraceLogs scans every log row of every batch in the catalog and
// orders the matches by the same total key the index read uses.
func oracleTraceLogs(t *testing.T, repository *Repository, query telemetry.TraceQuery) []telemetry.TraceLog {
	t.Helper()
	var matches []oracleLog
	for _, batch := range repository.Parquet.BatchMetadata() {
		if batch.Logs == 0 {
			continue
		}
		file, err := os.Open(filepath.Join(repository.Parquet.BatchPath(batch.ID), "logs.parquet"))
		if err != nil {
			t.Fatal(err)
		}
		reader := parquet.NewGenericReader[oracleLogRow](file)
		rows := make([]oracleLogRow, batch.Logs)
		if n, err := reader.Read(rows); n != len(rows) || (err != nil && !errors.Is(err, io.EOF)) {
			t.Fatalf("read %d of %d rows: %v", n, len(rows), err)
		}
		_ = reader.Close()
		_ = file.Close()
		for i, row := range rows {
			namespaced := query.ExactNamespace || query.Namespace != ""
			if row.TraceID != query.TraceID || row.Time < query.StartNanos || row.Time >= query.EndNanos || namespaced && row.Namespace != query.Namespace {
				continue
			}
			matches = append(matches, oracleLog{log: telemetry.TraceLog{
				Namespace: row.Namespace, ServiceName: row.Service, TraceID: row.TraceID, SpanID: row.SpanID,
				Severity: row.Severity, Body: row.Body, TimeUnixNanos: row.Time,
			}, batch: batch.ID, row: i})
		}
	}
	slices.SortFunc(matches, func(a, b oracleLog) int {
		if a.log.TimeUnixNanos != b.log.TimeUnixNanos {
			return int(max(-1, min(1, a.log.TimeUnixNanos-b.log.TimeUnixNanos)))
		}
		if c := strings.Compare(a.log.ServiceName, b.log.ServiceName); c != 0 {
			return c
		}
		if c := strings.Compare(a.log.SpanID, b.log.SpanID); c != 0 {
			return c
		}
		if c := strings.Compare(a.batch, b.batch); c != 0 {
			return c
		}
		return a.row - b.row
	})
	out := make([]telemetry.TraceLog, 0, len(matches))
	for _, m := range matches[:min(len(matches), query.Limit)] {
		out = append(out, m.log)
	}
	return out
}

// sameTraceLogs compares the exported fields; the index read's tie-breaking
// batch and row are unexported.
func sameTraceLogs(got, want []telemetry.TraceLog) bool {
	return slices.EqualFunc(got, want, func(a, b telemetry.TraceLog) bool {
		return a.Namespace == b.Namespace && a.ServiceName == b.ServiceName && a.TraceID == b.TraceID &&
			a.SpanID == b.SpanID && a.Severity == b.Severity && a.Body == b.Body && a.TimeUnixNanos == b.TimeUnixNanos
	})
}

// Trace logs read through logs.fidx equal a full scan of every log row under
// the same total order, after commit and after compaction across generations:
// with empty trace IDs, one trace ID in two namespaces, logs-only batches,
// window edges and limits.
func TestTraceLogsMatchAFullScanAcrossCompaction(t *testing.T) {
	ctx := context.Background()
	repository, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	r := rand.New(rand.NewPCG(5, 6))
	traces := []string{"shared", "t1", "t2", "t3", "t4"}
	base := int64(480_000) * int64(time.Hour)
	for b := range 41 {
		batch := Batch{ID: fmt.Sprintf("b%02d", b)}
		// Five ingest hours of an old day: hour merges, then a day merge,
		// give a second generation.
		at := base + int64(b/8)*int64(time.Hour) + int64(b)*int64(time.Second)
		if b == 40 {
			// A batch two days later ages the old day past its hour merges.
			at = base + 48*int64(time.Hour)
		}
		// Every fourth batch carries logs without spans.
		if b%4 != 0 {
			batch.Spans = []telemetry.Span{{Namespace: "a", TraceID: "t1", SpanID: fmt.Sprint("s", b), StartUnixNanos: at, EndUnixNanos: at + 1, IngestedAt: at}}
		}
		for i := range 30 + r.IntN(40) {
			trace := traces[r.IntN(len(traces))]
			if r.IntN(6) == 0 {
				trace = ""
			}
			namespace := []string{"a", "b"}[r.IntN(2)]
			// Few distinct times, services and span IDs force ties that only
			// the batch and row break.
			when := base + int64(r.IntN(50))*int64(time.Millisecond)
			batch.Logs = append(batch.Logs, telemetry.Log{
				Namespace: namespace, ServiceName: fmt.Sprint("svc", r.IntN(2)), TraceID: trace, SpanID: fmt.Sprint("span", r.IntN(3)),
				Severity: "INFO", Body: fmt.Sprintf("%s-%d", batch.ID, i), TimeUnixNanos: when, IngestedAt: at,
			})
		}
		if err := commitRestoredFixture(repository, ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	check := func(stage string) {
		t.Helper()
		windows := [][2]int64{{-1 << 63, 1<<63 - 1}, {base + 10*int64(time.Millisecond), base + 30*int64(time.Millisecond)}}
		for _, trace := range []string{"shared", "t2", "absent"} {
			for _, namespace := range []string{"", "b"} {
				for _, exact := range []bool{false, true} {
					for _, window := range windows {
						for _, limit := range []int{3, 500} {
							query := telemetry.TraceQuery{TraceID: trace, Namespace: namespace, ExactNamespace: exact, StartNanos: window[0], EndNanos: window[1], Limit: limit}
							got, err := repository.Parquet.TraceLogs(ctx, query)
							if err != nil {
								t.Fatal(err)
							}
							if want := oracleTraceLogs(t, repository, query); !sameTraceLogs(got, want) {
								t.Fatalf("%s: %+v\n got %+v\nwant %+v", stage, query, got, want)
							}
						}
					}
				}
			}
		}
	}
	check("committed")
	if got, _ := repository.Parquet.TraceLogs(ctx, telemetry.TraceQuery{TraceID: "shared", StartNanos: -1 << 63, EndNanos: 1<<63 - 1, Limit: 500}); len(got) <= 3 || !slices.ContainsFunc(got, func(l telemetry.TraceLog) bool { return l.Namespace == "a" }) ||
		!slices.ContainsFunc(got, func(l telemetry.TraceLog) bool { return l.Namespace == "b" }) {
		t.Fatal("fixture lacks one trace ID in two namespaces or more rows than the limit")
	}
	compactor := &testParquetCompactor{}
	for range 20 {
		n, err := repository.CompactParquet(ctx, compactor, minCompactionInputs)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		check("compacted")
	}
	generation := uint32(0)
	for _, batch := range repository.Parquet.BatchMetadata() {
		generation = max(generation, batch.Generation)
		if err := telemetry.ValidatePublishedBatch(repository.Parquet.BatchPath(batch.ID)); err != nil {
			t.Fatalf("verify %s: %v", batch.ID, err)
		}
	}
	if generation < 2 {
		t.Fatalf("compaction reached generation %d, want at least 2", generation)
	}
}
