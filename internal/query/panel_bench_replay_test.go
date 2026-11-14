//go:build readbench

package query

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
	"github.com/parquet-go/parquet-go"
)

func panelReplayInventory(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	out := map[string][32]byte{}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out[relative] = sha256.Sum256(raw)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPanelBenchmarkReplayRejectsOldAndFutureFormatsWithoutMutation(t *testing.T) {
	for _, version := range []int{0, 2, 4, 6} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "parquet", "batches", "bad.batch")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "metadata.json"), []byte(fmt.Sprintf(`{"version":%d,"id":"bad","spans":1}`, version)), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "spans.parquet"), []byte("untouched unsupported data"), 0o644); err != nil {
				t.Fatal(err)
			}
			before := panelReplayInventory(t, root)
			if _, err := panelReplayMetadata(root); !errors.Is(err, telemetry.ErrUnsupportedBatchFormat) {
				t.Fatalf("error=%v", err)
			}
			if !reflect.DeepEqual(before, panelReplayInventory(t, root)) {
				t.Fatal("unsupported input changed")
			}
		})
	}
}

func TestPanelBenchmarkReplayClippedSpillsEpochAndLists(t *testing.T) {
	root := t.TempDir()
	repo, err := telemetrystore.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	base := int64(time.Hour)
	batch := telemetrystore.Batch{ID: "source"}
	for i, at := range []int64{base - 1, base, base + 1, base + 2} {
		var version any = map[string]any{"unconvertible": true}
		var status any = "not an integer"
		if i == 2 {
			version, status = nil, nil
		}
		resource := map[string]any{"service.version": version}
		attrs := map[string]any{"http.response.status_code": status, "literal.dot": int64(7), "nested": map[string]any{"flag": true, "bytes": []byte{0, 255}}}
		batch.Spans = append(batch.Spans, telemetry.Span{ServiceName: "checkout", TraceID: fmt.Sprintf("trace%d", i), SpanID: fmt.Sprintf("span%d", i), ParentSpanID: "parent", StartUnixNanos: at, EndUnixNanos: at, IngestedAt: at, Resource: resource, Attributes: attrs,
			Events: []telemetry.Event{{TimeUnixNanos: at, Name: "event", Attributes: map[string]any{"a": int64(9)}}},
			Links:  []telemetry.Link{{TraceID: "linked", SpanID: "linkedspan", Attributes: map[string]any{"b": true}}}})
		batch.Logs = append(batch.Logs, telemetry.Log{ServiceName: "checkout", TraceID: fmt.Sprintf("trace%d", i), SpanID: fmt.Sprintf("span%d", i), EventUnixNanos: at, TimeUnixNanos: at, IngestedAt: at, Resource: resource, Attributes: attrs, Body: "complete"})
		batch.Metrics = append(batch.Metrics, telemetry.Metric{ServiceName: "checkout", Name: "histogram", Type: "histogram", Temporality: "cumulative", EventUnixNanos: at, TimeUnixNanos: at, IngestedAt: at, Resource: resource, Attributes: attrs,
			Histogram: telemetry.Histogram{Count: 3, Bounds: []float64{1, 2}, Counts: []uint64{1, 1, 1}},
			Exemplars: []telemetry.Exemplar{{TimeUnixNanos: at, TraceID: "trace", SpanID: "span", Value: 2, Attributes: map[string]any{"c": int64(3)}}}})
	}
	if err := repo.Commit(t.Context(), batch); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(root, "parquet", "batches", "source.batch")
	if err := telemetry.ValidatePublishedBatch(src); err != nil {
		t.Fatal(err)
	}
	before := panelReplayInventory(t, root)
	dst := filepath.Join(t.TempDir(), "shifted.batch")
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	metadata := telemetry.BatchMetadata{Version: panelBenchFormat, ID: "shifted"}
	for _, signal := range []string{"spans", "logs", "metrics"} {
		if err := tilePanelParquet(filepath.Join(src, signal+".parquet"), filepath.Join(dst, signal+".parquet"), signal, "copy_", time.Unix(0, base), time.Unix(0, base+2), -time.Hour, &metadata, map[string]bool{}, map[string]bool{}); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(filepath.Join(dst, signal+".parquet"))
		if err != nil {
			t.Fatal(err)
		}
		reader := parquet.NewReader(f)
		if got, ok := reader.File().Lookup("pitot.spill.service.version"); !ok || got != "1" {
			t.Fatalf("%s clipped resource spills=%q exists=%t", signal, got, ok)
		}
		if signal == "spans" {
			if got, ok := reader.File().Lookup("pitot.spill.http.response.status_code"); !ok || got != "1" {
				t.Fatalf("attribute spills=%q exists=%t", got, ok)
			}
		}
		if err := errors.Join(reader.Close(), f.Close()); err != nil {
			t.Fatal(err)
		}
	}
	if err := panelTraceIndex(dst); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "metadata.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if metadata.Spans != 2 || metadata.Logs != 2 || metadata.Metrics != 2 || metadata.MinSpanStartNanos != 0 || metadata.MaxSpanStartNanos != 1 {
		t.Fatalf("metadata=%+v", metadata)
	}
	// Native verification checks every physical column, dedicated coercion, list
	// lengths, complete sort tuples and exact trace-index ranges after rehashing.
	if err := telemetry.ValidatePublishedBatch(dst); err != nil {
		t.Fatal(err)
	}
	// Starting a second tile at epoch zero catches required raw timestamps
	// incorrectly treated as absent optional zero values.
	signed := filepath.Join(t.TempDir(), "negative.batch")
	if err := os.MkdirAll(signed, 0o755); err != nil {
		t.Fatal(err)
	}
	negative := telemetry.BatchMetadata{Version: panelBenchFormat, ID: "negative"}
	for _, signal := range []string{"spans", "logs", "metrics"} {
		if err := tilePanelParquet(filepath.Join(dst, signal+".parquet"), filepath.Join(signed, signal+".parquet"), signal, "again_", time.Unix(0, 0), time.Unix(0, 2), -time.Hour, &negative, map[string]bool{}, map[string]bool{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := panelTraceIndex(signed); err != nil {
		t.Fatal(err)
	}
	negativeJSON, err := json.Marshal(negative)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(signed, "metadata.json"), negativeJSON, 0o644); err != nil {
		t.Fatal(err)
	}
	if negative.MinSpanStartNanos != -base || negative.MaxSpanStartNanos != -base+1 {
		t.Fatalf("negative metadata=%+v", negative)
	}
	if err := telemetry.ValidatePublishedBatch(signed); err != nil {
		t.Fatal(err)
	}
	// Use a private engine directory to inspect payloads and exact epoch boundaries.
	state := t.TempDir()
	target := filepath.Join(state, "telemetry", "parquet", "batches", "shifted.batch")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dst, target); err != nil {
		t.Fatal(err)
	}
	negativeTarget := filepath.Join(state, "telemetry", "parquet", "batches", "negative.batch")
	if err := os.Rename(signed, negativeTarget); err != nil {
		t.Fatal(err)
	}
	engine, _ := panelBenchEngine(t, panelBenchData{Dir: state})
	var count int
	if err := engine.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM read_parquet(?,hive_partitioning=false) WHERE epoch_ns(start_time)=start_unix_nano AND epoch_ns(end_time)=end_unix_nano AND start_unix_nano IN (0,1) AND event_times[1]=start_time AND link_trace_ids[1]='copy_linked' AND link_span_ids[1]='copy_linkedspan' AND attributes['literal.dot']::BIGINT=7 AND attributes['nested']['flag']::BOOLEAN AND attributes['nested']['bytes']::BLOB=from_hex('00ff') AND link_attributes[1]['b']::BOOLEAN AND event_attributes[1]['a']::BIGINT=9`, filepath.Join(target, "spans.parquet")).Scan(&count); err != nil || count != 2 {
		t.Fatalf("span payload count=%d err=%v", count, err)
	}
	if err := engine.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM read_parquet(?,hive_partitioning=false) WHERE epoch_ns(exemplar_times[1])=epoch_ns(metric_time) AND exemplar_trace_ids[1]='copy_trace' AND exemplar_span_ids[1]='copy_span' AND hist_counts=[1,1,1]::UBIGINT[] AND hist_bounds=[1,2]::DOUBLE[] AND start_time_unix_nano=0 AND exemplar_attributes[1]['c']::BIGINT=3`, filepath.Join(target, "metrics.parquet")).Scan(&count); err != nil || count != 2 {
		t.Fatalf("metric payload count=%d err=%v", count, err)
	}
	if err := engine.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM read_parquet(?,hive_partitioning=false) WHERE epoch_ns(start_time)=start_unix_nano AND epoch_ns(end_time)=end_unix_nano AND start_unix_nano IN (?,?) AND epoch_ns(event_times[1])=start_unix_nano`, filepath.Join(negativeTarget, "spans.parquet"), -base, -base+1).Scan(&count); err != nil || count != 2 {
		t.Fatalf("negative required timestamps count=%d err=%v", count, err)
	}
	if err := engine.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM read_parquet(?,hive_partitioning=false) WHERE start_time_unix_nano=0 AND epoch_ns(exemplar_times[1])=epoch_ns(metric_time)`, filepath.Join(negativeTarget, "metrics.parquet")).Scan(&count); err != nil || count != 2 {
		t.Fatalf("optional zero starts count=%d err=%v", count, err)
	}
	if !reflect.DeepEqual(before, panelReplayInventory(t, root)) {
		t.Fatal("source changed")
	}
}

func TestPanelBenchmarkReplayRejectsLifecycleMarkersWithoutMutation(t *testing.T) {
	for _, marker := range []string{"COMPACTION.json"} {
		t.Run(filepath.Base(marker), func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "parquet", "batches", "old.batch")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "metadata.json"), []byte(`{"version":5,"id":"old","spans":1}`), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "spans.parquet"), []byte("preserve retired input"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, marker), []byte(`["old"]`), 0o644); err != nil {
				t.Fatal(err)
			}
			before := panelReplayInventory(t, root)
			if _, err := panelReplayMetadata(root); err == nil {
				t.Fatal("marked capture accepted; retired data could be duplicated")
			}
			if !reflect.DeepEqual(before, panelReplayInventory(t, root)) {
				t.Fatal("marked source changed")
			}
		})
	}
}

func TestPanelBenchmarkReplayCopiesRetirementMarkersWithoutDuplicates(t *testing.T) {
	root := t.TempDir()
	repo, err := telemetrystore.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	at := fixtureStart.Add(time.Minute).UnixNano()
	for _, id := range []string{"old", "replacement", "pruned"} {
		batch := telemetrystore.Batch{ID: id,
			Spans:   []telemetry.Span{{TraceID: "trace", SpanID: "span", ServiceName: "checkout", StartUnixNanos: at, EndUnixNanos: at + 1}},
			Logs:    []telemetry.Log{{TraceID: "trace", ServiceName: "checkout", EventUnixNanos: at, TimeUnixNanos: at, Body: "retained"}},
			Metrics: []telemetry.Metric{{ServiceName: "checkout", Name: "gauge", Type: "gauge", EventUnixNanos: at, TimeUnixNanos: at, Value: 1}},
		}
		if err := repo.Commit(t.Context(), batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "parquet", "batches", "replacement.batch", "replaces.json"), []byte(`["old"]`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "parquet", "batches", "pruned.batch", "pruned"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	before := panelReplayInventory(t, root)
	copied := copyMetricCrosscheckData(t, root)
	if copied.Spans != 1 || copied.Logs != 1 || copied.Metrics != 1 {
		t.Fatalf("copied retired rows: %+v", copied)
	}
	replayed := preparePanelReplay(t, root, 1)
	if replayed.Spans != 24 || replayed.Logs != 24 || replayed.Metrics != 24 {
		t.Fatalf("replayed retired rows: %+v", replayed)
	}
	if !reflect.DeepEqual(before, panelReplayInventory(t, root)) {
		t.Fatal("source lifecycle files changed")
	}
}
