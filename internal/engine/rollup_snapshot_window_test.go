package engine

import (
	"database/sql"
	"math"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/engine/writegate"
	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

func TestRollupSnapshotWindowRepairsOldMinuteFromAllCapturedFiles(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	at := time.Now().UTC().Truncate(time.Minute).Add(-24 * time.Hour)
	commit := func(id string, spanAt, otherAt time.Time, value float64) {
		t.Helper()
		if err := repo.Commit(t.Context(), rollupSnapshotBatch(id, spanAt, otherAt, value)); err != nil {
			t.Fatal(err)
		}
	}
	commit("old", at.Add(10*time.Second), at.Add(10*time.Second), 1)
	commit("unrelated", at.Add(-2*time.Hour), at.Add(-2*time.Hour), 99)
	if _, err := d.refreshServiceRollup(t.Context()); err != nil {
		t.Fatal(err)
	}
	before := d.rollupWatermarks[serviceRollupStateKey]
	// Only the span is in another hour. The new log and receiver point still
	// repair the old minute using their own signal's footer bounds.
	commit("late", at.Add(-time.Hour), at.Add(30*time.Second), 3)
	if _, err := d.refreshServiceRollup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if d.rollupWatermarks[serviceRollupStateKey] <= before {
		t.Fatal("late publication did not advance the analytical watermark")
	}
	var spans, logs, names int
	if err := d.DB.QueryRowContext(t.Context(), `SELECT spans,log_count,metric_count FROM service_rollup WHERE bucket=?::TIMESTAMP_NS AND service='svc'`, at).Scan(&spans, &logs, &names); err != nil || spans != 1 || logs != 2 || names != 1 {
		t.Fatalf("repaired old minute spans/logs/metric names=%d/%d/%d: %v", spans, logs, names, err)
	}
	var first, last, increase, seconds float64
	if err := d.DB.QueryRowContext(t.Context(), `SELECT first_point.value,last_point.value,counter_increase,counter_seconds FROM receiver_rollup WHERE bucket=?::TIMESTAMP_NS`, at).Scan(&first, &last, &increase, &seconds); err != nil || first != 1 || last != 3 || increase != 2 || seconds != 20 {
		t.Fatalf("receiver repair first/last/increase/seconds=%g/%g/%g/%g: %v", first, last, increase, seconds, err)
	}
	var resources, entities int
	if err := d.DB.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM resource_rollup WHERE bucket=?::TIMESTAMP_NS),(SELECT count(*) FROM receiver_entity_rollup WHERE bucket=?::TIMESTAMP_NS)`, at, at).Scan(&resources, &entities); err != nil || resources != 3 || entities != 1 {
		t.Fatalf("repaired resource/entity summaries=%d/%d: %v", resources, entities, err)
	}
}

func TestRollupSnapshotWindowUsesSignalBoundsAndIncludesUnknown(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	lo := time.Date(2026, 1, 1, 12, 0, 0, 7, time.UTC)
	hi := lo.Add(time.Minute)
	for _, point := range []struct {
		id string
		at time.Time
	}{{"before", lo.Add(-time.Nanosecond)}, {"lower", lo}, {"inside", hi.Add(-time.Nanosecond)}, {"upper", hi}, {"unknown", hi.Add(time.Hour)}} {
		if err := repo.Commit(t.Context(), rollupSnapshotBatch(point.id, point.at, point.at, 1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.Commit(t.Context(), rollupSnapshotBatch("disjoint", lo.Add(-time.Hour), lo, 1)); err != nil {
		t.Fatal(err)
	}
	pin := d.pinParquet()
	defer d.unpinParquet(pin)
	captured := repo.Parquet.BatchMetadata()
	for i := range captured {
		if captured[i].ID == "unknown" {
			// Optional footer statistics may be unavailable. Change only this
			// captured descriptor; every underlying format-5 file remains valid.
			captured[i].SpanTime.Known = false
			captured[i].LogTime.Known = false
			captured[i].MetricTime.Known = false
		}
	}
	tx, err := d.writer().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	unbind, err := d.bindRollupSnapshotFiles(t.Context(), tx, captured, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertRollupSnapshotCounts(t, tx, 6, 6, 6)
	window := queryrows.Window{Start: lo, End: hi}
	if _, err := d.bindRollupSnapshotFiles(t.Context(), tx, captured, &window); err != nil {
		t.Fatal(err)
	}
	// Bounds select whole files. Unknown bounds retain their out-of-window
	// row, and SQL predicates still remove it during aggregation.
	assertRollupSnapshotCounts(t, tx, 3, 4, 4)
	var spans, logs, metrics int
	if err := tx.QueryRowContext(t.Context(), `SELECT
	 (SELECT count(*) FROM spans WHERE start_time>=?::TIMESTAMP_NS::TIMESTAMPTZ_NS AND start_time<?::TIMESTAMP_NS::TIMESTAMPTZ_NS),
	 (SELECT count(*) FROM logs WHERE time>=?::TIMESTAMP_NS::TIMESTAMPTZ_NS AND time<?::TIMESTAMP_NS::TIMESTAMPTZ_NS),
	 (SELECT count(*) FROM metrics WHERE time>=?::TIMESTAMP_NS::TIMESTAMPTZ_NS AND time<?::TIMESTAMP_NS::TIMESTAMPTZ_NS)`, lo, hi, lo, hi, lo, hi).Scan(&spans, &logs, &metrics); err != nil || spans != 2 || logs != 3 || metrics != 3 {
		t.Fatalf("exact half-open rows=%d/%d/%d: %v", spans, logs, metrics, err)
	}
	if err := unbind(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestRollupSnapshotWindowReusesCapturedFilesAcrossRetirement(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	at := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	commitCurrentBatches(t, repo, "captured", 8, at)
	unlock := d.writeGate.Lock(writegate.RollupService)
	defer unlock()
	background, err := d.lockBackground(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer background()
	pin := d.pinParquet()
	defer d.unpinParquet(pin)
	captured := repo.Parquet.BatchMetadata()
	tx, err := d.writer().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	unbind, err := d.bindRollupSnapshotFiles(t.Context(), tx, captured, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertRollupSnapshotCounts(t, tx, 8, 8, 8)
	if n, err := repo.CompactParquet(t.Context(), d, 8); err != nil || n != 8 {
		t.Fatalf("compact captured inputs=%d: %v", n, err)
	}
	commitCurrentBatches(t, repo, "new-publication", 1, at)
	if n, err := repo.PruneParquet(t.Context(), d, math.MaxInt64, 16); err != nil || n != 2 {
		t.Fatalf("retire current catalog=%d: %v", n, err)
	}
	if active := repo.Parquet.BatchMetadata(); len(active) != 0 {
		t.Fatalf("active catalog has %d batches", len(active))
	}
	if _, err := repo.CollectRetiredParquet(); err != nil {
		t.Fatal(err)
	}
	window := queryrows.Window{Start: at, End: at.Add(time.Minute)}
	if _, err := d.bindRollupSnapshotFiles(t.Context(), tx, captured, &window); err != nil {
		t.Fatal(err)
	}
	// The captured eight inputs survive both their replacement and retirement.
	// The newer publication and current empty catalog cannot alter this pass.
	assertRollupSnapshotCounts(t, tx, 8, 8, 8)
	if err := unbind(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func rollupSnapshotBatch(id string, spanAt, otherAt time.Time, value float64) telemetrystore.Batch {
	resource := map[string]any{"host.name": "host", "service.version": "1"}
	m := telemetry.Metric{Namespace: "ns", ServiceName: "svc", Name: "system.cpu.time", Type: "counter", Temporality: "cumulative", Unit: "s", TimeUnixNanos: otherAt.UnixNano(), Value: value, Resource: resource, Attributes: map[string]any{"state": "user", "cpu": "0"}}
	m.SeriesID = telemetry.MetricSeriesID(m)
	return telemetrystore.Batch{ID: id,
		Spans: []telemetry.Span{{Namespace: "ns", ServiceName: "svc", TraceID: id, SpanID: id, StartUnixNanos: spanAt.UnixNano(), EndUnixNanos: spanAt.Add(time.Millisecond).UnixNano(), DurationMS: 1, Kind: "SPAN_KIND_SERVER", StatusCode: "STATUS_CODE_OK", Resource: resource}},
		Logs:  []telemetry.Log{{Namespace: "ns", ServiceName: "svc", TimeUnixNanos: otherAt.UnixNano(), Body: id, Resource: resource}}, Metrics: []telemetry.Metric{m}}
}

func assertRollupSnapshotCounts(t *testing.T, tx *sql.Tx, wantSpans, wantLogs, wantMetrics int) {
	t.Helper()
	var spans, logs, metrics int
	if err := tx.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM spans),(SELECT count(*) FROM logs),(SELECT count(*) FROM metrics)`).Scan(&spans, &logs, &metrics); err != nil || spans != wantSpans || logs != wantLogs || metrics != wantMetrics {
		t.Fatalf("bound rows=%d/%d/%d want%d/%d/%d: %v", spans, logs, metrics, wantSpans, wantLogs, wantMetrics, err)
	}
}
