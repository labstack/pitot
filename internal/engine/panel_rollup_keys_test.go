package engine

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/config"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

var panelRollupKeys = []struct {
	table string
	keys  string
}{
	{"span_rollup", "namespace,bucket,service,kind,status"},
	{"log_rollup", "namespace,bucket,service,severity"},
	{"endpoint_rollup", "namespace,bucket,service,kind,status,route,overflow"},
}

func TestPanelRollupKeysKeepNotNullWithoutIndexes(t *testing.T) {
	d, _ := newBatchCacheTest(t)
	for _, table := range panelRollupKeys {
		t.Run(table.table, func(t *testing.T) {
			var constraints, indexes int
			if err := d.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM duckdb_constraints() WHERE table_name=? AND constraint_type IN ('PRIMARY KEY','UNIQUE')`, table.table).Scan(&constraints); err != nil {
				t.Fatal(err)
			}
			if err := d.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM duckdb_indexes() WHERE table_name=?`, table.table).Scan(&indexes); err != nil {
				t.Fatal(err)
			}
			if constraints != 0 || indexes != 0 {
				t.Errorf("scan-only summary retains key constraints/indexes=%d/%d", constraints, indexes)
			}
			keys := strings.Split(table.keys, ",")
			for nullAt, key := range keys {
				values := make([]any, len(keys))
				for i, name := range keys {
					values[i] = ""
					if name == "bucket" {
						values[i] = time.Now().UTC().Truncate(time.Minute)
					} else if name == "overflow" {
						values[i] = false
					}
				}
				values[nullAt] = nil
				query := "INSERT INTO " + table.table + " (" + table.keys + ") VALUES (" + strings.TrimSuffix(strings.Repeat("?,", len(keys)), ",") + ")"
				if _, err := d.DB.ExecContext(t.Context(), query, values...); err == nil || !strings.Contains(err.Error(), "NOT NULL") || !strings.Contains(err.Error(), table.table+"."+key) {
					t.Errorf("NULL %s did not fail its NOT NULL constraint: %v", key, err)
				}
			}
		})
	}
}

func TestPanelRollupKeysSurviveLateRepairRetryAndRollback(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	at := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	batch := panelRollupKeyBatch("initial", at, 1, true)
	if err := repo.Commit(t.Context(), batch); err != nil {
		t.Fatal(err)
	}
	refreshPanelRollupKeys(t, d)
	// An idempotent publication retry and repeated refresh cannot append a
	// second aggregate for a previously processed complete key.
	if err := repo.Commit(t.Context(), batch); err != nil {
		t.Fatal(err)
	}
	refreshPanelRollupKeys(t, d)
	refreshPanelRollupKeys(t, d)
	for _, table := range panelRollupKeys {
		if _, err := d.DB.ExecContext(t.Context(), "CREATE TABLE before_"+table.table+" AS SELECT * FROM "+table.table); err != nil {
			t.Fatal(err)
		}
	}
	before, err := d.RollupWatermark(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Commit(t.Context(), panelRollupKeyBatch("late", at.Add(20*time.Second), 1, true)); err != nil {
		t.Fatal(err)
	}
	// Endpoint insertion follows the span aggregate insertion. Failure here
	// must roll back the shared transaction, including all earlier deletions.
	if _, err := d.DB.ExecContext(t.Context(), `ALTER TABLE endpoint_rollup ADD COLUMN incompatible BOOLEAN`); err != nil {
		t.Fatal(err)
	}
	if _, err := d.refreshServiceRollup(t.Context()); err == nil {
		t.Fatal("incompatible endpoint insertion unexpectedly committed")
	}
	if _, err := d.DB.ExecContext(t.Context(), `ALTER TABLE endpoint_rollup DROP COLUMN incompatible`); err != nil {
		t.Fatal(err)
	}
	for _, table := range panelRollupKeys {
		var differences int
		query := "SELECT count(*) FROM ((SELECT * FROM " + table.table + " EXCEPT ALL SELECT * FROM before_" + table.table + ") UNION ALL (SELECT * FROM before_" + table.table + " EXCEPT ALL SELECT * FROM " + table.table + "))"
		if err := d.DB.QueryRowContext(t.Context(), query).Scan(&differences); err != nil || differences != 0 {
			t.Fatalf("rollback changed %s: differences=%d err=%v", table.table, differences, err)
		}
	}
	if got, err := d.RollupWatermark(t.Context()); err != nil || !got.Equal(before) {
		t.Fatalf("rollback changed watermark: %v -> %v: %v", before, got, err)
	}
	refreshPanelRollupKeys(t, d)
	refreshPanelRollupKeys(t, d)
	var collisionGroups int
	if err := d.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM endpoint_rollup WHERE route='Other routes (overflow)'`).Scan(&collisionGroups); err != nil || collisionGroups != 2 {
		t.Fatalf("literal route and overflow groups=%d want2: %v", collisionGroups, err)
	}
}

func TestPanelRollupKeysStayUniqueAcrossAffectedChunks(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	at := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	// Each service-minute belongs to one chunk even though every service has
	// multiple observations. Cross the actual production key-chunk boundary.
	for _, id := range []string{"first", "late"} {
		if err := repo.Commit(t.Context(), panelRollupKeyBatch(id, at, affectedKeyChunkSize+1, false)); err != nil {
			t.Fatal(err)
		}
		refreshPanelRollupKeys(t, d)
	}
	refreshPanelRollupKeys(t, d)
}

func TestPanelRollupKeysUpgradePreviousCacheOnReopen(t *testing.T) {
	cfg := config.Config{DataDir: t.TempDir(), DuckDBMemory: "256MB", DuckDBThreads: 2, DuckDBMaxConns: 2}
	repo, err := telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDuck(t.Context(), cfg, repo)
	if err != nil {
		_ = repo.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if d != nil {
			_ = d.Close()
		}
		if repo != nil {
			_ = repo.Close()
		}
	})
	at := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	batch := panelRollupKeyBatch("previous-cache", at, 2, false)
	if err := repo.Commit(t.Context(), batch); err != nil {
		t.Fatal(err)
	}
	refreshPanelRollupKeys(t, d)
	if _, err := d.RefreshReadCaches(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := assertCommittedWatermarkParity(t, d); got.IsZero() {
		t.Fatal("fixture did not warm analytical progress")
	}
	metadata := repo.Parquet.BatchMetadata()
	for _, table := range panelRollupKeys {
		// Recreate the exact previous physical key design, rather than only
		// changing a version number on an already unindexed cache.
		if _, err := d.DB.ExecContext(t.Context(), "ALTER TABLE "+table.table+" ADD PRIMARY KEY ("+table.keys+")"); err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []string{
		`UPDATE read_cache_version SET version=18`,
		`UPDATE rollup_schema_version SET version=4`,
		`INSERT INTO anomaly_log VALUES ('upgrade','svc','latency',now(),now(),'witness','preserve me')`,
		`INSERT INTO read_edge_buckets VALUES ('upgrade',now(),10)`,
		`INSERT INTO edge_rollup (namespace,bucket,caller,callee,calls) VALUES ('upgrade',now(),'a','b',1)`,
		`INSERT INTO version_rollup VALUES ('upgrade','svc','v1',now(),now())`,
		`INSERT INTO version_rollup_batches VALUES ('previous-cache',1)`,
		`INSERT INTO rollup_state VALUES ('edge_rollup_v2',1,now()),('edge_rollup_v2_substart',1,now()),('edge_rollup_v2_subwindow_end',2,now())`,
	} {
		if _, err := d.DB.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	var markers int
	if err := d.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM read_batches`).Scan(&markers); err != nil || markers != 1 {
		t.Fatalf("fixture markers=%d: %v", markers, err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	repo = nil
	repo, err = telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err = NewDuck(t.Context(), cfg, repo)
	if err != nil {
		t.Fatal(err)
	}
	for table, want := range map[string]int{"read_cache_version": 19, "rollup_schema_version": 5} {
		var got int
		if err := d.DB.QueryRowContext(t.Context(), "SELECT version FROM "+table).Scan(&got); err != nil || got != want {
			t.Fatalf("reopened %s=%d want=%d: %v", table, got, want, err)
		}
	}
	for _, table := range []string{"span_rollup", "log_rollup", "endpoint_rollup", "service_rollup", "edge_rollup", "receiver_rollup", "resource_rollup", "receiver_entity_rollup", "rollup_state", "version_rollup", "version_rollup_batches", "read_batches", "read_trace_parts", "read_trace_candidates", "read_resource_inventory", "read_resource_seconds", "read_metric_names", "read_edge_buckets"} {
		var count int
		if err := d.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("previous %s survived rebuild: count=%d: %v", table, count, err)
		}
	}
	if got := assertCommittedWatermarkParity(t, d); !got.IsZero() {
		t.Fatalf("previous in-memory progress survived reopen: %v", got)
	}
	var anomalies int
	if err := d.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM anomaly_log WHERE namespace='upgrade' AND service='svc'`).Scan(&anomalies); err != nil || anomalies != 1 {
		t.Fatalf("anomaly witness lost: count=%d: %v", anomalies, err)
	}
	if got := repo.Parquet.BatchMetadata(); !reflect.DeepEqual(got, metadata) {
		t.Fatal("cache upgrade changed canonical batch metadata")
	}
	var spans, logs int
	if err := d.QueryRowScan(t.Context(), []any{&spans, &logs}, `SELECT (SELECT count(*) FROM spans),(SELECT count(*) FROM logs)`); err != nil || spans != len(batch.Spans) || logs != len(batch.Logs) {
		t.Fatalf("canonical rows after reopen=%d/%d: %v", spans, logs, err)
	}
	for _, table := range panelRollupKeys {
		var indexes int
		if err := d.DB.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM duckdb_constraints() WHERE table_name=? AND constraint_type IN ('PRIMARY KEY','UNIQUE'))+(SELECT count(*) FROM duckdb_indexes() WHERE table_name=?)`, table.table, table.table).Scan(&indexes); err != nil || indexes != 0 {
			t.Fatalf("reopened %s key indexes=%d: %v", table.table, indexes, err)
		}
		for _, key := range strings.Split(table.keys, ",") {
			var required int
			if err := d.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM duckdb_constraints() WHERE table_name=? AND constraint_type='NOT NULL' AND list_contains(constraint_column_names,?)`, table.table, key).Scan(&required); err != nil || required != 1 {
				t.Fatalf("reopened %s.%s NOT NULL constraints=%d: %v", table.table, key, required, err)
			}
		}
	}
	refreshPanelRollupKeys(t, d)
	if _, backlog, err := d.refreshReadCaches(t.Context()); err != nil || backlog {
		t.Fatalf("rebuilt cache failed to converge: backlog=%v err=%v", backlog, err)
	}
	if err := d.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM read_batches WHERE batch_id='previous-cache'`).Scan(&markers); err != nil || markers != 1 {
		t.Fatalf("rebuilt markers=%d: %v", markers, err)
	}
	var candidates int
	if err := d.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM read_trace_candidates WHERE min_start=?`, at.UnixNano()).Scan(&candidates); err != nil || candidates != len(batch.Spans) {
		t.Fatalf("rebuilt exact candidates=%d: %v", candidates, err)
	}
	if got := assertCommittedWatermarkParity(t, d); got.IsZero() {
		t.Fatal("rebuilt analytical progress did not advance")
	}
}

func panelRollupKeyBatch(id string, at time.Time, services int, overflow bool) telemetrystore.Batch {
	batch := telemetrystore.Batch{ID: id}
	for service := range services {
		routes := []string{"/a", "/b"}
		if overflow {
			routes = []string{"Other routes (overflow)", "Other routes (overflow)", "Other routes (overflow)"}
			for i := range endpointRouteLimit + 2 {
				routes = append(routes, fmt.Sprintf("/route/%03d", i))
			}
		}
		for i, route := range routes {
			ns, svc := "ns'", fmt.Sprintf("svc-%04d", service)
			duration := float64(i % 17)
			batch.Spans = append(batch.Spans, telemetry.Span{Namespace: ns, ServiceName: svc, TraceID: fmt.Sprintf("%s-%d-%d", id, service, i), SpanID: "span", StartUnixNanos: at.UnixNano(), EndUnixNanos: at.Add(time.Duration(duration) * time.Millisecond).UnixNano(), DurationMS: duration, Kind: "SPAN_KIND_SERVER", StatusCode: "STATUS_CODE_OK", Attributes: map[string]any{"http.route": route}})
			severity := "INFO"
			if i%2 == 0 {
				severity = "ERROR"
			}
			batch.Logs = append(batch.Logs, telemetry.Log{Namespace: ns, ServiceName: svc, TimeUnixNanos: at.UnixNano(), Severity: severity, Body: id})
		}
	}
	return batch
}

func refreshPanelRollupKeys(t *testing.T, d *Duck) {
	t.Helper()
	if _, err := d.refreshServiceRollup(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, table := range panelRollupKeys {
		var duplicates int
		query := "SELECT count(*) FROM (SELECT " + table.keys + " FROM " + table.table + " GROUP BY " + table.keys + " HAVING count(*)<>1)"
		if err := d.DB.QueryRowContext(t.Context(), query).Scan(&duplicates); err != nil || duplicates != 0 {
			t.Fatalf("%s duplicate complete keys=%d: %v", table.table, duplicates, err)
		}
	}
	// Independent raw populations check all keys, counts, duration statistics
	// and every histogram bin, not only the number of persisted aggregates.
	hist := `histogram(CASE WHEN duration_ms>0 THEN coalesce(try_cast(ceil(log2(duration_ms)*8) AS INTEGER),2147483647) ELSE -2147483648 END)`
	expected := map[string]string{
		"span_rollup": `SELECT namespace,date_trunc('minute',start_time::TIMESTAMP_NS) bucket,service,coalesce(kind,'') kind,coalesce(status,'') status,count(*) spans,sum(duration_ms) duration_sum,min(duration_ms) duration_min,max(duration_ms) duration_max,` + hist + ` hist FROM spans GROUP BY ALL`,
		"log_rollup":  `SELECT namespace,date_trunc('minute',time::TIMESTAMP_NS) bucket,service,coalesce(severity,'') severity,count(*) logs FROM logs GROUP BY ALL`,
		"endpoint_rollup": fmt.Sprintf(`WITH eligible AS (
	 SELECT namespace,date_trunc('minute',start_time::TIMESTAMP_NS) bucket,service,coalesce(kind,'') kind,coalesce(status,'') status,attributes['http.route']::VARCHAR route,duration_ms FROM spans WHERE attributes['http.route']::VARCHAR<>''
	 ), populations AS (SELECT namespace,bucket,service,route,count(*) n FROM eligible GROUP BY ALL),
	 ranked AS (SELECT *,row_number() OVER(PARTITION BY namespace,bucket,service ORDER BY n DESC,route) rank FROM populations),
	 folded AS (SELECT e.* EXCLUDE(route),CASE WHEN r.rank<=%d THEN e.route ELSE 'Other routes (overflow)' END route,r.rank>%d overflow FROM eligible e JOIN ranked r USING(namespace,bucket,service,route))
	 SELECT namespace,bucket,service,kind,status,route,overflow,count(*) spans,sum(duration_ms) duration_sum,min(duration_ms) duration_min,max(duration_ms) duration_max,%s hist FROM folded GROUP BY ALL`, endpointRouteLimit, endpointRouteLimit, hist),
	}
	for _, table := range panelRollupKeys {
		var differences int
		query := "WITH expected AS (" + expected[table.table] + "), differences AS ((SELECT * FROM expected EXCEPT ALL SELECT * FROM " + table.table + ") UNION ALL (SELECT * FROM " + table.table + " EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM differences"
		if err := d.QueryRowScan(t.Context(), []any{&differences}, query); err != nil || differences != 0 {
			t.Fatalf("%s raw/summary differences=%d: %v", table.table, differences, err)
		}
	}
}
