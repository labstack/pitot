package engine

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// One late span (or one skewed clock) pulls the affected hull back many days.
// The span path already filters its scan and aggregation to the affected keys,
// but the log and metric aggregates used to run over the whole hull with no
// key filter: a single old bucket then forced a hash aggregate over every
// service-minute of dense history, which blew memory and froze the watermark.
// This pins that the log/metric aggregates stay bounded to the affected keys.
func TestServiceLateSignalLogMetricMemory(t *testing.T) {
	if !rollupMemoryProcess(t) {
		return
	}
	db := openTestDuck(t)
	if err := CreateTables(db); err != nil {
		t.Fatal(err)
	}
	if err := CreateViews(db); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	logSchema, logData := filepath.Join(dir, "log_schema.parquet"), filepath.Join(dir, "log_data.parquet")
	metricSchema, metricData := filepath.Join(dir, "metric_schema.parquet"), filepath.Join(dir, "metric_data.parquet")

	// A dense 20-day hull: one log and one metric for every service-minute. The
	// affected set is five recent service-minutes plus one 20-day-old bucket,
	// which is all the pass should aggregate.
	const minutes, services, oldMinute = 28800, 40, 0
	recent := minutes - 1
	affected := fmt.Sprintf(`SELECT 'ns' AS namespace,make_timestamp_ns(%d::BIGINT*60000000000)::TIMESTAMP_NS AS bucket,'svc-'||i::VARCHAR AS service FROM range(5) t(i)
    UNION ALL SELECT 'ns',make_timestamp_ns(%d::BIGINT*60000000000)::TIMESTAMP_NS,'svc-0'`, recent, oldMinute)

	for _, q := range []string{
		"SET memory_limit='2GB'", "SET threads=4",
		fmt.Sprintf(`CREATE VIEW log_fixture AS SELECT 'ns' AS namespace,'svc-'||(i%%%d)::VARCHAR AS service,
    make_timestamp_ns((i//%d)::BIGINT*60000000000)::TIMESTAMPTZ_NS AS time
    FROM range(%d) t(i)`, services, services, minutes*services),
		fmt.Sprintf(`CREATE VIEW metric_fixture AS SELECT 'ns' AS namespace,'svc-'||(i%%%d)::VARCHAR AS service,
    make_timestamp_ns((i//%d)::BIGINT*60000000000)::TIMESTAMPTZ_NS AS time,'m-0' AS name
    FROM range(%d) t(i)`, services, services, minutes*services),
		"COPY (SELECT * FROM log_fixture WHERE false) TO " + sqlLiteral(logSchema) + " (FORMAT PARQUET)",
		"COPY log_fixture TO " + sqlLiteral(logData) + " (FORMAT PARQUET)",
		"COPY (SELECT * FROM metric_fixture WHERE false) TO " + sqlLiteral(metricSchema) + " (FORMAT PARQUET)",
		"COPY metric_fixture TO " + sqlLiteral(metricData) + " (FORMAT PARQUET)",
		"CREATE OR REPLACE VIEW logs AS SELECT * FROM read_parquet([" + sqlLiteral(logSchema) + "," + sqlLiteral(logData) + "],union_by_name=false,hive_partitioning=false)",
		"CREATE OR REPLACE VIEW metrics AS SELECT * FROM read_parquet([" + sqlLiteral(metricSchema) + "," + sqlLiteral(metricData) + "],union_by_name=false,hive_partitioning=false)",
		"CREATE TEMP TABLE service_pass_affected AS " + affected,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}

	before := peakRSS(t)
	started := time.Now()
	lo, hi := time.Unix(0, 0).UTC(), time.Unix(int64(minutes*60), 0).UTC()
	if _, err := insertServiceRollup(t.Context(), db, lo, hi); err != nil {
		t.Fatal(err)
	}
	growth := peakRSS(t) - before
	t.Logf("service log/metric rollup: %s, peak RSS growth %.1f MiB", time.Since(started), float64(growth)/(1<<20))
	if growth > 96<<20 {
		t.Errorf("log/metric aggregates over %d service-minutes grew RSS by %d, want <=96 MiB; a late bucket must not force a full-hull aggregate", minutes*services, growth)
	}

	// Only the six affected service-minutes are summarized, each with its one
	// log and one metric. The dense history behind the hull is never counted.
	var rows, logCount, metricCount int
	if err := db.QueryRow("SELECT count(*),sum(log_count),sum(metric_count) FROM service_rollup").Scan(&rows, &logCount, &metricCount); err != nil {
		t.Fatal(err)
	}
	if rows != 6 || logCount != 6 || metricCount != 6 {
		t.Fatalf("service_rollup rows=%d log_count=%d metric_count=%d, want 6/6/6", rows, logCount, metricCount)
	}
}
