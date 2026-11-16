//go:build filebench

package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/labstack/pitot/internal/config"
	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// A/B for parquet_metadata_cache (#484). The engine passes an explicit per-file
// Parquet list on every statement, and a panel issues several. With the footer
// cache OFF, each statement re-reads every file footer; with it ON, footers are
// cached by path+mtime+size. Both arms read the SAME immutable fixtures and run
// the same explicit-list statement; the only difference is the per-connection
// cache SET. Both engines are opened and warmed up front, then sampled with
// AB/BA alternation so OS page-cache and time drift affect both arms equally.
// This is an engine-internal control, not a product code path and not capacity
// evidence.
//
// go test -tags=filebench ./internal/engine -run '^TestParquetMetadataCacheComparisonBenchmark$' -count=1 -v
func TestParquetMetadataCacheComparisonBenchmark(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	cfg := config.Config{DataDir: t.TempDir()}
	repo, err := telemetrystore.Open(cfg.TelemetryDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()

	base := time.Date(2026, 11, 1, 12, 0, 0, 123, time.UTC)
	w := queryrows.Window{Start: base, End: base.Add(time.Minute)}
	signal, _ := telemetry.LookupSignal("spans")

	published := 0
	for _, files := range []int{128, 512, 1024} {
		for ; published < files; published++ {
			batch := telemetrystore.Batch{ID: fmt.Sprintf("mdc_%05d", published)}
			for row := range 32 {
				service := []string{"api", "worker", "db"}[row%3]
				n := base.Add(time.Duration(row) * time.Millisecond).UnixNano()
				if row == 0 {
					n = w.Start.UnixNano()
				}
				if row == 31 {
					n = w.End.UnixNano() - 1
				}
				status := "STATUS_CODE_OK"
				if row%7 == 0 {
					status = "STATUS_CODE_ERROR"
				}
				span := telemetry.Span{Namespace: "prod", ServiceName: service, TraceID: fmt.Sprintf("mdc_trace_%05d_%02d", published, row/2), SpanID: fmt.Sprint(row), StartUnixNanos: n, EndUnixNanos: n + int64(row+1)*int64(time.Millisecond), DurationMS: float64(row + 1), StatusCode: status, IngestedAt: base.UnixNano() + int64(published)}
				batch.Spans = append(batch.Spans, span)
			}
			if err := repo.Commit(ctx, batch); err != nil {
				t.Fatal(err)
			}
		}
		batches := repo.Parquet.BatchMetadata()
		if len(batches) != files {
			t.Fatalf("captured files=%d expected=%d", len(batches), files)
		}

		sort.Slice(batches, func(i, j int) bool { return batches[i].ID < batches[j].ID })
		paths := make([]string, 0, len(batches))
		for _, b := range batches {
			paths = append(paths, sqlLiteral(filepath.ToSlash(filepath.Join(repo.Parquet.Dir(), "batches", b.ID+".batch", "spans.parquet"))))
		}
		physical := "SELECT " + rawProjection(signal) + " FROM read_parquet([" + joinCSV(paths) + "], union_by_name=false, hive_partitioning=false)"
		statement := `SELECT service,count(*)::BIGINT,count(DISTINCT trace_id)::BIGINT,
 sum(end_unix_nano-start_unix_nano)::BIGINT,
 sum(CASE WHEN status='STATUS_CODE_ERROR' THEN 1 ELSE 0 END)::BIGINT
 FROM (` + cleanSource("spans", physical) + `) WHERE start_time>=?::TIMESTAMP_NS::TIMESTAMPTZ_NS AND start_time<?::TIMESTAMP_NS::TIMESTAMPTZ_NS GROUP BY service ORDER BY service`

		off := newMetadataCacheArm(t, ctx, statement, w, false)
		on := newMetadataCacheArm(t, ctx, statement, w, true)

		// Warm both arms (populates the footer cache for the ON arm, and the OS
		// page cache for both).
		for range 5 {
			off.exec()
			on.exec()
		}

		// AB/BA alternation: each arm sampled 100 times, order flips per sample.
		for sample := range 100 {
			for offset := range 2 {
				if (sample+offset)%2 == 0 {
					off.sample()
				} else {
					on.sample()
				}
			}
		}

		for _, arm := range []*metadataCacheArm{off, on} {
			result := arm.report()
			result["experiment"] = "parquet_metadata_cache"
			result["files"] = files
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			t.Log("RESULT", string(encoded))
			arm.close()
		}
	}
}

func joinCSV(items []string) string {
	out := ""
	for i, s := range items {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}

type mdcConnector struct {
	*duckdb.Connector
	cache bool
}

func (c *mdcConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	ex := conn.(driver.ExecerContext)
	stmts := []string{"LOAD parquet", "SET TimeZone='UTC'"}
	if c.cache {
		stmts = append(stmts, "SET parquet_metadata_cache=true")
	}
	for _, stmt := range stmts {
		if _, err := ex.ExecContext(ctx, stmt, nil); err != nil {
			return nil, errors.Join(err, conn.Close())
		}
	}
	return conn, nil
}

type metadataCacheArm struct {
	t         *testing.T
	ctx       context.Context
	pool      *sql.DB
	conn      *sql.Conn
	tx        *sql.Tx
	statement string
	w         queryrows.Window
	cache     bool
	timings   []float64
}

func newMetadataCacheArm(t *testing.T, ctx context.Context, statement string, w queryrows.Window, cache bool) *metadataCacheArm {
	t.Helper()
	dsn, err := duckDSN(filepath.Join(t.TempDir(), "arm.duckdb"), "4GB", 4)
	if err != nil {
		t.Fatal(err)
	}
	native, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := sql.OpenDB(&mdcConnector{Connector: native, cache: cache})
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	conn, err := pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got bool
	if err := conn.QueryRowContext(ctx, "SELECT current_setting('parquet_metadata_cache')::BOOLEAN").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != cache {
		t.Fatalf("arm cache=%v but connection reports %v", cache, got)
	}
	tx, err := conn.BeginTx(context.WithoutCancel(ctx), nil)
	if err != nil {
		t.Fatal(err)
	}
	return &metadataCacheArm{t: t, ctx: ctx, pool: pool, conn: conn, tx: tx, statement: statement, w: w, cache: cache}
}

func (a *metadataCacheArm) exec() {
	rows, err := a.tx.QueryContext(a.ctx, a.statement, a.w.Start, a.w.End)
	if err != nil {
		a.t.Fatal(err)
	}
	for rows.Next() {
		var s string
		var x, y, z, q int64
		if err := rows.Scan(&s, &x, &y, &z, &q); err != nil {
			a.t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		a.t.Fatal(err)
	}
	rows.Close()
}

func (a *metadataCacheArm) sample() {
	started := time.Now()
	a.exec()
	a.timings = append(a.timings, float64(time.Since(started))/float64(time.Millisecond))
}

func (a *metadataCacheArm) report() map[string]any {
	sort.Float64s(a.timings)
	var sum float64
	for _, v := range a.timings {
		sum += v
	}
	var objectCacheBytes int64
	_ = a.tx.QueryRowContext(a.ctx, "SELECT coalesce(sum(memory_usage_bytes),0)::BIGINT FROM duckdb_memory() WHERE tag='OBJECT_CACHE'").Scan(&objectCacheBytes)
	n := len(a.timings)
	return map[string]any{
		"arm":                map[bool]string{false: "cache_off", true: "cache_on"}[a.cache],
		"cache_enabled":      a.cache,
		"samples":            n,
		"mean_ms":            sum / float64(n),
		"median_ms":          (a.timings[n/2-1] + a.timings[n/2]) / 2,
		"p95_ms":             a.timings[int(float64(n)*0.95)-1],
		"p99_ms":             a.timings[int(float64(n)*0.99)-1],
		"min_ms":             a.timings[0],
		"max_ms":             a.timings[n-1],
		"object_cache_bytes": objectCacheBytes,
		"unit":               "native_execute_scan_same_tx_ab_ba",
	}
}

func (a *metadataCacheArm) close() {
	_ = a.tx.Rollback()
	_ = a.conn.Close()
	_ = a.pool.Close()
}
