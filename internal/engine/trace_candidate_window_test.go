package engine

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

// Compare the complete ranking query with raw rows, not merely the candidate
// source: boundary candidates and raw tail overlap and must merge exactly.
func TestCompletedTraceWindowsMatchRawSpans(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	span := func(namespace, service, trace string, start, end int64, failed bool) telemetry.Span {
		status := "STATUS_CODE_OK"
		if failed {
			status = "STATUS_CODE_ERROR"
		}
		return telemetry.Span{Namespace: namespace, ServiceName: service, TraceID: trace,
			SpanID: fmt.Sprintf("%s-%s-%d", namespace, service, start), StartUnixNanos: start,
			EndUnixNanos: end, DurationMS: float64(end-start) / 1e6, StatusCode: status, IngestedAt: 1}
	}
	commit := func(id string, spans ...telemetry.Span) {
		t.Helper()
		for i := range spans {
			if id == "interior" {
				spans[i].IngestedAt = 2
			} else if id == "late" {
				spans[i].IngestedAt = 3
			}
		}
		if err := commitRestoredFixture(repo, t.Context(), telemetrystore.Batch{ID: id, Spans: spans}); err != nil {
			t.Fatal(err)
		}
	}
	// The file straddles both boundaries, but includes an entirely contained
	// trace and an interior part of a trace whose other parts cross the start.
	commit("boundaries",
		span("prod", "api", "contained", 0, 5, false),
		span("prod", "api", "partial", -15, -10, true),
		span("prod", "api", "partial", -5, -1, false),
		span("prod", "api", "mixed", 1, 7, false),
		span("prod", "db", "mixed", -15, 200, true),
		span("alpha", "api", "collision", -3, 4, false),
		span("beta", "api", "collision", 1, 2, true),
		span("prod", "api", "at_start", -10, -8, false),
		span("prod", "api", "at_end", 10, 30, true),
	)
	commit("interior",
		span("prod", "api", "partial", 0, 9, false),
		span("prod", "api", "contained", 3, 7, false),
		span("prod", "db", "mixed", 4, 5, true),
	)
	if _, err := d.RefreshReadCaches(t.Context()); err != nil {
		t.Fatal(err)
	}
	check := func(stage string) {
		t.Helper()
		for _, scope := range [][2]string{{"", ""}, {"prod", ""}, {"prod", "api"}, {"prod", "db"}, {"alpha", "api"}, {"beta", "api"}} {
			for _, bounds := range [][2]int64{{-10, 10}, {-5, 4}, {0, 1}, {-20, 30}} {
				for _, limit := range []int{1, 3, 20} {
					w := queryrows.Window{Start: time.Unix(0, bounds[0]).UTC(), End: time.Unix(0, bounds[1]).UTC(), Namespace: scope[0], Service: scope[1], Kind: queryrows.TraceCandidateRead}
					args := []any{w.Start, w.End, scope[0], scope[0], scope[1], scope[1], limit}
					// Expose the kernel's aggregate bounds as well as its public
					// identity/error projection, without replacing any merge logic.
					query := strings.Replace(queryrows.CompletedTraceQuery, "SELECT namespace,trace_id,has_error FROM candidates", "SELECT namespace,trace_id,has_error,min_start,max_end FROM candidates", 1)
					got := completedTraceWindowRows(t, d, w, query, args)
					w.Kind = queryrows.RawRead
					want := completedTraceWindowRows(t, d, w, rawCompletedTraceWindowQuery, args)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("%s scope=%v bounds=%v limit=%d: cached=%v raw=%v", stage, scope, bounds, limit, got, want)
					}
				}
			}
		}
	}
	check("cached")
	commit("late", span("prod", "api", "partial", -1, 20, true), span("prod", "api", "new", 2, 8, false))
	check("uncached late publication")
	if _, err := d.RefreshReadCaches(t.Context()); err != nil {
		t.Fatal(err)
	}
	check("acknowledged late publication")
	for i := range 8 {
		commit(fmt.Sprintf("filler_%d", i), span("prod", "api", fmt.Sprintf("filler_%d", i), -1, 0, false))
	}
	if _, err := d.RefreshReadCaches(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n, err := repo.CompactParquet(t.Context(), d, 8); err != nil || n == 0 {
		t.Fatalf("compaction inputs=%d error=%v", n, err)
	}
	check("compaction before retired marker cleanup")
	if n, err := repo.PruneParquet(t.Context(), d, 3, 8); err != nil || n == 0 {
		t.Fatalf("retention removed=%d error=%v", n, err)
	}
	check("retention before marker cleanup")
	var surviving int
	if err := d.QueryRowScan(t.Context(), []any{&surviving}, `SELECT count(*) FROM spans WHERE trace_id='new'`); err != nil || surviving != 1 {
		t.Fatalf("late trace must survive retention: rows=%d error=%v", surviving, err)
	}
	if _, err := d.RefreshReadCaches(t.Context()); err != nil {
		t.Fatal(err)
	}
	check("retired marker cleanup")
}

const rawCompletedTraceWindowQuery = `SELECT namespace,trace_id,
 max(CASE WHEN status='STATUS_CODE_ERROR' THEN 1 ELSE 0 END) AS has_error,
 min(start_unix_nano),max(end_unix_nano)
 FROM spans WHERE start_time >= $1::TIMESTAMP_NS::TIMESTAMPTZ_NS
 AND start_time < $2::TIMESTAMP_NS::TIMESTAMPTZ_NS
 AND ($3='' OR namespace=$4) AND ($5='' OR service=$6) AND trace_id<>''
 GROUP BY namespace,trace_id
 ORDER BY has_error DESC,max(end_unix_nano)-min(start_unix_nano) DESC,trace_id,namespace LIMIT $7`

type completedTraceWindowRow struct {
	namespace, trace string
	hasError         int
	minStart, maxEnd int64
}

func completedTraceWindowRows(t testing.TB, d *Duck, w queryrows.Window, sql string, args []any) []completedTraceWindowRow {
	t.Helper()
	rows, err := d.QueryContext(queryrows.WithWindow(t.Context(), w), sql, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []completedTraceWindowRow
	for rows.Next() {
		var row completedTraceWindowRow
		if err := rows.Scan(&row.namespace, &row.trace, &row.hasError, &row.minStart, &row.maxEnd); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
