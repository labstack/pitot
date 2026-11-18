package engine

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
	telemetrystore "github.com/labstack/pitot/internal/telemetry/store"
)

type skylineCase struct {
	part  int // partition: batch*minutes + minute
	trace string
	span  exemplarSpan
}

// bruteTopTraces ranks traces by their best span among in-window spans.
func bruteTopTraces(spans []skylineCase, in func(skylineCase) bool, limit int) []string {
	best := map[string]skylineCase{}
	for _, s := range spans {
		if !in(s) {
			continue
		}
		if b, ok := best[s.trace]; !ok || skylineBetter(s, b) {
			best[s.trace] = s
		}
	}
	list := make([]skylineCase, 0, len(best))
	for _, s := range best {
		list = append(list, s)
	}
	sort.Slice(list, func(i, j int) bool { return skylineBetter(list[i], list[j]) })
	out := []string{}
	for i := 0; i < len(list) && i < limit; i++ {
		out = append(out, list[i].trace)
	}
	return out
}

func skylineBetter(a, b skylineCase) bool {
	if a.span.err != b.span.err {
		return a.span.err
	}
	if a.span.dur != b.span.dur {
		return a.span.dur > b.span.dur
	}
	return a.trace < b.trace
}

// keepSkyline returns each partition's rows with their flags, ranking traces
// within a partition by their names as the SQL dense rank does.
func keepSkyline(spans []skylineCase) ([]skylineCase, []uint8) {
	parts := map[int][]int{}
	for i, s := range spans {
		parts[s.part] = append(parts[s.part], i)
	}
	var rows []skylineCase
	var flags []uint8
	for _, members := range parts {
		names := map[string]bool{}
		for _, i := range members {
			names[spans[i].trace] = true
		}
		sorted := make([]string, 0, len(names))
		for name := range names {
			sorted = append(sorted, name)
		}
		sort.Strings(sorted)
		rank := map[string]int64{}
		for i, name := range sorted {
			rank[name] = int64(i + 1)
		}
		in := make([]exemplarSpan, len(members))
		for j, i := range members {
			in[j] = spans[i].span
			in[j].trace = rank[spans[i].trace]
		}
		for j, f := range exemplarSkyline(in) {
			if f != 0 {
				rows = append(rows, spans[members[j]])
				flags = append(flags, f)
			}
		}
	}
	return rows, flags
}

// The pure skyline must answer every window exactly from full rows of covered
// minutes, suffix rows of the start minute and prefix rows of the end minute,
// including after compaction ranks the union of its inputs' rows again.
func TestTraceExemplarSkylineMatchesBruteForce(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	const minutes, batches = 4, 3
	for iter := range 200 {
		var all []skylineCase
		perBatch := make([][]skylineCase, batches)
		for b := range batches {
			for range 60 + r.Intn(500) {
				m := r.Intn(minutes)
				s := skylineCase{part: b*minutes + m, trace: fmt.Sprintf("t%02d", r.Intn(30+r.Intn(60)))}
				s.span = exemplarSpan{start: int64(m*60 + r.Intn(60)), err: r.Intn(25) == 0, dur: int64(r.Intn(20))}
				all = append(all, s)
				perBatch[b] = append(perBatch[b], s)
			}
		}
		rows, flags := keepSkyline(all)
		// Compaction: batches 0 and 1 merge; rank the union of their rows.
		var inputs []skylineCase
		for i, row := range rows {
			if row.part/minutes < 2 {
				_ = flags[i]
				row.part = row.part % minutes
				inputs = append(inputs, row)
			}
		}
		merged, mergedFlags := keepSkyline(inputs)
		for i, row := range rows {
			if row.part/minutes == 2 {
				merged = append(merged, row)
				mergedFlags = append(mergedFlags, flags[i])
			}
		}
		for range 30 {
			s0 := int64(r.Intn(minutes * 60))
			e0 := s0 + 1 + int64(r.Intn(minutes*60-int(s0)))
			if r.Intn(4) == 0 {
				s0 -= s0 % 60
			}
			if r.Intn(4) == 0 && e0%60 != 0 && e0+60-e0%60 <= minutes*60 {
				e0 += 60 - e0%60
			}
			if s0/60 == (e0-1)/60 && s0%60 != 0 && e0%60 != 0 {
				continue // one minute reads raw spans
			}
			in := func(s skylineCase) bool { return s.span.start >= s0 && s.span.start < e0 }
			for _, limit := range []int{1, traceExemplarDepth} {
				want := bruteTopTraces(all, in, limit)
				for name, set := range map[string][2]any{"batches": {rows, flags}, "compacted": {merged, mergedFlags}} {
					rs, fs := set[0].([]skylineCase), set[1].([]uint8)
					var read []skylineCase
					for i, row := range rs {
						m := row.span.start / 60 * 60
						switch {
						case m >= s0 && m+60 <= e0:
							if fs[i]&exemplarFull != 0 {
								read = append(read, row)
							}
						case m < s0 && m+60 > s0:
							if fs[i]&exemplarSuffix != 0 && in(row) {
								read = append(read, row)
							}
						case m < e0 && m+60 > e0:
							if fs[i]&exemplarPrefix != 0 && in(row) {
								read = append(read, row)
							}
						}
					}
					if got := bruteTopTraces(read, in, limit); !reflect.DeepEqual(got, want) {
						t.Fatalf("iter %d %s window [%d,%d) limit %d: got %v want %v", iter, name, s0, e0, limit, got, want)
					}
				}
			}
		}
	}
}

type oracleSpan struct {
	namespace, service, trace string
	start, end                int64
	failed                    bool
}

type oracleRow struct {
	namespace, trace string
	hasError         int
}

// bruteCompletedTraces ranks raw spans in Go, independently of SQL.
func bruteCompletedTraces(spans []oracleSpan, start, end int64, namespace, service string, limit int) []oracleRow {
	type key struct{ namespace, trace string }
	type score struct {
		failed bool
		dur    int64
	}
	best := map[key]score{}
	for _, s := range spans {
		if s.start < start || s.start >= end || s.trace == "" || namespace != "" && s.namespace != namespace || service != "" && s.service != service {
			continue
		}
		k, sc := key{s.namespace, s.trace}, score{s.failed, s.end - s.start}
		if b, ok := best[k]; !ok || sc.failed && !b.failed || sc.failed == b.failed && sc.dur > b.dur {
			best[k] = sc
		}
	}
	keys := make([]key, 0, len(best))
	for k := range best {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := best[keys[i]], best[keys[j]]
		if a.failed != b.failed {
			return a.failed
		}
		if a.dur != b.dur {
			return a.dur > b.dur
		}
		if keys[i].trace != keys[j].trace {
			return keys[i].trace < keys[j].trace
		}
		return keys[i].namespace < keys[j].namespace
	})
	out := []oracleRow{}
	for _, k := range keys[:min(limit, len(keys))] {
		flag := 0
		if best[k].failed {
			flag = 1
		}
		out = append(out, oracleRow{k.namespace, k.trace, flag})
	}
	return out
}

func completedTraces(t *testing.T, d *Duck, w queryrows.Window, limit int) []oracleRow {
	t.Helper()
	w.Kind = queryrows.TraceCandidateRead
	rows, err := d.QueryContext(queryrows.WithWindow(t.Context(), w), queryrows.CompletedTraceQuery, w.Start, w.End, w.Namespace, w.Namespace, w.Service, w.Service, limit)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := []oracleRow{}
	for rows.Next() {
		var row oracleRow
		if err := rows.Scan(&row.namespace, &row.trace, &row.hasError); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// rawOracleSpans reads every retained span, so the oracle follows retention.
func rawOracleSpans(t *testing.T, d *Duck) []oracleSpan {
	t.Helper()
	all := queryrows.Window{Start: time.Unix(0, -1<<62).UTC(), End: time.Unix(0, 1<<62).UTC(), Kind: queryrows.RawRead}
	rows, err := d.QueryContext(queryrows.WithWindow(t.Context(), all), `SELECT coalesce(namespace,''),coalesce(service,''),trace_id,start_unix_nano,end_unix_nano,status='STATUS_CODE_ERROR' FROM spans`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []oracleSpan
	for rows.Next() {
		var s oracleSpan
		if err := rows.Scan(&s.namespace, &s.service, &s.trace, &s.start, &s.end, &s.failed); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// Compare the production ranking with brute force over raw spans through
// late publication, an uncached tail, covered and uncovered compaction,
// compaction of compaction outputs, and retention. Traces span batches,
// minutes, namespaces and services; starts and durations tie; windows are
// shorter than, about, and longer than a minute, aligned or not.
func TestCompletedTraceQueryMatchesBruteForce(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	r := rand.New(rand.NewSource(11))
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	const minutes = 5
	hour := int64(time.Hour)
	ingest := base.UnixNano()
	solo := ""
	commit := func(id string, ingestedAt int64, n int, minute func() int) {
		t.Helper()
		batch := telemetrystore.Batch{ID: id}
		for i := range n {
			namespace := "prod"
			if r.Intn(6) == 0 {
				namespace = "edge"
			}
			service := "api"
			if r.Intn(4) == 0 {
				service = "db"
			}
			if solo != "" {
				namespace, service = "prod", solo
			}
			// Coarse starts and durations force ties on both; some starts sit on
			// a minute boundary.
			start := base.UnixNano() + int64(minute())*minuteNanos + int64(r.Intn(12))*5*int64(time.Second)
			status := "STATUS_CODE_OK"
			if r.Intn(25) == 0 {
				status = "STATUS_CODE_ERROR"
			}
			dur := int64(1+r.Intn(12)) * int64(time.Millisecond)
			batch.Spans = append(batch.Spans, telemetry.Span{Namespace: namespace, ServiceName: service,
				TraceID: fmt.Sprintf("trace-%03d", r.Intn(400)), SpanID: fmt.Sprintf("%s-%d", id, i),
				StartUnixNanos: start, EndUnixNanos: start + dur, DurationMS: float64(dur) / 1e6,
				StatusCode: status, IngestedAt: ingestedAt})
		}
		if err := commitRestoredFixture(repo, t.Context(), batch); err != nil {
			t.Fatal(err)
		}
	}
	refresh := func() {
		t.Helper()
		for {
			rows, err := d.RefreshReadCaches(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if rows == 0 {
				return
			}
		}
	}
	anyMinute := func() int { return r.Intn(minutes) }
	type scope struct{ namespace, service string }
	// A scope one batch owns ranks from a single partition per minute, so
	// every kept or missing span decides the result.
	scopes := []scope{{"", ""}, {"prod", ""}, {"prod", "api"}, {"edge", "db"}, {"", "db"}, {"prod", "solo"}}
	check := func(stage string) {
		t.Helper()
		raw := rawOracleSpans(t, d)
		at := func(seconds int64) time.Time { return base.Add(time.Duration(seconds) * time.Second) }
		windows := [][2]int64{{0, minutes * 60}, {-30, minutes*60 + 30}, {60, 120}, {65, 115}, {55, 65}, {60, 65}, {55, 60}, {30, 90}, {5, 245}, {20, 200}}
		for range 6 {
			s := int64(r.Intn(minutes*12)) * 5
			windows = append(windows, [2]int64{s, s + int64(1+r.Intn(minutes*12))*5})
		}
		for _, bounds := range windows {
			for _, sc := range scopes {
				for _, limit := range []int{1, traceExemplarDepth} {
					w := queryrows.Window{Start: at(bounds[0]), End: at(bounds[1]), Namespace: sc.namespace, Service: sc.service}
					got := completedTraces(t, d, w, limit)
					want := bruteCompletedTraces(raw, w.Start.UnixNano(), w.End.UnixNano(), sc.namespace, sc.service, limit)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("%s window=%v scope=%v limit=%d:\n got %v\nwant %v", stage, bounds, sc, limit, got, want)
					}
				}
			}
		}
	}
	// Like live ingest, each batch covers about a minute.
	for i := range 12 {
		commit(fmt.Sprintf("a%02d", i), ingest+int64(i), 2000, func() int { return (i + r.Intn(2)) % minutes })
	}
	solo = "solo"
	commit("solo", ingest+12, 3000, func() int { return 1 + r.Intn(3) })
	solo = ""
	refresh()
	check("cached")
	// Late publication: recent ingest, old event times, not yet cached.
	for i := range 4 {
		commit(fmt.Sprintf("late%02d", i), ingest+int64(20+i), 1000, func() int { return r.Intn(2) })
	}
	check("uncached tail")
	refresh()
	check("acknowledged late publication")
	if n, err := repo.CompactParquet(t.Context(), d, 8); err != nil || n == 0 {
		t.Fatalf("compaction inputs=%d error=%v", n, err)
	}
	check("covered compaction before marker cleanup")
	refresh()
	check("covered compaction")
	// An uncovered compaction: inputs that were never cached.
	for i := range 8 {
		commit(fmt.Sprintf("b%02d", i), ingest+int64(40+i), 800, anyMinute)
	}
	if n, err := repo.CompactParquet(t.Context(), d, 8); err != nil || n == 0 {
		t.Fatalf("uncovered compaction inputs=%d error=%v", n, err)
	}
	check("uncovered compaction")
	refresh()
	check("uncovered compaction cached")
	// Once their hour settles, the covered and the uncovered compaction
	// outputs merge again from their transferred rows.
	for i := range 3 {
		commit(fmt.Sprintf("c%02d", i), ingest+int64(60+i), 800, anyMinute)
	}
	refresh()
	commit("newest", ingest+48*hour, 20, anyMinute)
	refresh()
	if n, err := repo.CompactParquet(t.Context(), d, 64); err != nil || n == 0 {
		t.Fatalf("day compaction inputs=%d error=%v", n, err)
	}
	merged := false
	for _, b := range repo.Parquet.BatchMetadata() {
		merged = merged || b.Generation >= 2
	}
	if !merged {
		t.Fatal("compaction outputs did not merge again")
	}
	check("mixed generation compaction")
	refresh()
	check("mixed generation compaction cached")
	if n, err := repo.PruneParquet(t.Context(), d, ingest+30, 64); err != nil || n == 0 {
		t.Fatalf("retention removed=%d error=%v", n, err)
	}
	check("retention before marker cleanup")
	refresh()
	check("retention")
	var stale int
	if err := d.DB.QueryRow(`SELECT count(*) FROM read_trace_exemplars WHERE batch_id NOT IN (SELECT batch_id FROM read_batches)`).Scan(&stale); err != nil || stale != 0 {
		t.Fatalf("orphaned exemplar rows=%d error=%v", stale, err)
	}
}

// Minutes before the epoch use floor division in both Go and SQL: rows land
// in the minute their start belongs to, and windows across negative minute
// boundaries rank exactly.
func TestCompletedTraceQueryMatchesBruteForceBeforeEpoch(t *testing.T) {
	d, repo := newBatchCacheTest(t)
	r := rand.New(rand.NewSource(5))
	for b := range 3 {
		batch := telemetrystore.Batch{ID: fmt.Sprintf("pre%d", b)}
		for i := range 900 {
			// Starts from -180s to +60s on a 5s grid, never exactly zero.
			start := int64(r.Intn(48)-36)*5*int64(time.Second) + int64(r.Intn(2))
			if start == 0 {
				start = -1
			}
			status := "STATUS_CODE_OK"
			if r.Intn(30) == 0 {
				status = "STATUS_CODE_ERROR"
			}
			dur := int64(1+r.Intn(9)) * int64(time.Millisecond)
			batch.Spans = append(batch.Spans, telemetry.Span{Namespace: "prod", ServiceName: "api", TraceID: fmt.Sprintf("trace-%03d", r.Intn(300)),
				SpanID: fmt.Sprintf("%d-%d", b, i), StartUnixNanos: start, EndUnixNanos: start + dur, DurationMS: float64(dur) / 1e6, StatusCode: status, IngestedAt: int64(b + 1)})
		}
		if err := commitRestoredFixture(repo, t.Context(), batch); err != nil {
			t.Fatal(err)
		}
	}
	for {
		rows, err := d.RefreshReadCaches(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if rows == 0 {
			break
		}
	}
	var misplaced int
	if err := d.DB.QueryRow(`SELECT count(*) FROM read_trace_exemplars WHERE minute>start_unix_nano OR minute+60000000000<=start_unix_nano OR minute%60000000000<>0`).Scan(&misplaced); err != nil || misplaced != 0 {
		t.Fatalf("misplaced minutes=%d: %v", misplaced, err)
	}
	for _, s := range []int64{-180, -125, -120, -61, -60, -35, -1} {
		if got := floorMinute(s * int64(time.Second)); got > s*int64(time.Second) || got%minuteNanos != 0 || s*int64(time.Second)-got >= minuteNanos {
			t.Fatalf("floorMinute(%ds)=%d", s, got)
		}
	}
	raw := rawOracleSpans(t, d)
	for _, bounds := range [][2]int64{{-180, 60}, {-125, -5}, {-120, -60}, {-95, -65}, {-65, -55}, {-62, 3}, {-1, 30}, {-170, -61}} {
		for _, limit := range []int{1, traceExemplarDepth} {
			for _, service := range []string{"", "api"} {
				w := queryrows.Window{Start: time.Unix(0, bounds[0]*int64(time.Second)).UTC(), End: time.Unix(0, bounds[1]*int64(time.Second)).UTC(), Namespace: "prod", Service: service}
				got := completedTraces(t, d, w, limit)
				want := bruteCompletedTraces(raw, w.Start.UnixNano(), w.End.UnixNano(), "prod", service, limit)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("window=%v limit=%d service=%q:\n got %v\nwant %v", bounds, limit, service, got, want)
				}
			}
		}
	}
}
