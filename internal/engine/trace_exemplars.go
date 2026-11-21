package engine

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
)

// read_trace_exemplars ranks traces by their best in-scope, in-window span:
// an error first, then a longer span, then trace_id and namespace. Rows are
// kept per (batch, namespace, service, minute of span start) partition:
//
//   - in_full: the best span of each of the partition's top traces, enough to
//     rank any window that covers the whole minute;
//   - in_suffix: spans that fewer than traceExemplarDepth other traces outrank
//     using spans starting at or after them, enough for a window starting in
//     the minute;
//   - in_prefix: the mirror, enough for a window ending in the minute.
//
// A trace's score is a maximum over its spans, so it decomposes over any
// union of partitions and raw spans. A span another trace's in-window spans
// outrank at least traceExemplarDepth times can never be a top trace's best,
// so any superset of these rows ranks exactly.
const createTraceExemplarsTable = `CREATE TABLE IF NOT EXISTS read_trace_exemplars (batch_id VARCHAR,namespace VARCHAR,service VARCHAR,minute BIGINT,trace_id VARCHAR,start_unix_nano BIGINT,duration BIGINT,has_error INTEGER,in_full BOOLEAN,in_suffix BOOLEAN,in_prefix BOOLEAN)`

// traceExemplarDepth bounds every completed-trace ranking limit.
const traceExemplarDepth = queryrows.CompletedTraceLimit

// tracePruneTailSpans is the raw tail size from which ranking reads a cached
// threshold to prune it. The threshold costs a ranking of the cached rows
// (about 6 ms over a cached 24-hour window in the drill-down bench), so it
// pays only once the tail's aggregation costs more: it saves about 40 ms on a
// 3M-span tail and nothing on 100k spans spread over 200 files. Pruning or
// not, the result is the same.
const tracePruneTailSpans = 250_000

const minuteNanos = int64(60_000_000_000)

// floorMinute is floor division, also for instants before the epoch.
func floorMinute(nanos int64) int64 {
	m := nanos % minuteNanos
	if m < 0 {
		m += minuteNanos
	}
	return nanos - m
}

// minuteSQL matches floorMinute: DuckDB's % keeps the dividend's sign.
func minuteSQL(column string) string {
	return column + "-((" + column + "%60000000000)+60000000000)%60000000000"
}

const (
	exemplarFull   = 1
	exemplarSuffix = 2
	exemplarPrefix = 4
)

type exemplarSpan struct {
	id    int64
	trace int64 // dense rank of trace_id within its partition
	start int64
	dur   int64
	err   bool
}

// better reports whether a outranks b. In one partition the namespace is
// fixed, so the trace rank completes the global tie order.
func (a exemplarSpan) better(b exemplarSpan) bool {
	if a.err != b.err {
		return a.err
	}
	if a.dur != b.dur {
		return a.dur > b.dur
	}
	return a.trace < b.trace
}

// exemplarSkyline returns each span's flags for one partition.
func exemplarSkyline(spans []exemplarSpan) []uint8 {
	flags := make([]uint8, len(spans))
	traces := int64(0)
	for _, s := range spans {
		traces = max(traces, s.trace)
	}
	best := make([]int, traces+1)
	for i := range best {
		best[i] = -1
	}
	for i, s := range spans {
		if b := best[s.trace]; b < 0 || s.better(spans[b]) {
			best[s.trace] = i
		}
	}
	var top []int
	for _, b := range best {
		if b >= 0 {
			top = append(top, b)
		}
	}
	sort.Slice(top, func(i, j int) bool { return spans[top[i]].better(spans[top[j]]) })
	for _, i := range top[:min(len(top), traceExemplarDepth)] {
		flags[i] |= exemplarFull
	}
	order := make([]int, len(spans))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return spans[order[i]].start > spans[order[j]].start })
	exemplarPass(spans, order, best, flags, exemplarSuffix)
	for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
		order[i], order[j] = order[j], order[i]
	}
	exemplarPass(spans, order, best, flags, exemplarPrefix)
	return flags
}

// exemplarPass scans spans in order, grouping equal start times, and keeps a
// span when fewer than traceExemplarDepth other traces outrank it with spans
// already scanned. It tracks the depth+2 best per-trace scores; per-trace bests
// only rise, so a trace outside that set is outranked by depth+1 others and
// cannot change any count below the depth.
func exemplarPass(spans []exemplarSpan, order, best []int, flags []uint8, bit uint8) {
	for i := range best {
		best[i] = -1
	}
	capacity := traceExemplarDepth + 2
	top := make([]int, 0, capacity) // indexes of per-trace bests
	for g := 0; g < len(order); {
		e := g
		for e < len(order) && spans[order[e]].start == spans[order[g]].start {
			e++
		}
		for _, i := range order[g:e] {
			s := spans[i]
			if b := best[s.trace]; b >= 0 && !s.better(spans[b]) {
				continue
			}
			best[s.trace] = i
			found := false
			for j, o := range top {
				if spans[o].trace == s.trace {
					top[j], found = i, true
					break
				}
			}
			if found {
				continue
			}
			if len(top) < capacity {
				top = append(top, i)
				continue
			}
			worst := 0
			for j := range top {
				if spans[top[worst]].better(spans[top[j]]) {
					worst = j
				}
			}
			if s.better(spans[top[worst]]) {
				top[worst] = i
			}
		}
		for _, i := range order[g:e] {
			n := 0
			for _, o := range top {
				if spans[o].trace != spans[i].trace && spans[o].better(spans[i]) {
					n++
				}
			}
			if n < traceExemplarDepth {
				flags[i] |= bit
			}
		}
		g = e
	}
}

// insertTraceExemplars reads source's projected spans (id, batch_id,
// namespace, service, minute, trace_id, start_unix_nano, duration, has_error),
// computes each partition's rows and appends them. Only integers cross into
// Go; the kept ids return as one bound string.
func insertTraceExemplars(ctx context.Context, tx *sql.Tx, source string) (int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id,dense_rank() OVER (ORDER BY batch_id,namespace,service,minute) AS part,
 dense_rank() OVER (PARTITION BY batch_id,namespace,service,minute ORDER BY trace_id) AS trace,start_unix_nano,duration,has_error
 FROM `+source+` ORDER BY part`)
	if err != nil {
		return 0, err
	}
	var kept strings.Builder
	var spans []exemplarSpan
	flush := func() {
		for i, f := range exemplarSkyline(spans) {
			if f != 0 {
				if kept.Len() > 0 {
					kept.WriteByte(',')
				}
				kept.WriteString(strconv.FormatInt(spans[i].id, 10))
				kept.WriteByte(':')
				kept.WriteByte('0' + f)
			}
		}
		spans = spans[:0]
	}
	current := int64(-1)
	for rows.Next() {
		var s exemplarSpan
		var part, failed int64
		if err := rows.Scan(&s.id, &part, &s.trace, &s.start, &s.dur, &failed); err != nil {
			rows.Close()
			return 0, err
		}
		s.err = failed != 0
		if part != current && len(spans) > 0 {
			flush()
		}
		current = part
		spans = append(spans, s)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	if len(spans) > 0 {
		flush()
	}
	if kept.Len() == 0 {
		return 0, nil
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO read_trace_exemplars SELECT s.batch_id,s.namespace,s.service,s.minute,s.trace_id,s.start_unix_nano,s.duration,s.has_error,
 (k.f&1)<>0,(k.f&2)<>0,(k.f&4)<>0
 FROM `+source+` s JOIN (SELECT split_part(e,':',1)::BIGINT AS id,split_part(e,':',2)::INTEGER AS f FROM unnest(string_split(?::VARCHAR,',')) AS u(e)) k ON s.id=k.id`, kept.String())
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// traceExemplarSource is the private trace_exemplars relation: the in-window,
// in-scope spans that can decide a ranking. Covered minutes contribute their
// full rows, a partial start minute its suffix rows, a partial end minute its
// prefix rows, and uncached files their raw spans. A window inside one minute
// reads the overlapping files' raw spans.
//
// The raw tail keeps only spans that score at least the traceExemplarDepth-th
// best cached trace. Cached rows are real in-window spans, so at least that
// many traces score this threshold or more, and so does every trace ranked at
// or above that depth. A pruned span scores below it: it is the best span of
// no such trace, and removing it leaves every other trace's score unchanged.
// Ties keep their spans. With fewer cached traces than the depth, nothing is
// pruned. The threshold is a constant read in the same snapshot from the
// cached rows also flagged in_full: any subset of in-window spans gives a
// valid bound, and this one is tight and cheaper to rank than every row.
func (d *Duck) traceExemplarSource(ctx context.Context, db snapshotSQL, batches []telemetry.BatchMetadata, markers map[string]bool, w queryrows.Window) (string, error) {
	start, end := w.Start.UnixNano(), w.End.UnixNano()
	startMinute, endMinute := floorMinute(start), floorMinute(end)
	partialStart, partialEnd := start != startMinute, end != endMinute
	exact := partialStart && partialEnd && startMinute == endMinute
	var cachedIDs []string
	var raw []telemetry.BatchMetadata
	for _, b := range batches {
		if !overlapping(b, "spans", w) {
			continue
		}
		if markers[b.ID] && !exact {
			cachedIDs = append(cachedIDs, b.ID)
		} else {
			raw = append(raw, b)
		}
	}
	var scope strings.Builder
	if w.Namespace != "" {
		scope.WriteString(" AND namespace=" + sqlLiteral(w.Namespace))
	}
	if w.Service != "" {
		scope.WriteString(" AND service=" + sqlLiteral(w.Service))
	}
	tail := func(threshold string) string {
		return `SELECT namespace,service,trace_id,start_time,start_unix_nano,duration,has_error FROM (SELECT coalesce(namespace,'') AS namespace,coalesce(service,'') AS service,trace_id,start_time,start_unix_nano,end_unix_nano-start_unix_nano AS duration,
 CASE WHEN status='STATUS_CODE_ERROR' THEN 1 ELSE 0 END AS has_error FROM (` + cleanSource("spans", d.snapshotSource("spans", raw, batches)) + `) WHERE ` + windowPredicate("start_time", w) + ` AND trace_id<>'') WHERE true` + scope.String() + threshold
	}
	if len(cachedIDs) == 0 {
		return tail(""), nil
	}
	fullFrom := startMinute
	if partialStart {
		fullFrom += minuteNanos
	}
	ranges := []string{fmt.Sprintf("(in_full AND minute>=%d AND minute<%d)", fullFrom, endMinute)}
	if partialStart {
		ranges = append(ranges, fmt.Sprintf("(in_suffix AND minute=%d AND start_unix_nano>=%d)", startMinute, start))
	}
	if partialEnd {
		ranges = append(ranges, fmt.Sprintf("(in_prefix AND minute=%d AND start_unix_nano<%d)", endMinute, end))
	}
	cached := `SELECT namespace,service,trace_id,make_timestamp_ns(start_unix_nano)::TIMESTAMPTZ_NS AS start_time,start_unix_nano,duration,has_error FROM read_trace_exemplars WHERE batch_id IN (SELECT unnest([` + idsSQL(cachedIDs) + `]))` +
		fmt.Sprintf(" AND minute>=%d AND minute<=%d", startMinute, endMinute) + scope.String() + ` AND (` + strings.Join(ranges, " OR ") + `)`
	tailSpans := 0
	for _, b := range raw {
		tailSpans += b.Spans
	}
	threshold := ""
	if len(raw) > 0 && tailSpans >= d.tracePruneSpans {
		var traces, failed, duration int64
		if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*),coalesce(min(score)[1],0),coalesce(min(score)[2],0) FROM (SELECT max([has_error::BIGINT,duration]) AS score FROM (%s) GROUP BY namespace,trace_id ORDER BY score DESC LIMIT %d)`, cached+" AND in_full", traceExemplarDepth)).Scan(&traces, &failed, &duration); err != nil {
			return "", err
		}
		switch {
		case traces < traceExemplarDepth:
		case failed != 0:
			threshold = fmt.Sprintf(" AND has_error=1 AND duration>=%d", duration)
		default:
			threshold = fmt.Sprintf(" AND (has_error=1 OR duration>=%d)", duration)
		}
	}
	return cached + ` UNION ALL ` + tail(threshold), nil
}
