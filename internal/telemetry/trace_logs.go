package telemetry

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"io"
	"path/filepath"
	"sort"

	"github.com/zeebo/xxh3"
)

// TraceLog is the narrow projection of a log row that trace detail shows.
type TraceLog struct {
	Namespace     string
	ServiceName   string
	TraceID       string
	SpanID        string
	Severity      string
	Body          string
	TimeUnixNanos int64
	// The batch and row break ties, so the order is total within a catalog.
	batch string
	row   uint32
}

// traceLogRow reads the TraceLog columns of a logs file.
type traceLogRow struct {
	Namespace string `parquet:"namespace"`
	Time      int64  `parquet:"log_time,timestamp(nanosecond)"`
	Severity  string `parquet:"severity"`
	Body      string `parquet:"body"`
	Service   string `parquet:"service"`
	TraceID   string `parquet:"trace_id"`
	SpanID    string `parquet:"span_id"`
}

// TraceLogs reads one trace's logs through each batch's log index: a probe
// per batch whose log times meet the window, then only the posted rows. It
// keeps the first Limit rows by (time, service, span ID, batch, row), a total
// order that refines time order.
func (p *ParquetStore) TraceLogs(ctx context.Context, query TraceQuery) ([]TraceLog, error) {
	if query.TraceID == "" {
		return nil, nil
	}
	if err := query.validate(); err != nil {
		return nil, err
	}
	return traceLogs(ctx, &p.footers, p.catalog(), query)
}

func traceLogs(ctx context.Context, footers *footerCache, batches []*storedBatch, query TraceQuery) ([]TraceLog, error) {
	hash := xxh3.HashString(query.TraceID)
	selected := make(traceLogHeap, 0, query.Limit)
	for _, batch := range batches {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if batch.metadata.Logs == 0 {
			continue
		}
		if bounds := batch.metadata.LogTime; bounds.Known &&
			(bounds.MaxNanos < query.StartNanos || bounds.MinNanos >= query.EndNanos) {
			continue
		}
		rows, err := batch.logs.Lookup(hash)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			continue
		}
		if err := readTraceLogs(ctx, footers, batch, rows, query, &selected); err != nil {
			return nil, err
		}
	}
	out := []TraceLog(selected)
	sort.Slice(out, func(i, j int) bool { return traceLogEarlier(out[i], out[j]) })
	return out, nil
}

// readTraceLogs seeks to each run of consecutive posted rows; the reader
// decodes only the projected columns' pages that hold them.
func readTraceLogs(ctx context.Context, footers *footerCache, batch *storedBatch, rows []uint32, query TraceQuery, selected *traceLogHeap) (err error) {
	reader, closeReader, err := openCachedReader[traceLogRow](footers, filepath.Join(batch.dir, "logs.parquet"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeReader()) }()
	buffer := make([]traceLogRow, min(len(rows), 256))
	for start := 0; start < len(rows); {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + 1
		for end < len(rows) && rows[end] == rows[end-1]+1 && end-start < len(buffer) {
			end++
		}
		if err := reader.SeekToRow(int64(rows[start])); err != nil {
			return err
		}
		n, err := reader.Read(buffer[:end-start])
		if n < end-start {
			if err == nil || errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
		for i, row := range buffer[:n] {
			if !query.matches(row.TraceID, row.Namespace, row.Time) {
				continue
			}
			selected.Add(TraceLog{
				Namespace: row.Namespace, ServiceName: row.Service, TraceID: row.TraceID, SpanID: row.SpanID,
				Severity: row.Severity, Body: row.Body, TimeUnixNanos: row.Time,
				batch: batch.metadata.ID, row: rows[start+i],
			}, query.Limit)
		}
		clear(buffer[:n])
		start = end
	}
	return nil
}

func traceLogEarlier(left, right TraceLog) bool {
	if c := cmp.Or(
		cmp.Compare(left.TimeUnixNanos, right.TimeUnixNanos),
		cmp.Compare(left.ServiceName, right.ServiceName),
		cmp.Compare(left.SpanID, right.SpanID),
		cmp.Compare(left.batch, right.batch),
	); c != 0 {
		return c < 0
	}
	return left.row < right.row
}

// traceLogHeap keeps the latest selected log at its root.
type traceLogHeap []TraceLog

func (h traceLogHeap) Len() int           { return len(h) }
func (h traceLogHeap) Less(i, j int) bool { return traceLogEarlier(h[j], h[i]) }
func (h traceLogHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *traceLogHeap) Push(value any)    { *h = append(*h, value.(TraceLog)) }
func (h *traceLogHeap) Pop() any {
	old := *h
	value := old[len(old)-1]
	*h = old[:len(old)-1]
	return value
}

func (h *traceLogHeap) Add(log TraceLog, limit int) {
	if len(*h) < limit {
		heap.Push(h, log)
		return
	}
	if traceLogEarlier(log, (*h)[0]) {
		(*h)[0] = log
		heap.Fix(h, 0)
	}
}
