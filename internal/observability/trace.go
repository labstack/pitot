package observability

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/labstack/pitot/internal/queryrows"
	"github.com/labstack/pitot/internal/telemetry"
)

func (s *Service) Trace(ctx context.Context, scope Scope, traceID, service string, limit int) (Result[TraceDetail], error) {
	scope, err := s.normalizeScope(scope)
	if err != nil {
		return Result[TraceDetail]{}, err
	}
	limit, err = normalizeLimit(limit)
	if err != nil {
		return Result[TraceDetail]{}, err
	}
	traceID, service = strings.TrimSpace(traceID), strings.TrimSpace(service)
	ctx = queryrows.WithWindow(ctx, queryrows.Window{Start: scope.Start, End: scope.End, Namespace: scope.Namespace, Service: service})
	namespace, exactNamespace := scope.Namespace, false
	if traceID == "" {
		candidateCtx := queryrows.WithWindow(ctx, queryrows.Window{Start: scope.Start, End: scope.End, Namespace: scope.Namespace, Service: service, Kind: queryrows.TraceCandidateRead})
		rows, queryErr := s.db.QueryContext(candidateCtx, queryrows.CompletedTraceQuery, scope.Start, scope.End, scope.Namespace, scope.Namespace, service, service, 1)
		if queryErr != nil {
			return Result[TraceDetail]{}, fmt.Errorf("query recent trace: %w", queryErr)
		}
		if rows.Next() {
			if err := rows.Scan(&namespace, &traceID, new(int)); err != nil {
				rows.Close()
				return Result[TraceDetail]{}, fmt.Errorf("scan recent trace: %w", err)
			}
		}
		exactNamespace = true
		if err := rows.Err(); err != nil {
			rows.Close()
			return Result[TraceDetail]{}, fmt.Errorf("iterate recent trace: %w", err)
		}
		rows.Close()
	}

	data := TraceDetail{TraceID: traceID, Services: []string{}, Spans: []TraceSpan{}, Logs: []LogEntry{}}
	if traceID != "" {
		// Spans and logs come from one pinned catalog, each through its index.
		stored, readErr := s.repository.TraceDetail(ctx, telemetry.TraceQuery{
			TraceID: traceID, Namespace: namespace, ExactNamespace: exactNamespace,
			StartNanos: scope.Start.UnixNano(), EndNanos: scope.End.UnixNano(), Limit: limit,
		})
		if readErr != nil {
			return Result[TraceDetail]{}, fmt.Errorf("read indexed Parquet trace: %w", readErr)
		}
		totals := stored.Totals
		for _, row := range stored.Spans {
			data.Spans = append(data.Spans, TraceSpan{SpanID: row.SpanID, ParentSpanID: row.ParentSpanID, Service: row.ServiceName, Operation: row.Name, Kind: row.Kind, Start: time.Unix(0, row.StartUnixNanos).UTC(), DurationMS: row.DurationMS, Status: row.StatusCode, StatusMessage: row.StatusMsg})
		}

		// Only the page's own service list is derived here. Duration and
		// has_error describe the whole trace, so they come from the totals the
		// repository accumulated over every span it walked -- not from this
		// page, which is why trace_detail used to report the page's span count
		// and duration as the trace's.
		serviceSet := make(map[string]struct{})
		for _, span := range data.Spans {
			if span.Service != "" {
				serviceSet[span.Service] = struct{}{}
			}
		}
		for name := range serviceSet {
			data.Services = append(data.Services, name)
		}
		sort.Strings(data.Services)
		// The page above is what limit admitted. Everything the caller reads as
		// a fact about the trace — how many spans it has, how many services it
		// crosses, how long it took, whether it failed — describes the whole
		// trace, so a narrow page cannot silently redefine it.
		//
		// These come from the index walk that produced the page: it already
		// decodes every row of the trace and applies the same predicates, and
		// only the heap is bounded by limit. An aggregate over every batch file
		// would answer the same question and cost a second pass.
		data.SpanCount, data.ServiceCount = totals.Spans, totals.Services
		data.DurationMS, data.HasError = totals.DurationMS, totals.HasError
		data.Truncated = data.SpanCount > len(data.Spans)
		for _, row := range stored.Logs {
			data.Logs = append(data.Logs, LogEntry{
				Time: time.Unix(0, row.TimeUnixNanos).UTC(), Severity: row.Severity, Service: row.ServiceName,
				Body: redactLogBody(row.Body), TraceID: row.TraceID, SpanID: row.SpanID,
			})
		}
	}

	summary := "No traces found in this telemetry window"
	if traceID != "" {
		summary = fmt.Sprintf("Trace %s contains %d spans across %d services", traceID, data.SpanCount, data.ServiceCount)
		if data.Truncated {
			summary += fmt.Sprintf("; showing %d", len(data.Spans))
		}
	}
	return Result[TraceDetail]{Summary: summary, Data: data, Provenance: s.provenanceFor(scope)}, nil
}
