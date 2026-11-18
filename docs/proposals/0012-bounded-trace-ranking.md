# Bounded trace ranking for drill-down

Status: Accepted
Date: 2026-11-18
Pull request: #504

## Summary

Drill-down ranks every retained trace in its window to return the 21 slowest
or failing ones. Its cost, and the read-cache maintenance behind it, grow with
retained data, so it times out on a server that has run for a while. This
proposal ranks each trace by its best in-scope span and caches a small,
append-only list of span exemplars per batch, scope and minute. Nothing in the
cache spans batches. A query reads the lists of the minutes in its window,
unions the uncached batches ranked the same way, and takes the top K. Cost
depends on window length and the number of services, not on retained traces.

## Motivation

`queryrows.CompletedTraceQuery` serves service-map drill-down
(`indexedExemplars` in `internal/query/exemplars.go`, limit 21) and the
recent-trace lookup (`observability.Service.Trace`, limit 1). It orders every
in-window trace candidate by `has_error DESC, max_end-min_start DESC` from
`read_trace_candidates` and `read_trace_parts`.

Evidence:

- pitot-bench on a Hetzner ccx33 pair, `v2026.11.9-alpha`, 48k items/s mixed
  ingest with 20 UI reads/s. A fresh server (about 15M rows) passes the read
  gate at 20/20 ops/s on 3.25 cores. After a 10-minute prefill (about 180M
  rows) the same load completes 2.6/20 ops/s on 7.3 cores. All 1,716 failures
  are 504s on `/api/queries/exemplars`, and no drill-down or trace detail
  succeeds. A local run reproduces it (4.8/20 ops/s).
- `internal/engine/trace_candidate_scale_bench_test.go` (build tag
  `filebench`), fully cached:

  | Cached traces | Single-service traces | Two-service traces |
  |---|---|---|
  | 400k | 21 ms | 93 ms |
  | 1.6M | 197 ms | 633 ms |
  | 16M (4 GB engine) | 831 ms | 2,426 ms |

  With the test engine's 256 MB limit, the 16M two-service case runs out of
  memory.
- `EXPLAIN ANALYZE` at 1.6M two-service traces: candidates of multi-service
  traces have `service=NULL`, so a service scope aggregates every retained
  `read_trace_parts` row, and DuckDB materializes and spills the candidate set
  (275 MB).
- Maintenance grows too. The per-pass candidate MERGE joins every retained
  trace: 27, 280 and 627 ms per pass at 3M, 30M and 100M traces.

The stored per-trace state is what fails to scale. Rewriting the query's joins
gave no gain once the index was fully cached.

## Goals and non-goals

Goals:

- Drill-down and recent-trace cost independent of retained traces, for every
  scope (namespace and service, each optional) and windows up to retention.
- Read-cache maintenance cost per batch independent of retained traces.
- Exact results for the ranking defined below, including late publication,
  compaction, retention and immediately visible uncached batches.

Non-goals:

- Approximate ranking.
- Trace search with arbitrary attribute filters, which keeps the generic path.

## Ranking

A trace's score in a scope and window is that of its best span among the spans
that match the scope and start in `[start, end)`:

1. the span failed (`STATUS_CODE_ERROR`) first;
2. then longer span duration;
3. then `trace_id`, then `namespace`, ascending, as today.

`has_error` is still true when the trace has any in-scope, in-window error
span, so the error-first selection is unchanged.

This replaces the scoped extent (`max_end - min_start`). The extent is never
shown: the drill list displays service, operation and root duration
(`ui/host/src/dashboards/drill.tsx`), and `indexedExemplars` re-sorts the
selected traces by root status and duration. The generic exemplar path already
ranks by span `duration_ms`, as proposal 0004 documents, so both paths now
agree. Which traces fill the 20 slots can differ from today: a service with
several short disjoint spans in one trace no longer ranks by the time between
them, which included time spent in other services. Peers that rank traces
(Honeycomb, SigNoz, Uptrace) also rank by a span's duration.

A best-span score decomposes: the best span over a union of partitions is the
best of each partition's best spans. That is what lets the cache hold no
cross-batch state.

## Design

### Exemplar lists

A private read-cache table, `read_trace_exemplars`, holds rows keyed by
(batch, namespace, service, minute of span start). Each partition keeps three
skyline sets of spans:

- `full`: the top 21 distinct traces of the partition, each by its best span;
- `suffix`: every span that fewer than 21 other traces outrank using spans
  that start at or after it;
- `prefix`: the mirror, using spans that start at or before it.

For a window starting inside a partition's minute, its top 21 is contained in
the `suffix` rows with start at or after the window start. For a window ending
inside one, its top 21 is contained in the `prefix` rows that start before the
window end. List size grows with the logarithm of spans per partition: about
210 rows at 2.4k spans and 480 at 1M.

### Query

A window `[S, E)` reads:

- `full` rows of the minutes fully inside it;
- `suffix` rows of the minute containing `S`, with start at or after `S`;
- `prefix` rows of the minute containing `E`, with start before `E`;
- `UNION ALL` the raw in-scope spans of uncached batches, so new data is
  visible at once.

It then groups by trace, takes the best span, and returns the top K. The tail
is a union, not a join, so its cost is that of the uncached data only. A window
whose start and end fall in the same minute reads footer-pruned Parquet of the
overlapping files exactly, as the generic drill path already does.

### Exactness

If a trace's best in-window span lies in partition P and the trace is in the
global top K, then fewer than K traces outrank it anywhere in the window, so
fewer than K outrank it within P's part of the window. Its best span is
therefore in P's `full`, `suffix` or `prefix` set for that part. Every other
trace in those sets is also in the window, so no excluded trace can displace a
winner. An oracle test in the prototype compares against brute force over raw
spans on 300 random datasets × 20 windows, including traces split across
batches and minutes, and compaction.

### Lifecycle

- **Publication.** Lists are built from the batch's spans in the
  completed-batch write, in the same transaction as the batch markers.
- **Compaction.** The output's lists are recomputed from the union of the
  inputs' rows in the same transaction as the marker transfer. A superset of
  the true lists keeps the query exact. An uncovered compaction builds from
  Parquet like any batch.
- **Retention.** Rows are deleted by batch.

Late spans need no update path: each is a row in its own batch's list, and the
query's maximum merges it.

### Removed

`read_trace_candidates`, `read_trace_parts`, the trace tail join,
`traceCandidateSource`, retired-trace repair and the partial-trace raw path.
The read-cache version goes from 19 to 20, so a mismatched cache rebuilds once
on upgrade. No telemetry is lost.

## Alternatives

- **Rank lists over merged per-trace candidates.** Exact for the extent key,
  but they keep the per-trace candidate MERGE and tail join, which grow with
  retention (about 1.4–5.8 s per pass at 700M traces). Bounding them needs a
  merge horizon that splits traces with gaps into segments, which users would
  see.
- **Go-side per-batch sidecars and an in-memory split-trace registry.** Exact
  for the extent key, with microsecond queries in memory. But the registry
  grows with retention, filters need about 2.25 GB of memory at 1B traces a
  day, the on-disk form was not built, and the measurements were unstable.
- **Per-batch top-K of trace extents.** Inexact for traces split across
  batches.
- **Threshold algorithms over per-batch partials.** Unseen batches can extend
  any trace's extent, so the upper bounds are too loose to stop early.
- **Join rewrites.** No gain once the index is fully cached.

## Gates

1. An in-tree oracle against brute force over raw spans. It covers random
   windows, namespace, service and empty scopes, K of 1 and 21, ties,
   identical trace IDs in different namespaces, sub-minute and multi-minute
   windows, late publication, compaction across generations, retention and an
   uncached tail.
2. Scale benchmark: a 24h window stays flat in retained traces and under 50 ms
   at 16M traces, for single- and two-service traces.
3. Maintenance: list build per batch and refresh pass time flat from 1.6M to
   16M traces.
4. `TraceSummaries` root lookup for 20 traces over 24h stays under 150 ms at a
   day's batch count. If it does not, add a per-batch trace-hash filter.
5. pitot-bench on Hetzner with a full-day prefill, then 48k items/s with 20
   reads/s: the read gate passes.

## Risks

- **`TraceSummaries`** still opens each in-window batch's trace index. Gate 4
  measures it.
- **Sub-minute windows** on old, large compacted files decode whole columns
  (estimated 0.1–0.3 s). Add a finer tier only if measurements need it.
- **Planner estimates.** Bound every source with constant time predicates and
  check plans in tests.
