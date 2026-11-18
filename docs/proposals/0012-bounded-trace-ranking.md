# Bounded trace ranking for drill-down

Status: Draft
Date: 2026-11-18
Pull request: TBD

## Summary

Drill-down ranks every retained trace in its window to return the 21 slowest
or failing ones, so its cost grows with retained data and it times out on a
server that has run for a while. This proposal keeps a small, exactly
maintained ranked list of traces per time bucket and scope in the read cache.
A query merges the lists of the buckets inside its window and scans exactly
only what the lists cannot answer. Cost then depends on window length and list
size, not on how many traces are retained.

## Motivation

`queryrows.CompletedTraceQuery` serves service-map drill-down
(`indexedExemplars` in `internal/query/exemplars.go`, limit 21) and the
recent-trace lookup (`observability.Service.Trace`, limit 1). It orders every
in-window trace candidate by `has_error DESC, max_end-min_start DESC`.

Evidence:

- pitot-bench on a Hetzner ccx33 pair, `v2026.11.9-alpha`, 48k items/s mixed
  ingest with 20 UI reads/s. A fresh server (about 15M rows) passes the read
  gate at 20/20 ops/s on 3.25 cores. After a 10-minute prefill (about 180M
  rows) the same load completes 2.6/20 ops/s on 7.3 cores. Every one of 1,716
  failures is a 504 on `/api/queries/exemplars`, and no drill-down or trace
  detail succeeds. A local run on an M-series Mac reproduces it (4.8/20 ops/s).
- `internal/engine/trace_candidate_scale_bench_test.go` (build tag
  `filebench`) times the query against a fully cached index with a two-batch
  uncached tail:

  | Cached traces | Single-service traces | Two-service traces |
  |---|---|---|
  | 400k | 21 ms | 93 ms |
  | 1.6M | 197 ms | 633 ms |

  Four times the traces costs seven to nine times the time. The Hetzner run
  held roughly 30M traces; a day at 48k rows/s holds billions of rows, and
  dashboard windows reach 24h.
- `EXPLAIN ANALYZE` at 1.6M two-service traces (0.59 s): every candidate has
  `service=NULL`, so a service scope takes the repair path and aggregates every
  retained `read_trace_parts` row (`HASH_JOIN RIGHT_SEMI` 207 ms with a 64,000
  row estimate against 1.6M actual, `HASH_GROUP_BY` 258 ms, a second 206 ms
  group-by for the boundary subquery). DuckDB factors `trace_candidates` into a
  materialized common subplan despite the `NOT MATERIALIZED` hint and spills
  275 MB. The top-N steps themselves cost about 4 ms.

Rewriting the joins does not help once the index is fully cached (603 ms
against 624 ms). The work is proportional to retained traces by construction.

## Goals and non-goals

Goals:

- Drill-down and recent-trace cost independent of retained traces, for every
  scope (namespace and service set or empty) and window up to the retention
  period.
- Exactly the current results, including ties (`trace_id`, then
  `namespace`), late publication, compaction and retention.
- Bounded maintenance cost per cached batch.

Non-goals:

- Changing the ranking key or the definition of a trace's duration
  (`max_end - min_start` over in-window, in-scope spans).
- Approximate ranking. Peers rank by root-span duration (SigNoz, Honeycomb,
  Uptrace), return the first N matches (Tempo, Jaeger), or accept per-block
  durations that understate split traces (Tempo). Pitot keeps its exact
  definition.
- Trace search with arbitrary attribute filters, which keeps the generic path.

## Design

### Rank lists

A new private read-cache table, `read_trace_rank`:

| Column | Meaning |
|---|---|
| `tier` | minute, hour or day |
| `bucket` | bucket start, Unix nanoseconds |
| `namespace`, `service` | scope; `service` is `''` for the all-services list |
| `trace_id`, `min_start`, `max_start`, `max_end`, `has_error` | the trace's score in that scope |

Each (tier, bucket, namespace, service) group holds the top
`queryrows.TraceRankDepth` (21) traces by the production ordering. A trace
belongs to the minute bucket of its `min_start`. Only short traces
(`max_start - min_start` under one minute) enter lists.

A trace whose spans cover several services gets one row per service. Each
row's score is computed from that service's parts, because a service-scoped
query ranks only that service's spans. Rank rows store results only. The
"one incremental candidate per trace" rule still holds for
`read_trace_candidates`.

### Why it is exact

Each short trace lies in exactly one minute bucket per scope, so buckets
partition the traces. If a trace is in the global top K of a window, at most
K-1 traces outrank it anywhere, so at most K-1 outrank it in its own bucket.
It is therefore in its bucket's list, and the union of the lists of fully
covered buckets contains the global top K over those buckets. Hour and day
lists are the top K of their child lists, by the same argument.

Scores come from merged candidates, never from per-batch parts. A newly
published span only lowers `min_start`, raises `max_end` or sets `has_error`,
so a trace's score never falls on publication. Updating a list is then an
insert with eviction of the lowest entry. Two events can lower a score or move
a trace between buckets: retention retiring part of a trace, and `min_start`
moving into an earlier bucket. Both rebuild the affected bucket from
`read_trace_candidates`, at a cost bounded by that bucket's traces.

### Maintenance

In the same `cacheBatches` transaction that inserts parts, merges candidates
and writes markers (`internal/engine/batch_cache.go`):

1. Before the candidate MERGE, record the bucket keys of the candidates it
   touches. After the MERGE, record their new keys.
2. Recompute those minute groups from `read_trace_candidates` and, for
   per-service scores, from that bucket's `read_trace_parts`, using
   `QUALIFY row_number() OVER (PARTITION BY namespace, service ORDER BY ...)
   <= 21`.
3. Recompute affected hour groups from their minute lists and affected day
   groups from their hour lists.

Retention repair runs the same steps for the keys in `cache_retired_traces`.
A compaction covered by a cached replacement chain changes no candidate and
does no rank work. An uncovered one is treated as retention. Contributions,
rank rows and markers commit together, so a reader in one read transaction
under the pinned snapshot sees them consistently.

### Query

`CompletedTraceQuery` and its tests stay unchanged. `aggregateSources` and
`traceCandidateSource` bind `trace_candidates` as the union of:

- rank rows of the day, hour and minute buckets covered by the window,
  splitting it greedily into the largest covered buckets. A bucket is covered
  when it starts at or after the window start and ends at least one minute
  before the window end, because a short trace can extend up to a minute past
  its bucket. Every short trace in a covered bucket is then fully inside the
  window, as the index path requires (`min_start >= S AND max_start < E`);
- an exact candidate scan of the uncovered minutes at each edge;
- an exact scan of long traces (`max_start - min_start` of one minute or more)
  inside the window, a set bounded by how many long traces exist;
- the existing partial-trace and uncached-tail sources.

A limit above `TraceRankDepth`, or a bucket that query-time repair has marked
dirty, falls back to the exact candidate scan for those buckets.

Rows read per request are about (day + hour + minute buckets) × groups × 21,
plus the edge minutes, the long traces and the tail. For a 24h window that is
at most about 2 + 46 + 118 buckets, independent of retention.

### Remaining scan

The `touched` stage still needs the cached rows of traces that also appear in
the uncached tail, and the boundary `partial` filter still reads the index.
Neither has an index today: a per-trace index ran out of memory, and
`views_test.go` asserts that the read tables have none. The first build
bounds both by pruning `read_trace_candidates` on `min_start` within the tail
and edge time ranges. Phase 2 measures whether that is enough.

### Versioning

The rank table changes read-cache schema and semantics, so `readCacheVersion`
goes from 19 to 20, and a mismatch rebuilds every private read table.

## Phases

1. **Harness, done.** The scale benchmark above, with full cache refresh and
   a two-service toggle. The pitot-bench `--prefill` phase
   (labstack/pitot-bench#14) runs measured reads on a populated server.
2. **Minute tier.** `read_trace_rank` with minute buckets only, maintenance
   in `cacheBatches` and retention repair, and query binding. Gate: the
   existing exactness suite (`trace_candidate_window_test.go`) passes
   unchanged, and an added oracle test compares random windows, scopes and
   limits against raw spans after late publication, compaction and retention.
   The scale benchmark must show drill-down time growing no more than the
   number of minutes in the window between 1.6M, 16M and 30M traces.
3. **Hour and day tiers.** Gate: a 24h window costs under 50 ms at 30M
   traces in the scale benchmark, and refresh cost per batch rises by no more
   than 50%.
4. **End-to-end gate.** pitot-bench on Hetzner with a full-day prefill (about
   4B rows; about 4 hours of prefill on a ccx33 pair), then 48k items/s with
   20 reads/s. The read gate must pass. A 10-minute prefill is not enough,
   because it holds about 1/20 of a day's data.

## Alternatives

- **Rewriting the joins.** No gain once the index is fully cached.
- **Per-batch top-K.** Inexact, because a trace split across batches has a
  merged duration that no batch's partial shows. Its cost also grows with the
  number of batches.
- **Threshold algorithms (Fagin TA/NRA) over per-batch partials.** Unseen
  batches can extend any trace, so the only available upper bounds are
  window-wide and too loose to stop early.
- **Ordered index on the score.** Exact, but DuckDB indexes do not serve
  ordered limit scans, and a per-trace index already ran out of memory.
- **Sampling, or Jaeger-style per-bucket `LIMIT N*k`.** Approximate, with no
  bound on missing the true top K.

## Risks

- Maintenance cost. Measured on a simulated table: 7–12 ms per 40k-part
  batch, against about 25 ms per batch for today's refresh. Recomputing only
  touched buckets should be cheaper; phase 2 measures it.
- Long traces bypass the lists. A workload of mostly long traces degrades
  toward today's cost. Phase 2 reports their share in the benchmark and on
  Hetzner.
- Planner estimates. Bad cardinality estimates already reversed a hash build
  in the exemplar path. Bound the new sources with explicit predicates and
  check the plans in tests.
- Materialization. DuckDB 2 may factor repeated sources into materialized
  subplans regardless of hints. Keep the merged rank source small enough that
  materializing it is cheap.

## Open questions

- Should the one-minute short-trace threshold follow the bucket width or be
  measured from real span-duration distributions?
- Is zonemap pruning on `min_start` enough for the tail join, or does the
  tail need its own bounded structure?
- Rank depth: 21 fits both callers. Should the trace search page reuse the
  lists at a larger depth?
