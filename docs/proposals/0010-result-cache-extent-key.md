# Result-cache key normalization and an extent cache

Status: Draft
Date: 2026-11-15
Pull request: TBD

## Summary

Rework the dashboard result cache so a panel's cache key depends only on what
actually changes its result, and so the stable, already-computed part of a
relative window is reused instead of recomputed every refresh. Key on a
normalized panel spec, the resolved bucket interval, and only the variables the
panel references; align relative windows to the resolved step; and cache the
closed buckets older than the rollup watermark with a long TTL, re-querying only
the moving tail. This follows the query-frontend extent cache in Mimir, Cortex,
and Loki, and Druid's per-segment result cache.

## Motivation

The result cache (`internal/query/exec.go`, `internal/query/result_cache.go`)
rarely hits, so a single viewer re-runs DuckDB for work it just did. The engine
audit found several independent reasons, all in how the key is built and how
windows are chosen:

- **Raw pixel width in the key.** The key uses the raw viewport `Width`
  (`exec.go:417,478`, `result_cache.go:445-452`) even though `AutoInterval`
  (`bucket.go:40-52`) quantizes it to a handful of intervals. Every viewport
  width therefore starts its own DuckDB run, although most resolve to the same
  bucket grain.
- **Whole-dashboard hashing.** The key hashes the whole dashboard, so any edit
  invalidates every panel, and two identical panels on different dashboards
  never share a result.
- **All variables, not referenced ones.** The key includes every dashboard
  variable rather than only those the panel's query references, so changing an
  unrelated variable misses the cache.
- **TTL shorter than the refresh period.** The default TTL is 10s
  (`config.go:101`) against a 30s refresh (`exec.go:167-168`), so an entry
  expires before the next refresh reads it: a single viewer sees a 0% hit rate.
- **Unaligned relative windows.** Relative windows are not step-aligned, so no
  two refreshes cover the same buckets and the cache cannot reuse a prior run.

(See issue: "result-cache key fragments and TTL is shorter than the refresh
period".)

## Goals and non-goals

Goals:

- A cache key that changes only when the panel's result would change: the
  normalized panel spec, the resolved interval, and the referenced variables.
- Hits across refreshes of the same panel, across identical panels on different
  dashboards, and across viewport widths that resolve to the same interval.
- Reuse of the stable, closed-bucket portion of a relative window, recomputing
  only the tail that is still changing.

Non-goals:

- No change to what a panel computes or to the arbitrary-SQL safety boundary.
  Normalization is for keying only; it never alters the statement sent to the
  engine.
- No change to the rollup watermark or freshness semantics; the extent cache is
  bounded by the existing watermark, not a new freshness guarantee.

## Design

**Normalized key.** Build the key from a canonical form of the single panel's
query (spec fields in a fixed order, with display-only fields such as raw pixel
width removed), the interval that `AutoInterval` resolves the width to, and only
the variables the panel's query references. Panels that produce the same
statement over the same interval and variables share one entry regardless of the
dashboard they live on or the exact viewport width.

**Step-aligned relative windows.** Snap a relative window's start and end to the
resolved step before keying and querying, so successive refreshes of a "last 1h"
panel cover the same bucket boundaries and a refresh can reuse the previous
run's buckets.

**Extent cache.** Split a window at the rollup watermark. Buckets older than the
watermark are closed: their result cannot change, so cache them with a long TTL
keyed by the aligned extent. Re-query only the open tail between the watermark
and now on each refresh, and stitch the cached closed extent to the fresh tail.
This is the query-frontend extent cache used by Mimir, Cortex, and Loki, and
Druid's per-segment result cache. The TTL question above disappears for the
closed portion, because a closed extent is immutable.

**TTL.** Set the live (tail) TTL at least as long as the refresh period so a
single viewer's own refresh hits, while the closed extents carry their own long
TTL.

## Alternatives

- **Just raise the TTL.** Lengthening the TTL past the refresh period fixes the
  single-viewer 0% hit rate but leaves the key fragmentation (pixel width, whole
  dashboard, all variables, unaligned windows), so cross-panel and cross-refresh
  reuse still miss. The TTL change is the cheapest first step and can ship alone,
  but it is not sufficient.
- **Cache only whole-window results.** Simpler than an extent cache, but a
  relative window never repeats exactly, so a whole-window entry for "last 1h"
  is stale on the next refresh. Extents are what make a moving window reusable.

## Risks

- Normalization must be exact: two panels share an entry only if their
  statements and results are truly identical, so the canonical form must capture
  every field that affects the result and nothing that does not. A missed field
  would serve a wrong result.
- Stitching a cached closed extent to a fresh tail must preserve the exact
  bucket boundaries and any cross-bucket aggregation, or a boundary bucket could
  be double-counted or dropped.
- The watermark split must use the same watermark the rollups use, or a closed
  extent could cache a bucket that is still changing.

## Open questions

- Does the extent cache live at the result-cache layer (keyed extents of the
  frame) or deeper, and how does it interact with the result cache's existing
  single-flight coalescing?
- Which variables count as "referenced" — only those in the compiled statement,
  or also those in filters that were dropped as All?
- What is the measured hit-rate and latency improvement versus the TTL-only
  change alone, on non-pico data?

## Research validation

A three-angle research pass (prior art, in-codebase fit, performance) was run
before build, grounded in the current result-cache code and the
`v2026.11.8-alpha` read-gate benchmark.

**Headline correction — 0010 is a repeated-request / cross-viewer efficiency
change, not a lever for the sustained read-rate gate.** The gate fails on
`drill-traces` (19/1000) and `trace-detail` (75/1000). Neither is a time-bucketed
additive aggregate the extent cache can split and stitch: `trace-detail` is a
point lookup by `trace_id` and is already whole-result-cached end to end
(`cachedTraces.Trace`), and `drill-traces` is a ranking/dedup scan over the
window that does not decompose over per-bucket partials. The one op the extent
cache fits — `dashboard-refresh`, which reads the bucketed `service_rollup` — is
the cheap, already-passing op (837/1000). And result caching only helps
*repeated* keys: a load that reads varied traces gets a ~0% hit rate whatever the
TTL or key shape. So **0007 owns the two failing ops** (make drill/aggregate reads
hit rollups instead of raw scans; a `trace_id` point lookup is cost-bound on the
trace index regardless), **0011 owns p99 under pressure**, and 0010 improves
multi-viewer and refresh-cadence efficiency on top. State this in the proposal:
do not credit 0010 with gate throughput.

**Prior art.** The extent cache is the Grafana Mimir/Cortex/Thanos query-frontend
pattern: split a range query, cache answered extents, detect partial overlap,
re-query only the missing sub-range, stitch cached + fresh. Step alignment
(`align-queries-with-step`) is their precondition for caching a range at all.
Thanos `response-cache-max-freshness` (default 1m) is a time-based "do not cache
the moving tail" floor; pitot's rollup watermark is a better, data-driven version
of the same guard — keep both (`min(watermark, now − freshness)`). TimescaleDB
real-time continuous aggregates are the watermark split as a storage primitive
(`cagg_watermark`: read materialized below, compute live above) and carry the
late-data caution. Druid per-segment caching keys on an immutable boundary and
never caches the real-time segment. ClickHouse's query cache keys on the
canonical AST with display-only settings stripped and refuses non-deterministic
queries by default (the `now()` trap). Request coalescing
(`golang.org/x/sync/singleflight`) is the stampede guard — pitot already has an
equivalent (`cacheFlight`).

**Current mechanics (in-codebase).** `panelCacheKey`
(`result_cache.go:456`) keys on `Spec` (a sha256 of the *whole* dashboard JSON —
any panel edit busts every panel), `Vars` (*all* resolved variables, not the
panel's referenced set), and `Width` (the *raw* pixel width), confirming audit
items (a)-(c). `QueryCacheTTL` defaults to `10s` (`config.go:101`) against the
~30s refresh, so a lone viewer's scheduled refresh always ages out → cold miss
(item d). `AutoInterval` (`bucket.go:40-52`) quantizes width over a fixed ladder,
so a wide band of pixel widths resolves to the same interval — the resolved
interval, not the raw width, is the only width-derived input that changes a
result. A per-panel referenced-variable set is cheaply derivable from
`checked.Filters[p.ID]` → `Filter.Params` (plus SQL-AST params for arbitrary-SQL
panels, plus any measure params). The single-flight (`cacheFlight`,
`result_cache.go:80`) and LRU bounds (1024 entries / 64 MiB,
`sizing.go:47-48`) already exist. `TestPanelCacheKeyCoversRequest` guards the key
shape and must be extended with any change.

**Watermark — use the snapshot-consistent horizon.** The proposal says "the same
watermark the rollups use," but there are two: `RollupWatermark`
(`duck.go:2740`, an in-memory atomic resolved in a standalone preflight, *not*
tied to the panel's pinned Parquet snapshot) and `coverage.FreshTo()`
(`exec.go:751`, read inside the panel's read transaction). The extent split must
use the **snapshot-consistent** `coverage.FreshTo()`, or a closed extent can be
cached against a horizon the panel's own snapshot does not match. The freshness
signal is resolved today only for rollup-planned panels (`exec.go:731-749`); raw
spans/logs panels have no exposed ingest horizon, so for them the closed boundary
must come from footer event-time bounds or the split falls back to off.

**Required revisions before build:**
1. **Reframe the contribution** (above): 0010 is cross-viewer/repeated-request
   efficiency; the failing ops belong to 0007; measuring ~0 gate movement from
   0010 alone is the expected, confirming result.
2. **Split Feature 1 (key + TTL normalization) from Feature 2 (extent cache).**
   Feature 1 is cheap and independently valuable. **Gate the extent cache behind
   0007** — it only pays off once there is an additive aggregate to split, and it
   needs the watermark/safety-lag boundary to exist.
3. **Resolve and step-align the relative window before it touches the key or the
   query** — one operation, not two. The key carries the resolved,
   interval-aligned `[start, end)`, never the literal range or an unresolved
   `now()` (the non-determinism trap).
4. **Closed/open boundary = `min(coverage.FreshTo(), now − safety_lag)`**, reusing
   the rollup's existing safety-lag, never wall clock, with a `max-freshness`
   floor as belt-and-suspenders against late data.
5. **Half-open `[start, end)` stitch at `floor(freshTo, interval)` on the
   `1970-01-01` bucket grid**, so the boundary bucket is counted exactly once
   (mirror the clip discipline in `limitBatchFrames`, `exec.go:619`); **recompute
   Totals over the full window** rather than stitching them (Totals are a separate
   no-bucket scan).
6. **Referenced-variable set must be the transitive closure** (a variable
   expression can reference other variables); under-computing it serves a wrong
   result. Keep and extend `TestPanelCacheKeyCoversRequest` with a property check
   (same reduced key ⇒ identical result under perturbation of the dropped fields).
7. **Bounded memory for extent entries.** A long TTL does not protect an entry
   from LRU eviction in the shared 64 MiB pool, and storing a whole-window result
   per refresh explodes entry count; store the closed-prefix aggregate once per
   `(panel, interval, referenced-vars)` and give extent entries a separate budget
   or reservation.
8. **Subscribe the extent cache to compaction/retention invalidation** (the engine
   already must invalidate retired contributions), or a closed extent will serve
   data for files compacted away.
9. **Key normalization is key-only.** It never alters the statement sent to the
   engine; arbitrary-SQL panels are key-normalization-only in the first phases and
   opt out of the extent split (they carry a `metricLookback` back-scan the split
   must respect).

**Phased build sequence:**
- **Phase 1 — TTL ≥ refresh (one line).** Raise the `QueryCacheTTL` default past
  the refresh period (`config.go:101`, e.g. `60s`); the stale-while-revalidate
  path already serves one background refresh. Converts a lone viewer from ~0% to a
  high hit rate. Zero code-path change.
- **Phase 2 — normalized key.** Replace `Width` with the resolved interval, scope
  `Vars` to the panel's referenced set, and (last, largest) move from the
  whole-dashboard hash to a per-panel spec hash. Pure key change; coarser keys
  make more requests coalesce onto one flight. Update the covering test.
- **Phase 3 — step-aligned relative windows** (`resolveWindow`,
  `timerange.go`), compatible with the `1970-01-01` grid and SQL `metricLookback`.
  Makes consecutive refreshes target identical bucket ranges (precondition for any
  extent reuse) and stabilizes the Phase-2 key across refreshes.
- **Phase 4 — extent cache (hardest, gated behind 0007).** Split at
  `floor(coverage.FreshTo(), interval)`, cache the closed half under a
  watermark-pinned long-TTL fixed-window flight, re-query only the tail, stitch
  half-open. Highest correctness surface (watermark race, boundary bucket,
  invalidation); gate on those tests and a measured long-window raw-aggregate cost.

**Risks (ranked):** (1) a watermark race caches a still-changing bucket and the
long TTL serves it wrong for the whole TTL; (2) stitch-seam double-count or drop;
(3) the referenced-variable reduction drops a result-affecting field; (4) LRU
eviction defeats the long TTL, or extent entries blow the 64 MiB budget; (5) the
step-aligned window is a visible semantic change interacting with the existing
relative-vs-fixed stamp logic.

**Measurement.** A/B one lever per branch against the same Hetzner ccx33 read
gate, never co-merged: 0007 should move `drill-traces`/`trace-detail`; 0010
should move the multi-viewer hit rate and `dashboard-refresh` cost but **not** the
failing ops; 0011 changes p99. Add a multi-viewer test (a single stream cannot
exercise cross-viewer sharing) and report the cache hit rate so a gain is not
mis-attributed.

**Build readiness.** Phases 1-2 are buildable now, cheap and low-risk, and ship
independently of 0007. Phases 3-4 follow, with Phase 4 gated behind 0007 and the
correctness tests above.
