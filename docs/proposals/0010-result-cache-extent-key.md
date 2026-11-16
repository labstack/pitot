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

This proposal has not yet had the three-angle research pass (prior art,
in-codebase fit, performance) that precedes a build. The prior art named above
(Mimir/Cortex/Loki query-frontend extent cache, Druid per-segment cache) comes
from the audit; it must be validated against the current result-cache and
watermark code before implementation, and the TTL-only first step measured
separately.
