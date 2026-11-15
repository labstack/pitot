# Additive per-batch rollup aggregates

Status: Draft
Date: 2026-11-14
Pull request: TBD

## Summary

Replace the raw re-aggregation rollup model for the scan summaries
(`span_rollup`, `log_rollup`, `endpoint_rollup`, and the resource/receiver
summaries) with additive per-batch partial aggregates that are merged at read
and folded during compaction. Cost becomes proportional to newly ingested rows
rather than to the event-time span touched by a pass. Keep watermark
re-aggregation only for the rollups whose output is not a simple additive
merge of per-batch partials.

## Motivation

`refreshServiceRollup` discovers the affected keys for a pass, derives one
event-time hull `[min(bucket), max(bucket)+1m)`, and re-scans raw Parquet over
that hull once per key-chunk to rebuild every summary. This has three
structural costs, all surfaced by the engine audit:

- **A poison pill.** One span with an old `start_time` widens the hull to days,
  and the unfiltered `log_agg`/`metric_agg` statements then OOM or hold the
  write gate, freezing the watermark for every service-derived rollup.
  (See issue: "one late or clock-skewed span stalls every service-derived
  rollup".)
- **Cost = chunks × hull.** Unordered affected keys reuse the global `[lo,hi)`,
  and the `struct_pack(...) IN (...)` predicate does not push into the Parquet
  scan, so each 1000-key chunk decodes the whole hull. A 60k-key backfill is
  ~60 chunks × four full span scans.
- **Delete churn and a convention-only uniqueness invariant.** Each pass deletes
  its window and reinserts grouped rows, and correctness depends on every
  insert path keeping the delete-first/group-by discipline.

The repository already runs the additive model for `read_trace_parts` and
`read_resource_seconds` via `read_batches` markers and
`transferBatchCacheLocked` (`internal/engine/batch_cache.go:555-589`). This
proposal generalizes that proven mechanism to the summary rollups.

## Goals and non-goals

Goals:

- Make summary-rollup maintenance cost proportional to new rows.
- Remove the event-time hull, the IN-list chunking, the poison pill, and the
  per-pass delete churn for the summaries moved onto the model.
- Preserve exact aggregates, histogram bins, NOT NULL keys, and the existing
  read contracts.

Non-goals:

- No change to the canonical Parquet telemetry format in the first phase (the
  partials live in the disposable DuckDB read cache, which is already
  version-gated and rebuildable).
- No change to `edge_rollup`, `receiver_rollup`, or the top-128 route fold,
  which are join-shaped or order-dependent and stay on watermark
  re-aggregation.

## Design

Each summary rollup becomes a set of per-batch partial aggregates keyed by the
summary's logical key plus the contributing batch ID, written once when a batch
first enters the read cache, exactly as `read_trace_parts` is today. A read or
a compaction merges the partials additively:

- Counts and sums add directly.
- `min`/`max` take the extreme.
- Histograms merge by bin (the scale-3 layout already in `span_rollup.hist`
  is mergeable).

Because partials are additive, a duplicate per logical key is expected and
merges correctly, so the uniqueness invariant disappears for these tables and
the ART primary keys become unnecessary (already dropped; see the read-perf
work). A batch's partials are retired when the batch is compacted away, with the
compaction output contributing its own merged partials — the same lifecycle the
trace caches already follow.

A later phase can compute the partials in Go at `Repository.Commit` and persist
them as an immutable per-batch sidecar, removing DuckDB writes, gates, WAL and
checkpoints for these tables entirely, at the cost of a batch-format bump.

## Alternatives

- **Keep raw re-aggregation, fix the cliffs only.** Bucket-order the affected
  keys with per-chunk bounds and key-filter the log/metric aggregates. Removes
  the poison pill and most of the chunks × hull cost without a model change.
  This is the lower-risk fallback if the additive model does not pay off, and
  the two are not mutually exclusive — the cliff fixes can ship first.
- **Materialized views.** DuckDB has no incremental materialized views that fit
  this ingest pattern; emulating them would reproduce the current rescan.

## Risks

- Correctness of additive merges for every statistic, especially histogram bin
  alignment and the endpoint top-128 fold (which is why the fold stays on
  re-aggregation).
- Read-time merge cost for very long windows; mitigated by folding partials
  during compaction so a read never merges more partials than live batches.
- Cache-version and rebuild semantics must cover the new partial tables.

## Open questions

- Which summaries move in phase one (proposal: span and log first, endpoint and
  resource second)?
- Does the read-time merge stay within the panel latency budget at 30-day
  windows, or does it force the compaction-fold step into phase one?
- Measurement: A/B the additive model against the current model on non-pico
  data before committing.

## Research validation (2026-11-15)

A three-angle research pass (prior art, in-codebase fit, performance) was run
before build. Findings:

**Prior art.** This design is ClickHouse's `AggregatingMergeTree` with the
`-State` / `-Merge` combinators: write a partial aggregate state per batch,
combine states at read, fold states during a background merge. Apache Druid's
ingestion-time rollup is the same idea for the phase-2 immutable sidecar. The
repository already runs this pattern for `read_trace_parts` via
`transferBatchCacheLocked`, so there is an in-repo precedent, not only external
ones. Keeping only associative/commutative stats on the additive model and
leaving non-additive ones (endpoint top-128 fold, joins) on re-aggregation
matches ClickHouse and Pinot guidance exactly.

**Required revisions before build:**

1. **Make the compaction fold mandatory in phase one, not an open question.**
   ClickHouse's best-known production failure is "too many unmerged parts": a
   read-time `-Merge` over thousands of per-batch states is slow, and the whole
   design depends on background merges keeping the state count low. Phase one
   without the fold reproduces that slowness at long windows. The in-repo
   `transferBatchCacheLocked` already folds inputs into the compaction output,
   so commit to it from the start.
2. **State the correctness invariants explicitly:** exactly-once contribution (a
   batch's partial is counted once across retries via its `read_batches` marker,
   and a compaction output *replaces* its inputs, never adds to them); store
   `sum` + `count`, never `avg`, and divide at read for error rate and means;
   every batch histogram is at the same scale-3 layout so bin-merge is exact
   (reject or downscale-align anything else).
3. **Resolve the cross-gate snapshot design.** Moving span/log/endpoint rollups
   onto per-batch partials moves them from the writeGate/watermark world to the
   cacheGate/`read_batches` world, while `service_rollup` (0008) and
   `edge_rollup` stay analytical. A consumer that reads a span partial *and* an
   analytical rollup then spans two gates/snapshots and can see a torn view.
   AGENTS.md requires maintenance to hold both gates for checkpointing; the
   proposal must specify how a reader gets a consistent cross-gate snapshot.
4. **Name the residual.** Phase one keeps partials in the version-gated
   disposable cache, so a cache-version bump re-derives all partials from raw
   Parquet — the exact full-hull scan this removes, merely deferred to rebuild.
   Only the phase-2 immutable Parquet sidecar removes that scan permanently.

**Status confirmed.** The ART primary keys on `span`/`log`/`endpoint`/`service`/
`edge_rollup` are already dropped on `main` (`rollup_schema_version` 5), so the
"already dropped" premise holds; the additive model's duplicate-per-batch rows
will not violate a PK. Versioning: a `readCacheVersion` + `rollup_schema_version`
bump (rebuildable, no format bump, no Goose) for phase one; a batch-format bump
only for the phase-2 sidecar.

**Build readiness:** buildable after the cross-gate design (item 3) is specified
and the compaction fold is committed (item 1). Lower-risk fallback remains the
0007 "cliff fixes" (bucket-order affected keys, per-chunk bounds, key-filter the
log/metric aggregates) if the 30-day read-merge A/B blows the panel budget.
