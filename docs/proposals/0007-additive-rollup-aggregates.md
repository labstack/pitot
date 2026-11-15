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
