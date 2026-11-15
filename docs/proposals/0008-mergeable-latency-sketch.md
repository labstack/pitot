# Mergeable latency sketch and derived service rollup

Status: Draft
Date: 2026-11-14
Pull request: TBD

## Summary

Stop storing final, unmergeable p50/p95 values in `service_rollup`. Derive
service latency percentiles, error rate, and counts from the mergeable
`span_rollup` histogram and counts at read time, so every surface computes the
same number and any window is re-aggregable. `service_rollup` becomes a thin
derivation (or is removed) rather than an independent summary.

## Motivation

`service_rollup` stores a per-minute t-digest result as final `p50_ms`/`p95_ms`
columns. A t-digest result cannot be merged across minutes, so each consumer
invents its own cross-minute combination:

- Alerts take an unweighted `avg(p95_ms)` and `avg(error_rate)`
  (`internal/alert/engine.go:520-530`).
- The overview takes `MAX(p95_ms)` (`internal/observability/latency_sql.go:15`).
- Dashboards merge the `span_rollup` histogram correctly
  (`internal/query/rollup_compile.go:75-81`).

The same service and window therefore reports three different p95 values. (See
issue: "three percentile merges disagree for the same service and window".)
The `span_rollup` histogram is already a mergeable, OTel-style scale-3
exponential histogram, so the correct value is already computable — it is simply
not used consistently.

## Goals and non-goals

Goals:

- One definition of service p50/p95/error rate, computed from mergeable inputs,
  used by alerts, the overview, and dashboards.
- Re-aggregable over any window without a stored final percentile.
- Remove the t-digest scan and the `COUNT(DISTINCT m.name)` aggregate from the
  rollup pass.

Non-goals:

- No change to the histogram layout or its bin semantics.
- No new precision guarantee beyond what the existing scale-3 histogram gives;
  this proposal makes the existing precision consistent, not finer.

## Design

Percentiles for a service over a window are read from the summed `span_rollup`
histogram for that service and window, using the same bin-interpolation the
dashboards already use. Error rate is `sum(error spans) / sum(spans)` from the
span counts. Log and metric counts come from `log_rollup` and
`read_metric_names`. Alerts, the overview, and dashboards call one shared helper
so the definition cannot drift again.

`service_rollup` keeps only the genuinely additive per-minute columns it still
needs as a convenience (counts), or is dropped entirely if nothing needs a
pre-joined per-minute row once percentiles are derived. The rollup tables are a
rebuildable disposable cache, so this needs no batch-format change.

## Alternatives

- **Store a mergeable sketch state** (DDSketch or ClickHouse-style
  `quantileState`) in `service_rollup` instead of deriving from the histogram.
  More storage and a second sketch to maintain, when the span histogram already
  carries the information.
- **Pick one of the three existing merges and apply it everywhere.** `avg` and
  `MAX` are both wrong; standardizing on a wrong merge is not acceptable.

## Risks

- The histogram's bin resolution sets percentile accuracy; confirm it meets the
  alerting and overview needs before removing the t-digest.
- Read-time percentile computation cost over long windows; bounded by the
  rollup grain and mitigated by the additive-partials proposal (0007).

## Open questions

- Keep a slim `service_rollup` for counts, or remove it and read counts from
  `span_rollup`/`log_rollup` directly?
- Does deriving percentiles change any alert threshold's observed behavior
  enough to need a migration note for existing alert rules?
