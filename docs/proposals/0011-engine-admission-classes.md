# One cost-aware admission layer at the engine boundary

Status: Draft
Date: 2026-11-15
Pull request: TBD

## Summary

Put every read through one admission layer at the engine boundary, with a small
set of classes (interactive, panel, background, agent) that shed by deadline
rather than by connection count, and give each panel run a single pinned
snapshot and connection shared by its sub-reads. Today only some readers are
admitted, a slot is just a connection with no notion of thread or memory cost,
and the single background slot lets one rollup pass block the read-cache
refresh.

## Motivation

Admission is partial and not cost-aware (`internal/query/exec.go:120-126` and the
engine's slot machinery). The engine audit found three structural problems:

- **Admission is not global.** Only panels, exemplars, and trace detail pass
  through admission. The observability readers
  (`internal/observability/{overview,topology,dependencies,trace,health_trend}.go`),
  annotations, and variable-option queries (`internal/query/variables.go:70-73,107`)
  call the engine directly and compete for the two spare connections and all
  DuckDB threads without being admitted or shed. (See issue: "read admission is
  neither global nor cost-aware".)
- **A slot has no cost.** A slot is one connection, with no notion of the thread
  or memory a statement will use, so a cheap point read and a wide multi-day scan
  count the same against the pool.
- **The background slot is a single choke point.** `backgroundSlots` has
  capacity 1 (`internal/engine/duck.go:540`), so a rollup pass blocks
  `refreshReadCaches` for its full duration. This contradicts the stated intent
  that completed-batch writes have their own gate and slot.

## Goals and non-goals

Goals:

- Every read, whatever its entry point, passes through one admission layer, so
  load is actually bounded and shedding is uniform.
- Classes with distinct policies: interactive and panel reads are latency-first
  and shed by deadline; background and agent work yields to them.
- Admission that accounts for cost (threads, and a memory estimate) rather than
  only a connection count.
- One pinned snapshot and one connection per panel run, shared by its sub-reads,
  so a panel's several statements see one consistent catalog and do not each take
  a separate slot.
- Background work (rollups, read-cache refresh) that no longer serializes behind
  a single slot where the design calls for separate gates.

Non-goals:

- Not a general query scheduler or priority preemption of running statements in
  the first phase; shedding is at admission (by deadline), not mid-flight
  cancellation beyond what the existing deadlines already do.
- No change to the pinned-snapshot correctness contract or the write gates'
  semantics; this reorganizes who waits, not what a reader sees.

## Design

**One admission layer.** Route every engine read — panels, exemplars, trace
detail, the observability readers, annotations, and variable options — through a
single admit call at the engine boundary. Nothing reaches a connection without
being admitted.

**Classes.** Define a small set: interactive (health checks, variable options,
single-trace detail), panel (dashboard panel runs), background (rollups,
read-cache refresh, compaction-adjacent reads), and agent (LLM-driven queries).
Interactive and panel are latency-first and carry a deadline; admission sheds
them when the wait would exceed the deadline rather than queueing unboundedly.
Background and agent yield to interactive and panel.

**Cost awareness.** Admit against thread and memory budgets, not only a
connection count, so one wide scan does not crowd out several cheap reads and
the pool reflects what statements actually consume.

**One snapshot and connection per panel run.** A panel run pins one catalog
snapshot and takes one connection that all of its sub-reads share, so the run is
internally consistent and counts once against admission instead of once per
sub-read.

**Separate background lanes.** Give the rollup pass and the read-cache refresh
their own admission so a long rollup does not block the read-cache refresh,
honoring the "completed-batch writes have their own gate and slot" intent.

## Alternatives

- **Widen the existing slot pool.** More connections without classes or cost
  awareness defers the problem: a burst of wide scans still starves interactive
  reads, and background work still competes directly.
- **Admit only at the HTTP layer.** Admitting requests before they reach the
  engine misses the engine's own background work and the sub-reads a single
  request fans out into, which is where the contention actually is.

## Risks

- A single admission layer is a central chokepoint; its own overhead and
  fairness must be measured so it does not become the bottleneck it manages.
- Deadline-based shedding changes observed behavior: a read that used to queue
  and eventually run may now be shed. The classes and deadlines must be chosen
  so this only sheds work that would have missed its deadline anyway.
- A memory estimate per statement is approximate; a bad estimate could admit too
  much (OOM) or too little (idle pool). Start from a coarse, conservative
  estimate and measure.

## Open questions

- How is per-statement cost estimated before execution — from the compiled plan,
  the file count and window, or a learned estimate?
- Do agent queries get their own class with a separate budget, or share the
  panel class with a lower priority?
- How does this layer interact with the result cache's single-flight coalescing
  and with the write gates, so a reader is not admitted twice or counted against
  two budgets?

## Research validation (2026-11-17)

A three-angle research pass (prior art, in-codebase fit, performance) was run
before build, grounded in a fresh Hetzner `ccx33` benchmark of `v2026.11.8-alpha`
(`publish` preset, `--query-rate 20`) that reproduces #480: 3.0/20 ops/s
completed, 1546 errors, 3533 shed, p99 8644 ms; dashboard-refresh 837/1000,
drill-traces 19/1000, trace-detail 75/1000.

**Headline correction — this proposal is necessary but not sufficient for the
read gate.** Admission bounds concurrency and sheds; it cannot make a scan
cheaper. In the benchmark the box is already at its configured caps (7.9/8 cores
from thread oversubscription — up to `slots`≈10 scans × 8 DuckDB threads on 8
cores — and 19.4 GiB RSS, which is the `duckDBMemoryPercent=60` limit on 32 GiB
working as designed, not a leak). Reaching 20 ops/s needs ~6.7× more read
throughput, which scheduling cannot manufacture. dashboard-refresh is
rollup-backed and cheap (hence 837/1000); the hard failures are drill-traces and
trace-detail, which scan the **growing raw Parquet snapshot** and are cost-bound.
Those pass only once per-op read cost drops — i.e. **0010** (result-cache extent
reuse) and **0007** (additive rollups so drill/exemplar/trace reads hit an
aggregate instead of re-scanning). This proposal is the stability/fairness layer
that makes the box degrade gracefully while those land. Its own exit test is
**errors → ~0, no thrash pegging, dashboard-refresh goodput up, ingest flat** —
not 20 ops/s, which binds to 0007/0010.

**Prior art.** The design maps onto established systems: CockroachDB elastic
admission control (Go; gate on runnable-goroutines/CPU and scheduler latency, not
connection count; a cooperatively-yielding elastic/background class), Druid query
laning + automatic prioritization (cost estimated from **segment count ×
interval**, a file-count + window heuristic — no plan analysis — with interactive
capacity guaranteed and background lanes capped to spare %), Trino resource
groups (the `slot = 1 connection` model is a documented production bug: one query
spawning many drivers saturates the cluster, so admission must carry a per-query
cost and a bounded queue with explicit reject), Grafana Mimir/Loki query
scheduler (round-robin fairness across classes; isolate the untrusted/agent class
like a misbehaving tenant), and LinkedIn Bouncer / Facebook epoch-LIFO (deadline
admission needs an estimated-wait model and must not shed mid-work-sequence).

**Current mechanics (in-codebase).** Two separate things exist. The only hard
engine-boundary bound is the read pool `SetMaxOpenConns` (`duck.go:552`), FIFO,
no classes, entered through `readConn`/`acquireRead` (`duck.go:2440-2455`). The
class-aware semaphore `readAdmission` (`admission.go`) lives only in
`query.Executor`, sized to `pool-2` (`sizing.go:56-73`), leaving two raw
connections for annotations and readiness. The collapse is structural: the
observability readers (`overview`/`topology`/`dependencies`/`trace`/
`health_trend`), annotations, and variable-options call `Duck.QueryContext`
directly and **never pass through `readAdmission`**, so a saturating dashboard
oversubscribes the pool (admitted `pool-2` panels + unbounded observability reads
→ `DB.Conn` blocks), defeating deadline shedding and even starving the two
reserved connections.

**Required revisions before build:**

1. **Bind the success criterion.** 0011's exit test is errors→~0 / no thrash /
   dashboard-refresh goodput up / ingest flat. The 20 ops/s gate binds to
   0007 + 0010 (read cost), not here. Do not benchmark 0011 against a target it
   structurally cannot hit.
2. **Overload signal is measured, not connection count.** Gate on DuckDB
   worker-thread saturation + measured admission queue-wait (+ runnable
   goroutines/CPU, available cheaply in Go). Delete "2 spare connections" as the
   control variable. **Under-admit relative to cores** (Cockroach #91536: even a
   perfect layer loses isolation if it admits more concurrent work than the
   thread pool runs without scheduler-latency blowup — do not admit 8 scans on 8
   cores).
3. **Cost estimate v1 = file-count-in-window × window-span** (Druid), bucketed
   into coarse cost classes (cheap rollup read / single raw-scan / multi-statement
   panel fan-out). Explicitly **not** a DuckDB plan (no reliable pre-exec memory
   estimate). Keep DuckDB's own `memory_limit` as the hard backstop (AGENTS: keep
   defaults). Compute the estimate from already-maintained catalog stats
   (`updateParquetStats`) + footer bounds, **outside** the admission mutex. Peak
   memory depends on query shape (GROUP BY / DISTINCT / VARIANT), not file bytes,
   so charge a class, not a precise prediction, and feed observed cost back to
   correct it (SQL Server grant-feedback). `applyMemoryHeadroom` already turns a
   misgrant into a clean Retryable error, not a crash.
4. **Charge per-class thread + memory budget and a per-query thread cap** (Trino;
   SQL Server `REQUEST_MAX_MEMORY_GRANT_PERCENT` analog) so one wide scan cannot
   serialize the box inside a single slot.
5. **Bounded queue + explicit reject** per class (panels/dashboard-refresh → a
   `ErrBusy` 503 + Retry-After the UI renders; agent → retriable). Never an
   unbounded wait.
6. **Admit the panel run as a unit.** The one pinned snapshot+connection shared by
   a panel's sub-reads is a work-sequence: grant admission once for the whole run
   against its total estimated cost; sub-reads inherit it and never re-queue
   (epoch-LIFO breaks sequences — a panel whose first sub-reads succeeded must not
   have later ones shed). This is also the **only** part of 0011 that reduces
   cost rather than bounding it (reuse one snapshot/scan across a panel's ~6-7
   serial statements — `runScope`, `totals`, previous-period, `attachTableTrends`,
   `annotationScope`, `applyMetricUnit`, `diagnose`; a 15-panel refresh is ~90
   statements today), so measure it by statements-per-panel and snapshot-cache hit
   rate.
7. **Deadline shedding must close long readers promptly.** A per-run shared read
   transaction extends reader lifetime, and a non-FORCE `CHECKPOINT` refuses while
   a reader that began before the last write commit is still open
   (`duck.go:131-141,781`) → unbounded WAL growth on a busy instance. Keep readers
   short or checkpointing starves. Capture the Parquet pin **and** the DuckDB MVCC
   read tx once at run start, or sub-reads tear across a publish/commit boundary.
8. **Preserve the existing `panelBatchAdmission` invariants** (lifetime-bounded
   `reserveBatch`, panels retain one progress slot, interactive borrows idle
   slots). Extract the pure semaphore core into an engine-owned type the engine
   gates on in `readConn`; keep the batch-ticket logic as a thin query-layer
   wrapper. A full rewrite risks re-introducing solved bugs (panel starvation,
   unbounded resume population, the #481 memory loop).
9. **Round-robin fairness across classes**; treat the agent/arbitrary-SQL class as
   the untrusted tenant with the hardest caps, killable by deadline (poison query).
10. **Keep the admit decision O(1) and lock-free during execution** (admit →
    release the admission lock → execute); per-class FIFO, not a global
    priority-heap reorder churned by thousands of sheds — or the central layer
    becomes the bottleneck it manages.

**Phased build sequence** (each independently shippable; later phases gated on
earlier measurements, A/B on chip x86_64 / Hetzner, never pico):

- **Phase 0 — make admission global (pure plumbing, no policy, ship first).**
  Route every bypass reader (`observability.{overview,topology,dependencies,
  trace,health_trend}`, annotations, `query.variables` options) through the
  existing `readAdmission` interactive lane via an admitting
  `queryrows.Queryer`/`ReadTransactioner` wrapper defined in package `query`
  (`observability` must not import `query`; `query` already imports it) and passed
  into `observability.New`/`annotations.New` at wiring time. The wrapper holds the
  slot **across row iteration** (release on `Rows.Close`, mirroring the engine's
  `lockedRows`) and admits via the caller's own deadline. This restores the pool
  invariant (admitted ≤ `pool-2` < pool), so a saturating dashboard can no longer
  starve `DB.Conn`; bypass reads are deadline-shed as `ErrBusy` instead of piling
  up, and refusals feed the existing `interactiveRefusals` → ingest backpressure.
  Wrapping the injected `db` fixes the MCP observability path too. Add the metrics
  every later phase needs: per-path queue depth, wait, admit/reject counts, DuckDB
  thread saturation, runnable-goroutines/CPU. `Duck.Trace` (snapshot-pinned,
  index-bounded, takes no pool connection) stays raw for now. This directly
  addresses the measured "unadmitted readers oversubscribe the pool" collapse and
  yields the clean baseline that quantifies how much per-op cost 0007/0010 must
  still remove.
- **Phase 1 — split the background choke.** `backgroundSlots` is one permit
  (`duck.go:35/680/692`) shared by rollups and the read-cache refresh even though
  they already write disjoint tables under disjoint gates (`writeGate` vs
  `cacheGate`). Give the read-cache refresh its own lane so a rollup pass cannot
  block it; maintenance/checkpoint must hold both lanes (as it already holds both
  gates); watch the doubled concurrent background memory (#481). Secondary:
  read-vs-background is not the primary bench contention (ingest is flat).
- **Phase 2 — classes + bounded queue + deadline shed.** Four classes
  (interactive/panel/background/agent), round-robin, per-class `maxQueued`,
  admit-time deadline shedding using the Phase-0 service-time EWMA; admit panels
  as a unit (revision 6).
- **Phase 3 — cost-aware admission (highest uncertainty, last).** file-count ×
  window cost estimate, per-class thread+memory budgets and per-query thread cap,
  observed-cost feedback. Ship behind the metrics proving Phases 0-2 recovered
  most of the collapse.

The per-run shared snapshot+connection (the one cost-reducing move) folds into
Phase 2/3 and carries the checkpoint-refusal hazard (revision 7), so it is not in
the Phase 0 slice.

**Build readiness.** Phase 0 is buildable now: bounded, additive, no policy, no
engine/snapshot/sizing change. Phases 2-3 are gated on Phase-0 production metrics
and per-phase A/B. The 20 ops/s gate stays owned by 0007/0010.
