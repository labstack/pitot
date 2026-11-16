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

## Research validation

This proposal has not yet had the three-angle research pass (prior art,
in-codebase fit, performance) that precedes a build. Admission-control and
cost-based class scheduling have strong prior art (database workload management
and admission controllers) that must be surveyed and mapped onto the current
slot and gate machinery before implementation. This is a design-heavy change and
should not be built as a single pass without that validation.
