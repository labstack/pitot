# Read performance under sustained ingest

Status: Draft
Date: 2026-11-14
Pull request: none yet

## Summary

Qualify Pitot by the ingest rate it can sustain while dashboards, searches and
trace investigation remain responsive. Keep the existing representative-data
targets of 500 ms p95 per warm panel and 1,500 ms p95 for a complete twelve-panel
dashboard. Add tail-latency, freshness and offered-load accounting to the
benchmark before treating any new run as evidence of mixed-load capacity.

The partial-window trace-candidate experiment was rejected: its SQL-kernel gain
did not survive completed-query measurement. The current implementation
includes exact checkpoint-conflict retries, explicit captured file lists, earlier
startup maintenance, service-rollup footer pruning and fresh fixed-window
coalescing. Projection-based histogram expansion has been reintroduced
with those changes after failing earlier backlog comparisons.
An atomically published committed service watermark removes the standalone
preflight lookup that blocked all five panel slots in a captured stall.

That runtime combination is **not qualified**: repeated runs still produced
backend busy responses and missed generator arrivals. A three-table analytical
cache-layout change is now retained after a
source-backed checkpoint lock diagnosis and deterministic native proof. Its
90-second comparison had no backend refusals, but two generator misses still
fail offered-load acceptance. Repeated representative qualification remains
outstanding. Faster successful reads do not erase earlier refusals; neither these local experiments nor the proposed numerical goals
establish deployed mixed-load capacity.

### Fresh fixed-window requests: share one later replacement

The fresh-result path exposed a separate, deterministic source of duplicate
work. When callers bypass stored results and wait for an older flight, its
replacement inherited the initiating caller's original arrival stamp. Other
waiting callers could therefore reject that replacement even though it was
registered after all their arrivals. For an absolute query window, they
requested the same data interval and could safely share that newer read.

The cache now records registration time separately for explicitly fixed-window
panel computations. Both the initiating computation and the joining caller
must have clock-independent windows. Effective request time overrides control
eligibility, and the full-admission-queue path uses the same rule. Every read
snapshot starts after registration. A caller arriving after registration must
still wait for another fresh computation. Original stamps continue to control
relative-window matching and stored-result TTL; no limits or TTLs are increased.
Generic cached operations and relative windows retain their previous behavior.

A real Executor regression holds an old native snapshot, commits late telemetry
and schedules two fresh callers before one replacement is registered. Before
the fix it executes three queries; after the fix it executes two. Both fresh
callers observe the late record, and the old response retains its original
snapshot. Relative-window controls retain three queries and their distinct
window ends. Additional cases protect request time overrides, later arrivals,
canceling one shared waiter, and joining through a full admission queue.
Mixed-load evidence for this fix is recorded separately from the earlier SQL
experiments and benchmark-control versions.

## Follow-through: measured read experiments and maintenance fix

Astra, AGY Carbon and Claude Fable were consulted independently. Their source
reviews identified hypotheses, not measured gains. We selected complete-query
profiling and a fixed-rate mixed-load experiment, including raw distinct-trace
panels, rather than accepting single-kernel timings or changing admission limits.
The other retention, footer-cache, compaction-layout and admission proposals
remain unimplemented pending measurements and correctness/resource review.

### Duration quantiles: measured gain, current candidate under evaluation

The projection candidate expands each row's map entries in a SELECT projection,
replacing correlated lateral `FROM source, unnest(map_entries(hist))`. Grouping,
totals, cumulative counts and the existing quantile interpolation remain the
same. Inputs have fixed internal schemas, so the new entry alias does not
collide with user measure aliases. No physical format, cache schema, resource
budget, admission policy or approximation change is involved.

A paired full-Executor experiment used one canonical generated snapshot,
disabled result caching, three warmups and 100 interleaved samples per variant.
Complete results matched across nine shapes. Extra canonical records made
otherwise undefined equal-value LIMIT ties unambiguous instead of weakening
the result comparison. Representative p95 measurements:

| Executor shape | Previous SQL | Projection UNNEST |
| --- | --- | --- |
| Latency time series | 14.92 ms | 8.33 ms |
| Quantile stat | 15.27 ms | 8.32 ms |
| Multiple quantiles | 17.61 ms | 10.35 ms |
| Ranked quantiles | 30.64 ms | 15.02 ms |
| Limited quantile table | 26.78 ms | 9.57 ms |
| Trends | 33.13 ms | 17.19 ms |
| Previous-period comparison | 23.48 ms | 14.64 ms |
| Protected raw-count control | 17.81 ms | 17.72 ms |
| Twelve-panel executor request | 60.07 ms | 54.61 ms |

An independent latency repeat measured 15.67 to 8.52 ms p95. Native profiles
explain the improvement: the lateral delimiter/hash join and INOUT_FUNCTION
are replaced by plain UNNEST, expanding the same 54,630 bin rows into the same
9,072 grouped bins and 144 result rows. Native peak buffer memory decreased
from 113.6 to 64.3 MB, with zero temporary-directory spill in both variants.
These engine-buffer figures are distinct from whole-process RSS. Native
correctness tests cover empty/null maps, zero bins, multiple quantiles, totals,
folded groups and raw-vs-rollup parity.

### Checkpointing: allow an older reader to drain

A native regression reproduced a maintenance failure even while both writer
gates were held: an older read snapshot plus newer committed WAL work makes
non-FORCE CHECKPOINT return its transaction-conflict diagnostic. READ ONLY
transactions exhibit the same native behavior. The previous assumption that
writer gates alone prevented this refusal was incorrect.

Checkpointing now retains both gates and one writer connection, retrying only
the exact typed conflict from the pinned engine. Retry admission and waits
have a 250 ms budget with context-aware 10 ms intervals. Native checkpoint I/O
keeps the original caller deadline; it is not given a new 250 ms execution cap.
Cancellation, unrelated errors and exhausted retries remain explicit failures.
Metrics count one logical checkpoint, including retry wait, rather than one
failure per transient attempt. No FORCE operation interrupts active readers.

Tests prove success after the old reader drains, both gates remaining held,
bounded explicit exhaustion, cancellation, unrelated-error propagation and
unchanged native execution deadlines. The tradeoff is up to 250 ms additional
contention wait holding writer gates; mixed acceptance must inspect ingestion,
cache/compaction debt, freshness and memory as well as read latency.

### Mixed-load methodology

The opt-in mixed harness uses a fixed populated 24-hour history, warmed
analytical and completed-batch caches, the real Executor, and background
ingestion/rollups/compaction. Requests explicitly bypass result caching; normal
in-flight coalescing remains enabled. Each arrival is one of a single quantile
panel, a four-panel request, or the existing varied twelve-panel dashboard with
three raw distinct-trace panels. Exact counts and rankings are checked.

The historical v5 comparison fixes 4 GB native memory, four native threads, eight read
connections, 32 client workers and queue capacity 64 across all rates. It ran
on the local Apple M3 Max, not an isolated four-vCPU host. It offered 20, 40, 80
and 160 operations/s for 15 seconds each, with 2,048 spans and 2,048 logs per
live batch at five commits/s. Actual accepted ingestion is reported separately
from the 20,480-item/s target. Maintenance runs at an explicitly accelerated
five-second interval to expose overlap; the product default remains one hour.

The scheduler records every offered slot, missed arrivals, client queue-full
events, backend busy responses, errors and during-load/drain completions.
Successful latency starts at scheduled arrival. Background operational errors
invalidate a run; shutdown cancellation does not. Peak RSS, cache markers,
compaction debt and analytical lag are reported separately. Fewer than 1,000
successful samples per kind cannot establish the proposed p99 qualification.
Short synthetic probes do not replace representative production replay or a
long sustained qualification run.

### Current runtime candidates and mixed-load result

Startup maintenance now starts before the initial analytical rollup rather
than waiting for it to finish. Regular regressions hold the analytical gate
and verify independent compaction, exact raw reads, eventual completed-cache
and analytical convergence, retention and cancellation. An opt-in cold-start
guard also exercises compaction into an output larger than the incremental
cache row budget before the analytical gate is released. These are lifecycle
and correctness guards, not capacity measurements.

Service rollup first discovers affected buckets from its captured catalog, then
rebinds that **same captured catalog** using each signal's event-time footer
bounds for the complete affected-minute interval. Tests protect late records
repairing an old minute, disjoint span/log/metric bounds, unknown statistics,
nanosecond half-open boundaries and captured inputs surviving publication,
compaction and retirement. This does not refresh the catalog mid-transaction
or replace event time with ingestion time.

After earlier maintenance and explicit file lists, projection histogram SQL
and fixed-window coalescing were combined for new mixed-load checks. With
786,432 historical spans and 786,432 logs, initial compaction debt of 257,
40 scheduled operations/s and approximately 20,200 accepted ingest items/s,
the first combined run completed 600/600 reads; single- and twelve-panel p95
were 10.4 and 71.5 ms. That success did not repeat cleanly: a later run completed
597/600 successfully, with transient backend busy responses, single-panel p95
12.514 ms, twelve-panel p95 136.971 ms and accepted ingestion 20,207 items/s.
Successful-read tails are encouraging, but the refusals fail the zero-error
goal and each short run has insufficient per-kind samples for p99 qualification.

The committed-service-watermark candidate publishes only after successful
service-rollup, startup rewind or skip-to-latest commits. It leaves all
snapshot-bound coverage SQL intact. Five regression tests pass with the race
detector, including exhausted read-pool access, cancellation, uncommitted and
failed refreshes, successful retries, restart and concurrent readers. The old
getter fails the exhausted-pool regression.

The indexed combination's 90-second result still failed offered-load acceptance.
Its comparison with the subsequent cache-layout change is recorded below;
neither run establishes an isolated watermark speedup.

### Checkpoint scan lock and retained analytical cache layout

Pinned DuckDB 2's scan initialization can acquire the database's shared vacuum
lock before deciding whether an ART index can answer a filtered scan. A
compound-key index can therefore introduce a wait even when the final operation
is a sequential scan. The acquisition precedes the index-probe outcome in
[TableScanInitGlobal](https://github.com/duckdb/duckdb/blob/v2.0.0/src/function/table/table_scan.cpp#L937-L987).
A full checkpoint acquires the exclusive side and retains its guard across the
checkpoint in
[CreateCheckpoint](https://github.com/duckdb/duckdb/blob/v2.0.0/src/storage/storage_manager.cpp#L703-L754).
This supplies a concrete mechanism for the observed checkpoint-associated
scan-initialization waits; it does not attribute every busy response to that lock.

The regular native regression
[checkpoint_scan_lock_test.go](../../internal/engine/checkpoint_scan_lock_test.go)
uses persisted, identical NOT NULL tables, with a compound primary key on only
the control. After committed WAL-producing updates, ordinary CHECKPOINT enters
a test-only one-second debug phase. A newly flushed WAL checkpoint record
establishes the phase; the control's native execution frame establishes that
it has started. The heap range scan returns all 96 expected rows while both
checkpoint and indexed control remain pending. The control returns the same
96 rows about one second later. No guessed startup delay or production setting
change establishes this result. It proves the physical-design distinction,
not a mixed-load operating envelope.

The retained implementation removes compound primary keys only from `span_rollup`,
`log_rollup` and `endpoint_rollup`, preserving every key column's NOT NULL
constraint. It moves uniqueness responsibility to the existing serialized,
transactional delete-and-rebuild algorithm: delete the complete affected keys,
then insert aggregates grouped by each complete key. It therefore removes the
scan-only tables' index-probe path rather than changing query results or
admission limits. Tests check complete-key uniqueness and full raw-aggregate
parity across late repair, publication retries, repeated refreshes, failed
insertion rollback, affected-key chunks and endpoint overflow/literal-route
collisions. Other indexed cache tables retain their existing constraints.

The first 30-second overlay run completed 1,200/1,200 offered reads, with
single-panel p95 8.523 ms and twelve-panel p95 63.610 ms. A subsequent local
90-second comparison used the same frozen 40-operations/s workload and resource
budgets as the prior indexed run, with the other fixes held constant:

| 90-second local result | Compound primary keys | Three scan-only heaps |
| --- | --- | --- |
| Offered / dispatched / successful | 3,600 / 3,592 / 3,583 | 3,600 / 3,598 / 3,598 |
| Backend busy / generator misses | 9 / 8 | 0 / 2 |
| Single-panel p95 / p99 | 32.23 / 91.61 ms | 12.02 / 16.45 ms |
| Four-panel p95 / p99 | 50.47 / 143.73 ms | 15.15 / 21.58 ms |
| Twelve-panel p95 / p99 | 275.21 / 543.80 ms | 74.14 / 106.39 ms |
| Accepted ingest items/s | 20,480 | 20,480 |
| Ending compaction debt | 0 | 0 |
| Ending analytical lag | 1,000 ms | 1,401 ms |
| Process-lifetime peak RSS, including setup | 1,171 MiB | 1,075 MiB |

Neither run reported background operational errors. Each read kind has enough
successful samples for the p99 sample guard; percentiles describe successes
only. The heap run's two unissued generator arrivals still make the complete
harness **fail** zero-loss offered-load acceptance. Misses are distinct from
backend busy and do not identify a backend capacity ceiling. These single local
before/after runs support the mechanism and retained change, but establish
neither a stable 40/s operating envelope nor repeated representative capacity.

The disposable cache versions advance to read-cache 19 and analytical 5.
Upgrading triggers a one-time rebuild of derived caches while preserving
canonical telemetry and anomaly history. A new upgrade regression checks those
boundaries and passes on the retained schema. The full engine and query suites,
targeted race tests, opt-in harness compilation, generated-document checks,
formatting and behavioral test-name checks pass. The cold-start guard also
passes with 262,145 exact raw/candidate rows, complete cache markers and
watermarks, and no retired contributions. These correctness checks do not
change the mixed run's offered-load failure classification.

### Controlled broad-file source comparison

The native control holds the selected file set and pinned transaction constant,
uses three warmups and 100 interleaved executions per strategy and scope, and
compares the previous broad glob plus filename predicate with an explicit
captured list. The fixture uses 32 canonical spans per file, four native
threads and a 4 GB native memory budget. All-scope results were:

| Captured files | Glob median / p95 | Explicit-list median / p95 | Mean process CPU, glob / list |
| --- | --- | --- | --- |
| 64 | 12.06 / 13.69 ms | 11.85 / 12.69 ms | 52.7 / 47.1 ms |
| 384 | 124.95 / 139.82 ms | 53.25 / 60.74 ms | 409.2 / 254.5 ms |
| 1,024 | 652.30 / 694.46 ms | 119.68 / 127.28 ms | 1,741.9 / 672.3 ms |

Service-scoped results showed the same broad-selection improvement. Every
execution matched the complete Go oracle, including exact distinct traces,
nanosecond bounds, duration and error totals. This measures native preparation,
execution and row consumption; it excludes Go snapshot preparation, HTTP,
ingestion, maintenance and browser time. Process CPU comes from getrusage;
unavailable native profile fields provide no operator attribution.

Runtime now uses the explicit captured list for broad selections as well as
narrow ones. Lifecycle tests retain the old captured source across late
publication, compaction and retention while independent current reads change.
UTF-8 paths, SQL escaping and empty-source schemas remain protected. The
controlled native gain supports this change; it alone does not prove a
mixed-load capacity gain.

### Historical comparisons and measurement limitations

Earlier projection-only tests improved small-history single-panel tails and
completed 1,200/1,200 requests at 80 operations/s. Both variants failed at
160/s. Larger-history backlog A/B/B/A runs instead produced 8 and 1 backend
busy responses with previous SQL versus 50 and 14 with projection SQL;
twelve-panel p95 was 490–644 versus 570–987 ms. With history compacted before
timing, all four runs completed 600 requests and projection improved individual
panels. Projection was rejected under that scheduling/source configuration;
its current reintroduction requires new paired evidence. A weighted-histogram
alternative increased high-diversity CPU and memory without a latency benefit
and remains rejected. The partial-window trace rewrite below also remains
rejected.

The first harness versions omitted the application's foreground-pressure
callback for bounded background yielding. Correcting that wiring did not remove
catch-up failures. Explicit lists alone also failed to eliminate refusals in
initial mixed probes. A metadata-cache-only experiment failed its mixed probe
and is not retained. Those observations must not be described as clean current
baselines or proof that any one component caused all contention.

Subsequent measurements keep read-pressure wiring and resource budgets fixed,
record offered slots, refusal timing and maintenance state, and use an
automatically cleaned temporary fixture. RSS is process-lifetime peak getrusage
RSS including setup, distinct from earlier phase-sampled current RSS. Substantial
unrelated workstation CPU/I/O and varying generator misses limit causal claims
from small differences. Typed native interruptions are excluded only after
explicit background shutdown intent and cancellation; running-phase interrupts
and unrelated shutdown errors still invalidate a run.

### Retained-file preparation experiment

A separate bounded native experiment held selected data constant at four
files, 128 spans and 64 traces while adding 256, 1,024 and 4,096 unrelated
canonical files. Raw complete-query median rose from 3.97 to 4.86 ms;
completed-candidate median rose from 5.15 to 6.86 ms. Candidate binding rose
from 0.88 to 2.63 ms while bound native execution stayed approximately
4.3–4.6 ms. This identifies a modest preparation cost at larger file counts,
not a demonstrated need for a new cache. The twenty-sample exploratory stages
are not additive and exclude HTTP, ingestion and cold-disk behavior. Marker
and snapshot reuse must preserve same-transaction retirement and scope repair;
no runtime metadata-cache change is retained from this experiment.

## Earlier trace-candidate experiment: rejected

Astra reviewed the approach and required a real completed-query comparison
against raw telemetry before retaining a runtime change. The implementation
adds that lifecycle oracle and an opt-in four-way plan comparison. The oracle
compares identity, order, error status and aggregate bounds across namespaces,
services, signed nanosecond boundaries, uncached/acknowledged late publications,
compaction and retention. A surviving late trace prevents empty lifecycle
comparisons from passing vacuously.

The diagnostic fixture uses 20,000 shuffled trace identities, 80,000 parts,
four real Parquet batches, mixed-service traces, and boundary spans on both
sides of the window. DuckDB 2.0.0 uses two threads and a 244.1 MiB resolved
memory cap. Each plan has three warmups and 100 interleaved samples. Every
variant must match the raw ranking before timing. These numbers time execution
of the fully bound completed query on one pinned snapshot; they exclude
binding, HTTP and browser time.

| Boundary traces | Service scope | Existing inline p95 | Rewrite inline p95 | Rewrite shared p95 |
| --- | --- | --- | --- | --- |
| 1% | All services | 12.34 ms | 13.30 ms | 12.61 ms |
| 1% | API, including mixed-service traces | 15.86 ms | 21.20 ms | 19.47 ms |
| 10% | All services | 12.64 ms | 13.56 ms | 12.79 ms |
| 10% | API, including mixed-service traces | 16.41 ms | 23.13 ms | 21.37 ms |

Sharing the old candidate expression also provided no consistent material gain.
The rewrite increased mixed-service p95 by roughly 34–41% with inlining;
sharing still left it slower than the existing plan. Therefore neither the
rewrite nor a materialization-policy change is retained in product code.
The earlier standalone SQL measurements below remain exploratory history,
not an achieved improvement. The opt-in experiment preserves the rejected
alternative so future changes can reproduce this result:

```sh
rtk bash scripts/with-duckdb.sh go test ./internal/engine -tags=filebench \
  -run '^TestTraceCandidateWindowPlanComparisonBenchmark$' -count=1 -v
```

Benchmark-tool repairs are independent of that negative performance result:
the format-5 replay and tagged compilation path are repaired in this repository,
and scheduled-arrival accounting and per-kind HTTP tail gates are implemented
in Pitot Bench. HTTP dashboard completion remains distinct from browser last-panel
paint. Short sweeps with fewer than 1,000 successes per kind cannot qualify
the proposed p99 goal; they must retain an insufficient-evidence verdict.

The repaired executor microbenchmark completed with 100 warm samples per shape
on generated data: 196,608 spans and 196,608 logs across 192 Parquet files, no
metric data. Analytical summaries were populated before timing and the in-memory
result cache was disabled. On an Apple M3 Max (macOS ARM64), DuckDB 2.0.0 with
14 threads, a 20.5 GiB resolved memory cap and 16 read connections reported:

| Generated-fixture shape | p95 executor latency |
| --- | --- |
| Distinct trace count, all services | 17.46 ms |
| Distinct trace count, one service | 18.42 ms |
| Distinct trace count, query-variable selection | 16.93 ms |
| Span count | 2.08 ms |
| Log count | 2.42 ms |
| Per-service duration p95 | 13.28 ms |
| Complete twelve-panel executor request | 55.32 ms |

No measured shape exceeded the microbenchmark's reported 500 ms panel target.
The fixture is small, synthetic, warm and local; these are not improvements
over a measured earlier product version, representative-capacity evidence,
browser paint times or sustained-ingest results. Metric shapes were skipped
because the generated fixture contains no metrics. The improvement here is
that the harness builds and measures populated current-code query paths.

## Motivation and evidence

The [published November 6 run](https://github.com/labstack/pitot-bench/tree/main/published/2026-11-06-ccx33x2-v2026.11.0-alpha)
measured v2026.11.0-alpha at 507,311 mixed items/s on a Hetzner ccx33 with a
separate generator. That is an ingest-only result using synthetic telemetry.
Its unit is individual telemetry items, whereas the historical 20/s read load
counts user operations, including dashboards that issue multiple queries.
Twenty operations/s was a selected test point, not a measured read ceiling.
Read capacity also depends on the scanned history and query shape; dividing
500,000 by 20 does not compare equivalent work.
[Issue #419](https://github.com/labstack/pitot/issues/419) recorded read p95 of
7.4 seconds, errors and shedding during sustained partial-load ingest.
[PR #423](https://github.com/labstack/pitot/pull/423) subsequently fixed the
service drill-down scan responsible for those failures. Its local median
measurements do not establish current sustained-load p95 or p99.

This investigation inspected product revision
`74f2157dd1fa2981ca326fdf7aadaecb315e1762` and benchmark revision
`04ae89d2597789bb7bd7d67c001beda7b1cdba3f`. Historical issue descriptions are
leads to verify against current code, rather than descriptions of today's
architecture. No representative workload was run in this investigation.

## Goals and non-goals

Qualify a declared hardware, retained dataset and query mix together. Protect
interactive investigations while maintaining bounded compaction debt, cache
backlog, analytical lag and process memory. Publish failures alongside latency.

These goals do not promise a latency for arbitrary SQL, arbitrary retention,
unbounded traces or every hardware size. They do not authorize changing storage
semantics, approximating exact queries, provisioning benchmark hosts, or raising
admission/time limits to make an existing workload pass.

## Design

### Proposed acceptance envelope

Use an 8-vCPU/32-GB host and separate generator for continuity. Record disk,
engine version, all resource limits, retained rows/bytes/files, actual populated
event-time bounds, cardinality, query limits and cache state. Populate the full
24-hour fixture before claiming a 24-hour read result.

The first qualification load is 20 scheduled mixed user operations/s. A dashboard
refresh is one user operation that fans out into panels and annotations; it is
not one panel query. Publish both operation and statement counts. Keep the
existing workload for comparison and add separate first-page log/trace search
coverage. Freeze the exact queries, limits, mix and selectivity before testing.

| Read class | Baseline p95 / p99 | Stretch p95 / p99 |
| --- | --- | --- |
| Warm individual dashboard panel | 500 ms / 1 s | 250 ms / 500 ms |
| Twelve-panel, 24-hour dashboard, trigger through last visible panel painted | 1.5 s / 3 s | 1 s / 2 s |
| Bounded trace-ID detail lookup | 300 ms / 750 ms | 150 ms / 300 ms |
| Selective drill-down or bounded first search page | 750 ms / 1.5 s | 300 ms / 750 ms |

Only the panel and dashboard baseline p95 values are existing acceptance
criteria in [testing.md](../testing.md#dashboard-performance-measurements).
The other values are proposed engineering goals; attainability under mixed
load is unproven. CI's calibrated 1,800 ms dashboard ceiling remains separate.
Report cold-engine, cold-result-cache and warm-result-cache runs separately.
Large scans get a fixed query-specific budget and separate completion reporting.

Within the qualified envelope, require zero observed valid-request errors,
timeouts or shedding, correct results, no dropped acknowledged data or
restarts, and the scheduled operation rate to be sustained. Short tests with
zero errors are observations, not production availability guarantees.

Measure capacity jointly: report the highest accepted ingest rate that also
meets these read goals. Seventy percent of isolated ingest throughput is a
test point, not established headroom. Under deliberate overload, report early
retryable refusals, successful goodput, bounded queues/memory and recovery
time separately; refused operations never count as fast successful reads.

### Freshness and correctness

Use uniquely identified sentinel records to test visibility after durable
acknowledgement through raw reads with result-cache bypass. Record publication
to visibility latency and fail missing acknowledged data. Existing raw-cache
and uncached-tail visibility semantics must remain exact.

The default result-cache TTL is ten seconds. Report cached result window age,
cache outcome and refresh cadence with latency; a fast cached response does
not prove a newly ingested event is visible. Respect the configured TTL rather
than assuming every deployment uses the default. Measure analytical watermark
lag separately from raw visibility and event-time lateness. Set a numerical
analytical freshness goal after recording current scheduling and recovery
behavior; do not invent an unsupported sub-cycle freshness promise.

### Measurement requirements and implemented repairs

Scheduled-arrival accounting and per-kind HTTP gates are implemented in the
benchmark working tree; published legacy runs retain their original semantics.
Browser measurement and representative qualification remain outstanding.

1. Start client latency at the operation's scheduled arrival, retaining
   scheduler queue wait, HTTP time, retries and completion. The inspected baseline
   [scheduler](https://github.com/labstack/pitot-bench/blob/04ae89d2597789bb7bd7d67c001beda7b1cdba3f/internal/bench/query.go#L56)
   starts timing after dequeue. Track successful, failed, timed-out and shed
   populations separately, with all offered operations as the denominator.
   Derive expected arrivals from elapsed time and the configured rate so dropped
   scheduler ticks cannot silently reduce demand. Separate completions during
   load from work drained after the load stops.
2. Add per-operation p95/p99 gates. The inspected baseline
   [read gate](https://github.com/labstack/pitot-bench/blob/04ae89d2597789bb7bd7d67c001beda7b1cdba3f/internal/bench/runner.go#L560)
   checks throughput, failures and shedding without enforcing tail latency.
   A mixed percentile must not hide a slow drill-down behind cached dashboards.
3. Add browser trigger-to-last-panel paint measurements. Preserve existing
   minimums of 100 warm samples per panel shape and 20 dashboard samples for
   p95; obtain at least 1,000 samples per class before treating p99 as a useful
   qualification statistic. State sample counts and retain failed samples.
4. Record response-relative freshness, expected nonempty results and correctness
   checks. A present watermark field, empty panel or cache hit is insufficient.
5. Make representative performance-gate failures exit unsuccessfully. The
   current panel microbenchmark reports target violations without failing
   its outer benchmark; its small generated fixture is not a capacity test.

Server panel execution histograms exclude admission and cache hits. Decompose
end-to-end latency into scheduler, admission, snapshot/bind, execution,
serialization/transfer and rendering where measurable. Use existing pool-wait,
statement, write-gate, cache, compaction and rollup metrics before adding new
instrumentation. Do not subtract independently aggregated percentiles.

### Prioritized optimization experiments

| Priority | Current code evidence and experiment | Prediction and acceptance condition |
| --- | --- | --- |
| 1 | Repeat the retained combination against its indexed baseline with early maintenance, exact captured lists, coalescing, projection expansion and committed watermark held constant. | Repeated complete offered load must pass correctness, zero refusals, ingest/freshness, debt and memory gates before claiming mixed-load improvement. |
| 2 | [exec.go](../../internal/query/exec.go): profile main frame, trends, annotations, totals, comparisons and units before sharing compatible work. | Reduce full-request occupancy while preserving totals, comparisons, annotations, limits and partial-result behavior. |
| 3 | [batch_cache.go](../../internal/engine/batch_cache.go) and [file_snapshot.go](../../internal/engine/file_snapshot.go): extend the measured fixed-selection retention control only if preparation becomes material. | Reuse must preserve transactionally consistent markers, late publication and compaction/retention repair; current modest preparation costs do not justify speculative metadata caching. |
| 4 | [pressure.go](../../internal/readpressure/pressure.go) and [duck.go](../../internal/engine/duck.go): correlate background work with refusal timing, CPU, disk and tails before changing cadence or bounds. | Improvement must persist without increasing long-run debt, analytical lag or peak RSS. Separate SQL handles do not reserve CPU or I/O. |

These are falsifiable opportunities, not diagnosed bottlenecks. Interactive
reservations, bounded queues, sibling-panel limits, separate writer/parser
handles, coalescing, incremental caches and full-path filename pruning already
exist. Do not propose them as missing. Preserve namespace/trace identity,
nanosecond window boundaries, typed attributes, redaction, late publications,
uncached active files and exact mixed-scope results in every experiment.

### Execution and publication

1. With repaired benchmark accounting and gates, capture a current-code
   baseline on a pinned representative dataset. Archive exact source identity,
   harness settings, raw samples, generated SQL and profiles privately until
   reviewed for public release.
2. Sweep ingest-only, reads-only and mixed loads at 30%, 50%, 70% and 85% of
   the separately measured ingest ceiling, then deliberate saturation. At each
   stage record actual offered and accepted ingest rather than the nominal
   generator setting. Do not change read/admission limits between comparisons.
3. Run the candidate qualification point for thirty minutes, independently
   repeat it three times and run a one-hour soak. The harness's sweep repeat
   count does not repeat its sustained phase. Include normal compaction,
   rollups, late arrivals and a separate retention/restart recovery exercise.
4. Change one measured cost at a time, starting with qualification of the
   current candidate combination and isolated follow-up comparisons. Keep data,
   resource budgets and offered arrivals fixed for A/B comparisons. Report correctness, goodput, p95/p99, cache/freshness, RSS,
   debt and recovery together.
5. Publish the qualified joint ingest/read operating envelope and its limits,
   alongside the isolated ingest ceiling. Update public performance claims
   only after the evidence passes the declared gates.

## Alternatives

Increasing connections or native threads can increase contention; benchmark
before changing defaults. Longer cache TTL can improve apparent latency while
delaying visibility. Pausing compaction can hide debt until a short run ends.
Approximate rollups cannot replace exact raw-query semantics silently.

DuckDB recommends profiling workload shape, pushdown and repeated-query costs;
many small concurrent queries are not its primary optimization goal. Preserve
its memory-governed asynchronous I/O defaults unless measurements justify a
change. See [DuckDB 2 workload tuning](https://duckdb.org/docs/2.0/guides/performance/how_to_tune_workloads).

## Risks and open questions

Trace-candidate rewrites have substantial correctness risk at scope/window and
publication boundaries. Shared preparation can retain stale file references.
Background throttling can exchange visible latency for hidden maintenance debt.
Prepared-query reuse must account for snapshot-specific file bindings.

Before public qualification, agree on representative row/file counts, realistic
attribute cardinality, populated retention, trace-size limits, search
selectivity and dashboard count. Determine the analytical freshness budget and
the minimum accepted ingest rate the product should advertise with reads active.
Numerical goals here are proposed acceptance criteria rather than an industry
standard. User-oriented latency, errors, freshness and correctness follow the
measurement approach in [Google SRE's SLO guidance](https://sre.google/workbook/implementing-slos/).

## Benchmark tooling and follow-up controls

### Repaired opt-in benchmark

An initial invocation before the repair failed to build:

```sh
rtk bash scripts/with-duckdb.sh go test ./internal/query -tags=readbench \
  -run '^$' -bench BenchmarkPanelQueries24Hours -benchtime=100x -count=1 -v
```

The failure is `backlog_pressure_test.go:77: unknown field ServiceVersion in
struct literal of type telemetry.Span`. Without `-tags=readbench`, the same
benchmark selection runs no benchmarks and can misleadingly print `PASS`.

The repair moves that fixture's service version to the current
typed resource attributes (`Resource: map[string]any{"service.version": version}`),
and the tagged suite now compiles. The replay loader in
`panel_bench_data_test.go` also checked `m.Version != 4`, and replay output
constructed version-4 metadata, despite the format-5-only product. The replay
path now uses the authoritative current
format contract, including physical schemas, trace indexes and metadata
validation; changing the version number alone does not prove it works.
The clipping writer recomputes format-5 spill footer counts for retained rows.
Source-preservation tests cover normal input and captures containing compacted
and pruned files. Replay preserves retirement sidecars on a private copy and
tiles only its active catalog; a live root compaction transaction is rejected
before replay. A reusable bounded verifier checks copied and generated batches.
A compile-only `readbench-check` recipe now runs in CI to prevent drift.

This failure is in optional measurement tooling. It does not establish a defect
in ordinary product query execution. That failed attempt produced no timings;
the repaired generated-fixture measurements are recorded above.

The panel microbenchmark also supplies an explicit environment to configuration
loading; exporting `PITOT_DUCKDB_*` settings does not currently change its
resource budget. Add deliberate test-only controls before claiming a knob A/B
comparison. Likewise, the managed external harness strips inherited `PITOT_`
variables: exporting `PITOT_PPROF_ENABLED` alone will not enable managed-server
profiling. Product pprof support already exists; the harness needs explicit
diagnostic forwarding and private artifact collection.

### Retain negative results and profile the complete path

The initial isolated partial-trace SQL kernel improved an unusually favorable
trace-grouped fixture with one boundary trace, but its gain shrank when 10% of
traces crossed the boundary. The later completed-query oracle and timings above
rejected that rewrite. It is historical evidence for requiring full-path
measurement, not the next proposed production change.

For repeated dashboard reads, use DuckDB 2's supported profiling interface:
discover `duckdb_available_metrics()` and select available `tracked_metrics`.
Capture actual scan rows/groups, intermediate rows/bytes, planning time, native
CPU and peak buffer/temp usage. The old `custom_profiling_settings` interface
is not the current profiling recipe. Keep internal profiling on a dedicated
trusted connection without opening arbitrary-query configuration access.
See [profiling](https://duckdb.org/docs/2.0/dev/profiling) and
[metrics](https://duckdb.org/docs/2.0/dev/metrics).

Test sharing a filtered projection only after observing duplicate scan work.
DuckDB already materializes many multiply referenced CTEs and can push partial
aggregation below joins. Forcing materialization everywhere can block useful
filters or increase memory. See [CTE behavior](https://duckdb.org/docs/2.0/sql/query_syntax/with).

### A small tuning matrix instead of guessed defaults

If CPU contention remains after reducing demonstrated query work, compare the
current global worker count against half that count at fixed admitted read
load. Hold the memory budget, queries, ingestion, maintenance and cache policy
constant. `threads` governs the native instance globally, rather than being
multiplied per connection. Record the tradeoff between small interactive reads
and large scans instead of treating more threads as automatically faster.
See [configuration](https://duckdb.org/docs/2.0/configuration/overview).

As proposed experiment gates, require at least a 10% p95 improvement across
repeated paired runs, no more than a 5% regression in protected read/ingest
workloads, unchanged results and bounded memory, debt and analytical lag.
These percentages are acceptance choices, not predicted gains. Retain default
memory-governed read-ahead; a positive fixed depth removes that budget.
See [DuckDB 2 I/O](https://duckdb.org/docs/2.0/data/io).
