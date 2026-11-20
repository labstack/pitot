# Indexed trace logs

Status: Draft
Date: 2026-11-20

## Summary

Trace detail finds a trace's spans through each batch's trace index, then finds
its logs with `trace_id = ?` over every log file in the window. Logs are sorted
by time bucket, namespace, service and time, so `trace_id` has no locality and
every lookup decodes it for every retained log row in the window. This proposal
gives each batch a log trace index, `logs.fidx`, that maps a trace hash to its
log rows (batch format 6), and reads those rows in Go, as spans already are. A
lookup then costs one probe per batch plus the trace's own rows, independent of
retained logs.

## Motivation

pitot-bench on a Hetzner ccx33 pair, `v2026.11.10-alpha`, after a 4-hour
prefill (about 1.9B rows, a third of them logs, 115 GB of Parquet), with 20
UI reads/s:

- The read cache drained the prefill backlog in 2,604 s and kept up afterwards,
  so read-cache lag is not the cause.
- The read gate completed 5.5 of 20 operations/s. Every one of the 250
  trace-detail timeouts failed in `query trace parquet logs`. Trace detail over
  6h–24h windows averaged 8.9 s on the server and was 68–80% of server read
  time in the first mixed phases. Drill-down and annotations queued behind it
  for DuckDB read slots (718 `server is busy` rejections, 570 drill-down
  timeouts), so the 8.9 s includes queueing as well as the scan.

`internal/observability/trace_logs_bench_test.go` (build tag `readbench`)
reproduces the scan on an idle 18-core laptop, one log per span, random trace
IDs, spread over 20 hours:

| Log rows | Batches | Span lookup | Log lookup | Log lookup, bloom prototype | 20 trace summaries |
|---|---|---|---|---|---|
| 10M | 100 | 3.2 ms | 39.5 ms | 10.8 ms | 30 ms |
| 100M | 1,000 | 25 ms | 327 ms | 62 ms | 160–177 ms |

The log lookup grows linearly with retained log rows. A day at 48k items/s is
about 1.3B log rows: about 4 s idle on this machine without pruning, and about
0.8 s with Parquet bloom filters, which still check every row group.

## Goals

- Trace-detail log lookup cost independent of retained logs; p95 under 300 ms
  on an 8-vCPU host under ingest with a day retained.
- Exact results under a total order that refines today's.
- Per-batch trace probes that read one index block, for trace detail and
  drill-down summaries alike.

## Design

### Log index

Each batch with a logs file gets `logs.fidx` next to `trace.fidx`:

- Header: magic, entry count, posting count.
- Entries sorted by trace hash, 16 bytes: `hash u64 | offset u32 | count u32`.
- Postings: one `u32` `logs.parquet` row number per log row with a nonempty
  `trace_id`, grouped by entry and ascending within it.

The hash is the same 64-bit hash of the lowercase hex trace ID that spans sort
by. Readers still compare the exact trace ID and namespace. Writers reject a
file with more rows than `u32` holds; compaction outputs are capped at 25M
rows.

The index is a pure function of `logs.parquet`: decode `trace_id`, hash each
nonempty value, sort `(hash, row)` pairs, write. Commit, compaction and
verification share that one function. Commit and compaction run it on the file
they just wrote; offline verification rebuilds the index and compares bytes.
Memory is 12 bytes per traced row, at most 300 MB for a 25M-row compaction
output; above a threshold the build sorts in hash-range passes.

Spans keep `trace.fidx` and its format unchanged. Logs keep their schema and
sort order.

### Probes

Loading a batch keeps, for each index, the first hash of every block of 256
entries in memory. A probe binary-searches the fences, then reads one block
with `pread`. Both `trace.fidx` and `logs.fidx` use it, for `Trace`,
`TraceSummaries` and `TraceLogs`. Today a probe opens the file and makes about
17 reads.

### Reading logs

`ParquetStore.TraceLogs` replaces the DuckDB query in
`internal/observability/trace.go`, under the same pinned catalog snapshot as
the trace's spans:

1. Skip batches whose log time bounds miss the window.
2. Probe `logs.fidx`; for each hit, open `logs.parquet` with its offset index
   and seek to each run of posted rows, reading only the projected columns.
3. Keep rows whose trace ID matches exactly, whose `log_time` is in the window,
   and whose namespace matches when the scope names one or the trace's
   namespace is exact, as the span read does.
4. Keep the first `limit` rows by `(log_time, service, span_id, batch, row)`,
   a total order that refines today's `ORDER BY time`, and redact bodies as
   today.

Parsed Parquet footers and offset indexes of recently read batches are kept in
a bounded cache, dropped on retirement, because large compacted files have
large footers.

These reads keep read admission: they take the interactive read slot that the
DuckDB query held, with the same deadline shedding.

### Format

Batch format 6. Format 5 batches are rejected like any other unsupported
format; servers upgrading start with empty telemetry. The read-cache version
does not change. AGENTS.md, the storage and data-layout docs and diagrams, and
the release-notes script follow.

## Alternatives

- **Parquet bloom filters on logs `trace_id`.** No format change and about 5x
  faster at 100M rows, but one check per row group, about 26k for a day of
  logs. A bloom filter per batch still scans each positive batch, up to 25M
  rows after compaction.
- **Bounding the log lookup to the trace's span extent plus a margin**, as
  Grafana's trace-to-logs does. Constant cost, but logs outside the margin
  disappear; Grafana documents empty results with its default shift.
- **Postings per row group.** Still needs a row map through compaction, and
  each hit decodes a whole group's `trace_id`.
- **Sorting logs by trace hash, or a trace-sorted copy.** Loses the time and
  service pruning other log reads rely on, or nearly doubles log storage.
- **One combined index for spans and logs.** Compaction merges each signal
  separately and in parallel, so a combined file would join the two merges.
- **Remapping input postings through compaction.** Avoids rescanning the
  output, but its row map approaches one run per row when log times interleave
  across batches (hundreds of MB at 25M rows), and it is where a silent
  misattribution would hide.
- **Memory-mapping the indexes.** No measured need over one `pread` per probe;
  adds unmap lifecycle and the Linux mapping limit.

## Gates

1. Oracle test: trace detail equals a full scan ordered by the same total key,
   for random data after commit and after compaction across generations,
   including hash collisions, empty trace IDs, the same trace ID in two
   namespaces, logs-only batches and the limit.
2. Offline verification rejects a missing, duplicated or misattributed posting.
3. Benchmark after compaction: log lookup at 100M log rows is flat against 10M
   and under 20 ms, including a hit deep in a row group; probe cost per batch
   at 1,000 and 10,000 batches.
4. Ingest A/B: commit latency and ingest throughput unchanged within noise;
   compaction time, peak memory and index bytes per log row reported.
5. pitot-bench on Hetzner, 4-hour prefill, `--sustained-rate 48000`, 20 reads/s:
   no trace-detail timeouts, and the read gate's trace-detail p95 target met.

## Risks

- Index size for traces with few logs per batch: 16 bytes per entry plus 4 per
  row. Gate 4 measures it.
- Seek cost inside large body pages. Gate 3 measures a hit deep in a group.
- Startup time if every entry is validated at load; load reads fences only,
  and full checks stay in offline verification.
