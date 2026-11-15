# Manifest catalog, write-ahead log, and parallel compaction

Status: Draft
Date: 2026-11-14
Pull request: TBD

## Summary

Replace the filesystem-as-catalog model with an append-only manifest, add a
write-ahead log with a queryable head block so an acknowledgement is not a full
Parquet publish, and run compaction in parallel lanes with time-leading span
order. These are coupled bets about the ingest and storage layer; this proposal
records them together and the trade-offs that gate them.

## Motivation

The engine audit found the ingest/storage layer correct and crash-safe but
structurally expensive:

- **Filesystem as catalog.** Boot is proportional to stored data (per batch:
  `metadata.json` read twice, three footers, the whole `trace.fidx`), and
  `BatchMetadata()` copies and sorts the entire catalog under a lock on every
  statement. (Issues: "BatchMetadata copies and sorts the whole catalog";
  startup cost.)
- **No WAL.** Every group commit is a sort, up to three wide Parquet encodes,
  up to seven fsyncs, a rename, and a footer re-parse, and it creates a
  generation-0 directory re-encoded at least twice. The debt limit, admission
  bands, and pressure windows all exist to manage this flood.
- **Single compaction lane with discard-on-preempt**, and span files ordered
  trace-hash-first so they have no time locality (day merges capped at 250k
  rows, narrow-time queries decode whole files).

## Goals and non-goals

Goals:

- Boot proportional to the manifest, not to stored data.
- One refcounted catalog snapshot per epoch, shared by all readers, replacing
  the pin/epoch scheme and the per-statement catalog copy.
- Acknowledge ingest after a bounded append, not a full publish.
- Parallel compaction with no discarded work and time-prunable large files.

Non-goals:

- Not a move to an external catalog service or a second storage engine; the
  manifest stays a plain local file so ingest fsyncs stay independent of control
  state.
- Not a change to UTC `TIMESTAMPTZ_NS` instant semantics.

## Design

**Manifest.** An append-only log of edit records (add / replace / prune), each
fsynced, carrying per-batch row counts and event-time bounds, plus namespace and
service sets for pruning. It replaces `metadata.json` re-reads, `replaces.json`,
`pruned`, `COMPACTION.json`, the `.retired` renames, and the large-directory
fsync. Boot is a log replay; a snapshot is a refcounted manifest version, which
subsumes `pinMu`/epoch and lets `BatchMetadata()` become one immutable load.

**WAL + head block.** Ingest appends group-committed rows to a WAL and
acknowledges after one `fdatasync`; a sorted Parquet batch is cut at a size or
time threshold (~200k–1M rows or a few seconds). This removes one compaction
generation and most of the debt and admission machinery. The head block must
stay queryable, or the product accepts a few seconds of freshness lag — this
conflicts with today's "immediately visible" behavior and must be decided first.

**Parallel tiered compaction.** Two or more lanes over disjoint input groups,
with no discard-on-preempt. In a new batch-format version, order spans by time
bucket then trace hash, with `trace.fidx` mapping a hash to several ranges, so
large multi-day files stay prunable by row-group statistics and the 250k-row cap
can go.

## Alternatives

- **Incremental fixes without the manifest.** The atomic-snapshot fix for
  `BatchMetadata()` (issue) and header-only boot loading recover much of the
  per-statement and boot cost without the manifest. These should ship first
  regardless; the manifest is justified only if those prove insufficient.
- **SQLite as the catalog.** Couples ingest durability to control-plane state
  and adds a second fsync domain; rejected in favor of a plain append-only file.

## Risks

- The freshness-guarantee decision for the WAL head block is load-bearing and
  product-visible.
- A format bump for span ordering must be validated on non-pico data before
  adoption, per the no-curve-fitting rule.
- Manifest replay and compaction crash-recovery must match today's
  fsync-before-rename durability exactly.

## Open questions

- Keep "immediately visible" by making the head block queryable, or accept a
  bounded freshness lag?
- Can the manifest ship incrementally (catalog snapshot first, WAL second,
  compaction/order third), and which steps need the format bump?
- What is the measured boot and ingest win versus the incremental fixes alone?
