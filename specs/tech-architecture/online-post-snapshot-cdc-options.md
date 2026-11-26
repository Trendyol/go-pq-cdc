# Online post-snapshot CDC: transition options

## Purpose and status

Compare two ways to give the online connector an efficient CDC path after its snapshot completes. This document records the design discussion, required code changes, and failure cases. **Neither option is implemented or approved by this document.**

- **A: Keep the online reader and introduce faster steady-state CDC processing.**
- **B: Stop the online reader and hand over to the actual legacy CDC stream.**

Recommendation: pursue A first for an incremental improvement. Consider B when consolidating both strategies onto one CDC engine is the objective.

## Evidence and current behavior

The [CDC-only benchmark evidence](../verifications/online-10m-snapshot-benchmark/evidence/cdc-only.json) compares the two paths on one shared PostgreSQL 16 source at 1,000 inserts/s for 120 seconds, followed by one sentinel transaction.

| Metric | Legacy | Online |
| --- | ---: | ---: |
| CDC deliveries | 120,001 | 120,001 |
| Stop at sentinel, seconds | 120.048 | 507.630 |
| Delivery rate over measured run, events/s | 999.6 | 236.4 |
| Peak retained WAL, bytes | 13,659,656 | 28,073,712 |
| Peak confirmed lag, bytes | 10,241,560 | 20,451,720 |
| Sentinel commit LSN | `0/322E9E0` | `0/322E9E0` |

Legacy disabled snapshotting. Online completed a vacuous snapshot on an empty table before writers started; both delivered zero snapshot rows. This is local, single-run evidence with counting handlers, not a durable-target benchmark or production SLO. The average delivery rate is not a measured maximum capacity.

The result isolates a performance gap in the CDC paths. It does **not** identify the dominant function without profiling.

Relevant current behavior:

- `pq/snapshot/online/coordinator.go`: `requestNext()` is a channel handoff, **not a SQL window claim**.
- `Coordinator.process()` calls `checkSchema()` for each committed envelope. That validates the publication through SQL.
- `pq/replication/online.go`: `OnlineReader.Next()` performs a primary-visibility gate before returning a committed envelope.
- The existing post-snapshot coordinator loop already processes CDC without READ conflict tracking or LOW/HIGH windows. Removing snapshot logic alone will not resolve the CDC-only gap.
- The legacy processing loop directly invokes its listener and waits for the callback to return. ACK is caller-controlled; it is not inherently fire-and-forget.

## Safe completion barrier

Neither option switches when the last SELECT finishes. A safe transition requires:

1. Process the final window's HIGH marker.
2. Successfully deliver all remaining READ events from that window.
3. Durably complete all table/window progress.
4. Retain a durable transaction-end delivery frontier and persist snapshot completion before entering steady-state CDC.

Current window completion persists progress before advancing safe ACK; job completion is a subsequent metadata transaction. A redesign must preserve the safety of that ordering. It does not necessarily require combining those writes into one transaction.

After this barrier, no snapshot READ remains to reconcile with a future UPDATE. A CDC backlog does **not** need to be empty before switching.

## Option A: same reader, faster steady-state processing

### Design

```text
Same replication connection and slot
                 |
          Online WAL reader
                 |
        +--------+--------+
        |                 |
 During snapshot     After completion
 READ/CDC merge      Buffered CDC processing
        |                 |
        +--------+--------+
          Durable checkpoint
                 |
          Safe replication ACK
```

Keep the connection, slot, reader queue, and event contract. At completion, change the processing path that owns subsequent envelopes. Improve scheduling and validation costs rather than merely renaming the current tail loop.

### Pros

| Benefit | Consequence |
| --- | --- |
| No reconnect or slot handover | No additional disconnect/startup failure window. |
| Keeps buffered transactions | No transfer or replay of the reader backlog is required. |
| Preserves online handler and event contracts | Stable IDs, ordering tokens, and errors can retain their meaning. |
| Simpler crash recovery | Resume from existing metadata and the durable checkpoint. |
| Incremental development | Profile and improve scheduling, checks, and checkpointing separately. |
| Can switch while behind WAL head | The faster path can drain the remaining backlog. |

### Cons

| Cost | Consequence |
| --- | --- |
| Separate CDC implementation remains | Legacy and online can continue to diverge. |
| No automatic legacy performance | Online decoding, visibility, and schema checks still require analysis. |
| Pipelining complicates progress tracking | Received, validated, delivered, and checkpointed positions must stay distinct. |
| Additional buffers consume memory | Queue byte limits and backpressure are mandatory. |
| Validation optimizations can weaken guarantees | A replacement policy must define what changes it detects before delivery. |

### Required code changes

| File or area | Proposed changes |
| --- | --- |
| `pq/snapshot/online/coordinator.go` | Separate window execution and steady-state execution. Make completion explicit. Replace the request/receive/process rhythm with bounded pipelining where useful. Preserve one ordered delivery frontier. Batch durable checkpoints without ACK overtaking delivery. |
| Publication/schema validation | Remove per-envelope full validation only if an alternative preserves the required safety. Cached validation needs reliable invalidation. Periodic checks alone change the detection guarantee. |
| `pq/replication/online.go`, `online_decode.go` | Profile visibility and decoding. Allow read-ahead/validation to overlap handler delivery while preserving order. Keep transaction and queue byte limits. Receipt and validation must not auto-ACK. |
| `pq/snapshot/online/store.go` | Retain durable completion, checkpoint ownership, and generation fencing. Resolve uncertain metadata writes through persisted state or fail closed. Serialize checkpoint work safely if moved to a separate goroutine. |
| `online_connector.go` | Route completed jobs directly to the fast path on restart. Preserve cancellation, ownership, and error propagation across phases. |

### Edge cases

| Case | Required behavior |
| --- | --- |
| Final SELECT completes before READ delivery | Stay in snapshot mode until READ delivery and completion persistence succeed. |
| CDC backlog at completion | Switch without emptying it; retain ordered, exclusive ownership. |
| Transaction still being assembled | The same reader completes it; release only after commit and validation. |
| Handler fails midway through a transaction | Do not checkpoint its end. Already-applied events can replay; preserve stable IDs and idempotency. |
| Crash after completion persists but before fast-loop startup | Resume directly in steady-state CDC from the durable checkpoint. |
| Checkpoint commits but response is lost | Resolve persisted progress or fail closed. Never guess and ACK. |
| Parallel deliveries complete out of order | ACK only the contiguous completed prefix. Prefer ordered delivery initially. |
| Schema/publication changes after completion | Continue an explicit validation policy; completion does not make changes safe. |
| Input remains above delivery capacity | Apply bounded backpressure. WAL retention can still grow. |
| Cancellation during transition | Do not cancel `Next()` to change modes; cancellation is terminal for this reader. |
| Empty table or resumed completed job | Enter the same steady-state path without unnecessary windows or READ delivery. |

## Option B: hand over to the legacy CDC stream

### Design

```text
Online snapshot and reader
             |
 Persist completion + safe frontier F
             |
 Stop online dispatch and join reader
             |
 Start legacy stream on SAME slot
             |
 Resume from certified position F
             |
 Adapt messages to online handler
```

F is a durable **transaction-end delivery frontier**, not received WAL, WAL head, or a timestamp. Reuse the stream machinery, not the whole legacy connector's startup/snapshot orchestration unchanged.

### Pros

| Benefit | Consequence |
| --- | --- |
| Reuses the established pipeline | Less duplicate transport and dispatch logic. |
| Builds on measured legacy performance | That path kept up with the benchmark's 1,000 inserts/s. |
| Snapshot strategy becomes initialization | Both strategies can converge on common steady-state behavior. |
| Shared optimizations | Future transport improvements benefit both modes. |
| Clear phase separation | Snapshot window machinery can terminate after initialization. |

### Cons

| Cost | Consequence |
| --- | --- |
| New handover failure window | Recover across disconnect, reconnect, and startup failures. |
| Different handler and message contracts | Adapt errors, event identity, order, and checkpoint semantics. |
| Legacy ACK behavior is insufficient unchanged | Retain online's durable checkpoint-before-ACK contract. |
| Buffered data needs an explicit policy | Replay from F is simpler than buffer transfer but can repeat deliveries. |
| Recovery validation must stay coherent | Legacy must not confirm beyond metadata that online later needs to resume safely. |
| More lifecycle and ownership work | Exactly one reader and one checkpoint owner must run. |

### Required code changes

| File or area | Proposed changes |
| --- | --- |
| `online_connector.go` | Supervise snapshot, handover, and steady-state phases. Retain job ownership. Stop/join the old reader before opening the new stream. Route completed jobs to shared CDC on restart. Recover startup failures without recreating the slot or snapshot. |
| `pq/replication/stream.go` | Add a caller-certified start-position contract rather than relying on default startup behavior. Expose reliable startup/termination/error results. Expose transaction-end delivery completion. Support synchronous handler errors. Keep received positions separate from durable confirmation. |
| Message/event adaptation | Convert legacy messages to `online.Event`. Preserve event IDs and order across decoder changes and replay. Filter watermark/control events. Provide sufficient transaction and ordinal information; commit LSN alone does not identify every row event. |
| `pq/snapshot/online/store.go` and recovery | Persist the completion frontier and phase/version metadata if needed. Continue steady-state checkpoints, generation fencing, and source/slot binding validation. Keep slot confirmation at or behind the certified durable checkpoint. |
| Legacy orchestration | Prevent slot creation/drop policies, snapshot metadata, and connection ownership from conflicting with the online job. |

### Edge cases

| Case | Required behavior |
| --- | --- |
| Crash before completion persists | Resume the online snapshot protocol. |
| Crash after completion but before legacy starts | Start shared CDC from F; do not start a new snapshot. |
| Reader received transactions beyond F | Discard and replay volatile buffers, or implement an ordered transfer. Receipt is not ACK permission. |
| Events delivered beyond the last checkpoint | Expect replay. Event identity must remain stable across implementations. |
| Old reader has not disconnected | Wait for termination; do not open another reader on the same slot. |
| Legacy startup fails | Keep completion and F; recover through the supervised restart path. |
| Slot confirmed position is behind F | Handle replay before/at F safely; delayed feedback must not cause missing or duplicate-identity events. |
| Slot confirmed position is ahead of durable F | Treat this as an invariant violation. Requesting F cannot recover WAL already released by the slot. |
| Large/multi-row transaction crosses handover time | Handover at a completed transaction frontier. Replay unfinished work; do not split ownership. |
| Handler fails after some rows | Halt without confirming transaction end; retain replay semantics. |
| Watermark/control transactions enter legacy stream | Filter business delivery while advancing safe progress only after processing the transaction. |
| Publication changes or failover during handover | Revalidate supported schema, source identity, slot binding, and generation. Fail closed on mismatch. |
| Slot invalidates during disconnect | Require explicit recovery; do not silently recreate a replacement slot. |
| Shutdown while handover is running | Join active work and persist only certified progress. Restart from durable phase state. |

## Guarantees common to both options

- Do not switch before pending READ delivery and durable completion succeed.
- Do not ACK received or merely validated data as though it were delivered.
- Preserve transaction order and a contiguous delivery frontier.
- Keep bounded buffers, ownership fencing, source identity checks, and applicable schema/visibility guarantees.
- Preserve stable event identity across restart and replay.
- Do not claim exactly-once target effects from checkpointing alone. A crash after target application but before source checkpointing can repeat events. The handler needs idempotency or deduplication.
- Do not assume faster CDC eliminates WAL retention: sustained input above delivery capacity still accumulates retained WAL.

## Recommendation and verification plan

For A, profile the current CDC-only path first. Improve the measured critical path while retaining reader and event contracts. Verify that the completed-snapshot path is faster without weakening safety.

For B, first make the shared stream support the online durability and event contracts. Then implement a supervised handover. Constructor switching alone is not sufficient.

Required tests for either option:

1. Empty snapshot and restart of a completed snapshot enter steady-state CDC directly.
2. Final READ delivery precedes the first fast-path event; backlog need not be empty.
3. Multi-row transactions, handler failure, and replay retain stable IDs and order.
4. Fault injection around completion/checkpoint persistence and lost replies never ACKs beyond durable progress.
5. Schema change, ownership loss, slot invalidation, and source mismatch fail safely.
6. B additionally covers crashes at every handover stage, slot-in-use, and duplicate replay across decoders.
7. Repeat CDC-only and snapshot-plus-CDC benchmarks; compare throughput, memory, retention, and measured final lag without using peak lag as a substitute.

Existing opt-in CDC-only benchmark command (runs a live local database workload):

```sh
CDC_ONLINE_10M_BENCH=1 POSTGRES_VERSION=16-alpine go test ./integration_test \
  -run '^TestOnlineCDCOnlyHeadToHead$' -count=1 -timeout=30m -v
```

This command measures current behavior; it does not verify an unimplemented transition design.
