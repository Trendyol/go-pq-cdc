# Specification: legacy-based CDC reader for online snapshots

**Status:** proposed; runtime implementation is not changed by this document.
**Purpose:** reuse the legacy CDC receive/dispatch implementation during and after online snapshots without changing the implemented DBLog-style LOW/HIGH merge, conflict elimination, event identity, or recovery contracts.
**Source baseline:** current working-tree code inspected for this specification, including local coordinator/window changes. No assumption that uncommitted changes are upstream behavior.

## 1. Scope and decision

Extract a shared replication reader engine from the legacy pipeline. Keep two explicit adapters:

- **Legacy row adapter:** preserves existing `NewConnector`, listener, decoding, and ACK behavior.
- **Online committed adapter:** produces the existing immutable `OnlineCommittedEnvelope` contract and accepts only coordinator-certified ACKs.

Use the shared engine from the start of online capture, before upper-key capture and LOW emission, through normal CDC tailing. Do not reconnect, switch slots, or hand over to a second reader at snapshot completion.

“Same as legacy” means sharing transport ownership, CopyBoth receive scheduling, WAL framing, asynchronous receive/dispatch structure, and feedback machinery. It does **not** mean copying legacy early-row delivery, permissive parsing, default codecs, optional visibility guard, or automatic heartbeat ACKs into online mode.

Non-goals: change the watermark algorithm; parallel handler application; exactly-once target transactions; disk spill; automatic source/slot adoption; HA; new PostgreSQL-major qualification; protocol-2 qualification; removal of strict schema/visibility checks without separate proof.

## 2. Code analysis and evidence

### 2.1 Legacy implementation

| Source | Actual behavior | Reuse or restriction |
| --- | --- | --- |
| `pq/replication/stream.go:NewStream` | Creates a 1,000-message channel and separate sink/process goroutines. | Reuse pipelined ownership; a count-limited queue alone is not an online byte budget. |
| `stream.setup` | Starts replication from default/in-memory position or legacy snapshot metadata. | Add explicit certified start position for online; never use WAL head or legacy metadata as its checkpoint. |
| `sinkLoop` | Reads under `connMu`, releases lock before channel dispatch; uses 300ms receive deadlines. | Reuse scheduling/serialization principles; preserve bounded feedback even when dispatch is blocked. |
| `handleXLogData` | Uses `ParseXLogData` and `message.New`; logs/skips decode failures. | Share framing; online errors must be fatal, never skipped. |
| `messageBuffer` | One-message look-ahead. Earlier messages can leave before COMMIT; only the final message gets transaction-end position. | Preserve for legacy only. Insufficient for online committed envelopes. |
| `streamTxBuffer` | Protocol-2 per-XID buffering, commit release, whole/subtransaction abort handling. | Prior art for a separately qualified protocol-2 extension, not automatic online support. No aggregate byte limit is provided here. |
| `processLoop` | Calls listener synchronously; caller ACK advances in-memory confirmation. Optional gate once per transaction. | Preserve public legacy behavior. Online requires handler errors and coordinator-only durable confirmation. |
| Heartbeat handling | Heartbeat can bypass the legacy visibility gate and auto-ACK. | Forbidden in the online adapter. Control transactions require normal visibility and checkpoint safety. |
| Feedback and close | Feedback on receive timeout/keepalive; final confirmed-position flush on close. | Add periodic feedback for sustained traffic/backpressure. Online close must not invent or advance an ACK. |

Legacy row callbacks do not expose a complete transaction's ordered DML list, reliable row ordinals, or a transaction-end field on every row. The final-message rewrite is an ACK convenience, not the online envelope interface.

### 2.2 Online implementation

| Source | Actual behavior | Preserve |
| --- | --- | --- |
| `online_connector.go:Run` | Provisions/validates binding, starts protocol-1 reader, then coordinator. PG16-only checks. | Startup before snapshot bounds; binding, ownership, explicit checkpoint and supported profile. |
| `pq/replication/online.go:readLoop` | Dedicated I/O owner; byte-reserved assembly/queue; periodic safe feedback; fatal protocol errors. | All safety and admission contracts. Sharing must not remove already-existing concurrent I/O. |
| `OnlineReader.Next` | Serial dequeue, fresh primary visibility/identity gate, then immutable envelope handoff. | No transaction before commit/visibility; no ACK on return. |
| `online_decode.go:onlineAssembler` | Strict B..C assembly, transaction-end LSN, DML ordinals including controls, metadata accounting. | Whole-envelope validation and stable ordinal semantics. |
| `pq/message/tuple/online.go` | Lossless JSON numbers, qualified type profile, complete tuples, rejects unresolved TOAST/binary values. | Do not replace with legacy decoded maps. |
| `Coordinator.process` | Validates schema and all controls/business images before delivering any event in an envelope. | No partial delivery caused by an unsupported later change. |
| `Coordinator.requestNext` | Channel handshake, not SQL. | Earlier claims that it performs a window-claim SQL query were incorrect. |
| `Coordinator.window`, `ReadWindow` | LOW → SELECT → HIGH → conflict-elided READs → durable completion → ACK. | Algorithm and observation order unchanged. |
| `Store`, `ValidateResume` | Generation ownership, conditional metadata transitions, checkpoint-before-ACK, rejects slot confirmation beyond durable checkpoint. | Recovery safety unchanged. |

The online reader already receives independently of merge dispatch. Creating goroutines alone is not a performance fix. Per-envelope fresh visibility SQL and publication/schema SQL are real work absent or optional in the legacy benchmark path.

### 2.3 Measured evidence and limits

[CDC-only evidence](../verifications/online-10m-snapshot-benchmark/evidence/cdc-only.json): shared PG16 source, 1,000 inserts/s × 120s, one sentinel, zero delivered READs. Both arms delivered 120,001 CDC events at commit LSN `0/322E9E0`.

| Metric | Legacy | Online |
| --- | ---: | ---: |
| Stop at sentinel, seconds | 120.048 | 507.630 |
| Average deliveries/s over run | 999.6 | 236.4 |
| Peak retained WAL, bytes | 13,659,656 | 28,073,712 |
| Peak confirmed lag, bytes | 10,241,560 | 20,451,720 |

This proves an end-to-end gap, not its root cause or maximum capacity. Handler counting is not a durable target. The writer rate was reported using nominal duration; new evidence must record actual elapsed time and pacing misses. Final sentinel-delivery confirmed lag was not measured and must not be inferred as zero: handler delivery, durable checkpoint, and server feedback are distinct events. Online heartbeats/metadata also generate WAL after business writers stop.

## 3. Proposed architecture

```text
Existing connector orchestration         Online job/source orchestration
                 |                                  |
        Legacy reader options              Strict online reader options
                 +------------------+---------------+
                                    |
                    Shared replication engine
                  receive + framing + decode policy
                 bounded dispatch + periodic feedback
                         /                    \
             Legacy row adapter       Online committed adapter
                processLoop           strict assembler + gate
                ListenerFunc          immutable envelopes / Next
                caller ACK                      |
                                      Existing coordinator
                               LOW/SELECT/HIGH + conflict map
                                ordered synchronous handler
                              metadata commit → safe ACK only
```

The shared engine lives in `pq/replication`; it must not import snapshot metadata/store code. `pq/snapshot/online` remains the merge and durability owner. Keep the external online handler and legacy listener interfaces unchanged.

Prefer small explicit policy choices to arbitrary hooks: delivery mode, value-decoding profile, error policy, byte limits, visibility policy, and feedback frontier authority. Invalid combinations fail startup. Online policy is fixed strict behavior, not caller-selectable fail-open flags.

## 4. Reader and adapter contracts

### 4.1 Startup and lifecycle

- Online provides explicit nonzero durable `StartLSN`, existing slot/publication, source identity, and limits.
- Shared engine creates neither online slots nor jobs; existing online orchestration retains that authority.
- Startup returns only after identity validation and CopyBoth readiness; startup timeout is distinct from run lifetime.
- No transparent reconnect/retry for online. Terminal failure requires job/slot revalidation through existing recovery.
- One I/O owner per replication connection. Reads, frontend writes, deadline watchers, and cleanup must not race.
- Propagate typed terminal errors and `Done`; do not use legacy background panic/log-only behavior as online's lifecycle interface.

### 4.2 Decoding and committed assembly

Share framing and raw tuple parsing. Retain separate value policies: legacy codecs for legacy; `DecodeOnlineData` for online. Where extraction would run legacy codecs first, split raw parsing from value decoding rather than converting lossy decoded maps afterward.

For online protocol 1:

1. Validate wire shape before invoking permissive parsers.
2. BEGIN opens one bounded transaction and records XID/commit LSN.
3. Relation/Type/Origin metadata does not consume a DML ordinal.
4. Every I/U/D, including watermark DML, consumes a zero-based ordinal.
5. COMMIT validates BEGIN/COMMIT consistency and sets `TransactionEndLSN`.
6. Publish an immutable envelope only after COMMIT; retain empty/control-only transactions.
7. Reject unsupported messages, TRUNCATE, incomplete images, unknown relations, and malformed input before checkpoint advancement.

Keep `OnlineCommittedEnvelope` and `OnlineChange` compatible initially. Do not reconstruct envelopes from legacy listener callbacks: earlier rows can escape before COMMIT, and callbacks omit information required for online order/identity.

### 4.3 Visibility and schema policy

Baseline shared-reader release keeps online fresh-snapshot identity/visibility checks and `Coordinator.checkSchema` behavior. Legacy's disabled/fail-open guard, cached-xmin shortcut, and heartbeat bypass are not valid substitutions.

Performance work must measure these costs separately. A follow-up optimization may gate a bounded batch of **already committed envelopes** using one fresh primary snapshot/identity query, checking each XID with the existing wrap-aware visibility logic. Release only a visible prefix; never assume XID order equals commit order. Preserve per-envelope timeout accounting, bounded batch size, and identity failure semantics.

Batch gating is a behavior change requiring separate tests and approval. Likewise, periodic schema checks or relation-only validation are not drop-in equivalents to per-envelope catalog checks. This spec does not authorize weakening them to meet a throughput target.

### 4.4 Bounded scheduling and feedback

- Reader can receive/decode ahead while coordinator is blocked in SELECT or handler delivery.
- Reserve room for a maximum transaction before assembly; maintain online queue+assembly encoded-byte contract.
- Bound relation metadata separately. Track envelopes retained by visibility/dispatch owners and the coordinator; moving them between queues must not erase budget accounting.
- Preserve current one-window payload bound; do not introduce a durable row outbox or unbounded secondary queue.
- Feedback timer must progress under continuous WAL input and full dispatch queues, not only receive timeout or keepalive requests.
- Never block the sole I/O owner indefinitely on a channel send. Use capacity-aware admission/scheduling that still services safe feedback. Avoid a busy loop when a queue is full.
- Encoded-byte limits are not heap-byte limits. Report actual heap/RSS and count/read-side frontend limits separately.

### 4.5 ACK authority and transaction frontiers

For online, use distinct positions:

- `received`: transport progress, diagnostic only.
- `consumed`: envelope returned after visibility validation.
- `delivered`: contiguous transactions whose handlers finished; HIGH READ obligations may still prohibit confirmation.
- `durable`: coordinator/store-certified restart boundary.
- `safeACK`: at or below durable and consumed progress; never receipt-driven.

Reuse `AdvanceSafeACK` monotonic/below-consumed validation. The engine cannot infer target success. Only the coordinator grants confirmation after the required metadata transaction succeeds.

During LOW/SELECT/HIGH work, preserve the LOW ceiling until READ delivery and `CompleteAttempt` commit. During tailing, preserve `Store.Checkpoint(frontier)` before ACK. All online standby write/flush/apply positions stay at safeACK, as current online transport does; do not substitute legacy's server-head write position.

Close can stop I/O and release volatile memory, but cannot advance safeACK. If a previously certified ACK is flushed during close, document/test that behavior explicitly; preserve current no-new-ACK online semantics as the baseline.

## 5. Preserve the LOW/HIGH algorithm exactly

### 5.1 Window sequence

1. Register attempt under current generation.
2. Emit LOW in its own committed watermark transaction.
3. Consume ordered, committed, visible envelopes through the exact LOW marker.
4. Persist `RecordLOW`, then advance ACK to LOW.
5. Freeze coordinator dispatch while SELECT reads the bounded page and HIGH is separately committed. Reader I/O and safe feedback continue.
6. Consume envelopes through exact HIGH. For every business event in this interval, remove its `(table,key)` from the READ candidates and deliver the CDC event.
7. Persist `RecordHIGH`; validate schema; deliver surviving READs in SELECT key order with HIGH phase-1 tokens.
8. Commit `CompleteAttempt`, advancing cursor/completion and durable boundary; only then ACK HIGH.

Do not let read-ahead validation or buffered dispatch deliver an event after HIGH before surviving READs. There is one coordinator and one active window. Do not parallelize `Conflict` and `Reads`.

### 5.2 Deduplication and identity

There are two distinct mechanisms:

- Window conflict elimination: removes candidate READs touched by CDC between LOW and HIGH. Repeated CDC updates are **not** coalesced or dropped.
- Replay identity/versioning: `CDCEventID(slotBinding, EndLSN, ordinal)` is stable across generation/restart; READ IDs retain job/attempt/table/key identity. Target idempotency/version checks remain required.

Preserve phase ordering: CDC phase 0; READ phase 1 at the dedicated HIGH end LSN. Metadata does not change DML ordinals; filtered control DML still occupies an ordinal. Never replace EndLSN with CommitLSN or row WALStart.

### 5.3 Controls and completion

Run `DecodeControl`/`matchMarker` before business delivery. Exact connector/job/generation/attempt/marker identity remains mandatory. Old-generation controls cannot satisfy new fences or grant ACK; current-generation stale LOW/HIGH attempts fail closed. Dedicated marker transaction shape remains enforced.

After every table is complete and no window remains active, persist `MarkSnapshotComplete` and signal completion. Continue on the same engine and envelope adapter. No LOW/HIGH work or READ conflict map is needed afterward; heartbeat and durable checkpoints remain.

## 6. Failure and edge-case requirements

| Case | Required outcome |
| --- | --- |
| WAL COMMIT received before SQL visibility | No envelope delivery or marker acceptance until the gate passes. |
| SELECT includes an UPDATE later represented between LOW/HIGH | Remove that key's READ; preserve every committed CDC update. |
| CDC event after HIGH already buffered | Hold it until surviving READs and window completion finish. |
| Multiple updates/deletes to one key | Elide one READ candidate; deliver all CDC events with distinct ordinals. |
| Multi-table transaction | Preserve global source DML ordinals; conflict only with active window table. |
| Unsupported image in a later transaction change | Validate entire envelope before releasing earlier business events. |
| PK mutation, TRUNCATE, unresolved TOAST or unsupported type | Existing fail-closed rejection; no checkpoint beyond offending transaction. |
| Large transaction exceeds configured bytes | Fail before envelope release; no unbounded memory or spill implied. |
| Queue full while SELECT/handler stalls | Feedback remains live at previous safeACK; WAL retention can grow legitimately. |
| Handler fails after partial transaction delivery | Halt without confirming transaction end; replay uses stable IDs. |
| Crash after READ application before completion metadata | Resume via existing attempt recovery; target version/idempotency prevents stale overwrite. |
| Metadata commit response lost | Conditional identity lookup or terminal failure; never unconditional replay/ACK. |
| Store ownership/source identity lost | Cancel dispatch/reader; do not reconnect or adopt another slot. |
| Snapshot completes with nonempty queue | No reconnect; tail consumes FIFO backlog after completion barrier. |
| Empty table or resumed completed job | Zero READs or direct tailing respectively; same ACK/checkpoint contract. |
| Idle/control-only source | Process visible control/empty envelopes and checkpoint them safely; no legacy auto-ACK shortcut. |
| Cancellation during gate/startup/full queue | Bounded termination, joined owners, no deadline race or new ACK. |
| Slot confirms beyond durable metadata | `ValidateResume` rejects; never claim an older start request recovers recycled WAL. |
| XID wraparound or out-of-order commits | Wrap-aware snapshot visibility; transaction order determined by validated end LSN, not XID sorting. |

## 7. Protocol compatibility and existing plans

This specification's initial release retains current runtime PG16/protocol-1 support. The separate [PG16–18/protocol-2 plan](../../docs/online-snapshot/pg16-18-protocol2-plan.md) remains proposed work and contains different aggregate-admission decisions.

Do not silently merge those decisions into this change. If protocol 2 is later added, port/test per-XID commit/abort handling with explicit pending-stream restart/ACK safety, byte accounting, ordinal determinism, and metadata interpretation. Sharing `streamTxBuffer` does not qualify those properties. Resolve admission-policy conflicts explicitly before combining the projects.

## 8. File-level implementation sequence

| Step | Changes | Exit condition |
| --- | --- | --- |
| 1. Baseline/profile | Instrument existing reader, gate, schema query, decode, handler, checkpoint, feedback; preserve old evidence. | Reproducible CDC-only timing with actual writer elapsed time and per-stage cost. |
| 2. Extract engine | Add `pq/replication/reader_engine.go` (proposed name); move shared transport scheduling/framing and policy validation out of `stream.go`. | Legacy behavior/tests unchanged; strict failures not accidentally applied to permissive legacy behavior. |
| 3. Isolate decoders | Separate raw tuple parsing from value policies as necessary; retain online strict validator/assembler and lossless codecs. | Differential online envelopes/images/ordinals identical to baseline. |
| 4. Connect adapters | Legacy adapter retains `messageBuffer`/listener path. Online adapter retains committed envelopes and explicit safe ACK; use shared engine in `StartOnlineReader`. | Online startup/Next/Stats/Done interface remains compatible; duplicate receive loops removed, not layered behind two readers. |
| 5. Integrate merge | Keep `window.go`, control matching, event IDs and store transitions behaviorally unchanged; update only reader dependency/counters if necessary. | LOW/HIGH observation traces and final target state match baseline. |
| 6. Feedback/lifecycle | Periodic feedback under pressure; typed terminal results, joined owners, no online panic-driven recovery. | Continuous traffic, blocked delivery, cancellation and ownership-loss tests pass. |
| 7. Optimize proven costs | Separate approved work for bounded gate batching or schema-validation redesign; do not bypass checks. | Safety equivalence established before throughput claims. |
| 8. Qualify/report | Full tests, sentinel benchmarks, target-state checks, time-series evidence. | All correctness gates pass; measured performance reported including unresolved bottlenecks. |

Expected touched files: `stream.go`, `online.go`, `online_decode.go`, raw tuple/message parsing where necessary, `online_connector.go`, reader/coordinator test files. Store, event, control and window algorithm changes are not expected; any necessary semantic change requires explicit review against §5.

## 9. Acceptance criteria and test plan

### Correctness gates

- Same ordered online event IDs, images, EndLSN/phase/ordinals, and target state for identical transactions under old and shared readers.
- Existing LOW/HIGH/recovery model and observation tests retain their invariants.
- No early transaction release, partial unsupported-envelope delivery, skipped strict parse error, heartbeat auto-ACK, or confirmation beyond durable progress.
- Online byte/metadata limits and feedback liveness pass under full queues and long SELECT/handler waits.
- Legacy listener semantics, snapshot startup, ACK tests, streaming abort/savepoint tests, and shutdown behavior remain compatible.
- Unit/fuzz tests cover malformed/trailing frames, relation mutation, giant values, integer/JSON precision, controls, empty transactions and wrap-aware visibility.
- Fault injection covers each window/store/ACK transition plus disconnect, identity change, ownership loss, timeout and cancellation.

Relevant existing suites: `pq/replication/online*_test.go`, `stream*_test.go`, `pq/message/tuple/online_test.go`, `pq/snapshot/online/*_test.go`, integration natural overlap/order, recovery and capacity tests. Extend rather than replacing these oracles with throughput assertions.

### Performance qualification

Run shared-source legacy vs online at 1,000 tx/s for 120s with both INSERT and UPDATE streams. For updates, establish identical initial target/source state before measurement without including seeding in the timed window. Wait for snapshot completion before CDC-only timing. Then run snapshot-plus-updates with fixed chunk size and identical sentinel barrier.

Record actual writer elapsed time/rate/pacing misses; per-second delivery backlog; received/consumed/durable/server-confirmed positions; retained WAL from restart_lsn; confirmed lag; feedback age; heap/RSS; query counts/latencies; sentinel commit and handler-delivery timestamps. Sample final lag both before shutdown and after checkpoint/feedback catches up, labeling sampling delay. Do not report peaks as final lag.

Proposed same-machine target: no sustained online delivery backlog at 1,000 tx/s, 120,001 events received by each arm, identical sentinel commit LSN, and sentinel delivery within 5s of commit with no snapshot work in the measurement. This is a proposed qualification target, not a production SLO or guaranteed outcome from code reuse. If safety-preserving baseline cannot meet it, retain correctness and document/profile the blocker.

### Commands

```sh
go test ./pq/replication ./pq/message/tuple ./pq/snapshot/online ./...
go test -race ./pq/replication ./pq/message/tuple ./pq/snapshot/online
go vet ./...
CDC_ONLINE_10M_BENCH=1 POSTGRES_VERSION=16-alpine go test ./integration_test \
  -run '^TestOnlineCDCOnlyHeadToHead$' -count=1 -timeout=30m -v
```

The commands are a future implementation verification plan; they were not run as part of this documentation-only analysis. Performance success cannot waive any correctness gate.
