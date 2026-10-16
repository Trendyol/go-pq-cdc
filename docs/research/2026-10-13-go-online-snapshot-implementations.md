# Go online snapshots: implementation review and design lessons

Date: 2026-10-13. Status: Source-based research, not runtime qualification or an upstream bug confirmation.

Purpose: Identify reusable designs and failure tests for `go-pq-cdc` without treating README claims as correctness proofs. This report supplements the [online snapshot RFC](../online-snapshot/design-rfc.md). It does not amend or approve that RFC.

## 1. Executive finding

There is relevant Go prior art, but the inspected projects implement different consistency contracts. None is a verified drop-in replacement for this library's proposed merger.

- Estuary is the strongest reference here for checkpoint-grouped incremental backfill and acknowledgement accounting.
- The pgcapture project is explicitly DBLog-derived, but its gateway interleaves replica dumps rather than implementing classic LOW/HIGH conflict removal.
- Kaptanto implements concurrent reads and WAL capture, but source inspection exposes important ordering, transaction, durability, and HA concerns.
- The pgstream and PeerDB projects provide useful snapshot engineering, but their inspected PostgreSQL initial-load paths remain snapshot-first.

The central lesson is to distinguish **state convergence**, **raw event preservation**, and **no stale snapshot emission**. These are different contracts. A solution designed for one cannot be copied unchanged into another.

## 2. Method, scope, and revisions

Search results identified candidates. Shallow source checkouts supplied the evidence. Three independent read-only reviewers examined pgcapture, Estuary, and Kaptanto. The parent reviewed selected findings and the pgstream/PeerDB comparison paths.

No third-party code, build scripts, or tests were executed. Test files were inspected. Failure schedules in this report are source-derived unless explicitly labeled as a hypothesis. No upstream issue or production incident was filed or verified.

| Project | Pinned revision | Actual inspected approach | Reuse assessment |
|---|---|---|---|
| [replicase/pgcapture](https://github.com/replicase/pgcapture) | `e9f4d88d4be1f12bddd72c86bbecfeddc9ea2c62` | Source → Pulsar → logical replicas, with per-consumer replica dumps | Patterns, not an unchanged merger |
| [estuary/connectors](https://github.com/estuary/connectors) | `116e4297937349c477efd6b78bd2838c97a23037` | Chunk scan → WAL fence → grouped checkpoint | Strong design reference, license review required |
| [olucasandrade/kaptanto](https://github.com/olucasandrade/kaptanto) | `368c4f77894bf019dd6f7b057af96987da5fa740` | Concurrent SELECT and WAL, logged-event conflict checks | Useful components, unsafe unchanged adoption |
| [xataio/pgstream](https://github.com/xataio/pgstream) | `7cf534b0dcdbfa7c8e7936c37648d850445a9230` | Initial snapshot completes before replication starts | Legacy-mode reference |
| [PeerDB-io/peerdb](https://github.com/PeerDB-io/peerdb) | `a96b7ce8175cf9c202bd042cc5e78aa9b5ee621e` | Initial snapshot workflow completes before CDC sync | Workflow/recovery-test reference |

The pgcapture checkout was obtained through the older `rueian/pgcapture` URL. Its module and documentation identify `replicase/pgcapture`.

Search coverage is not exhaustive. Some search queries failed, and one exploratory `pgflo/pg_flo` URL was unavailable. Those results are excluded from implementation claims. No maintenance ranking relies on stars or unverified deployment counts.

## 3. Estuary: borrow checkpoint structure, not an assumed DBLog algorithm

### 3.1 Estuary code path

```text
restore source and table cursors
    → start replication and catch up to a fence
    → scan one chunk and emit its rows
    → sample a source fence, write a watermark for progress
    → consume WAL through a transaction-end position at/after the fence
    → emit one checkpoint for the chunk plus WAL catch-up
    → wait for runtime checkpoint ACK before source confirmation
```

The current code does not retain a snapshot-row map and delete conflicting rows between matching LOW/HIGH UUIDs. It emits snapshot rows first and then catches up through WAL. Replication event processing pauses during the synchronous chunk scan/output. The watermark write supplies traffic so an idle source can reach the fence. It is not a requirement to observe that exact UUID.

Evidence: [main loop and checkpoint grouping](https://github.com/estuary/connectors/blob/116e4297937349c477efd6b78bd2838c97a23037/sqlcapture/capture.go#L445-L467), [scan and immediate output](https://github.com/estuary/connectors/blob/116e4297937349c477efd6b78bd2838c97a23037/sqlcapture/capture.go#L982-L1064), [fence implementation](https://github.com/estuary/connectors/blob/116e4297937349c477efd6b78bd2838c97a23037/source-postgres/replication.go#L333-L399).

### 3.2 Estuary strengths

1. Output and progress have a shared commit boundary. Intermediate checkpoints are suppressed during chunk catch-up. Flow's runtime durability contract is part of correctness.
2. ACKs account for a contiguous prefix of checkpoints. Acknowledging N checkpoints consumes the first N pending cursors, not an arbitrary maximum completed row LSN.
3. Feedback has an independent worker. A SELECT or output pause does not eliminate periodic feedback. This does not free WAL beyond durable progress.
4. Key ordering is a checked assumption. Precise scans validate encoded-key order against observed SQL order. String comparisons can use `COLLATE "C"`.
5. Tests use a state-machine oracle. Their validator evaluates legal key transitions rather than only counting messages.

Evidence: [ACK queue](https://github.com/estuary/connectors/blob/116e4297937349c477efd6b78bd2838c97a23037/sqlcapture/capture.go#L1093-L1199), [feedback worker](https://github.com/estuary/connectors/blob/116e4297937349c477efd6b78bd2838c97a23037/source-postgres/replication.go#L404-L425), [ordering check](https://github.com/estuary/connectors/blob/116e4297937349c477efd6b78bd2838c97a23037/sqlcapture/capture.go#L988-L1017), [oracle](https://github.com/estuary/connectors/blob/116e4297937349c477efd6b78bd2838c97a23037/sqlcapture/tests/tests.go#L434-L488).

### Limits and concerns

**Contract mismatch: precise mode does not preserve every CDC event.** Events with keys ahead of the scanned cursor are discarded. An insert/update/delete lifecycle entirely ahead of that cursor can disappear from output. This can preserve final table state, but not an audit/event history. Unfiltered mode has a different contract.

**Raw output can temporarily go backward.** A SELECT can see a newer value than an earlier WAL event emitted during catch-up. The validator reduces each key to its last event within the checkpoint transaction. That runtime boundary cannot be replaced with arbitrary per-event callbacks or unrelated Kafka topics.

**Source-proven fail-open concern:** a requested resume cursor behind an existing slot's `confirmed_flush_lsn` produces a warning, not a hard failure. PostgreSQL can start later than requested. Our library must require explicit recovery for an incompatible slot/checkpoint pair.

**Source-proven operation limitation:** TRUNCATE is warned about and ignored. Mutable primary keys also need destination-aware old-key cleanup. Neither behavior qualifies for our proposed restricted contract without additional handling.

**Untested visibility hypothesis:** a numeric fence does not prove that every preceding decoded transaction is SQL-visible. Retain the RFC's delayed-visibility and restart tests. This source review did not reproduce a failure in Estuary.

Evidence: [precise filtering](https://github.com/estuary/connectors/blob/116e4297937349c477efd6b78bd2838c97a23037/sqlcapture/capture.go#L917-L939), [resume warning](https://github.com/estuary/connectors/blob/116e4297937349c477efd6b78bd2838c97a23037/source-postgres/replication.go#L237-L264), [TRUNCATE](https://github.com/estuary/connectors/blob/116e4297937349c477efd6b78bd2838c97a23037/source-postgres/replication.go#L858-L871).

### 3.3 Estuary lessons for go-pq-cdc

Adopt the checkpoint-prefix accounting, narrow scan/replication interfaces, explicit ordering tests, and independent feedback design. Do not silently adopt precise-mode CDC filtering. Treat Flow's transactional output contract as infrastructure that must be replaced, not omitted.

## 4. pgcapture: useful replica isolation, weaker gateway ordering

### 4.1 pgcapture code path

```text
source PostgreSQL → Pulsar → logical replica
                            ↘ gateway CDC stream
logical replica → CTID dump → same selected gateway stream
```

The gateway remembers the last received source LSN. When a dump arrives, it pauses CDC dispatch, requests a replica page range, emits the returned rows, and resumes CDC. The dumper requires an applied checkpoint at least as large as the requested LSN. It does not read exactly at that LSN or return a matching HIGH fence.

Evidence: [gateway merge](https://github.com/replicase/pgcapture/blob/e9f4d88d4be1f12bddd72c86bbecfeddc9ea2c62/pkg/dblog/gateway.go#L149-L218), [replica check and SELECT](https://github.com/replicase/pgcapture/blob/e9f4d88d4be1f12bddd72c86bbecfeddc9ea2c62/pkg/dblog/dumper.go#L122-L188).

### 4.2 pgcapture strengths

- Dump I/O moves away from the primary. Source capture is independent of an individual consumer's dump.
- `SourceResolver`, `SourceDumper`, and `Scheduler` separate transport, read location, and scheduling.
- Pulsar batch ACK logic counts completion of batch members.
- Decoder tests explicitly cover omitted TOAST values and FULL-identity reconstruction.

Evidence: [batch completion](https://github.com/replicase/pgcapture/blob/e9f4d88d4be1f12bddd72c86bbecfeddc9ea2c62/pkg/source/pulsar.go#L248-L285), [TOAST tests](https://github.com/replicase/pgcapture/blob/e9f4d88d4be1f12bddd72c86bbecfeddc9ea2c62/pkg/decode/pgoutput_test.go#L48-L153).

### Limits and source-derived failures

**No no-time-travel guarantee from the gateway merge alone.** Suppose the gateway is at LSN 10 and the replica already contains value 30. The dump emits 30, then queued CDC at LSN 20 emits 20. A later event can repair the state, but the raw stream regressed. Broker redelivery can create a terminal stale write unless the consumer enforces order or versions.

**Asynchronous dump completion hazard:** successful ACK of the last dump row removes the whole dump from the pending map. If an earlier row is still pending and later fails, its requeue can find no pending dump. This schedule requires out-of-order/asynchronous acknowledgements. A strictly synchronous consumer avoids that schedule.

**Partial SQL results can look successful:** the dump loop returns accumulated rows without checking `rows.Err()`. A failure after a returned prefix can therefore be reported as a successful partial dump. This is a source-proven error-handling gap, not a reproduced production incident.

**Recovery scope is limited:** the scheduler is memory-only. Controller loss needs an external owner to resubmit work. CTID ranges, background contexts, and fully materialized dumps are not a hard byte or time budget.

Evidence: [dump ACK handling](https://github.com/replicase/pgcapture/blob/e9f4d88d4be1f12bddd72c86bbecfeddc9ea2c62/pkg/dblog/gateway.go#L97-L105), [pending-map removal](https://github.com/replicase/pgcapture/blob/e9f4d88d4be1f12bddd72c86bbecfeddc9ea2c62/pkg/dblog/gateway.go#L246-L261), [missing iteration error check](https://github.com/replicase/pgcapture/blob/e9f4d88d4be1f12bddd72c86bbecfeddc9ea2c62/pkg/dblog/dumper.go#L152-L169), [scheduler state](https://github.com/replicase/pgcapture/blob/e9f4d88d4be1f12bddd72c86bbecfeddc9ea2c62/pkg/dblog/scheduler.go#L25-L49).

### 4.3 pgcapture lessons for go-pq-cdc

Borrow the separation between source capture and consumer bootstrap, and explicit batch completion accounting. Do not replace the RFC's primary-read visibility contract with a replica checkpoint inequality. Replica reads need their own source identity, visibility, and ordering proof.

Pulsar, logical replicas, a DDL extension, and gRPC make this a platform architecture, not a small library dependency.

## 5. Kaptanto: real concurrency exposes the critical race

### 5.1 Kaptanto code path

Replication starts before backfill. A separate connection scans keyset pages. A watermark index records logged CDC keys with LSN greater than a sampled `SnapshotLSN`. Each snapshot row checks that index before append. There is no LOW/HIGH closure for each chunk.

Evidence: [startup](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/internal/source/postgres/connector.go#L423-L485), [read boundary](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/internal/backfill/backfill.go#L290-L391).

### 5.2 Kaptanto strengths

- Quoted identifiers, bound continuation values, and composite keyset comparison.
- Conflict-index registration before scanning existing log entries, plus synchronous append observation.
- A memory cap with a scan fallback for the conflict index, rather than silent eviction.
- Stable snapshot IDs and output identities, with page progress saved after output append.
- Explicit tests for failed batch flushes leaving cursor progress unchanged.

Evidence: [cursor construction](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/internal/backfill/cursor.go#L24-L88), [index lifecycle](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/internal/backfill/watermark.go#L161-L250), [flush-failure test](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/internal/backfill/snapshot_engine_test.go#L254-L303).

### Four important concerns

**K1: conflict check and append are not one ordered operation.** The checker releases its lock before the snapshot event enters a batch. The append lock serializes appends, but does not include the earlier decision.

```text
SELECT K returns v1
    → ShouldEmit(K) returns true
    → WAL UPDATE K=v2 is appended
    → buffered snapshot K=v1 is appended
    → final log order is v2, then v1
```

This schedule follows from the code and contradicts stale-row prevention. A deterministic test must force it. The check-error path also emits the row, which is not fail-closed for precedence.

Evidence: [check and buffer](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/internal/backfill/backfill.go#L440-L469), [checker lock](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/internal/backfill/watermark.go#L238-L250), [append lock](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/internal/source/postgres/connector.go#L283-L295).

**K2: one buffer is not enough for interleaved streamed transactions.** Streaming is enabled, but receive state holds one `walBuf`. An ordinary COMMIT flushes it, and a stream ABORT clears it. Large transaction A can stream rows, transaction B can commit, and B can flush A's rows before A later aborts. The inspected implementation lacks per-XID isolation for that schedule.

Evidence: [receive state](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/internal/source/postgres/connector.go#L589-L624), [commit/abort handling](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/internal/source/postgres/connector.go#L657-L682).

**K3: process-crash durability is not host-failure durability.** Badger opens with default options and no explicit sync-write setting. Its pinned v4.9.6 dependency defaults `SyncWrites=false`. Badger explicitly distinguishes process-crash survival from hard-reboot durability. Source ACK after an unsynced append cannot support an unconditional host-failure no-loss claim.

Evidence: [Badger configuration](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/internal/eventlog/badger.go#L41-L47), [pinned dependency semantics](https://github.com/dgraph-io/badger/blob/fbd8d2eefad8be8757249767255faf989945b599/options.go#L370-L380).

**K4: HA metadata can outlive the data it describes.** `--ha` uses shared PostgreSQL checkpoints. Without cluster mode, the event log remains local Badger. If leader A's disk disappears after it advances the shared checkpoint, successor B can resume beyond events it does not possess. A new snapshot cannot reconstruct a missed delete. Cluster mode changes this storage model and requires separate qualification.

Evidence: [storage wiring](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/internal/cmd/root.go#L189-L285), [resume rule](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/internal/source/postgres/connector.go#L124-L132).

Retention also limits recovery: event and dedup records expire. The same-directory SIGKILL test does not prove power-loss or independent-node recovery. None of K1–K4 was executed as a reproduction during this review.

### 5.3 Kaptanto lessons for go-pq-cdc

Adopt cursor, identity, and index-lifecycle ideas. Keep reconciliation and final output ordering under one owner. Preserve our existing per-XID transaction isolation. Define process, host, and node-loss durability separately. Never describe a shared checkpoint database alone as a recoverable HA capture system.

## 6. Useful comparators that do not solve this problem unchanged

### pgstream

`Listener.snapshotAndListen` gets a current LSN, calls `CreateSnapshot`, then calls `StartReplicationFromLSN`. Its inspected `initial` path therefore has the same snapshot-wide streaming delay we want to remove.

Useful practices include small interfaces, shared exported views for CTID parallelism, explicit source-instance checks, cancellation, and schema-column handling. The instance probe explicitly states its probabilistic limitation instead of claiming certainty.

Evidence: [startup](https://github.com/xataio/pgstream/blob/7cf534b0dcdbfa7c8e7936c37648d850445a9230/pkg/wal/listener/postgres/wal_pg_listener.go#L115-L132), [shared CTID view](https://github.com/xataio/pgstream/blob/7cf534b0dcdbfa7c8e7936c37648d850445a9230/pkg/snapshot/generator/postgres/data/ctid_table_reader.go#L31-L55), [instance probe](https://github.com/xataio/pgstream/blob/7cf534b0dcdbfa7c8e7936c37648d850445a9230/pkg/stream/preflight/snapshot_source_instance.go#L10-L44).

### PeerDB

The inspected normal initial-load workflow waits for `SnapshotFlowWorkflow` to finish before entering CDC `SyncFlow`. Its parallel snapshot workers do not imply continuous consumption of the same slot during that snapshot.

Useful practices include explicit workflow phases, source-slot snapshot lifetime management, controlled resync/target swap, and replay tests against recorded Temporal histories. Those replay tests detect workflow nondeterminism after upgrades. They do not alone establish CDC data correctness.

Evidence: [snapshot completion barrier](https://github.com/PeerDB-io/peerdb/blob/a96b7ce8175cf9c202bd042cc5e78aa9b5ee621e/flow/workflows/cdc_flow.go#L729-L842), [subsequent sync](https://github.com/PeerDB-io/peerdb/blob/a96b7ce8175cf9c202bd042cc5e78aa9b5ee621e/flow/workflows/cdc_flow.go#L846-L862), [snapshot lifetime](https://github.com/PeerDB-io/peerdb/blob/a96b7ce8175cf9c202bd042cc5e78aa9b5ee621e/flow/workflows/snapshot_flow.go#L296-L330), [replay test](https://github.com/PeerDB-io/peerdb/blob/a96b7ce8175cf9c202bd042cc5e78aa9b5ee621e/flow/workflows/cdc_flow_replay_test.go#L11-L35).

## 7. What this changes in our implementation plan

| Keep or add | Evidence behind the decision |
|---|---|
| Explicit logical-stream versus materialized-state contract | Estuary deliberately relies on checkpoint reduction and mode-dependent filtering |
| One ordered merger, not independent listener calls | Kaptanto's check/append separation permits a stale-row schedule |
| Gap-aware event and batch completion | Estuary's prefix ACK queue is a strong pattern, pgcapture's dump-last-row ACK is not |
| Transaction boundaries keyed by XID | Kaptanto's shared streamed buffer shows the failure mode our current code must avoid |
| Stage durability matched to failure model | Badger process survival does not imply host durability, shared metadata does not imply shared output |
| Cursor advances after recoverable output | Kaptanto's failed-flush cursor test is directly transferable |
| Retention tied to pending obligations | TTL must not silently erase required output, conflict history, or dedup evidence |
| Explicit stale-attempt and old-owner fencing | Late row ACKs and replay must not complete a different attempt |
| Primary-read visibility proof | Neither a replica lower-bound check nor a numeric source LSN establishes the full read boundary |
| Fail-closed slot/checkpoint mismatch | Continuing after a warning can hide a missing replay interval |

The RFC's stricter design remains justified. A simpler Estuary-style grouped-reduction profile is worth evaluating only if consumers accept its different contract. It must not become the default for existing Kafka event consumers without an explicit decision.

## 8. First regression tests to add

1. Pause after conflict check, append a newer CDC event, then release snapshot append. Require no stale output.
2. Stream A, commit B, then abort A. Require only B's committed effects.
3. ACK the final snapshot row while an earlier row is unfinished or failed. Require the attempt to remain unresolved.
4. Return SQL rows and then an iteration error. Require failed chunk completion.
5. Lose the leader's local stage after the shared checkpoint advances. Require recovery from shared output or fail-closed handling.
6. Resume behind slot confirmation. Require an explicit incompatible-checkpoint error.
7. Expire retention while a window or consumer is pending. Require safe blocking or a declared recovery incident, not silent continuation.
8. Delay source SQL visibility across ACK and restart. Require the next snapshot read to retain its predecessor-visibility guarantee.

These are proposed tests. Their names and scenarios must not be counted as executed coverage.

## 9. Reuse and licensing

pgcapture, Kaptanto, and pgstream contain Apache-2.0 license files. Preserve applicable notices and review dependencies before copying. PeerDB's root license is AGPL-3.0. Estuary defaults to BSL 1.1, with per-directory exceptions possible. No override was found in the inspected `source-postgres` or `sqlcapture` directories.

Estuary's license notice allows Apache/MIT use with written consent. Its BSL terms contain a Data Processing Service restriction and a 2028-02-21 change date. Do not treat the repository as automatically permissive because Apache/MIT files also exist. Legal review must determine permitted reuse for the exact code version and intended product.

Sources: [Estuary notice](https://github.com/estuary/connectors/blob/116e4297937349c477efd6b78bd2838c97a23037/LICENSE), [BSL terms](https://github.com/estuary/connectors/blob/116e4297937349c477efd6b78bd2838c97a23037/LICENSE-BSL), [pgcapture](https://github.com/replicase/pgcapture/blob/e9f4d88d4be1f12bddd72c86bbecfeddc9ea2c62/LICENSE), [Kaptanto](https://github.com/olucasandrade/kaptanto/blob/368c4f77894bf019dd6f7b057af96987da5fa740/LICENSE), [pgstream](https://github.com/xataio/pgstream/blob/7cf534b0dcdbfa7c8e7936c37648d850445a9230/LICENSE), [PeerDB](https://github.com/PeerDB-io/peerdb/blob/a96b7ce8175cf9c202bd042cc5e78aa9b5ee621e/LICENSE).

**Decision:** build the small library-specific merger and recovery boundary, borrow proven engineering patterns, and validate adverse schedules before adopting code. This review supports feasibility, not production readiness.

**Document verify:** `test -s docs/research/2026-10-13-go-online-snapshot-implementations.md`. Every implementation claim links to a pinned source revision. The existing RFC and PDF package remain unchanged.
