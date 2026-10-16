# Concurrent Snapshot + Logical Decoding (DBLog-style) — Design Record

Status: **proposal — prior art reviewed, key decisions resolved (§7), implementation not started.**
One open decision remains (Q5, minimum PostgreSQL version).
Related: `README.md` "Snapshot Feature", `docs/SNAPSHOT_FEATURE.md`, `docs/visibility-gate-design.md`.
References: DBLog (arXiv:2010.12597); Flink CDC incremental snapshot; Debezium PostgreSQL connector;
Brex, *Making CDC snapshot merges correct*. §8 is the prior-art review and currently the authoritative
design section; §3–§6 are the reasoning that led to it.

---

## 1. Problem

`snapshot.mode: initial` creates the replication slot and then **does not consume it** until the
snapshot finishes. On a large, hot table the snapshot runs for hours, `restart_lsn` stays frozen,
`confirmed_flush_lsn` never moves, and WAL accumulates until `max_slot_wal_keep_size` invalidates
the slot (`pg_replication_slots.invalidation_reason = 'wal_removed'`).

There is a **second, unreported** instance of the same class of problem.
`exportSnapshotTransaction` opens one `REPEATABLE READ` transaction and holds it open for the whole
snapshot with `idle_in_transaction_session_timeout = 0`, `statement_timeout = 0` and a 30s
`SELECT 1` keepalive. That backend advertises `pg_stat_activity.backend_xmin`, which prevents
vacuum from reclaiming **any** dead tuple created after that point in that database. On a hot table
this is table and index bloat plus a rising `age(datfrozenxid)`. The same incident class, different
resource.

## 2. Current behaviour, precisely

```
connector.Start()
 ├─ prepareSnapshotAndSlot()                                   connector.go:341
 │   ├─ slot.Create()        CREATE_REPLICATION_SLOT n LOGICAL pgoutput   slot.go:110
 │   │                        → WAL pinned at restart_lsn FROM THIS MOMENT
 │   ├─ snapshotter.Prepare()                                   snapshot.go:99
 │   │   ├─ setupJob() → getCurrentLSN() = pg_current_wal_lsn() on metadataConn
 │   │   ├─ initializeCoordinator()
 │   │   │   ├─ exportSnapshotTransaction()  REPEATABLE READ, held OPEN   coordinator.go
 │   │   │   └─ createMetadata()             chunks built inside that view
 │   │   └─ (advisory lock pg_try_advisory_lock(hash(slotName)))
 │   ├─ stream.OpenFromSnapshotLSN()         sets a bool only              stream.go:884
 │   └─ executeSnapshotWithRetry() → snapshotter.Execute()
 │                                    *** BLOCKS FOR THE WHOLE SNAPSHOT ***
 ├─ slot.Connect()
 ├─ CaptureSlot()
 ├─ stream.Connect()                         IDENTIFY_SYSTEM
 └─ stream.Open()                            START_REPLICATION SLOT n LOGICAL <snapshot_lsn>
                                             → FIRST EVER read from the slot
```

The pause is not incidental — it is pinned by a test. `TestNoWalsenderDuringSnapshot`
(`integration_test/snapshot_no_walsender_test.go:209`) asserts **zero** walsender connections
during the snapshot phase and exactly one afterwards. Any concurrent design must invert that
assertion behind a flag, not silently break it.

### 2.1 Correctness facts to preserve or deliberately change

| # | Fact | Where | Implication |
|---|---|---|---|
| F1 | `snapshot_lsn` is `pg_current_wal_lsn()` on `metadataConn`, taken **before** `exportSnapshotTransaction`, and the slot is created **before** both | `coordinator.go` `setupJob` / `getCurrentLSN` | Three different points in time on three connections. Today the error direction is "stream replays a superset" → duplicates, not loss. That is accidental, not designed. |
| F2 | PostgreSQL exposes the exact boundary: `CREATE_REPLICATION_SLOT` returns `consistent_point` and `snapshot_name`; §47.2.5 "shows exactly the state of the database after which all changes will be included in the change stream" | protocol-replication docs | Replacing F1 with these two values removes the duplicate window by construction. Caveat: the returned `snapshot_name` is "valid until a new command is executed on this connection or the replication connection is closed" → must be imported immediately, or the creating replication connection kept, or a version-specific persistence path used. Needs a PG-version matrix before it is load-bearing. `slot.execReplicationCommand` currently discards the result row and must decode it. |
| F3 | Last CTID chunk has **no upper bound**, "to catch rows added after metadata creation" | `buildCTIDBlockQuery` | A boundary patch. Must not survive into the concurrent mode. |
| F4 | `fetchSnapshotLSN` retries **infinitely** until `cdc_snapshot_job.completed = true` | `stream.go:930` | `Open()` can block forever; concurrent mode removes the ordering dependency that requires it. |
| F5 | `streamTxBuffer` is unbounded; `messageCH` cap 1000; `listenerFunc` is **synchronous** | `stream.go` | Any new buffering must respect existing back-pressure, not add a second unbounded park. |
| F6 | `two_phase` is never requested and correctness comments depend on it staying off | `stream.go:657-660`, visibility-gate P9 | The visibility gate's guarantee breaks if this ever changes. |
| F7 | `messages 'true'` **is** requested (proto v2) but the `'M'` message type is **not parsed** — dropped at Debug | `replication.go:24`, `message.go` default branch | Transactional logical messages are the natural watermark primitive. The parser must be written before any watermark design. |
| F8 | Snapshot events have a **no-op `Ack`** and `CommitLSN = 0` | `connector.go` `snapshotHandler` | Surviving snapshot rows are not durably checkpointed. In a concurrent design the slot cannot be advanced past an open window unless this changes. Breaking change for existing listeners → gate it. |
| F9 | Snapshot `Data` and CDC `Decoded` do not share a key set — `tuple.Data.DecodeWithColumn` silently drops `b` (binary) and `u` (toast) columns | `tuple/data.go:77-93` | A per-key merge that assumes symmetry is wrong. |
| F10 | `queryCondition` filters the snapshot but **not** CDC | documented | Concurrent mode makes the asymmetry visible: orphan `UPDATE`/`DELETE` for rows the snapshot deliberately excluded. |
| F11 | Coordinator lock is `pg_try_advisory_lock(hashString(slotName))`, session-scoped on `metadataConn`; `hashString` is a weak `h*31+c` | `coordinator.go` | Advisory-lock collisions between differently-named slots are possible. Worth hardening while we are here. |

## 3. What DBLog actually specifies

Two corrections to the usual retelling, both load-bearing:

1. There is no "Consistency Component" in the paper. The mechanism is **§3.2 "Full state capture"**
   plus **Algorithm 1 "Watermark-based Chunk Selection"**.
2. **Watermarks are not timestamps.** Algorithm 1 line (1) is `lw := uuid(), hw := uuid()`, written
   as a single-row `UPDATE` into a dedicated watermark table. Their meaning comes only from the
   position at which they reappear in the change stream. A clock-based watermark cannot be detected
   in the log and is not what is specified.

Algorithm 1, per chunk:

```
(1) pause log event processing;  lw := uuid(); hw := uuid()
(2) update watermark table set value = lw
(3) chunk := select next chunk from table        # stored in memory, indexed by PK
(4) update watermark table set value = hw
(5) resume log event processing;  inwindow := false
    loop:
      e := next event from changelog
      if not inwindow:
          if e is not a watermark:  append e to outputbuffer
          elif e.value == lw:       inwindow := true
      else:
          if e is not a watermark:
              (6) if chunk contains e.key: remove e.key from chunk   # removal only
                  append e to outputbuffer                            # always
          elif e.value == hw:
              (7) for each row in chunk: append row to outputbuffer
                  inwindow := false
(8) goto (1) if more chunks
```

Properties that matter for us:

- **Reconciliation rule:** the log wins unconditionally. It is a **set-membership test on the
  primary key**, not a value or version comparison. Removal and append are two independent actions —
  an implementation that `continue`s after removing the key silently loses a live change event.
- **In-window log events are never buffered.** §3.2: *"Log event processing is resumed
  event-by-event after step 4 … without ever needing to cache log event entries."* The only retained
  structure is the chunk, which shrinks monotonically. **Peak memory = one chunk.**
- **Buffer bound = chunk size, and nothing else.**
- **Output order per chunk:** log events up to HW, then surviving chunk rows, then post-HW events.
  Downstream needs no per-key reordering; it applies in arrival order.
- **Guarantee delivered is per-key "order of history", not global commit-LSN order.** Surviving
  chunk rows are emitted at the HW position although they were read at an unknown position inside
  the window — §3.2: *"the exact position of the select is unknown"*. The paper never states it
  gives up global ordering; that is a reading of §1(d) + Fig. 4 + Table 1(d). Do not over-cite it.
- **Steps (2)-(4) run with log processing paused**, and (3) *is* the SELECT. The true property is
  **bounded stall**, and the bound rests entirely on an assumption: chunk selection is *"expected to
  be fast"*. Chunk SELECT latency therefore **is** CDC latency. Nothing in the paper bounds, retries,
  or aborts a slow chunk.
- **No long read transaction.** Non-stale reads are achieved *"via the read committed isolation
  level for single select transactions"* (§3.3). State kept between chunks is a cursor
  (*"the last row of a completed chunk"*) — no snapshot position held across the table.
- **The paper is silent on exactly the things we most need**: crash inside an open window, the
  checkpoint-vs-HW rule, Postgres slot/WAL retention during a dump, output-buffer backpressure
  ("backpressure" has zero occurrences), and any measurement at all. It has **no evaluation
  section**: no snapshot duration, no WAL retained, no throughput, no memory figures. Treat any
  circulating DBLog number as unverified.

Postgres has no `WITH CONSISTENT SNAPSHOT`-style equivalent and does not expose a SELECT's log
position, so the UUID-watermark construction is the correct translation — but Postgres gives us a
cleaner primitive than a watermark table, see §4.

## 4. Postgres primitives available

| Need | Primitive | Notes |
|---|---|---|
| Ordered, in-stream position marker tied to a commit | `pg_logical_emit_message(true, prefix, content)` | `transactional = true` makes the message part of the emitting transaction, so it is decoded in commit order. No watermark table, no row churn, no bloat. Requires pgoutput `messages 'true'` — **already requested** by this library — plus the missing `'M'` parser (F7). |
| Exact snapshot/CDC boundary for a slot | `CREATE_REPLICATION_SLOT` result: `consistent_point`, `snapshot_name`; `SNAPSHOT { 'export' \| 'use' \| 'nothing' }` | See F2. |
| Observe WAL pressure | `pg_replication_slots.wal_status` (`reserved`/`extended`/`unreserved`/`lost`), `safe_wal_size`, `restart_lsn`, `confirmed_flush_lsn`, `inactive_since`, `invalidation_reason` (`wal_removed`, `rows_removed`, `idle_timeout`, `wal_level_insufficient`) | `safe_wal_size` is the natural alerting signal and is currently not exported by this library. |
| Observe MVCC pressure | `pg_stat_activity.backend_xmin`, `pg_replication_slots.xmin` / `catalog_xmin`, `age(datfrozenxid)` | Today only `catalog_xmin` is indirectly relevant; the open export transaction is the bigger hazard. |
| Commit position of a decoded change | pgoutput `Begin.FinalLSN`, `Commit.TransactionEndLSN`, `StreamCommit.CommitLSN`; already surfaced as `ListenerContext.CommitLSN` | `format.*.LSN` is `XLogData.WALStart` (record start), **not** a commit LSN — do not use it for reconciliation. |
| Snapshot visibility predicate | `pg_current_snapshot()` (`xmin:xmax:xip`), PG≥13; `txid_current_snapshot()` before | Already implemented client-side in `visibility.go`. |

Gotchas to design around:

- `idle_replication_slot_timeout` (PG17+) **invalidates** a slot that no replication connection has
  used for longer than the configured duration. Today's flow leaves the slot unconnected for the
  entire snapshot. If any target environment sets this, the current design can invalidate its own
  slot. The concurrent design removes the exposure; note it in the release notes either way.
- `max_slot_wal_keep_size` → `wal_status` goes `unreserved` then `lost`; the slot is then unusable
  and re-snapshot is mandatory. No amount of cleverness removes the need to detect and handle it.
- The exporting transaction of `CREATE_REPLICATION_SLOT` is short-lived by protocol. Do not assume
  `snapshot_name` outlives the creating connection without verifying per supported PG version.

## 5. Options

### A. Drain the slot, buffer durably, replay after snapshot
Keep the exported snapshot; start the stream concurrently; hold CDC events in a bounded, LSN-ordered,
spillable staging buffer from snapshot start until snapshot `END`; then drain in LSN order.

- Preserves today's guarantee exactly: zero duplicates, global LSN order. Smallest conceptual change.
- **It does not solve the problem, it relocates it.** WAL volume generated during the snapshot
  becomes local disk volume, roughly one-for-one. To keep the buffer bounded you must throttle the
  snapshot, which re-introduces the lag.
- Useful only as a bounded grace mode or a stepping stone.

### B. DBLog watermark reconciliation, single reconciler *(recommended target)*
Per chunk: emit LW (transactional message) → read chunk in its **own short** transaction → emit HW.
One reconciler sits in the log path and implements Algorithm 1.

- Buffer = one chunk. Slot advances continuously. WAL does not accumulate.
- No long `REPEATABLE READ` transaction → vacuum works; `backend_xmin` advances.
- Requires: `'M'` parser **(superseded — see §8.2/§8.3: LSN-based brackets do not need it)**; PK-only
  chunking with an efficient range scan (DBLog: *"we only allow selects on tables that have a primary
  key"*); a single ordered emit path.
- Changes the guarantee to per-key ordering. **Resolved: acceptable (§7 Q1).**

### C. B with distributed multi-pod chunk processing
Same, but N workers read chunks concurrently → N simultaneously open windows keyed by a window UUID
carried in the message payload; all chunk rows funnel through the one reconciler.

- Memory = N windows × chunkSize rows. Feasible.
- This is where the real complexity is: worker→reconciler transport, per-window state, and
  crash-in-window recovery that the paper does not define.

### D. Debezium-style exported snapshot with a concurrent stream
Effectively A. Same WAL complaint, from the same cause.

**Recommendation:** **B′ (§8.3, option E)** — DBLog's reconciliation rule, Flink's LSN-bracket
semantics, soundness from a per-chunk temporary slot, buffering bounded to one chunk's duration.
Single reconciler per §7 Q3. Retain A as a bounded fallback for tables the reconciler cannot handle
(no PK, `queryCondition` set, heavy TOAST). Add multi-window (C) only if measured throughput requires it.

## 6. Implementation sketch

Superseded in detail by **§8.3 (option E)**; items 1, 5, 8–13 below stand unchanged.

**The reconciler must be a pure component over an event stream.** That is non-negotiable: it is the
only way to get the exhaustive property-based confidence in §7 that a mission-critical deployment
needs. Design it as `reconciler.Feed(event) → []emittedEvent` with injected clock and injected
store, before wiring it to anything.

New / changed:

1. `pq/message/format` — parse `'M'` (`LogicalMessage`: `transactional`, `isLast`, `prefix`,
   `content`, LSN) and the streamed variant.
2. `pq/snapshot/reconciler.go` — window state machine (Algorithm 1), PK set per window, bounded
   store with optional spill, monotonic LSN guard.
3. `pq/snapshot/coordinator.go`
   - concurrent mode: **no** long-lived `exportSnapshotTransaction`;
   - per chunk: `BEGIN; SELECT pg_logical_emit_message(true,'go-pq-cdc:lw',…); COMMIT` →
     chunk `SELECT` in its own `REPEATABLE READ` transaction → `…'go-pq-cdc:hw'…`;
     LW commit must precede the read and HW commit must follow it, so the bracket **contains** the
     whole read. That containment is what makes a multi-row scan sound (the paper's own acknowledged
     gap: a multi-row scan is not instantaneous).
   - new columns on `cdc_snapshot_chunks`: `window_id`, `lw_lsn`, `hw_lsn`, `window_state`.
4. `connector.go` — start `slot.Connect` / `stream.Connect` / `stream.Open` **before**
   `snapshotter.Execute`.
5. **Single emit path.** Today `snapshotHandler` and `processLoop` both call `listenerFunc`
   independently. The reconciler must be the only writer. This is the largest structural change and
   the reason to add a new orchestrator type rather than patch `Start()`.
6. **Checkpoint rule (the correctness-critical one):** never ack the slot past the earliest open
   window's LW until that window's HW has been observed **and** its surviving rows handed to a
   listener that acked. This requires a real `Ack` on snapshot events (F8) — a breaking semantic,
   gated behind the new mode.
7. **Recovery rule:** window state is never trusted across a restart. On start, ignore any LW with
   no matching HW, re-read those chunks (chunk status is the durable truth), and resume decoding at
   the checkpointed LSN ≤ earliest open LW.
8. Chunking restriction: concurrent mode requires a single-column PK and `integer_range`. Exclude
   `offset` outright. `ctid_block` has no clean proof under concurrent updates (one key can appear in
   two chunks' reads); either exclude it or gate it behind an explicit opt-in with the risk stated.
9. Slot start LSN: switch from `pg_current_wal_lsn()` to the slot's `consistent_point` (F2).
10. Special cases to handle explicitly: `TRUNCATE` inside a window (no per-key events ⇒ stale rows
    survive ⇒ must force re-snapshot of that table), DDL/schema drift mid-snapshot, `queryCondition`
    orphan events (F10), toast/binary key-set asymmetry (F9).
11. Config: `snapshot.mode: concurrent_initial`, `maxInflightWindows`, `chunkBufferBytes`,
    `spillDir`, `throttleWhenFull`, `chunkReadIsolation`, `fallbackMode`.
12. Metrics: open-window count, chunk-set bytes, chunk-rows-dropped-vs-emitted, messageCH depth,
    `streamTxBuffer` depth/bytes, `safe_wal_size`, `age(xmin)`/`age(catalog_xmin)`, `inactive_since`,
    `invalidation_reason`, snapshot event delivery count.
13. Tests: invert `TestNoWalsenderDuringSnapshot` into
    `TestWalsenderActiveDuringConcurrentSnapshot`; keep the old one for the serial mode.

## 7. Decisions

Resolved 2026-10-13:

- **Q1 Ordering — RESOLVED: per-key order is sufficient.** DBLog-style reconciliation is admissible.
  Consequence: README's **"No Duplicates: Seamless transition from snapshot to CDC"** and
  `docs/SNAPSHOT_FEATURE.md`'s **"No overlap! CDC starts from snapshot LSN"** become false for the new
  mode. They must be rewritten, not qualified. Fix the contract *before* the code; see the note under
  §9.2 item 3 about the global-LSN-order corollary.
- **Q2 Delivery — RESOLVED: at-least-once, sinks idempotent.** The real-`Ack` breaking change is
  admissible behind the new mode. Matches Flink CDC's own failover behaviour (§8.2): a chunk is
  re-read wholesale on restart, so the sink already has to absorb duplicates.
- **Q3 Workers — RESOLVED: single reconciler for v1.** One in-flight window, one slot pair
  (main + at most one temporary). Removes multi-window state, worker→reconciler transport, and N-way
  crash recovery from v1.
- **Q4 Scope — RESOLVED: WAL retention and `backend_xmin` bloat in the same change.**
  Independently corroborated as a real production failure mode, see §8.4 item 6.
- **Q5 Floor — OPEN.** PG15/16/17 are in CI. Still needs a stated minimum, because several
  observability columns (`safe_wal_size` semantics, `inactive_since`, `invalidation_reason`,
  `idle_replication_slot_timeout`) are PG17-only.

## 8. Prior art, and the design it points to

### 8.1 Debezium for PostgreSQL is Option A, and it corroborates F2

- **It does not read the log during the initial snapshot.** Retained WAL *is* the buffer. That is
  precisely the complaint being fixed here; do not treat Debezium as evidence that A is adequate.
- Debezium never calls `pg_export_snapshot()` itself. It takes the `snapshot_name` returned by
  `pg_create_logical_replication_slot` and runs `SET TRANSACTION SNAPSHOT '<name>'`, re-importing it
  on each pooled connection when `snapshot.max.threads > 1` "so that a parallel snapshot is
  consistent across connections". **This is F2 confirmed in production code by the most widely
  deployed implementation of this exact problem.**
- Mode enum correction worth recording: `exported` and `recovery` are **not** PostgreSQL modes
  (`recovery` is binlog/MySQL). Current PG values: `always`, `initial`, `initial_only`, `no_data`,
  `when_needed`, plus community `configuration_based` / `custom`.
- `snapshot.locking.mode` is PG-specific with default **`none`**; `shared` only takes
  `ACCESS SHARE` (`lock_timeout` = `snapshot.lock.timeout`, 10s) and prevents schema change, not
  writes. The real PG knob is `snapshot.isolation.mode`. Do not port MySQL locking assumptions.

### 8.2 Flink CDC is *not* a literal DBLog implementation — and its shape fits this repo better

Flink CDC's incremental snapshot writes **nothing to the source** and uses **no exported snapshot**:

- `AbstractScanFetchTask`: `LOW = currentOffset()` → ranged chunk `SELECT` → `HIGH = currentOffset()`
  → backfill the WAL range `(LOW, HIGH]`, terminated by an `END` watermark.
- Watermarks are fabricated in-process records (schema
  `io.debezium.connector.flink.cdc.embedded.watermark`, fields `split_id` / `watermark_kind` ∈
  {LOW, HIGH, END}) carrying offsets obtained from a JDBC query. There is no "chunk freeze" event.
- `IncrementalSourceScanFetcher` buffers `[LOW … rows … HIGH]`, rewrites it with post-HIGH changelog
  records by key, stops at END. `shouldEmit` on the stream side passes a change event only if its key
  is inside a *finished* chunk's range **and** its offset is after that chunk's high watermark; at the
  maximum high watermark the table enters pure stream phase.
- **Per-chunk slots.** `PostgresScanFetchTask` creates `slot.name_<subtaskId>` before the chunk read
  and drops it on split completion (`DROP_SLOT_ON_STOP=true`); the global stream split keeps its slot.
  Peak ≈ parallelism + 1 slots.
- Per-chunk read is a plain `autoCommit(false)` ranged `SELECT` with no isolation override and no
  snapshot import → effectively READ COMMITTED.
- `scan.incremental.snapshot.backfill.skip=true` sets `HIGH := LOW` and its own source comment says
  this "downgrades the delivery guarantee to at-least-once" — i.e. **the backfill is the correctness
  mechanism**.
- Failover mid-chunk: split state holds only start / end / highWatermark, no row cursor ⇒ the chunk
  is re-read wholesale ⇒ sinks must be idempotent (consistent with Q2).
- `PostgresStreamFetchTask.commitCurrentOffset` deliberately commits the *checkpoint* LSN, not the
  current COMMIT LSN, "to avoid losing a `BEGIN/UPDATE/COMMIT` sequence after restore". Compare F4
  and §9.2 item 3.

**Consequence for the design: the `'M'` logical-message parser is not required for this feature.**
It is still worth building (F7 is a real blind spot), but it is not on the critical path once
watermarks are LSNs rather than in-stream tokens.

### 8.3 The actual hard problem: bracket soundness

This is the part the DBLog paper does not solve, Flink CDC has shipped bugs in
(flink-cdc #4323, lost events after checkpoint recovery), and any implementation here will get wrong
on the first attempt. State it explicitly:

> The reconciliation is correct **iff** every change not reflected in the chunk read has a log
> position inside `(LOW, HIGH]`.

Any change with a log position ≤ LOW that the read missed is already emitted downstream, so the
stale chunk row becomes permanent time travel — bug class 3 in §8.4.

The reason `LOW = pg_current_wal_lsn()` before the read is **not** sufficient: Postgres flushes a
transaction's commit record to WAL *before* that backend leaves the proc array
(`ProcArrayEndTransaction`), and with `synchronous_standby_names` set it waits for standby
acknowledgement in between. So a transaction can have commit LSN < LOW and still be invisible to a
snapshot taken after LOW. **This repo already diagnosed and solved exactly that race** — it is the
entire content of `pq/replication/visibility.go` and design decision P5 in
`docs/visibility-gate-design.md`. The precedent is in-tree.

Candidate bracketings:

| Bracketing | Sound? | Cost |
|---|---|---|
| `LOW/HIGH = pg_current_wal_lsn()` around the read (Flink's approach) | Not unconditionally. Residual sync-replication race; invisible txn with commit LSN < LOW ⇒ permanent stale row. | Cheapest. No extra slot, no messages. |
| DBLog literal: transactional logical messages as LW/HW, `messages 'true'` | Same residual race, relocated: LW's completion being observable does not imply every transaction that completed before LW has left the proc array (independent sync-rep waits; xid order is assignment order, not commit order). | Needs the `'M'` parser. Still no stronger guarantee. |
| Guard-gated per-chunk `REPEATABLE READ`: wait for the last emitted xid via the existing `visibilityGuard`, then read | Better, still not a proof. Visibility of one xid does not imply visibility of all previously committed xids. | Free — reuses shipped code. |
| **Per-chunk temporary logical slot**: `CREATE_REPLICATION_SLOT … TEMPORARY`, read the chunk under the returned `snapshot_name`** | **Sound by definition.** §47.2.5: the slot's exported snapshot "shows exactly the state of the database after which all changes will be included in the change stream". The snapshot↔decoding-position relationship is defined by the server, not inferred from wall-clock. This is the only Postgres primitive with the right semantics. | One temporary slot + one dedicated connection per in-flight chunk. With Q3 (single reconciler) that is exactly **+1 slot and +1 connection total.** |

**Recommended v1 shape (call it E):** Flink's bracket semantics, DBLog's reconciliation rule,
implemented against the *live* stream so no backfill is needed:

1. The main stream runs continuously into the reconciler (Q3: one reconciler, single writer).
2. Open a window: record `LOW` = current main-stream position; take a **per-chunk temporary slot**
   and read the chunk under its exported `snapshot_name`.
3. Hold the slot's ack back to the earliest open `LOW`, and buffer in-window events in LSN order.
   Retention is bounded by **one chunk's SELECT duration**, not the snapshot's duration — that is the
   whole win, and it applies to WAL and to `backend_xmin` equally.
4. For each in-window event whose PK ∈ chunk set → remove the chunk row; **always emit the event**
   (removal and emission are independent; conflating them loses changes).
5. On chunk completion: emit surviving chunk rows, then release buffered in-window events in LSN
   order, then advance the ack past `LOW`.
6. No backfill, no watermark messages, no writes to the source database.

Then prototype the cheaper guard-gated variant behind the same interface and **spend a week trying to
break it** with the §8.4 item-3 test (Brex's pgbench harness, synchronous standby forced on). If it
survives, ship it instead of the extra slot and record why.

### 8.4 Publicly reported failure modes → mandatory test matrix

Symptom — cause — reference — the test that catches it.

1. **Boundary duplicates.** 17M-row table produced 54M messages; snapshot READs emitted for rows the
   stream had already passed. [DBZ-8704](https://github.com/debezium/dbz/issues/1336) ·
   [report](https://groups.google.com/g/debezium/c/r80xVM5BWxk) — *Test:* snapshot while upserts hammer
   a small key space; assert one terminal value per key.
2. **Lost events at the boundary.** Restored offset had `lsn == lsn_proc == lsn_commit`, so
   `WalPositionLocator(C0,C0)` advanced `startStreamingLsn` past the first transaction.
   [flink-cdc #4323](https://github.com/apache/flink-cdc/pull/4323) — *Test:* checkpoint immediately
   after a COMMIT, restart, write one transaction, assert delivery. Directly relevant to F4 and to any
   change in the ack path.
3. **Snapshot overwrites a newer update** — the time-travel class. Precedence lived in TTL state and
   "the dangerous condition is forgetting precedence too early": the marker expired before the
   delayed READ. [Brex, *Making CDC snapshot merges correct*, part 2](https://www.brex.com/journal/making-cdc-snapshot-merges-correct/part-2)
   — *Test:* their pgbench harness with a snapshot triggered mid write-load, then set-difference sink
   vs source must be empty; add a forced marker-expiry variant.
   **Required reading for whoever implements this.**
4. **Isolated snapshot blocks DDL.** `ALTER TABLE` on a captured table hangs for the whole snapshot;
   the snapshot transaction also blocks DB-wide index creation.
   [SO](https://stackoverflow.com/questions/77287022/schema-changes-cannot-complete-during-debezium-snapshotting)
   · [DBZ-4272](https://github.com/debezium/debezium/pull/2969) — *Test:* snapshot a large table while
   running `ADD COLUMN`.
5. **Slot invalidation.** `wal_status = lost` under `max_slot_wal_keep_size` across a long snapshot;
   requires drop + re-snapshot. [Patroni #3641](https://github.com/patroni/patroni/issues/3641) ·
   [writeup](https://nryanov.com/postgresql/debezium-production-issues/) — *Test:* small
   `max_slot_wal_keep_size`, slow snapshot on a busy DB; assert a loud, actionable failure rather than
   a silent gap.
6. **Long snapshot transaction blocks vacuum cluster-wide** — one `REPEATABLE READ` snapshot
   transaction pins the horizon so vacuum cannot clean tables the session never touched
   [writeup](https://nryanov.com/postgresql/debezium-production-issues/). **This is §1's second
   problem, independently confirmed in production.** — *Test:* assert `age(xmin)` /
   `age(catalog_xmin)` and `pg_stat_activity.backend_xmin` stay bounded throughout a snapshot under
   write load; the per-chunk design must pass, today's code must fail.
7. **DELETE of an already-read row resurrects it.** Streaming DELETE precedes the chunk READ; without
   a delete marker the READ re-emits the row. [Brex part 2](https://www.brex.com/journal/making-cdc-snapshot-merges-correct/part-2)
   — *Test:* DELETE a row in an unread chunk before it is scanned; assert no terminal READ for that PK.
   Note Brex's own e2e omits deletes — cover that gap.
8. **INSERT after chunk read, before watermark** — row appears twice; Debezium documents this as an
   expected trade-off under weaker isolation.
   [Debezium PG docs](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
   — *Test:* insert into an already-scanned chunk range pre-watermark; assert one terminal value.
9. **Chunk drift / key types.** `MIN(uuid)/MAX(uuid)` unsupported; varchar-leading PKs yield one giant
   chunk → OOM; chunk key ≠ PK makes ranges overlap.
   [FLINK-34883](https://issues.apache.org/jira/browse/FLINK-34883) ·
   [FLINK-35740](https://github.com/apache/flink-cdc/pull/3448) ·
   [FLINK-37332](https://github.com/apache/flink-cdc/pull/3928) — *Test:* UUID / varchar / composite-PK
   tables under write load; assert ranges are disjoint and their union equals a full read.
   Reinforces §6 item 8. **NO PUBLIC REPORT** was found for a PK updated *during its own chunk* —
   treat that sub-case as inferred and test it anyway.
10. **Restart mid-snapshot → gap or double-read.** Offset flush advanced `restart_lsn` without a
    durable checkpoint; chunks re-emitted after restore.
    [RisingWave #25071](https://github.com/risingwavelabs/risingwave/issues/25071) ·
    [flink-cdc #1061](https://github.com/apache/flink-cdc/issues/1061) ·
    [DBZ-1961](https://issues.redhat.com/browse/DBZ-1961) — *Test:* kill at each chunk boundary;
    assert no chunk skipped, re-reads converge under upsert, and **the slot is never advanced past
    un-checkpointed data** (the §6 item 6 rule).
11. **Cross-table window leakage.** Backfill filtering NPE'd/hung when WAL carried rows for *other*
    captured tables mid-chunk because `isChangeRecordInChunkRange` skipped the `tableId` filter.
    [FLINK-39633](https://issues.apache.org/jira/browse/FLINK-39633) ·
    [FLINK-39621](https://github.com/apache/flink-cdc/pull/4388) — *Test:* two captured tables under
    load, snapshot only one. Very likely to be hit here because the reconciler will sit on the shared
    `processLoop` path that already multiplexes every published table.

Caveats on the above, stated honestly: items 1 and 8 are connector-generic / SQL Server and not
Postgres-confirmed; items 5–6 are Postgres *operations* incidents rather than connector bug reports
(mechanism solid, incident match approximate); no single public report shows a Postgres restart
producing both a gap and a double-read. Debezium documents boundary duplicates as expected while
DBZ-8704 files the same symptom as a bug — the difference is whether suppression was **promised**,
which is why the Q1 contract rewrite in §7 is a gate, not a follow-up.

## 9. Reliability plan

### 9.1 Define the correctness oracle before writing code

Adopt **snapshot-equivalence** as the single stated property:

> Replaying the emitted event stream onto an empty copy of the table must yield a table identical to
> the source at one single point in time, and for every key the sequence of values observed must be a
> subsequence of that key's true version history.

Everything else (no-loss, no-duplicate, no-time-travel) is a corollary. Write this down as a doc
before the first commit; it is the thing every test asserts.

### 9.2 Test layers

1. **Model-based property tests on the pure reconciler.** Generated interleavings of chunk reads,
   LW/HW, DML, deletes-then-inserts, PK drift; checked against the model oracle. Runs in seconds,
   covers the cases that are nearly impossible to hit on a live server. This is where most of the
   confidence lives. Requires §6's purity constraint.
2. **Wire-trace replay.** Record real pgoutput byte streams from testcontainers under seeded
   workloads; replay them through the reconciler across a config matrix (chunk size, write rate, PK
   density, streaming on/off, proto v1/v2). Deterministic, no flakes, diffs on every change.
3. **Integration on real PostgreSQL** (extends the existing testcontainers harness). Reuse the
   existing CI matrix (15, 15.7, 16, 16.3, 17). Missing harness pieces that must be built as
   reusable helpers, not one-off tests:
   - sustained multi-connection write-load generator at a target TPS;
   - event recorder + final-DB-state oracle (set diff + per-key sequence check across the whole
     snapshot→stream boundary — no existing test does this; today's tests count messages);
   - monotonic-LSN validator (restart resumes at the checkpointed LSN, no gap, no overlap);
   - kill/restart-mid-snapshot and kill-inside-a-window helpers;
   - PostgreSQL restart helper (testcontainers supports it; no test uses it);
   - `max_slot_wal_keep_size` fault helper that forces `wal_status='lost'`;
   - network-partition helper (Toxiproxy or testcontainers network disconnect) — none exists today;
   - chaos-loop runner interleaving the above for N rounds.
4. **Differential comparison.** `benchmark/benchmark_cdc` already runs Debezium with Redpanda and
   Grafana. Use it as an oracle: same workload, same table, diff go-pq-cdc's output against
   Debezium's. Existing infrastructure, external reference implementation. Two limits: Debezium is
   at-least-once by design, so the diff must compare *terminal state per key*, not event streams; and
   the boundary-duplicate case is where Debezium's own behaviour is contested (§8.4 items 1 and 8), so
   a disagreement there is information, not automatically a go-pq-cdc defect.
5. **Fault matrix with required behaviour written down first.** At minimum: crash after LW before
   HW; crash after HW before flush; crash mid-chunk; chunk reclaimed by `claimTimeout` while the
   reconciler still holds its set; PG restart; network partition; slot `wal_removed`; slot
   `idle_timeout`; `rows_removed`; standby promotion with `slot.failover`; DDL mid-snapshot;
   `TRUNCATE` mid-window; PK update moving a row across a chunk boundary; delete-then-insert inside
   a window; insert into a chunk range after the read but before HW.
6. **Soak / load.** Sustained write load at production-equivalent rate for ≥ the expected worst-case
   snapshot duration. Pass criteria expressed in Postgres terms: `wal_status` never leaves
   `reserved`/`extended`, `safe_wal_size` never below a floor, `age(backend_xmin)` of CDC backends
   stays bounded, no oracle violation.
7. **Continuous reconciliation in production.** A scheduled source-vs-target checksum over sampled
   key ranges. It is the only mechanism that detects silent corruption after launch, and it doubles
   as the evidence for the release gate. Cheapest high-value item on this list.

### 9.3 Rollout gates

1. Feature flag default **off**; opt-in per slot; documented rollback to serial mode.
2. **Shadow mode** on a canary: run the concurrent reconciler and the existing serial path in
   parallel and diff their outputs. Only viable on a low-write table, but it is the strongest
   pre-production evidence available.
3. Explicit **auto-fallback**: if window buffers or retained WAL exceed budget, stop the concurrent
   path and fall back, rather than invalidating the slot.
4. Runbook: slot invalidated mid-snapshot ⇒ re-snapshot is mandatory; how to trigger it
   (`snapshot.resnapshot`) and how to verify before re-enabling CDC.
5. ADR + README rewrite + migration guide. The documented guarantee changes: README currently claims
   **"No Duplicates: Seamless transition from snapshot to CDC"** and `docs/SNAPSHOT_FEATURE.md`
   asserts *"No overlap! CDC starts from snapshot LSN."* If Q1 lands on per-key ordering, those
   sentences become false for the new mode and must be rewritten, not qualified.
6. Version matrix for `messages`, `safe_wal_size`, `invalidation_reason`, `inactive_since`,
   `idle_replication_slot_timeout`, and `snapshot_name` lifetime across every supported PG version.

### 9.4 Known doc/code defects found during this survey (fix regardless)

- `docs/SNAPSHOT_FEATURE.md` documents `forceResnapshot`; the real key is `resnapshot`.
- `publication_names` is interpolated into `START_REPLICATION` without escaping
  (`replication.go:32`).
- `queryCondition` is string-interpolated into chunk queries; `docs/MR_REVIEW_feature_snapshot_query_condition.md`
  flags an OR-precedence bug and SQL injection. Verify the fixes landed before relying on filters.
- `'M'` messages are requested but silently dropped at Debug level — a data-loss-shaped blind spot
  for anyone using logical decoding messages.
