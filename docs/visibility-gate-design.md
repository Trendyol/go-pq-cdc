# Visibility gate — implementation handoff

Status: **implemented** (PRs #166–#169 and the README sections "Visibility Guard", "Commit LSN and reading from a standby", "Failover slots"); kept as the design record. Decided 2026-09-03 by a three-round consensus between Codex, Claude Opus 4.8 and Claude Fable 5.1, each verifying claims against PostgreSQL `master` sources. Full report (mechanism, verdict tables, decisions): https://claude.ai/code/artifact/555f3849-212d-4de4-ad33-0a10d01eb93d

## Problem in two sentences

A logical walsender streams a transaction as soon as its commit record is flushed (`xact.c RecordTransactionCommit` → `XLogFlush` wakes walsenders), but the row becomes visible to new snapshots only after `ProcArrayEndTransaction`, which runs after `SyncRepWaitForLSN`. With Patroni `synchronous_mode` the gap is at least the standby ack round-trip, so a consumer that reads the primary right after receiving the CDC event can miss the row (read-after-write); the same ordering also allows "phantom" events if the primary dies before the standby received the commit.

## Agreed decisions (P1–P10)

| # | Decision |
|---|---|
| P1 | Predicate is **client-side, epoch-free, 32-bit**: `visible ⇔ int32(xid − uint32(xmax)) < 0 ∧ xid ∉ {uint32(x) : x ∈ xip}` over `pg_current_snapshot()::text` (`txid_current_snapshot()` on PG < 13). No `pg_xact_status`, no `pg_visible_in_snapshot`, no xid8/epoch reconstruction: `pg_snapshot_xmax` is `latestCompletedXid + 1`, so inside the gap `xid ≥ xmax` and any xmax-anchored epoch math is wrong. |
| P2 | Guard connection lifetime is bound to the replication session: opened right after `IDENTIFY_SYSTEM`, to the same host as the replication DSN, autocommit, never reconnects on its own; any guard error is a stream error → existing restart path → redelivery from confirmed LSN. At open, **mandatory** checks: `pg_is_in_recovery() = false` and live timeline `== s.system.Timeline` via `pg_walfile_name(pg_current_wal_lsn())`. Per poll: `pg_is_in_recovery()` in the same statement as the snapshot (never `pg_walfile_name` per poll; it ERRORs in recovery). `system_identifier` is not compared (shared by the whole Patroni cluster). |
| P3 | One knob `failMode`, default `closed`. closed = timeout or guard error ⇒ error ⇒ stream restart (with backoff and a distinct error class "visibility guard unreachable") ⇒ redelivery. open = dispatch after timeout with warning + counter. **No circuit breaker.** |
| P4 | Timeout default 10 s. At open read `SHOW wal_sender_timeout`; if non-zero and gate timeout ≥ half of it, reject config with a clear error (while `process()` blocks, `messageCH` fills, sink blocks, keepalive replies stop). Poll 5 ms doubling to 250 ms cap with jitter. Non-positive durations rejected. |
| P5 | Sync-rep corollary (documented, not code): with `synchronous_standby_names` active, "visible on primary" ⇒ "acknowledged by the sync standby at the configured `synchronous_commit` level", so a fail-closed gate also prevents phantom events on Patroni failover without PG17. Caveats: Patroni non-strict mode may turn sync off; cancel/terminate during `SyncRepWaitForLSN` makes the tx visible without ack (`syncrep.c:301-332`); per-session `synchronous_commit = local/off`; `remote_write` is receipt, not flush. |
| P6 | `slot.failover: true` (PG17+) ⇒ `CREATE_REPLICATION_SLOT … (FAILOVER true)`; `ALTER_REPLICATION_SLOT … (FAILOVER true)` for an existing slot. Docs: needs `synchronized_standby_slots` on the primary (empty ⇒ no waiting), `sync_replication_slots` on standbys; an inactive listed slot stalls the logical walsender; Patroni ≥ 4.1.0 leaves failover slots alone and does not set the GUC. Complementary to the gate. |
| P7 | Expose `CommitLSN` (= `Begin.FinalLSN`, start of the commit record) on every delivered message. Documented replica rule is **strict**: `pg_last_wal_replay_lsn() > CommitLSN` (or PG17 `pg_wal_replay_wait(CommitLSN)` then the same strict check). `>= TransactionEndLSN` is equivalent where the end LSN is available. |
| P8 | `StreamAbort` sub-transaction bug is real and is fixed **first, in a separate PR**. |
| P9 | Gate lives in `process()`, on the first message of each new xid. xid is stamped in `sink()` from `Begin.Xid` (non-streaming) and `StreamStart.Xid` (streaming); `flushWithLSN` / `flushTx` copy it when rebuilding `Message`. Heartbeat-table messages bypass the gate. Gate state reset on stream restart. Code comment: "everything in `messageCH` is committed on the server as long as `two_phase` is off" (`ReorderBufferCommit` is reached only from `DecodeCommit`; `DecodePrepare` would break it; this library never sends `two_phase`). |
| P10 | Docs state the guarantee (a fresh snapshot on the same primary taken after dispatch sees the row) and its limits: replica reads, pooler routing to a replica, the consumer's own open REPEATABLE READ / SERIALIZABLE snapshot, later deletes, RLS/privileges. Consumer retry and self-contained outbox payloads remain recommended. Say plainly that replica/pooler routing is the more likely cause of the reported symptom in Patroni deployments. |

## Build order

### 0. Fix `StreamAbort` sub-transaction handling (separate PR, first)

- Bug: `pq/replication/stream.go` `dispatchMessage` calls `streamBuf.discardTx(msg.Xid)` on every `StreamAbort`, but `format.StreamAbort` carries `Xid` **and** `SubXid`. A `ROLLBACK TO SAVEPOINT` or a plpgsql `EXCEPTION` block inside a transaction larger than `logical_decoding_work_mem` drops every buffered row; `STREAM COMMIT` then emits nothing. Default config (proto v2, streaming on) is affected.
- Fix, mirroring `worker.c stream_abort_internal`: streamed DML messages already carry the (sub)xid (`format.Insert.XID` etc.). In `streamTxBuffer.append` record the slice index at which each new sub-xid first appears for the active top-level xid. On `StreamAbort` with `SubXid != Xid`, truncate `txns[Xid]` to that index and drop the recorded sub-xids at or after it. Only `SubXid == Xid` discards the whole transaction.
- Tests: sub-abort in the middle of a chunk; nested sub-xids; interleaved transactions (TX-A chunk, TX-B chunk, TX-A sub-abort); top-level abort.

### 1. Visibility gate (opt-in, core stream)

Config (`config/config.go`, validated in `SetDefault`/validation path):

```yaml
visibilityGuard:
  enabled: true
  failMode: closed        # closed | open
  timeout: 10s            # must be < wal_sender_timeout / 2 (checked at open; skipped when wal_sender_timeout = 0)
  pollInterval: 5ms       # backoff doubles to 250ms cap, with jitter
```

Guard (`pq/replication/visibility.go`, rewrite; keep the existing parser test style):

```sql
-- at open (one statement); must return (false, s.system.Timeline)
SELECT pg_is_in_recovery(),
       ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int;
SHOW wal_sender_timeout;
-- every poll (autocommit); PG < 13: txid_current_snapshot()
SELECT pg_is_in_recovery(), pg_current_snapshot()::text;
```

- Parse `xmin:xmax:xip1,xip2,...` as uint64, compare with **low 32 bits** and PostgreSQL's modular rule (`int32(a-b) < 0`, valid within 2^31, same as the server). Cache the last parsed snapshot: an xid that precedes the cached `xmin` (modular compare) is visible without a query; anything else takes a fresh poll.
- Probe `pg_current_snapshot()` at open; on `42883` switch to `txid_current_snapshot()` **and run it once** to confirm.
- `pg_is_in_recovery() = true` on any poll, timeline mismatch at open, malformed snapshot: error (stream restart). Never treat errors as "visible".
- The guard never reconnects. `Close()` order: cancel context first, then close guard, then replication connection; set the guard field to nil.

Plumbing (`pq/replication/stream.go`):

- Add `xid uint32` (and `commitLSN`, see step 2) to `Message`; stamp in `dispatchMessage` on `Begin` / `StreamStart`; `flushWithLSN` and `flushTx` must copy it.
- In `process()`: keep `lastGatedXid`; when `msg.xid != lastGatedXid` and the message is not a heartbeat, call `guard.wait(ctx, xid)` before invoking the listener; on `closed` policy an error is returned from the stream (new error class) and the message is dropped un-acked; on `open` log warn, increment counter, dispatch.
- Open the guard in `Open()` after `IdentifySystem` succeeded and slot capture is settled (avoid the `ErrorSlotInUse` retry churn), i.e. after `setup()`.
- Metrics: `visibility_wait_duration` histogram, `visibility_timeout_total`, `visibility_fail_open_total`.

Tests: predicate table (below xmin, in xip, between, wraparound at 2^32 boundary, xid ≥ xmax ⇒ not visible), snapshot cache, gate integration with a multi-message transaction (only the first message waits), streaming `flushTx` path, heartbeat bypass, `failMode` both ways, timeout-vs-`wal_sender_timeout` config rejection, `pg_is_in_recovery` flip mid-stream ⇒ error, close ordering.

### 2. `CommitLSN` on every message

`Begin.FinalLSN` is already parsed; carry it on `Message` and expose it on the listener context (streaming: `StreamCommit.TransactionEndLSN` is available in `flushTx`; still name the field `CommitLSN` and document the strict `>` rule).

### 3. `slot.failover`

`pq/slot/slot.go` currently builds `CREATE_REPLICATION_SLOT %s LOGICAL pgoutput` with no options. Add `failover` to `slot.Config`; append `(FAILOVER true)` on PG ≥ 17 (reject on older servers with a clear error); for an existing slot issue `ALTER_REPLICATION_SLOT %s (FAILOVER true)` when the option is on and `pg_replication_slots.failover` is false.

### 4. Docs (README section)

Mechanism in five lines, the config block, the guarantee and its limits (P10), the sync-rep corollary and caveats (P5), the PG17 failover-slot setup for Patroni (P6), the replica rule (P7).

## Do not

- Do not use `pg_xact_status`, `pg_visible_in_snapshot`, `txid_status`, or any xid8/epoch reconstruction.
- Do not compare `system_identifier`.
- Do not add a circuit breaker or a second failure knob.
- Do not gate inside `sink()` (it owns keepalive replies) and do not gate at `COMMIT` (earlier rows are already emitted by the one-message look-ahead buffer).
- Do not poll `pg_stat_replication` for "LSN on all replicas": it does not order against `ProcArrayEndTransaction`.
- Do not call `txid_current()` / `pg_current_xact_id()` on the guard (they assign an xid).

## Repo state to clean up before starting

Untracked leftovers from an earlier exploration, all superseded by this document:

- `pq/replication/visibility.go` — prototype with the epoch bug; references `config.VisibilityGuardConfig`, which does not exist, so the package does not compile as-is.
- `pq/replication/visibility_test.go` — parser tests for the old predicate; the table-driven style is worth keeping, the expectations are not.
- `REVIEW.md` — review of that prototype.

Remove them (or rewrite in place) as the first step of the gate PR.

## Source references (PostgreSQL master, 2026-09-03)

`xact.c` RecordTransactionCommit: XactLogCommitRecord L1484, XLogFlush L1544, TransactionIdCommitTree L1550, SyncRepWaitForLSN L1599; CommitTransaction: ProcArrayEndTransaction L2430. `xlog.c` XLogFlush → WalSndWakeupProcessRequests L2943. `walsender.c` XLogSendLogical / GetFlushRecPtr; NeedToWaitForStandbys L1866 (checks `MyReplicationSlot->data.failover`). `slot.c` StandbySlotsHaveCaughtup. `decode.c` DecodeCommit → ReorderBufferCommit L755; DecodePrepare L359. `procarray.c` GetSnapshotData xmax = latestCompletedXid + 1 L2194; TransactionIdIsInProgress L1470. `syncrep.c` SyncRepWaitForLSN L149, fast exit L178-181, cancel/terminate escapes L301-332. `heapam_visibility.c` header comment (the race). `xid8funcs.c` pg_xact_status (in-progress before clog). `xlogfuncs.c` pg_walfile_name uses GetWALInsertionTimeLine, ERRORs in recovery. `system_functions.sql`: none of pg_control_*, pg_walfile_name, pg_current_wal_lsn, pg_current_snapshot, pg_is_in_recovery are REVOKEd. `worker.c` stream_abort_internal (sub-xid truncation). Patroni `docs/releases.rst`: 4.1.0 (2025-09-23) "Avoid interactions with slots created with the failover=true option"; 2.1.0 slot copy + pg_replication_slot_advance ("some events could be delivered more than once").
