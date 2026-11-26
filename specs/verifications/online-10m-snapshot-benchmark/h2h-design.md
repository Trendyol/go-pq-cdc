# Head-to-head benchmark design: how both connectors stop on the same CDC message

Test file: `integration_test/online_snapshot_h2h_benchmark_test.go`
Test name: `TestOnlineInitialSnapshot10MHeadToHead`
Opt-in: `CDC_ONLINE_10M_BENCH=1` (row-count override: `CDC_H2H_ROWS`)

## Problem being solved

All earlier comparisons were unfair at the finish line:

- Legacy `consistent` stopped when its snapshot ended. It had not consumed the concurrent CDC stream at all.
- `online` was still consuming snapshot reads AND CDC at the same time when the measurement stopped.

So the arms did not do the same work, and there was no shared end condition. The fix: give the shared transaction stream a defined last message (a **sentinel**) and stop each arm only after it has finished its snapshot and delivered that exact message.

## Design

### 1. One shared stream, two arms

Both connectors run concurrently against the same PostgreSQL container, each with its own slot and publication:

| Arm | Slot | Publication |
| --- | --- | --- |
| legacy consistent | `bench_legacy_slot` | `bench_legacy_pub` |
| online | `bench_online_slot` | `bench_online_pub` |

The same WAL feeds both slots, so the transaction sequence each connector decodes is identical. Per-slot metrics stay independent.

### 2. Common start line

Writers do not start until both arms are actually capturing:

```go
waitOnlineCapture(t, onlineRun)   // online slot created, streaming
<-legacyReady                     // legacy exported snapshot, delivering
writersStart = time.Now()
```

Both slots exist before the first update, so neither arm can miss any transaction in the shared stream.

### 3. The stream has a defined last message

The workload is bounded: 120 steady updates/s for 60s, then a single sentinel transaction ends the stream:

```sql
-- writer, after the steady phase:
SELECT pg_current_wal_flush_lsn();          -- preLSN
UPDATE workload SET value = 9000000007 WHERE id = <rows>;   -- sentinel
SELECT pg_current_wal_flush_lsn();          -- postLSN
```

- The sentinel value is a magic constant that can never collide with steady updates (`value` = write counter).
- The writer records the WAL flush LSN before and after the commit. This `[preLSN, postLSN]` window is the ground truth for the sentinel transaction.

### 4. Identical stop predicate per arm

An arm stops only when:

```go
snapshotDone && sentinels > 0
```

- legacy: `SnapshotEventTypeEnd` seen **and** a `format.Update` with `id == rows, value == 9000000007` delivered.
- online: `cdc_online.jobs.status = 'snapshot_complete'` **and** a CDC `online.Event` with the same key/value delivered.

This removes the old unfairness: legacy cannot finish at snapshot end anymore, and online is not measured while still mid-catch-up. Both arms must reach **snapshot complete + full stream drained to the same final message**.

Logical argument: per-relation delivery is ordered, and the sentinel is the last committed transaction. Therefore "delivered the sentinel" implies "delivered everything before it", so both arms' stop points are the same logical CDC position.

### 5. Physical proof it was the same message

Each arm records the commit LSN observed on the sentinel event:

```go
// legacy handler
legacyState.sentinelCommitLSN.Store(uint64(l.CommitLSN))
// online handler
onlineState.sentinelCommitLSN.Store(uint64(event.CommitLSN))
```

Final assertions:

```go
require.Equal(int64(1), legacyState.sentinels.Load())  // exactly once
require.Equal(int64(1), onlineState.sentinels.Load())  // exactly once
require.NotZero(legacyState.sentinelCommitLSN.Load())
require.NotZero(onlineState.sentinelCommitLSN.Load())
require.GreaterOrEqual(legacySentinelLSN, writerPreLSN)
require.LessOrEqual(legacySentinelLSN, writerPostLSN)
require.GreaterOrEqual(onlineSentinelLSN, writerPreLSN)
require.LessOrEqual(onlineSentinelLSN, writerPostLSN)
```

Plus accounting closure: each arm's CDC deliveries must equal `writes + 1` (all steady updates + sentinel), exactly:

```go
require.Equal(writes.count+1, legacyState.cdcDelivered.Load())
require.Equal(writes.count+1, onlineState.cdcDelivered.Load())
```

### 6. What is measured

Headline per arm, from the common `writersStart` to the arm's own stop instant:

```text
stopAtSentinelSeconds = arm stop time - writersStart
```

Meaning: time to finish the snapshot **and** drain the identical stream up to the identical terminating transaction.

Also per arm, sampled per slot name so arms stay independent:

- `peakSlotRetainedWAL` (`pg_current_wal_lsn() - restart_lsn`)
- `peakConfirmedLagBytes` (`pg_current_wal_lsn() - confirmed_flush_lsn`)
- `peakFeedbackAgeSeconds` (age of last `reply_time` on the walsender)

Plus per arm: snapshot rows, snapshot key checksum, snapshot completion time, CDC delivery count; process-wide heap/RSS peaks; source read stats from `pg_stat_statements`; online `Connector.Stats()`.

## Caveats

- Legacy finishes far earlier, so it stops first and the online arm then runs without a competing consumer. Per-slot metrics are independent, but host-level CPU/IO contention is not perfectly symmetric after the first arm stops.
- No durable target sink; handlers count and checksum only.
- Local single-run evidence, not a production SLO.

## Status

- Written, `gofmt` and `go vet` clean; skips unless `CDC_ONLINE_10M_BENCH=1`.
- First smoke run failed on a missing `bench_online_pub` (the online connector requires the publication to exist before `EnsureWatermarkPublication`); fixed by creating it in setup.
- The sentinel assertions still need one successful smoke run (`CDC_H2H_ROWS=200000`) and one full 10M run before the numbers are trusted.

## Commands

Smoke:

```sh
CDC_ONLINE_10M_BENCH=1 CDC_H2H_ROWS=200000 POSTGRES_VERSION=16-alpine \
  go test ./integration_test -run '^TestOnlineInitialSnapshot10MHeadToHead$' \
  -count=1 -timeout=10m -v
```

Full 10M:

```sh
CDC_ONLINE_10M_BENCH=1 POSTGRES_VERSION=16-alpine \
  go test ./integration_test -run '^TestOnlineInitialSnapshot10MHeadToHead$' \
  -count=1 -timeout=30m -v \
  2>&1 | tee specs/verifications/online-10m-snapshot-benchmark/evidence/run-h2h.log
```

Evidence output: `specs/verifications/online-10m-snapshot-benchmark/evidence/h2h.json`
