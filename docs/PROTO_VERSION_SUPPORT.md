# Protocol Version Support and Streaming Transactions

## Overview

This document explains the changes introduced on `feature/protoversion1-support` compared with `main`.

Main improvements:

- Added configurable `slot.protoVersion` (`1` or `2`)
- Defaulted protocol version to `2` when not set
- Updated replication startup arguments according to protocol version
- Added full decoding for `STREAM START` and `STREAM ABORT` payloads
- Fixed streamed transaction buffering so rolled-back streamed data is never emitted
- Expanded integration tests to validate both protocol versions and streaming rollback scenarios

## What Changed vs `main`

### 1) Configurable protocol version

`slot.Config` now includes:

- `protoVersion: 1` for compatibility mode
- `protoVersion: 2` for streaming-aware mode (default)

Validation now rejects unsupported values.

### 2) Replication start behavior is version-aware

Replication startup now uses `slot.protoVersion` and the server version:

- Always sends `proto_version '<N>'`
- Sends `streaming 'true'` only when `protoVersion >= 2`
- Sends `messages 'true'` on PostgreSQL 14 and newer, on either protocol version, unless `slot.logicalMessages` is `off`. Older servers reject that option, so it is omitted and startup still proceeds

This keeps startup compatible with older PostgreSQL versions while preserving advanced behavior on newer versions.

### 3) Stream message decoding is complete

`STREAM START` and `STREAM ABORT` messages are now parsed into typed structures:

- `StreamStart { Xid, FirstSegment }`
- `StreamAbort { Xid, SubXid }`

This allows transaction-level stream control using real XIDs.

### 4) Streamed transaction handling is now commit-safe

For `protoVersion: 2`, streamed transactions can be split and interleaved by PostgreSQL.
The stream sink now buffers messages per transaction XID and applies this policy:

- On `STREAM START`: begin/continue buffering for XID
- On `STREAM STOP`: keep buffered data, emit nothing
- On `STREAM COMMIT`: emit buffered data for XID, set last event WAL to commit end LSN
- On `STREAM ABORT`: discard buffered data for XID

Result: rolled-back streamed transactions are never delivered to handlers.

## Choosing `protoVersion`

### Use `protoVersion: 1` when:

- PostgreSQL version is older than 14
- You need maximum compatibility with minimal protocol features
- You do not require streamed in-progress transaction handling

### Use `protoVersion: 2` when:

- PostgreSQL version is 14+
- You want support for large/streamed in-progress transactions
- You want strict rollback safety for streamed transactions

## Configuration Examples

### YAML

```yaml
slot:
  name: cdc_slot
  createIfNotExists: true
  slotActivityCheckerInterval: 3000
  protoVersion: 2
```

### Go

```go
Slot: slot.Config{
    Name:                        "cdc_slot",
    CreateIfNotExists:           true,
    SlotActivityCheckerInterval: 3000,
    ProtoVersion:                2,
},
```

## Compatibility Matrix

| slot.protoVersion | Minimum PostgreSQL | Streaming protocol messages | Logical decoding messages             |
|-------------------|--------------------|-----------------------------|---------------------------------------|
| 1                 | 10                 | No                          | Requested on PostgreSQL 14+ (default) |
| 2                 | 14                 | Yes                         | Requested on PostgreSQL 14+ (default) |

`slot.logicalMessages: off` stops requesting logical decoding messages on either protocol version; `on` requires them, so PostgreSQL 10–13 fails startup instead of omitting the option silently. The default `auto` requests them exactly where the table says.

## Test Coverage Added

The branch adds/updates tests to protect behavior:

- Unit tests for stream payload parsing:
  - `NewStreamStart`
  - `NewStreamAbort`
- Integration tests for streaming behavior:
  - Large streamed commit delivery
  - Interleaved streamed transactions
  - Streamed rollback emits no events
  - Rollback followed by commit emits only committed events
- Integration test helpers now run protocol-version subtests (`v1` and `v2`) where applicable

## Migration Notes

If you do not set `slot.protoVersion`, default is `2`.

Recommended migration path:

1. Keep `protoVersion: 2` on PostgreSQL 14+.
2. Set `protoVersion: 1` explicitly if your PostgreSQL is older.
3. Run your CDC integration tests with both versions if you support mixed environments.

## Memory Bounds & Disk Spilling

When `protoVersion: 2` is active, PostgreSQL streams in-progress transactions exceeding `logical_decoding_work_mem` (default 64 MiB) in interleaved `STREAM START` / `STREAM STOP` chunks before `STREAM COMMIT` or `STREAM ABORT`.

### Lazy Raw-Frame Buffering (Always On)

Streamed chunks are buffered as raw `pgoutput` WAL bytes in a flat, pointer-free byte slice per transaction and decoded lazily only on `STREAM COMMIT`. This eliminates Go heap amplification and GC pointer scanning while transactions are in flight, and avoids tuple-decoding CPU entirely when a streamed transaction aborts (`STREAM ABORT`).

### Opt-In Disk Spilling (`slot.spill.*`)

To enforce a strict memory ceiling across all concurrent streamed transactions, enable disk spilling under `slot.spill`:

```yaml
slot:
  name: cdc_slot
  protoVersion: 2
  spill:
    enabled: true
    maxMemoryBytes: 67108864 # 64 MiB global in-memory budget across open streamed TXs
    spillDir: ""             # empty defaults to os.TempDir()
    maxDiskBytes: 0          # 0 = unlimited; when > 0, must be >= maxMemoryBytes
```

Key operational properties:

- **Largest-TX-first eviction:** Whenever appending a streamed frame pushes total in-memory buffer bytes above `maxMemoryBytes`, the largest in-memory streamed transaction is written to a private temporary file (`0600`) inside a per-session directory (`0700`) and freed from memory.
- **Single-FD bound:** At most one spill file descriptor is kept open at any instant (writers flush and close on every `STREAM STOP` and lazily reopen on the next `STREAM START`).
- **Fail-closed safety:** Any spill I/O error or `maxDiskBytes` breach (`ErrStreamSpillDiskLimitExceeded`) aborts and restarts the stream without advancing `confirmed_flush_lsn`, preventing data loss.
- **Zero FS touch when disabled:** With `slot.spill.enabled: false` (default), no files or directories are created, preserving compatibility with `readOnlyRootFilesystem: true` containers.
- **Keep `statusUpdateInterval` enabled:** Disabling `statusUpdateInterval` (`0`) is discouraged when replaying multi-gigabyte spilled transactions, because consumer backpressure on `messageCH` delays the sink from reading keepalive requests and can trigger `wal_sender_timeout` disconnects.
- **Ephemeral volume sizing:** In Kubernetes, mount an `emptyDir` volume for `slot.spill.spillDir` (with `sizeLimit` matching or exceeding `slot.spill.maxDiskBytes`) rather than spilling onto the root filesystem.


