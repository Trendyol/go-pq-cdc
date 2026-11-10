# Online initial snapshots

Online snapshots interleave bounded reads with CDC delivery. Enable them with
`snapshot.strategy: online` and `NewOnlineConnector`. The default `consistent`
strategy remains unchanged.

## How it works

For each keyset page, the connector commits a LOW watermark, selects rows in a
fresh transaction, then commits a HIGH watermark. It consumes CDC through HIGH,
suppresses READs for keys changed in that window, and delivers surviving READs
before later CDC. The cursor follows the last selected key, not the last
surviving READ.

Source progress is committed before ACK advancement. Unresolved windows hold
ACK at LOW. There is no single cross-table snapshot time or exactly-once
external-effect guarantee; the sink contract below is required.

## Supported profile

Build and run with Go 1.26.10 or a later patched, supported Go release.
The module declares this minimum, and CI reads the version from `go.mod`.
Do not qualify a build with Go 1.25.0 or vulnerable dependency versions.

The source is one PostgreSQL 16 primary with direct SQL and replication connections.
The source identity, timeline, database, publication and slot remain fixed for a job.
Application DDL and primary keys remain unchanged during capture and recovery.

Business tables require one immutable `int2`, `int4`, or `int8` primary key and `REPLICA IDENTITY FULL`.
The publication includes all four operations: `INSERT`, `UPDATE`, `DELETE`, and `TRUNCATE`.
All published business tables receive validation, including tables outside the snapshot selection.

Supported columns are boolean, integer-family, text-family, `float4`, `float8`, `JSON`, `JSONB`, and their qualified arrays.
Arrays have one dimension and lower bound 1. Empty arrays and NULL values are supported.
Online JSON numbers retain `json.Number`. Float non-finites use `Infinity`, `-Infinity`, and `NaN` strings.

Unsupported features fail closed:

- Standbys, failover, source replacement, connection poolers and protocol 2.
- Composite or mutable primary keys, partitions, inheritance, RLS and generated columns.
- Publication filters, column lists, `FOR ALL TABLES`, and two-phase slots.
- Domains, custom types, enums, composites, ranges, numeric, temporal, `bytea`, and business UUID columns.

Runtime `TRUNCATE`, key changes, detected schema drift and binding changes stop capture.
Internal watermark UUIDs do not imply support for business UUID columns.

## Delivery and ACK contract

`NewOnlineConnector` requires a synchronous handler. Nil means the sink committed the effect durably.
Nil must not mean that an asynchronous queue accepted the event.
Handlers must honor their context. Go cannot terminate an arbitrary callback or undo an external commit.

Delivery is at least once. A restart can repeat effects.
CDC identities remain stable across recovery. Recovered READ identities can differ by attempt.
An event order is `(EndLSN, phase, ordinal)`. READs use phase 1 after their HIGH watermark.

The sink must reject inconsistent identity reuse and stale per-key orders.
DELETE effects require retained versioned tombstones. Old READs and CDC events must not resurrect deleted rows.
Keep identity records and tombstones for the possible replay horizon.
Without a proven finite replay horizon, do not prune them during an active job or recovery.

Successful handler completion precedes committed source metadata and ACK advancement.
An unresolved window holds safe ACK at LOW. Feedback does not prove that PostgreSQL received an ACK.
Final projection equality alone does not prove that every intermediate CDC operation arrived.

## Source setup procedure

1. Use an authorized schema owner for the source migration.
2. Provision the permanent `cdc_online` metadata relations with the checked-in [SQL](../example/online-initial-poc/provisioning.sql).
3. Give the runtime role `LOGIN`, `REPLICATION`, database `CONNECT`, and `SELECT` on published business tables.
4. Grant schema access and the specific metadata permissions in the migration.
5. Grant execution of `pg_catalog.pg_control_system()` to the runtime role.
6. Set each business table to `REPLICA IDENTITY FULL`.
7. Create an explicit publication for business tables and `cdc_online.watermarks`.
8. Exclude `jobs`, `table_progress`, `windows`, and `schema_version` from the publication.
9. Keep `fsync`, `synchronous_commit`, and `full_page_writes` enabled on every durability boundary.
10. Supply credentials through private configuration or the host application's secret system.

The runtime role does not need to own the metadata schema or be a superuser.
Automatic publication creation requires the appropriate publication and table ownership permissions.
Metadata relations must be permanent. Source row payloads are not stored in a metadata outbox.

## Configuration example

The following limits are safety defaults, not measured throughput recommendations.
Explicit `slot.protoVersion: 1` is required. Protocol 2 is not silently downgraded.

```yaml
host: source-primary
port: 5432
username: cdc_role
password: supplied-secret
# Use a private file. Do not commit real credentials.
database: source_db
publication:
  name: items_online
  createIfNotExists: false
slot:
  name: items_online_slot
  createIfNotExists: true
  protoVersion: 1
  infoTimeout: 5s
snapshot:
  enabled: true
  mode: initial
  strategy: online
  chunkSize: 1000
  online:
    queryTimeout: 5s
    deliveryTimeout: 30s
    chunkBytes: 4194304
    transactionBytes: 16777216
    queueBytes: 33554432
    checkpointInterval: 1s
    markerInterval: 5s
```

Use `NewOnlineConnector`, not the legacy `NewConnector`, for this strategy.
The default strategy remains `consistent` for compatibility.
A row-count limit and an encoded-byte limit both apply to a chunk.
A larger chunk can exceed `chunkBytes` even with a supported schema.
Encoded-byte limits do not bound total process heap or RSS.
`slot.infoTimeout` bounds legacy slot-status queries, including admission and their single reconnect.
Online source queries use `snapshot.online.queryTimeout` instead.

## Startup and readiness

The constructor validates static configuration without a network connection.
`Run(ctx)` performs source, catalog, ownership and slot checks before capture readiness.
Each instance accepts one call to `Run`. A restart requires a new instance.

`WaitUntilCaptureReady` means that capture started. It does not mean the sink caught up.
`WaitUntilSnapshotComplete` means the initial snapshot completion notification arrived.
It does not certify a deployment, an arbitrary handler, or current sink freshness.
`Done` and `Err` expose the terminal lifecycle.

`Health()` exposes `Running`, `CaptureReady`, and `SnapshotComplete` without a network query.
A host readiness endpoint must require `Running` and the readiness flag appropriate to its service.
A stopped instance retains its notification flags. Notification flags alone are not health checks.
The online connector does not start an HTTP listener automatically.

The legacy `GET /status` endpoint returns a fixed `OK` body.
It proves HTTP reachability, not PostgreSQL connectivity, CDC readiness, snapshot completion or sink freshness.
`GET /slot` performs a bounded slot-status query. It is not a substitute for application readiness.

## Monitoring procedure

1. Register `connector.MetricsCollector()` once in the host application's Prometheus registry.
2. Expose that registry through the host application's protected metrics endpoint.
3. Scrape process heap, RSS, CPU and file descriptors separately.
4. Observe source slot retention, physical WAL disk use and filesystem free space separately.
5. Alert on increasing sink lag and sustained encoded backlog.
6. Alert before WAL retention or disk use reaches the deployment's capacity limit.
7. Alert on terminal connector errors and repeated restart failures.
8. Verify alert thresholds under a slow sink and a lost source connection.

The collector exposes `go_pq_cdc_online_*` metrics with the configured `slot_name` label:

| Suffix | Meaning |
|---|---|
| `running` | Current instance lifecycle is active |
| `capture_ready` | Capture notification occurred in this instance |
| `snapshot_complete` | Snapshot notification occurred in this instance |
| `read_queries_total` | Snapshot read queries in this instance |
| `peak_chunk_raw_bytes` | Peak retained raw chunk bytes |
| `peak_read_latency_seconds` | Peak snapshot query duration |
| `peak_transaction_encoded_bytes` | Peak encoded transaction bytes |
| `peak_queue_and_assembly_encoded_bytes` | Peak encoded queue and assembly bytes |
| `peak_relation_encoded_bytes` | Peak encoded relation-cache bytes |
| `buffered_encoded_bytes` | Current queued and visibility-gating encoded envelope bytes |
| `feedback_writes_total` | Feedback writes reported by the reader |
| `last_feedback_timestamp_seconds` | Last recorded feedback time, or zero before feedback |

Counters reset with each connector instance. Peaks are not current occupancy or process memory.
`buffered_encoded_bytes` excludes active transaction assembly, the relation cache, snapshot chunks and caller-retained envelopes.
It also excludes envelopes delivered to a slow handler. Zero does not prove that no work remains retained.
Scrape process memory and monitor sink lag separately.
`Stats().SafeACK` is an exact LSN value. It is not exported as a floating-point position.
`WithOnlineObserver` exposes immutable metadata outside component locks.
Observer errors stop the run but do not reverse committed checkpoints or ACK advancement.

Feedback uses a one-second interval. Slow handlers and checkpoint failures can retain WAL.
Provision `wal_sender_timeout` for the feedback interval and the observed query and write deadlines.
Do not equate a source write rate with durable sink throughput.
Backpressure bounds retained work. It does not increase sink capacity or remove source disk requirements.

## Recovery procedure

1. Preserve the original slot, source metadata, identity records and tombstones.
2. Inspect the terminal error before a restart.
3. Confirm the source identity, database, timeline, publication and frozen schema.
4. Confirm that the original slot still exists and its WAL remains available.
5. Use the same binding and configuration fingerprint with a new connector instance.
6. Require the sink's idempotency and ordering checks during replay.
7. Compare source and sink projections after catch-up.
8. Check intermediate event coverage separately from final projection equality.

A new job requires a fresh connector-created slot.
An unbound existing slot is not adopted, dropped or recreated automatically.
Interrupted initial setup can require operator reconciliation.
Do not advance a slot to clear an error. That action can discard unread CDC.
Do not delete a job or replace its slot without an approved rebuild plan.

## Shutdown procedure

1. Cancel the context passed to `Run`.
2. Keep the handler available until its current call returns.
3. Wait for `Done` with a deployment-specific deadline.
4. Inspect `Err` and the durable sink outcome.
5. Retain source metadata and the original slot for recovery.

Cancellation does not undo an external sink effect.
The handler must return an error for unknown or non-durable completion.
The legacy metrics server permits five seconds for graceful shutdown, then closes active connections.
Server shutdown cannot terminate an arbitrary non-cooperative HTTP handler.

## Credential rotation and deployment checks

1. Rotate credentials through the deployment's secret system.
2. Verify the new role permissions before replacing the connector instance.
3. Preserve source identity and the original slot during rotation.
4. Perform a rolling restart only with the documented source-owner exclusion.
5. Verify recovery and idempotency after the restart.

Credentials are not part of the configuration fingerprint.
The runtime must not use a pooler or source-switching endpoint for this profile.
Code tests do not prove power-loss durability, backup restoration, deployment alerts or an arbitrary production adapter.
The [separate-target example](../example/online-initial-poc/README.md) demonstrates a durable sink contract, not a universal adapter.

## Validation

Run unit tests, race detection and vet from the repository root:

```sh
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```

Run the complete integration suite against PostgreSQL 16 with a working
Docker-compatible testcontainers endpoint:

```sh
cd integration_test
POSTGRES_VERSION=16-alpine go test -race -p=1 -count=1 -timeout=30m ./...
```

Use `make init/lint` to install the pinned linter, then run
`golangci-lint run -c .golangci.yml` from the root and
`golangci-lint run -c ../.golangci.yml` from `integration_test`.
Run `govulncheck ./...` from the root and `govulncheck -test ./...` from
`integration_test` to check the actual toolchain and dependency graph.

These checks do not replace deployment verification of sink durability,
permissions, backups, monitoring or source/slot recovery.
