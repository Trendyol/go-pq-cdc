# Online initial snapshot: opt-in POC

This example uses a **separate durable PostgreSQL target cluster**, not a print/enqueue callback. Nil from its handler means identity ledger + full-image versioned projection committed with synchronous_commit=on. DELETE leaves a versioned tombstone. Keep the ledger for the entire job; do not prune it on restart. This is a test-contract demonstration, not a production sink adapter or exactly-once transport.

Requirements: PostgreSQL **primary**, protocol **1**, fsync=on at source and target, immutable single integer-family primary keys, REPLICA IDENTITY FULL, explicit I/U/D/TRUNCATE publication, frozen schema. All published business tables are validated, even those not selected for READs. Partitions, inheritance, RLS, generated columns, publication filters/column lists, two-phase slots and protocol 2 are rejected. Runtime TRUNCATE, key changes and detected schema drift stop the run. Source payloads remain in bounded memory; slow sinks retain WAL.

From the repository root:

```sh
go run ./example/online-initial-poc -print-source-sql -role your_cdc_role > /tmp/online-source.sql
# Execute the SQL on the source as an authorized schema owner; it is also
# checked in as provisioning.sql without role-specific grants.
psql "$SOURCE_ADMIN_DSN" -f /tmp/online-source.sql
# Grant separately as an authorized owner (quote your actual identifiers):
# GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO your_cdc_role;
# GRANT USAGE ON SCHEMA public TO your_cdc_role;
# GRANT SELECT ON public.items TO your_cdc_role;
# Runtime role also needs LOGIN, REPLICATION, database CONNECT.
# Set items REPLICA IDENTITY FULL and pre-provision publication membership,
# or use publication.createIfNotExists with a publication-owning role.
export CDC_ONLINE_TARGET_DSN='postgres://.../separate_target'
go run ./example/online-initial-poc -setup-target
go run ./example/online-initial-poc -config /path/to/source-online.yaml
```

Use private config files (never commit credentials). Minimal source profile:

```yaml
host: localhost
port: 5432
username: your_cdc_role
password: supplied-secret
database: source_db
publication:
  name: items_online
  createIfNotExists: false
slot:
  name: items_online_slot
  createIfNotExists: true
  protoVersion: 1
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

Pre-provision an explicit publication for `public.items, cdc_online.watermarks` with `publish='insert,update,delete,truncate'`. Never publish jobs/table_progress/windows/schema_version or the metadata schema. `watermarks` is FULL in migration v1. When automatic publication management is disabled, missing membership returns the required ALTER SQL. Provisioning does not need to make the runtime role a superuser or metadata schema owner.

`NewOnlineConnector` validates static configuration without network side effects. `Run(ctx)` performs source/catalog/slot setup synchronously before capture readiness. `WaitUntilCaptureReady` and `WaitUntilSnapshotComplete` distinguish capture from snapshot delivery. `Done`/`Err` expose terminal failure. One instance is single-run; restart by constructing a new instance using the same source, slot and configuration. Owner lock loss cancels work. Handlers must honor cancellation; arbitrary callbacks cannot be forcibly terminated.

A new job requires a **fresh connector-created slot**. An existing unbound slot or a slot left after interrupted initialization requires explicit operator reconciliation; the example does not adopt/drop/recreate it. Resume requires the original source identity, slot binding, config/catalog fingerprint and a confirmed position no newer than durable metadata. Never point an existing job at another source or silently change strategies.

READ IDs are job/attempt/table/key and their order is `(HIGH end LSN,1,selected ordinal)`; `CommitLSN` is zero. CDC IDs are stable slot binding/end LSN/ordinal and use phase 0. A restart can repeat older deliveries; the target rejects stale effects and inconsistent ID reuse. Source ACK advances only after synchronous delivery and committed metadata, with unresolved windows held at LOW.

The CI profiles are `15-alpine`, `15.7-alpine`, `16-alpine`, `16.3-alpine`, and `17-alpine`. There is no PostgreSQL-major allowlist: unsupported features, incompatible slots, and violations of correctness guards fail admission. Untested releases are not automatically qualified. See `specs/verifications/e02-feature-admission-result.md` for the current policy and verification limits.

Limits are provisional experimental defaults, not production recommendations. Encoded-byte limits do not bound total heap/RSS. Feedback uses a 1s interval; provision/qualify wal_sender_timeout comfortably above it and the write deadlines. Review `specs/verifications/e01-POC-RESULT.md` for executed evidence and unmet qualification gates. Kafka/Elasticsearch adapters are not enabled.

## Image representation and observation

Online JSON/JSONB values (including arrays) retain `json.Number`, not rounded
`float64`. PostgreSQL float non-finites are the strings `Infinity`, `-Infinity`
and `NaN`; float arrays use the same representation. Qualified business columns
are bool, int2/int4/int8, text/varchar/bpchar, float4/float8, JSON/JSONB and their
one-dimensional, lower-bound-1 arrays. Empty/NULL arrays are supported; actual
shape/bounds are checked before READ/envelope delivery. Domains, custom,
composite, enum, range, numeric, temporal, bytea and business UUID columns are
rejected at startup. Unknown OIDs never silently fall back to wire strings.
UUID is supported only for exact internal watermark control fields. This
explicit POC profile is not full PostgreSQL type or production adapter coverage.

`NewOnlineConnector` additionally accepts optional `WithOnlineObserver` for
immutable metadata observations outside component locks. The observer must
honor its context. Errors stop Run but cannot roll back a checkpoint already
committed at `COMPLETION_COMMITTED`, or an ACK already advanced at `ACK_ADVANCED`.
Source confirmation is a separate server observation. `Stats()` returns actual
raw chunk/read latency and encoded transaction/queue/cache counters, not total
heap/RSS or unbounded delivery history. Default nil observer changes nothing.

Both source metadata and this target's ledger/projection must be permanent
relations. Rollback has a finite cleanup deadline; failed cleanup discards the
connection. Startup/acquisition deadlines are separate from successful reader
and owner lifetimes. The final root race/vet, full PG16 and explicit matched
capacity gates now pass for this restricted POC. Earlier timeouts and fixture
failures remain archived. Human UAT/production readiness and adapter enablement
remain unconfirmed; consult the current POC result for exact executed evidence.
