# go-pq-cdc & go-pq-cdc-kafka Version Upgrade Migration Guide

## Scope

| Library | From | To |
|---------|------|----|
| `go-pq-cdc` | v1.0.2 | v1.9.10 |
| `go-pq-cdc-kafka` | v1.0.1 | v1.2.14 |

> **go-pq-cdc-kafka** embeds `go-pq-cdc` config via `Connector.CDC` field. All go-pq-cdc config changes below apply to go-pq-cdc-kafka users as well.

---

## 1. BREAKING: HeartbeatConfig Restructured

**Affects:** All applications using heartbeat feature.

The `HeartbeatConfig` struct has been simplified. `Enabled` and `Query` fields were **removed** and replaced with a `Table` field.

### Before (v1.0.x)

```go
Heartbeat: config.HeartbeatConfig{
    Enabled:  true,
    Query:    "UPDATE cdc_heartbeat_table SET last_heartbeat = now()",
    Interval: 5 * time.Second,
}
```

### After (v1.9.10)

```go
Heartbeat: config.HeartbeatConfig{
    Table: publication.Table{
        Name: "cdc_heartbeat_table",
        // Schema defaults to "public" if omitted
    },
    Interval: 100 * time.Millisecond, // default changed from 5s to 100ms
}
```

### What changed:
- `Enabled` field removed — heartbeat is active when `Table.Name` is set, inactive when empty.
- `Query` field removed — the library auto-generates the UPDATE query internally.
- **Heartbeat table is now auto-created** by the library at startup. You can remove the `CREATE TABLE` statement for the heartbeat table from your `init.sql` or migration scripts.
- Default interval changed from **5 seconds** to **100 milliseconds**. Set your preferred value explicitly.

### Action required:
- [ ] Update `HeartbeatConfig` struct in your application config.
- [ ] Remove heartbeat table creation from init scripts (library handles it).
- [ ] Review and explicitly set the `Interval` value if 100ms default is not suitable.

---

## 2. IMPORTANT: PostgreSQL Version Compatibility Check

**Affects:** All applications.

A new `Slot.ProtoVersion` field was added with **default value 2**, which requires **PostgreSQL 14+**.

- If your PostgreSQL version is **14 or above**: No action needed.
- If your PostgreSQL version is **below 14**: You **must** add `ProtoVersion: 1` to your slot config, otherwise the application will fail at startup.

```go
Slot: slot.Config{
    Name:              "your_slot_name",
    CreateIfNotExists: true,
    ProtoVersion:      1, // Required for PostgreSQL < 14
}
```

### Action required:
- [ ] Verify your PostgreSQL server version.
- [ ] Add `ProtoVersion: 1` if running PostgreSQL < 14.

---

## 3. Behavioral Changes (No Config Change Required)

These are internal improvements. No config changes needed, but be aware of the behavioral differences:

| Change | Impact | Details |
|--------|--------|---------|
| WAL position handling improved | Positive | LSN confirmation is now more reliable. Fixes edge cases where WAL position could regress. |
| Streaming transaction fix | Positive | Large in-progress transactions are now handled correctly, preventing message loss in stream mode. |
| Truncate message decoding | New | `TRUNCATE` replication messages are now decoded and forwarded to your handler. If your handler does not expect truncate events, ensure it handles unknown message types gracefully. |
| Metadata table auto-migration | Positive | Internal metadata tables are migrated automatically between versions. No manual intervention needed. |
| Pending message ACK fix (kafka) | Positive | Fixes a bug where pending messages could be lost due to incorrect ACK ordering. |

---

## 4. New Optional Features Available After Upgrade

These are opt-in. Your existing config will continue to work without them.

| Feature | Config Field | Description |
|---------|-------------|-------------|
| Publication column filtering | `Publication.Tables[].Columns` | Publish only specific columns instead of full row. |
| Partitioned table support | `Publication.Tables[].Partitioned` | Enables `publish_via_partition_root` for partitioned tables. |
| Snapshot query condition | `Snapshot.QueryCondition` | Add a WHERE clause to snapshot queries. |
| Snapshot partition strategy | `Snapshot.Tables[].SnapshotPartitionStrategy` | Use `ctid`-based partitioning for snapshot parallelism. |
| Custom snapshot ID | `Snapshot.ID` | Set a custom identifier for snapshot tracking. |
| Re-snapshot | `Snapshot.Resnapshot` | Trigger a fresh snapshot on next startup. |
| TimescaleDB support | `ExtensionSupport.EnableTimeScaleDB` | Enable hypertable detection for TimescaleDB. |
| Slot info endpoint | Automatic | Slot info is now exposed via metrics endpoint. |

---

## 5. Default Value Changes

| Config Field | Old Default | New Default | Notes |
|-------------|-------------|-------------|-------|
| `Heartbeat.Interval` | 5s | 100ms | Only applies when heartbeat is enabled. Set explicitly if 100ms is too aggressive. |
| `Kafka.ProducerBatchBytes` | (unset) | `1mb` | go-pq-cdc-kafka only. Review if your messages are large. |
| `Slot.ProtoVersion` | (none) | 2 | Requires PG 14+. |

---

## Upgrade Checklist Per Application

For each of the 5 applications, verify:

- [ ] **Heartbeat config**: If using heartbeat, migrate to the new `Table`-based config structure.
- [ ] **PostgreSQL version**: Confirm PG 14+ or set `ProtoVersion: 1`.
- [ ] **Heartbeat init scripts**: Remove manual heartbeat table creation if present.
- [ ] **Heartbeat interval**: Decide on interval value (new default is 100ms vs old 5s).
- [ ] **Truncate handling**: Ensure your message handler won't break on truncate events.
- [ ] **Kafka batch bytes**: If using go-pq-cdc-kafka, verify `1mb` default is acceptable.
- [ ] **Test in staging**: Deploy to staging first and verify WAL replication, snapshot (if used), and message delivery.

---

## Version Compatibility Matrix

| go-pq-cdc-kafka | go-pq-cdc (embedded) | Min PostgreSQL |
|-----------------|--------------how-------|----------------|
| v1.0.1 | v1.0.2 | 10+ |
| v1.2.14 | v1.9.10 | 14+ (or 10+ with `ProtoVersion: 1`) |
