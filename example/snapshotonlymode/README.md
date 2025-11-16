# Snapshot-Only Mode Example

This example demonstrates how to use `snapshot_only` mode. In this mode:

- ✅ **Snapshot is taken** (captures existing data)
- ✅ **Process automatically exits** (finite mode)
- ❌ **NO CDC streaming** (no continuous listening)
- ❌ **No replication slot created**
- ❌ **No publication created** (optional)

## 🎯 Use Cases

- **One-time data export**: Export data once
- **Initial data migration**: Move existing data to another system
- **Database snapshot**: Take a snapshot of data at a specific point in time
- **Batch processing**: Process data periodically

## 🚀 Quick Start

### 1. Start PostgreSQL

```bash
make up
```

This command:
- Starts the PostgreSQL container (port 5433)
- Creates a sample database (`testdb`)
- Adds test data:
  - `users` table: 100 rows
  - `orders` table: 500 rows

### 2. Run Snapshot

```bash
make run
```

This command:
- Compiles the example program
- Starts in snapshot-only mode
- Takes snapshots of all tables
- Automatically exits when completed

### 3. Verify

```bash
make verify
```

Checks that the snapshot job has completed.

## 📝 Configuration Explanation

```yaml
# config.yml

snapshot:
  enabled: true
  mode: snapshot_only  # ✨ Main setting: snapshot only
  chunkSize: 1000      # How many rows per chunk
  
# Publication and slot NOT REQUIRED!
publication:
  tables:
    - name: users
      schema: public
    - name: orders
      schema: public
```

### Important Points:

1. **Mode**: Must be `snapshot_only`
2. **Tables**: Specify in `publication.tables`
3. **Slot**: Automatically created: `snapshot_only_<database>`
4. **Resume**: If it crashes, it resumes from where it left off

## 📊 Output Example

```
🚀 Starting Snapshot-Only Mode Example
📖 Mode: snapshot_only (finite, no CDC)
================================================================================
📝 Configuration loaded from: ./config.yml
   Database: testuser@localhost:5433/testdb
   Tables: 2
   Chunk Size: 1000

📸 Starting snapshot process...

📸 SNAPSHOT BEGIN
   LSN: 0/1234567
   Time: 2025-01-13 15:30:00
--------------------------------------------------------------------------------
📊 Row #1      | public.users            | {"id":1,"name":"User 1",...}
📊 Row #2      | public.users            | {"id":2,"name":"User 2",...}
...
📊 Row #100    | public.users            | {"id":100,"name":"User 100",...}
📊 Row #100    | public.orders           | {"id":100,"user_id":50,...}
📊 Row #200    | public.orders           | {"id":200,"user_id":25,...}
...

⏱️  PROGRESS | Elapsed: 10s | Rows: 300 | Throughput: 30 rows/sec

...
📊 Row #600    | public.orders           | {"id":600,"user_id":80,...}
--------------------------------------------------------------------------------
📸 SNAPSHOT END
   LSN: 0/1234567
   Total Rows: 600
   Duration: 15.2s

✅ Snapshot completed successfully!

================================================================================
📊 FINAL STATISTICS
================================================================================
Snapshot Events:
  • BEGIN:  1
  • DATA:   600 rows
  • END:    1

Performance:
  • Total Duration:    15.234s
  • Average Throughput: 39 rows/sec
  • Avg Time per Row:  25.39 ms
================================================================================

👋 Snapshot-only process finished. Exiting...
```

## 🧪 Test Scenarios

### Scenario 1: Basic Snapshot

```bash
make test
```

### Scenario 2: Resume After Crash

```bash
# Terminal 1: Start snapshot
make run

# Terminal 2: Cancel after a few seconds
# Ctrl+C in Terminal 1

# Terminal 1: Restart - will resume!
make run
```

### Scenario 3: Idempotent Run

```bash
# First run - completes
make run

# Second run - skips (already completed)
make run
# Output: "snapshot-only already completed, exiting"
```

## 🛠️ Troubleshooting

### To restart the snapshot:

```bash
make reset
```

### To view the metadata table:

```bash
docker-compose exec postgres psql -U testuser -d testdb -c \
  "SELECT * FROM cdc_snapshot_job WHERE slot_name LIKE 'snapshot_only_%';"
```

### To view chunk details:

```bash
docker-compose exec postgres psql -U testuser -d testdb -c \
  "SELECT table_name, status, COUNT(*) FROM cdc_snapshot_chunks \
   WHERE slot_name LIKE 'snapshot_only_%' GROUP BY table_name, status;"
```

## 📚 More Information

- [SNAPSHOT_FEATURE.md](../../docs/SNAPSHOT_FEATURE.md) - Detailed documentation
- [Integration Tests](../../integration_test/snapshot_only_test.go) - Test examples

## 🧹 Cleanup

```bash
make clean
```

This command:
- Stops the PostgreSQL container
- Deletes volumes
- Removes the binary

