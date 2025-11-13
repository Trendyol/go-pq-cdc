# Snapshot Mode (Initial) Example

This example demonstrates how to use `initial` snapshot mode. In this mode:

- ✅ **Initial snapshot is taken** (captures existing data first)
- ✅ **CDC streaming starts** (continuous listening after snapshot)
- ✅ **Hybrid approach** (snapshot + real-time changes)
- ✅ **Replication slot created**
- ✅ **Publication created**

## 🎯 Use Cases

- **Complete data synchronization**: First snapshot existing data, then stream changes
- **Zero data loss**: Capture everything from snapshot point onwards
- **Initial load + sync**: Start with historical data, then keep in sync
- **Database replication**: Replicate entire database including existing and new data

## 🔄 How It Works

1. **Phase 1 - Snapshot**: 
   - Takes snapshot of all existing data in tables
   - Uses chunked processing for large tables
   - Tracks progress for crash recovery
   
2. **Phase 2 - CDC Streaming**:
   - Automatically switches to CDC mode after snapshot completes
   - Listens for INSERT, UPDATE, DELETE events
   - Runs indefinitely until stopped

## 🚀 Quick Start

### 1. Start PostgreSQL

```bash
make db-up
```

This command:
- Starts PostgreSQL container (port 5433)
- Creates a sample database (`testdb`)
- Configures PostgreSQL for logical replication

### 2. Setup Test Data

```bash
# Test 1: Basic test with crash_test table (500 rows)
make test-setup-crash
```

This creates a `crash_test` table with 500 rows.

### 3. Run CDC with Initial Snapshot

```bash
make run
```

This command:
- Compiles the example program
- Takes initial snapshot of all data
- Switches to CDC mode
- Listens for real-time changes (runs indefinitely)

### 4. Test Real-Time Changes (in another terminal)

```bash
# Insert new data
make db-shell
# Then in psql:
INSERT INTO crash_test (data) VALUES ('New data');
UPDATE crash_test SET data = 'Updated' WHERE id = 1;
DELETE FROM crash_test WHERE id = 2;
```

You'll see these changes appear in real-time in the CDC output!

## 📝 Configuration Explanation

```yaml
# config.yml

snapshot:
  enabled: true
  mode: initial         # ✨ Takes snapshot first, then starts CDC
  chunkSize: 8000       # Rows per chunk during snapshot
  claimTimeout: 30s     # Chunk claim timeout
  heartbeatInterval: 10s # Heartbeat interval during snapshot

publication:
  name: cdc_publication_testm3
  createIfNotExists: true
  operations:
    - INSERT
    - UPDATE
    - DELETE
  tables:
    - name: crash_test
      replicaIdentity: FULL
      schema: public

slot:
  name: cdc_slot_testm3
  createIfNotExists: true
  slotActivityCheckerInterval: 3000
```

### Important Points:

1. **Mode**: `initial` means snapshot first, then CDC
2. **Chunk Size**: Controls memory usage during snapshot (8000 rows per chunk)
3. **Replica Identity**: Set to `FULL` to capture all column values in updates
4. **Slot**: Required for CDC streaming
5. **Publication**: Defines which tables and operations to track

## 📊 Output Example

```
📝 Configuration loaded from: ./config.yml
   Database: testuser@localhost:5433/testdb
   Tables: 1 (crash_test)
   Chunk Size: 8000

🚀 Starting snapshot + CDC mode...
✅ CDC connector is ready!

📸 SNAPSHOT BEGIN | LSN: 0/1234567 | Time: 15:30:00
📸 SNAPSHOT DATA [1] | Table: public.crash_test | Data: {"id":1,"data":"..."}
📸 SNAPSHOT DATA [2] | Table: public.crash_test | Data: {"id":2,"data":"..."}
...
📸 SNAPSHOT DATA [100] | Table: public.crash_test | Data: {"id":100,"data":"..."}
...
📸 SNAPSHOT DATA [500] | Table: public.crash_test | Data: {"id":500,"data":"..."}
📸 SNAPSHOT END | LSN: 0/1234567 | Total Rows: 500

📊 METRICS | Snapshot: BEGIN=1 DATA=500 END=1 | CDC: INSERT=0 UPDATE=0 DELETE=0

🔄 Now listening for real-time changes...

🔄 CDC INSERT | Table: public.crash_test | Data: {"id":501,"data":"New data"}
📊 METRICS | Snapshot: BEGIN=1 DATA=500 END=1 | CDC: INSERT=1 UPDATE=0 DELETE=0

🔄 CDC UPDATE | Table: public.crash_test | Data: {"id":1,"data":"Updated"}
📊 METRICS | Snapshot: BEGIN=1 DATA=500 END=1 | CDC: INSERT=1 UPDATE=1 DELETE=0

🔄 CDC DELETE | Table: public.crash_test | Data: {"id":2,"data":"..."}
📊 METRICS | Snapshot: BEGIN=1 DATA=500 END=1 | CDC: INSERT=1 UPDATE=1 DELETE=1

... (continues indefinitely)
```

## 📊 Metrics

The application prints metrics every 5 seconds:

```
📊 METRICS | Snapshot: BEGIN=1 DATA=500 END=1 | CDC: INSERT=10 UPDATE=5 DELETE=2
```

- **Snapshot BEGIN**: Number of snapshot start events (should be 1)
- **Snapshot DATA**: Total rows captured in snapshot
- **Snapshot END**: Number of snapshot completion events (should be 1)
- **CDC INSERT**: Number of INSERT events captured
- **CDC UPDATE**: Number of UPDATE events captured
- **CDC DELETE**: Number of DELETE events captured

## 🧪 Test Scenarios

### Scenario 1: Single Table, Small Dataset

```bash
make test-setup-1  # 50 rows in users table
make run
```

### Scenario 2: Single Table, Larger Dataset

```bash
make test-setup-2  # 500 rows in orders table
make run
```

### Scenario 3: Multiple Tables

```bash
make test-setup-3  # 150 rows in users + 80 rows in products
make run
```

Update `config.yml` to include both tables:

```yaml
publication:
  tables:
    - name: users
      schema: public
    - name: products
      schema: public
```

### Scenario 4: Empty Table

```bash
make test-setup-empty  # Empty table
make run
```

### Scenario 5: Table Without Primary Key

```bash
make test-setup-no-pk  # Table without PK
make run
```

### Scenario 6: Crash Recovery

```bash
# Terminal 1: Start snapshot
make run

# Terminal 2: Kill process during snapshot (Ctrl+C)
# Snapshot progress is saved in metadata tables

# Terminal 1: Restart - will resume from last chunk!
make run
```

## 🔍 Monitoring

### Check Snapshot Job Status

```bash
make status
```

Shows:
- Job completion status
- Total chunks vs completed chunks
- Snapshot LSN
- Start time

### Verify Completion

```bash
make verify
```

Shows:
- Job completion details
- Chunks by table
- Rows processed per table

## 🛠️ Troubleshooting

### "Slot already exists" error

```bash
make db-clean  # Removes slot, publication, and metadata tables
```

### Check what's in the database

```bash
make db-shell
```

Then in psql:
```sql
-- View snapshot job
SELECT * FROM cdc_snapshot_job;

-- View snapshot chunks
SELECT * FROM cdc_snapshot_chunks ORDER BY table_name, chunk_index;

-- View table data
SELECT COUNT(*) FROM crash_test;
```

### Check replication slot

```bash
docker-compose exec postgres psql -U testuser -d testdb -c \
  "SELECT * FROM pg_replication_slots WHERE slot_name = 'cdc_slot_testm3';"
```

## 🏗️ Code Structure

### Handler Function

The handler processes both snapshot and CDC events:

```go
func handler(ctx *replication.ListenerContext) {
    switch v := ctx.Message.(type) {
    case *format.Insert:
        // Handle CDC INSERT
    case *format.Update:
        // Handle CDC UPDATE
    case *format.Delete:
        // Handle CDC DELETE
    case *format.Snapshot:
        // Handle snapshot events (BEGIN/DATA/END)
    }
    ctx.Ack()  // Acknowledge message
}
```

### Snapshot Event Types

```go
switch s.EventType {
case format.SnapshotEventTypeBegin:
    // Snapshot started
case format.SnapshotEventTypeData:
    // Snapshot data row
case format.SnapshotEventTypeEnd:
    // Snapshot completed
}
```

## 📚 More Information

- [go-pq-cdc Documentation](https://github.com/Trendyol/go-pq-cdc) - Main library
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html) - PostgreSQL docs

## 🧹 Cleanup

### Clean CDC artifacts only

```bash
make db-clean  # Removes slot, publication, metadata tables
```

### Stop PostgreSQL

```bash
make db-down
```

### Full cleanup

```bash
make clean      # Remove binary
make db-down    # Stop PostgreSQL
```

## 🆚 Comparison: Initial vs Snapshot-Only

| Feature | Initial Mode | Snapshot-Only Mode |
|---------|-------------|-------------------|
| Takes snapshot | ✅ Yes | ✅ Yes |
| CDC streaming | ✅ Yes | ❌ No |
| Process behavior | Infinite (until stopped) | Finite (auto-exits) |
| Replication slot | ✅ Required | ❌ Not needed |
| Use case | Full sync + real-time | One-time export |

## 💡 Tips

1. **Chunk Size**: Adjust based on your table size and memory
   - Small tables (< 10K rows): 10000
   - Medium tables (10K-100K rows): 5000
   - Large tables (> 100K rows): 1000-2000

2. **Replica Identity**: Use `FULL` to capture all columns in UPDATE events
   - `DEFAULT`: Only sends PK + changed columns
   - `FULL`: Sends all columns (before and after)

3. **Monitoring**: Watch metrics to understand throughput and performance

4. **Crash Recovery**: The system automatically resumes from the last completed chunk

5. **Multiple Instances**: You can run multiple instances in parallel - they'll coordinate using chunk claims

## 🚀 Next Steps

1. **Integrate with your application**: 
   - Replace the handler logic with your business logic
   - Send data to message queue, database, etc.

2. **Production deployment**:
   - Add proper error handling and retries
   - Set up monitoring and alerting
   - Configure log levels appropriately

3. **Scale**:
   - Run multiple instances for faster snapshot processing
   - Adjust chunk size based on performance testing

Good luck! 🚀

