# 🚀 Snapshot Mode (Initial) - Quick Start

This guide helps you test `initial` snapshot mode in 5 minutes.

## ⚡ 1 Minute Test

```bash
# 1. Start PostgreSQL (5 seconds)
make db-up

# 2. Setup test data (5 seconds)
make test-setup-crash

# 3. Run snapshot + CDC (runs indefinitely)
make run
```

**Done! ✅** Snapshot taken, now listening for changes in real-time.

Press `Ctrl+C` to stop.

## 🔍 What Happened?

### 1️⃣ PostgreSQL Started
```bash
make db-up
```
- Docker container started (port 5433)
- `testdb` database created
- PostgreSQL configured for logical replication

### 2️⃣ Test Data Created
```bash
make test-setup-crash
```
- `crash_test` table created
- 500 rows of test data inserted

### 3️⃣ Snapshot + CDC Started
```bash
make run
```
- **Phase 1**: Initial snapshot taken (500 rows captured)
- **Phase 2**: CDC streaming started (listening for changes)
- Process runs indefinitely until you stop it

## 📊 Output Example

```
🚀 Starting Snapshot Mode Example
📖 Mode: initial (snapshot first, then CDC)
================================================================================
📝 Configuration loaded from: ./config.yml
   Database: testuser@localhost:5433/testdb
   Tables: 1 (crash_test)
   Chunk Size: 8000

🚀 Starting snapshot process...
✅ CDC connector is ready!

📸 SNAPSHOT BEGIN | LSN: 0/15A5F10 | Time: 15:30:00
📸 SNAPSHOT DATA [1] | Table: public.crash_test | Data: {"id":1,"data":"abc123..."}
📸 SNAPSHOT DATA [2] | Table: public.crash_test | Data: {"id":2,"data":"def456..."}
...
📸 SNAPSHOT DATA [100] | Table: public.crash_test | Data: {"id":100,"data":"xyz789..."}
📸 SNAPSHOT DATA [200] | Table: public.crash_test | Data: {"id":200,"data":"mno345..."}
...
📸 SNAPSHOT DATA [500] | Table: public.crash_test | Data: {"id":500,"data":"pqr678..."}
📸 SNAPSHOT END | LSN: 0/15A5F10 | Total Rows: 500

📊 METRICS | Snapshot: BEGIN=1 DATA=500 END=1 | CDC: INSERT=0 UPDATE=0 DELETE=0

🔄 Snapshot completed! Now listening for real-time changes...

(waiting for database changes...)
```

## 🧪 Test Real-Time Changes

**While the application is running**, open another terminal and insert/update/delete data:

### Terminal 2 - Make Changes

```bash
# Open database shell
make db-shell
```

In psql, try these commands:

```sql
-- Insert new row
INSERT INTO crash_test (data) VALUES ('New data from terminal');

-- Update existing row
UPDATE crash_test SET data = 'Updated data' WHERE id = 1;

-- Delete row
DELETE FROM crash_test WHERE id = 2;
```

### Terminal 1 - See Changes in Real-Time

```
🔄 CDC INSERT | Table: public.crash_test | Data: {"id":501,"data":"New data from terminal"}
📊 METRICS | Snapshot: BEGIN=1 DATA=500 END=1 | CDC: INSERT=1 UPDATE=0 DELETE=0

🔄 CDC UPDATE | Table: public.crash_test | Data: {"id":1,"data":"Updated data"}
📊 METRICS | Snapshot: BEGIN=1 DATA=500 END=1 | CDC: INSERT=1 UPDATE=1 DELETE=0

🔄 CDC DELETE | Table: public.crash_test | Data: {"id":2,"data":"..."}
📊 METRICS | Snapshot: BEGIN=1 DATA=500 END=1 | CDC: INSERT=1 UPDATE=1 DELETE=1
```

**Amazing! 🎉** You're seeing changes in real-time!

## 🎯 Key Features

### ✅ Phase 1: Snapshot
- All existing data captured (500 rows)
- Chunked processing (memory efficient)
- Progress tracked (crash recoverable)

### ✅ Phase 2: CDC Streaming
- Real-time INSERT events captured
- Real-time UPDATE events captured
- Real-time DELETE events captured
- Runs indefinitely until stopped

## 📊 Metrics Every 5 Seconds

```
📊 METRICS | Snapshot: BEGIN=1 DATA=500 END=1 | CDC: INSERT=10 UPDATE=5 DELETE=2
```

- **Snapshot BEGIN**: Snapshot start events
- **Snapshot DATA**: Total rows from snapshot
- **Snapshot END**: Snapshot completion events
- **CDC INSERT/UPDATE/DELETE**: Real-time change counts

## 🧪 Advanced Tests

### Test 1: Small Dataset (50 rows)

```bash
make test-setup-1  # 50 rows in users table
# Update config.yml to use "users" table
make run
```

### Test 2: Medium Dataset (500 rows)

```bash
make test-setup-2  # 500 rows in orders table
# Update config.yml to use "orders" table
make run
```

### Test 3: Multiple Tables

```bash
make test-setup-3  # 150 rows in users + 80 rows in products
```

Update `config.yml`:

```yaml
publication:
  tables:
    - name: users
      schema: public
      replicaIdentity: FULL
    - name: products
      schema: public
      replicaIdentity: FULL
```

Then run:

```bash
make run
```

You'll see snapshot data from both tables, then CDC events from both!

### Test 4: Crash Recovery

```bash
# Terminal 1: Start
make run

# Wait a few seconds (let some snapshot chunks complete)
# Then press Ctrl+C to interrupt

# Restart - it will resume from the last completed chunk!
make run
```

You'll see:
```
📸 Resuming snapshot from chunk 3...
📸 SNAPSHOT DATA [200] | Table: public.crash_test | ...
```

### Test 5: Empty Table

```bash
make test-setup-empty
# Update config.yml to use "empty_table"
make run
```

Output:
```
📸 SNAPSHOT BEGIN
📸 SNAPSHOT END | Total Rows: 0
🔄 Now listening for real-time changes...
```

Then insert data and see it appear in real-time!

## 🛠️ Troubleshooting

### PostgreSQL not starting

```bash
# Check if container is running
docker ps

# If not, start it
make db-up

# Check PostgreSQL logs
docker-compose logs postgres
```

### "Slot already exists" error

```bash
# Clean CDC artifacts
make db-clean

# Then try again
make run
```

### Check snapshot progress

```bash
make status
```

Shows:
```
=== Job Status ===
slot_name         | completed | total_chunks | completed_chunks | snapshot_lsn | started_at
------------------+-----------+--------------+------------------+--------------+------------
cdc_slot_testm3   | true      | 1            | 1                | 0/15A5F10    | 2025-01-13...
```

### View metadata tables

```bash
# Job status
docker-compose exec postgres psql -U testuser -d testdb -c \
  "SELECT * FROM cdc_snapshot_job;"

# Chunk details
docker-compose exec postgres psql -U testuser -d testdb -c \
  "SELECT * FROM cdc_snapshot_chunks ORDER BY chunk_index;"
```

## 🎓 Understanding the Flow

```
┌─────────────────────────────────────────────────────────────┐
│ Initial Mode Flow                                            │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  1. Application starts                                       │
│     ↓                                                        │
│  2. Create replication slot                                  │
│     ↓                                                        │
│  3. Take snapshot of all tables                              │
│     ├── Chunk 1: rows 1-8000                                │
│     ├── Chunk 2: rows 8001-16000                            │
│     └── ...                                                  │
│     ↓                                                        │
│  4. Snapshot completed                                       │
│     ↓                                                        │
│  5. Switch to CDC mode                                       │
│     ↓                                                        │
│  6. Listen for changes (INSERT/UPDATE/DELETE)                │
│     ↓                                                        │
│  7. Process events in real-time                              │
│     ↓                                                        │
│  ∞. Runs indefinitely until stopped                          │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## 🆚 Mode Comparison

### Initial Mode (This Example)
```
[Snapshot] → [CDC Streaming] → [Runs Forever]
✅ Captures existing data
✅ Captures new changes
♾️  Runs indefinitely
```

### Snapshot-Only Mode
```
[Snapshot] → [Exit]
✅ Captures existing data
❌ No CDC streaming
✅ Exits automatically
```

## 📚 More Information

- **[README.md](./README.md)** - Detailed documentation
- **[config.yml](./config.yml)** - Configuration file
- **[main.go](./main.go)** - Source code
- **[Makefile](./Makefile)** - All available commands

## 🧹 Cleanup

```bash
# Stop the application
# Press Ctrl+C in the terminal running `make run`

# Clean CDC artifacts (slot, publication, metadata)
make db-clean

# Stop PostgreSQL
make db-down

# Remove binary
make clean
```

## 💡 Next Steps

### 1. Customize Handler Logic

Edit `main.go` and modify the handler function to do something useful:

```go
func handler(ctx *replication.ListenerContext) {
    switch v := ctx.Message.(type) {
    case *format.Insert:
        // Send to Kafka, write to file, etc.
        sendToKafka(v.TableName, v.Decoded)
    
    case *format.Update:
        // Update your cache, sync to another DB, etc.
        updateCache(v.TableName, v.NewDecoded)
    
    case *format.Delete:
        // Remove from search index, etc.
        removeFromIndex(v.TableName, v.OldDecoded)
    
    case *format.Snapshot:
        // Bulk load to data warehouse, etc.
        if v.EventType == format.SnapshotEventTypeData {
            bulkLoad(v.Table, v.Data)
        }
    }
    ctx.Ack()
}
```

### 2. Test with Your Data

```bash
# 1. Edit docker-compose.yml to mount your init SQL
# 2. Update config.yml with your tables
# 3. Adjust chunk size based on table sizes
# 4. Run and test!
```

### 3. Deploy to Production

- Add proper error handling and retries
- Configure logging appropriately
- Set up monitoring (metrics endpoint on port 8081)
- Add alerting for failures
- Scale with multiple instances if needed

## ❓ Common Questions

### Q: How do I monitor the application?

A: Metrics are exposed on port 8081:
```bash
curl http://localhost:8081/metrics
```

### Q: Can I run multiple instances?

A: Yes! Multiple instances will coordinate and split the work during snapshot phase. During CDC phase, only one will be active (the others will be standby).

### Q: What happens if the process crashes?

A: It will resume from the last completed chunk. No data is lost or duplicated.

### Q: How do I change the chunk size?

A: Edit `config.yml`:
```yaml
snapshot:
  chunkSize: 1000  # Smaller = less memory, more chunks
```

### Q: Can I skip the snapshot?

A: Yes, set `snapshot.enabled: false` in config.yml. Then it will only do CDC streaming.

## 🎉 You're Ready!

You now know how to:
- ✅ Take initial snapshot of existing data
- ✅ Stream real-time changes with CDC
- ✅ Monitor progress and metrics
- ✅ Handle crash recovery
- ✅ Test different scenarios

Start building your real-time data pipeline! 🚀

Good luck! 💪

