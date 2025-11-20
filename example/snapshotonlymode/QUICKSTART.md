# 🚀 Snapshot-Only Mode - Quick Start

This guide helps you test `snapshot_only` mode in 5 minutes.

## ⚡ 1 Minute Test

```bash
# 1. Start PostgreSQL (5 seconds)
make up

# 2. Run snapshot (30 seconds)
make run

# 3. Verify
make verify
```

**Done! ✅** Snapshot taken and process exited.

## 🔍 What Happened?

### 1️⃣ PostgreSQL Started
```bash
make up
```
- Docker container started (port 5433)
- `testdb` database created
- 2 tables + 600 rows of test data added:
  - `users`: 100 rows
  - `orders`: 500 rows

### 2️⃣ Snapshot Taken
```bash
make run
```
- Connector started in `snapshot_only` mode
- All tables snapshotted (600 rows)
- Process automatically exited
- **Total time: ~15 seconds**

### 3️⃣ Verified
```bash
make verify
```
- Job metadata checked
- All chunks completed
- `completed = true` ✅

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
   LSN: 0/15A5F10
   Time: 2025-11-13 15:30:00
--------------------------------------------------------------------------------
📊 Row #1      | public.users            | {"id":1,"name":"User 1","email":"user1@example.com","age":21}
📊 Row #2      | public.users            | {"id":2,"name":"User 2","email":"user2@example.com","age":22}
...
📊 Row #100    | public.users            | {"id":100,"name":"User 100","email":"user100@example.com","age":70}
📊 Row #100    | public.orders           | {"id":100,"user_id":50,"product":"Product 10","quantity":1,"price":19.00}
...
📊 Row #600    | public.orders           | {"id":500,"user_id":25,"product":"Product 15","quantity":5,"price":95.00}
--------------------------------------------------------------------------------
📸 SNAPSHOT END
   LSN: 0/15A5F10
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

## 🎯 Key Features

### ✅ What Was Done
- Snapshot of existing data taken
- Memory efficient execution with chunk-based processing
- Progress tracked with metadata tracking
- Process successfully exited

### ❌ What Was NOT Done
- CDC streaming did not start (no infinite listening)
- Replication slot not created
- Publication not created (optional)
- Background listener did not start

## 🧪 Advanced Tests

### Resume Test (Crash Recovery)
```bash
# 1. Start snapshot
make run &
JOB_PID=$!

# 2. Cancel after a few seconds
sleep 5
kill $JOB_PID

# 3. Restart - will resume from where it left off!
make run
```

### Multi-Pod Test (Parallel Processing)
```bash
# Run 3 pods in parallel
make multipod

# Verify results
make verify-multipod
```

### Idempotent Test (Duplicate Prevention)
```bash
# First run - takes snapshot
make run

# Second run - skips (already completed)
make run
# Output: "snapshot-only already completed, exiting"
```

## 🛠️ Troubleshooting

### PostgreSQL didn't start
```bash
# Check container
docker ps

# If not running
make up

# Check logs
make logs
```

### I want to rerun the snapshot
```bash
# Reset database (metadata + data)
make reset

# Then run again
make run
```

### I want to view metadata tables
```bash
# Job table
docker-compose exec postgres psql -U testuser -d testdb -c \
  "SELECT * FROM cdc_snapshot_job WHERE slot_name LIKE 'snapshot_only_%';"

# Chunks table
docker-compose exec postgres psql -U testuser -d testdb -c \
  "SELECT table_name, status, COUNT(*) FROM cdc_snapshot_chunks \
   WHERE slot_name LIKE 'snapshot_only_%' GROUP BY table_name, status;"
```

## 📚 More Information

- **[README.md](./README.md)** - Detailed documentation
- **[config.yml](./config.yml)** - Config example
- **[main.go](./main.go)** - Code example
- **[SNAPSHOT_FEATURE.md](../../docs/SNAPSHOT_FEATURE.md)** - Feature documentation

## 🧹 Cleanup

```bash
# Clean everything (container + volume + binary)
make clean
```

## 💡 Next Steps

1. **Test with your own data:**
   - Edit the `init.sql` file
   - Add your own tables
   - Specify tables in `config.yml`

2. **Adapt for production:**
   - Update `config.yml` with production database
   - Adjust `chunkSize` based on table sizes
   - Add monitoring/alerting

3. **Scale with multi-pod:**
   - Create deployment in Kubernetes
   - Reduce snapshot time with parallel processing
   - Optimize replica count

Good luck! 🚀

