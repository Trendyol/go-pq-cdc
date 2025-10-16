# Snapshot Example

This example demonstrates the **Initial Snapshot** feature of go-pq-cdc, which captures existing data from PostgreSQL tables before starting CDC streaming.

## Features Demonstrated

- 📸 **Initial Snapshot**: Read 50,000 existing rows before CDC
- 📦 **Chunked Reading**: Process data in configurable batch sizes (5,000 rows)
- 💾 **Checkpoint Management**: Save progress every 5 batches for recovery
- 📊 **Progress Tracking**: Real-time metrics via Prometheus
- 🔄 **Retry Logic**: Automatic retry on failures with exponential backoff
- 🎯 **Type Conversion**: Proper PostgreSQL type mapping (int, decimal, timestamp)

## Prerequisites

- Docker & Docker Compose
- Go 1.21+
- Port 5432 (PostgreSQL) and 8082 (Metrics) available

## Quick Start

### 1. Start PostgreSQL with Initial Data

```bash
cd example/snapshot
docker-compose up -d
```

This will:
- Start PostgreSQL 16 with logical replication enabled
- Create `products` table
- Insert 50,000 initial rows across 4 categories

### 2. Run the Example

```bash
# Install dependencies
go mod download

# Run the snapshot example
go run main.go
```

### 3. Expected Output

```
📸 SNAPSHOT STARTED lsn=0/1A2B3C4D time=2024-01-15T10:30:00Z

📦 SNAPSHOT DATA table=products data=map[id:1 name:Product 1 price:125.50 stock:442 category:Electronics] isLast=false
📦 SNAPSHOT DATA table=products data=map[id:2 name:Product 2 price:87.30 stock:156 category:Clothing] isLast=false
📦 SNAPSHOT DATA table=products data=map[id:3 name:Product 3 price:342.10 stock:789 category:Food] isLast=false

📦 SNAPSHOT PROGRESS table=products rows=10000 elapsed=2s rowsPerSec=5000
📦 SNAPSHOT PROGRESS table=products rows=20000 elapsed=4s rowsPerSec=5000
📦 SNAPSHOT PROGRESS table=products rows=30000 elapsed=6s rowsPerSec=5000
📦 SNAPSHOT PROGRESS table=products rows=40000 elapsed=8s rowsPerSec=5000
📦 SNAPSHOT PROGRESS table=products rows=50000 elapsed=10s rowsPerSec=5000

✅ SNAPSHOT COMPLETED totalRows=50000 duration=10s avgRowsPerSec=5000 lsn=0/1A2B3C4D

============================================================
📊 SNAPSHOT STATISTICS
============================================================
Total Rows:        50000
Duration:          10s
Avg Rows/Second:   5000
============================================================
🎯 Now listening for CDC events...
   Try: INSERT, UPDATE, DELETE operations
============================================================
```

### 4. Test CDC Events

Open a new terminal and connect to PostgreSQL:

```bash
psql "postgres://cdc_user:cdc_pass@127.0.0.1/cdc_db"
```

Try some operations:

```sql
-- Insert new product
INSERT INTO products (name, price, stock, category) 
VALUES ('New Laptop', 1299.99, 50, 'Electronics');

-- Update existing product
UPDATE products SET price = 999.99 WHERE id = 1;

-- Delete product
DELETE FROM products WHERE id = 2;
```

You'll see CDC events in the application output:

```
📝 CDC INSERT table=products data=map[id:50001 name:New Laptop price:1299.99 stock:50 category:Electronics] totalInserts=1
✏️  CDC UPDATE table=products old=map[id:1 price:125.50] new=map[id:1 price:999.99] totalUpdates=1
🗑️  CDC DELETE table=products old=map[id:2 name:Product 2 price:87.30] totalDeletes=1
```

## Configuration

Key snapshot configuration in `main.go`:

```go
Snapshot: config.SnapshotConfig{
    Enabled:            true,                       // Enable snapshot
    Mode:               config.SnapshotModeInitial, // Only on first run
    BatchSize:          5000,                       // Rows per batch
    CheckpointInterval: 5,                          // Save state every 5 batches
    MaxRetries:         3,                          // Retry attempts
    RetryDelay:         5 * time.Second,           // Delay between retries
    Timeout:            10 * time.Minute,          // Overall timeout
}
```

### Snapshot Modes

- **`initial`**: Take snapshot only if no previous snapshot exists (default)
- **`never`**: Skip snapshot, only CDC from now on

## Prometheus Metrics

View metrics at: http://localhost:8082/metrics

Key snapshot metrics:

```promql
# Is snapshot in progress?
go_pq_cdc_snapshot_in_progress

# Total tables to snapshot
go_pq_cdc_snapshot_total_tables

# Completed tables
go_pq_cdc_snapshot_completed_tables

# Total rows read
go_pq_cdc_snapshot_total_rows

# Duration in seconds
go_pq_cdc_snapshot_duration_seconds

# Snapshot progress percentage
(go_pq_cdc_snapshot_completed_tables / go_pq_cdc_snapshot_total_tables) * 100
```

## Recovery Test

To test checkpoint recovery:

1. Start the application
2. Wait until snapshot processes ~25,000 rows (50% complete)
3. Kill the application (Ctrl+C)
4. Restart the application

The snapshot will **resume from the last checkpoint** instead of starting over!

```
📦 SNAPSHOT PROGRESS rows=25000 elapsed=5s
^C (killed)

# Restart
go run main.go

# Will resume from checkpoint:
📸 SNAPSHOT STARTED (resuming from checkpoint)
📦 SNAPSHOT PROGRESS rows=30000 elapsed=1s (resumed)
```

## State Management

Snapshot state is stored in PostgreSQL:

```sql
SELECT * FROM cdc_snapshot_state;

 slot_name     | last_snapshot_lsn | current_table | current_offset | total_rows | completed
---------------|-------------------|---------------|----------------|------------|----------
 snapshot_slot | 0/1A2B3C4D       | public.products | 25000        | 25000      | false
```

## Performance Tuning

### Large Tables (Millions of Rows)

```go
Snapshot: config.SnapshotConfig{
    BatchSize:          10000,  // Increase batch size
    CheckpointInterval: 10,     // Save less frequently
    Timeout:            60 * time.Minute, // Longer timeout
}
```

### Small Tables (Thousands of Rows)

```go
Snapshot: config.SnapshotConfig{
    BatchSize:          1000,   // Smaller batches
    CheckpointInterval: 5,      // More frequent checkpoints
}
```

## Cleanup

```bash
# Stop and remove containers
docker-compose down -v

# Remove snapshot state
psql "postgres://cdc_user:cdc_pass@127.0.0.1/cdc_db" -c "DROP TABLE IF EXISTS cdc_snapshot_state;"
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  go-pq-cdc Snapshot                     │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  1. BEGIN TRANSACTION (REPEATABLE READ)                │
│     - Captures consistent snapshot at specific LSN     │
│                                                         │
│  2. BATCH PROCESSING                                    │
│     ┌─────────────────────────────────────┐            │
│     │ SELECT * FROM products              │            │
│     │ ORDER BY id                         │            │
│     │ LIMIT 5000 OFFSET 0;                │            │
│     └─────────────────────────────────────┘            │
│     ↓                                                   │
│     [5000 rows] → Handler → Application                │
│     ↓                                                   │
│     Checkpoint saved (every 5 batches)                 │
│     ↓                                                   │
│     Next batch (OFFSET 5000)                           │
│                                                         │
│  3. COMMIT TRANSACTION                                  │
│                                                         │
│  4. Switch to CDC mode                                  │
│     - Listen for INSERT/UPDATE/DELETE                  │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## Learn More

- [Main README](../../README.md)
- [Configuration Guide](../../README.md#configuration)
- [Metrics Documentation](../../README.md#exposed-metrics)

