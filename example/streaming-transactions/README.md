# Streaming Transactions Example

## Overview

This example demonstrates PostgreSQL's **streaming in-progress transactions** feature. In normal operation, PostgreSQL accumulates all WAL (Write-Ahead Log) data until a transaction commits, and only then sends it to CDC consumers.

However, with large transactions (such as inserting millions of rows), this approach consumes excessive memory. Starting with PostgreSQL 14, the "streaming" mode allows sending data from uncommitted transactions in chunks:

```
STREAM START  → chunk begins
  INSERT ...
  INSERT ...
STREAM STOP   → chunk ends (not yet committed!)
... other transactions can interleave ...
STREAM START  → next chunk
  INSERT ...
STREAM STOP
STREAM COMMIT → transaction completed
```

## What This Example Does

1. **Starts PostgreSQL** via docker-compose with `logical_decoding_work_mem=64kB` (default is 64MB; we reduce it to easily trigger streaming behavior)
2. **Starts a CDC consumer** that listens for changes
3. **Inserts 500 rows** in a single transaction
4. **Counts every received message** and verifies the total at the end

## The Problem (Issue #85)

**BEFORE the fix**: The last few rows would be lost because the buffer wasn't flushed on `STREAM STOP`.

**AFTER the fix**: All 500 rows are received correctly.

## Prerequisites

- Docker and Docker Compose installed
- Go 1.21+ installed
- Port 5433 available (or modify `docker-compose.yml` to use a different port)

## Usage

```bash
# Navigate to the example directory
cd example/streaming-transactions

# Start PostgreSQL with streaming configuration
docker compose up -d

# Wait for PostgreSQL to be ready (~3 seconds)
sleep 3

# Run the example
go run main.go

# Clean up when done
docker compose down
```

## Expected Output

The application will:
1. Create the CDC connector with necessary publication and slot
2. Wait for the connector to be ready
3. Insert 500 rows in a single transaction
4. Receive and count all INSERT messages
5. Log the first few, every 100th, and last few messages
6. Verify all 500 messages were received

Example output:
```
INFO CDC consumer ready, starting large transaction... satırSayısı=500
INFO INSERT received sayaç=1 id=1 name=user-1
INFO INSERT received sayaç=2 id=2 name=user-2
INFO INSERT received sayaç=3 id=3 name=user-3
INFO INSERT received sayaç=100 id=100 name=user-100
INFO INSERT received sayaç=200 id=200 name=user-200
...
INFO INSERT received sayaç=499 id=499 name=user-499
INFO INSERT received sayaç=500 id=500 name=user-500
INFO SUCCESS: All messages received! expected=500 received=500
```

## Configuration Details

- **PostgreSQL Port**: 5433 (to avoid conflicts with default PostgreSQL installations)
- **Database**: `cdc_db`
- **User**: `cdc_user` / Password: `cdc_pass`
- **Publication**: `cdc_publication`
- **Replication Slot**: `cdc_slot_streaming`
- **Table**: `public.users` (automatically created)
- **Logical Decoding Work Memory**: 64kB (configured to trigger streaming)

## How It Validates the Fix

The example deliberately:
- Uses a low `logical_decoding_work_mem` value to force streaming behavior
- Inserts enough rows (500) to exceed the memory limit multiple times
- Counts every single message received
- Fails if any messages are lost (timeout or count mismatch)

This ensures that the fix for Issue #85 (proper buffer flushing on `STREAM STOP`) is working correctly.

## Troubleshooting

**If the test fails with timeout:**
- Check PostgreSQL logs: `docker compose logs postgres`
- Ensure the database is fully initialized before running
- Verify port 5433 is not already in use

**If you see "kayıp" (lost) messages:**
- This indicates the streaming transaction fix may have regressed
- Check that you're using the fixed version of the library

**Connection errors:**
- Ensure Docker Compose is running: `docker compose ps`
- Wait a few more seconds for PostgreSQL initialization
- Check network connectivity to localhost:5433
