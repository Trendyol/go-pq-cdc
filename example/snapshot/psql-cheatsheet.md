# PostgreSQL Cheat Sheet - Snapshot Example

## Connection

```bash
# Connect to database
psql "postgres://cdc_user:cdc_pass@127.0.0.1/cdc_db"

# Or using docker-compose
docker-compose exec postgres psql -U cdc_user -d cdc_db
```

## Basic Commands

```sql
-- List all tables
\dt

-- List all tables with size
\dt+

-- Describe table structure
\d products
\d cdc_snapshot_state

-- List all schemas
\dn

-- List all databases
\l

-- Show current database
SELECT current_database();

-- Exit psql
\q
```

## Snapshot State Table

```sql
-- View snapshot state (checkpoint table)
SELECT * FROM cdc_snapshot_state;

-- View with formatted columns
SELECT 
    slot_name,
    last_snapshot_lsn,
    last_snapshot_at,
    current_table,
    current_offset,
    total_rows,
    completed
FROM cdc_snapshot_state;

-- Check if snapshot completed
SELECT 
    slot_name,
    completed,
    total_rows,
    last_snapshot_at
FROM cdc_snapshot_state
WHERE slot_name = 'snapshot_slot';

-- Delete snapshot state (force re-snapshot)
DELETE FROM cdc_snapshot_state WHERE slot_name = 'snapshot_slot';

-- Drop the state table completely
DROP TABLE IF EXISTS cdc_snapshot_state;
```

## Products Table

```sql
-- Count total products
SELECT COUNT(*) FROM products;

-- View first 10 products
SELECT * FROM products LIMIT 10;

-- View products by category
SELECT 
    category, 
    COUNT(*) as count,
    AVG(price)::numeric(10,2) as avg_price,
    MIN(price)::numeric(10,2) as min_price,
    MAX(price)::numeric(10,2) as max_price
FROM products 
GROUP BY category
ORDER BY category;

-- View most expensive products
SELECT id, name, price, category 
FROM products 
ORDER BY price DESC 
LIMIT 10;

-- Check table size
SELECT 
    pg_size_pretty(pg_total_relation_size('products')) as total_size,
    pg_size_pretty(pg_relation_size('products')) as table_size,
    pg_size_pretty(pg_indexes_size('products')) as indexes_size;
```

## Replication Slot Info

```sql
-- View all replication slots
SELECT * FROM pg_replication_slots;

-- View snapshot slot details
SELECT 
    slot_name,
    slot_type,
    active,
    active_pid,
    restart_lsn,
    confirmed_flush_lsn,
    wal_status,
    pg_current_wal_lsn() as current_lsn
FROM pg_replication_slots
WHERE slot_name = 'snapshot_slot';

-- Check replication lag
SELECT 
    slot_name,
    pg_size_pretty(
        pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
    ) as lag_bytes,
    pg_current_wal_lsn() as current_lsn,
    confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = 'snapshot_slot';

-- Drop replication slot (if needed)
-- SELECT pg_drop_replication_slot('snapshot_slot');
```

## Publication Info

```sql
-- View all publications
SELECT * FROM pg_publication;

-- View publication tables
SELECT 
    p.pubname,
    t.schemaname,
    t.tablename
FROM pg_publication p
JOIN pg_publication_tables t ON p.pubname = t.pubname
WHERE p.pubname = 'snapshot_publication';

-- View publication operations
SELECT 
    pubname,
    pubinsert,
    pubupdate,
    pubdelete,
    pubtruncate
FROM pg_publication
WHERE pubname = 'snapshot_publication';
```

## Testing CDC Operations

```sql
-- Insert new product
INSERT INTO products (name, price, stock, category) 
VALUES ('Test Laptop', 1299.99, 50, 'Electronics');

-- Update product
UPDATE products 
SET price = 999.99, stock = 45 
WHERE name = 'Test Laptop';

-- Delete product
DELETE FROM products 
WHERE name = 'Test Laptop';

-- Bulk insert for testing
INSERT INTO products (name, price, stock, category)
SELECT
    'Bulk Product ' || i,
    ROUND((random() * 500 + 50)::numeric, 2),
    floor(random() * 100)::int,
    'Electronics'
FROM generate_series(1, 100) AS i;
```

## Monitoring Queries

```sql
-- Current WAL position
SELECT pg_current_wal_lsn();

-- Database size
SELECT pg_size_pretty(pg_database_size('cdc_db'));

-- Active connections
SELECT 
    pid,
    usename,
    application_name,
    client_addr,
    state,
    query
FROM pg_stat_activity
WHERE datname = 'cdc_db';

-- Check WAL sender processes (replication)
SELECT * FROM pg_stat_replication;

-- View table statistics
SELECT 
    schemaname,
    tablename,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows,
    last_vacuum,
    last_autovacuum
FROM pg_stat_user_tables
WHERE tablename = 'products';
```

## Troubleshooting

```sql
-- Check if logical replication is enabled
SHOW wal_level;
-- Should be 'logical'

-- Check max replication slots
SHOW max_replication_slots;

-- Check if table has replica identity
SELECT 
    c.relname,
    c.relreplident
FROM pg_class c
WHERE c.relname = 'products';
-- 'f' = FULL, 'd' = DEFAULT

-- Set replica identity to FULL (if needed)
ALTER TABLE products REPLICA IDENTITY FULL;

-- View locks on tables
SELECT 
    l.pid,
    l.mode,
    l.granted,
    c.relname as table_name
FROM pg_locks l
JOIN pg_class c ON l.relation = c.oid
WHERE c.relname IN ('products', 'cdc_snapshot_state');
```

## Cleanup Commands

```sql
-- Clear all products
TRUNCATE TABLE products;

-- Reset auto-increment
ALTER SEQUENCE products_id_seq RESTART WITH 1;

-- Re-insert test data
INSERT INTO products (name, price, stock, category)
SELECT
    'Product ' || i,
    ROUND((random() * 1000 + 10)::numeric, 2),
    floor(random() * 1000)::int,
    CASE 
        WHEN i % 4 = 0 THEN 'Electronics'
        WHEN i % 4 = 1 THEN 'Clothing'
        WHEN i % 4 = 2 THEN 'Food'
        ELSE 'Other'
    END
FROM generate_series(1, 50000) AS i;

-- Drop everything (nuclear option)
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS cdc_snapshot_state CASCADE;
DROP PUBLICATION IF EXISTS snapshot_publication;
-- SELECT pg_drop_replication_slot('snapshot_slot'); -- Only if not active
```

## Useful Shortcuts in psql

```
\?          - Show all psql commands
\h          - Show SQL command help
\timing     - Toggle query timing
\x          - Toggle expanded display (vertical format)
\pset pager off  - Disable paging
\watch 1    - Execute query every 1 second (like watch)
```

## Example: Watch Snapshot Progress

```sql
-- In psql, run this to watch snapshot progress
\watch 1
SELECT 
    slot_name,
    current_table,
    current_offset,
    total_rows,
    completed,
    last_snapshot_at
FROM cdc_snapshot_state
WHERE slot_name = 'snapshot_slot';
```

