-- Example tables demonstrating different partition strategies
-- 1. products: STRING PK -> auto CTID partitioning
-- 2. orders: STRING PK -> auto CTID partitioning
-- 3. events: INTEGER PK (hash-based) -> explicit CTID partitioning

-- Products table with UUID-like string primary key
CREATE TABLE products (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Orders table with composite-like string primary key
CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    quantity INT NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    order_date TIMESTAMPTZ DEFAULT NOW()
);

-- Events table with INTEGER PK but values are HASH-BASED (not sequential)
-- Range partitioning would create very uneven chunks for this table
-- That's why we use explicit SnapshotPartitionStrategy: "ctid_block" in config
CREATE TABLE events (
    id BIGINT PRIMARY KEY,  -- Hash-based, not sequential!
    event_type TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert sample data for products (5000 rows with string PKs)
INSERT INTO products (id, name, category, price)
SELECT
    'PROD-' || LPAD(i::text, 8, '0'),
    'Product ' || i,
    CASE (i % 5)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Books'
        WHEN 3 THEN 'Home'
        ELSE 'Sports'
    END,
    ROUND((RANDOM() * 1000 + 10)::numeric, 2)
FROM generate_series(1, 5000) AS i;

-- Insert sample data for orders (10000 rows with string PKs)
INSERT INTO orders (order_id, product_id, customer_name, quantity, total_amount)
SELECT
    'ORD-' || TO_CHAR(NOW(), 'YYYYMMDD') || '-' || LPAD(i::text, 8, '0'),
    'PROD-' || LPAD((1 + (i % 5000))::text, 8, '0'),
    'Customer ' || (i % 1000),
    1 + (i % 10),
    ROUND((RANDOM() * 500 + 10)::numeric, 2)
FROM generate_series(1, 10000) AS i;

-- Insert sample data for events with HASH-BASED integer IDs
-- These IDs are NOT sequential - they simulate hash function output
-- Range partitioning would be terrible for this data!
INSERT INTO events (id, event_type, payload)
SELECT
    -- Simulate hash-based IDs using hashtext (sparse, non-sequential values)
    ABS(hashtext('event_' || i::text))::bigint,
    CASE (i % 4)
        WHEN 0 THEN 'page_view'
        WHEN 1 THEN 'click'
        WHEN 2 THEN 'purchase'
        ELSE 'signup'
    END,
    jsonb_build_object('user_id', i % 1000, 'session', 'sess_' || i)
FROM generate_series(1, 8000) AS i
ON CONFLICT (id) DO NOTHING;  -- Skip duplicates from hash collisions

-- Verify data
SELECT 'Products count: ' || COUNT(*) FROM products;
SELECT 'Orders count: ' || COUNT(*) FROM orders;
SELECT 'Events count: ' || COUNT(*) FROM events;
SELECT 'Events ID range: ' || MIN(id) || ' to ' || MAX(id) || ' (sparse, hash-based)' FROM events;

