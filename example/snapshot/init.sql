-- Initialize database for snapshot example

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price DECIMAL(10,2),
    stock INTEGER,
    category TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

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
FROM generate_series(1, 1000000) AS i;

-- Create indexes for better performance
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_created_at ON products(created_at);

-- Show statistics
SELECT 
    category, 
    COUNT(*) as count, 
    AVG(price)::numeric(10,2) as avg_price,
    SUM(stock) as total_stock
FROM products 
GROUP BY category
ORDER BY category;

