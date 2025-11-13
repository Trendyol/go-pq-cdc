-- Initialize test database with sample data

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product VARCHAR(100) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample users (100 users)
INSERT INTO users (name, email, age)
SELECT
    'User ' || generate_series,
    'user' || generate_series || '@example.com',
    20 + (generate_series % 50)
FROM generate_series(1, 100);

-- Insert sample orders (500 orders)
INSERT INTO orders (user_id, product, quantity, price)
SELECT
    1 + (generate_series % 100),
    'Product ' || (1 + (generate_series % 20)),
    1 + (generate_series % 10),
    (10 + (generate_series % 90))::decimal
FROM generate_series(1, 500);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO testuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO testuser;

