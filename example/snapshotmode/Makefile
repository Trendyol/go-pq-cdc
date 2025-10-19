.PHONY: help start stop restart clean build run logs db-up db-down db-shell db-clean test-setup

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Database operations
db-up: ## Start PostgreSQL
	docker-compose up -d postgres
	@echo "Waiting for PostgreSQL..."
	@sleep 3
	@docker-compose exec -T postgres pg_isready -U testuser -d testdb || (echo "PostgreSQL not ready" && exit 1)
	@echo "✅ PostgreSQL is ready!"

db-down: ## Stop PostgreSQL
	docker-compose down

db-restart: db-down db-up ## Restart PostgreSQL

db-shell: ## Open psql shell
	docker-compose exec postgres psql -U testuser -d testdb

db-clean: ## Clean all tables and slots
	docker-compose exec -T postgres psql -U testuser -d testdb -c "DROP TABLE IF EXISTS cdc_snapshot_job CASCADE;"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "DROP TABLE IF EXISTS cdc_snapshot_chunks CASCADE;"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "SELECT pg_drop_replication_slot('cdc_slot') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'cdc_slot');"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "DROP PUBLICATION IF EXISTS cdc_publication;"
	@echo "✅ Cleaned all CDC tables and slots"

# Application operations
build: ## Build the application
	go mod download
	go build -o snapshot-test .

run: build ## Run the application
	./snapshot-test

clean: ## Clean build artifacts
	rm -f snapshot-test

# Quick test setups
test-setup-1: ## Setup for Test 1.1: Single table, single chunk
	$(MAKE) db-clean
	docker-compose exec -T postgres psql -U testuser -d testdb -c "DROP TABLE IF EXISTS users CASCADE;"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT);"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "INSERT INTO users SELECT i, 'user_' || i, 'user' || i || '@test.com' FROM generate_series(1, 50) i;"
	@echo "✅ Test 1.1 setup complete: 50 rows in users table"

test-setup-2: ## Setup for Test 1.2: Single table, multiple chunks
	$(MAKE) db-clean
	docker-compose exec -T postgres psql -U testuser -d testdb -c "DROP TABLE IF EXISTS orders CASCADE;"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INT, amount DECIMAL, created_at TIMESTAMP);"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "INSERT INTO orders SELECT i, (i % 100) + 1, random() * 1000, NOW() FROM generate_series(1, 500) i;"
	@echo "✅ Test 1.2 setup complete: 500 rows in orders table"

test-setup-3: ## Setup for Test 1.3: Multiple tables
	$(MAKE) db-clean
	docker-compose exec -T postgres psql -U testuser -d testdb -c "DROP TABLE IF EXISTS users CASCADE;"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "DROP TABLE IF EXISTS products CASCADE;"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "INSERT INTO users SELECT i, 'user_' || i FROM generate_series(1, 150) i;"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, price DECIMAL);"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "INSERT INTO products SELECT i, 'product_' || i, i * 10.5 FROM generate_series(1, 80) i;"
	@echo "✅ Test 1.3 setup complete: 150 rows in users, 80 rows in products"

test-setup-empty: ## Setup for Test 3.2: Empty table
	$(MAKE) db-clean
	docker-compose exec -T postgres psql -U testuser -d testdb -c "DROP TABLE IF EXISTS empty_table CASCADE;"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "CREATE TABLE empty_table (id SERIAL PRIMARY KEY, data TEXT);"
	@echo "✅ Test 3.2 setup complete: empty_table created"

test-setup-no-pk: ## Setup for Test 3.3: Table without primary key
	$(MAKE) db-clean
	docker-compose exec -T postgres psql -U testuser -d testdb -c "DROP TABLE IF EXISTS no_pk_table CASCADE;"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "CREATE TABLE no_pk_table (name TEXT, value INT);"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "INSERT INTO no_pk_table SELECT 'user_' || i, i FROM generate_series(1, 100) i;"
	@echo "✅ Test 3.3 setup complete: 100 rows in no_pk_table (no PK)"

test-setup-crash: ## Setup for Test 2.1: Crash test
	$(MAKE) db-clean
	docker-compose exec -T postgres psql -U testuser -d testdb -c "DROP TABLE IF EXISTS crash_test CASCADE;"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "CREATE TABLE crash_test (id SERIAL PRIMARY KEY, data TEXT);"
	docker-compose exec -T postgres psql -U testuser -d testdb -c "INSERT INTO crash_test SELECT i, md5(random()::text) FROM generate_series(1, 500) i;"
	@echo "✅ Test 2.1 setup complete: 500 rows in crash_test"

# Utility commands
logs: ## Show application logs (if running in background)
	tail -f snapshot-test.log

status: ## Show current database state
	@echo "=== Job Status ==="
	@docker-compose exec -T postgres psql -U testuser -d testdb -c "SELECT slot_name, completed, total_chunks, completed_chunks, snapshot_lsn, started_at FROM cdc_snapshot_job;" 2>/dev/null || echo "No job found"
	@echo ""
	@echo "=== Chunk Status ==="
	@docker-compose exec -T postgres psql -U testuser -d testdb -c "SELECT table_name, status, COUNT(*) as count, SUM(rows_processed) as total_rows FROM cdc_snapshot_chunks GROUP BY table_name, status ORDER BY table_name, status;" 2>/dev/null || echo "No chunks found"

verify: ## Verify test expectations
	@echo "=== Job Completion ==="
	@docker-compose exec -T postgres psql -U testuser -d testdb -t -c "SELECT 'completed=' || completed || ', total_chunks=' || total_chunks || ', completed_chunks=' || completed_chunks FROM cdc_snapshot_job;"
	@echo ""
	@echo "=== Chunks by Table ==="
	@docker-compose exec -T postgres psql -U testuser -d testdb -c "SELECT table_name, COUNT(*) as chunk_count, SUM(rows_processed) as total_rows FROM cdc_snapshot_chunks GROUP BY table_name;"
	@echo ""
	@echo "=== All Chunks ==="
	@docker-compose exec -T postgres psql -U testuser -d testdb -c "SELECT chunk_index, table_name, status, rows_processed FROM cdc_snapshot_chunks ORDER BY table_name, chunk_index;"

