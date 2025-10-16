.PHONY: up down logs run test-insert test-update test-delete test-all clean

up:
	@echo "🚀 Starting PostgreSQL..."
	docker-compose up -d
	@echo "⏳ Waiting for PostgreSQL to be ready..."
	@sleep 5
	@echo "✅ PostgreSQL is ready!"
	@echo "📊 Database stats:"
	@docker-compose exec -T postgres psql -U cdc_user -d cdc_db -c "SELECT category, COUNT(*) as count, AVG(price)::numeric(10,2) as avg_price FROM products GROUP BY category ORDER BY category;"

down:
	@echo "🛑 Stopping PostgreSQL..."
	docker-compose down -v

logs:
	docker-compose logs -f postgres

run:
	@echo "🏃 Running snapshot example..."
	go run main.go

test-insert:
	@echo "📝 Testing INSERT..."
	docker-compose exec -T postgres psql -U cdc_user -d cdc_db -c "INSERT INTO products (name, price, stock, category) VALUES ('Test Product', 99.99, 100, 'Electronics');"

test-update:
	@echo "✏️  Testing UPDATE..."
	docker-compose exec -T postgres psql -U cdc_user -d cdc_db -c "UPDATE products SET price = 149.99 WHERE id = 1;"

test-delete:
	@echo "🗑️  Testing DELETE..."
	docker-compose exec -T postgres psql -U cdc_user -d cdc_db -c "DELETE FROM products WHERE id = 2;"

test-all: test-insert test-update test-delete

# Database inspection commands
psql:
	@echo "🔍 Connecting to PostgreSQL..."
	docker-compose exec postgres psql -U cdc_user -d cdc_db

check-snapshot:
	@echo "📊 Checking snapshot state..."
	@docker-compose exec -T postgres psql -U cdc_user -d cdc_db -c "\
		SELECT slot_name, current_table, current_offset, total_rows, completed, last_snapshot_at \
		FROM cdc_snapshot_state \
		WHERE slot_name = 'snapshot_slot';" 2>/dev/null || echo "⚠️  No snapshot state found yet"

check-tables:
	@echo "📋 Listing all tables..."
	@docker-compose exec -T postgres psql -U cdc_user -d cdc_db -c "\dt"

check-products:
	@echo "📦 Products table stats..."
	@docker-compose exec -T postgres psql -U cdc_user -d cdc_db -c "\
		SELECT \
			COUNT(*) as total_rows, \
			COUNT(DISTINCT category) as categories, \
			AVG(price)::numeric(10,2) as avg_price \
		FROM products;"

check-replication:
	@echo "🔄 Replication slot info..."
	@docker-compose exec -T postgres psql -U cdc_user -d cdc_db -c "\
		SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, wal_status \
		FROM pg_replication_slots \
		WHERE slot_name = 'snapshot_slot';"

watch-snapshot:
	@echo "👀 Watching snapshot progress (Ctrl+C to stop)..."
	@while true; do \
		clear; \
		echo "Snapshot Progress (refreshing every 2s)"; \
		echo "======================================"; \
		docker-compose exec -T postgres psql -U cdc_user -d cdc_db -c "\
			SELECT slot_name, current_table, current_offset, total_rows, completed \
			FROM cdc_snapshot_state \
			WHERE slot_name = 'snapshot_slot';" 2>/dev/null || echo "Waiting for snapshot to start..."; \
		sleep 2; \
	done

reset-snapshot:
	@echo "🔄 Resetting snapshot state..."
	@docker-compose exec -T postgres psql -U cdc_user -d cdc_db -c "\
		DELETE FROM cdc_snapshot_state WHERE slot_name = 'snapshot_slot';"
	@echo "✅ Snapshot state cleared. Restart app to take fresh snapshot."

clean:
	@echo "🧹 Cleaning up..."
	docker-compose down -v
	rm -f go.sum

help:
	@echo "Available commands:"
	@echo ""
	@echo "🚀 Setup & Run:"
	@echo "  make up                  - Start PostgreSQL with initial data"
	@echo "  make down                - Stop PostgreSQL"
	@echo "  make logs                - View PostgreSQL logs"
	@echo "  make run                 - Run the snapshot example"
	@echo ""
	@echo "🧪 Testing CDC:"
	@echo "  make test-insert         - Test CDC INSERT"
	@echo "  make test-update         - Test CDC UPDATE"
	@echo "  make test-delete         - Test CDC DELETE"
	@echo "  make test-all            - Run all CDC tests"
	@echo ""
	@echo "🔍 Database Inspection:"
	@echo "  make psql                - Open psql shell"
	@echo "  make check-snapshot      - View snapshot checkpoint state"
	@echo "  make check-tables        - List all tables"
	@echo "  make check-products      - View products table stats"
	@echo "  make check-replication   - View replication slot info"
	@echo "  make watch-snapshot      - Watch snapshot progress (live)"
	@echo "  make reset-snapshot      - Clear snapshot state (force re-snapshot)"
	@echo ""
	@echo "🧹 Cleanup:"
	@echo "  make clean               - Clean up everything"

