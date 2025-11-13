.PHONY: help up down clean run build test logs reset multipod verify-multipod

# Default target
help:
	@echo "📖 Snapshot-Only Mode Example - Available Commands:"
	@echo ""
	@echo "  make up            - Start PostgreSQL with sample data"
	@echo "  make down          - Stop PostgreSQL"
	@echo "  make run           - Run snapshot-only example (single pod)"
	@echo "  make multipod      - Run with 3 pods in parallel"
	@echo "  make build         - Build the example binary"
	@echo "  make test          - Run snapshot and verify"
	@echo "  make logs          - Show PostgreSQL logs"
	@echo "  make verify        - Verify snapshot job status"
	@echo "  make clean         - Clean up everything"
	@echo "  make reset         - Reset database to initial state"
	@echo ""

# Start PostgreSQL
up:
	@echo "🚀 Starting PostgreSQL..."
	docker-compose up -d
	@echo "⏳ Waiting for PostgreSQL to be ready..."
	@sleep 5
	@docker-compose exec -T postgres pg_isready -U testuser -d testdb
	@echo "✅ PostgreSQL is ready!"
	@echo ""
	@echo "📊 Database info:"
	@docker-compose exec -T postgres psql -U testuser -d testdb -c "SELECT 'users' as table, COUNT(*) as rows FROM users UNION ALL SELECT 'orders', COUNT(*) FROM orders;"

# Stop PostgreSQL
down:
	@echo "🛑 Stopping PostgreSQL..."
	docker-compose down

# Clean everything
clean: down
	@echo "🧹 Cleaning up..."
	docker-compose down -v
	rm -f snapshot-only-example
	@echo "✅ Cleanup complete!"

# Build the example
build:
	@echo "🔨 Building snapshot-only example..."
	go build -o snapshot-only-example main.go
	@echo "✅ Build complete!"

# Run the example
run: build
	@echo "🚀 Running snapshot-only example..."
	@echo ""
	./snapshot-only-example -config ./config.yml

# Test: Start DB, run snapshot, verify
test: up
	@echo ""
	@echo "🧪 Running snapshot test..."
	@sleep 2
	@make run
	@echo ""
	@echo "✅ Snapshot test complete!"

# Show PostgreSQL logs
logs:
	docker-compose logs -f postgres

# Reset database to initial state
reset:
	@echo "🔄 Resetting database..."
	docker-compose down -v
	@make up
	@echo "✅ Database reset complete!"

# Check if snapshot completed successfully
verify:
	@echo "🔍 Verifying snapshot completion..."
	@docker-compose exec -T postgres psql -U testuser -d testdb -c \
		"SELECT slot_name, completed, total_chunks, completed_chunks \
		 FROM cdc_snapshot_job \
		 WHERE slot_name LIKE 'snapshot_only_%';"