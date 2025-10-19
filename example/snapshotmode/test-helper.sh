#!/bin/bash

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

PSQL_CMD="docker-compose exec -T postgres psql -U testuser -d testdb"

# Helper functions
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Database query helpers
query() {
    $PSQL_CMD -c "$1"
}

query_silent() {
    $PSQL_CMD -c "$1" > /dev/null 2>&1
}

# Insert test data
insert_users() {
    local count=${1:-100}
    log_info "Inserting $count users..."
    query "INSERT INTO users SELECT i, 'user_' || i, 'user' || i || '@test.com' FROM generate_series(1, $count) i;"
    log_success "Inserted $count users"
}

insert_orders() {
    local count=${1:-500}
    log_info "Inserting $count orders..."
    query "INSERT INTO orders SELECT i, (i % 100) + 1, random() * 1000, NOW() FROM generate_series(1, $count) i;"
    log_success "Inserted $count orders"
}

insert_products() {
    local count=${1:-80}
    log_info "Inserting $count products..."
    query "INSERT INTO products SELECT i, 'product_' || i, i * 10.5 FROM generate_series(1, $count) i;"
    log_success "Inserted $count products"
}

# CDC test operations
test_cdc_insert() {
    local table=${1:-users}
    log_info "Testing CDC INSERT on $table..."
    
    if [ "$table" = "users" ]; then
        query "INSERT INTO users (name, email) VALUES ('cdc_user', 'cdc@test.com');"
    elif [ "$table" = "orders" ]; then
        query "INSERT INTO orders (user_id, amount, created_at) VALUES (999, 123.45, NOW());"
    elif [ "$table" = "products" ]; then
        query "INSERT INTO products (name, price) VALUES ('cdc_product', 99.99);"
    fi
    
    log_success "INSERT executed"
}

test_cdc_update() {
    local table=${1:-users}
    log_info "Testing CDC UPDATE on $table..."
    
    if [ "$table" = "users" ]; then
        query "UPDATE users SET name='updated_user' WHERE id=1;"
    elif [ "$table" = "orders" ]; then
        query "UPDATE orders SET amount=999.99 WHERE id=1;"
    elif [ "$table" = "products" ]; then
        query "UPDATE products SET price=199.99 WHERE id=1;"
    fi
    
    log_success "UPDATE executed"
}

test_cdc_delete() {
    local table=${1:-users}
    log_info "Testing CDC DELETE on $table..."
    
    if [ "$table" = "users" ]; then
        query "DELETE FROM users WHERE id=1;"
    elif [ "$table" = "orders" ]; then
        query "DELETE FROM orders WHERE id=1;"
    elif [ "$table" = "products" ]; then
        query "DELETE FROM products WHERE id=1;"
    fi
    
    log_success "DELETE executed"
}

# Status checking
show_job_status() {
    log_info "Job Status:"
    query "SELECT slot_name, completed, total_chunks, completed_chunks, snapshot_lsn, started_at FROM cdc_snapshot_job;"
}

show_chunks_status() {
    log_info "Chunks Status:"
    query "SELECT table_name, status, COUNT(*) as count, SUM(rows_processed) as total_rows FROM cdc_snapshot_chunks GROUP BY table_name, status ORDER BY table_name, status;"
}

show_all_chunks() {
    log_info "All Chunks:"
    query "SELECT id, table_name, chunk_index, status, rows_processed, claimed_by FROM cdc_snapshot_chunks ORDER BY table_name, chunk_index;"
}

show_stale_chunks() {
    log_info "Stale Chunks (heartbeat > 5 min):"
    query "SELECT id, table_name, chunk_index, status, claimed_by, heartbeat_at, NOW() - heartbeat_at as stale_duration FROM cdc_snapshot_chunks WHERE status = 'in_progress' AND heartbeat_at < NOW() - INTERVAL '5 minutes';"
}

# Verification helpers
verify_test_11() {
    log_info "Verifying Test 1.1: Single table, single chunk"
    log_info "Expected: 1 chunk, 50 rows, completed=true"
    echo ""
    show_job_status
    echo ""
    show_chunks_status
}

verify_test_12() {
    log_info "Verifying Test 1.2: Single table, multiple chunks"
    log_info "Expected: 5 chunks, 500 rows total, completed=true"
    echo ""
    show_job_status
    echo ""
    show_chunks_status
}

verify_test_13() {
    log_info "Verifying Test 1.3: Multiple tables"
    log_info "Expected: 3 chunks total (users=2, products=1), completed=true"
    echo ""
    show_job_status
    echo ""
    show_chunks_status
}

# Crash simulation
simulate_crash() {
    log_warning "Simulating crash..."
    pkill -9 -f "snapshot-test" || log_info "No running process found"
    log_success "Process killed"
}

# Main command dispatcher
case "$1" in
    insert-users)
        insert_users $2
        ;;
    insert-orders)
        insert_orders $2
        ;;
    insert-products)
        insert_products $2
        ;;
    cdc-insert)
        test_cdc_insert $2
        ;;
    cdc-update)
        test_cdc_update $2
        ;;
    cdc-delete)
        test_cdc_delete $2
        ;;
    job-status)
        show_job_status
        ;;
    chunks-status)
        show_chunks_status
        ;;
    all-chunks)
        show_all_chunks
        ;;
    stale-chunks)
        show_stale_chunks
        ;;
    verify-1.1)
        verify_test_11
        ;;
    verify-1.2)
        verify_test_12
        ;;
    verify-1.3)
        verify_test_13
        ;;
    crash)
        simulate_crash
        ;;
    *)
        echo "Usage: $0 {command} [args]"
        echo ""
        echo "Data Operations:"
        echo "  insert-users [count]      - Insert users (default: 100)"
        echo "  insert-orders [count]     - Insert orders (default: 500)"
        echo "  insert-products [count]   - Insert products (default: 80)"
        echo ""
        echo "CDC Test Operations:"
        echo "  cdc-insert [table]        - Test CDC INSERT (default: users)"
        echo "  cdc-update [table]        - Test CDC UPDATE (default: users)"
        echo "  cdc-delete [table]        - Test CDC DELETE (default: users)"
        echo ""
        echo "Status Commands:"
        echo "  job-status               - Show job status"
        echo "  chunks-status            - Show chunks grouped by table/status"
        echo "  all-chunks               - Show all chunks"
        echo "  stale-chunks             - Show stale chunks"
        echo ""
        echo "Verification:"
        echo "  verify-1.1               - Verify Test 1.1"
        echo "  verify-1.2               - Verify Test 1.2"
        echo "  verify-1.3               - Verify Test 1.3"
        echo ""
        echo "Utils:"
        echo "  crash                    - Simulate process crash"
        exit 1
        ;;
esac

