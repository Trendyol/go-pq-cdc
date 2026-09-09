#!/bin/sh
# Runs the six README steps unattended against the docker-compose PostgreSQL 17.
# Needs: docker compose, go, curl. Prints one or two lines per step.
set -eu
cd "$(dirname "$0")"
BIN=$(mktemp -d)/vg
LOG=$(mktemp -d)
docker compose up -d --wait >/dev/null 2>&1
go build -o "$BIN" .
psql()    { docker compose exec -T postgres psql -U cdc_user -d cdc_db -Atc "$1"; }
waitlog() { i=0; until grep -q "$2" "$1"; do i=$((i+1)); [ $i -gt 80 ] && { echo "timeout waiting for '$2'"; return 1; }; sleep 0.5; done; }
freeze()  { psql "ALTER SYSTEM SET synchronous_standby_names = 'ghost'" >/dev/null; psql "SELECT pg_reload_conf()" >/dev/null; }
release() { psql "ALTER SYSTEM RESET synchronous_standby_names" >/dev/null; psql "SELECT pg_reload_conf()" >/dev/null; }
start()   { "$BIN" "$@" > "$LOG/cur.log" 2>&1 & PID=$!; waitlog "$LOG/cur.log" "slot captured"; }
stop()    { kill $PID 2>/dev/null; wait $PID 2>/dev/null || true; }
insert()  { psql "INSERT INTO orders (note) VALUES ('$1')" >/dev/null & INS=$!; }

echo "## 1. guard off"
start
psql "INSERT INTO orders (note) VALUES ('normal')" >/dev/null; waitlog "$LOG/cur.log" VISIBLE; grep VISIBLE "$LOG/cur.log" | tail -1
freeze; insert frozen; waitlog "$LOG/cur.log" MISS; grep MISS "$LOG/cur.log" | tail -1
echo "rows visible to a fresh session while frozen: $(psql "SELECT count(*) FROM orders WHERE note = 'frozen'")"
release; wait $INS; stop

echo "## 2. guard on, failMode closed"
start -guard -timeout 5s
freeze; insert frozen-closed; waitlog "$LOG/cur.log" "visibility guard failed"; grep "visibility guard failed" "$LOG/cur.log" | tail -1
wait $PID 2>/dev/null || echo "consumer exited, event not acked"
release; wait $INS
start -guard -timeout 5s; waitlog "$LOG/cur.log" frozen-closed; grep frozen-closed "$LOG/cur.log" | tail -1; stop

echo "## 3. guard on, failMode open"
start -guard -fail-mode open -timeout 3s
freeze; insert frozen-open; waitlog "$LOG/cur.log" MISS; grep -E "dispatching anyway|MISS" "$LOG/cur.log" | tail -2
curl -s localhost:8081/metrics | grep -E "^go_pq_cdc_visibility_(timeout|fail_open)_total"
release; wait $INS; stop

echo "## 4. caveat: synchronous_commit = off"
start -guard -timeout 5s
freeze; psql "SET synchronous_commit = off; INSERT INTO orders (note) VALUES ('async')" >/dev/null
waitlog "$LOG/cur.log" async; grep async "$LOG/cur.log" | tail -1
release; stop

echo "## 5. CommitLSN: compare commitLSN with walNow in the lines above"

echo "## 6. failover slot"
echo "before: $(psql "SELECT failover FROM pg_replication_slots WHERE slot_name = 'cdc_slot'")"
start -failover; stop
echo "after:  $(psql "SELECT failover FROM pg_replication_slots WHERE slot_name = 'cdc_slot'")"

docker compose down -v >/dev/null 2>&1
echo "done"
