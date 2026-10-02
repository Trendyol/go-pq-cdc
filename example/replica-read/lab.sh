#!/bin/sh
# Runs the README steps unattended. Needs: docker compose, go.
set -eu
cd "$(dirname "$0")"
BIN=$(mktemp -d)/rr
LOG=$(mktemp)
docker compose up -d --wait >/dev/null 2>&1
go build -o "$BIN" .
psql()    { docker compose exec -T primary psql -U cdc_user -d cdc_db -Atc "$1" >/dev/null; }
waitlog() { i=0; until grep -q "$1" "$LOG"; do i=$((i+1)); [ $i -gt 80 ] && { echo "timeout waiting for '$1'"; exit 1; }; sleep 0.5; done; }
start()   { : > "$LOG"; "$BIN" "$@" >> "$LOG" 2>&1 & PID=$!; waitlog "slot captured"; : > "$LOG"; }
stop()    { kill $PID 2>/dev/null; wait $PID 2>/dev/null || true; : > "$LOG"; }
show()    { grep -E "at event|after strict|NOT FOUND" "$LOG"; }

echo "## 1. two sync standbys, guard on: the event arrives while standby2 sits at CommitLSN"
start; sleep 4   # let standby2 apply the publication/slot commits first
psql "INSERT INTO orders (note) VALUES ('plain')"; waitlog "after strict"; show; : > "$LOG"

echo "## 2. synchronous_commit = remote_apply + guard: nothing left to wait for"
psql "SET synchronous_commit = remote_apply; INSERT INTO orders (note) VALUES ('remote_apply')"; waitlog "after strict"; show
stop

echo "## 3. remote_apply without the guard: the event outruns the primary's own wait"
start -guard=false
psql "SET synchronous_commit = remote_apply; INSERT INTO orders (note) VALUES ('remote_apply-no-guard')"; waitlog "after strict"; show
stop

echo "## 4. check on standby1, read on standby2: the production symptom"
start -check-port 5438 -read-port 5439
psql "INSERT INTO orders (note) VALUES ('split')"; waitlog "NOT FOUND"; show
stop

echo "## 5. visibilityGuard.replicas: the connector waits for standby2, the handler finds the row at once"
start -replicas 127.0.0.1:5439
psql "INSERT INTO orders (note) VALUES ('replicas')"; waitlog "after strict"; show
stop

docker compose down -v >/dev/null 2>&1
echo "done"
