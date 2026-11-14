#!/bin/sh
# Runs the README steps against the docker-compose lab. Needs: docker compose, dig.
#   ./lab.sh          all five steps, then docker compose down -v
#   ./lab.sh 3        one step (the lab stays up; a step sets up what it needs)
#   ./lab.sh down     docker compose down -v
# Every step ends in PASS or FAIL lines; the exit status is 1 if any FAIL.
set -u
cd "$(dirname "$0")"
DNS="@127.0.0.1 -p 5354 +tcp +short"
FAILS=0
STATE=""   # "<config>/<hosts>" the connector was started with; lets steps 3-5 continue without a restart
MARK=0

# --- DNS records (dns/hosts, edited in place so the bind mount keeps working) ---
# Before the first edit, the file is copied to $SAVE; restore_hosts puts that copy back on exit.
SAVE=""
save_hosts() {
  [ -n "$SAVE" ] && return 0
  SAVE=$(mktemp) || exit 1
  cp dns/hosts "$SAVE" || { echo "cannot copy dns/hosts"; rm -f "$SAVE"; SAVE=""; exit 1; }
}
restore_hosts() {
  [ -n "$SAVE" ] || return 0
  if cat "$SAVE" > dns/hosts; then rm -f "$SAVE"; SAVE=""; else echo "could not restore dns/hosts from $SAVE"; fi
}
hosts() {
  save_hosts
  { echo "# One line per record; edit in place while the lab runs (CoreDNS reloads within ~1s)."
    echo "# 10.99.0.11 / 10.99.0.12 are mars / venus; 10.99.1.11 is a second address of mars."
    echo "10.99.0.11 replica.demo.mars.lab"
    [ "$1" != "venus-gone" ] && [ "$1" != "venus-empty" ] && echo "10.99.0.12 replica.demo.venus.lab"
    [ "$1" != "venus-empty" ] && echo "10.99.1.11 replica.demo.venus.lab"
    true
  } > dns/hosts
}
# Waits until the lab DNS (over TCP) answers the venus name with $1 records ("0" = no answer).
dns_wants() {
  i=0
  while [ "$(dig $DNS replica.demo.venus.lab A | grep -c .)" != "$1" ]; do
    i=$((i+1)); [ $i -gt 30 ] && { echo "timeout waiting for $1 venus records"; return 1; }; sleep 0.5
  done
}

# --- connector log: only what was written since MARK ---
clog()    { docker compose logs --no-log-prefix connector 2>&1; }
mark()    { MARK=$(clog | wc -l); }
fresh()   { clog | tail -n +$((MARK+1)); }
brief()   { sed -e 's/^{"time":"[^"]*","level":"\([^"]*\)","msg":/\1 /'; }
waitlog() { i=0; until fresh | grep -q "$1"; do i=$((i+1)); [ $i -gt ${2:-60} ] && return 1; sleep 0.5; done; }

# begin <config> <hosts>: (re)start the connector unless it already runs that way.
begin() {
  [ "$STATE" = "$1/$2" ] && { mark; return; }
  hosts "$2"; dns_wants "$([ "$2" = full ] && echo 2 || echo 1)" 2>/dev/null || true
  MARK=0
  CONFIG="connector-$1.yml" docker compose up -d --force-recreate --no-deps connector >/dev/null 2>&1
  waitlog "slot captured" 120 || { echo "connector did not start:"; fresh | brief | tail -5; exit 1; }
  STATE="$1/$2"; mark
}

insert() { docker compose exec -T primary psql -U cdc_user -d cdc_db -Atqc "INSERT INTO orders (note) VALUES ('$1') RETURNING id"; }
# event_of <id> [seconds]: waits for the handler's line of that row, sets EV and the parsed fields.
event_of() {
  waitlog "event id=$1 " $(( ${2:-20} * 2 )) || { EV=""; return 1; }
  EV=$(fresh | grep "event id=$1 " | head -1)
  HELD=$(echo "$EV" | sed -n 's/.*held_ms=\([0-9]*\).*/\1/p')
  MARS=$(echo "$EV" | sed -n 's/.* mars=\([a-z]*\).*/\1/p')
  VENUS=$(echo "$EV" | sed -n 's/.* venus=\([a-z]*\).*/\1/p')
  echo "    $EV"
}
pass() { echo "  PASS  $1"; }
fail() { echo "  FAIL  $1"; FAILS=$((FAILS+1)); }
check() { if eval "$2"; then pass "$1"; else fail "$1"; fi; }
# Waits one refresh interval (5s, see config/) plus a margin, so the next gated transaction re-reads DNS.
age_out() { sleep 6; }

step1() {
  echo "## 1. only the mars name configured: the event does not wait for venus (the incident)"
  begin mars full
  ID=$(insert only-mars); event_of "$ID" 20 || { fail "no event for row $ID within 20s"; return; }
  check "event arrived at once (held ${HELD}ms)" '[ "$HELD" -lt 2000 ]'
  check "row readable on mars" '[ "$MARS" = true ]'
  check "row NOT readable on venus: it is not a gated member" '[ "$VENUS" = false ]'
}

step2() {
  echo "## 2. both names configured: the event is held until venus has replayed the commit"
  begin both full
  clog | grep "replica set changed" | head -1 | brief | cut -c1-400
  ID=$(insert both-names); event_of "$ID" 30 || { fail "no event for row $ID within 30s"; return; }
  check "event held about the apply delay (held ${HELD}ms, delay 5000ms)" '[ "$HELD" -ge 4000 ] && [ "$HELD" -le 9000 ]'
  check "row readable on mars at dispatch" '[ "$MARS" = true ]'
  check "row readable on venus at dispatch" '[ "$VENUS" = true ]'
}

step3() {
  echo "## 3. drop venus from its name while another address stays: no restart, venus is no longer gated"
  echo "    (the venus name keeps 10.99.1.11, mars's second address, so the name never empties)"
  begin both full
  hosts venus-gone; dns_wants 1 || { fail "DNS did not change"; return; }
  age_out
  ID=$(insert venus-dropped); event_of "$ID" 30 || { fail "no event for row $ID within 30s"; return; }
  fresh | grep "replica set changed" | tail -1 | brief | cut -c1-400
  check "member dropped in the log, no connector restart" 'fresh | grep -q "replica set changed.*\"removed\":\[\"10.99.0.12:5432\"\]" && [ "$(clog | grep -c "cdc stream started")" = 1 ]'
  check "event arrived at once (held ${HELD}ms)" '[ "$HELD" -lt 2000 ]'
  check "row NOT readable on venus: it left the gated set" '[ "$VENUS" = false ]'
  STATE="both/venus-gone"
}

step4() {
  echo "## 4. put venus back: gated again after a refresh"
  begin both venus-gone
  hosts full; dns_wants 2 || { fail "DNS did not change"; return; }
  age_out
  ID=$(insert venus-back); event_of "$ID" 30 || { fail "no event for row $ID within 30s"; return; }
  fresh | grep "replica set changed" | tail -1 | brief | cut -c1-400
  check "member added in the log" 'fresh | grep -q "replica set changed.*\"added\":\[\"10.99.0.12:5432\"\]"'
  check "event held about the apply delay (held ${HELD}ms)" '[ "$HELD" -ge 4000 ] && [ "$HELD" -le 9000 ]'
  check "row readable on venus at dispatch" '[ "$VENUS" = true ] && [ "$MARS" = true ]'
  STATE="both/full"
}

step5() {
  echo "## 5. empty the venus name completely: nothing is dispatched until it has records again"
  begin both full
  hosts venus-empty; dns_wants 0 || { fail "DNS did not change"; return; }
  echo "    venus name now: NXDOMAIN ($(dig @127.0.0.1 -p 5354 +tcp +noall +comments replica.demo.venus.lab A | grep -o 'status: [A-Z]*'))"
  age_out
  ID=$(insert while-empty)
  waitlog "refresh failed" 20
  fresh | grep "refresh failed" | grep "no A or AAAA record" | head -1 | brief | cut -c1-400
  check "connector logged the refresh failure with the no-record reason" 'fresh | grep "refresh failed" | grep -q "no A or AAAA record"'
  WAIT=16   # waitlog polls, 0.5 s each: the window is WAIT / 2 seconds
  if waitlog "event id=$ID " $WAIT; then fail "row $ID was dispatched while the name was empty"; else pass "row $ID not dispatched while the name was empty ($((WAIT / 2))s)"; fi
  hosts full; dns_wants 2 || { fail "DNS did not change"; return; }
  STATE="both/full"
  event_of "$ID" 15 || { fail "row $ID did not arrive after the records came back"; return; }
  check "row dispatched after restore, readable on both" '[ "$VENUS" = true ] && [ "$MARS" = true ]'
}

up() { docker compose up -d --build --wait >/dev/null 2>&1 || { echo "docker compose up failed"; exit 1; }; }
down() { hosts full; docker compose down -v >/dev/null 2>&1; }
trap restore_hosts EXIT
trap 'restore_hosts; exit 130' INT
trap 'restore_hosts; exit 143' TERM

case "${1:-all}" in
  down) down; echo "down"; exit 0 ;;
  all)  up; step1; step2; step3; step4; step5; down ;;
  [1-5]) up; "step$1" ;;
  *) echo "usage: $0 [1|2|3|4|5|down]"; exit 2 ;;
esac
echo
if [ $FAILS -eq 0 ]; then echo "done: all checks passed"; else echo "done: $FAILS check(s) FAILED"; exit 1; fi
