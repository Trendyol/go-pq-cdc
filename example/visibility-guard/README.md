# Visibility Guard Lab

A hands-on tour of three features on PostgreSQL 17: `visibilityGuard`, `ctx.CommitLSN` and `slot.failover`.
The consumer in `main.go` plays a downstream service: for every insert event it opens a fresh snapshot on the
primary and reports **VISIBLE** (the row is there) or **MISS** (the event arrived before the row is visible).

## The window, in one paragraph

When a transaction commits, PostgreSQL first writes the commit record to WAL and flushes it. At that moment the
logical walsender already sends the change to go-pq-cdc. Only afterwards does the committing backend mark the
transaction as finished so that new snapshots can see its rows. Normally that gap is microseconds. With
synchronous replication (Patroni `synchronous_mode`) the backend waits for the standby's acknowledgement between
those two steps, so the gap grows to a network round-trip, and a consumer that reads the primary right after the
event can miss the row.

This lab makes the gap infinite on purpose: `synchronous_standby_names = 'ghost'` makes every commit wait for a
standby that does not exist. The WAL is flushed, the event is sent, the row stays invisible until you release it.

## Setup

```bash
cd example/visibility-guard
docker compose up -d --wait   # PostgreSQL 17 on port 5436 (5432-5435 are often taken)
go run .                      # guard off
```

`./lab.sh` runs all six steps below unattended in about 30 seconds and prints what it saw; the manual walk-through
is where the learning is.

In a second terminal, open a psql session inside the container:

```bash
docker compose exec postgres psql -U cdc_user -d cdc_db
```

> Start the consumer **before** freezing commits. Creating the publication and the slot are commits too, and they
> would hang.

## 1. Guard off: see the MISS

```sql
INSERT INTO orders (note) VALUES ('normal');          -- consumer logs VISIBLE
ALTER SYSTEM SET synchronous_standby_names = 'ghost';
SELECT pg_reload_conf();                               -- from now on every commit waits forever
INSERT INTO orders (note) VALUES ('frozen');          -- this statement hangs, that is the point
```

The consumer logs:

```
INFO VISIBLE id=1 note=normal commitLSN=0/192E498 walNow=0/192E4C8
WARN MISS: event received but row not visible yet id=2 note=frozen commitLSN=0/192E558 walNow=0/192E588
```

In a third psql session `SELECT * FROM orders WHERE note = 'frozen'` returns nothing: the event exists, the row
does not (yet).

Release the commit:

```sql
ALTER SYSTEM RESET synchronous_standby_names;
SELECT pg_reload_conf();                               -- the hanging INSERT returns, the row is visible
```

## 2. Guard on, `failMode: closed` (default)

```bash
go run . -guard -timeout 5s
```

```sql
ALTER SYSTEM SET synchronous_standby_names = 'ghost';
SELECT pg_reload_conf();
INSERT INTO orders (note) VALUES ('frozen-closed');
```

Freeze and insert again. Nothing reaches the handler: the guard polls `pg_current_snapshot()` and the transaction
is still in progress. After `timeout` the process logs `visibility guard failed, restarting stream` with the error
class `visibility guard unreachable` and **exits** (the same crash-and-restart path as a lost replication
connection; in production your orchestrator restarts it). The event was never acked, so it is redelivered on the
next start. Release the commit, run the consumer again: the row arrives as VISIBLE.

```
ERROR visibility guard failed, restarting stream error="visibility guard unreachable: visibility guard timeout: xid 744 not visible after 5s"
(process exits; release; start again)
INFO VISIBLE id=3 note=frozen-closed commitLSN=0/192E620 walNow=0/192E688
```

Closed means "never hand out an event I cannot certify". The price is a restart loop while the primary is stuck,
which is what you want to be alerted on.

## 3. Guard on, `failMode: open`

```bash
go run . -guard -fail-mode open -timeout 3s
```

```sql
ALTER SYSTEM SET synchronous_standby_names = 'ghost';
SELECT pg_reload_conf();
INSERT INTO orders (note) VALUES ('frozen-closed-v2');
```

Freeze and insert. After `timeout` the consumer logs `visibility guard timeout, dispatching anyway (failMode: open)`
followed by the MISS, and keeps running. Check the counters:

```bash
curl -s localhost:8081/metrics | grep visibility
```

```
WARN visibility guard timeout, dispatching anyway (failMode: open) xid=745
WARN MISS: event received but row not visible yet id=4 note=frozen-open ...
go_pq_cdc_visibility_timeout_total{...} 1
go_pq_cdc_visibility_fail_open_total{...} 1
```

Open means "prefer latency over certainty and tell me about it". Release the commit when done.

## 4. Caveat: sessions that skip the wait

```sql
SET synchronous_commit = off;
INSERT INTO orders (note) VALUES ('async');           -- returns immediately even while frozen
```

The row is visible at once and the guard lets the event through: the session opted out of the standby wait, so
"visible on the primary" no longer implies "acknowledged by the standby". This is the first caveat of the
synchronous-replication corollary in the README; `pg_cancel_backend(<pid of a hanging INSERT>)` demonstrates the
second one (the transaction commits locally and becomes visible without the acknowledgement).

## 5. `ctx.CommitLSN`

Every VISIBLE/MISS line prints `commitLSN` (the transaction's commit record) next to `walNow`
(`pg_current_wal_lsn()` at read time). On a standby the read rule is strict:

```sql
SELECT pg_last_wal_replay_lsn() > '<commitLSN>'::pg_lsn;
```

`pg_last_wal_replay_lsn()` is the end of the last replayed record, so it equals `commitLSN` just before the
commit record is applied; `>=` would pass too early.

## 6. Failover slot

Stop the consumer (the slot must be inactive for `ALTER_REPLICATION_SLOT`), then:

```bash
go run . -failover
```

```sql
SELECT slot_name, failover FROM pg_replication_slots;  -- cdc_slot | t
```

The existing slot was altered; a fresh slot would be created with `FAILOVER true`. Switch the image in
`docker-compose.yml` to `postgres:16-alpine` and the same flag fails at startup with
`slot.failover requires PostgreSQL 17 or newer`. A failover slot only pays off with a standby running
`sync_replication_slots` and `synchronized_standby_slots` set on the primary; see the main README.

## Cleanup

```bash
docker compose down -v
```
