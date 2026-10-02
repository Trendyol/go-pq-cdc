# Reading a standby after a CDC event

`ctx.CommitLSN` is the start of the transaction's commit record. A hot standby has applied that transaction only once

```sql
SELECT pg_last_wal_replay_lsn() > '<ctx.CommitLSN>'::pg_lsn;   -- strict
```

This lab shows why `>` and not `>=`, why two synchronous standbys do not make the check unnecessary, what the one
common mistake looks like, and what `visibilityGuard.replicas` changes. The consumer in `main.go` reads a standby
after every insert event: it prints what the loose (`>=`) and strict (`>`) checks say at the moment the event arrives,
then waits for the strict check with `replication.WaitReplayed` on one connection and reads the row on another, the
way a connection pool would.

## Topology

`docker-compose.yml` starts PostgreSQL 17 as primary (`5437`) plus two streaming standbys, `standby1` (`5438`) and
`standby2` (`5439`), with `synchronous_standby_names = 'ANY 2 (standby1, standby2)'`: every commit waits for both
standbys to **flush** the WAL. `standby2` also runs `recovery_min_apply_delay = 3s`, which delays exactly the
**apply of commit records**. Every event therefore arrives while `standby2` has the row's WAL on disk but the
transaction is not yet applied. That is the state of any lagging replica, frozen for three seconds so you can look at it.

```bash
cd example/replica-read
docker compose up -d --wait
go run .                      # guard on, check and read on standby2
```

`./lab.sh` runs the five steps below unattended in about a minute. Inserts go to the primary:

```bash
docker compose exec primary psql -U cdc_user -d cdc_db
```

## 1. Two sync standbys, guard on: still not applied

```sql
INSERT INTO orders (note) VALUES ('plain');
```

```
INFO at event id=1 commitLSN=0/5051A90 replay=0/5051A90 loose(>=)=true strict(>)=false visible=false
INFO VISIBLE after strict check id=1 waited=2.992s
```

The primary acknowledged the commit (both standbys flushed it), the visibility guard certified it on the primary, the
event arrived, and `standby2` is still behind: it has applied the row's own WAL record (`replay == commitLSN`) but
not the commit record, so the row is invisible. `synchronous_commit = on` is a flush guarantee, not an apply
guarantee. The strict check waits, and the read on the same standby afterwards finds the row. (If `standby2` still
has older delayed commits queued, `replay` shows an even lower position and `loose(>=)` is `false` too.)

## 2. `synchronous_commit = remote_apply` + guard: nothing left to wait for

```sql
SET synchronous_commit = remote_apply; INSERT INTO orders (note) VALUES ('remote_apply');
```

```
INFO at event id=2 commitLSN=0/5006048 replay=0/5014138 loose(>=)=true strict(>)=true visible=true
INFO VISIBLE after strict check id=2 waited=1ms
```

With `remote_apply` the primary's commit waits until both standbys have **applied** the transaction, and the guard holds
the event until that commit is visible on the primary. The consumer finds the row on the standby without waiting. This
is the sync-rep corollary from the main README taken to the apply level: `remote_apply` on the producer, every replica
you read from listed in `synchronous_standby_names`, guard `closed`. The INSERT itself now takes three seconds, which is
the cost.

## 3. `remote_apply` without the guard: the boundary

```bash
go run . -guard=false
```

```
INFO at event id=3 commitLSN=0/50504C8 replay=0/50504C8 loose(>=)=true strict(>)=false visible=false
INFO VISIBLE after strict check id=3 waited=3.029s
```

The walsender sends the change as soon as the commit record is flushed, while the primary's backend is still waiting for
the standbys. So without the guard the event outruns `remote_apply`. Look at the LSNs: `replay == commitLSN`.
`pg_last_wal_replay_lsn()` is the end of the last replayed record, the one just before the commit record, and it equals
the commit record's start. `>=` says yes, the row is not there. `>` waits until the commit record itself is applied.

## 4. Check on one standby, read on the other: the production symptom

```bash
go run . -check-port 5438 -read-port 5439
```

```sql
RESET synchronous_commit;   -- SET is session-scoped: a psql session left on remote_apply from step 2 reproduces step 2, not this
INSERT INTO orders (note) VALUES ('split');
```

```
INFO at event id=4 commitLSN=0/5050588 replay=0/50505B8 loose(>=)=true strict(>)=true visible=false
WARN NOT FOUND after strict check passed: check and read hit different standbys id=4 checkPort=5438 readPort=5439
```

`standby1` has no apply delay, so the strict check passes at once. The read goes to `standby2`, which has not applied
the commit yet. The check is only meaningful for the server that answered it. Any replica endpoint that spreads
connections over several standbys (HAProxy, pgbouncer with several hosts, a DNS name with several addresses, a pool in
the application) turns a correct check into this.

## 5. `visibilityGuard.replicas`: the library waits, the handler does not

```bash
go run . -replicas 127.0.0.1:5439
```

```
INFO at event id=5 commitLSN=0/5053648 replay=0/5053678 loose(>=)=true strict(>)=true visible=true
INFO VISIBLE after strict check id=5 waited=3ms
```

With `standby2` listed, the connector itself polls it (same strict rule, its own connection) before dispatching, so
the event arrives about three seconds after the commit with the row already there; `WaitReplayed` in the handler
returns at once. The guarantee covers the listed standbys only: `-replicas 127.0.0.1:5438 -read-port 5439` reproduces
step 4, because `standby2` is not in the list.

## The rule, in full

1. Compare with `>`, never `>=`, and poll: PostgreSQL 17 and 18 have no server-side wait for a replay position
   (`pg_wal_replay_wait` is not in either release).
2. Run the check and the read on the **same server**: same connection, or one transaction, or a pool pinned to one
   standby.
3. Check first, read second, as separate statements, in `READ COMMITTED`. A `REPEATABLE READ` transaction keeps the
   snapshot of its first statement; a single statement such as `SELECT … WHERE id = $1 AND pg_last_wal_replay_lsn() > $2`
   takes its snapshot before the function runs.
4. `pg_last_wal_replay_lsn()` is `NULL` on a primary. Getting `NULL` from the "replica" endpoint means the endpoint
   routed you to the leader (or a promoted standby); do not read it as "already applied". `WaitReplayed` returns
   `ErrNotStandby` for it.
5. `synchronous_commit = on` means flushed, not applied. Only `remote_apply` on the producer, with the replica you read
   from listed in `synchronous_standby_names`, removes the need for the check, and Patroni may drop a standby from that
   list at any time.
6. `visibilityGuard.replicas` moves the wait into the connector for the standbys you list; anything you read from
   must be in that list, and pooled endpoints do not belong in it.

## Cleanup

```bash
docker compose down -v
```
