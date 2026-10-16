# MR review: `feature/partition-publication`

A condensed copy of this document appears at the top of the Cursor plan **Partition publication guide** (`partition_publication_guide_73c0617e.plan.md` under your Cursor plans directory).

Branches/commits reviewed: `7b480b6` (feat: add publish_via_partition_root param for partitioned tables) and `cec7d7c` (adding example).

---

## Summary

The change adds `publication.Table.Partitioned`, wires it into `CREATE PUBLICATION ... WITH (publish = '...', publish_via_partition_root = <bool>)`, extends `Tables.Diff`, documents the flag in the README, adds **integration tests**, and adds an **example** with Docker Compose + `init.sql` + `main.go`. The core behavior matches PostgreSQL semantics: when `publish_via_partition_root` is true, logical decoding reports the **root** relation name for changes that physically land on **child** partitions.

---

## Commit 1 — `7b480b6` feat: add publish_via_partition_root param for partitioned tables

### Strengths

- **Small, focused API**: One boolean on `Table` is easy to explain and map to YAML/JSON (`partitioned`).
- **Correct use of Postgres**: `publish_via_partition_root` is a **publication-level** option; deriving a single `hasPartitionedTable` from “any table has `Partitioned: true`” matches what one `CREATE PUBLICATION` can express.
- `**Tables.Diff` updated**: Including `v.Partitioned != t.Partitioned` avoids silently ignoring config changes when comparing desired vs actual table lists (used from replica-identity flow).
- **README**: New row documents the flag and ties it to `publish_via_partition_root`.
- `**TestPublicationPartitionedTable`**: Strong test — real partitioned table, inserts targeting **logical** `orders`, asserts payload **and** `msg.TableName == "orders"` for both `orders_us` and `orders_eu` routing. That is the user-visible guarantee that matters.

### Issues and gaps

1. **Test name vs what `TestPublicationPartitionedCreateAndInfo` asserts**
  The comment says it verifies `publish_via_partition_root = true` **via `Info()`**, but `decodePublicationInfoResult` in `pq/publication/publication.go` never reads `pg_publication.pubviaroot`. The test only checks `info.Name` and `info.Tables` non-empty.  
   **Suggestion**: Either rename/narrow the test comment, or extend `infoQuery` / decode to surface `pubviaroot`, or add a raw SQL assertion in the test, e.g. `SELECT pubviaroot FROM pg_publication WHERE pubname = $1`.
2. **“Per-table” flag vs publication-wide effect**
  README says the flag sets `publish_via_partition_root` when creating the publication; it does **not** spell out that **one** `true` turns it on for the **entire** publication (all listed tables). Worth one sentence in README to avoid surprises when mixing partitioned and non-partitioned tables in the same publication.
3. **Existing publication + config change**
  `Create()` returns early if the publication already exists and does not run `ALTER PUBLICATION ... SET (publish_via_partition_root = ...)`. Flipping `Partitioned` from false to true later will **not** update an existing publication.  
   **Suggestion**: Document this limitation, or add an explicit path (manual `ALTER` or connector logic) — out of scope for v1 is fine if documented.
4. **Typo in comment** (`pq/publication/table.go`)
  `publicaiton` → `publication`.
5. **Postgres version**
  `publish_via_partition_root` exists from **PostgreSQL 13** onward. If the project supports PG 10–12 for other features, call out that this option requires a supported version (README compatibility table might need a note).
6. `**Tables.Diff` map key** (pre-existing, but related)
  Key is `t.Name + t.ReplicaIdentity` without **schema**; two same-named tables in different schemas could collide. Not introduced by this MR, but `Partitioned` in the value does not fix that.
7. `**TestPublicationPartitionedCreateAndInfo` cleanup**
  Uses `fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName)` with a constant name — fine here; for consistency with SQL safety elsewhere, `QuoteIdentifier` would match the rest of the codebase style.

---

## Commit 2 — `cec7d7c` adding example

### Strengths

- **Clear narrative in code**: Comments in `main.go` explain default false vs true and what appears in `msg.TableName`.
- **Schema matches the story**: Range-partitioned `test_events` with `test_events_1` … `test_events_10` and a commented bulk `INSERT` that spans days is a good manual demo.
- **Compose**: `wal_level=logical`, replication slots/senders tuned for local dev.

### Issues

1. **Broken healthcheck** (`example/partitioned-table-mapping/docker-compose.yml`)
  Uses `pg_isready -U testuser -d testdb` but the service defines `cdc_user` / `cdc_db`. Healthcheck will **always fail**; orchestration that waits on “healthy” will hang.  
   **Fix**: e.g. `pg_isready -U cdc_user -d cdc_db`.
2. **No newline at end of file** on `docker-compose.yml` (minor; some style checkers flag it).
3. **Example discoverability**
  README “Examples” list does not link `example/partitioned-table-mapping`. Consider adding one bullet under Examples.
4. **No local `go.mod`**
  Same pattern as some other examples — running from repo root with `go run ./example/partitioned-table-mapping` is fine; a one-line note in a comment or README avoids confusion.
5. **Comment typos in `main.go`**
  `isnt` → `isn't`, `the the` → `the`, `thought` → `though` (optional polish).
6. `**psql` comment**
  DSN uses port **5432** in the comment but the example uses host port **5433** — easy to fix in the comment for copy-paste.

---

## Security / correctness

- No new risky SQL with untrusted input in these commits.
- Integration tests use fixed object names and cleanup paths; reasonable for CI.

---

## Verdict

**Approve with nits**: The feature is implemented in the right place (`createQuery`), tests meaningfully validate **CDC message table names**, and the example illustrates the feature. Before merge, **fix the docker-compose healthcheck**, **align `TestPublicationPartitionedCreateAndInfo` with what it actually checks** (or assert `pubviaroot`), and **fix the comment typo** on `Table`. README tweaks for publication-wide behavior and PG version would make the MR easier to operate safely in production.