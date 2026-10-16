# PR #110 Review: `feat: enable custom query condition in snapshot mode`

**PR:** https://github.com/Trendyol/go-pq-cdc/pull/110
**Changes:** +1036 / -20 across 11 files
**Scope:** Adds `queryCondition` to snapshot config (global + per-table), threads it through all query builders, with unit tests, integration tests, docs, and example.

---

## 1. SQL Operator Precedence Bug — Data Duplication across Chunks

**Severity: Critical correctness bug**

`andCondition` concatenates without parenthesizing the user condition:

```go
func andCondition(existing, extra string) string {
    // ...
    return fmt.Sprintf("%s AND %s", existing, extra)
}
```

If a user configures `queryCondition: "status = 'a' OR status = 'b'"`, the generated SQL becomes:

```sql
-- integer_range chunk:
WHERE id >= 1 AND id <= 1000 AND status = 'a' OR status = 'b'
```

SQL `AND` binds tighter than `OR`. This evaluates as:

```sql
WHERE (id >= 1 AND id <= 1000 AND status = 'a') OR (status = 'b')
```

**Every row with `status = 'b'` in the entire table is returned for every chunk.** Result: massive data duplication, wrong row counts, broken snapshot integrity.

Same issue in CTID block queries — chunk isolation is completely broken.

**Fix:**

```go
return fmt.Sprintf("%s AND (%s)", existing, extra)
```

The tests don't catch this because none use `OR` conditions. Need a test like:

```go
t.Run("OR in condition is correctly parenthesized", func(t *testing.T) {
    chunk := &Chunk{...integer range...}
    q := s.buildChunkQuery(chunk, "id", []string{"id"}, "status = 'a' OR status = 'b'")
    assert.Contains(t, q, "AND (status = 'a' OR status = 'b')")
})
```

---

## 2. SQL Injection — No Input Validation (CWE-89)

**Severity: High (DREAD ~7.6)**

`queryCondition` is a **structural SQL fragment** (WHERE clause), not a value. This is the hardest injection category to defend — parameterized queries (`$1`) only work for values, not for SQL structure like column names, ORDER BY, or WHERE clause fragments.

### Attack Surface Trace

```
YAML config
  → SnapshotConfig.QueryCondition       (config/config.go:291)
  → publication.Table.QueryCondition     (publication/table.go:42)
  → getQueryCondition()                  (coordinator.go:460)
  → andCondition() / fmt.Sprintf         (coordinator.go:486, 507, 527, 536, 546, 564)
  → conn.Exec() — pgconn simple protocol (helpers.go:26)
```

**Critical detail:** `conn.Exec()` uses pgconn's **simple query protocol**. Unlike the extended protocol, simple protocol supports **multiple statements separated by `;`**. This means:

```yaml
snapshot:
  queryCondition: "1=1; DROP TABLE users; --"
```

...executes as **three separate statements** in a single roundtrip. No prepared statement boundary protects you.

### Trust Boundary Analysis

- Primary trust boundary: operator-controlled YAML config (not end-user HTTP input), so exploitability is lower
- However: multi-tenant platforms may generate config programmatically from less-trusted sources
- Additional attack vectors beyond `;`: `--` line comments, `/* */` block comments, `$$ ... $$` PL/pgSQL blocks, `COPY TO PROGRAM`
- **No validation exists anywhere** — `Table.Validate()` checks `Name`, `ReplicaIdentity`, `ReplicaIdentityIndex`, `Columns`, but skips `QueryCondition` entirely
- `SnapshotConfig.Validate()` also has no check on `QueryCondition`

### DREAD Score Breakdown

| Factor | Score | Rationale |
|--------|-------|-----------|
| Damage | 10 | Full DB control — DROP, DELETE, COPY TO PROGRAM (RCE) |
| Reproducibility | 10 | Deterministic — same config always triggers |
| Exploitability | 4 | Requires config access (operator-level, not HTTP) |
| Affected Users | 8 | All users of the feature |
| Discoverability | 6 | Visible in config docs, obvious interpolation pattern |
| **Average** | **7.6** | **High** |

### Fix: Defense-in-Depth (Three Independent Layers)

#### Layer 1 — Static Validation at Config Parse Time

No DB connection needed. Catches the most dangerous patterns at startup:

```go
package config

import (
    "fmt"
    "regexp"
    "strings"
)

var (
    // DDL/DCL/DML keywords that have no place in a read-only WHERE clause.
    // Word-boundary anchored to avoid false positives (e.g., column named "updated").
    unsafeKeywordPattern = regexp.MustCompile(
        `(?i)\b(DROP|ALTER|CREATE|TRUNCATE|INSERT|UPDATE|DELETE|GRANT|REVOKE|COPY|EXECUTE|DO)\b`,
    )

    // Characters/sequences that enable multi-statement or comment-based attacks.
    unsafeSequences = []struct {
        seq    string
        reason string
    }{
        {";", "statement separator — enables multi-statement execution"},
        {"--", "line comment — can truncate intended query"},
        {"/*", "block comment — can alter query parsing"},
        {"$$", "dollar-quoted string — can inject PL/pgSQL blocks"},
    }
)

func ValidateQueryCondition(cond string) error {
    if cond == "" {
        return nil
    }

    for _, s := range unsafeSequences {
        if strings.Contains(cond, s.seq) {
            return fmt.Errorf(
                "queryCondition contains %q (%s): %q",
                s.seq, s.reason, cond,
            )
        }
    }

    if match := unsafeKeywordPattern.FindString(cond); match != "" {
        return fmt.Errorf(
            "queryCondition contains disallowed keyword %q: %q",
            match, cond,
        )
    }

    return nil
}
```

#### Layer 2 — PostgreSQL Parser Validation at Startup

The strongest layer. Delegates validation to the only authority that truly knows PostgreSQL syntax — PostgreSQL itself:

```go
func (s *Snapshotter) validateQueryConditionSQL(
    ctx context.Context, conn pq.Connection, condition string,
) error {
    // PREPARE parses but does not execute.
    // Multi-statement payloads like "1=1; DROP TABLE x" fail here
    // because PREPARE accepts exactly one statement.
    query := fmt.Sprintf(
        "PREPARE _qc_validate AS SELECT 1 WHERE (%s)",
        condition,
    )
    if err := pq.ExecSQL(ctx, conn, query); err != nil {
        return fmt.Errorf(
            "queryCondition is not valid SQL: %q: %w",
            condition, err,
        )
    }
    _ = pq.ExecSQL(ctx, conn, "DEALLOCATE _qc_validate")
    return nil
}
```

Why this works:
- `PREPARE` accepts exactly **one** SQL statement — multi-statement injection fails at protocol level
- PostgreSQL parses the expression for syntactic validity — catches malformed SQL
- Wrapping in `SELECT 1 WHERE (...)` ensures condition is parsed as a boolean expression
- No data is read or modified — pure syntax check
- Parentheses around `%s` also solve Finding #1 (operator precedence)

#### Layer 3 — Always Parenthesize at Interpolation Point

Limits blast radius even if layers 1-2 are bypassed. Also fixes Finding #1:

```go
func andCondition(existing, extra string) string {
    if existing == "" {
        return extra
    }
    if extra == "" {
        return existing
    }
    return fmt.Sprintf("%s AND (%s)", existing, extra)
}
```

### Where to Wire Validation

```go
// config/config.go — SnapshotConfig.Validate()
func (s *SnapshotConfig) Validate() error {
    // ... existing checks ...
    if err := ValidateQueryCondition(s.QueryCondition); err != nil {
        return err
    }
    return nil
}

// publication/table.go — Table.Validate()
func (tc Table) Validate() error {
    // ... existing checks ...
    if err := config.ValidateQueryCondition(tc.QueryCondition); err != nil {
        return fmt.Errorf("table %q: %w", tc.Name, err)
    }
    return nil
}

// pq/snapshot/coordinator.go — during snapshot init, after connection is available
func (s *Snapshotter) validateAllQueryConditions(ctx context.Context, conn pq.Connection) error {
    if s.config.QueryCondition != "" {
        if err := s.validateQueryConditionSQL(ctx, conn, s.config.QueryCondition); err != nil {
            return err
        }
    }
    for _, t := range s.tables {
        if t.QueryCondition != "" {
            if err := s.validateQueryConditionSQL(ctx, conn, t.QueryCondition); err != nil {
                return fmt.Errorf("table %s.%s: %w", t.Schema, t.Name, err)
            }
        }
    }
    return nil
}
```

### Defense Layer Summary

| Layer | What | When | Stops |
|-------|------|------|-------|
| Static validation | Blocklist keywords + dangerous sequences | Config parse (no DB) | `; DROP TABLE`, `-- comment`, `$$ plpgsql $$`, DDL keywords |
| PostgreSQL parser | `PREPARE ... SELECT 1 WHERE (cond)` | Startup (with DB conn) | Multi-statement, invalid syntax, non-boolean expressions |
| Parenthesization | `AND (%s)` in `andCondition` | Query build time | Operator precedence exploits, blast radius containment |

Three independent layers. Each catches what the others miss. No single point of failure.

### DREAD After Fix

| Factor | Before | After |
|--------|--------|-------|
| Damage | 10 | 3 (constrained to WHERE expression) |
| Reproducibility | 10 | 3 (must bypass 3 layers) |
| Exploitability | 4 | 2 (keyword + parser + parentheses) |
| Affected Users | 8 | 2 (only misconfigured) |
| Discoverability | 6 | 2 (attacker must know internal parsing) |
| **DREAD Score** | **7.6 (High)** | **2.4 (Low)** |

### Documentation

Add to YAML config comments:

```yaml
snapshot:
  # queryCondition: SQL WHERE clause fragment applied to all snapshot queries.
  # OPERATOR-TRUSTED: This value is interpolated into SQL. It must be a valid
  # boolean expression (e.g., "status = 'active' AND created_at > '2024-01-01'").
  # Validated at startup — rejects statement separators, DDL keywords,
  # and PostgreSQL-invalid syntax.
  queryCondition: "status = 'active'"
```

---

## 3. Override Semantics — Test Comment Contradicts Code

**Severity: Medium (correctness/UX confusion)**

Integration test `TestSnapshotQueryConditionPerTable`, lines 831-833:

```go
// Note: Global condition (created_at >= '2024-01-01') should also be applied
// But since all rows have created_at = '2024-02-01', all should pass global condition
// So we expect 2 rows (id=1 and id=3, both have deleted_at IS NULL)
```

This comment says global **should also be applied** alongside per-table. But `getQueryCondition` does a **full replacement** — per-table wins entirely, global is ignored:

```go
func (s *Snapshotter) getQueryCondition(tableSchema, tableName string) string {
    // ...
    if tblSchema == normalizedSchema && table.Name == tableName && table.QueryCondition != "" {
        return table.QueryCondition // global is never combined
    }
    return s.config.QueryCondition
}
```

The test passes by coincidence — all rows happen to match the global condition regardless. If test data included a row with `created_at = '2023-01-01'`, the test would still pass, proving the global condition is **not applied**.

**Fix:** Either:
- (a) Remove the misleading comment, assert exactly 2 rows (not `GreaterOrEqual`), and document "per-table replaces global"
- (b) If combining is the desired behavior, change `getQueryCondition` to use `andCondition(perTable, global)`

The docs say "fully overrides" so (a) is the intended behavior — fix the comment and tighten the assertion.

---

## 4. `buildOffsetQuery` — Duplicated Sprintf, Inconsistent Pattern

**Severity: Low (DRY violation)**

`buildIntegerRangeQuery` and `buildCTIDBlockQuery` (bounded branches) use `andCondition` to compose WHERE clauses. `buildOffsetQuery` duplicates the entire Sprintf:

```go
func (s *Snapshotter) buildOffsetQuery(chunk *Chunk, orderByClause string, queryCondition string) string {
    cols := selectSnapshotColumns(chunk.TableColumns)
    if queryCondition != "" {
        return fmt.Sprintf(
            "SELECT %s FROM %s.%s WHERE %s ORDER BY %s LIMIT %d OFFSET %d",
            // ... 7 args
        )
    }
    return fmt.Sprintf(
        "SELECT %s FROM %s.%s ORDER BY %s LIMIT %d OFFSET %d",
        // ... 6 args
    )
}
```

Same issue in `buildCTIDBlockQuery`'s `BlockStart == nil` branch — inline `if/else` instead of `andCondition`.

**Fix:** Unify with the same pattern:

```go
func (s *Snapshotter) buildOffsetQuery(chunk *Chunk, orderByClause, queryCondition string) string {
    cols := selectSnapshotColumns(chunk.TableColumns)
    where := ""
    if queryCondition != "" {
        where = "WHERE (" + queryCondition + ") "
    }
    return fmt.Sprintf("SELECT %s FROM %s.%s %sORDER BY %s LIMIT %d OFFSET %d",
        cols, chunk.TableSchema, chunk.TableName, where,
        orderByClause, chunk.ChunkSize, chunk.ChunkStart)
}
```

---

## 5. `getTableRawCountWithConn` — Redundant Sprintf

**Severity: Low**

```go
query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, table)
if queryCondition != "" {
    query = fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE %s", schema, table, queryCondition)
}
```

First `Sprintf` is wasted when condition exists.

**Fix:**

```go
query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, table)
if queryCondition != "" {
    query += " WHERE (" + queryCondition + ")"
}
```

---

## 6. Config Struct Field Reorder — Unnecessary Diff

**Severity: Nit**

`Heartbeat` field moved within `Config` struct. Creates diff noise for no functional change. Likely a linter auto-fix (fieldalignment). Not blocking, but clutters the diff.

---

## 7. Integration Tests — Timing-Based Collection

**Severity: Nit**

```go
timeout := time.After(5 * time.Second)
for !snapshotCompleted {
    select {
    case msg := <-messageCh: ...
    case <-timeout:
        snapshotCompleted = true
    }
}
```

Always waits the full 5 seconds. The snapshot pipeline emits `SnapshotEventTypeEnd` — listen for it to break early. Makes tests faster and deterministic.

---

## Summary Table

| # | Severity     | Issue                                                                                    | Action Required        |
|---|-------------|------------------------------------------------------------------------------------------|------------------------|
| 1 | **Critical** | `andCondition` missing parentheses — OR conditions break chunk isolation, data duplication | Wrap: `AND (%s)` + add OR test case |
| 2 | **High**     | SQL injection — no validation on `queryCondition` (CWE-89, DREAD 7.6)                    | 3-layer defense: static blocklist + PREPARE parser validation + parenthesization |
| 3 | **Medium**   | Test comment contradicts override semantics; assertion too loose (`GreaterOrEqual`)        | Fix comment, tighten to `assert.Len` |
| 4 | **Low**      | `buildOffsetQuery` / CTID nil-branch duplicate Sprintf instead of using `andCondition`    | Unify pattern |
| 5 | **Low**      | `getTableRawCountWithConn` redundant Sprintf                                             | Append WHERE clause |
| 6 | **Nit**      | Config struct field reorder creates diff noise                                            | Revert or separate commit |
| 7 | **Nit**      | Integration tests wait full 5s instead of breaking on SnapshotEnd event                   | Use event-driven collection |

**Items 1 and 2 should be addressed before merge.** Item 1 is a silent data corruption bug that will hit any user with `OR` in their condition.

---

## Go Code Review Findings

### 8. Error Handling: `processChunk` Swallows Handler Error

**Severity: Medium (error-handling anti-pattern)**

`coordinator.go:446` — handler error return is discarded:

```go
_ = handler(&format.Snapshot{
    EventType: format.SnapshotEventTypeData,
    // ...
})
```

If `handler` returns an error (e.g., downstream write failure), it's silently ignored. Every subsequent row in the chunk continues processing against a potentially failed downstream. The `_` has no justifying comment.

**Fix:** Propagate or at minimum log:

```go
if err := handler(&format.Snapshot{...}); err != nil {
    return 0, fmt.Errorf("snapshot handler failed at row %d: %w", i, err)
}
```

### 9. Missing `QueryCondition` Validation in `Table.Validate()`

**Severity: Medium (defense gap) — subsumed by Finding #2's fix**

`publication/table.go:48-70` — `Table.Validate()` checks `Name`, `ReplicaIdentity`, `ReplicaIdentityIndex`, and `Columns`, but does not validate `QueryCondition`. Since this field is interpolated raw into SQL (see finding #2), the validation boundary is incomplete.

**Fix:** See Finding #2's "Where to Wire Validation" section — `ValidateQueryCondition` should be called in both `Table.Validate()` and `SnapshotConfig.Validate()`.

### 10. `getQueryCondition` Linear Scan per Chunk

**Severity: Low (performance)**

`coordinator.go:460-477` — iterates `s.tables` on every call. Called once per chunk during `processChunk` and once during `getTableRawCountWithConn`. With thousands of chunks across many tables, this is O(chunks * tables).

Not a problem for typical deployments (< 50 tables), but could be optimized with a pre-built map during `Snapshotter` initialization:

```go
type Snapshotter struct {
    // ...
    queryConditions map[string]string // "schema.table" -> condition
}
```

### 11. `QueryCondition` Missing from `Table.Diff()`

**Severity: Low (consistency)**

`publication/table.go:88-103` — `Tables.Diff()` compares `ReplicaIdentity`, `ReplicaIdentityIndex`, `Columns`, and `Partitioned`, but not `QueryCondition`. If a user changes only the query condition, `Diff()` won't detect it. Whether this matters depends on how `Diff()` is used (publication recreation logic), but it's an inconsistency with the other per-table fields.

### 12. Go Style: Grouped Parameter Declaration

**Severity: Nit**

`coordinator.go:556`:

```go
func (s *Snapshotter) buildOffsetQuery(chunk *Chunk, orderByClause string, queryCondition string) string {
```

Per Go conventions, consecutive parameters of the same type should be grouped:

```go
func (s *Snapshotter) buildOffsetQuery(chunk *Chunk, orderByClause, queryCondition string) string {
```

Same applies to `buildIntegerRangeQuery` (though it has `pkColumns []string` between them, so it doesn't apply there).

### 13. Dead Code: Commented-Out Query

**Severity: Nit**

`coordinator.go:1196`:

```go
// query := fmt.Sprintf("SELECT reltuples::bigint FROM pg_class WHERE oid = '%s.%s'::regclass", schema, table)
```

Commented-out code should be removed. Git history preserves it.

---

## Updated Summary Table

| # | Severity     | Category | Issue |
|---|-------------|----------|-------|
| 1 | **Critical** | Correctness | `andCondition` missing parentheses — OR conditions break chunk isolation |
| 2 | **High**     | Security | SQL injection — no validation on `queryCondition` (CWE-89, DREAD 7.6) — 3-layer fix |
| 3 | **Medium**   | Testing | Test comment contradicts override semantics; assertion too loose |
| 8 | **Medium**   | Error Handling | `processChunk` discards handler error with bare `_ =` |
| 9 | **Medium**   | Validation | `Table.Validate()` doesn't check `QueryCondition` |
| 4 | **Low**      | DRY | `buildOffsetQuery` / CTID nil-branch duplicate Sprintf |
| 5 | **Low**      | DRY | `getTableRawCountWithConn` redundant Sprintf |
| 10 | **Low**     | Performance | `getQueryCondition` linear scan per chunk |
| 11 | **Low**     | Consistency | `Table.Diff()` ignores `QueryCondition` |
| 6 | **Nit**      | Style | Config struct field reorder creates diff noise |
| 7 | **Nit**      | Testing | Integration tests wait full 5s instead of event-driven |
| 12 | **Nit**     | Style | Consecutive string params not grouped |
| 13 | **Nit**     | Dead Code | Commented-out reltuples query |

**Must fix before merge:** #1 (data corruption), #2 (SQL injection — DREAD 7.6), #8 (silent error swallowing).
**Should fix:** #3, #9 (now subsumed by #2's fix — wire `ValidateQueryCondition` into both `Validate()` methods).
**Nice to have:** #4, #5, #10, #11, #12, #13.
