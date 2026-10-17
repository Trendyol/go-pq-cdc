# Online feature/state admission without a major allowlist

## Policy

There is no PostgreSQL-major allowlist. CI's five PG15/16/17 image profiles are test targets, not permitted-major entries. An unqualified major is not rejected solely by its version. Missing required capabilities still produce errors; removing the allowlist does not claim compatibility with every historical or future release.

Removed the preliminary connector version query, major-policy calls, OnlineMajorPolicy implementation, and unsupported-version error aliases. Those symbols were introduced only in the uncommitted changes.

## Retained guards

- Primary, source system identifier, database, and timeline fencing.
- Fresh SQL snapshot/XID visibility, malformed response validation, and timeouts.
- Explicit metadata provisioning and permanent-table/fsync durability checks.
- Publication membership, schema/profile restrictions, and schema fingerprints.
- Logical pgoutput slot, database binding, non-temporary/inactive slot checks.
- Version-specific two-phase, lost-WAL, conflicting, invalidation, failover, and synced state rejection.
- Resume binding, confirmed LSN versus durable checkpoint, and configuration fingerprint validation.
- Existing durable-handler, LOW/HIGH merge, metadata-before-ACK, and resource behavior.

## Version-selected slot fields

| Field | Selected from |
|---|---|
| wal_status | PG13 |
| two_phase | PG14 |
| conflicting | PG16 |
| invalidation_reason, failover, synced | PG17 |
| two_phase_at | PG18 |

The PG18 scheduled two-phase check is restored because PG18 is no longer blocked by policy. No automatic slot adoption/drop/recreation is introduced. PG18 idle-timeout invalidation is rejected by non-null invalidation_reason.

Official [PG13 slot](https://www.postgresql.org/docs/13/view-pg-replication-slots.html) and [PG14 slot](https://www.postgresql.org/docs/14/view-pg-replication-slots.html) documentation was inspected for the earlier field boundaries. Other required catalog features remain checked by the existing SQL paths, which can fail on older releases.

## Verification

Updated visibility regression failed against the old allowlist for PG14/18/19, then passed after removal. Startup wire tests advertise PG14/18/19 and prove they reach the real fsync durability guard without permitting publication/slot/job mutations. Slot query tests cover PG12–19 column boundaries; they do not qualify those releases end to end.

Root race tests, root vet, integration-module vet, and git diff --check passed.

Each live profile executed 29 top-level online tests with race detection, count=1, serial package execution, and a 30-minute timeout. Capacity was explicitly excluded.

| Image | Result | Duration |
|---|---|---|
| 15-alpine | PASS | 120.572s |
| 15.7-alpine | PASS | 118.912s |
| 16-alpine | PASS | 121.000s |
| 16.3-alpine | PASS | 122.270s |
| 17-alpine | PASS | 121.941s |
| 18-alpine | PASS, additional non-CI regression | 123.634s |

Raw logs in `e02-feature-admission-evidence` include commands, actual image/server versions and digests, protocol 1, and exit codes. PG18 is not added to CI. Its run verifies the current existing online suite, not every PG18-only state transition; scheduled two_phase_at is covered by query tests, not independently induced live here.

Active LSP checks covered ten changed Go files: no errors; three heuristic warnings incorrectly classify channel Context.Done calls as WaitGroup.Done. No WaitGroup code changed.

## Limits

The full legacy integration suite, capacity, hosted CI, protocol-2 parity, production readiness, and arbitrary older/future releases are not qualified by this task. The destructive opt-in catalog matrix was not run against caller-owned clusters. Historical PG15–17 and PG16–18 results remain unchanged and describe their earlier tested trees, not current admission policy. Existing caller-owned containers and local workspace configuration were preserved.
