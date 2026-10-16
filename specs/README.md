# One-window online initial-snapshot POC

**Status:** approved restricted-profile scenarios and final automated root/full PG16/matched-capacity gates pass; human UAT and independent acceptance review are unconfirmed. See [POC result](verifications/e01-POC-RESULT.md), [self-audit](verifications/e01-self-audit.md), and exact command evidence. No production readiness or adapter enablement is claimed.

## Start here

- [Implementation plan](epics/e01-online-initial-poc/implementation-plan.md): runtime flow, configuration, metadata, delivery, recovery, and scope limits.
- [Testing plan](tech-architecture/e01-TEST_PLAN_LATEST.md): 24 scenarios, deterministic fixtures, crash points, capacity experiment, and acceptance gates.
- [Impact assessment](IMPACT_LATEST.md): affected contracts, compatibility risks, and existing test limitations.
- [Scope](product/SCOPE_LATEST.yaml): the confirmed user decisions and explicit exclusions.

## Execution order

| Slice | Specification | Task ledger |
|---|---|---|
| One-window tracer | [e01s01](epics/e01-online-initial-poc/e01s01-single-window.md) | [4 tasks](epics/e01-online-initial-poc/e01s01-tasks.yaml) |
| Multi-chunk recovery | [e01s02](epics/e01-online-initial-poc/e01s02-recovery.md) | [4 tasks](epics/e01-online-initial-poc/e01s02-tasks.yaml) |
| Qualification and demo | [e01s03](epics/e01-online-initial-poc/e01s03-qualification.md) | [3 tasks](epics/e01-online-initial-poc/e01s03-tasks.yaml) |

Task ledgers distinguish command success from complete scenario coverage. Current story/task statuses are passing for the approved business type/array profile through transparent shared final-gate evidence. Exactly one new full PG16 suite and one explicit matched-capacity command ran after the last code fix. Presence guards and shell syntax were checked for all 14 task/story requirements; redundant composites were not rerun. Earlier timeouts and failed array fixtures remain archived, not passing current evidence.

## Differences from the earlier RFC

The POC uses `snapshot.mode: initial` plus `snapshot.strategy: online`, not a new mode. It uses a published watermark table, not logical messages. Source tables contain metadata only, not durable output staging. A separate PostgreSQL test target supplies durable, order-aware delivery.

The first profile uses one window and protocol 1. Kafka/Elasticsearch adapters, parallel windows, and full feature parity are deferred. A sink outage can still retain source WAL. The earlier reports and PDFs remain unchanged as broader design context.

## Planning checks

YAML parsing, three story manifests, 11 task entries, 14 shell-command syntax checks, and complete mapping of 24 test scenarios passed. Local document links were checked. Those were planning-only checks. All 14 runtime verify commands have now executed; consult the result rather than treating this older paragraph as runtime evidence.

The packaged `plan-consistency-check.sh` did not complete because its Python environment lacks PyYAML. Node's installed YAML parser supplied the cross-artifact checks instead. Do not describe that packaged checker as passing. Restore its prerequisites before using it as an implementation gate.

## Next action

Independently review the restricted-profile implementation and confirm human UAT before parent acceptance/further profile or adapter work. Preserve the approved reconnect archive and unrelated local work. Automated POC success is not production safety, full PostgreSQL type coverage or adapter enablement.

**Verify this package:** `test -s specs/epics/e01-online-initial-poc/implementation-plan.md && test -s specs/tech-architecture/e01-TEST_PLAN_LATEST.md`.
