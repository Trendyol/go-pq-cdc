# e01s01: demonstrate one online chunk through a durable target

## 1. Business narrative

A library user starts an initial snapshot without waiting for the entire table before CDC can progress. This first slice demonstrates one window only.

## 2. Metadata

Type: feature. Risk: P0. Status: planned. Complexity: 8 relative BCPs, not elapsed time. Depends on: none.

## 3. Actors

The caller owns configuration and durable-handler behavior. The connector owns source capture, the window, and progress. The test target owns atomic state application.

## 4. Preconditions

Select an approved feature baseline. PostgreSQL 16 and a separate target are available. The user opted into `initial`/`online`, protocol 1, and eligible tables.

## 5. Main flow

Start capture, establish LOW with visibility, read one chunk, emit HIGH, reconcile CDC, deliver surviving READs, and persist completion before ACK. Follow the [plan](implementation-plan.md).

## 6. Alternative and error flows

Reject an ineligible table, publication, protocol, or constructor. A failed source read or sink call returns an error without successful window completion.

## 7. Requirements

ADDED: An explicit online initial strategy with an additive error-returning API.

ADDED: Published LOW/HIGH control updates with exact identity and commit validation.

ADDED: One-window reconciliation into a separate durable, order-aware test target.

Existing configurations and callback contracts remain unchanged.

## 8. Data

Create the four metadata/control tables defined by the plan. New source metadata/control tables never contain row payloads. The test target stores event identities, order tokens, rows, and tombstones.

## 9. Interface contract

The online handler returns nil only after durable completion. Calls are serialized. Existing NewConnector rejects online and points to the new constructor.

## 10. State and ownership

One source owner and one window. Replication and SQL connections have distinct owners. Register a unique attempt before LOW. Keep source identity and slot binding explicit.

## 11. Security

Validate exact publication membership and permissions. Quote identifiers and bind values. Reject broad publications and RLS. No new security finding is acceptable in these paths.

## 12. Compatibility

Omitted strategy selects the legacy consistent path. Keep `snapshot_only`, `never`, default protocol selection, and old metadata intact.

## 13. Observability

Record active phase, current table/attempt, read duration, bytes, and source/target errors. Never log payloads or credentials by default.

## 14. Resource constraints

Enforce row, byte, transaction, queue, and time limits from the plan. Feedback must remain responsive during a bounded SELECT or full queue.

## 15. Abstractions and dependencies

The online connector isolates lifecycle and completion semantics from legacy callers. The committed reader preserves transaction boundaries. The window state machine makes interleavings testable. The metadata store owns conditional progress transitions. Existing pgx/testify/testcontainers are `[OK]`. No production dependency is added.

## 16. Implementation steps

1. Build the separate durable target and isolated POC fixtures → verify: task 1 in [ledger](e01s01-tasks.yaml).
2. Add strategy/configuration and additive constructor validation → verify: task 2.
3. Implement publication controls and bounded committed source events → verify: task 3.
4. Connect one LOW/read/HIGH window to the target → verify: task 4.

## 17. Acceptance criteria

- Given omitted strategy, when old constructors run, then legacy defaults and behavior remain unchanged. `SC-e01s01-P1-01`.
- Given a target commit with a lost response, when its event repeats, then identity/version rules prevent duplicate or stale application. `SC-e01s01-P0-02`.
- Given marker traffic, when its transaction commits, then only the matching internal control is applied. `SC-e01s01-P0-03`.
- Given UPDATE and DELETE inside a window, when HIGH arrives, then their READs disappear and the real CDC events remain. `SC-e01s01-P0-04`.
- Given a partially decoded transaction, when COMMIT is absent, then no online transaction-end checkpoint is released. `SC-e01s01-P0-05`.
- Given unsupported visibility, publication, or schema configuration, when startup validates it, then capture fails before unsafe progress. `SC-e01s01-P0-06`.

## 18. Out of scope

Multi-chunk restart qualification, sink adapters, protocol 2, HA, filters, parallel windows, full source outbox, and production rollout.

## 19. Verification evidence

Future tests: `TestOnlineTargetContract`, `TestOnlineStrategyValidation`, `TestOnlineControlProtocol`, `TestOnlineCommittedEnvelope`, and `TestOnlineInitialSmoke`. [Test plan](../../tech-architecture/e01-TEST_PLAN_LATEST.md). Presence guards in the ledger prevent zero-test success.

## 20. Definition of done

The tracer produces exact target state under an in-window update/delete schedule. Control rows never reach the user handler. All four tasks pass with logs. The slice remains experimental and does not claim restart qualification until e01s02/e01s03 complete.
