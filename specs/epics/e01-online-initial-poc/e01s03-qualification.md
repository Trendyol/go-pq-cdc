# e01s03: qualify bounded operation and legacy compatibility

## 1. Business narrative

A maintainer can run a repeatable POC, inspect its correctness and capacity evidence, and decide whether to proceed with sink-adapter work.

## 2. Metadata

Type: qualification. Risk: P0/P1. Status: planned. Complexity: 5 relative BCPs. Depends on: e01s02.

## 3. Actors

Core maintainer, database reviewer, and test runner. The test target remains a fixture, not a production adapter.

## 4. Preconditions

The one-window and recovery slices pass. The implementation baseline is reconciled. Required source/target fixtures and the capacity environment are available.

## 5. Main flow

Run model, visibility, full-image, resource, and regression gates. Execute the opt-in workload. Produce an evidence report and a runnable demonstration with explicit limits.

## 6. Alternative and error flows

Missing proof or an unavailable environment is blocked, not passing. A correctness violation fails the POC. A missed workload rate is non-qualifying capacity evidence.

## 7. Requirements

ADDED: Deterministic adverse schedules and exact-state qualification.

ADDED: An opt-in load comparison and source-retention measurements.

ADDED: A reproducible example and documented sink contract.

## 8. Data

Record code/configuration revision, test image identifiers, workload seeds, actual source rate, peak resources, checkpoint trajectories, and state differences. Never include credentials or production payloads.

## 9. Interface contract

Demonstrate the real online handler contract with a durable target. Do not use a logging-only handler that returns nil as if data were recoverable.

## 10. State and ownership

Use deterministic barriers to force visibility and ordering schedules. Use isolated resources and explicit teardown. Do not run overlapping source-owner tests against the same slot.

## 11. Security

Run restricted-role and metadata-leakage tests. Control metadata must not enter business output. Reject broad publications and unsupported table policies.

## 12. Compatibility

Run root tests and the full separate integration module. Preserve existing protocol-2 rollback tests and the legacy no-walsender intent. Do not skip known baseline failures to claim success.

## 13. Observability

Publish raw measurements with peak memory, retained WAL, read latency, feedback age, and actual rates. Explain the difference between source ACK and destination completeness.

## 14. Resource constraints

Bound query, transaction, queue, and handler work. Demonstrate confirmation progress before overall snapshot completion. A stalled sink must retain safe source progress rather than acknowledge missing data.

## 15. Abstractions and dependencies

Reference model: makes ordering failures reproducible. Workload harness: separates correctness from throughput. Example target: teaches the durability contract without adding a production adapter dependency.

## 16. Implementation steps

1. Complete model, visibility, and full-image adverse tests → verify: task 1 in [ledger](e01s03-tasks.yaml).
2. Add the bounded workload and feedback/retention experiment → verify: task 2.
3. Add the runnable example, compatibility evidence, and POC verdict → verify: task 3.

## 17. Acceptance criteria

- Given decoded commits that remain SQL-invisible, when a new LOW/read follows checkpoint or restart, then the visibility precondition still holds. `SC-e01s03-P0-01`.
- Given post-HIGH updates, cross-table keys, and stale controls, when the model explores legal schedules, then no precedence invariant fails. `SC-e01s03-P0-02`.
- Given unchanged TOAST and NULL values, when a READ loses to CDC, then the target receives a complete correct image. `SC-e01s03-P0-03`.
- Given unsupported runtime operations, when detected, then the connector stops before unsafe ACK progress. `SC-e01s03-P0-04`.
- Given full queues and sustained input, when feedback and cancellation run, then their configured deadlines hold. `SC-e01s03-P1-05`.
- Given ongoing writes and multiple windows, when the POC runs, then confirmed source progress advances before the whole snapshot finishes. `SC-e01s03-P1-06`.
- Given metadata/marker writes, when capture runs, then there is no payload persistence on source or business-output feedback loop. `SC-e01s03-P1-07`.
- Given legacy configurations and callbacks, when regressions run, then their contracts remain supported. `SC-e01s03-P0-08`.

## 18. Out of scope

Production readiness, promised throughput, automatic HA, multiple windows, protocol 2 in online mode, or enabling online mode in existing adapters.

## 19. Verification evidence

Future tests: `TestOnlineModel`, `TestOnlineVisibilityBarrier`, `TestOnlineFullImages`, `TestOnlineInitialCapacity`, and `TestOnlineInitialDemo`. The test plan defines required records and verdicts.

## 20. Definition of done

All correctness and compatibility gates pass. Required visibility evidence exists. The workload meets its declared inputs or the result remains unqualified. The example is reproducible, limitations are explicit, and no production release claim is made.
