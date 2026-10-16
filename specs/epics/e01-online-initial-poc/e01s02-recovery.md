# e01s02: resume multi-chunk capture without stale target state

## 1. Business narrative

A completed chunk survives a process restart. Incomplete work repeats safely without skipping rows or overwriting newer target values.

## 2. Metadata

Type: feature. Risk: P0. Status: planned. Complexity: 8 relative BCPs. Depends on: e01s01.

## 3. Actors

The coordinator owns cursor progress. The source preserves replayable WAL. The qualified target rejects old or duplicate effects. The caller responds to terminal errors.

## 4. Preconditions

The one-window tracer passes. The original slot and metadata are present. Source and target use durable test profiles with independent storage.

## 5. Main flow

Complete keyset windows sequentially. Persist each completed interval and HIGH source checkpoint together. On restart, validate the binding, restore progress, and use fresh attempts for unfinished intervals.

## 6. Alternative and error flows

Stop on sink failure, absent WAL, incompatible source/slot state, owner-lock loss, or rejected attempt transitions. Never silently recreate a slot or reuse volatile rows.

## 7. Requirements

ADDED: Finite per-table keyset progress and explicit source checkpoints.

ADDED: LOW ACK ceilings, durable completion barriers, and conditional attempt transitions.

ADDED: Process-kill recovery under the test target's identity/order contract.

## 8. Data

Use the metadata records in the [plan](implementation-plan.md). Keep the stable slot binding across restart. Change the attempt UUID for a new read, not the identity of replayed CDC.

## 9. Interface contract

Preserve complete images and comparable output-order tokens. A READ's HIGH token is not its source commit LSN. A handler timeout is unresolved delivery, not success.

## 10. State and ownership

Completion and abandonment are mutually exclusive conditional transitions. Late callbacks cannot complete a successor attempt. Owner-lock loss cancels source, read, and delivery work.

## 11. Security

Validate source/metadata identity and configuration fingerprints. Use conditional updates with affected-row checks. Preserve metadata from other connectors and attempts.

## 12. Compatibility

Do not reuse or delete legacy snapshot jobs. Reject online resnapshot/mode migration in the POC rather than converting an incomplete job implicitly.

## 13. Observability

Expose last completed key, ACK ceiling, durable checkpoint, resumed/abandoned attempts, and terminal recovery reason. Keep row keys out of metric labels.

## 14. Resource constraints

No whole-job transaction or complete CDC backlog in memory. WAL retention during a sink outage is expected. Keep bounded attempt history and steady-state checkpoint writes.

## 15. Abstractions and dependencies

Progress transactions: couple cursor and source position. Order tokens: separate replay correctness from transport uniqueness. Process fixture: verifies loss of volatile state. No new production dependencies.

## 16. Implementation steps

1. Complete metadata transactions and ACK ceilings → verify: task 1 in [ledger](e01s02-tasks.yaml).
2. Extend the tracer to finite multi-table keyset progress → verify: task 2.
3. Add hard-kill and lost-response recovery → verify: task 3.
4. Add fail-closed ownership/source/read/delivery behavior → verify: task 4.

## 17. Acceptance criteria

- Given unfinished output or an active window, when a later heartbeat completes, then source ACK cannot skip the obligation. `SC-e01s02-P0-01`.
- Given a metadata failure, when completion commits or rolls back, then cursor, attempt status, and source checkpoint agree. `SC-e01s02-P0-02`.
- Given a hard kill at each window barrier, when the connector restarts, then completed intervals remain complete and unfinished work is safe to repeat. `SC-e01s02-P0-03`.
- Given a lost sink response or delayed old READ, when newer data reaches the target, then replay cannot regress it or resurrect a delete. `SC-e01s02-P0-04`.
- Given an abandoned attempt, when its late completion arrives, then the current attempt remains unchanged. `SC-e01s02-P0-05`.
- Given empty/sparse/minimum-key/all-conflict data, when keyset scanning runs, then it terminates with complete coverage. `SC-e01s02-P1-06`.
- Given inserts outside the remaining scan interval, when capture runs, then CDC supplies their coverage. `SC-e01s02-P0-07`.
- Given read/delivery errors or incompatible slot/owner state, when the connector handles them, then it fails before unsafe progress. `SC-e01s02-P0-08`, `SC-e01s02-P0-09`, `SC-e01s02-P0-10`.

## 18. Out of scope

Exactly-once arbitrary side effects, loss of both source and target durable storage, automatic failover, indefinite dedup retention policy, and adapter implementation.

## 19. Verification evidence

Future tests: `TestOnlineCheckpointAtomicity`, `TestOnlineInitialProgress`, `TestOnlineInitialRecovery`, `TestOnlineInitialFailureSafety`. Logs must record fault barriers and expected versus actual metadata/source/target state.

## 20. Definition of done

All crash points and fail-closed cases pass against durable fixtures. Required CDC events remain present, target state is exact, and completed cursors never outrun safe output. Performance and wider compatibility remain e01s03 gates.
