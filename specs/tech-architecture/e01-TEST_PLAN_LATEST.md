# Test architecture: e01 online initial-snapshot POC

**Status:** implemented restricted-profile scenarios; final automated gates passed 2026-10-15; human UAT unconfirmed. See [execution result](../verifications/e01-POC-RESULT.md). **Risk:** P0 data-loss and stale-state boundaries. **Scope:** [implementation plan](../epics/e01-online-initial-poc/implementation-plan.md).

## 1. Oracle and failure contract

The source is authoritative. The separate durable target applies each event in a transaction with an identity ledger and a per-key order token. Deletes preserve tombstone order. Verify final visible rows, absent rows, full field values, and legal per-key transitions.

At a comparison fence, stop source writers, commit a final control marker, wait for confirmed delivery through it, and compare complete source/target state. Never compare against an independently moving source. Verify required CDC event identities separately from final state so a missing event cannot hide behind a later update.

Steady-state merge order must not regress. After restart, raw transport can repeat older events. The qualified target must not apply them as stale updates. Nil from the handler means its durable transaction committed. A failed or unknown response must not release later work.

## 2. Scenario matrix

| Scenario ID | Behavior | Level | Planned test/file |
|---|---|---|---|
| SC-e01s01-P1-01 | Omitted strategy preserves defaults; online requires initial and protocol 1 | Unit | `config/online_strategy_test.go`, `TestOnlineStrategyValidation` |
| SC-e01s01-P0-02 | Durable target atomically applies identity, value, version and tombstone | Integration | `integration_test/online_target_test.go`, `TestOnlineTargetContract` |
| SC-e01s01-P0-03 | Marker is committed, correctly identified, and hidden from business output | Unit + integration | `pq/replication/online_test.go`, `TestOnlineControlProtocol` |
| SC-e01s01-P0-04 | LOW → SELECT → update/delete → HIGH preserves precedence | Model + integration | `online_initial_smoke_test.go`, `TestOnlineInitialSmoke` |
| SC-e01s01-P0-05 | One transaction's rows cannot escape without its end boundary | Unit | `pq/replication/online_test.go`, `TestOnlineCommittedEnvelope` |
| SC-e01s01-P0-06 | Missing marker membership, broad publication, RLS, key/filter/schema restriction fail early | Integration | `online_control_test.go`, `TestOnlineControlProtocol` |
| SC-e01s02-P0-01 | Earlier pending delivery or open LOW prevents later heartbeat/HIGH ACK | Model + integration | `online_checkpoint_test.go`, `TestOnlineCheckpointAtomicity` |
| SC-e01s02-P0-02 | Cursor/status/source checkpoint commit together or not at all | Integration | `online_checkpoint_test.go`, `TestOnlineCheckpointAtomicity` |
| SC-e01s02-P0-03 | Fresh attempt after hard crash preserves completed ranges | Process E2E | `online_initial_recovery_test.go`, `TestOnlineInitialRecovery` |
| SC-e01s02-P0-04 | Lost sink reply, stale READ replay and replayed DELETE preserve target state | Process E2E | `online_initial_recovery_test.go`, `TestOnlineInitialRecovery` |
| SC-e01s02-P0-05 | Delayed old attempt completion cannot mutate current progress | Model + integration | `online_failure_test.go`, `TestOnlineInitialFailureSafety` |
| SC-e01s02-P1-06 | Empty table, minimum/negative keys, sparse keys and all-conflict chunks finish | Integration | `online_initial_progress_test.go`, `TestOnlineInitialProgress` |
| SC-e01s02-P0-07 | Inserts behind cursor or beyond upper bound remain covered by CDC | Integration | `online_initial_progress_test.go`, `TestOnlineInitialProgress` |
| SC-e01s02-P0-08 | Handler failure, rows.Err, query timeout and byte cap cannot complete a chunk | Integration | `online_failure_test.go`, `TestOnlineInitialFailureSafety` |
| SC-e01s02-P0-09 | Missing slot, advanced confirmation, missing metadata and changed source identity fail closed | Integration | `online_failure_test.go`, `TestOnlineInitialFailureSafety` |
| SC-e01s02-P0-10 | Second owner is rejected; owner-lock loss stops further work | Integration | `online_failure_test.go`, `TestOnlineInitialFailureSafety` |
| SC-e01s03-P0-01 | Decoded commit precedes SQL visibility, including checkpoint/restart | Model + source fixture | `online_model_test.go` and `online_visibility_test.go`, `TestOnlineVisibilityBarrier` |
| SC-e01s03-P0-02 | Post-HIGH transaction, same key in another table, duplicate/stale controls | Model + integration | `pq/snapshot/online/model_test.go`, `TestOnlineModel` |
| SC-e01s03-P0-03 | FULL identity supplies complete unchanged TOAST and NULL semantics | Integration | `online_image_test.go`, `TestOnlineFullImages` |
| SC-e01s03-P0-04 | Unsupported runtime TRUNCATE or detected schema/key change stops before unsafe ACK | Integration | `online_image_test.go`, `TestOnlineFullImages` |
| SC-e01s03-P1-05 | Continuous input and full queue do not starve safe feedback or cancellation | Unit + integration | `online_capacity_test.go`, `TestOnlineInitialCapacity` |
| SC-e01s03-P1-06 | Sustained writes and several completed windows advance source confirmation before job completion | Load integration | `online_capacity_test.go`, `TestOnlineInitialCapacity` |
| SC-e01s03-P1-07 | New metadata/control tables contain no row payload; no metadata/heartbeat feedback loop | Integration | `online_control_test.go`, `TestOnlineControlProtocol` |
| SC-e01s03-P0-08 | Existing initial/only/never modes, snapshots, and protocol-2 behavior remain supported | Regression | Root and full integration suites |

### Approved value-profile qualification supplement (2026-10-15)

`TestOnlineTypeProfileStartup` rejects business domain/custom/composite/enum/range, numeric, temporal, bytea, UUID and declared multidimensional columns before a slot/job or delivery exists. The qualified scalars are bool, int2/int4/int8, text/varchar/bpchar, float4/float8 and JSON/JSONB; their qualified arrays are one-dimensional with lower bound 1. `TestOnlineQualifiedArrays` exercises all qualified scalar/array families, empty/NULL arrays, NULL elements, quoted braces/commas/quotes and exact large JSON numbers for READ/CDC. Actual multidimensional/nondefault-bound values reject before READ/envelope delivery or ACK; catalog dimensions alone are not a runtime constraint. Internal UUID control decoding remains supported without admitting business UUID columns. Unit `TestOnlineLosslessValues` rejects unknown OIDs and invalid actual shapes. Legacy codecs are unchanged; additional types are deferred.

The exact oracle uses source whole-row `to_jsonb` with an alias selected not to collide with any column, lossless JSON decoding and exact rational number comparison. Its independent >2^53 negative control distinguishes true precision loss; no JSONEq/float64 oracle or arbitrary temporal/bytea string coercion is used. Temporal/bytea/numeric business types are not qualified by this POC.

Historical failed array fixtures in `type-profile-green-first.log` omitted metadata provisioning and never reached value decoding. They are not passing evidence. Provisioned fixtures then exposed a column/whole-row-alias oracle collision, corrected before the final focused/full/capacity gates.

All source and target fixtures must use exact keys and values. Assertions must not accept “most” rows. Capacity results must distinguish a failed correctness invariant from an unachieved workload target.

## 3. Fixture design

### Pure state machine

Use injected events, transaction boundaries, handler completions, source positions, and storage outcomes. Enumerate small schedules with three keys, two chunks, and two transactions. Persist only explicit durable state between simulated restarts. Use seeded random schedules and preserve minimized failure traces.

Expose internal test barriers, not production sleeps, at LOW observation, read start/result, HIGH observation, first READ completion, metadata commit, and feedback send. Include cancellation and late completion of the same attempt.

### Separate durable databases

Use independent PostgreSQL containers/clusters for source and target. Two databases in the same cluster still share WAL and contaminate the source-load measurement. Keep `fsync=on` and durable storage for recovery tests.

Retain the existing fast integration profile. POC fixtures opt into their own database setup with explicit settings and small publications. Test a non-superuser role with separately provisioned metadata/publication permissions. Keep source progress tables outside the publication.

### Durable target

Maintain a projection table with full row data, a delete flag, and `(order_lsn, phase, ordinal)`. Maintain an event-identity ledger in the same transaction. Reject stale order tokens before updating the projection. Preserve tombstones and dedup records throughout the run.

Expose faults before commit, after commit/before response, and delayed old-attempt writes. Verify that a retry either resolves the existing event or safely applies a newer token. An unversioned upsert fixture is a negative control, not a valid POC sink.

### Process helper

Dispatch a dedicated `CDC_ONLINE_HELPER` mode at the start of `TestMain`, before ordinary test container setup. Pass isolated source/target configuration to the child test process. Use bounded pipes and explicit barrier acknowledgements. Kill the process without calling Close, then restart with the same source metadata and target.

This is a test helper, not an agent workflow. Do not reuse or silently fix the unrelated untracked disconnect helper. Reconcile that baseline test separately before the full-suite gate.

### Visibility test

The pure model must reproduce commit-before-SQL-visibility deterministically. A source fixture must also exercise the visibility guard with a held transaction and an independently visible later transaction. Use controlled synchronous-replication timing or explicit test instrumentation rather than assuming a sleep proves the race.

If the real-source delayed-visibility fixture is unavailable, mark the POC unqualified for that condition. Do not treat the model alone as server-behavior evidence.

## 4. Required crash points

| Barrier | Assertions after restart |
|---|---|
| After LOW checkpoint, before SELECT | No skipped interval; new attempt ID; earlier output not lost |
| After SELECT, before HIGH | Volatile rows are discarded; fresh read reconstructs coverage |
| After HIGH, before first READ | CDC replay is safe; cursor stays at the previous interval |
| After one READ succeeds | Old partial READs cannot overwrite newer target changes |
| After last READ, before metadata | Repeat is safe; no false completed interval |
| After metadata commits, before reply/ACK | Reload detects completion and avoids restarting from an unsafe boundary |
| After source confirmation | Every acknowledged obligation remains durable in the test target |

Combine these with updates, deletes, delete/reinsert, and a sink response lost after commit. Pause an old request until the new attempt applies a newer value, then release it. Require version/tombstone protection to reject the old effect.

## 5. Capacity experiment and report

Use a reproducible opt-in test, not a timing-sensitive ordinary unit test. Suggested starting fixture: 10,000 rows with approximately 1 KiB payloads, 100-row chunks, 100 transactions/s for 60 seconds. Record the achieved rate. The settings are experimental inputs, not promised throughput.

Record maximum chunk/transaction/queue bytes, heap/RSS, source WAL retained from `restart_lsn`, confirmed lag, read latency, feedback age, and target latency. Include at least two completed windows during active writes. Then stop writes, wait for a terminal fence, compare state, and verify backlog drains within a fixture deadline.

Compare legacy `consistent` and new `online` strategies with the same workload on separate runs. Record actual peak retention rather than asserting an unexplained improvement ratio. A deliberately stalled sink must hold ACK progress: this proves safety and the expected limitation, not a failure to isolate outages.

Use the configured row/byte caps as deterministic gates. Set a source disk/WAL safety budget before the run and stop before exhausting it. A missed workload rate or unavailable environment is a non-qualifying result, never a passing benchmark.

## 6. Acceptance commands

Each task ledger names its exact future test and first checks that the test exists. A command that selects zero tests must not count as evidence.

Final gates after implementation:

```bash
go test -race -count=1 ./...
go vet ./...
(cd integration_test && POSTGRES_VERSION=16-alpine go test -race -count=1 -p=1 -timeout=20m ./...)
(cd integration_test && CDC_ONLINE_CAPACITY=1 POSTGRES_VERSION=16-alpine \
  go test -race -count=1 -p=1 -timeout=10m -run '^TestOnlineInitialCapacity$' ./...)
```

The first integration command includes the POC's durable fixtures but keeps legacy defaults for existing tests. The capacity command must fail rather than skip when explicitly requested. Archive source revision, configuration, image identifiers, seeds, logs, actual rates, and state differences.

## 7. POC verdict

- Pass: All correctness and compatibility gates pass, required source-visibility evidence exists, and the capacity report states a qualifying workload.
- Blocked: Missing environment, incomplete test selection, missing proof, or unresolved baseline failure.
- Fail: Any gap, stale target application, unsafe ACK, false chunk completion, or unsupported configuration accepted silently.

A restricted-profile automated pass permits further review/qualification, not mission-critical deployment or adapter enablement. The original planning exercise executed no runtime tests; current execution evidence is recorded separately in the result and final gate logs. Human UAT remains unconfirmed.
