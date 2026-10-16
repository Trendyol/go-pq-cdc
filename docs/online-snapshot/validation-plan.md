# Online snapshots: validation and qualification plan

**Status:** proposed acceptance plan, not completed test evidence. **Date:** 2026-10-13.

**Purpose:** turn [RFC](design-rfc.md) invariants C1–C9 into falsifiable release gates. Core, database, adapter, and SRE owners must sign their assigned evidence. Test names in this plan are proposed unless [Evidence](evidence.md) lists them as existing.

## 1. Test oracle

Use a persistent reference target keyed by source table and primary key. Record source operations, transaction identities, commit order, and each logical output sequence. Distinguish logical emissions from transport retries.

After bootstrap, stop writes and commit a final marker. Drain required delivery through that marker. Compare complete target keys and values against the source. Include deleted keys in the comparison. For live comparisons, first define a coordinated source view and fence.

Require zero missing keys, zero unexpected keys, zero value differences, and zero stale application. Counts alone can hide one missing row and one duplicate. Transport duplicates are permitted only when the destination handles them without stale effects.

A CDC row can supply snapshot coverage. Do not require one READ per source row. For ordinary CDC, preserve all required committed events. Do not suppress real CDC merely because a snapshot row conflicts.

## 2. Deterministic model and fault scheduler

Implement a pure merger model before a database connector. Begin with three keys, two chunks, two source transactions, a persistent fake stage, and a fake destination. Enumerate legal schedules. Then add seeded random schedules with minimized counterexamples.

The scheduler controls these boundaries:

| Boundary | Controlled action |
|---|---|
| Source commit | Publish a committed event or abort a streamed transaction |
| Visibility | Delay SQL visibility independently of decoded commit order |
| LOW | Commit and observe the exact marker after earlier relevant visibility checks |
| SELECT | Acquire the statement snapshot and return results as separate actions |
| HIGH | Commit and observe the exact marker after result capture |
| Stage | Commit an append, fail it, or lose its success reply |
| Destination | Complete selected items, fail items, delay writes, or lose responses |
| Progress | Persist cursor/delivery state or fail after the write |
| Restart | Destroy all volatile state and rebuild from durable records |
| Ownership | Replace the generation while the former owner still has pending work |

Never model a crash as graceful `Close`. Never retain the touched-key set or SELECT result across a simulated process death unless the specified store persists it.

## 3. Traceability matrix

| Test ID | Required scenario | Invariants | Pass evidence |
|---|---|---|---|
| T01 | Update after LOW, before SELECT result arrives | C1, C2 | CDC remains, stale READ absent |
| T02 | Delete during the read, then delete/reinsert | C1–C3 | No resurrection, later insertion wins |
| T03 | Every selected row conflicts | C1, C6 | Cursor advances from selected keys, no endless repeat |
| T04 | Empty table, empty final page, minimum and sparse integer keys | C1, C6 | Complete coverage and finite completion |
| T05 | Transaction begins before HIGH, commits afterward | C2, C4 | Its committed change follows surviving READs |
| T06 | Streamed marker/data abort and subtransaction rollback | C4 | No aborted output or progress |
| T07 | Same key in another table, duplicate marker, obsolete generation or abandoned attempt | C2, C8 | Reject late SELECT/HIGH results, including same-generation retries |
| T08 | Later ACK and heartbeat while earlier output is unfinished | C5 | Safe frontier cannot cross the gap |
| T09 | Crash at every transition in section 2 | C1–C7 | Recovery preserves exact state and safe cursor |
| T10 | Stage commit succeeds but reply or source ACK is lost | C5–C7 | Resolve ambiguous attempt completion before retry, no reordered stage output |
| T11 | Destination write succeeds but response is lost | C2, C3, C7 | Ordered retry or version rejection prevents stale effects |
| T12 | Old owner sends late metadata and destination writes | C7, C8 | New owner cannot be corrupted by stale work |
| T13 | Earlier commit stays SQL-invisible while a later one is visible, including an ACK/crash/restart schedule | C1, C2, C5 | Visibility gates survive restart through the durable-cursor precondition |
| T14 | Large unchanged TOAST, NULL transitions, UPDATE before baseline | C1 | Complete correct image or explicit unsupported failure |
| T15 | PK mutation, TRUNCATE, DDL, filters, RLS with an unchanged hidden row | C1–C6 | Explicit failure before unsafe ACK, no silent partial support |
| T16 | Maximum chunk, transaction, touched-key, and stage budgets | C5, C9 | Bounded resources, no unsafe eviction or drop |
| T17 | Continuous input without receive-idle gaps | C5, C9 | Feedback progresses on an explicit bounded cadence |
| T18 | Walsender loss, blackholed connection, stalled feedback write | C5, C7, C9 | Bounded shutdown/recovery and no skipped obligations |
| T19 | Lost slot or missing stage state | C5, C7 | Fail closed with an actionable incident, no silent slot recreation |
| T20 | Retained legacy modes and public callbacks | Compatibility | Existing configurations and semantics remain supported |

Run the model under `go test -race`. Add fuzz targets for marker framing, lengths, streamed-XID handling, tuple presence, and key encoding. Malformed input must not panic or allocate unbounded memory. Distinguish intentionally ignored messages from failed decoding of required events.

## 4. PostgreSQL integration matrix

Qualify PostgreSQL 16 and each additional major advertised by the release. Pin exact test images or digests in release evidence. Do not interpret PostgreSQL 16+ as automatic certification of future releases.

| Dimension | Required profiles |
|---|---|
| Wire protocol | Protocol 1 plus logical messages. Protocol 2 only after its stream/abort/buffer gates pass |
| Durability | Functional fast profile and separate `fsync=on` crash profile with durable volumes |
| Authentication | Restricted replication/SELECT/marker role, not only superuser |
| Transactions | Small commits, large commits, rollback, savepoint rollback, long-lived source transactions |
| Read timing | Explicit SELECT barriers with writes inside the actual window |
| Visibility | Synchronous-replication fixture or equivalent deterministic server instrumentation for T13 |
| Lifecycle | Hard connector kill, PostgreSQL crash/restart, reconnect, stage restart |
| Retention | Small controlled WAL allowance, forced checkpoints, retained-WAL observation and slot loss |
| Identity | Startup identity check and fail-closed behavior on identity/timeline mismatch |

Verify that the intended wire path actually occurred. A protocol-2 test that never observes streamed messages does not cover stream handling. Test helper processes must dispatch their special mode before normal test setup.

Automatic promotion continuity is outside the first profile. If later supported, test actual promotion with correctly synchronized slot state. A test that only reads a failover flag is insufficient.

## 5. Adapter contract suite

Apply the same fault scenarios to both adapters. Use real Kafka and Elasticsearch for final qualification, not only mocks.

| Contract | Kafka evidence | Elasticsearch evidence |
|---|---|---|
| Durable success | Approved broker ACK/replication policy and confirmed producer write | Every required bulk item succeeds under approved durability settings |
| Stable identity | Same source identity uses a stable partition route | Same source identity uses a stable document ID and routing |
| Ordering | READ and CDC retain order across batches and retries | Concurrent bulks cannot apply older same-key work after newer work |
| Partial failure | Failed records remain unresolved | Failed items remain unresolved even with an HTTP success |
| Replay | Restarted producer/consumer handles stable output IDs and sequences | Stale retries and deletes respect the version/tombstone policy |
| Expansion | All outputs from one source event participate in completion | All actions from one source event participate in completion |
| Lifecycle | Producer batch loop starts before online capture can block | Bulk loop starts before online capture can block |

A custom error handler that logs and returns MUST NOT turn failure into success. Oversized required records and mapping failures must remain unresolved or trigger an explicit operator decision. A dead-letter policy changes the zero-loss-to-target contract and requires separate approval.

## 6. Capacity experiment

Measure the legacy mode and the candidate under the same database size, row widths, source resources, write mix, and destination limits. Record source latency before the experiment. Use an isolated database, not a production source.

Include sustained inserts, updates, deletes, a hot-key workload, wide TOAST values, long transactions, and a bounded destination outage. Run for at least the target snapshot duration plus the recovery interval. Record peak usage, not only memory after garbage collection.

| Budget | Value required before P4 | Measurement |
|---|---|---|
| Source workload | Writes/s, WAL bytes/s, transaction-size distribution | Source workload recorder and WAL deltas |
| Snapshot workload | Rows, bytes, chunk row/byte caps, maximum query duration | Query timing and scanner counters |
| CDC service level | Allowed percentile latency and feedback age | Source commit-to-durable-output timing |
| Memory and disk | Heap/RSS peak, stage capacity and disk reserve | Process and store telemetry |
| Source impact | Allowed query latency, CPU and I/O increase | Comparison against baseline |
| Outage tolerance | Maximum sink and stage outage durations | Controlled fault injection |
| Recovery | Maximum catch-up time after the outage | Durable destination checkpoint lag |
| Retention | Allowed source WAL and transaction horizon | `restart_lsn`, `confirmed_flush_lsn`, slot and backend views |

All values are **TBD by database/SRE owners**. No production benchmark is available. Stage ingress must keep up with measured CDC plus READ output during normal operation. Catch-up capacity must exceed new arrival rate after an outage.

Use the source's total retained WAL, not only captured table activity. `active=true` and advancing `confirmed_flush_lsn` are not sufficient: long transactions can hold `restart_lsn` back.

## 7. Release evidence and gates

| Gate | Required artifact | Sign-off |
|---|---|---|
| G0 Baseline | Exact legacy assertions, ACK-gap reproduction, source revisions | Core |
| G1 Algorithm | Model schedules, seeds, minimized counterexamples, C1–C9 results | Core + database |
| G2 Source protocol | Versioned wire traces and real LOW/read/HIGH/abort tests | Database |
| G3 Recovery | Hard-crash traces, durable state before/after, replay results | Core + SRE |
| G4 Destinations | Kafka and Elasticsearch partial-failure/replay reports | Both adapter owners |
| G5 Capacity/security | Signed budgets, measured peaks, restricted-role results | Database + SRE |
| G6 Rollout | Canary comparison, alert tests, rollback and rebuild drill | Service owner + SRE |

Archive the exact code and configuration revisions, database image digests, test commands, logs, workload seeds, and expected/actual state differences. Preserve failed cases as regression fixtures. Require independent review of the recovery state machine before release.

## 8. Verification commands and next action

Existing baseline commands, not online qualification:

```bash
go test -race -count=1 ./...
go vet ./...
(cd integration_test && POSTGRES_VERSION=16-alpine go test -race -count=1 -p=1 ./...)
```

The integration module is separate. Root `go test ./...` does not run it. The full integration command in this section is a proposed gate. The full suite was not executed during this assessment.

Next action: Assign owners for T08, T09, and T13. Implement those falsifiers and the pure model before wiring the online mode into `connector.Start`.

**Document verify:** `grep -c '^| T[0-9][0-9]' docs/online-snapshot/validation-plan.md` must report `20`. This verifies traceability entries, not executed tests.
