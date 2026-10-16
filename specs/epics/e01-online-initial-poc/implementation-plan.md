# POC implementation plan: one-window online initial snapshots

**Status:** implemented and automated POC gates checked; human UAT unconfirmed. **Plan date:** 2026-10-13; **execution evidence:** 2026-10-15 ([result](../../verifications/e01-POC-RESULT.md)). **Scope approval:** one window, metadata-only source tables, published watermarks, core library plus durable test sink.

**Baseline:** `4a7e9b094a19935591b691cf8de38546a98b69d0` plus the existing local changes. Do not overwrite or fold the reconnect work into this feature. This plan narrows the earlier [RFC](../../../docs/online-snapshot/design-rfc.md). Its durable-stage and logical-message proposals are not part of this POC.

## 1. Goal and explicit limits

Implement `snapshot.mode: initial` with a new `snapshot.strategy: online`. Keep `consistent` as the default strategy and preserve the current exported-snapshot behavior.

Prove that the source can deliver CDC between short snapshot chunks, with correct READ/CDC precedence and safe restarts. Use one active window globally across the selected tables. This is not a global T0 snapshot or production certification.

The source database stores progress and marker metadata only. Selected row payloads stay in bounded memory until the sink confirms delivery. A slow or unavailable sink therefore retains WAL. No automatic fallback to a whole-table snapshot or slot recreation is allowed.

The first profile requires PostgreSQL 16, protocol 1, immutable integer-family primary keys, an explicit table publication, and verified complete row images. Require INSERT, UPDATE, DELETE, and TRUNCATE publication visibility so unsupported destructive operations can be detected. Reject partitions, inheritance, RLS, row filters, custom snapshot conditions, publication column lists, generated columns, mutable keys, protocol 2, two-phase decoding, `resnapshot`, and `FOR ALL TABLES`/schema-wide publications. Qualify additional PostgreSQL majors separately.

**Approved execution restriction (2026-10-15):** qualified business columns are bool, int2/int4/int8, text/varchar/bpchar, float4/float8 and JSON/JSONB, plus explicitly tested one-dimensional arrays with lower bound 1, including empty/NULL arrays. Runtime array shape is checked by codec metadata, not brace counting; declared dimensions are only an early catalog check. Domains, custom/composite/enum/range types, numeric, temporal, bytea and business UUID columns are unqualified and rejected at startup before capture. Reserved watermark UUID fields retain an internal exact control codec, not business UUID permission. JSON numbers are lossless; float non-finites use the approved strings Infinity/-Infinity/NaN. Legacy codecs are unchanged. Additional value/type qualification is deferred, not full PostgreSQL type coverage.

Validate every captured business table, not only the snapshot-selected subset. Freeze application schema during a successful POC run. Check schema fingerprints before reads and before releasing READ output. Detected drift fails closed. This is not a general DDL-capture feature.

## 2. Proposed configuration and public contract

```yaml
slot:
  protoVersion: 1       # Explicit: the current library default is 2.
snapshot:
  enabled: true
  mode: initial
  strategy: online     # New. Omitted strategy remains consistent.
  chunkSize: 1000       # Example POC setting, not a production recommendation.
  online:
    queryTimeout: 5s
    deliveryTimeout: 30s
    chunkBytes: 4194304
    transactionBytes: 16777216
    queueBytes: 33554432
    checkpointInterval: 1s
    markerInterval: 5s
```

The numeric limits are initial test settings. Validate positive values and require the queue budget to admit at least one maximum-sized transaction. Publish measured results before selecting defaults. Count encoded bytes and measure actual heap overhead separately.

Add `NewOnlineConnector(ctx, cfg, handler)` and an error-returning `Run(ctx)` lifecycle. Use `OnlineHandler func(context.Context, OnlineEvent) error`. Nil means durable completion, not enqueue success. The connector invokes only one handler at a time. A failed or timed-out call stops forward delivery and prevents unsafe ACK progress.

Handlers must honor cancellation. Go cannot forcibly terminate an arbitrary callback that ignores its context. Test cancellation with a compliant durable handler and do not promise bounded callback termination otherwise.

Keep `ListenerFunc`, `Connector.Start`, and legacy ACK behavior unchanged. The existing constructor rejects `strategy: online` with an instruction to use the new API. Reject online with `never` or `snapshot_only`. Do not silently downgrade protocol 2. New readiness signals distinguish capture readiness, snapshot delivery completion, and terminal failure.

## 3. Runtime layout

```text
One replication-I/O owner             One SQL coordinator
  receive + bounded feedback            LOW → SELECT → HIGH
             │                                   │
             ▼                                   ▼
      bounded committed events ──────► single-window merger
                                                │
                                                ▼
                                     serialized durable handler
                                                │
                                      completion / error
                                                │
                                                ▼
                            source metadata checkpoint → safe ACK
```

Use separate connections for active replication, chunk/marker SQL, and owner-lock/metadata operations. Never issue SELECT on an active replication connection. No network operation shares a pgconn without explicit ownership.

Use the simpler Netflix-style processing pause: consume LOW, stop merge dispatch during the bounded SELECT, emit HIGH, then drain events through HIGH. Replication I/O and safe status updates remain separately managed. There is no concurrent conflict check followed by an uncoordinated READ append.

The I/O owner must still send periodic feedback when its bounded event queue is full. Use a cancellable pending-send state and bounded receive deadlines, not a blocking channel send that starves feedback. Do not introduce an unbounded secondary queue.

## 4. Source schema: metadata only

Create a dedicated `cdc_online` schema, separate from legacy `cdc_snapshot_*` tables. Provision it idempotently. Allow manual pre-provisioning for restricted roles. Use versioned migrations and parameterized values with quoted identifiers.

| Table | Required metadata | Purpose |
|---|---|---|
| `cdc_online.jobs` | Job UUID, slot binding UUID, source identity, generation, config fingerprint, status, durable source LSN | Own a resumable strategy and source position |
| `cdc_online.table_progress` | Job/table identity, schema/key fingerprint, upper key bound, last completed selected key, completion flag | Preserve finite, ordered scan coverage |
| `cdc_online.windows` | Attempt UUID, job/generation/table, previous cursor, last selected key, LOW/HIGH IDs and end LSNs, status | Distinguish active, completed, and abandoned attempts |
| `cdc_online.watermarks` | Connector/job identity, generation, attempt UUID, phase, marker UUID | One small mutable control row per connector |

Do not add row payload, full-row JSON, or outbox columns. Avoid precomputing every chunk or running COUNT(*) for correctness. Retain one active attempt plus bounded diagnostic history.

Publish only `watermarks`, not the progress tables. Before stream startup, add it to an owned publication only when automatic publication management is enabled. Otherwise validate pre-provisioning and return the required SQL as an actionable error. Re-read catalog membership rather than assuming the existing `Publication.Create` adds missing tables.

Explicitly exclude control tables from snapshot discovery and business output. Reject publications that implicitly capture the entire metadata schema. This prevents checkpoint writes from feeding back into the capture stream.

## 5. Ownership and source initialization

Acquire a session advisory lock before creating job state or starting reads. A second process fails fast. Loss of the lock connection cancels the run. Do not reconnect that connection transparently. No lease-based takeover or HA promise is part of the POC.

A new job requires a fresh connector-owned slot. Expose creation provenance and its consistent start position through a narrow slot helper without changing legacy callers. Record a stable slot-binding identity and source identity before progress can advance. If initialization stops between slot creation and job binding, require explicit reconciliation rather than adopting an unknown existing slot.

A resumed job requires its original slot, source identity, and configuration fingerprint. If the slot is missing, invalid, or confirmed beyond the durable metadata checkpoint, fail closed. If the slot is behind the durable checkpoint, resume from that checkpoint: its required outputs already completed.

Start replication and the sink lifecycle before selecting a chunk. Capture each table's upper key bound only after capture starts. Use fresh primary-side reads, not an imported whole-job snapshot.

## 6. One-window running flow

1. Create an attempt UUID and record its previous completed key. Register the attempt before emitting LOW.
2. Commit a LOW update in its own transaction. Consume CDC through the committed LOW event.
3. Apply the fail-closed visibility guard to every relevant preceding transaction before its checkpoint can advance. Do not infer visibility from only the final XID.
4. Persist the active attempt and LOW source checkpoint together after all preceding output completes. LOW becomes the temporary ACK ceiling.
5. Pause merge dispatch. Run one fresh READ COMMITTED keyset query on the SQL connection, with row, byte, and time limits.
6. Capture its rows by table/key and remember the last selected key. Commit HIGH separately after the complete result is available.
7. Resume merge dispatch. For each committed CDC event before HIGH, remove any matching READ and deliver the CDC event normally.
8. At HIGH, deliver surviving READs in stable selected-key order. Post-HIGH events must not overtake them.
9. After all required output through this boundary succeeds, atomically complete the attempt, advance the table cursor, and save HIGH as the source checkpoint.
10. Only then release the ACK ceiling and begin another chunk. After the last chunk, continue CDC with periodic safe metadata checkpoints.

This extends the blog's sequence with an observed LOW visibility barrier and explicit durable completion. LOW and HIGH are separate committed transactions, not one transaction around the SELECT. Control effects wait for their transaction-end boundary.

A representative query after a completed key of 3000 is `WHERE id > $1 AND id <= $2 ORDER BY id LIMIT $3`. The first page omits the lower predicate. Do not use a sentinel that excludes the minimum integer key. Advance by the last selected key, even if every READ conflicts. An empty table still passes a completion fence.

## 7. Committed transactions and internal controls

Add an online-only protocol-1 reader that reuses existing wire decoders and connection utilities. Assemble each transaction until COMMIT so its end LSN and stable row ordinals are known. Enforce `transactionBytes` before delivery. Do not reinterpret the legacy stream's early per-row dispatch as a completed transaction.

Expose a narrow internal committed-transaction envelope with XID, commit start/end LSN, stable DML ordinal, and decoded changes. Preserve ordinary CDC events for all captured business tables. Relation metadata is not a business row and does not consume a DML ordinal.

Recognize watermarks by exact schema/table and validated connector, generation, attempt, and marker IDs. Ignore unrelated connector markers only as control traffic. Never send them through the legacy heartbeat auto-ACK path. Failed decoding of required events is terminal for online mode, not a debug-level skip.

During quiet CDC, small internal heartbeat marker updates allow progress without business writes. Suppress new heartbeat writes while the same SQL connection executes a chunk. Received heartbeats cannot bypass an active ACK ceiling or unfinished delivery. Batch steady-state metadata checkpoints to avoid one extra source write per application row.

## 8. Delivery identities and the durable test target

The source stores no output outbox. The sink contract must therefore handle replay and delayed old writes. An idempotent unversioned upsert alone is insufficient.

Each online event carries a stable event ID, table/key, operation, complete image, and a comparable order token:

| Event | Event identity | Order token |
|---|---|---|
| CDC | Stable source/slot binding + transaction end LSN + DML ordinal | `(transaction_end_lsn, 0, ordinal)` |
| READ | Job + attempt + table/key | `(high_transaction_end_lsn, 1, selected_row_ordinal)` |

The READ token is an output-order boundary, not the source commit LSN of that row. Expose it separately from `CommitLSN`. Dedicated marker transactions contain no business DML, so READ phase 1 follows their boundary and precedes later CDC. Old attempts use older HIGH boundaries.

The separate PostgreSQL test target atomically records event identity and applies the row only if its order token is newer. Preserve a tombstone and its token for deletes. Keep the dedup ledger for the full POC run. This prevents an old replay or late READ from overwriting a later value or delete.

This target is a test fixture and example of the required contract, not a new production sink adapter. Kafka and Elasticsearch must later implement an equivalent ordered-replay/version policy before this strategy is enabled there.

## 9. Metadata commits, ACKs, and restart

Track delivery obligations in order. For the POC, one sink invocation at a time provides simple completion ordering. Internal controls are not permission to skip earlier work. Source confirmation is bounded by the durable metadata checkpoint and any unresolved window.

| Failure point | Required behavior |
|---|---|
| Before LOW is durable | Keep the preceding source checkpoint; retry with a new attempt |
| SELECT error, timeout, or byte limit | Abandon the attempt, emit no READs, retain safe source progress, return a typed error |
| CDC or READ handler fails | Stop later delivery, leave attempt incomplete, do not advance beyond LOW |
| Sink write commits, reply is lost | Retry on restart under stable identity/order tokens; target rejects duplicate or stale effects |
| Some READs succeed, then process dies | Replay CDC from the durable checkpoint, abandon old rows, run a fresh attempt from the previous completed key |
| Chunk metadata commit succeeds, reply is lost | Reload by attempt UUID and resolve its committed state before retry |
| Metadata commits, feedback is lost | Resume from the durable metadata checkpoint; no need to repeat the completed interval |
| Owner/source identity changes or WAL is absent | Fail closed; no silent resnapshot or slot replacement |

On restart, catch up through a fresh LOW with all-prior visibility checks before reading. CDC IDs remain stable across run generations. Reject late marker/read completions from abandoned attempts, including retries in the same process. Use conditional attempt transitions and check affected rows.

The qualified target prevents stale replay, but raw at-least-once delivery can repeat older events after restart. Document that distinction. No exactly-once transport or arbitrary-callback guarantee is claimed.

## 10. Changes by repository seam

| Location | Planned change | Keep unchanged |
|---|---|---|
| `config/config.go` | Strategy enum, online limits, mode/protocol validation | Existing defaults and modes |
| New `online_connector.go` | Additive constructor, Run/error lifecycle, readiness | Existing public Connector and ListenerFunc contracts |
| New `pq/replication/online.go` plus small shared helpers | Bounded committed envelopes, feedback and controls | Existing streamed-transaction behavior |
| New `pq/snapshot/online/` | Metadata, keyset reader, coordinator, pure window state machine | Exported-snapshot coordinator/worker path |
| `pq/publication/` | Explicit watermark membership setup/check | Legacy publication selection |
| `pq/slot/` | Slot-creation provenance and restart validation seam | Existing Create/Info callers and reconnect work |
| `integration_test/` | Separate durable POC fixtures and deterministic tests | Existing fast test profile |
| New `example/online-initial-poc/` | Runnable opt-in demonstration and manual metadata SQL | Production adapter repositories |

No logical `M` decoder, transient cache, embedded database, or new production runtime dependency is required. Reuse pgx, pgoutput decoders, existing metrics facilities, testify, and testcontainers. See [impact](../../IMPACT_LATEST.md) and [test architecture](../../tech-architecture/e01-TEST_PLAN_LATEST.md).

## 11. Delivery slices and task order

| Slice | Demonstrable outcome | Ordered task groups |
|---|---|---|
| [e01s01](e01s01-single-window.md) | One chunk merges correctly into a durable test target | Baseline/fixtures → API/validation → source controls → integrated one-window path |
| [e01s02](e01s02-recovery.md) | Multi-chunk progress survives hard process restarts | Metadata/ACK barriers → finite scan → replay/order tests → failure handling |
| [e01s03](e01s03-qualification.md) | Reproducible POC result and compatibility evidence | Adversarial tests → workload/retention experiment → example and release decision |

All tasks start `failing` to mean not yet implemented or verified, not a current baseline failure. Their verify commands target the future tests and must fail when those tests are absent. No task is marked passing by this planning exercise.

## 12. POC exit criteria and non-goals

Require exact source/target equality at a final fence, no stale target application, complete required CDC coverage, and no source ACK beyond the proven frontier. Require at least two completed windows while writes remain active and observed confirmation progress before the whole snapshot completes.

Run deterministic failure schedules, process-kill recovery, restricted-role tests, and root/legacy regressions. Report peak memory, retained WAL, query latency, feedback age, and source/target throughput under the documented workload. A POC result is not a production SLO.

No sink-outage WAL isolation, parallel windows, online mode switching, new failover promise, full feature parity, or unqualified adapter support is included. Approval to implement this plan is separate from approval to ship the feature.

## 13. Planning verification

The [scope](../../product/SCOPE_LATEST.yaml), story specs, task ledgers, and test scenarios must agree. Run capsule structure checks and validate YAML/command syntax before implementation. Actual runtime verification belongs to the task commands and the test-plan gates.

**Current next step:** independent review and human UAT confirmation. Final root/race/vet/full PG16 and explicit matched-capacity checks passed for the restricted POC; this is not production-readiness or adapter-enablement approval.
