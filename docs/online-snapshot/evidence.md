# Online snapshots: evidence and provenance

**Date:** 2026-10-13. **Purpose:** separate source observations, executed checks, published mechanisms, and proposed guarantees.

## 1. Repository baseline

| Repository | Inspected revision | Qualification |
|---|---|---|
| `go-pq-cdc` | `4a7e9b094a19935591b691cf8de38546a98b69d0` | Branch `fix/slot-info-reconnect-on-closed-conn`, including local changes and new tests |
| `go-pq-cdc-kafka` | `28f6072c4924bb7a8c2244128423b0bd64ab340b` | Local adapter source inspection, not runtime qualification |
| `go-pq-cdc-elasticsearch` | `5d8774443ff36c0b62c12f1bc9dbde2469e87cfe` | Local adapter source inspection, not runtime qualification |

The checkout was not clean. The root commit alone does not reproduce these results. [Baseline output](baseline-checks.txt) records the tracked working-diff digest and toolchain. Untracked tests also participated. No Go implementation file was changed for this document package.

The existing [concurrent snapshot draft](../concurrent-snapshot-design.md) and [presentation proposal](../presentations/online-snapshot/README.md) remain unchanged. They are design input, not deployed-feature evidence. This package independently compares their proposals.

## 2. Source observations

| Claim | Source location | Classification |
|---|---|---|
| Snapshot completes before replication opens | [`connector.Start` and `prepareSnapshotAndSlot`](../../connector.go#L243) | Implemented control flow |
| Coordinator holds an exported view across chunks | [`exportSnapshotTransaction`](../../pq/snapshot/coordinator.go#L105), [`executeInTransaction`](../../pq/snapshot/worker.go#L306) | Implemented behavior |
| Current WAL sample is independent of the exported snapshot | [`setupJob`](../../pq/snapshot/coordinator.go#L219), [`getCurrentLSN`](../../pq/snapshot/coordinator.go#L375) | Source-derived overlap risk, no boundary reproduction in this package |
| Snapshot listener ACK does nothing | [`snapshotHandler`](../../connector.go#L571) | Implemented behavior |
| Snapshot row-handler errors are ignored | [`processChunk`](../../pq/snapshot/coordinator.go#L446) | Implemented behavior |
| CDC confirmation keeps a maximum position | [`processLoop`](../../pq/replication/stream.go#L678), [`UpdateConfirmedXLogPos`](../../pq/replication/stream.go#L903) | Implemented behavior, insufficient for independent asynchronous completions |
| Logical messages are not decoded | [`message.New`](../../pq/message/message.go#L42) | Implemented gap |
| Message and streaming options are coupled | [`Replication.Start`](../../pq/replication/replication.go#L23) | Implemented behavior |
| Streamed transactions accumulate until commit | [`streamTxBuffer`](../../pq/replication/stream.go#L279) | Implemented behavior, no configured byte limit |
| Visibility guard checks source transaction visibility | [`visibilityGuard`](../../pq/replication/visibility.go#L19) | Existing component, not an online-window proof |
| Incomplete legacy jobs can restart with a fresh view | [`initializeCoordinator`](../../pq/snapshot/coordinator.go#L28) | Implemented behavior, not target reconciliation |
| Unchanged TOAST can require reconstruction | [`Update.decode`](../../pq/message/format/update.go#L102), [`DecodeWithColumn`](../../pq/message/tuple/data.go#L78) | Existing tuple semantics |

Line anchors describe this inspection and can move. Function names are the durable lookup keys.

### Adapter observations

- In normal CDC startup, Kafka `connector.Start` waits for core readiness before `StartBatch`. `Batch.flushMessages` uses the final listener context as a cumulative ACK convention.
- In normal CDC startup, Elasticsearch `connector.Start` waits for core readiness before `StartBulk`. `Bulk.flushMessages` can clear and acknowledge a failed batch when a custom response handler is installed.
- The separate `snapshot_only` path creates no replication slot. Both adapters start their batch loops before snapshot-only core startup.
- These observations motivate an explicit online delivery contract. They are not a complete audit of either adapter.

Adapter locations are external to this repository: `go-pq-cdc-kafka/connector.go`, `kafka/producer/producer_batch.go`, `go-pq-cdc-elasticsearch/connector.go`, and `elasticsearch/bulk/bulk.go` at the revisions in [Repository baseline](#1-repository-baseline).

## 3. Executed baseline checks

The checks were repeated on 2026-10-13 during document preparation. [Captured output](baseline-checks.txt) records success. Toolchain: Go 1.26.2, macOS arm64. The integration harness used Podman and its PostgreSQL `16-alpine` image profile.

```bash
go test -race -count=1 ./...
go vet ./...
```

The separate integration module ran this selection:

```bash
cd integration_test
POSTGRES_VERSION=16-alpine go test -race -count=1 -p=1 -timeout=120s \
  -run '^(TestSnapshotConcurrentInsertNoDuplicates|TestNoWalsenderDuringSnapshot|TestSnapshotBasicSingleChunk|TestConcurrentTxOrdering|TestStreamingSubTransactionRollback|TestSIGTERMFlushesPendingConfirmedLSN|TestSlotInfoReconnectsAfterBackendTermination)$' ./...
```

| Result | Scope |
|---|---|
| PASS | Root race-enabled tests |
| PASS | Root `go vet` |
| PASS | Seven selected PostgreSQL 16 integration tests |
| NOT RUN | Full integration suite and full supported-version matrix |
| NOT RUN | Kafka and Elasticsearch runtime suites |
| NOT RUN | Online merger, recovery, fault injection, sustained-load, and HA qualification |

### Limits of the passing tests

[`TestSnapshotConcurrentInsertNoDuplicates`](../../integration_test/snapshot_consistency_test.go#L90) waits for readiness before it starts concurrent writes. Readiness currently follows the snapshot. The test accepts 45 of 51 CDC keys and at least 95 of 101 combined keys. It does not prove zero loss during a snapshot race.

[`TestNoWalsenderDuringSnapshot`](../../integration_test/snapshot_no_walsender_test.go#L134) starts its sampling interval after readiness. It does not require an observation during actual snapshot execution.

[`TestMain`](../../integration_test/main_test.go#L104) uses `fsync=off`. Its CDC role is a superuser. Separate durable and restricted-role profiles are required.

The untracked disconnect test defines a helper without dispatch from `TestMain`. It was not part of the selected run. Repair its helper wiring before relying on it as crash evidence. A forced process exit alone does not prove continuity after restart.

## 4. Published mechanisms and their limits

| Reference | Supported claim | Not established by the reference |
|---|---|---|
| [DBLog paper, section 3.2 / Algorithm 1](https://arxiv.org/pdf/2010.12597) | LOW/SELECT/HIGH reconciliation, CDC-wins conflict removal, short per-chunk processing pause | This library's recovery protocol, bounded total memory, or WAL benchmark |
| [PostgreSQL 16 logical replication parameters](https://www.postgresql.org/docs/16/protocol-logical-replication.html) | `messages=true` and separate transaction-streaming options | Correct application dispatch or acknowledgement behavior |
| [PostgreSQL 16 message format](https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html) | `M` payload layout and optional streamed XID, unchanged TOAST representation | A complete bootstrap row from every UPDATE |
| [PostgreSQL 16 administrative functions](https://www.postgresql.org/docs/16/functions-admin.html#FUNCTIONS-REPLICATION) | Transactional logical-message emission | Permission to ACK on message emission |
| [PostgreSQL 16 logical decoding concepts](https://www.postgresql.org/docs/16/logicaldecoding-explanation.html) | A slot-exported snapshot corresponds to that slot's subsequent change stream | Safe substitution into a different slot's stream |
| [PostgreSQL 16 replication commands](https://www.postgresql.org/docs/16/protocol-replication.html) | Slot creation results, snapshot lifetime, logical start-position rules | A reusable application crash/checkpoint protocol |
| [PostgreSQL 16 slot view](https://www.postgresql.org/docs/16/view-pg-replication-slots.html) | `restart_lsn`, `confirmed_flush_lsn`, `wal_status`, and `safe_wal_size` | Guaranteed bounded WAL from slot activity alone |

The paper describes watermark-table updates. Logical-message transport is a PostgreSQL-specific adaptation. The concurrent read extension, visibility barrier, durable-stage protocol, and release gates are proposals in this RFC.

Do not use older comparative tables in the paper to describe current competing products. This package does not certify the existing draft's external incident summaries or Flink implementation claims.

## 5. Independent review and document status

The preceding analysis used separate read-only code and reliability reviewers. This package also received independent protocol and evidence reviews. Parent revisions incorporated restart-complete visibility, attempt-level fencing, explicit RLS exclusion, and mode-qualified startup descriptions.

The reviewers did not execute tests or validate PDF rendering. Their review is not an implementation proof or formal approval. The parent checked the final documents and exports separately.

**Current conclusion:** feasible to validate, not approved for production. No performance improvement, recovery-time target, or zero-loss production result is claimed.

**Document verify:** `test -s docs/online-snapshot/baseline-checks.txt && grep -q 'All selected baseline checks: PASS' docs/online-snapshot/baseline-checks.txt`.
