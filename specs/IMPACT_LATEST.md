# Impact assessment: online initial-snapshot POC

**Risk: High.** The proposed change crosses configuration, source progress, delivery, metadata, and public lifecycle boundaries. This assessment authorizes no code changes.

## Target and purpose

Add an opt-in initial strategy with one window, metadata-only source storage, and published watermark rows. Preserve the legacy exported-snapshot path. See [implementation plan](epics/e01-online-initial-poc/implementation-plan.md).

## Existing contracts and callers

| Module | Purpose | Callers and dependent behavior | POC treatment |
|---|---|---|---|
| `config/config.go` | Defaults and validation for all connectors | Core constructors, examples, Kafka/Elasticsearch wrappers | Add strategy without changing existing defaults; reject invalid online combinations |
| `connector.go` | Legacy setup, snapshot-first startup, readiness, shutdown | All existing Connector users | Add a separate online API; preserve legacy behavior |
| `pq/replication/stream.go` | Legacy reader, callback delivery, ACKs, protocol-2 buffering | Core connector, slot feedback/metrics, transaction tests | Reuse small helpers only; do not change legacy completion semantics |
| `pq/message/` | pgoutput decoding and type conversion | Legacy stream and proposed online reader | Reuse supported decoders with strict online error handling |
| `pq/publication/` | Publication creation and catalog metadata | Core startup, heartbeat setup, selected snapshot tables | Add explicit marker membership validation/provisioning |
| `pq/slot/` | Slot creation, source position and metrics | Core, HTTP health, reconnect tests | Add creation provenance and online resume checks behind a narrow seam |
| `pq/snapshot/` | Shared-view snapshot workers and chunk metadata | `initial`, `snapshot_only`, examples and wrappers | Keep legacy state and workers separate from new online package |
| `integration_test/main_test.go` | Shared container lifecycle and test helper entry | Every integration test | Add isolated POC/helper profiles without changing fast defaults |

The current source uses keyed configuration literals in inspected examples. New struct fields remain source-compatible for those callers. Unkeyed external literals are a residual public-API compatibility risk and must be documented.

## Relevant evidence

- `connector.go:360–389`: slot creation precedes the full snapshot, and CDC opens afterward.
- `connector.go:571–579`: snapshot ACK is a no-op under the existing listener contract.
- `pq/replication/stream.go:903–910`: legacy confirmation retains a maximum LSN.
- `pq/publication/publication.go:29–42`: an existing publication is returned without adding missing tables.
- `pq/slot/slot.go:179–195`: current creation helper discards the replication-command result.
- `integration_test/main_test.go:104`: the default test profile uses `fsync=off`.

These are reasons to isolate the POC path, not permission to silently fix unrelated legacy behavior.

## Existing coverage and gaps

| Coverage | Evidence and remaining gap |
|---|---|
| Root config and decoding tests | Useful compatibility gate; no online strategy tests exist |
| Replication transaction and abort tests | Protect existing per-XID semantics; POC explicitly rejects protocol 2 |
| Snapshot basic/only/force-resnapshot tests | Protect unchanged modes; do not prove online reconciliation |
| `TestSnapshotConcurrentInsertNoDuplicates` | Timing follows readiness and permits missing keys; repair or add deterministic barrier coverage |
| `TestNoWalsenderDuringSnapshot` | Preserve legacy intent, but require real observation samples when repairing the test |
| Reconnect/shutdown tests | Separate status-connection recovery from source continuity |
| Untracked disconnect helper test | Known helper-dispatch gap from prior review; baseline owner must resolve it before an all-tests gate can be trusted |

The prior [baseline log](../docs/online-snapshot/baseline-checks.txt) passed root race tests, vet, and seven selected PostgreSQL tests. It is not a fresh full-suite baseline for implementation.

## Story ownership

- Story e01s01 owns the additive strategy/API, marker publication, strict source path, and durable sink tracer.
- Story e01s02 owns persisted source/table progress, attempt fencing, ACK barriers, and replay correctness.
- Story e01s03 owns adversarial qualification, bounded-load evidence, compatibility, and the demonstration.

## Source-side blast radius

New logged metadata tables add small control traffic to the source. Only the watermark table enters the publication. Progress tables must remain excluded. The POC rejects broad publications rather than risking feedback or metadata leakage.

Application SELECT load, marker transactions, checkpoint frequency, and held WAL during sink failure must be measured. The plan does not write snapshot row payloads back into source tables.

## Dependencies and authority

Existing dependencies: pgx `[OK]`, testify `[OK]`, testcontainers-go `[OK]`, existing YAML/metrics utilities `[OK]`. These are scope-fit classifications, not vulnerability-scan results. No new production dependency is proposed.

Kafka/Elasticsearch runtime changes are deferred. Their existing cumulative ACK conventions must not be reinterpreted through the new handler API.

## Recommended action

Proceed with the planned test-first POC only after selecting an approved implementation baseline. Keep the dirty reconnect work intact. Use a dedicated feature branch/worktree after owner approval, not this mixed checkout as disposable state.

**Verification:** `test -f specs/epics/e01-online-initial-poc/implementation-plan.md && grep -q 'Risk: High' specs/IMPACT_LATEST.md`.
