# Online snapshots with WAL watermarks

Presentation-ready proposal for `go-pq-cdc`, `go-pq-cdc-kafka` and `go-pq-cdc-elasticsearch`.

**Main message:** keep replication capture running while scanning short snapshot chunks. Use WAL markers to order the merge; use recoverable delivery/checkpoints to safely advance source acknowledgments.

This is a proposed architecture, not an implemented or benchmarked feature. It is inspired by Sequin/DBLog principles. The standard LOW/HIGH protocol illustrated here is distinct from Sequin's pre-registered-PK + HIGH implementation reviewed at source revision `46ce4e1048437575ce3c40ebb3eb589a4b9e4f27`.

## Files and presenting

| File | Use |
|---|---|
| `index.html` | Offline, five-slide browser presentation; no external dependencies |
| `online-snapshot-design.pdf` | Five-page, 16:9 presentation export |
| `architecture.svg` | Vector architecture diagram for slide editors |
| `watermark-walkthrough.svg` | Vector merge walkthrough for slide editors |
| `slide-01.png` … `slide-05.png` | 1600 × 900 slide images for PowerPoint/Google Slides |
| `export.mjs` | Repeatable export and basic layout/navigation validation |

Open `index.html` in a browser. Use arrow keys, Page Up/Page Down, Home/End, or the on-screen controls. Use **Fullscreen** for presenting and **Print / PDF** to print all slides. The PDF and SVGs are the preferred scalable formats. The exported slides are images, not an editable PowerPoint deck.

On macOS:

```bash
open docs/presentations/online-snapshot/index.html
open docs/presentations/online-snapshot/online-snapshot-design.pdf
```

To regenerate PDF and PNG files with Node.js 22+ and a local Chrome/Chromium:

```bash
node docs/presentations/online-snapshot/export.mjs

# Linux / custom installation:
CHROME_BIN=/usr/bin/chromium node docs/presentations/online-snapshot/export.mjs
```

The exporter starts a temporary headless browser, checks basic slide bounds and navigation, and cleans up its temporary profile. It uses Node's built-in WebSocket and fetch APIs; no npm packages are needed. Visual review is still necessary after editing. Do not run two exports concurrently into the same directory.

## Three-minute talk track

1. **Problem:** today's connector finishes the full snapshot before starting replication. Long snapshots retain WAL and intentionally leave the slot without a streaming reader.
2. **Design:** run a replication reader and a short-query backfill worker concurrently. Put LOW/HIGH markers into the same WAL stream. Keep snapshot rows in a bounded chunk buffer.
3. **Merge:** if a CDC event touches a buffered row, discard that row's READ. At HIGH, insert remaining READs before later same-key changes. This avoids stale updates and resurrected deletes.
4. **Safety:** active slot does not mean healthy slot. Advance ACKs only across recoverable work; preserve per-key order, checkpoint backfill progress, and handle replay idempotently.
5. **Decision:** opt into online backfill for write-heavy, large datasets. Retain consistent initial snapshot mode when a global snapshot boundary is required.

## Slide 1 — Keep CDC moving. Merge one chunk at a time.

**What to point at:** the two source paths, their meeting point in the merger, and the green feedback loop.

- Business changes flow into WAL; short SELECT pages flow into pending chunk state.
- The backfill worker uses a regular SQL connection on the primary. The replication reader uses its own replication connection; one reader owns a slot at a time.
- LOW/HIGH are control messages emitted into WAL with `pg_logical_emit_message(true, ...)`, not table mutations and not direct messages to the Go worker.
- The common library owns decoding, chunk windows, ordering and recovery coordination. Sink adapters keep Kafka and Elasticsearch durability semantics explicit.
- The output can use direct delivery or durable staging. Persisting a raw event is not enough if a lost merge window could emit stale snapshot data after restart; the recovery protocol must protect both.
- Safe source progress means a contiguous recoverable prefix. With multiple sinks/partitions, account for all required outputs, not the fastest one.
- Short reads avoid retaining one whole-job MVCC snapshot. They still create source read/CPU/I/O load and must be throttled.

**Important qualification:** restarting or backpressuring the reader can still make it unavailable. The benefit is removal of the planned whole-snapshot inactive phase, not a claim that the slot is always active or never lags.

## Slide 2 — At HIGH, discard stale READs. Insert the survivors.

The illustration is conceptual; horizontal spacing is not elapsed time or a benchmark.

1. Commit LOW in a standalone transaction.
2. Read a chunk using a fresh statement snapshot, after LOW commits. Buffer its READ rows.
3. Concurrent CDC observes UPDATE 42 and DELETE 85; record those keys even if the SELECT result has not yet returned.
4. Commit HIGH after the SELECT result has been captured. Route the committed HIGH to the merger.
5. Remove READ 42 and READ 85. Keep their CDC events. Emit READ 99.
6. Place the remaining READs before later same-key CDC in the logical output order. Later UPDATE 99 must win over READ 99.

The price example is illustrative: READ 42=100 loses to UPDATE 42=120; row 85 stays deleted; READ 99=300 is followed by UPDATE 99=350.

**Do not overclaim:** not every row produces a READ; a CDC event can supply its coverage instead. This is a current-state bootstrap, not historical reconstruction. UPDATE-before-READ needs an upsert/full-row policy; unchanged TOAST or partial updates may require reconstruction. Filters, PK mutation, schema changes and TRUNCATE require explicit handling.

**Operational detail:** a source transaction may have started before HIGH but commit afterward. Treat it as a later committed CDC change, not as something excluded because its transaction began early.

## Slide 3 — A small WAL message joins two independent readers.

- `pg_logical_emit_message` writes arbitrary text/binary payload into WAL and returns an associated record LSN. JSON and prefix naming are application conventions.
- `transactional=true` ties decoding to transaction outcome. A rollback must not trigger a completed control action. Streamed in-progress transactions can expose bytes before COMMIT; gate their effects and discard on ABORT.
- Commit LOW, perform the fresh SELECT, then commit HIGH separately. Wrapping all three in one transaction is not the standard LOW/HIGH window-opening protocol.
- PostgreSQL 14+ `pgoutput` forwarding requires `messages=true`; logical messages also work with protocol 1 on supported servers. Protocol 2 is necessary for in-progress transaction streaming, not inherently for the marker itself.
- The `M` wire message contains a streamed XID only when inside a streamed transaction; do not parse one merely because protocol 2 was configured.
- The marker is database-scoped, not publication-table scoped or addressed to a particular slot. Validate the payload's connector, generation and chunk identifiers.
- Check emitter EXECUTE grants, replication-reader privileges and table SELECT permissions independently. Prefixes are not authentication. Limit payloads and do not include secrets.
- Marker return LSN, marker record position, transaction commit/end positions and source confirmed progress are different concepts. Do not ACK based on return value or arrival alone.
- Read-replica SELECTs require additional replay/visibility coordination; this diagram deliberately starts with primary reads.

**Current local implementation gap:** `pq/replication/replication.go` enables `messages=true` for protocol >=2, but `pq/message/message.go` defines `LogicalByte='M'` without decoding it. Add decoding, transaction-aware control dispatch and merger integration; do not silently ignore the marker or auto-ACK it like a heartbeat.

## Slide 4 — Receiving a marker is not enough. Recovery must be safe.

Keep these boundaries separate:

| Boundary | Required invariant |
|---|---|
| Commit | Transactional marker actions do not survive source ABORT |
| Ordering | A surviving READ cannot follow a newer same-key CDC change |
| Durability | Source ACK cannot skip undelivered/unrecoverable required output |
| Checkpoint | Saved keyset progress cannot skip unfinished chunk coverage |
| Capacity | Pending windows, keys, bytes and sink work remain bounded |

A practical chunk lifecycle is:

```text
PENDING → LOW COMMITTED → READING → HIGH COMMITTED
        → MERGED / ORDERED → RECOVERABLE → CHECKPOINTED
```

Transient in-memory states are not automatically restart-safe. An implementation can persist enough window/output state or conservatively retry chunks with fresh reads, coordinated replay and idempotency. This proposal does not claim the entire crash protocol is already implemented.

Monitor retained WAL and source disk budget independently of slot `active`. A long transaction can pin `restart_lsn` even when `confirmed_flush_lsn` advances. Track slot invalidation, source throughput, buffer bytes/age, pending windows, backfill progress, sink lag and failures. Apply throughput/load limits before the source or ingestion layer runs out of capacity.

## Slide 5 — Prefer online backfill for large, write-heavy clusters.

### Pros

- Removes the intentional snapshot-wide inactive-reader phase.
- Allows safe feedback to advance during backfill, reducing prolonged source WAL retention when capacity permits.
- Avoids a whole-job MVCC transaction and its prolonged vacuum horizon.
- Supports resumable and selective backfills using checkpointed keyset progress.
- Logical markers avoid a separate watermark table and business-row rewrites.
- Shares core bootstrap behavior across Kafka and Elasticsearch adapters.

### Cons / limits

- Chunks observe different times: not a single cross-table T0 snapshot or a cross-table transactional materialization guarantee.
- More complex decoding, commit handling, merge windows, durable checkpoints and restart semantics.
- Needs stable indexed row identity; PK changes, TOAST, filtering and DDL need defined policy. CTID is not a stable incremental cursor across changing snapshots.
- Sink adapters must honor same-key ordering and success/failure semantics. Kafka partitioning and broker confirmation; Elasticsearch per-item bulk results and bootstrap upserts matter.
- At-least-once duplicates remain possible. Marker replay and output retries must be idempotent.
- More source queries and small control transactions; concurrency increases I/O and decoding pressure. No performance numbers are claimed.
- Slow sinks, large source transactions and full buffers can still grow WAL retention. Watermarks alone do not fix capacity.

### Recommended rollout

1. Add an opt-in online/incremental mode; retain the current consistent initial mode.
2. Start sink batch loops and source metric polling before backfill; separate capture readiness from bootstrap completion.
3. Implement the `M` decoder and transaction-aware marker routing.
4. Implement stable-key chunking, the merger and same-key output barriers.
5. Implement safe ACK tracking, durability policy, recoverable cursors and generation fencing.
6. Test writes during reads, SELECT/marker races, deletes, PK changes, source aborts, failed bulk items, worker takeover, restarts and buffer exhaustion.
7. Run sustained-write load tests: assert final target state, no missing required events, bounded source/buffer budgets and recoverable progress—not merely `active=true`.

For Elasticsearch live-index exposure, consider a staging index + catch-up + alias swap. For Kafka, use stable keys and coordinated partition routing. Separate snapshot and CDC topics do not provide cross-topic ordering automatically.

## Sequin distinction

The published Sequin article describes LOW → SELECT → HIGH. The inspected source revision instead first fetches candidate PKs, registers them in an ETS multiset, fetches rows, emits HIGH and removes observed CDC PKs from pending batches. HIGH triggers filtering and insertion into its message store. This deck illustrates the easier-to-explain standard protocol; it does not label that protocol as an exact copy of current Sequin.

Sequin also gates source progress using unpersisted output positions. Failed/old messages can be persisted in its internal PostgreSQL buffer; cursor/delivery tracking uses Redis. This storage is ingestion infrastructure, not a recommendation to write the CDC backlog into the same production source database.

## Sources

- [PostgreSQL logical-message function](https://www.postgresql.org/docs/current/functions-admin.html)
- [PostgreSQL 14 pgoutput parameters](https://www.postgresql.org/docs/14/protocol-logical-replication.html)
- [Logical-message wire format](https://www.postgresql.org/docs/14/protocol-logicalrep-message-formats.html)
- [Sequin watermark explanation](https://blog.sequinstream.com/using-watermarks-to-coordinate-change-data-capture-in-postgres/)
- [Sequin consistency documentation](https://sequinstream.com/docs/concepts/consistency-model)
- [Sequin TableReader at reviewed revision](https://github.com/sequinstream/sequin/blob/46ce4e1048437575ce3c40ebb3eb589a4b9e4f27/lib/sequin/runtime/table_reader.ex)
- [Sequin TableReaderServer at reviewed revision](https://github.com/sequinstream/sequin/blob/46ce4e1048437575ce3c40ebb3eb589a4b9e4f27/lib/sequin/runtime/table_reader_server.ex)
- [Sequin SlotProcessorServer at reviewed revision](https://github.com/sequinstream/sequin/blob/46ce4e1048437575ce3c40ebb3eb589a4b9e4f27/lib/sequin/runtime/slot_processor_server.ex)
- [DBLog watermark framework](https://arxiv.org/abs/2010.12597)

Local implementation references: `connector.go`, `pq/message/message.go`, `pq/replication/replication.go`, `pq/snapshot/coordinator.go`, and the Kafka/Elasticsearch wrapper connectors inspected during this discussion.
