# 10M-row online snapshot benchmark report

Updated 2026-11-08. **The headline test is the fair head-to-head sentinel-barrier benchmark (`TestOnlineInitialSnapshot10MHeadToHead`) at 1,000 updates/s for 5 minutes: both connectors consume the same transaction stream from one shared source concurrently and stop on the same final CDC message.** The earlier snapshot-then-drain comparison (`TestOnlineInitialSnapshot10MComparison`) is kept below as historical evidence; its end condition was unfair because legacy stopped at snapshot end and never had to consume the concurrent CDC stream inside its snapshot window.

This is a local single-run measurement, not a production SLO and not a correctness substitute for the existing recovery/capacity gates.

## Head-to-head with sentinel barrier (fair)

Test: `TestOnlineInitialSnapshot10MHeadToHead` in `integration_test/online_snapshot_h2h_benchmark_test.go`.

The problem and the solution:

- Problem: legacy consumes CDC only after its snapshot, online consumes CDC during its snapshot, so any "stop when snapshot ends" comparison measures different total work.
- Solution: one shared PostgreSQL source; legacy (`bench_legacy_slot`/`bench_legacy_pub`) and online (`bench_online_slot`/`bench_online_pub`) connectors run concurrently against the same table. A writer sends `1,000 updates/s` for `5 minutes` and then commits exactly one final sentinel transaction (`UPDATE workload SET value=9000000007 WHERE id=10000000`). Each arm stops only when its own snapshot is complete AND it has delivered that sentinel. Delivery is ordered per relation, so both arms must process the identical logical stream prefix up to the same final CDC message. The writer records `pg_current_wal_flush_lsn()` before and after the sentinel commit; each arm reports the sentinel's commit LSN, and the test requires the two arms' commit LSNs to be equal to each other and inside the writer window. Each arm must deliver exactly `writes + 1` CDC changes.

Run-length guarantee (why 5 minutes at 1000 RPS): legacy's snapshot lands in ~22s, so it spends ~278s consuming live CDC before the sentinel — its snapshot definitively ends while the stream is still flowing. Online absorbs the entire live 300k-event stream into its snapshot (completion at ~19 min, after writers stop), so the sentinel is its only post-snapshot CDC message — online never runs a separate CDC phase after its snapshot.

Exact command:

```sh
CDC_ONLINE_10M_BENCH=1 CDC_H2H_DEADLINE_MIN=240 POSTGRES_VERSION=16-alpine go test ./integration_test \
  -run '^TestOnlineInitialSnapshot10MHeadToHead$' \
  -count=1 -timeout=300m -v \
  2>&1 | tee specs/verifications/online-10m-snapshot-benchmark/evidence/run-h2h-1000rps-5m.log
```

Optional env: `CDC_H2H_ROWS=<n>` shrinks the table for a mechanism smoke, `CDC_H2H_SECONDS=<n>` changes the writer duration, `CDC_H2H_DEADLINE_MIN=<n>` overrides the arm-stop deadline (at 1000 RPS on this VM online needs ~4x the writer duration; the default is `3T+30m`).

SVG summary:

```text
specs/verifications/online-10m-snapshot-benchmark/comparison-h2h.svg
```

Result (2026-11-08, `evidence/run-h2h-1000rps-5m.log`, `evidence/h2h.json`):

```text
--- PASS: TestOnlineInitialSnapshot10MHeadToHead (1152.41s)
```

| Metric | legacy consistent | online |
| --- | ---: | ---: |
| Stop-at-sentinel seconds (fair total-work metric) | 300.041 | 1143.729 |
| Snapshot complete seconds | 21.882 | 1143.729 |
| Live CDC phase after own snapshot seconds | 278.120 | 0 (sentinel only) |
| Snapshot rows delivered | 10,000,000 | 9,995,000 |
| CDC deliveries (writes + sentinel) | 300,001 | 300,001 |
| Sentinel commit LSN | `0/5ABE9D78` | `0/5ABE9D78` |
| Writer sentinel window | `0/5ABE70C8` .. `0/5ABE9DA8` | same |
| Peak slot-retained WAL bytes | 36,409,240 | 106,215,824 |
| Peak confirmed lag bytes | 26,352,120 | 95,284,352 |
| Peak feedback age seconds | 29.193 | 0.670 |

Writers ran the full 300s at exactly 1,000 tx/s (300,000 writes). Online conflict-elided exactly one window's worth of keys (5,000 rows) into CDC delivery. Shared-process peaks: heap 399,672,152 bytes, RSS 476,856,320 bytes. Source container `5b2755b408cef17f5fd47f6bf1c54b4d84bbe2f23df9cd34dbda43d20fea085b`.

Interpretation:

- The fairness gate holds: both arms delivered exactly `300,000 + 1` CDC changes, each delivered the sentinel exactly once, and both reported the same sentinel commit LSN inside the writer-recorded window. Both connectors stopped on the identical CDC message.
- At 1000 RPS online is **3.81x** legacy wall clock to the shared stop point (at the earlier 120 RPS load it was 1.85x; archived in `evidence/h2h-120rps-60s.json`). Online's snapshot completion moves from ~1.85x the writer window at low load to ~3.8x at 1000 RPS: online's delivery throughput (~300 events/s on this VM under sustained 1000 tx/s) becomes the bottleneck and the snapshot swallows the whole live stream.
- WAL picture inverts vs the 120 RPS run: online's slot retains the undelivered in-flight backlog (peak 106MB retained, feedback itself fresh at 0.67s), while legacy's fast snapshot lets it confirm near the stream head soon (36MB retained but stale 29s feedback during its snapshot). Retained-WAL leadership is workload-rate dependent, not a fixed advantage.
- Caveat: after legacy stops at the sentinel (300s) the online arm continues alone until ~1144s, so the source is unloaded during online's final stretch — biased optimistic for online, not against it. Single local run, 6GB VM, no durable target sink.

## Scope

The historical snapshot-then-drain run compared initial snapshot behavior while business updates were generated at a steady rate:

- PostgreSQL image: `docker.io/postgres:16-alpine`
- Source durability: `fsync=on`, `synchronous_commit=on`, `full_page_writes=on`, `wal_level=logical`
- Table: `public.workload(id bigint primary key, value bigint)`
- Rows: `10,000,000`
- Chunk size: `5,000` rows
- Expected chunks / source read queries: `2,000`
- Update workload: `UPDATE workload SET value=$1 WHERE id=$2`
- Requested update rate: `120 updates/s`
- Requested update duration: `60s`
- Pattern source: `integration_test/online_capacity_test.go` steady-update ticker pattern

## Snapshot-then-drain comparison (historical, less fair)

Test: `TestOnlineInitialSnapshot10MComparison`. Legacy stopped at snapshot end and its CDC drain was opportunistic; online had to process the same concurrent updates inside its snapshot window. Kept for continuity.

Raw evidence is in `specs/verifications/online-10m-snapshot-benchmark/evidence/`:

| File | Purpose |
| --- | --- |
| `run.log` | First snapshot-only run |
| `run-with-wal.log` | Snapshot-only rerun with WAL-retention sampling |
| `run-steady-updates.log` | Final steady-update run |
| `run-h2h-1000rps-5m.log` | Fair head-to-head sentinel-barrier run, 1000 RPS × 5 min (headline) |
| `h2h.json` | Head-to-head metrics from both arms (1000 RPS run) |
| `run-h2h-120rps-60s.log` | Archived head-to-head at 120 RPS × 60s |
| `h2h-120rps-60s.json` | Archived head-to-head metrics at 120 RPS |
| `consistent.json` | Legacy/consistent metrics from historical steady-update run |
| `online.json` | Online metrics from historical steady-update run |

SVG summary:

```text
specs/verifications/online-10m-snapshot-benchmark/comparison.svg
```

## Exact command

```sh
CDC_ONLINE_10M_BENCH=1 POSTGRES_VERSION=16-alpine go test ./integration_test \
  -run '^TestOnlineInitialSnapshot10MComparison$' \
  -count=1 -timeout=30m -v \
  2>&1 | tee specs/verifications/online-10m-snapshot-benchmark/evidence/run-steady-updates.log
```

Final steady-update result:

```text
--- PASS: TestOnlineInitialSnapshot10MComparison (190.55s)
    --- PASS: TestOnlineInitialSnapshot10MComparison/consistent (68.98s)
    --- PASS: TestOnlineInitialSnapshot10MComparison/online (121.57s)
PASS
ok  github.com/Trendyol/go-pq-cdc/integration 191.707s
```

## Measurements

| Metric | legacy consistent | online |
| --- | ---: | ---: |
| Source rows | 10,000,000 | 10,000,000 |
| Chunk rows | 5,000 | 5,000 |
| Source read queries | 2,000 | 2,000 |
| Requested updates/s | 120 | 120 |
| Actual source tx/s | 119.98 | 120.00 |
| Writes | 7,199 | 7,200 |
| CDC deliveries | 7,199 | 7,200 |
| Snapshot rows delivered | 10,000,000 | 9,999,996 |
| Snapshot key checksum | 50,000,005,000,000 | 50,000,004,999,978 |
| CDC key checksum | 25,923,599 | 25,930,800 |
| Snapshot duration seconds | 15.531 | 113.965 |
| End-to-end duration seconds | 61.316 | 113.965 |
| Snapshot rows/s | 643,869 | 87,747 |
| Drain seconds | 0.603 | 0.000 |
| Max source read execution ms | 4.139 | 3.990 |
| Peak whole-process heap bytes | 5,206,496 | 10,313,056 |
| Peak whole-process RSS bytes | 35,733,504 | 43,810,816 |
| Peak main-slot retained WAL bytes | 7,472,144 | 4,924,688 |
| Peak confirmed lag bytes | 4,687,272 | 761,032 |
| Peak feedback age seconds | 14.777 | 1.256 |
| Online peak raw chunk bytes | n/a | 75,001 |
| Online peak read latency ns | n/a | 4,872,666 |
| Online feedback writes | n/a | 114 |

Container IDs:

| Strategy | Source container |
| --- | --- |
| consistent | `a2dfc7c605ad7df1f755973122201bfd18542c3db955478cb61f1d1921e03ed4` |
| online | `eb7c1a2b98afae7732e288aafc3209563ecd007f7572e98324fdff04b91654c0` |

## Interpretation

The legacy `consistent` snapshot remains faster for raw snapshot scan throughput: about **7.3x** higher delivered snapshot-row throughput than the online path in this steady-update run.

The online result is expected to deliver fewer READ events than source rows when keys are updated during an active online read window. Those keys are conflict-elided from READ delivery and delivered by CDC instead. In the final run, online delivered `9,999,996` READ rows and `7,200` CDC updates; the combined coverage exceeds the 10M source-row table size under the unique-key update workload.

WAL behavior flips compared with the snapshot-only run. Under steady updates, online retained less main-slot WAL at peak (`4.92MB` vs `7.47MB`) and had much lower confirmed-flush lag (`0.76MB` vs `4.69MB`). This matches the design goal: online actively streams and sends feedback during snapshot work, while legacy consistent only starts its active CopyBoth stream after the exported snapshot phase.

The online total duration is longer because it serializes read windows with LOW/HIGH watermarks and CDC conflict handling. Legacy consistent uses its existing snapshot worker pool and then drains CDC after the snapshot.

## Caveats

- This benchmark does not apply rows to a durable target. The handler counts rows/checksums only.
- Online READ rows can be lower than table cardinality under concurrent writes because updated keys are intentionally represented by CDC events instead of duplicate READ events.
- The update workload touches sequential IDs modulo table size at 120/s for 60s, matching the capacity-test ticker style. It is not a production traffic model.
- Whole-process heap/RSS includes Go test harness, fixture, client and connector work.
- This is one local run on Podman/Docker host memory reported as ~5.9GB. It should not be used as a production SLO.

## Verdict

**PASS.** The fair head-to-head run proves both connectors can be stopped on the same CDC message via the sentinel barrier. At 1000 RPS × 5 min both arms delivered 300,001 CDC changes and reported the identical sentinel commit LSN; legacy stopped at 300.0s (after ~278s of live CDC consumption following its 21.9s snapshot) and online at 1143.7s, having absorbed the entire live stream into its snapshot with the sentinel as its only post-snapshot CDC message — a 3.81x wall-clock ratio at this rate on this VM.
