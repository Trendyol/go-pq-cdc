# Replica guard — implementation handoff

Status: **implemented** (core guard, config and metrics; integration test `integration_test/replica_guard_test.go` against a standby with `recovery_min_apply_delay = 3s`; README "Visibility Guard" and "Commit LSN and reading from a standby"; `example/replica-read` step 5). Decided 2026-09-20 by a four-round consensus
between Codex, Claude Opus and Claude Fable 5.1: round 1 independent plans, round 2 the four disputes below, rounds
3 and 4 the replay-LSN cache. Extends [visibility-gate-design.md](./visibility-gate-design.md) (P1–P10); the rows
here are numbered R1–R13 (R13 added 2026-09-20 in review); the replica-discovery rows R14–R20 (agreed 2026-09-21),
R21–R31 (agreed 2026-11-11, after an incident; the Patroni REST source in R22–R27 and R31 is superseded) and R32–R40
(DNS discovery, agreed 2026-11-11, which replaces the Patroni REST source) are at the end.

## Problem in two sentences

The visibility guard certifies that a transaction is visible on the **primary** when its first event is dispatched;
it says nothing about standbys, which apply the commit record later (`synchronous_commit = on` is a flush guarantee,
not an apply guarantee). The documented consumer-side rule (P7, strict `pg_last_wal_replay_lsn() > CommitLSN` on the
same server as the read) only works when the consumer's pool pins check and read to one standby, which application
pools rarely guarantee.

## Agreed decisions (R1–R13)

| # | Decision |
|---|---|
| R1 | **Scope: both.** `replication.WaitReplayed(ctx, q, commitLSN, opts)` is the exported primitive: the consumer runs check and read on its own pooled connection, same server. The in-library gate is built on the same poll and configured as `visibilityGuard.replicas: ["host:port", ...]`; it requires `visibilityGuard.enabled: true`. Guarantee: every listed replica has applied the commit when the handler runs. Limit: unlisted hosts and pooler routing are not covered. List direct standby hosts, never a pooled or load-balanced endpoint. |
| R2 | **Target set: static list, no discovery.** `pg_stat_replication.replay_lsn` rejected: the standby reports its apply position immediately only for `remote_apply` commits (`xact_redo_commit` → `XLogRequestWalReceiverReply` when `XactCompletionApplyFeedback`), otherwise on the next flush reply or `wal_receiver_status_interval` (10 s default); non-privileged roles see only `pid` (`pg_read_all_stats`); cascading standbys are absent; no `application_name` → host mapping. Patroni REST discovery: new dependency, new failure mode. |
| R3 | **Predicate: one autocommit statement per poll.** `SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn(), (pg_control_recovery()).min_recovery_end_timeline`. Pass iff in recovery ∧ `replay > CommitLSN` (strict, compared on the 64-bit value) ∧ timeline ≤ `s.system.Timeline`. `pg_is_in_recovery() = false` or a NULL replay position ⇒ error (the "replica" is a primary, a promoted standby, or a pooler routed to one), never "applied". A server answer is classified **before** the deadline is examined: an answer that arrives as the shared budget runs out stays fatal instead of being reported as a timeout, which `failMode: open` would dispatch on. Only a poll cut by the wait's own deadline is the timeout. `CommitLSN == 0` (snapshot events) skips the gate; heartbeat events already bypass it. |
| R4 | **Timeline: `(pg_control_recovery()).min_recovery_end_timeline`.** `UpdateMinRecoveryPoint` writes the replay timeline (`GetCurrentReplayRecPtr(&tli)`) on every buffer flush during recovery (`XLogFlush` → `UpdateMinRecoveryPoint`), and `pg_control_*` is not REVOKEd, so no extra grant. Rule is `<=`, not `==`: on an idle standby the control file lags after a failover until the next buffer flush or restartpoint, and `==` would refuse to start. `>` means the replica followed a newer primary ⇒ error ⇒ stream restart. **Residual race (documented):** a standby already receiving the new timeline but not yet flushed a buffer reports the old one. The real closure is P5 (synchronous replication + fail-closed primary guard), which is one more reason `replicas` requires the primary guard. Rejected: `pg_stat_wal_receiver.received_tli` (needs `pg_read_all_stats`, verified in `walreceiver.c` `pg_stat_get_wal_receiver`: unprivileged users get only `pid`; the row disappears while the walreceiver restarts) and `pg_control_checkpoint().timeline_id` (refreshed only at restartpoints). |
| R5 | **Down or lagging replica: fail closed, but reconnect.** Replica connections reconnect inside the wait (backoff shared with the poll loop, open checks re-run, R13 included). A standby shutting down answers `57P01`/`57P02` (or a class `08` connection exception) on the open connection before it closes; that is a restart, not a verdict, and is redialed like a reset rather than failing the stream. This deliberately diverges from P2: the primary guard is bound to the replication session and its loss *is* a stream failure, while a standby restart is routine and must not restart the CDC stream. No per-replica skip, no second knob (P3 stands). A replica unreachable at startup fails startup. |
| R6 | **Replay cache, per connection, bounded to 1 s** (revised 2026-09-23, replaces "no cache in v1"). Per-transaction polls cap throughput at 1/(RTT × replicas) and pinned a high-write slot in production; the age-based `replicaBypass` (c30de08) was rejected and reverted because it switches on exactly when a standby lags (event age is mostly the replica wait itself), compares two clocks, and is the skip-lagging mode the do-not list forbids. Instead, like the primary's `xmin` shortcut: each `replica` keeps the replay position of its last poll that passed every check (in recovery, timeline, R13 on dial) with the poll's **start** time. A later transaction passes without a query iff the connection is open (`!IsClosed()`), `replayed > CommitLSN` (strict, as the poll) and both the monotonic and the wall-clock age are in `[0, 1 s)` (monotonic stops during host suspend, wall steps). The cache is zeroed on dial and on every dropped connection, never written from a rejected answer. Soundness: `pg_last_wal_replay_lsn()` only moves backwards across a standby restart, which kills the backend; promotion, `pg_wal_replay_pause()`, `recovery_min_apply_delay` and cascading never un-apply a lower LSN. The unobserved kill (half-open socket), a re-provisioned `host:port`, and the skipped in-recovery/timeline checks are bounded by the 1 s ttl and fall in R12(b)'s class. Constant, not config (R5: no second knob). Observability: `go_pq_cdc_visibility_replica_checks_total{replica,result=cached|polled}` (added on review request) and `cached_age_ms` on the wait-completed log line; the lag gauge is not written on a hit (the poll that filled the cache already set it to 0). `WaitReplayed` keeps polling: a consumer's pooled connection has no stable backend. Rejected earlier and still rejected: `CheckConn` (deprecated in pgx v5.9.2, a 1 ms blocking read) and `Ping` (costs the round trip). Pre-existing, not widened: the orphaned-history false positive (Correction to R4) and a pooler swapping the backend (R10 precondition). Decided by Codex, Claude Opus and Claude Fable 5.1 in two rounds. |
| R7 | **One budget.** The existing `visibilityGuard.timeout` covers the primary wait plus all replicas under a shared deadline; the `wal_sender_timeout / 2` startup check is unchanged. Replicas are polled **concurrently** (revised 2026-09-27 on the maintainer's request, replaces "sequentially"): one goroutine per replica whose cache (R6) does not already certify `CommitLSN`, each touching only its own connection and cache, so the cost is about max lag plus one round trip instead of N. A server answer (fatal) cancels the other polls and is reported before a sibling's timeout, which `failMode: open` would dispatch on; `visibility_timeout_total` is counted once per wait. `open` still dials one after another. |
| R8 | **Order: primary guard first, then replicas**, both once per xid on the first message (`lastGatedXid`). Neither implies the other: a standby acks flush before apply, and the primary's `ProcArrayEndTransaction` runs after `SyncRepWaitForLSN`, so a standby can have applied while the primary is not yet visible, and the primary is usually visible long before an async standby applies. |
| R9 | **Metrics.** The three existing visibility metrics measure the whole gate (primary + replicas; the histogram is observed once per gated transaction in `gate`). One addition: `go_pq_cdc_visibility_replica_lag_bytes{replica="host:port"}`, set on every poll to `CommitLSN − replay` (0 once applied). Label is `host:port`, never a DSN. |
| R10 | **Config.** `visibilityGuard.replicas` list of `host:port`; credentials and database come from the main config exactly as the primary guard's DSN does. Empty list = off. Validation: requires `enabled: true`, `host:port` form, no duplicates. At open every replica must be in recovery on a timeline ≤ the session's, else startup fails. |
| R11 | **Tests.** Unit: predicate table (replay below, equal, above `CommitLSN`: only above passes), NULL replay and `in_recovery = false` ⇒ error, timeline above session ⇒ error, reconnect after a network error, `CommitLSN == 0` bypass, both `failMode`s, shared deadline, config validation. Integration (PR 2): lift `example/replica-read`'s compose (standby2 `recovery_min_apply_delay = 3s`) into `integration_test/`; assert dispatch is held ≥ 3 s and the row is readable on standby2 at dispatch; the split-standby case passes with both listed; `pg_wal_replay_pause()` for a deterministic timeout → restart test. |
| R12 | **Accepted trade-offs.** (a) Fail-closed on a chronically lagging replica is a restart loop by design: keepalive replies stop while the process loop is gated, so "keep waiting" is not available; raise `timeout` and `wal_sender_timeout` together, or use `failMode: open` with alerting on `visibility_timeout_total`. (b) A standby restart briefly rewinds visibility below an already certified `CommitLSN` (replay restarts at `RedoStartLSN`; hot standby accepts connections at `minRecoveryPoint`); stated **inside** the README guarantee paragraph, consumer retry covers it, no restart detection in the poll. |
| R13 | **Cluster identity, on every connection.** `SELECT (pg_control_system()).system_identifier` per replica on every dial (open and each reconnect), compared with `IDENTIFY_SYSTEM`'s. A `host:port` from another cluster passes every other check (it *is* a standby, in recovery, and its replay position is unrelated to `CommitLSN`, so the strict rule passes at once) and silently certifies a read that never happened. Not per poll: a backend cannot change identity without the connection breaking, and the reconnect is exactly where the check runs again (a `host:port` can be re-provisioned or re-routed between two dials); R6's reasoning applies to the replay position, not to this. `pg_control_system()` needs no grant either. The two sides render the value differently — `IDENTIFY_SYSTEM` unsigned, `pg_control_system()` as `int8` — so they are compared as `uint64` bits, never as text. |

## Build order

1. **PR 1 — core.** `pq/replication/replica.go`: `Querier`, `WaitOptions`, `WaitReplayed`, `ErrNotStandby`, the shared poll and backoff, the internal `replicaGuard` (dial, reconnect, open checks, timeline rule, lag gauge). `stream.gate(ctx, xid, commitLSN)`; replicas opened after the primary guard in `Open`, closed after the process goroutine in `Close`. `config.VisibilityGuardConfig.Replicas` + validation + `Config.ReplicaDSN`. Metric interface + gauge. Unit tests from R11.
2. **PR 2 — integration harness.** Compose fixture under `integration_test/`, the assertions from R11, CI matrix entry.
3. **PR 3 — docs.** README "Visibility Guard" gains the `replicas` block, the guarantee and its limits (R12 (b) inside the guarantee paragraph); `visibility-gate-design.md` gets a pointer here and the P2 divergence; `example/replica-read` handler uses `WaitReplayed` and gains a step 5 (guard on, `strict = true visible = true` at event time); permissions table: no new grant, `pg_hba.conf` must allow normal connections to the standbys.

## Do not

- Do not use `pg_stat_replication`, as predicate or for discovery.
- Do not use `>=`; do not fold check and read into one statement (the snapshot is taken before the function runs).
- Do not treat NULL `pg_last_wal_replay_lsn()`, `pg_is_in_recovery() = false`, a timeout or a connection failure as "replayed".
- Do not call `pg_walfile_name()` on a standby (ERRORs in recovery) or `pg_control_checkpoint().timeline_id` (restartpoint-stale).
- Do not reuse a replay position across connections, beyond 1 s, from a rejected answer, or in `WaitReplayed` (R6). Do not skip replica waits based on event age.
- Do not add a per-replica timeout, a skip-lagging mode, a quorum ("any N of M") or a circuit breaker.
- Do not gate per message, at `COMMIT`, or inside `sink()`; do not gate heartbeat or snapshot events. Do not dispatch a message whose transaction has no decoded `BEGIN` (xid 0 or `CommitLSN` 0 in the process loop): nothing can be certified for it, so the gate fails closed. A transaction is `(xid, CommitLSN)`, never xid alone.
- Do not list a pooled or load-balanced endpoint in `replicas`.

## Source references (PostgreSQL master, 2026-09-20)

`xlog.c` `UpdateMinRecoveryPoint` (`newMinRecoveryPoint = GetCurrentReplayRecPtr(&newMinRecoveryPointTLI)`), `XLogFlush` recovery branch (`if (!XLogInsertAllowed()) { UpdateMinRecoveryPoint(record, false); return; }`). `walreceiver.c` `pg_stat_get_wal_receiver` (`has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS)`, else only `pid`); `XLogWalRcvFlush` sets `receivedTLI` after `issue_xlog_fsync`. `xact.c` `XACT_COMPLETION_APPLY_FEEDBACK` only for `synchronous_commit >= remote_apply`; `xact_redo_commit` is the only caller of `XLogRequestWalReceiverReply`. `xlogrecovery.c`: replay position resets to `RedoStartLSN` at recovery start. pgx `v5.9.2` `pgconn/pgconn.go` `CheckConn` (deprecated, 1 ms `ReceiveMessage`).

## Replica discovery (R14–R40)

Status: R14 and R16–R19 decided 2026-09-21 by a three-round consensus (round 1 independent positions, round 2 seven
disputes, round 3 the freshness budget); R21–R31 decided 2026-11-11 after an incident (below) by three reviewers;
R32–R40 decided 2026-11-11 after a review of the Patroni REST source, which the maintainer then replaced by role-tagged
service DNS names. R15 (Consul source) is **superseded** by R22 (Patroni REST `/cluster`), and R22–R27 are **superseded**
by R32–R37 (DNS); R14's YAML deferral and R20's exclusions of a YAML block and of a reference resolver are **amended** by
R38 and R32; R28 now describes the `discovery.dns` block (R38), R31's Patroni residuals are replaced by R39. Superseded
rows are kept as history and say so. **Implemented:** the core (R14, R16–R19, R21, the logging part of R27, R29:
`config.VisibilityGuardConfig.ResolveReplicas`/`RefreshInterval`, `config.ReplicaMember`, `pq/replication/replica.go`,
`metric.DeleteVisibilityReplicaLag`), the reference resolver (R32–R37: package `dnsdiscovery`) and the YAML block
(R38) in the same change on top of that core; the integration tests (R40) are built in the same change. Supersedes R2's
"no discovery" for the library seam only; the static list stays the default.

### Problem

Every failover or topology change of the standby set breaks a static `replicas` list (the wording below is Patroni's,
the incident that motivated discovery). A listed standby that is promoted answers
`pg_is_in_recovery() = false` at `Open` → `ErrNotStandby` → the process exits and crash-loops until the list is edited.
Worse and silent: the demoted leader, once rewound and rejoined as a standby, is not listed and is never covered again
while the connector looks healthy. A decommissioned standby is a fail-closed restart loop; a new standby is uncovered
until someone edits the config.

### Verified facts (Patroni master sources and docs, 2026-09-21)

The Consul facts below are the history of R15; R22 (superseded) relied on the Patroni v4.0.6 `/cluster` facts listed with
R21–R31. The DNS facts R32–R37 rely on are listed with them.

- `ha.py touch_member` writes the member record `{conn_url, api_url, state, role, version, tags, xlog_location,
  replay_lsn, receive_lsn, replication_state, timeline, ...}` every HA loop (`loop_wait`, default 10 s). `timeline`
  is the member's **own** timeline (`pg_stat_wal_receiver.received_tli`, else `IDENTIFY_SYSTEM` on a replication
  connection to itself), published only in state RUNNING/RESTARTING/STARTING and only when it could be determined.
- `dcs/consul.py`: KV keys `service/<scope>/leader` (value = leader member name) and `service/<scope>/members/<name>`
  (the JSON above), both bound to a Consul session with `behavior = delete` (TTL, default 30 s). With
  `consul.register_service: true` the node registers service name `service_name_from_scope_name(scope)`, id
  `<scope>/<name>`, address/port from `conn_url` (the PostgreSQL endpoint), tags = role (`primary` plus `master`,
  `replica`, `standby-leader`) + `service_tags`, check = HTTP GET `api_url/<role>` every `service_check_interval`
  (5 s), deregistered after 10 × ttl; registered only for primary/replica/standby_leader in RUNNING, removed on STOPPED.
- REST `/replica` is 200 iff state `running` ∧ role `replica` ∧ no `noloadbalance` tag (`?lag=` optional). It has no
  timeline term. So the consumers' `replica.<scope>.service.consul` set = Consul health, tag `replica`, passing.
- `utils.py cluster_as_json` (`GET /cluster`): role ∈ {leader, standby_leader, sync_standby, quorum_standby,
  replica}; state = `replication_state` (`streaming`, `in archive recovery`) for non-leaders else `state`.

### Correction to R4

R4 says "the real closure is P5". P5 (synchronous replication + fail-closed primary guard) prevents *phantom* events
(a transaction visible on A is flushed on B and survives promotion). It says nothing about this case: A crashes at
X_A; B (sync) flushed X_B; C (async) received X_C with X_B < X_C ≤ X_A. B promotes on timeline N+1 at X_B. A **new**
commit L ∈ [X_B, X_C) is certified by C, whose replay position X_C > L on an orphaned history and whose
`min_recovery_end_timeline` N ≤ N+1 both pass, while `/replica` keeps C routable. The consumer reads C and gets
NOT_FOUND. The static design has this hole (≤ `loop_wait` + rewind, until Patroni reattaches C); R19 narrows it in
discovery mode when the source supplies timelines; static users keep it, and so do DNS users (R36), and the README
says so.

### Agreed decisions

| # | Decision |
|---|---|
| R14 | **Seam: a resolver hook, not a client.** `config.VisibilityGuardConfig.ResolveReplicas func(ctx) ([]config.ReplicaMember, error)` (`json:"-" yaml:"-"`), `ReplicaMember{Addr string; Timeline int32}` with `Timeline == 0` = unknown. Mutually exclusive with `Replicas`, requires `enabled: true`. Resolver output is validated like the static list (`host:port`, no duplicates): a bad entry is an error, never a silent drop. The doc comment carries the contract verbatim: *return an error if you cannot prove the cluster was reached and correctly identified; a non-nil empty slice asserts "proved, zero routable replicas"; a nil slice with a nil error is an invalid answer, never an empty set*. YAML users: a func field cannot come from `NewConnectorWithConfigFile`; the 09-21 deferral to a `consul:` block was replaced by R28's YAML block (first `discovery.patroni`, now `discovery.dns`, R38). |
| R15 | **Superseded by R22 (2026-11-11), which R32 in turn supersedes; kept as history.** **Reference resolver `patroni.Replicas(agentAddr, scope)`**, its own one-file stdlib package (net/http + encoding/json), exported because every service would otherwise copy it. Membership from Consul health: one `GET /v1/health/service/<scope>?passing=true` (no tag filter); a passing `primary`/`master`-tagged member proves the cluster, else error; members tagged `replica` give `Service.Address:Service.Port`. Timeline joined from `GET /v1/kv/service/<scope>/members/?recurse` (values base64 JSON, field `timeline`) on member name (health service id `<scope>/<name>` ↔ KV key `service/<scope>/members/<name>`); missing → 0. The doc comment names the assumptions: `register_service: true`, service name = scope, Patroni role tags, the `/replica` check semantics. Covers *routability* (the set consumers are routed to, `noloadbalance` and failing checks excluded by construction), uses the local agent, needs no Patroni API URL during a failover. Patroni REST `/cluster` rejected as the source: superset of the routable set, needs a live API address first. |
| R16 | **Only the source's answer changes membership.** Refresh runs lazily at the top of `wait()` when the set is older than `refreshInterval` (default 10 s = Consul check 5 s + `loop_wait` 10 s propagation floor), resolver call bounded at 1 s (≤ 10 % of the default gate budget, zero calls on an idle stream). No goroutine, no mutex: `Open` runs before `go process`, `Close` after `<-processEnd`. Any member error (`errReplicaConn`, `ErrNotStandby`, timeline ahead, cluster mismatch, PgError) triggers **at most one optional re-resolve per wait in total**; the member is dropped iff the source no longer lists it, otherwise the error is classified exactly as today. A poll blocked only by R19 spends that same optional re-resolve. This budget governs optional re-asks only; a *due* refresh (R17) is mandatory and unaffected by it. This is not a per-replica skip or circuit breaker: the error decides *when* the source is asked, the source alone decides *who* is a member, and a lagging listed member still blocks (R12(a)). |
| R17 | **Freshness budget.** The resolved set is trusted for one `refreshInterval`, its age counted from the start of the resolve call that produced it, and **it must be within `refreshInterval` at certification time**, not only when the wait starts. A refresh that is **due** (the set is older than `refreshInterval`) is mandatory: it is retried inside the gate with the poll backoff under the shared deadline, and the set does not certify until it succeeds; an answer already older than `refreshInterval` when it arrives (a resolver slower than the interval) is installed but counts as not yet refreshed and is retried the same way. A wait whose set ages past `refreshInterval` before it certifies (a long round, the optional re-resolve) runs the mandatory refresh again and repeats the round on the refreshed set under the same deadline: members certified earlier in the wait and still listed stay certified, added members are dialled, checked and polled. A resolver consistently slower than the interval therefore ends in the timeout path, never in a certification. At the deadline the gate returns `ErrVisibilityTimeout`, so `failMode` applies (closed: restart, `Open` re-resolves or fails; open: dispatch with the warning and the counter). A 1–3 s Consul agent restart is absorbed; a long outage fails closed like R12(a). Stated plainly in the README: with a resolver configured, the discovery source becomes a liveness dependency of the stream, the one place where a component otherwise off the CDC critical path can hold dispatch. At `Open` a resolver error fails startup. An **explicit non-nil empty slice is legitimate** (a nil slice is invalid, R14; "cluster proven, zero routable replicas": a two-node cluster during a switchover, a single replica being reinitialised) → primary-only gating, logged at **Warn** with the empty set, so operators can alert on it. |
| R18 | **Reconcile = diff a slice.** Removed → `conn.Close`, drop the `*replica`, `metric.DeleteVisibilityReplicaLag(name)` (new method; the GaugeVec otherwise leaks the label). Added → append `replica{name, dial}`; its first poll dials and runs the full checks (R13 system identifier, in recovery, timeline ≤ session) under the shared deadline. Every membership change is logged (Info) with the full set. `Open` resolves once, then dials and checks every member as today; a member on a stale timeline or merely behind does not fail `Open` (CrashLoopBackOff outlives every condition Patroni fixes on its own). |
| R19 | **Timeline rule (discovery only): block, do not exclude, do not call it a closure.** A routable member whose `Timeline` is known and ≠ the session's is treated as *not yet replayed*: it blocks until the shared deadline → timeout → `failMode`; never fatal, never fails `Open`. Staleness is directional (a record reads N+1 only after the member itself reported N+1), so a stale record can only over-block. `Timeline == 0` turns the rule off for that member: a residual hole, since `touch_member` omits `timeline` when it could not determine one. Cost: after a failover the gate is held ≤ `loop_wait` + rewind until every routable member follows N+1 (one or two restarts of `timeout` under `closed`); the trade is "consumers silently read a divergent replica" → "CDC stops", the same as R12(a). The per-poll `min_recovery_end_timeline > session` fatal rule (R4) is unchanged. |
| R20 | **Out of scope** (amended 2026-11-11: DNS names are the reference source, R32, and the YAML block is R38; Patroni REST was the reference source between R22 and R32). Leader discovery for the connector's own `host` (leader DNS/proxy plus break-and-restart *is* the failover handling; owning it means owning slot existence on the new leader). DNS SRV or Consul sources, YAML blocks for any source but R38's, per-source adapters, a background watcher or Consul blocking queries, quorum, per-replica skip, circuit breaker, "drop after N absences" grace windows, lag- or `replication_state`-based exclusion in the library (Patroni `/replica?lag=` and tags express that on the routing side), a max-staleness constant beyond `refreshInterval`, new metrics beyond the gauge deletion, a Consul or Patroni stack in CI. |

### Discovery after the incident (R21–R31, agreed 2026-11-11; the Patroni REST source is superseded by R32–R37)

Incident: the static `replicas` list covered only region-A standbys; a read-only proxy started routing region-B
consumers to region-B standbys, which were not gated, and they read Not Found. Topology: region A is the primary
Patroni cluster (leader, sync_standby, replica); region B is a Patroni **standby cluster** with the same scope name and
the same system identifier (a `standby_leader` cascading from a region-A replica, plus replicas cascading from it).
`GET /cluster` in each region lists only that region's members; one stable DNS name per region reaches the Patroni
REST API on :8008 (Patroni 4.0.6). This is the context of R22–R27, which were later replaced by DNS names (R32–R37);
what survives is the core (R14, R16–R21, R28–R30) and the lesson that coverage must span every region consumers read from.

Verified Patroni v4.0.6 facts: `/cluster` reads the DCS on every request and gives no HTTP response on a DCS failure
(never a stale list); members carry `name, role, state, api_url, host, port, timeline, lag, tags`; `/replica` is 200
only for role `replica` + state `running` + no `noloadbalance` (503 for a `standby_leader`); the `leader` role comes
from the leader key.

| # | Decision |
|---|---|
| R21 | **Seam and lifecycle (D1).** R14, R16–R19 unchanged. `config.VisibilityGuardConfig.ResolveReplicas func(ctx context.Context) ([]config.ReplicaMember, error)` (`json:"-" yaml:"-"`), `ReplicaMember{Addr, Source string; Timeline int32}` (`Timeline` 0 = unknown; `Source` names where the member was found and is used for logs only, added in implementation so the core can group the set by source). Mutually exclusive with `Replicas`; requires `enabled: true`. The answer is validated like the static list (`host:port`, no duplicates, and a timeline is never negative): a bad entry fails the answer, never a silent drop. A non-nil empty slice = "proved, zero routable replicas" → primary-only gating, Warn; a nil slice is invalid. An answer the core finds invalid (bad `host:port`, duplicate, negative timeline, nil slice) is a definite wrong answer, not an outage: it is returned at once as a non-timeout error that `failMode` never turns into a pass, whereas an error the resolver returns (unreachable, no leader, same cluster) is retried under the shared deadline and ends as a timeout. The trust window is `VisibilityGuardConfig.RefreshInterval` (`json:"-" yaml:"-"`, default `config.DefaultReplicaRefreshInterval` = 10 s, the typical health-check interval plus propagation; R16's Consul arithmetic is history); R38's `refreshInterval` sets it. |
| R22 | **Superseded by R32 (2026-11-11); kept as history.** **Reference resolver = Patroni REST `/cluster` (D2), replaces R15.** Package `patroni` (stdlib only: `net`, `net/http`, `encoding/json`), `patroni.Replicas(urls ...string)` returns the resolver func. Each URL is the base URL of **one** Patroni cluster (one source); the resolver requests `<url>/cluster`. |
| R23 | **Superseded by R32 (2026-11-11); kept as history.** **Per source, inside the resolver call's bound (R16: 1 s) (D3).** The URL host is resolved with a context-aware resolver (the lookup counts against the bound); a literal IP is used as is; zero addresses = source failure; a resolver built with no URL fails every resolve. All addresses are queried concurrently, the first valid answer wins, the rest are cancelled. Valid = HTTP 200, JSON parses, R24's proof holds and R25/R26's mapping passes (a gated member without a usable host and port makes the address invalid, so a fast malformed answer cannot cancel a healthy one). The IP is dialled but the URL host is kept for the `Host` header and for TLS SNI and certificate verification. Any 3xx is an error (no redirect following); the body is capped at 1 MiB; bodies are closed; keep-alives are off so every refresh re-resolves DNS. If every address fails: one error naming each address and its reason. |
| R24 | **Superseded by R33 (2026-11-11); kept as history.** **Proof per source (D4):** the answer contains a member with role `leader` or `standby_leader`; otherwise the source **fails** (for example the standby-leader key expired for up to `ttl`). Never confused with "zero gated members". |
| R25 | **Superseded by R32 and R39 (2026-11-11); kept as history.** **Gated members (D5)** = every member whose role is not `leader` (`replica`, `sync_standby`, `quorum_standby`, `standby_leader`, and any unknown role), filtered by state. Not gated, matched exactly: `stopped`, `stopping`, `stop failed`, `starting`, `starting after custom bootstrap`, `start failed`, `restarting`, `restart failed` and `restart failed (<state>)`, `crashed`, `creating replica`, `initializing new cluster`, `initdb failed`, `running custom bootstrap script`, `custom bootstrap failed`. Gated: `running`, `streaming`, `in archive recovery`. Any other state, including one that merely begins with `starting` or `restart failed`: gated **and** reported. `tags.noloadbalance == true` excludes only roles `replica`, `sync_standby`, `quorum_standby`; a `standby_leader` is always gated when its state passes. No lag-based filtering. |
| R26 | **Superseded by R32 and R36 (2026-11-11); kept as history.** **Member fields (D6):** `Addr` = `host:port` from the member's `host` and `port`; `Timeline` = the member's `timeline`, missing → 0. |
| R27 | **Superseded by R34 (2026-11-11); kept as history; the logging sentence is implemented by the core.** **Union across sources is all-or-nothing (D7):** any source failing fails the whole resolve (never a partial set). No dedupe: the same `host:port` from two sources is an **error** ("two sources resolved to the same cluster"), compared over each source's complete membership (leader, `standby_leader` and members the state or `noloadbalance` filters drop included) before filtering; region B's name resolving to cluster A is otherwise invisible because scope and system identifier are identical. A source with zero gated members is legitimate but logged at Warn whenever the set changes. The guard logs the resolved set grouped by `Source` on every change (Info; Warn when the whole set is empty); a source that contributes no member is invisible to the guard, so that Warn belongs to the resolver. |
| R28 | **YAML block (D8)**, mutually exclusive with `replicas` and with the Go hook; it only builds the reference resolver. The block was first `visibilityGuard.discovery.patroni.{refreshInterval, urls}` (superseded) and is now `visibilityGuard.discovery.dns` (R38). `config` must not import the resolver package (which imports `config` for `ReplicaMember`): the resolver is built where the connector builds the stream. |
| R29 | **Failover (D9).** A member promoted while the stream runs answers "not in recovery" on a poll; the wait's optional re-resolve (R16) no longer lists it → dropped, no crash loop. If the source still lists it, the error is fatal and the stream restarts (with DNS this is the bounded-in-practice loop of R37). The old leader rejoining as a replica appears in the source and is gated by the next refresh. |
| R30 | **Not in v1 (D10):** auth or mTLS fields, a Consul client, lag- or state-based exclusion (the source's health checks decide who is listed), proxy probing, removal of the static `replicas`. A deployment that needs these uses the Go hook. |
| R31 | **Superseded by R39 (2026-11-11) except its last clause (coverage is an operator contract); kept as history.** **Documented residuals (D11)**, in the README guarantee paragraph next to "narrows, not closes": a replica that loses its DCS connection drops out of `/cluster` after `ttl` while its own `/replica` check may still answer 200, so a proxy can route to it ungated for about `ttl + refreshInterval`; a node that just started can be routed for up to about `loop_wait + refreshInterval` before it is gated (R12(b)'s class); the `noloadbalance` exclusion assumes routing that honours `/replica` (deployments routing by `/read-only` or ignoring the tag use the hook or the static list); with discovery configured the Patroni REST API, and through it the DCS, is a liveness dependency of the stream; coverage is an operator contract: one URL per cluster consumers read from, safety needs routed ⊆ gated, and the library does not verify it. |

### DNS discovery (R32–R40, agreed 2026-11-11, replaces R22–R27 and R31)

The maintainer removed the Patroni REST `/cluster` resolver: membership comes from role-tagged service DNS names, one
per region (`replica.<scope>.service.services.<region>.<domain>`, Consul DNS behind a corporate forwarder). The core
(R14, R16–R21, R28–R30) is unchanged. Facts relied on: Consul answers a DNS service query with the passing instances
(warning-state ones too unless `only_passing`), cuts a UDP answer to `udp_answer_limit` (3) records when the query has
no EDNS, silently; `max_stale` and forwarder TTL floors age answers; a Patroni `standby_leader` is not tagged `replica`.

| # | Decision |
|---|---|
| R32 | **Package `dnsdiscovery` (root module, stdlib only), `Replicas(Options{Names, Port, Nameserver}) func(ctx) ([]config.ReplicaMember, error)`.** Each name is one source. Per name two lookups, `ip4` and `ip6` (`Resolver.LookupNetIP`), never one combined A/AAAA lookup, plus a third unfiltered `ip` lookup as a cross-check (`LookupNetIP("ip6")` silently drops an AAAA record that holds an IPv4-mapped address, `::ffff:a.b.c.d`; an address only the `ip` lookup returns fails the name, an `ip` error that is not "not found" fails it too, and members are unmapped), with `net.Resolver{PreferGo: true, StrictErrors: true}`. The name is made absolute (trailing dot) so a search list can never answer for it. A lookup error that is not "not found" (`*net.DNSError` with `IsNotFound`: NXDOMAIN or no data) fails the source. The one exception is an `ip6` lookup that reports "no suitable address found" (every AAAA record IPv4-mapped), which the cross-check covers; a cross-check that finds nothing while a family returned records fails the source. One family absent is fine only when the other returned records. `Addr = net.JoinHostPort(ip, port)`, `Source` = the configured name, `Timeline` = 0. An address repeated inside one name is collapsed. The lookups run inside the resolver call's existing 1 s bound; names are looked up concurrently. Invalid options (no name, bad port, a nameserver that is not a literal `ip:port`) make every call fail. An unexported lookup seam serves the unit tests. |
| R33 | **Empty answer or any lookup error = source failure, never "zero replicas".** No `allowEmpty`, no proof name. Both families absent is the empty answer. Cost: a region with zero passing standbys stops the stream under `failMode: closed` (and times out under `open`). Supersedes R24's proof of a leader. |
| R34 | **Union is all-or-nothing:** any failing name fails the whole resolve, and the same `host:port` under two names is an error (two names resolving to the same standby), never a merge. Supersedes R27. |
| R35 | **`nameserver` is optional and must be a literal `ip:port`** (a host name is a config error: it would need resolving itself). When set, every query goes to it over TCP (`Resolver.Dial` returns a TCP connection, which makes the Go resolver frame the query as a stream), meant for a Consul agent's DNS port: no forwarder hop. When unset the system resolver configuration is used and **one Warn is logged at startup** naming the risk: through a forwarder that queries without EDNS, Consul cuts the answer to 3 records silently; set `nameserver` or confirm the path returns complete answers. No "two identical answers" heuristic. Why optional: the deployment this was built for observed the names through the corporate forwarder and may not reach an agent DNS port from its pods; a required nameserver would block it. The contract goes to the operator in the README. |
| R36 | **Timeline is unknown in DNS mode.** Members carry Timeline 0, so R19 is off and the R4 orphaned-history hole (Correction to R4) is open, as it is for the static list; against the removed Patroni resolver it is a regression, against the static list it is not. The README states the guarantee in DNS mode as **conditional** on routing never sending reads to a standby on an orphaned history. No other unprivileged timeline source exists (`pg_stat_wal_receiver.received_tli` needs `pg_read_all_stats`, see R4); the Go hook stays for deployments that can supply timelines. |
| R37 | **Promotion while the name still lists the promoted node:** the member answers `ErrNotStandby`, the optional re-resolve still lists it, the error is fatal, the stream restarts and `Open` fails the same way until the name stops listing the node: typically about one health-check interval. No core change. There is no hard bound if the health publisher stalls or DNS is stale, and an orchestrator's restart backoff (for example CrashLoopBackOff) can stretch a few seconds of DNS lag into a minute of downtime. Liveness only, never a wrong certification; documented. |
| R38 | **YAML block**, exclusive with `replicas` and with the Go hook, requires `enabled: true`: `visibilityGuard.discovery.dns.{refreshInterval, port, nameserver, names}`. `names`: at least one, each a valid DNS name (not an IP, no port or scheme), no duplicates compared case-insensitively with a trailing dot ignored. `port`: 1-65535, default the main connection's `port`. `nameserver`: literal `ip:port`, optional. `refreshInterval` > 0, default 10 s. Wiring: `visibility_discovery.go` builds the resolver after `Validate` and after the logger is installed (the resolver warns at construction). Replaces the `discovery.patroni` block. |
| R39 | **Documented residuals (README):** forwarder completeness (the 3-record cut); `max_stale` and forwarder minimum-TTL caches bound answer freshness; `/etc/hosts` overrides; warning-state instances are returned unless Consul `only_passing` is set; a standby added or removed is picked up after the health-check interval plus up to `refreshInterval` plus those caches; DNS (and the registry) is a liveness dependency of the stream; coverage is an operator contract (a `standby_leader` is not under `replica.`: list its name if anything routes reads to it; a static list must cover every region too); the R4 hole (R36); the promotion restart loop (R37); message integrity is trusted to the standard library resolver, which accepts a TCP answer that still carries the truncation flag and can return the records parsed before a malformed tail (upgrade path: a message-level DNS client that rejects truncated or partially parsed answers). |
| R40 | **Tests.** Unit (`dnsdiscovery`, injected lookup, plus the real resolver against a TCP-only in-test DNS server): IPv6 join, absolute name, both families, one family absent, a non-"not found" failure of one family fails the source, empty, more than 3 records, a repeat inside a name, the same address under two names, one failing name fails the union, names looked up concurrently, invalid options (a host name as nameserver), the startup Warn, TCP forced. Config: block validation, defaults (port from the main connection), YAML and JSON decoding. Wiring: the YAML block reaches `dnsdiscovery.Options`. Integration (real PostgreSQL standbys, real YAML path, an in-test DNS responder reached through `nameserver`): two regions plus an incident control, member added, removed, promoted, source outage, the same address under both names, empty answer. Because a record carries no port, each standby is reached at its Docker network IP on port 5432 (`ContainerIP`), with no host port mapping for the gated addresses; where that IP is not reachable from the test process (Docker Desktop on macOS) the tests skip with a clear message, but never when `CI` is set. Loopback aliases (127.0.0.x) were rejected: ports Docker Desktop publishes there are not reachable from a sibling container, so there is no working local path. |

**How the core implements R16–R19 and R21** (`pq/replication/replica.go`). The set changes only on the goroutine that
runs `Open` and the gate, between two rounds of polls; no goroutine or mutex was added. `start` resolves once (an
error or an invalid answer fails `Open`), then dials and checks every member as with a static list; R19 is not
applied there. `wait` first runs a due refresh (`refresh`, when the set is older than `RefreshInterval` by the
monotonic or the wall clock, as R6's cache); it is retried with the poll backoff until it installs an answer that is
still younger than `RefreshInterval` on arrival or the shared deadline passes, which is `ErrVisibilityTimeout` naming
the resolver's last error. Before the wait certifies, the set's age is checked again; a set that aged past
`RefreshInterval` meanwhile is refreshed the same way and the rounds below repeat on the refreshed set (members
certified earlier in the wait and still listed are not polled again). Then one round of concurrent polls
(`round`): with discovery and the optional re-resolve unspent, a member error (a dropped connection, any server
answer, or a pass held only by R19) is not classified at once; it stops the round **between** two polls (never during
one, so sibling connections and caches survive), the source is asked once (`reResolve`), `apply` reconciles (closes
the removed, deletes their lag series, adds new members whose first poll dials and runs R13), a member the source no
longer lists is dropped, and every other held error is classified exactly as without discovery: a server answer is
fatal (it still outranks a timeout), the rest is retried. A second round, without the option, covers the reconciled
set; members certified in the first round stay certified unless R19 now blocks them. R19 is checked at certification
time, for a cached and for a polled pass. Static mode is the first round alone, as before.

### What happens on a promotion

Static: A demoted → walsender closes → `panic` → process restarts → reconnects to the leader DNS (B) →
`IDENTIFY_SYSTEM` timeline N+1 → `openReplicaGuard.open` dials B → not in recovery → `ErrNotStandby` → `Open` fails →
`connector.Start` returns → crash loop until `replicas` is edited; A, once rewound, is never covered again.

Discovery (R32's source, with the Timeline-0 caveat of R36): same up to `Open`. The name still lists B (it is now
the leader) until its health check turns the service tag over: B answers `ErrNotStandby`, the stream restarts, and
`Open` fails the same way until the name stops listing B (R37). A is stopped or rewinding and drops out of the name when
its check fails; C stays listed and is polled without a timeline rule, so the orphaned-history window of R4 is open
(R36). A rejoins within one health-check interval plus one refresh after it is passing again. Nothing is edited. A
member promoted while the stream is alive (outside a clean failover sequence, or a standby cluster promoted on purpose)
answers `ErrNotStandby` → one re-resolve → dropped if the name no longer lists it, fatal if it still does (R37).

### Guarantee sentence for the README

"With `ResolveReplicas` set, every standby that the resolver listed as a routable replica at the guard's last
successful refresh (at most `refreshInterval` old, plus the source's own propagation delay: with DNS names the health-check
interval and the answer caches, see R39) has applied the transaction when the handler is called. A standby that joined,
or was returned to the load balancer, more recently may not yet be covered. A routable standby whose timeline the source
reports and that differs from the replication session's holds the gate; DNS reports none (R36), so with
`discovery.dns` the guarantee is conditional on routing never sending reads to a standby on an orphaned history after a
crash failover."

Failure-handling paragraph, one added sentence: "With a resolver configured, the discovery source becomes a liveness
dependency of the stream: once the set is older than `refreshInterval` and a refresh has not succeeded, dispatch is
held and then follows `failMode`; closed restarts the stream, open dispatches with the warning and the counter."

### Build order (revised 2026-11-11: one change, in steps)

1. **Core (done).** `config`: `ReplicaMember`, `ResolveReplicas`, `RefreshInterval`, validation (exclusive with
   `Replicas`, `enabled: true`, no negative interval). `pq/replication/replica.go`: `start` (resolve at `Open`),
   `refreshIfDue`, `round`, `reResolve`, `apply`, R19 in the round's certification; the static list keeps its code path
   (no resolver, one round). `metric.DeleteVisibilityReplicaLag`. Unit tests with a fake resolver
   (`replica_discovery_test.go`); integration `TestReplicaGuardDiscovery` (one delayed standby listed, dropped to the
   empty set, re-added while streaming).
2. **`dnsdiscovery` package (R32–R37)**, unit tests as in R40. (A `patroni` package, R22–R27, was built first and
   removed the same day; its tests are history.)
3. **YAML block (R38)**, wiring and config tests; README: the guarantee sentence above, the R39 residuals, the rule
   that a static list must cover every region consumers read from, `ResolveReplicas` in the config table with the
   contract.
4. **Integration tests (R40).**

### Do not

- Do not let an error, a timeout or a lag value remove a member; only a successful resolve does.
- Do not certify on a set older than `refreshInterval` while a refresh is failing; do not make a refresh failure fatal
  either (it is a timeout under the shared deadline).
- Do not treat an empty resolved set as an error, and do not let a resolver return an empty slice without having proved
  the cluster; a nil slice is never "empty". The DNS resolver never returns an empty slice at all (R33).
- Do not exclude a routable member for a timeline mismatch; block on it. Do not make the mismatch fatal or fail `Open`.
- Do not use `pg_stat_replication` as the membership source; do not add Consul or Patroni client dependencies (the
  resolver of R32 is stdlib only); do not add YAML source blocks beyond R38's.
- Do not merge or dedupe sources: a `host:port` reported by two names fails the resolve (R34); a failing name
  fails it too, never a partial set. Do not read an empty answer as "zero replicas" (R33).
- Do not add a goroutine or a mutex for the refresh; do not re-resolve more than once per wait.
- Do not run Patroni or Consul in CI: the DNS source is tested against an in-test DNS responder (R40).
