# Replica guard — implementation handoff

Status: **implemented** (core guard, config and metrics; integration test `integration_test/replica_guard_test.go` against a standby with `recovery_min_apply_delay = 3s`; README "Visibility Guard" and "Commit LSN and reading from a standby"; `example/replica-read` step 5). Decided 2026-09-20 by a four-round consensus
between Codex, Claude Opus and Claude Fable 5.1: round 1 independent plans, round 2 the four disputes below, rounds
3 and 4 the replay-LSN cache. Extends [visibility-gate-design.md](./visibility-gate-design.md) (P1–P10); the rows
here are numbered R1–R13 (R13 added 2026-09-20 in review); the replica-discovery rows R14–R20 (agreed 2026-09-21, not implemented) are at the end.

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
| R6 | **Replay cache, per connection, bounded to 1 s** (revised 2026-09-23, replaces "no cache in v1"). Per-transaction polls cap throughput at 1/(RTT × replicas) and pinned a high-write slot in production; the age-based `replicaBypass` (c30de08) was rejected and reverted because it switches on exactly when a standby lags (event age is mostly the replica wait itself), compares two clocks, and is the skip-lagging mode the do-not list forbids. Instead, like the primary's `xmin` shortcut: each `replica` keeps the replay position of its last poll that passed every check (in recovery, timeline, R13 on dial) with the poll's **start** time. A later transaction passes without a query iff the connection is open (`!IsClosed()`), `replayed > CommitLSN` (strict, as the poll) and both the monotonic and the wall-clock age are in `[0, 1 s)` (monotonic stops during host suspend, wall steps). The cache is zeroed on dial and on every dropped connection, never written from a rejected answer. Soundness: `pg_last_wal_replay_lsn()` only moves backwards across a standby restart, which kills the backend; promotion, `pg_wal_replay_pause()`, `recovery_min_apply_delay` and cascading never un-apply a lower LSN. The unobserved kill (half-open socket), a re-provisioned `host:port`, and the skipped in-recovery/timeline checks are bounded by the 1 s ttl and fall in R12(b)'s class. Constant, not config (R5: no second knob). No metric; the wait-completed log carries `cached_age_ms`. `WaitReplayed` keeps polling: a consumer's pooled connection has no stable backend. Rejected earlier and still rejected: `CheckConn` (deprecated in pgx v5.9.2, a 1 ms blocking read) and `Ping` (costs the round trip). Pre-existing, not widened: the orphaned-history false positive (Correction to R4) and a pooler swapping the backend (R10 precondition). Decided by Codex, Claude Opus and Claude Fable 5.1 in two rounds. |
| R7 | **One budget.** The existing `visibilityGuard.timeout` covers the primary wait plus all replicas under a shared deadline; the `wal_sender_timeout / 2` startup check is unchanged. Replicas are polled **sequentially**: while replica 1 is polled the others catch up, so the cost is about max lag plus N round trips. Goroutine-per-replica withdrawn (same worst case, more code). |
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

## Replica discovery (R14–R20)

Status: **agreed, not implemented.** Decided 2026-09-21 by a three-round consensus between Codex, Claude Opus and
Claude Fable 5.1 (round 1 independent positions, round 2 seven disputes, round 3 the freshness budget). Supersedes
R2's "no discovery" for the library seam only; the static list stays the default.

### Problem

Every Patroni lifecycle event breaks a static `replicas` list. A listed standby that is promoted answers
`pg_is_in_recovery() = false` at `Open` → `ErrNotStandby` → the process exits and crash-loops until the list is edited.
Worse and silent: the demoted leader, once rewound and rejoined as a standby, is not listed and is never covered again
while the connector looks healthy. A decommissioned standby is a fail-closed restart loop; a new standby is uncovered
until someone edits the config.

### Verified facts (Patroni master sources and docs, 2026-09-21)

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
discovery mode; static users keep it, and the README says so.

### Agreed decisions

| # | Decision |
|---|---|
| R14 | **Seam: a resolver hook, not a client.** `config.VisibilityGuardConfig.ResolveReplicas func(ctx) ([]config.ReplicaMember, error)` (`json:"-" yaml:"-"`), `ReplicaMember{Addr string; Timeline int32}` with `Timeline == 0` = unknown. Mutually exclusive with `Replicas`, requires `enabled: true`. Resolver output is validated like the static list (`host:port`, no duplicates): a bad entry is an error, never a silent drop. The doc comment carries the contract verbatim: *return an error if you cannot prove the cluster was reached and correctly identified; an empty slice asserts "proved, zero routable replicas"*. YAML users are not covered in v1 (`ponytail:` deferral: a `consul:` block when a YAML user asks; a func field cannot come from `NewConnectorWithConfigFile`). |
| R15 | **Reference resolver `patroni.Replicas(agentAddr, scope)`**, its own one-file stdlib package (net/http + encoding/json), exported because every service would otherwise copy it. Membership from Consul health: one `GET /v1/health/service/<scope>?passing=true` (no tag filter); a passing `primary`/`master`-tagged member proves the cluster, else error; members tagged `replica` give `Service.Address:Service.Port`. Timeline joined from `GET /v1/kv/service/<scope>/members/?recurse` (values base64 JSON, field `timeline`) on member name (health service id `<scope>/<name>` ↔ KV key `service/<scope>/members/<name>`); missing → 0. The doc comment names the assumptions: `register_service: true`, service name = scope, Patroni role tags, the `/replica` check semantics. Covers *routability* (the set consumers are routed to, `noloadbalance` and failing checks excluded by construction), uses the local agent, needs no Patroni API URL during a failover. Patroni REST `/cluster` rejected as the source: superset of the routable set, needs a live API address first. |
| R16 | **Only the source's answer changes membership.** Refresh runs lazily at the top of `wait()` when the set is older than `refreshInterval` (default 10 s = Consul check 5 s + `loop_wait` 10 s propagation floor), resolver call bounded at 1 s (≤ 10 % of the default gate budget, zero calls on an idle stream). No goroutine, no mutex: `Open` runs before `go process`, `Close` after `<-processEnd`. Any member error (`errReplicaConn`, `ErrNotStandby`, timeline ahead, cluster mismatch, PgError) triggers **at most one optional re-resolve per wait in total**; the member is dropped iff the source no longer lists it, otherwise the error is classified exactly as today. A poll blocked only by R19 spends that same optional re-resolve. This budget governs optional re-asks only; a *due* refresh (R17) is mandatory and unaffected by it. This is not a per-replica skip or circuit breaker: the error decides *when* the source is asked, the source alone decides *who* is a member, and a lagging listed member still blocks (R12(a)). |
| R17 | **Freshness budget.** The resolved set is trusted for one `refreshInterval`. A refresh that is **due** (the set is older than `refreshInterval`) is mandatory: it is retried inside the gate with the poll backoff under the shared deadline, and the set does not certify until it succeeds; at the deadline the gate returns `ErrVisibilityTimeout`, so `failMode` applies (closed: restart, `Open` re-resolves or fails; open: dispatch with the warning and the counter). A 1–3 s Consul agent restart is absorbed; a long outage fails closed like R12(a). Stated plainly in the README: with a resolver configured, the discovery source becomes a liveness dependency of the stream, the one place where a component otherwise off the CDC critical path can hold dispatch. At `Open` a resolver error fails startup. An **empty slice is legitimate** ("cluster proven, zero routable replicas": a two-node cluster during a switchover, a single replica being reinitialised) → primary-only gating, logged at **Warn** with the empty set, so operators can alert on it. |
| R18 | **Reconcile = diff a slice.** Removed → `conn.Close`, drop the `*replica`, `metric.DeleteVisibilityReplicaLag(name)` (new method; the GaugeVec otherwise leaks the label). Added → append `replica{name, dial}`; its first poll dials and runs the full checks (R13 system identifier, in recovery, timeline ≤ session) under the shared deadline. Every membership change is logged (Info) with the full set. `Open` resolves once, then dials and checks every member as today; a member on a stale timeline or merely behind does not fail `Open` (CrashLoopBackOff outlives every condition Patroni fixes on its own). |
| R19 | **Timeline rule (discovery only): block, do not exclude, do not call it a closure.** A routable member whose `Timeline` is known and ≠ the session's is treated as *not yet replayed*: it blocks until the shared deadline → timeout → `failMode`; never fatal, never fails `Open`. Staleness is directional (a record reads N+1 only after the member itself reported N+1), so a stale record can only over-block. `Timeline == 0` turns the rule off for that member: a residual hole, since `touch_member` omits `timeline` when it could not determine one. Cost: after a failover the gate is held ≤ `loop_wait` + rewind until every routable member follows N+1 (one or two restarts of `timeout` under `closed`); the trade is "consumers silently read a divergent replica" → "CDC stops", the same as R12(a). The per-poll `min_recovery_end_timeline > session` fatal rule (R4) is unchanged. |
| R20 | **Out of scope.** Leader discovery for the connector's own `host` (leader DNS/proxy plus break-and-restart *is* the failover handling; owning it means owning slot existence on the new leader). Patroni REST or DNS SRV sources, a YAML source block, per-source adapters, a background watcher or Consul blocking queries, quorum, per-replica skip, circuit breaker, "drop after N absences" grace windows, lag- or `replication_state`-based exclusion in the library (Patroni `/replica?lag=` and tags express that on the routing side), a max-staleness constant beyond `refreshInterval`, new metrics beyond the gauge deletion, a Patroni/Consul stack in CI. |

### What happens on a promotion

Static (today): A demoted → walsender closes → `panic` → process restarts → reconnects to the leader DNS (B) →
`IDENTIFY_SYSTEM` timeline N+1 → `openReplicaGuard.open` dials B → not in recovery → `ErrNotStandby` → `Open` fails →
`connector.Start` returns → crash loop until `replicas` is edited; A, once rewound, is never covered again.

Discovery: same up to `Open`. The resolver answers from the local agent: B is tagged `primary` → out; A is
stopped/rewinding → not passing → out; C is passing, KV `timeline` still N → in the set but blocked by R19. `Open`
succeeds; the first gated transaction waits until C reports N+1 (≤ `loop_wait` once Patroni has reattached or rewound
it), timing out once or twice under `closed` in the worst case. A rejoins within one refresh after its `/replica` check
passes. Nothing is edited. A member promoted while the stream is alive (only outside Patroni's sequence, which demotes
the old leader first) answers `ErrNotStandby` → one re-resolve → dropped if the source no longer lists it as a replica,
fatal if it does (source and reality disagree).

### Guarantee sentence for the README

"With `ResolveReplicas` set, every standby that the resolver listed as a routable replica at the guard's last
successful refresh (at most `refreshInterval` old, plus the source's own propagation delay: Consul check 5 s, Patroni
`loop_wait` 10 s) has applied the transaction when the handler is called. A standby that joined, or was returned to
the load balancer, more recently may not yet be covered. A routable standby whose Patroni timeline differs from the
replication session's holds the gate; one whose timeline Patroni could not publish is polled without that rule, so the
divergent-standby window after a crash failover is narrowed, not closed."

Failure-handling paragraph, one added sentence: "With a resolver configured, the discovery source becomes a liveness
dependency of the stream: once the set is older than `refreshInterval` and a refresh has not succeeded, dispatch is
held and then follows `failMode`; closed restarts the stream, open dispatches with the warning and the counter."

### Build order

1. **PR 1 — core.** `config`: `ReplicaMember`, `ResolveReplicas`, validation (exclusive with `Replicas`,
   `enabled: true`). `pq/replication/replica.go`: `resolve`/`reconcile` at the top of `wait()`, freshness budget,
   one-re-resolve-per-wait on member errors, R19 in `poll`/`waitReplayed` classification, `resolvedAt`; static list
   becomes a constant resolver with refresh off. `metric.DeleteVisibilityReplicaLag`. Unit tests with a fake resolver:
   added member dialled and fully checked on the first wait after the interval; removed member closed and label
   deleted; refresh error → retried, stale set does not certify, timeout at deadline, both fail modes; empty set →
   primary-only + Warn; resolver error at `Open` fatal; member error → one re-resolve → dropped if gone, classified as
   today if listed; timeline mismatch blocks then unblocks after a re-resolve reports the session's timeline; resolver
   called at most once per interval and at most once per wait on errors; resolver output validation.
2. **PR 2 — `patroni` package + harness.** `patroni.Replicas` with `httptest` fixtures for the health and KV
   responses (cluster proof, tag filter, base64 join, missing timeline → 0, error paths). Integration: one test in the
   existing standby harness (`integration_test/replica_guard_test.go` style) with a fake resolver that returns
   `{standby1, standby2}`, then drops `standby2` and asserts dispatch stops waiting for it, then re-adds it.
3. **PR 3 — docs.** README: the guarantee sentence above, limits (uncovered join window, timeline-0 hole, static list
   keeps the divergent-standby hole), failure handling (freshness budget, empty set Warn), `ResolveReplicas` in the
   config table with the contract; `example/patroni-consul/` lab (Patroni + Consul compose, not in CI) showing
   switchover with no config edit; this doc's status line.

### Do not

- Do not let an error, a timeout or a lag value remove a member; only a successful resolve does.
- Do not certify on a set older than `refreshInterval` while a refresh is failing; do not make a refresh failure fatal
  either (it is a timeout under the shared deadline).
- Do not treat an empty resolved set as an error, and do not let a resolver return an empty slice without having proved
  the cluster (leader present).
- Do not exclude a routable member for a timeline mismatch; block on it. Do not make the mismatch fatal or fail `Open`.
- Do not use Patroni REST `/cluster` or `pg_stat_replication` as the membership source; do not add Consul or Patroni
  client dependencies; do not add a `consul:` YAML block before a YAML user asks.
- Do not add a goroutine or a mutex for the refresh; do not re-resolve more than once per wait.
- Do not run Patroni or Consul in CI.
