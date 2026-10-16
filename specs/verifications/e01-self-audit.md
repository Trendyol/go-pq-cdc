# e01 integration self-audit — retained completion pass

Updated before final handoff, 2026-10-15. Self-audit is not independent review or production certification. Branch/HEAD unchanged; sole writer; no staging, commits, pushes, stashes, resets, delegation, dependencies, unrelated deletions or archive restoration. Existing PDF/user artifacts and seven reconnect archives preserved.

## Scope / contracts

- New online API only: explicit initial/protocol1/PG16 primary, serialized synchronous durable handler, immutable optional observer and Stats snapshots. Existing calls remain source compatible; legacy listener/Start/ACK path unchanged except original online-constructor guard.
- Supervisor explicitly approved startup/lifetime separation, online-only lossless JSON/json.Number and special float strings, and optional bounded metadata observation/statistics. No source row outbox, reconnect, slot replacement, lease/failover, adapter enablement or parallel windows.
- Observers execute outside component locks; post-COMMIT/post-ACK errors cannot pretend rollback. Actual confirmation is independently observed, not inferred from callback phase.

## Security / data fidelity

- Data values bound; identifiers quoted. No online credential logging/fingerprinting. Restricted-role source tests use explicit pg_control_system EXECUTE and metadata/table DML, not runtime superuser.
- ALL captured business schemas qualify/fingerprint, including non-READ-selected tables. Explicit I/U/D/TRUNCATE publication, FULL images, one immutable integer PK; no RLS/inheritance/partitions/generated/filters/column lists/schema-wide/all-table publication. Runtime key/schema/TRUNCATE failures remain fail closed.
- Approved qualified business OIDs are bool, int2/int4/int8, text/varchar/bpchar, float4/float8 and JSON/JSONB plus 1D/lower-bound-1 arrays. NULL/empty arrays and quoted JSON/text elements are tested. Domains/custom/composite/enum/range, numeric, temporal, bytea and business UUID reject at startup before slot/job/delivery; internal UUID remains control-only. Actual dimensions/bounds reject via codec metadata before READ/envelope delivery. Unknown OIDs never silently stringify. Full PostgreSQL type coverage is deferred.
- Shared online value decoder and online-specific JSON array codecs avoid legacy float64 rounding; raw tuple parsing no longer executes legacy value codecs first. Non-finite float/array strings documented. Legacy codecs unchanged. Exact number oracle uses arbitrary-precision numeric equality and a deliberate >2^53 mismatch negative control.
- Wire body caps are installed before online operations with bounded 64KiB framing allowance and overflow validation. Real oversized COPY/DataRow proof complements assembler budgets. Encoded/raw limits are not total heap bounds.
- Permanent source metadata is required; idempotent provisioning does not silently convert UNLOGGED tables. Unknown/malformed controls fail closed; valid older-generation controls cannot satisfy fresh LOW. Trusted role/metadata owner is assumed; no protection against a deliberately malicious owner rewriting its own metadata.

## Error / ownership / recovery

- Separate direct replication, SQL visibility, chunk/marker and advisory owner connections. Startup acquisition is bounded without canceling successful lifetime; owner/source loss cancels Run; source ACK remains caller-certified durable/visible frontier.
- LOW precedes fresh bounded SELECT, HIGH commits separately, collision merge is table/key scoped, surviving READs precede post-HIGH CDC. Last selected key drives cursor even for all-conflict pages. Empty/minimum/negative/sparse/out-of-bound cases remain tested.
- Atomic completion/cursor/checkpoint precedes ACK; quiet checkpoints/heartbeats cannot bypass a window. Handler/observer errors stop forward delivery. Finite rollback cleanup discards broken connections; real rollback-response blackholes are tested.
- Resume preserves original source/config/schema/slot binding, rejects missing/invalid/advanced/unbound slots, abandons old attempts without cursor advance and uses fresh UUIDs. No received/keepalive position is a checkpoint. Committed/uncommitted source metadata reply cuts preserve the appropriate durable state and do not reconnect the owner.
- All seven hard process barriers are implemented; old target requests survive connector child death and cannot overwrite newer values/deletes/reinsert tokens or mutate source progress. Independent committed CDC oracle asserts intermediate identities/images/ordinals as well as final state.
- Root real visibility proof now includes metadata, target, hard child kill and original-slot restart. Seeded durable model exercises two windows/two transactions, target/storage failures and replay; envelope/control fuzz covers actual production seams. Model remains a finite qualification model, not a proof of all schedules.

## Measurements / verification disposition

- Matched fresh consistent/online 60s workload passes meaningful rate, exact CDC/final fence and online bounds assertions. Actual chunk/transaction/queue/cache/read metrics, process heap/RSS, PostgreSQL read execution, main-slot WAL/confirmed lag/feedback/target latency sampled through drain. Separate live full-queue compliant sink stall verifies feedback and cancellation. No unconditional failing placeholder or explicit-opt-in skip.
- Whole-process measurements include test/oracle/client work. Oracle retains its own fixture slot without ACKing volatile history; main-slot WAL attribution and legacy metric limitations are explicit. No production SLO or invented improvement ratio.
- Actual example binary provisioning/config/target/delivery/tombstone/restart is tested. Its target requires permanent relations and bounded rollback, but remains an example fixture, not a production adapter.
- After the last code fix, exactly one fresh root race/vet + integration vet + full PG16 suite passed (440.639s full gate), then exactly one explicit matched-capacity gate passed (181.280s). Old 20m oracle-lock timeout remains archived; corrected lock order/startup bound is now validated by the full suite. Current task associations transparently reuse these shared gates rather than claiming redundant composites were rerun.
- Failed latest array fixtures originally omitted metadata provisioning and never reached decoding; after provisionOnline, positive qualification exposed whole-row alias `t` colliding with a business column. A noncolliding alias selected from field descriptions fixed the oracle before final focused/full/capacity gates. No failed fixture was promoted to passing evidence.

## Residual risks / review gate

- Final restricted-profile automated gates now pass; independent parent review/acceptance remains outstanding. Human UAT is unconfirmed, not inferred from automated passes. No production-readiness or adapter-enablement claim.
- Client query cancellation/close does not guarantee immediate PostgreSQL abort during server lock wait; fixtures explicitly release locks. Runtime fails closed but may require operator resolution of source-side blocking; no HA/liveness promise.
- Existing legacy pool Close/Put panic/race was exposed by erroneous benchmark early cancellation. Legacy code was not changed; treat separately rather than attribute it to online correctness or manufacture a baseline fix.
- Context-ignoring callbacks cannot be forcibly terminated. Slow sinks retain WAL. Heap/RSS, disk/WAL budgets and value/type/workload/major coverage need further qualification. No production safety, exactly-once transport or adapter equivalence claim.
