# e01 POC result — restricted-profile automated gates PASS

Updated 2026-10-15 after the final retained recovery pass. **The restricted POC's automated root race/vet, integration vet, full PG16 and explicit matched-capacity gates now PASS on current code. Human UAT and independent acceptance review are unconfirmed. This is not production-readiness or adapter-enablement approval.** One fresh full suite and one matched capacity run followed the last code fix; redundant per-task full loops were not run. Exact outputs, exits and transparent shared task associations are archived. Earlier failures/timeouts remain historical evidence, not current passes.

## Ownership and environment

Branch `feat/online-initial-poc`, unchanged HEAD/baseline `6110b91596ea9597b588954f94a89a1b5f4cfcd6`. All changes unstaged. No commits, pushes, staging, stashes, resets, delegation, dependency additions, archive restoration or unrelated deletions. All seven approved reconnect archives, unrelated user files and previous PDF bundles remain preserved.

Go 1.26.2 darwin/arm64; existing pgx v5.9.2/testcontainers dependencies; PostgreSQL 16-alpine on Podman 6.0.2/API 1.44. Source/target POC fixtures are independent fsync=on/full_page_writes=on clusters. The existing fast legacy fixture remains fsync=off. Capacity enables the image's existing pg_stat_statements extension in its isolated source only. Image inspect/digests remain in `e01-evidence/pg16-image.json`; per-run container IDs are archived.

## Review corrections implemented

1. **Lossless images:** one online-specific decoder serves READ and CDC; JSON/JSONB and their arrays use json.Number. CDC reuses existing tuple/TOAST parsing with raw-text temporary relation types, then decodes original OIDs once, avoiding legacy JSON rounding/overflow. Legacy codecs are unchanged. Online non-finite floats (also float arrays) use strings `Infinity`, `-Infinity`, `NaN`, as approved. Tests cover large-number READ, CDC and committed-response-loss replay; the oracle has a negative precision control and exact rational JSON-number comparison. The approved business profile is explicitly restricted to bool, int2/int4/int8, text/varchar/bpchar, float4/float8, JSON/JSONB and qualified 1D/lower-bound-1 arrays. Domains/custom/composite/enum/range, numeric, temporal, bytea and business UUID are rejected at catalog startup before slot/job creation. Unknown OIDs do not fall back to strings; exact UUID decoding remains internal-control-only. Actual array dimensions/bounds are checked by pgtype shape metadata before READ/envelope delivery, including NULL/empty/quoted-brace cases. Additional types are deferred, not full PostgreSQL type coverage.
2. **Wire allocation limits:** online replication, guard, owner and chunk frontends have overflow-checked body limits before operations; payload budgets have a documented 64KiB protocol/metadata allowance. Actual oversized PG16 READ/CDC frames fail closed before unrestricted body allocation; assembler-only tests are not the sole proof.
3. **Cleanup:** metadata, target and example rollback use finite cleanup contexts and discard failed connections. Real blackholed rollback-response tests verify bounded termination/owner loss and no committed target effects.
4. **Durable provisioning:** all five source metadata relations must be ordinary permanent tables; UNLOGGED version/window/progress negative cases reject rather than transform them.
5. **Caller ownership:** constructor copies table slices before defaults; concurrent construction and input immutability are tested. Impossible BEGIN/COMMIT and overflowing wire budgets fail before Run.
6. **Startup/lifetime:** approved additive OpenStoreWithStartupTimeout and StartOnlineReaderWithStartupTimeout separate bounded acquisition from successful lifetime. Real proxy-held IDENTIFY, guard connect/query and START_REPLICATION stages and expired-startup/lifetime cancellation tests exist.
7. **Error causes:** catalog errors retain cancellation causes; a test-first cancellation regression fixes the final capacity cancellation failure rather than relaxing the assertion.

## APIs and bounded observation

The additive root NewOnlineConnector/Run/readiness/Done/Err/event/handler API remains source compatible. Approved variadic `WithOnlineObserver` observes immutable bounded metadata outside component locks; observer errors stop Run without claiming rollback of already committed metadata or ACK. Phases distinguish capture start, LOW recorded, read entry/result, HIGH recorded, READs delivered, completion committed and caller ACK advancement. Source confirmation is independently queried, never inferred from the ACK callback. Observer cancellation/pre/post-commit/ACK errors are tested.

`Stats()` is a thread-safe immutable snapshot of actual raw chunk bytes, read query latency/count, transaction/queue-and-assembly/relation encoded peaks, current buffering, safe ACK and feedback. It is not an RSS guarantee and retains no per-row history. Default nil observer has no callback behavior. Runtime continues to use one window, distinct direct connections, fail-closed visibility, serialized durable delivery, metadata-before-ACK and no automatic reconnect/slot adoption/recreation. No production adapter was enabled.

## Completed scenario coverage

- **Seven hard process-crash boundaries:** LOW before SELECT; selected result before HIGH; HIGH before first READ; one durable READ; last READ before completion; PostgreSQL metadata COMMIT completion held on real wire before client reply/ACK; actual source confirmation. Existing extra completed-window and HIGH-trigger schedules remain. Child processes are killed without Close.
- **Ambiguous metadata outcomes:** real committed-response cut and pre-COMMIT cut are independently observed; in-process ACK stays at LOW. Restart preserves committed cursor/HIGH or abandons uncommitted attempt and reconstructs coverage. No owner reconnect is required or implemented.
- **Surviving old target requests:** actual child-generated READ request is retained on the independent target, the connector child is hard killed, a new generation applies update/delete/delete-reinsert, and the old independent request is released. Projection, token/tombstone and current source progress stay unchanged.
- **Direct SELECT faults:** real result-stream cut after a valid row and genuine SELECT AccessShare lock timeout after schema validation. Typed ErrRead, zero emitted READs, unchanged cursor/LOW and no unsafe confirmation; fresh attempt reconstructs the page.
- **Model and visibility:** 32 seeds x seven crash schedules with three keys/two windows/two two-row transactions, explicit durable target/storage outcomes and restart state, stable CDC identity, late READ/version protection and bounded failure traces. Existing conflict/control model uses the actual runtime marker predicate. Bounded-envelope fuzz passed 253,706 executions/3s. Real PG16 remote_apply fixture now goes through child connector crash, original metadata/slot restart, target delivery and checkpoint/confirmation, not only direct reader calls.
- **Complete CDC oracle:** independent committed reader records exact source identities, full images, transaction boundaries and ordinals. Target retains event content with its ledger/hash. Recovery, delayed request, example and capacity assert every required intermediate CDC identity separately from final equality. Oracle history is test-only, capped at 20,000 events; it never ACKs volatile history and retains a separate fixture slot until completion.
- **Executable demo:** actual compiled binary prints/applies provisioning, creates target tables, loads private YAML, delivers snapshot/large JSON/CDC/tombstone, exits via SIGTERM, restarts original job/slot/target, reinserts and passes exact state/CDC ledger checks.
- Existing minimum/negative/sparse/empty/all-conflict/multi-table/behind+above-bound, owner/binding/slot, external TOAST/NULL, schema/key/TRUNCATE and restricted-role tests remain substantive.

## Matched capacity — PASS, not an SLO

The exact explicit command in `final-type-matched-capacity.log` passed (181.280s). Fresh independent clusters, 10,000 rows x 1,024-byte payloads, 100-row chunks, 120 requested updates/s for 60s, exact source CDC oracle and final business fence, 128MiB main-slot WAL budget and 3m drain. No unconditional failing placeholder or explicit-opt-in skip remains. A live full-queue compliant sink stall separately checks two additional safe feedbacks, unchanged ACK and cancellation within 5s.

| Actual measurement | consistent | online |
| --- | ---: | ---: |
| Achieved transactions/s | 119.882 | 119.890 |
| Writes | 7,217 | 7,216 |
| Durable deliveries | 17,218 | 17,118 |
| Peak whole-process heap bytes | 28,534,808 | 31,240,136 |
| Peak whole-process RSS bytes | 214,450,176 | 308,117,504 |
| Peak main-slot retained WAL bytes | 17,589,272 | 7,685,816 |
| Peak confirmed lag bytes | 17,589,216 | 2,330,664 |
| Peak target latency us | 32,492 | 40,505 |
| Maximum PostgreSQL READ execution ms | 0.616 | 0.543 |
| READ queries | 100 | 100 |
| Drain seconds | 4.351 | 39.166 |

Online actual peaks: raw chunk 103,200 bytes; transaction 2,217 encoded bytes; queue+assembly 980,359 encoded bytes; relation cache 210 bytes; end-to-end read 9.270625ms. Configured byte bounds, multiple windows and confirmation during active writes, full CDC identity coverage and exact final state all assert.

Attribution: whole-process heap/RSS includes fixture/oracle/client work and sequential allocator history. The oracle retains another source slot; reported WAL is explicitly the connector's main-slot requirement, not all physical WAL files. Legacy online byte Stats are not applicable (zeros in JSON), its raw durable READ batch is 102,901 bytes and READ execution is measured via pg_stat_statements. Legacy feedback can be absent until CopyBoth; its sampled age is zero, not an asserted zero-latency guarantee. Online sampled feedback age is 1.012685s. No production SLO, unexplained improvement ratio or total memory-bound claim. JSON reports retain container IDs/configuration/seed/metrics; previous runs remain archived.

## Actual verification and retained failures

Current final gate sequence after the last code fix (`final-type-gates.json`):

| Exact command | Result | Elapsed s |
| --- | --- | ---: |
| `go test -race -count=1 ./...` | PASS | 5.024 |
| `go vet ./...` | PASS | 1.004 |
| `(cd integration_test && go vet ./...)` | PASS | 0.685 |
| `(cd integration_test && POSTGRES_VERSION=16-alpine go test -race -count=1 -p=1 -timeout=20m ./...)` | PASS | 440.639 |
| `(cd integration_test && CDC_ONLINE_CAPACITY=1 POSTGRES_VERSION=16-alpine go test -race -count=1 -p=1 -timeout=10m -run '^TestOnlineInitialCapacity$' ./...)` | PASS | 181.280 |

- Focused type/array/control/recovery passed in 57.517s before final gates. All qualified scalar/array families, empty/NULL arrays, NULL elements, quoted braces/commas/quotes, large JSON and real READ/CDC shape/bound failures assert. Domain/custom/numeric/temporal/bytea/UUID negatives assert no slot/job/delivery; internal UUID LOW/HIGH controls still work. Shared decoder unit tests reject unknown OIDs and actual invalid dimensions/bounds.
- **Latest previously omitted failure:** all TestOnlineQualifiedArrays cases in `type-profile-green-first.log` failed before decoding because fixtures omitted cdc_online provisioning. The startup-negative test alone passed; this was not successful array evidence. Existing provisionOnline now provisions them. `type-profile-provisioned.log` then passed negative shape cases but exposed an oracle alias collision with a business column named `t`; source whole-row alias is now selected to avoid all column names. `type-profile-focused-final.log` and the final full suite pass after both corrections.
- Earlier final full run timed out at 1201.516s on oracle slot creation while parent HIGH lock held a source transaction. Oracle startup is now bounded and occurs after child kill/lock release, before every business write. Corrected focused recovery passed; the new full gate above supersedes that timeout for current code. Timeout/raw stack remains archived.
- Earlier RED/GREEN, precision/control/copy/static-budget/cancellation tests, legacy fixture context/readiness errors, incomplete capacity runs, lint corrections and failures remain preserved. Wrong benchmark early cancellation exposed an existing legacy pool Close/Put race/panic; legacy code was not changed and that separate condition is not claimed fixed.
- All 14 task/story presence guards and shell syntaxes were checked against current files. `final-ledger-associations.json` explicitly associates shared final gate evidence with task requirements; exact redundant per-task composite commands were NOT re-executed. Required named and supplementary scenarios ran in the full suite; capacity ran explicitly once in this final pass.

## Verdict / next step

**PASS for the approved restricted-profile automated POC checks, not production safety.** All story/task ledgers have current automated evidence through explicit shared-gate associations. Human UAT is **unconfirmed**; independent review/parent acceptance remains outstanding. No unqualified PostgreSQL type/major/workload, HA, arbitrary-callback termination, exactly-once transport, outage WAL isolation or adapter compatibility/enablement claim. Recommended next step is independent review of the qualified decoder/body caps/cleanup/observer/oracles, then human UAT and additional profile qualification. No further implementation or long test loop is started in this bounded final-gates pass.
