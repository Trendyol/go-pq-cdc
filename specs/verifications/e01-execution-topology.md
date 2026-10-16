# POC execution ownership

Baseline: `6110b91596ea9597b588954f94a89a1b5f4cfcd6`, branch `feat/online-initial-poc`. The seven stale untracked reconnect tests were archived with explicit user approval. Root race tests/vet and four selected PostgreSQL integration tests pass.

Classification: multi-seam. Source protocol/configuration, progress/target storage, and integration orchestration have separately testable contracts. They execute sequentially with one writer per cwd. No managed worktree allocation is requested because untracked user artifacts must remain available without committing/stashing them.

| Lane | Exclusive initial ownership | Gate / durable handoff |
|---|---|---|
| Source reader | `config/config.go`, new config strategy tests, `pq/replication/online*.go` | Bounded committed envelope, feedback, visibility, config gates; managed source-reader report |
| Progress and target | `pq/snapshot/online/event.go`, metadata/progress/identity helpers and tests; integration target/fixture helpers | Transactional metadata, attempt fencing, versioned durable test sink; managed progress-target report |
| Integration | Root online constructor/lifecycle and legacy constructor guard; coordinator, keyset/validation/window/recovery files; integration scenario tests, examples, evidence and ledgers | Wire component contracts without replacing them; all POC verify commands and self-audit |
| Review | Read-only protocol/recovery/security and test/compatibility reviewers | Fresh independent reports |
| Integration corrections | Same retained integration owner, scoped fixes following review | Revalidate touched behavior and report residual risks |

All lanes use `/Users/huseyin.akuzum/Projects/trendyol/go-pq-cdc`. No child may spawn other agents, commit/push/stash, delete unrelated files, or mutate other repositories. Logs/review handoffs use runtime-managed artifacts. Source changes remain unstaged for parent acceptance.

TDD uses test-first failing/passing command evidence rather than commits because commits and stashes are not authorized. No production readiness claim or automatic adapter enablement is accepted.
