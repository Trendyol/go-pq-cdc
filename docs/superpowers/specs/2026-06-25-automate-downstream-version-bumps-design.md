# Automate downstream go-pq-cdc version bumps

**Date:** 2026-06-25
**Status:** Approved (design)

## Problem

`go-pq-cdc` is consumed by four sibling repos:

- `Trendyol/go-pq-cdc-kafka`
- `Trendyol/go-pq-cdc-pq`
- `Trendyol/go-pq-cdc-elasticsearch`
- `Trendyol/go-pq-cdc-redis`

When `go-pq-cdc` releases a new tag, each downstream repo's `go.mod` must be
bumped to the new version. Today this is fully manual.

## Current state (investigated 2026-06-25)

| Repo | Renovate | Dependabot | Bumps go-pq-cdc? |
|------|----------|------------|------------------|
| kafka | none | `.github/dependabot.yml` | No |
| elasticsearch | none | `.github/dependabot.yml` | No |
| pq | none | none | No |
| redis | none | none | No |

All four pinned at `github.com/Trendyol/go-pq-cdc v1.10.11`.

The kafka & elasticsearch `dependabot.yml` is broken for this purpose:

```yaml
- package-ecosystem: "github-actions"        # scans workflows only
  allow:
    - dependency-name: github.com/jackc/pgx/v5   # gomod dep, never matches
    - dependency-name: github.com/lib/pq         # gomod dep, never matches
```

The `allow` filter matches only deps within the declared ecosystem. Ecosystem
is `github-actions` (scans `.github/workflows/*`), so the Go-module allow-list
matches nothing. No `gomod` block exists. Net effect: no Go deps update.

`go-pq-cdc` itself has the same broken pattern in its own
`.github/dependabot.yml`.

## Decisions

- **Tool:** Dependabot (GitHub-native, zero infra). Renovate rejected — user
  prefers fixing existing Dependabot.
- **Gate:** Auto-PR only. Dependabot opens PRs; humans review/merge. (Dependabot
  has no native auto-merge — matches the gate exactly.)
- **Scope:** all Go modules, not just go-pq-cdc.
- **Schedule:** daily.
- **Directory coverage:** glob `directories: ["/", "/**"]` — covers root + all
  nested module dirs, including future ones. (`directories` plural supports
  globbing per GitHub docs; the singular `directory` key does not.)
- **Self-fix:** also fix `go-pq-cdc`'s own `dependabot.yml`.

## Why "Why" matters: major vs minor/patch

Go module major version bumps change the import path (`/v2`), requiring manual
code edits. They must NOT be auto-grouped or silently bundled. The design groups
only `minor` + `patch`; majors fall out of the group and arrive as individual
PRs for careful review.

## Design

### Config template

Per-repo `.github/dependabot.yml`:

```yaml
version: 2
updates:
  - package-ecosystem: "gomod"
    directories:
      - "/"
      - "/**"
    schedule:
      interval: "daily"
    open-pull-requests-limit: 10
    commit-message:
      prefix: "chore"
    groups:
      go-deps:
        patterns: ["*"]
        update-types: ["minor", "patch"]

  - package-ecosystem: "github-actions"
    directory: "/"
    schedule:
      interval: "daily"
    commit-message:
      prefix: "chore"
    groups:
      gha:
        patterns: ["*"]
```

### Behavior

- All Go deps' minor+patch bumps bundle into one PR/day via the `go-deps` group.
  `go-pq-cdc` minor/patch lands here.
- Major bumps excluded from the group → own PR (import-path change needs review).
- `github-actions` block drops the broken gomod `allow`-list and just keeps
  actions current, grouped into one PR.
- `directories: ["/", "/**"]` covers every module directory now and in the
  future.

### Module directories per repo (reference)

The glob makes per-repo lists unnecessary, but for the record:

| Repo | go.mod dirs |
|------|-------------|
| kafka | `/`, `/benchmark/go-pq-cdc-kafka` |
| pq | `/` |
| elasticsearch | `/`, `/example/script-update`, `/example/simple` |
| redis | `/`, `/integration_test` |

## Rollout

One PR per repo (user reviews/merges):

1. **kafka** — edit existing `dependabot.yml`: add gomod block, fix actions block.
2. **elasticsearch** — same edit.
3. **pq** — create new `dependabot.yml`.
4. **redis** — create new `dependabot.yml`.
5. **go-pq-cdc** (self) — fix own `dependabot.yml` to the same template.

## Out of scope

- Renovate / Mend app.
- Auto-merge.
- Push-based instant propagation (repository_dispatch). Daily Dependabot poll is
  the agreed latency.

## Validation

After merge in one repo, confirm a Dependabot `gomod` PR appears (or trigger
manually via the repo's Insights → Dependency graph → Dependabot → "Check for
updates"). Confirm it touches nested module dirs, not just root.
```
