# Automate downstream go-pq-cdc version bumps — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable Dependabot Go-module updates in 4 downstream repos (+ fix go-pq-cdc's own), so every go-pq-cdc release surfaces as a bump PR daily across all module dirs.

**Architecture:** One identical `.github/dependabot.yml` per repo. A `gomod` block with `directories: ["/", "/**"]` (glob covers root + all nested modules) groups minor/patch into one PR/day; majors arrive as individual PRs. A `github-actions` block keeps workflow actions current. Delivered as one PR per repo against `main`.

**Tech Stack:** GitHub Dependabot (config v2), `gh` CLI, git.

---

## Critical constraints

- **No commit/push/PR without explicit user approval.** Each task ends at "open PR" — do NOT run the `gh pr create` / `git push` steps until the user says go.
- All 5 repos: default branch `main`, push access confirmed. Direct branch + PR (no fork).
- The final config is **byte-identical across all 5 repos** (glob removes per-repo dir lists).

## Canonical config (identical for every repo)

`.github/dependabot.yml`:

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

## Repo state recap (from spec investigation)

| Repo | Action | Existing file |
|------|--------|---------------|
| kafka | overwrite | `.github/dependabot.yml` (broken github-actions block) |
| elasticsearch | overwrite | `.github/dependabot.yml` (broken github-actions block) |
| pq | create | none |
| redis | create | none |
| go-pq-cdc (self) | overwrite | `.github/dependabot.yml` (broken github-actions block) |

---

## Task 0: Preflight

**Files:** none (verification only)

- [ ] **Step 1: Verify gh auth + push access**

Run:
```bash
gh auth status
for r in kafka pq elasticsearch redis; do
  echo -n "go-pq-cdc-$r push="; gh api repos/Trendyol/go-pq-cdc-$r --jq .permissions.push
done
echo -n "go-pq-cdc push="; gh api repos/Trendyol/go-pq-cdc --jq .permissions.push
```
Expected: authenticated; every line `push=true`.

- [ ] **Step 2: Create a working tmp dir for clones**

Run:
```bash
mkdir -p /tmp/depbot-rollout && cd /tmp/depbot-rollout && pwd
```
Expected: `/tmp/depbot-rollout`

---

## Task 1: go-pq-cdc-kafka

**Files:**
- Modify: `.github/dependabot.yml` (in Trendyol/go-pq-cdc-kafka, branch `ci/dependabot-gomod`)

- [ ] **Step 1: Clone + branch**

Run:
```bash
cd /tmp/depbot-rollout
gh repo clone Trendyol/go-pq-cdc-kafka
cd go-pq-cdc-kafka
git checkout -b ci/dependabot-gomod
```
Expected: on branch `ci/dependabot-gomod`.

- [ ] **Step 2: Write the config**

Write `.github/dependabot.yml` with the exact canonical config above (overwrites the broken file).

- [ ] **Step 3: Validate YAML + required keys**

Run:
```bash
python3 - <<'EOF'
import yaml,sys
d=yaml.safe_load(open(".github/dependabot.yml"))
assert d["version"]==2, "version must be 2"
eco=[u["package-ecosystem"] for u in d["updates"]]
assert "gomod" in eco, "missing gomod block"
gomod=next(u for u in d["updates"] if u["package-ecosystem"]=="gomod")
assert gomod["directories"]==["/","/**"], gomod.get("directories")
assert gomod["groups"]["go-deps"]["update-types"]==["minor","patch"]
print("OK: config valid")
EOF
```
Expected: `OK: config valid`

- [ ] **Step 4: Stage + commit (LOCAL ONLY)**

Run:
```bash
git add .github/dependabot.yml
git diff --cached --stat
```
Expected: shows `.github/dependabot.yml` staged. **Stop here — do NOT commit/push until user approves.**

- [ ] **Step 5: Commit + push + PR (ONLY AFTER USER APPROVAL)**

Run:
```bash
git commit -m "ci: manage Go modules with Dependabot

Add gomod ecosystem (root + all nested modules via glob), group
minor/patch into one daily PR, majors as individual PRs. Fix the
github-actions block that previously carried an ineffective gomod allow-list."
git push -u origin ci/dependabot-gomod
gh pr create --repo Trendyol/go-pq-cdc-kafka --base main --head ci/dependabot-gomod \
  --title "ci: manage Go modules with Dependabot" \
  --body "Enables daily Dependabot version updates for Go modules across all module directories (root + nested via \`directories: [/, /**]\`). Minor/patch grouped into one PR; majors individual. Replaces the previously broken github-actions-only config."
```
Expected: PR URL printed.

---

## Task 2: go-pq-cdc-pq

**Files:**
- Create: `.github/dependabot.yml` (in Trendyol/go-pq-cdc-pq, branch `ci/dependabot-gomod`)

- [ ] **Step 1: Clone + branch**

Run:
```bash
cd /tmp/depbot-rollout
gh repo clone Trendyol/go-pq-cdc-pq
cd go-pq-cdc-pq
git checkout -b ci/dependabot-gomod
mkdir -p .github
```
Expected: on branch `ci/dependabot-gomod`.

- [ ] **Step 2: Write the config**

Write `.github/dependabot.yml` with the exact canonical config above (new file).

- [ ] **Step 3: Validate YAML + required keys**

Run:
```bash
python3 - <<'EOF'
import yaml
d=yaml.safe_load(open(".github/dependabot.yml"))
assert d["version"]==2
gomod=next(u for u in d["updates"] if u["package-ecosystem"]=="gomod")
assert gomod["directories"]==["/","/**"]
assert gomod["groups"]["go-deps"]["update-types"]==["minor","patch"]
print("OK: config valid")
EOF
```
Expected: `OK: config valid`

- [ ] **Step 4: Stage + show diff (LOCAL ONLY)**

Run:
```bash
git add .github/dependabot.yml
git diff --cached --stat
```
Expected: `.github/dependabot.yml` staged. **Stop — wait for approval.**

- [ ] **Step 5: Commit + push + PR (ONLY AFTER USER APPROVAL)**

Run:
```bash
git commit -m "ci: manage Go modules with Dependabot

Add daily gomod updates across root + all nested modules (glob),
grouping minor/patch into one PR, majors individual."
git push -u origin ci/dependabot-gomod
gh pr create --repo Trendyol/go-pq-cdc-pq --base main --head ci/dependabot-gomod \
  --title "ci: manage Go modules with Dependabot" \
  --body "Adds daily Dependabot Go-module updates across all module directories (root + nested via \`directories: [/, /**]\`). Minor/patch grouped; majors individual."
```
Expected: PR URL printed.

---

## Task 3: go-pq-cdc-elasticsearch

**Files:**
- Modify: `.github/dependabot.yml` (in Trendyol/go-pq-cdc-elasticsearch, branch `ci/dependabot-gomod`)

- [ ] **Step 1: Clone + branch**

Run:
```bash
cd /tmp/depbot-rollout
gh repo clone Trendyol/go-pq-cdc-elasticsearch
cd go-pq-cdc-elasticsearch
git checkout -b ci/dependabot-gomod
```
Expected: on branch `ci/dependabot-gomod`.

- [ ] **Step 2: Write the config**

Write `.github/dependabot.yml` with the exact canonical config above (overwrites broken file).

- [ ] **Step 3: Validate YAML + required keys**

Run:
```bash
python3 - <<'EOF'
import yaml
d=yaml.safe_load(open(".github/dependabot.yml"))
assert d["version"]==2
gomod=next(u for u in d["updates"] if u["package-ecosystem"]=="gomod")
assert gomod["directories"]==["/","/**"]
assert gomod["groups"]["go-deps"]["update-types"]==["minor","patch"]
print("OK: config valid")
EOF
```
Expected: `OK: config valid`

- [ ] **Step 4: Stage + show diff (LOCAL ONLY)**

Run:
```bash
git add .github/dependabot.yml
git diff --cached --stat
```
Expected: `.github/dependabot.yml` staged. **Stop — wait for approval.**

- [ ] **Step 5: Commit + push + PR (ONLY AFTER USER APPROVAL)**

Run:
```bash
git commit -m "ci: manage Go modules with Dependabot

Add gomod ecosystem (root + all nested modules via glob), group
minor/patch into one daily PR, majors as individual PRs. Fix the
github-actions block that previously carried an ineffective gomod allow-list."
git push -u origin ci/dependabot-gomod
gh pr create --repo Trendyol/go-pq-cdc-elasticsearch --base main --head ci/dependabot-gomod \
  --title "ci: manage Go modules with Dependabot" \
  --body "Enables daily Dependabot Go-module updates across all module directories (root + nested via \`directories: [/, /**]\`). Minor/patch grouped; majors individual. Replaces the previously broken github-actions-only config."
```
Expected: PR URL printed.

---

## Task 4: go-pq-cdc-redis

**Files:**
- Create: `.github/dependabot.yml` (in Trendyol/go-pq-cdc-redis, branch `ci/dependabot-gomod`)

- [ ] **Step 1: Clone + branch**

Run:
```bash
cd /tmp/depbot-rollout
gh repo clone Trendyol/go-pq-cdc-redis
cd go-pq-cdc-redis
git checkout -b ci/dependabot-gomod
mkdir -p .github
```
Expected: on branch `ci/dependabot-gomod`.

- [ ] **Step 2: Write the config**

Write `.github/dependabot.yml` with the exact canonical config above (new file).

- [ ] **Step 3: Validate YAML + required keys**

Run:
```bash
python3 - <<'EOF'
import yaml
d=yaml.safe_load(open(".github/dependabot.yml"))
assert d["version"]==2
gomod=next(u for u in d["updates"] if u["package-ecosystem"]=="gomod")
assert gomod["directories"]==["/","/**"]
assert gomod["groups"]["go-deps"]["update-types"]==["minor","patch"]
print("OK: config valid")
EOF
```
Expected: `OK: config valid`

- [ ] **Step 4: Stage + show diff (LOCAL ONLY)**

Run:
```bash
git add .github/dependabot.yml
git diff --cached --stat
```
Expected: `.github/dependabot.yml` staged. **Stop — wait for approval.**

- [ ] **Step 5: Commit + push + PR (ONLY AFTER USER APPROVAL)**

Run:
```bash
git commit -m "ci: manage Go modules with Dependabot

Add daily gomod updates across root + all nested modules (glob),
grouping minor/patch into one PR, majors individual."
git push -u origin ci/dependabot-gomod
gh pr create --repo Trendyol/go-pq-cdc-redis --base main --head ci/dependabot-gomod \
  --title "ci: manage Go modules with Dependabot" \
  --body "Adds daily Dependabot Go-module updates across all module directories (root + nested via \`directories: [/, /**]\`). Minor/patch grouped; majors individual."
```
Expected: PR URL printed.

---

## Task 5: go-pq-cdc (self-fix)

**Files:**
- Modify: `.github/dependabot.yml` (in this repo, Trendyol/go-pq-cdc, branch `ci/dependabot-gomod`)

> This is the current working repo. Work on a branch off `main`.

- [ ] **Step 1: Branch**

Run:
```bash
cd /Users/huseyin.akuzum/Projects/trendyol/go-pq-cdc
git checkout main && git pull
git checkout -b ci/dependabot-gomod
```
Expected: on branch `ci/dependabot-gomod`.

- [ ] **Step 2: Write the config**

Overwrite `.github/dependabot.yml` with the exact canonical config above.

- [ ] **Step 3: Validate YAML + required keys**

Run:
```bash
python3 - <<'EOF'
import yaml
d=yaml.safe_load(open(".github/dependabot.yml"))
assert d["version"]==2
gomod=next(u for u in d["updates"] if u["package-ecosystem"]=="gomod")
assert gomod["directories"]==["/","/**"]
assert gomod["groups"]["go-deps"]["update-types"]==["minor","patch"]
print("OK: config valid")
EOF
```
Expected: `OK: config valid`

- [ ] **Step 4: Stage + show diff (LOCAL ONLY)**

Run:
```bash
git add .github/dependabot.yml
git diff --cached
```
Expected: shows gomod block added, broken allow-list removed. **Stop — wait for approval.**

- [ ] **Step 5: Commit + push + PR (ONLY AFTER USER APPROVAL)**

Run:
```bash
git commit -m "ci: manage Go modules with Dependabot

Add gomod ecosystem (root + all nested modules via glob), group
minor/patch into one daily PR, majors as individual PRs. Fix the
github-actions block that previously carried an ineffective gomod allow-list."
git push -u origin ci/dependabot-gomod
gh pr create --repo Trendyol/go-pq-cdc --base main --head ci/dependabot-gomod \
  --title "ci: manage Go modules with Dependabot" \
  --body "Fixes the dependabot config: adds a gomod ecosystem block (root + nested modules via \`directories: [/, /**]\`), groups minor/patch into one daily PR, majors individual. Removes the ineffective gomod allow-list under the github-actions block."
```
Expected: PR URL printed.

---

## Post-merge verification (per repo, after a PR is merged)

- [ ] In the merged repo: **Insights → Dependency graph → Dependabot → "Check for updates"** (or wait for the daily run).
- [ ] Confirm a `go-deps` group PR appears bumping `github.com/Trendyol/go-pq-cdc`.
- [ ] Confirm the PR touches nested module `go.mod` files (e.g. kafka `benchmark/go-pq-cdc-kafka/go.mod`), not just root — proves the `/**` glob works.

## Rollback

Revert the PR (or delete the merged `dependabot.yml` change). Dependabot stops on the next scheduled run; no other state to clean up.
```
