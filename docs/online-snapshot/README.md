# Online snapshot feasibility and design package

**Status:** proposal, not implementation or production approval. **Date:** 2026-10-13.

This package supports a decision on DBLog-style online snapshots in `go-pq-cdc`. It independently compares the existing temporary-slot draft and preserves both earlier proposals unchanged.

## Read or present

| Audience | Markdown source | Presentation export |
|---|---|---|
| Stakeholders | [Feasibility report](feasibility-report.md) | [Feasibility report PDF](feasibility-report.pdf) |
| Engineering reviewers | [Design RFC](design-rfc.md) | [Design RFC PDF](design-rfc.pdf) |
| Test and SRE owners | [Validation plan](validation-plan.md) | Included in the RFC PDF |
| Evidence reviewers | [Source and test evidence](evidence.md) | Included in both PDFs |

Start with the feasibility report. Use the RFC for architecture, algorithms, recovery, compatibility, operations, and open decisions D1–D6.

The recommendation is to approve bounded validation work. The first profile targets PostgreSQL 16+, Kafka and Elasticsearch adapters, and immutable integer primary keys. It does not claim a global T0 snapshot or unlimited outage tolerance.

## Included drawings

- [Architecture](diagrams/architecture.svg): current `initial` startup versus the proposed staged online path.
- [Watermark merge](diagrams/watermark-merge.svg): update/delete conflicts and the HIGH output barrier.

SVG sources are editable and remain sharp in slides and PDFs. The drawings are conceptual, not measured latency or throughput charts.

## Evidence and limitations

[Baseline output](baseline-checks.txt) records a fresh root race-test run, `go vet`, and seven selected PostgreSQL 16 integration tests. The evidence appendix states their limits. The full integration suite and online recovery/capacity qualification were not run.

Two independent read-only reviews covered protocol safety and evidence accuracy. Revisions addressed visibility across restart, attempt-level fencing, RLS exclusion, and startup-mode qualifiers. No reviewer or document grants production approval.

[Package checksums](package-manifest.sha256) bind the final sources and exports. They are integrity checks, not security signatures or correctness proofs.

## Regenerate the PDFs

Requirements: Node.js 22+, Chrome/Chromium, and `marked` 18.1.0. The export script adds no application dependencies. It embeds SVG images and creates PDFs with covers, reading guides, page numbers, and supporting appendices.

From the repository root, install the documentation renderer outside the project:

```bash
TOOLS=$(mktemp -d)
npm install --prefix "$TOOLS" --no-save --package-lock=false marked@18.1.0
MARKED_MODULE="$TOOLS/node_modules/marked/lib/marked.esm.js" \
  node docs/online-snapshot/export.mjs
```

On Linux or with a custom browser path, also set `CHROME_BIN=/path/to/chromium`. On macOS, the default is the standard Google Chrome application path. If `marked` is already resolvable from Node, omit `MARKED_MODULE`.

The exporter checks image loading, internal PDF-document anchors, and horizontal content bounds. It does not prove pagination quality. Inspect the PDFs after export. Do not run two exporters concurrently into this directory.

The PDF RFC includes the validation plan and evidence appendix. The feasibility PDF includes its evidence appendix. Source-code links resolve to the recorded commit. Companion files outside an export appear as named references rather than machine-specific file links.

## Verify and next action

```bash
(cd docs/online-snapshot && shasum -a 256 -c package-manifest.sha256)
```

After any source change, regenerate both PDFs and update the checksum manifest. Assign owners to D1–D6 before authorizing implementation. Do not treat an unresolved decision as an accepted default.
