# Design notes

## 1. Version chunks, not documents

A recall must name the *exact text* a user saw. Re-indexing a document in place loses
that, so every chunk is a row in `chunk_versions` with `valid_from_lsn`/`valid_to_lsn`
(SCD Type 2). A partial unique index (`WHERE is_current = 1`) makes "two live versions of
one section" impossible at the storage layer, not just in application code.

**Trade-off:** storage grows with history. In practice policies change rarely, and
history is exactly what audit asks for.

## 2. Stable section keys

Keys come from the heading text with numbering stripped (`4.2 Audit Log Retention`
becomes `audit-log-retention`), so renumbering does not look like delete-plus-add. The
checksum still includes the heading and the department:

- heading: what the user reads changed, so the version changes
- department: who may *see* the text changed, so existing answers must be recalled

Owner, title and effective-date edits update live rows in place and recall nothing.

## 3. Idempotency on source LSN, not on content

Content hashes cannot tell "the same event delivered twice" apart from "the policy was
changed back". The source log position can. `applied_events` records every LSN
(applied, stale or quarantined), and an event older than the policy's `last_lsn` is
skipped, which handles out-of-order delivery across partitions or replays.

The whole apply (idempotency check, version writes, recall, LSN mark) runs in one
`BEGIN IMMEDIATE` transaction. A crash rolls back everything, so redelivery is safe.
This is tested by injecting a failure mid-apply.

## 4. Entitlements before ranking

Filtering after top-k leaks information: scores, ordering and "no result" timing all
depend on restricted text. The retriever skips non-entitled chunks before scoring.

## 5. Refuse on low coverage

BM25 will rank a chunk that shares one rare word with the question. After the incident
runbook was deleted, the Sev-1 question started matching a break-glass sentence that
mentions "incident ticket". The service now also requires the top hit to cover at least
half of the question's IDF-weighted terms. This was found by a test, not designed up
front.

## 6. Golden-set drift is not a regression

When a policy changes, golden answers that cite it may become wrong. Treating those
failures as regressions blocks every legitimate policy update. Ignoring them hides real
regressions. The gate takes the release's changed keys and splits failures:

| Failure on... | Status | Effect |
|---|---|---|
| a section changed in this release | `GOLDEN_REVIEW_REQUIRED` | routed to the policy owner, not counted in metrics |
| an untouched section | `REGRESSION` | blocks the release |

## 7. Extractive by default

The default generator returns sentences from a single cited section, so answers are
grounded by construction and the project runs offline in CI. The `Generator` protocol
lets an LLM plug in (`adapters/openai_generator.py`); then the groundedness metric
becomes a real hallucination check.
