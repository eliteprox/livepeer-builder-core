# GitLab x402 provider prototype

## Status

Owner: local GitLab/Paperclip pilot. ELI-122 continues MR!2 (`engineer/issue-68-boundary`, base
`origin/releases/m3`). Recovered commits `073fc66` and `c431a30` stay in the history.

Local gates on this revision: `ruff check`, `ruff format --check`, `mypy`, full `pytest` (1577 passed,
54 skipped, total coverage 97.46%), `scripts/check_coverage.py` (all layer floors ok), `diff-cover`
95% against `origin/releases/m3`, `scripts/check_docs.py`, `lpb gen-docs --check`, and `lpb smoke`.

Provider-owned per-stream renewal is implemented. A persistent stream arms its own renewal task. A
default single-shot payment does not. Closing a stream cancels that stream only. A refused or unknown
renewal is on `X402StreamStatus` (no payment secret); an unknown renewal keeps the attempt cost
uncertain, and a refused renewal does not erase an earlier allowed cycle.

Real cryptographic signing, facilitator settlement, and current Livepeer endpoint interoperability are
out of scope. GitHub stays read-only. Tests use fake signers and no live funds.

## Goal

Prototype upstream issue 68 in this independent GitLab copy and prove real tests,
coverage, SonarQube and Paperclip review/fix iterations. GitHub remains read-only.

## Acceptance

- [x] Durable, caller-scoped intents before signing; idempotency and crash recovery.
- [x] Distinguish pre-dispatch refusal from uncertain signer outcomes; no unsafe failover.
- [x] Separate HTTP/SSE transport and payment-provider renewal lifecycle.
- [x] Preserve paid-attempt caps, orchestrator exclusions and delivered/failed cost separation.
- [x] Regression tests, coverage, SQLite, typing and local scanner gates pass. PostgreSQL and SonarQube remain GitLab CI.
- [ ] Review findings are resolved through GitLab merge requests. Independent review of this head is still required. Do not self-approve or merge.

## Progress

- [x] User authorized this isolated GitLab prototype and engineer MR workflow.
- [x] Imported repository history, branches and tags; M3 baseline pinned in PROTOTYPE.md.
- [x] x402 V2 wire (`PAYMENT-REQUIRED`/`PAYMENT-SIGNATURE`/`PAYMENT-RESPONSE`) in
  `adapters/x402.py`; signer is now explicitly supplied via `EngineSupply.x402_signer` (no
  `LPB_SIGNER_URL` auto-wiring).
- [x] `x402_intents_for_job` is an optional capability: dropped from the `CostStore` protocol and
  `CostSnapshot`; cost evidence uses a `getattr` fallback (legacy snapshots keep working).
- [x] `X402Authorization.payment` is `SecretStr | None`; the value is redacted in `repr`/JSON and
  unwrapped only at the encrypted secret store and the HTTP boundary (redaction asserted in tests).
- [x] Added x402 validation/error-branch tests: malformed V2 headers (`parse_x402_challenge`),
  unsupported requirement selection (`_select_requirement`), and authorizer error paths
  (signer with `payment=None`, mismatched intent-key reply, failed secret write, missing stored value).
- [x] Per-stream renewal: `X402PaymentProvider.open_stream` / `close_stream` / `renew_stream`.
  Metered streams renew only themselves. Single-shot payments and closed streams are not renewed in
  the background. `X402StreamStatus` shows `allowed`, `refused`, or `unknown` without a payment secret.
  The transport opens that lifecycle for `open_with_intent` and closes it in `aclose`. Cost
  classification treats an unknown renewal as uncertain and keeps an earlier allowed cycle pending
  when a later cycle was refused.
- [x] Local gates above passed on this working tree before publication.
- [ ] Independent review of the published MR head. PostgreSQL and SonarQube still run in GitLab CI;
  this local run did not execute those services.

## Decision log

- User authorization permits commits and branch pushes only to this local GitLab
  prototype for review. GitHub writes, live money movement and production deployment
  are excluded. This local authorization overrides the upstream pre-commit review rule
  for prototype MR branches; acceptance still needs independent review.
- Existing Livepeer signer/usage APIs do not promise an intent-id echo. Preserve the
  legacy correlation path and clearly mark that production interoperability gap.
  Do not fake remote idempotency or claim authorization always delays settlement.
- Use deterministic fake signers/facilitators in tests; never provision or fund real allocations.
- The engine does not start a global renewal loop over every allowed intent. Renewal tasks exist
  only for streams opened as `metered` (`runner.mode == "persistent"`).
