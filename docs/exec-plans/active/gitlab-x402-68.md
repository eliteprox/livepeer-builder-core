# GitLab x402 provider prototype

## Status

Owner: local GitLab/Paperclip pilot. Draft repair published (MR!2, commit 073fc66a). All local
gates are green (ruff, mypy, 1566 tests @ 97.47% total, diff-cover 95% vs origin/releases/m3,
docs, gen-docs --check, smoke). The per-stream/metered renewal lifecycle and usage integration are
**not implemented**; the provider still uses the global `start_renewals`/`stop_renewals` loop.

## Goal

Prototype upstream issue 68 in this independent GitLab copy and prove real tests,
coverage, SonarQube and Paperclip review/fix iterations. GitHub remains read-only.

## Acceptance

- [x] Durable, caller-scoped intents before signing; idempotency and crash recovery.
- [x] Distinguish pre-dispatch refusal from uncertain signer outcomes; no unsafe failover.
- [ ] Separate HTTP/SSE transport and payment-provider renewal lifecycle.
- [x] Preserve paid-attempt caps, orchestrator exclusions and delivered/failed cost separation.
- [x] Regression tests, coverage, SQLite/Postgres, typing and scanner gates pass.
- [x] Review findings are resolved through GitLab merge requests.

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
  Changed-line (diff) coverage went from 88% to 95% vs origin/releases/m3.
- [x] All local gates green: `uv sync --all-extras --frozen`, `ruff check`, `ruff format --check`,
  `mypy`, `pytest --cov --cov-report=xml --cov-report=json` (1566 passed, 97.47%),
  `scripts/check_coverage.py`, `diff-cover --compare-branch=origin/releases/m3 --fail-under=90`,
  `scripts/check_docs.py`, `lpb gen-docs --check`, `lpb smoke`.
- [ ] Paperclip implementation and independent review.
- [ ] Per-stream/metered renewal lifecycle and usage integration are NOT implemented; the provider
  still relies on the global `start_renewals`/`stop_renewals` background loop. Do not describe them as
  done.

## Decision log

- User authorization permits commits and branch pushes only to this local GitLab
  prototype for review. GitHub writes, live money movement and production deployment
  are excluded. This local authorization overrides the upstream pre-commit review rule
  for prototype MR branches; acceptance still needs independent review.
- Existing Livepeer signer/usage APIs do not promise an intent-id echo. Preserve the
  legacy correlation path and clearly mark that production interoperability gap.
  Do not fake remote idempotency or claim authorization always delays settlement.
- Use deterministic fake signers/facilitators in tests; never provision or fund real allocations.
