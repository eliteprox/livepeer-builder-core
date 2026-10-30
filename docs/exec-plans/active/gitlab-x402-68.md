# GitLab x402 provider prototype

## Status

Owner: local GitLab/Paperclip pilot. Implementation and review in progress.

## Goal

Prototype upstream issue 68 in this independent GitLab copy and prove real tests,
coverage, SonarQube and Paperclip review/fix iterations. GitHub remains read-only.

## Acceptance

- [ ] Durable, caller-scoped intents before signing; idempotency and crash recovery.
- [ ] Distinguish pre-dispatch refusal from uncertain signer outcomes; no unsafe failover.
- [ ] Separate HTTP/SSE transport and payment-provider renewal lifecycle.
- [ ] Preserve paid-attempt caps, orchestrator exclusions and delivered/failed cost separation.
- [ ] Regression tests, coverage, SQLite/Postgres, typing and scanner gates pass.
- [ ] Review findings are resolved through GitLab merge requests.

## Progress

- [x] User authorized this isolated GitLab prototype and engineer MR workflow.
- [x] Imported repository history, branches and tags; M3 baseline pinned in PROTOTYPE.md.
- [ ] Paperclip implementation and independent review.

## Decision log

- User authorization permits commits and branch pushes only to this local GitLab
  prototype for review. GitHub writes, live money movement and production deployment
  are excluded. This local authorization overrides the upstream pre-commit review rule
  for prototype MR branches; acceptance still needs independent review.
- Existing Livepeer signer/usage APIs do not promise an intent-id echo. Preserve the
  legacy correlation path and clearly mark that production interoperability gap.
  Do not fake remote idempotency or claim authorization always delays settlement.
- Use deterministic fake signers/facilitators in tests; never provision or fund real allocations.
