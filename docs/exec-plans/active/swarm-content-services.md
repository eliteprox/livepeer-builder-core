# Swarm content services

## Status

Owner: John. Reviewers: Mike for contracts and scope, Josh for engine implementation.
Ticket: not yet assigned. State: design ready for review; implementation not started.
Branch: `design/swarm-content-services`, based on `origin/releases/m3` `e69c85a`.

## Goal

Let an enterprise gateway use builder-core to publish batch and realtime content
through its own Bee, retain and render job outputs, meter publication/retrieval,
and optionally buy decentralized AAC retrieval from the Livepeer network. Preserve
enterprise ownership of retail billing and all existing paid-job failure semantics.
See the [implementation design](../../design-docs/swarm-content-services.md).

## Acceptance

- [x] Map current bytestream APIs and reserved result/session features to the proposal.
- [x] Specify direct and network providers, signing identities, ownership and billing boundaries.
- [x] Define batch/realtime flows, artifact rendering, usage evidence and durable recovery.
- [x] Identify security, retention, range integrity and bounded-resource requirements.
- [ ] Human review of the proposed contracts and relationship to ISS-15/ISS-26/ISS-27.
- [ ] Implement connected binary-image publication with a pre-provisioned batch.
- [ ] Implement reviewed artifact ports, migrations, outbox and recovery tests.
- [ ] Implement enterprise retail admission and rendering example.
- [ ] Implement direct realtime publication and optional network AAC retrieval.
- [ ] Validate real Bee/offchain integration, then a separately budgeted paid test.

## Progress

- [x] 2026-10-31: Inspected current M3 jobs, streams, native HTTP transport, costs,
  result/session reservations, and the existing Swarm publisher/playback experiment.
- [x] 2026-10-31: Created an isolated fresh worktree; preserved the user's existing
  edits on the discovery/rates checkout.
- [x] 2026-10-31: Wrote the implementation design and indexed this plan.
- [x] 2026-10-31: Documentation links/indexes/plan-shape checks and `git diff --check` pass. Both existing network capabilities were confirmed healthy.
- [ ] Implementation and contract changes await design review; no funds moved.

## Decision log

- 2026-10-31: User requested direct gateway publication and customer-paid delivery,
  with both existing network capabilities left deployed as alternatives.
- 2026-10-31: Preserve the accepted noncommercial-engine boundary: engine produces
  usage and wholesale evidence; enterprise application owns retail charges.
- 2026-10-31: Start with an optional companion adapter and connected batch-image
  slice. Do not silently implement reserved async jobs or persistent sessions.
- 2026-10-31: Keep compute and publication outcomes separate. A failed Swarm upload
  cannot trigger another paid inference request.
- 2026-10-31: Current network playback capability handles AAC only; arbitrary image
  retrieval uses direct Bee first and requires a distinct future network capability.
