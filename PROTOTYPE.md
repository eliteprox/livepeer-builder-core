# Independent GitLab prototype

Upstream issue: https://github.com/NE-Build-Track/livepeer-builder-core/issues/68

Source releases/m3 commit: `e6f4ba12053da50b2a111bbd80d43b2a6974230f`. All branches/tags were copied.

This is a separate application experiment. No GitHub writes or live payment execution.

[Execution plan](docs/exec-plans/active/gitlab-x402-68.md)

## Upstream issue snapshot

## Context

Across recent M3 payment-attribution work ([#44](https://github.com/NE-Build-Track/livepeer-builder-core/pull/44) / D42, [#61](https://github.com/NE-Build-Track/livepeer-builder-core/pull/61) / D52–D53, and [#64](https://github.com/NE-Build-Track/livepeer-builder-core/pull/64) / D54–D55), `livepeer-builder` has steadily hardened cost attribution, paid failover, and signing-failure semantics. However, doing so inside the current architecture required taking ownership of the SDK's `call_runner` loop (`src/livepeer_builder/adapters/runner_call.py`, tech debt `T18`) and building a multi-state SQL matching machine (`src/livepeer_builder/store/matching.py`) around an orchestrator-minted `manifest_id` (`T13`).

This issue documents how restructuring `builder-core` around an **x402 payment-provider / facilitator boundary** separates the media/inference plane from the payment plane while preserving (and simplifying) every guarantee enforced in D42, D52, D53, D54, and D55.

---

## 1. Why the current coupled loop forces complexity

Today, `PaidRequest._answered` in `src/livepeer_builder/adapters/runner_call.py` executes four distinct concerns inside one imperative loop:

1. Sends the initial HTTP/SSE request to the runner.
2. Parses the Livepeer `402` challenge (`payment_params`, `manifest_id`, `payment_url`).
3. Orchestrates a two-phase local DB dance around the orchestrator's `manifest_id`:
   - Calls `remember(manifest_id)` to persist `SignedSession(payment_state="unknown")` before asking the signer (D54).
   - Calls `LivePaymentSession.get_payment()` on the remote signer.
   - Calls `on_signed(manifest_id)` (`payment_state="paid"`, `payment_sent=True`) or `refused(manifest_id)` (`payment_state="unpaid"`).
4. Spawns `LivePaymentSession.run_payments()` for metered funding while reading the response or streaming chunks.

### Consequences of coupling media and payment state
- **Two-phase local state around a foreign correlation key (`T13`):** Because `go-livepeer` mints `manifest_id` as 4 random bytes (`core.RandomManifestID()`), it is fresh per challenge but not globally unique or caller-scoped. The engine must persist `unknown` → `paid`/`unpaid` transitions locally and run set-uniqueness matching (`unmatched`, `matched`, `ambiguous`) against Batteries usage rows.
- **Subtle cancellation and timeout windows (#64 review):** Because the media transport drives the pre-sign DB write (`remember()`), a `first_byte_timeout_s` or cancellation firing between `remember()` and `get_payment()` can leave a job `failed` while an `unknown` session remains attached (leaving `JobCost.status == "pending"` indefinitely), and `ambiguous` sets can shrink when an `unknown` sibling later transitions to `unpaid`.
- **Job-level vs. attempt-level cost conflation in downstream apps:** When a job fails over after payment (Attempt 1 = `failed_after_payment`, Attempt 2 = `delivered`), `JobCost` carries both total allocation spend (`fees`) and delivered-only spend (`billable`), plus per-attempt `AttemptCost`. Downstream consumers that collapse a job's network cost to a single `(status, total_amount)` tuple—such as `wholesale_from(cost)` in `chatlpt-sample-enterprise-app` (`src/chatlpt/adapters/engine.py`)—classify the entire job as `"delivered"` when Attempt 2 succeeds, folding Attempt 1's failed network spend into conversation-level `"delivered"` totals even though account-level summaries (`CostSummary.by_class`) and retail token billing remain exact:

```json
{
  "job_id": "31becae4-95ea-4524-a0ee-9d370c52a21a",
  "retail": [
    {
      "job_id": "31becae4-95ea-4524-a0ee-9d370c52a21a",
      "prompt_tokens": 56,
      "completion_tokens": 2,
      "retail_usd": "0.000034"
    }
  ],
  "job_network_total_usd": "0.000221628288545308",
  "engine_delivered_only_billable_fees": {
    "fee_eth": "4.745863282E-8",
    "fee_usd": "0.000110814144272654",
    "row_count": 1
  },
  "account_summary_for_job": {
    "delivered": { "amount": "0.000110814144272654", "currency": "USD" },
    "failed": { "amount": "0.000110814144272654", "currency": "USD" },
    "pending": 0
  },
  "conversation_summary_for_job": {
    "delivered": { "amount": "0.000221628288545308", "currency": "USD" },
    "failed": { "amount": "0", "currency": "USD" },
    "pending": 0
  }
}
```

---

## 2. Target Architecture: x402 Provider / Facilitator Separation

In the **x402** model, `402 Payment Required` is split across two orthogonal planes:

1. **Media / Data Plane (`RunnerTransport` ↔ Runner):**
   - Sends the HTTP/SSE request, receives either `402` (challenge headers/body) or `200 OK` (with settlement receipt), and streams the payload bytes.
   - Has zero knowledge of allocations, Batteries, outboxes, or SQL session tables.
2. **Payment Plane (`X402PaymentProvider` ↔ Signer / Facilitator + Ledger):**
   - Accepts a payment requirement (`challenge`) and a **caller-owned idempotency key** `payment_intent_id = (job_id, attempt_number, cycle)`.
   - Atomically records the intent, checks policy/allowance, and returns a signed payment header (`authorize`), or refuses before signing.

| Responsibility | Current `builder-core` (Coupled) | x402 Provider Architecture (Decoupled) |
| :--- | :--- | :--- |
| **Media transport** | `PaidRequest` in `runner_call.py` parses 402s, calls `remember()`, calls `remote_signer`, spawns `run_payments()`, and mutates `Attempt.sessions`. | Standard HTTP/SSE client with an **x402 interceptor**: on `402`, delegates challenge to `X402PaymentProvider.authorize(attempt_id, challenge)` and attaches the returned header. |
| **Intent durability (D54)** | Transport calls `remember(manifest_id)` over an `OnSigned` callback before calling `get_payment()`. | **Inside the x402 Provider / Signer:** `authorize()` takes a deterministic client key `(job_id, attempt_number, cycle)` and persists the intent in the same boundary that requests or mints the signature. |
| **Correlation key (`T13`)** | Orchestrator's 4-byte random `manifest_id` (can collide → `unmatched` / `ambiguous` state machine in `store/matching.py`). | **Client/Facilitator-scoped `payment_intent_id` / `nonce`** passed through to the signer/facilitator and echoed on usage rows. Join is 1:1 by primary key. |
| **Metered renewals** | Background `asyncio.Task` (`LivePaymentSession.run_payments()`) inside the media transport. | Session/voucher renewal lifecycle owned by the x402 payment provider, bound to the stream's context manager. |

---

## 3. Preserving Every Guarantee from D42, D52, D53, D54, and D55

1. **Pre-sign crash safety (D54 `remember`):**
   - *Invariant:* Never sign a payment unless the intent is durably linked to `(job_id, attempt_number)`. If the process crashes while waiting for the signer/facilitator, cost must be `pending`, never `none` ($0).
   - *x402 mechanism:* `X402PaymentProvider.authorize(attempt_id, challenge)` generates a deterministic idempotency key (`intent_id`) and writes `PaymentIntent(state="unknown")` before invoking the signer—and forwards `intent_id` to the signer/facilitator so usage rows carry `intent_id` directly. A crash mid-flight leaves the durable `PaymentIntent`, keeping the job `uncertain` and its cost `pending` until synced.

2. **Refusal vs. Lost Reply (`unpaid` vs. `unknown`):**
   - *Invariant:* Signer 400/401/402/403/480/481 means no money moved (`unpaid`, never billed); signer 5xx/timeout/cancel means money *might* have moved (`unknown`, hold failover and wait for settlement).
   - *x402 mechanism:* Because the x402 provider owns both the intent write and the signer call behind a single `authorize()` boundary, any deadline or error *before* the signer request is dispatched marks or rolls back the intent cleanly to `unpaid` (preventing the dangling-`unknown`-on-`failed`-job bug). And because correlation is keyed by `intent_id` rather than a shared `manifest_id`, a refused intent never collides with another attempt's session.

3. **Paid Failover, Orchestrator Exclusion, and Operator Cap (D52 & D55):**
   - *Invariant:* If an orchestrator returns 404/409/503 *after* payment is signed, fail over to a different orchestrator (up to `LPB_MAX_PAID_ATTEMPTS`), classify Attempt 1's spend as `failed_after_payment` (operator expense), and only count Attempt 2's `delivered` spend in `JobCost.billable`.
   - *x402 mechanism:* Each attempt creates its own `PaymentIntent` scoped to `(job_id, attempt_number)` and the target `payTo` / orchestrator origin. When the runner returns 404/409/503 after authorization, the attempt's intent is classified as `failed_after_payment`, the `payTo` origin is excluded for subsequent attempts, and `LPB_MAX_PAID_ATTEMPTS` bounds the total authorized attempts per job.
   - *Two-phase settlement upside:* In standard x402 facilitator flows where authorization (`PAYMENT-SIGNATURE`) is verified first and settlement (`PAYMENT-RESPONSE`) only commits when the resource server accepts the work (`200 OK` / first byte), pre-execution runner refusals (404/409/503) never settle on-chain or in the outbox—reducing `failed_after_payment` network waste to $0.

4. **Customer Billing Isolation (Retail vs. Wholesale Delivered vs. Wholesale Failed):**
   - Retail token billing remains strictly tied to completed replies.
   - For wholesale network cost reporting, exposing first-class `(delivered_fees, failed_fees)` (or `billable` vs. `fees - billable`) directly on the cost contract prevents downstream applications from collapsing multi-attempt failover jobs into a single `"delivered"` bucket when computing per-conversation totals.
