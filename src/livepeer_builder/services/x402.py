"""The x402 payment plane (ISS-68, PROTOTYPE.md): the provider owns the signer dispatch and the durable
intent state, separated from the media plane's RunnerTransport.

Guarantees preserved (PROTOTYPE.md #3):
1. Pre-sign crash safety (D54): the intent is durably written ``unknown`` *before* the signer is asked,
   so a crash mid-flight keeps the job ``uncertain`` and the cost ``pending``, never ``none``.
2. Refusal vs. lost reply: a signer refusal marks the intent ``refused`` (no money moved); a lost, timed-out
   or mismatched reply leaves it ``unknown`` (it may have charged, so no failover and no re-sign).
3. Paid failover (D52/D55): each attempt carries its own intent scoped to ``(job_id, attempt_number,
   cycle)``; an orchestrator that refused after payment is excluded for the next attempt.
4. Billing isolation (D44): the provider's renewal lifecycle keeps a stream's voucher renewed on a shielded
   background task that a host-side cancel or engine close must not leak.

ISS-68 review (priorities A and C):
- The media transport calls this provider's ``authorize``, never the backend signer (priority A).
- The Payment header value never rides on the intent row: it is stored in the encrypted ``SecretStore`` and
  the row carries only a ``SecretRef``; a failed secret write after signer dispatch leaves the intent
  ``unknown`` (no fabricated spend).
- A canonical challenge digest is journaled with the intent; reusing an allowed identity against a *changed*
  challenge (e.g. a new ``exchange``) raises ``X402ChallengeMismatch`` instead of silently replaying the old
  authorization, and a mismatched signer reply key leaves the intent ``unknown`` (no retry under a new key).
"""

import asyncio
import contextlib
from decimal import Decimal

from pydantic import SecretStr

from livepeer_builder.contracts import (
    X402Authorization,
    X402Challenge,
    X402IntentIdentity,
    X402IntentRecord,
)
from livepeer_builder.errors import NotFound, X402ChallengeMismatch, X402Refused, X402SignerUnknown
from livepeer_builder.protocols import Clock, SecretStore, X402IntentRepo, X402Signer
from livepeer_builder.utils import spawn_shielded


class X402PaymentProvider:
    """``authorize`` the x402 challenge against the caller-owned intent key; ``renew`` the next cycle.

    The provider is the single boundary that requests or mints the signature (PROTOTYPE.md #2): it writes
    the durable ``unknown`` intent first, asks the signer, verifies the reply key and challenge binding, and
    stores the returned ``Payment`` header value in the encrypted secret store (the intent row carries only
    its ``SecretRef``). A repeat of a held key never re-signs: ``allowed`` replays the stored value,
    ``refused`` fails loudly, and ``unknown`` refuses to re-sign.
    """

    def __init__(
        self,
        signer: X402Signer,
        intents: X402IntentRepo,
        clock: Clock,
        secrets: SecretStore,
        renewal_interval_s: float = 60.0,
    ) -> None:
        self._signer = signer
        self._intents = intents
        self._clock = clock
        self._secrets = secrets
        self._renewal_interval_s = renewal_interval_s
        self._renewal_task: asyncio.Task[None] | None = None

    async def authorize(
        self, identity: X402IntentIdentity, challenge: X402Challenge, credential: str | None = None
    ) -> X402Authorization:
        """Journal the intent, dispatch the signer, and persist the Payment header value in the secret store.

        Returns the signed ``Payment`` header value (``X402Authorization.payment``)."""
        record, created = await self._intents.insert_payment_intent(
            identity.job_id,
            identity.attempt,
            identity.cycle,
            challenge.origin,
            challenge.pay_to,
            challenge.asset,
            challenge.amount,
            challenge.nonce,
            self._clock.now(),
            challenge_digest=challenge.digest,
        )
        if created:
            try:
                auth = await self._signer.sign(identity, challenge, credential)
            except X402Refused:
                await self._intents.set_payment_intent_state(
                    identity.job_id, identity.attempt, identity.cycle, "refused", None, self._clock.now()
                )
                raise
            except X402SignerUnknown:
                # the intent stays ``unknown``: the signer was asked and gave no validated answer.
                raise
            if auth is None or auth.payment is None:
                # A validated answer is required; a None payment means the intent must stay ``unknown``.
                raise X402SignerUnknown("the signer gave no validated payment; the intent stays unknown")
            if auth.intent_key != identity.intent_key:
                # The reply is about a different intent key: untrusted, and a mismatched reply cannot be
                # retried under a new signer key (ISS-68 review, priority C).
                raise X402SignerUnknown(
                    f"the signer replied for intent key {auth.intent_key!r}, "
                    f"expected {identity.intent_key!r}; the intent stays unknown"
                )
            try:
                payment_ref = await self._secrets.put(
                    f"x402/{identity.intent_key}", auth.payment.get_secret_value()
                )
            except Exception as error:
                # The secret write failed after the signer was dispatched: the payment may have been charged,
                # so the intent must stay ``unknown`` (ISS-68 review, priority C).
                raise X402SignerUnknown(
                    "storing the payment value failed; the intent stays unknown"
                ) from error
            await self._intents.set_payment_intent_state(
                identity.job_id, identity.attempt, identity.cycle, "allowed", payment_ref, self._clock.now()
            )
            return auth
        return await self._settle(record, challenge)

    async def _settle(self, record: X402IntentRecord, challenge: X402Challenge) -> X402Authorization:
        """Settle an already-held intent: verify the challenge binding, replay the stored payment, or fail
        loudly."""
        if record.challenge_digest and record.challenge_digest != challenge.digest:
            # The challenge changed (e.g. ``exchange``) for a held key: refusing to silently replay the old
            # authorization (ISS-68 review, priority C).
            raise X402ChallengeMismatch("the challenge changed for an existing intent; refusing to re-sign")
        if record.state == "allowed":
            if record.payment_ref is None:
                raise X402SignerUnknown(
                    "the intent is allowed but has no secret reference; the signer may have charged"
                )
            try:
                payment = await self._secrets.get(record.payment_ref)
            except NotFound as error:
                # The stored value is missing: never fabricate a spend (ISS-68 review, priority B/C).
                raise X402SignerUnknown(
                    "the stored payment value is missing; the intent stays unknown"
                ) from error
            return X402Authorization(
                intent_key=record.intent_key,
                payment=SecretStr(payment),
                signed_at=record.updated_at,
            )
        if record.state == "refused":
            raise X402Refused("the signer already refused this intent; a repeat must not re-sign")
        # state is ``unknown``: the signer was asked and gave no validated answer.
        raise X402SignerUnknown("the intent is unknown (the signer may have charged); refusing to re-sign")

    async def renew(
        self, identity: X402IntentIdentity, challenge: X402Challenge, credential: str | None = None
    ) -> X402Authorization:
        """Re-sign for the next renewal cycle (PROTOTYPE.md #3.1). Idempotency is preserved by the
        ``(job_id, attempt_number, cycle)`` key: a repeat of a held cycle replays the stored payment."""
        next_identity = X402IntentIdentity(
            job_id=identity.job_id, attempt=identity.attempt, cycle=identity.cycle + 1
        )
        return await self.authorize(next_identity, challenge, credential)

    def start_renewals(self) -> None:
        """Start the shielded renewal task (PROTOTYPE.md #3.3): it keeps a stream's voucher renewed and
        survives a host that cancels every other await (D34, utils.run_shielded)."""
        if self._renewal_task is None:
            self._renewal_task = spawn_shielded(self._renewal_loop())

    async def stop_renewals(self) -> None:
        """Stop the renewal task: cancel it and wait for the in-flight renewal to finish, so a close
        never leaves a leaked task (D34)."""
        task, self._renewal_task = self._renewal_task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _renewal_loop(self) -> None:
        """Renew each ``allowed`` intent's voucher every ``renewal_interval_s`` (PROTOTYPE.md #3.3). A renewal
        failure (signer error) leaves the current cycle valid: the intent's state does not change."""
        while True:
            await asyncio.sleep(self._renewal_interval_s)
            for record in await self._intents.list_renewals():
                identity = X402IntentIdentity(
                    job_id=record.job_id, attempt=record.attempt, cycle=record.cycle
                )
                challenge = _challenge_of(record)
                try:
                    await self.renew(identity, challenge, None)
                except (X402Refused, X402SignerUnknown, X402ChallengeMismatch):
                    # A refused or unknown renewal leaves the current cycle valid (PROTOTYPE.md #3.3).
                    continue


def _challenge_of(record: X402IntentRecord) -> X402Challenge:
    return X402Challenge(
        origin=record.origin,
        pay_to=record.pay_to,
        asset=record.asset,
        amount=Decimal(record.amount),
        nonce=record.nonce,
    )
