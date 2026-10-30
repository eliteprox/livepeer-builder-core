"""The x402 payment plane (ISS-68, PROTOTYPE.md): the provider owns the signer dispatch and the durable
intent state, separated from the media plane's RunnerTransport.

Guarantees preserved (PROTOTYPE.md #3):
1. Pre-sign crash safety (D54): the intent is durably written ``unknown`` *before* the signer is asked,
   so a crash mid-flight keeps the job ``uncertain`` and the cost ``pending``, never ``none``.
2. Refusal vs. lost reply: a signer 400/401/402/403/480/481 marks the intent ``refused`` (no money moved);
   a 5xx, timeout, or cancel leaves it ``unknown`` (it may have charged, so no failover and no re-sign).
3. Paid failover (D52/D55): each attempt carries its own intent scoped to ``(job_id, attempt_number,
   cycle)``; an orchestrator that refused after payment is excluded for the next attempt.
4. Billing isolation (D44): the provider's renewal lifecycle keeps a stream's voucher renewed on a shielded
   background task that a host-side cancel or engine close must not leak.
"""

import asyncio
import contextlib
from decimal import Decimal

from livepeer_builder.contracts import (
    X402Authorization,
    X402Challenge,
    X402IntentIdentity,
    X402IntentRecord,
)
from livepeer_builder.errors import X402ChallengeMismatch, X402Refused, X402SignerUnknown
from livepeer_builder.protocols import Clock, X402IntentRepo, X402Signer
from livepeer_builder.utils import run_shielded, spawn_shielded


class X402PaymentProvider:
    """``authorize`` the x402 challenge against the caller-owned intent key; ``renew`` the next cycle.

    The provider is the single boundary that requests or mints the signature (PROTOTYPE.md #2): it writes
    the durable ``unknown`` intent first, asks the signer, and stores the returned ``Payment`` header value
    on success. A repeat of a held key never re-signs: ``allowed`` replays the stored value, ``refused``
    fails loudly, and ``unknown`` refuses to re-sign.
    """

    def __init__(
        self,
        signer: X402Signer,
        intents: X402IntentRepo,
        clock: Clock,
        renewal_interval_s: float = 60.0,
    ) -> None:
        self._signer = signer
        self._intents = intents
        self._clock = clock
        self._renewal_interval_s = renewal_interval_s
        self._renewal_task: asyncio.Task | None = None

    async def authorize(self, identity: X402IntentIdentity, challenge: X402Challenge, credential: str | None) -> X402Authorization:
        """Sign ``challenge`` under ``identity`` and persist the durable intent. Returns the signed ``Payment``
        header value (``X402Authorization.payment``)."""
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
            await self._intents.set_payment_intent_state(
                identity.job_id, identity.attempt, identity.cycle, "allowed", auth.payment, self._clock.now()
            )
            return auth
        return await self._settle(record, challenge, credential)

    async def _settle(self, record: X402IntentRecord, challenge: X402Challenge, credential: str | None) -> X402Authorization:
        """Settle an already-held intent: replay the stored payment, or fail loudly."""
        if (
            record.origin,
            record.pay_to,
            record.asset,
            record.amount,
            record.nonce,
        ) != (
            challenge.origin,
            challenge.pay_to,
            challenge.asset,
            challenge.amount,
            challenge.nonce,
        ):
            raise X402ChallengeMismatch(
                "the challenge changed for an existing intent; refusing to re-sign"
            )
        if record.state == "allowed":
            if record.payment is None:
                raise X402SignerUnknown("the intent is allowed but carries no payment value; the signer may have charged")
            return X402Authorization(
                intent_key=record.intent_key,
                payment=record.payment,
                signed_at=record.updated_at,
            )
        if record.state == "refused":
            raise X402Refused("the signer already refused this intent; a repeat must not re-sign")
        # state is ``unknown``: the signer was asked and gave no validated answer.
        raise X402SignerUnknown("the intent is unknown (the signer may have charged); refusing to re-sign")

    async def renew(self, identity: X402IntentIdentity, challenge: X402Challenge, credential: str | None) -> X402Authorization:
        """Re-sign for the next renewal cycle (PROTOTYPE.md #3.1). Idempotency is preserved by the
        ``(job_id, attempt_number, cycle)`` key: a repeat of a held cycle replays the stored payment."""
        next_identity = X402IntentIdentity(job_id=identity.job_id, attempt=identity.attempt, cycle=identity.cycle + 1)
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
                await run_shielded(task)

    async def _renewal_loop(self) -> None:
        """Renew each ``allowed`` intent's voucher every ``renewal_interval_s`` (PROTOTYPE.md #3.3). A renewal
        failure (signer error) leaves the current cycle valid: the intent's state does not change."""
        while True:
            await asyncio.sleep(self._renewal_interval_s)
            for record in await self._intents.list_renewals():
                identity = X402IntentIdentity(job_id=record.job_id, attempt=record.attempt, cycle=record.cycle)
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
