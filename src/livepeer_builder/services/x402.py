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
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from pydantic import SecretStr

from livepeer_builder.contracts import (
    X402Authorization,
    X402Challenge,
    X402IntentIdentity,
    X402IntentRecord,
    X402StreamMode,
    X402StreamStatus,
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
        self._streams: dict[tuple[UUID, int], _Stream] = {}

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

    async def open_stream(
        self,
        identity: X402IntentIdentity,
        challenge: X402Challenge,
        credential: str | None = None,
        *,
        mode: X402StreamMode = "single-shot",
    ) -> X402StreamStatus:
        """Bind one stream to the provider. Only a metered stream starts a renewal task."""
        await self.authorize(identity, challenge, credential)
        key = (identity.job_id, identity.attempt)
        session = self._streams.get(key)
        if session is None or session.phase == "closed":
            session = _Stream(
                identity=identity,
                challenge=challenge,
                credential=credential,
                mode=mode,
                phase="open",
                renewal="none",
                detail="",
            )
            self._streams[key] = session
        else:
            session.identity = identity
            session.challenge = challenge
            session.credential = credential
            session.mode = mode
            session.phase = "open"
        if mode == "metered":
            self._arm(key)
        return _status(session)

    async def close_stream(self, job_id: UUID, attempt: int) -> X402StreamStatus:
        """Close this stream and cancel its renewal. Other streams are left alone."""
        session = self._require(job_id, attempt)
        session.phase = "closed"
        await self._cancel(session)
        session.detail = "the stream is closed"
        return _status(session)

    def stream_status(self, job_id: UUID, attempt: int) -> X402StreamStatus:
        """The consumer-visible renewal outcome. The payment value is not on this object."""
        return _status(self._require(job_id, attempt))

    async def renew_stream(self, job_id: UUID, attempt: int) -> X402StreamStatus:
        """Renew this open metered stream once. A closed or single-shot stream is not re-signed."""
        session = self._require(job_id, attempt)
        if session.phase == "closed":
            session.detail = "a closed stream is not renewed"
            raise X402Refused(session.detail)
        if session.mode != "metered":
            session.detail = "a single-shot payment is not renewed"
            return _status(session)
        try:
            await self.renew(session.identity, session.challenge, session.credential)
        except X402Refused:
            session.renewal = "refused"
            session.detail = "the signer refused the renewal"
            raise
        except X402SignerUnknown:
            session.renewal = "unknown"
            session.detail = "the renewal is unknown; the signer may have charged"
            raise
        except X402ChallengeMismatch:
            session.renewal = "refused"
            session.detail = "the renewal challenge did not match the open stream"
            raise
        session.renewal = "allowed"
        session.detail = ""
        session.identity = X402IntentIdentity(
            job_id=session.identity.job_id,
            attempt=session.identity.attempt,
            cycle=session.identity.cycle + 1,
        )
        return _status(session)

    def renewal_task_count(self) -> int:
        """How many metered streams currently have a live renewal task."""
        count = 0
        for session in self._streams.values():
            task = session.task
            if task is not None and not task.done():
                count += 1
        return count

    def start_renewals(self) -> None:
        """Resume renewal tasks for streams already opened as metered.

        This does not scan every allowed intent. A default single-shot payment has no stream task, and a
        closed stream is not armed (PROTOTYPE.md #3.3)."""
        for key, session in self._streams.items():
            if session.phase == "open" and session.mode == "metered":
                self._arm(key)

    async def stop_renewals(self) -> None:
        """Close every open stream's renewal task so an engine stop cannot leak one (D34)."""
        for session in list(self._streams.values()):
            session.phase = "closed"
            await self._cancel(session)

    def _require(self, job_id: UUID, attempt: int) -> "_Stream":
        session = self._streams.get((job_id, attempt))
        if session is None:
            raise NotFound("no x402 stream is open for this attempt")
        return session

    def _arm(self, key: tuple[UUID, int]) -> None:
        session = self._streams[key]
        if session.task is not None and not session.task.done():
            return
        session.task = spawn_shielded(self._stream_loop(key))

    async def _cancel(self, session: "_Stream") -> None:
        task, session.task = session.task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _stream_loop(self, key: tuple[UUID, int]) -> None:
        """Renew only this stream. A refusal or an unknown answer stops the loop and stays visible."""
        while True:
            await asyncio.sleep(self._renewal_interval_s)
            session = self._streams.get(key)
            if session is None or session.phase != "open" or session.mode != "metered":
                return
            try:
                await self.renew_stream(key[0], key[1])
            except (X402Refused, X402SignerUnknown, X402ChallengeMismatch):
                return


@dataclass
class _Stream:
    """Provider-owned stream state. The credential stays here and never on ``X402StreamStatus``."""

    identity: X402IntentIdentity
    challenge: X402Challenge
    credential: str | None
    mode: X402StreamMode
    phase: Literal["open", "closed"]
    renewal: Literal["none", "allowed", "refused", "unknown"]
    detail: str
    task: asyncio.Task[None] | None = None


def _status(session: _Stream) -> X402StreamStatus:
    return X402StreamStatus(
        job_id=session.identity.job_id,
        attempt=session.identity.attempt,
        phase=session.phase,
        mode=session.mode,
        cycle=session.identity.cycle,
        renewal=session.renewal,
        detail=session.detail,
    )
