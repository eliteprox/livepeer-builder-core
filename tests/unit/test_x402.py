"""The x402 payment plane (ISS-68, PROTOTYPE.md): the provider's intent state machine and renewal
lifecycle.

The caller owns the intent key; the provider writes the durable ``unknown`` intent before the signer is
asked, replays the stored payment for a held key, and fails loudly when the key is refused or the
signer's answer is unknown (the signer may have charged, so no failover and no re-sign, PROTOTYPE.md
#3.2). A metered stream's voucher is kept renewed by a shielded background task (PROTOTYPE.md #3.3).
"""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from livepeer_builder.contracts import (
    X402Authorization,
    X402Challenge,
    X402IntentIdentity,
    X402IntentRecord,
)
from livepeer_builder.errors import X402ChallengeMismatch, X402Refused, X402SignerUnknown
from livepeer_builder.services.x402 import X402PaymentProvider
from livepeer_builder.testing import FixedClock


class FakeSigner:
    """Answers x402 challenges: ``ok`` returns a payment, ``refused`` raises, ``unknown`` raises unknown."""

    def __init__(self, behavior: str = "ok") -> None:
        self.behavior = behavior
        self.calls: list[tuple[X402IntentIdentity, X402Challenge, str | None]] = []

    async def sign(
        self, intent: X402IntentIdentity, challenge: X402Challenge, credential: str | None
    ) -> X402Authorization:
        self.calls.append((intent, challenge, credential))
        if self.behavior == "refused":
            raise X402Refused("the signer's params were rejected")
        if self.behavior == "unknown":
            raise X402SignerUnknown("the signer's reply was lost")
        return X402Authorization(
            intent_key=intent.intent_key,
            payment=f"pay-{intent.intent_key}",
            signed_at=datetime.now(UTC),
        )


class FakeIntentRepo:
    """In-memory ``X402IntentRepo`` with the same state semantics as ``SqlPaymentIntents``."""

    def __init__(self) -> None:
        self.records: dict[tuple[UUID, int, int], X402IntentRecord] = {}
        self.writes: list[tuple[UUID, int, int, str, str, str | None]] = []

    async def insert_payment_intent(
        self,
        job_id: UUID,
        attempt: int,
        cycle: int,
        origin: str,
        pay_to: str,
        asset: str,
        amount: Decimal,
        nonce: str,
        now: datetime,
    ) -> tuple[X402IntentRecord, bool]:
        key = (job_id, attempt, cycle)
        held = self.records.get(key)
        if held is not None:
            return held, False
        record = X402IntentRecord(
            job_id=job_id,
            attempt=attempt,
            cycle=cycle,
            intent_key=f"x402:{job_id}:{attempt}:{cycle}",
            state="unknown",
            origin=origin,
            pay_to=pay_to,
            asset=asset,
            amount=amount,
            nonce=nonce,
            payment=None,
            created_at=now,
            updated_at=now,
        )
        self.records[key] = record
        return record, True

    async def get_payment_intent(self, job_id: UUID, attempt: int, cycle: int) -> X402IntentRecord | None:
        return self.records.get((job_id, attempt, cycle))

    async def set_payment_intent_state(
        self,
        job_id: UUID,
        attempt: int,
        cycle: int,
        state: str,
        payment: str | None,
        now: datetime,
        from_state: str = "unknown",
    ) -> bool:
        record = self.records.get((job_id, attempt, cycle))
        if record is None or record.state != from_state:
            return False
        self.records[(job_id, attempt, cycle)] = record.model_copy(
            update={"state": state, "payment": payment, "updated_at": now}
        )
        self.writes.append((job_id, attempt, cycle, from_state, state, payment))
        return True

    async def unknown_payment_intents(self, limit: int = 100) -> list[X402IntentRecord]:
        return [record for record in self.records.values() if record.state == "unknown"][:limit]

    async def list_renewals(self, limit: int = 100) -> list[X402IntentRecord]:
        return [
            record for record in self.records.values() if record.state == "allowed" and record.payment is not None
        ][:limit]


def identity() -> X402IntentIdentity:
    return X402IntentIdentity(job_id=uuid4(), attempt=1, cycle=0)


def challenge(nonce: str = "n1", amount: Decimal = Decimal("100000")) -> X402Challenge:
    return X402Challenge(
        origin="https://runner.example", pay_to="0xpay", asset="0xasset", amount=amount, nonce=nonce
    )


async def test_authorize_writes_the_unknown_intent_before_asking_the_signer() -> None:
    repo = FakeIntentRepo()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock())
    ident = identity()
    challenge_ = challenge()
    auth = await provider.authorize(ident, challenge_, "cred-1")
    record = repo.records[(ident.job_id, 1, 0)]
    assert (record.state, record.payment) == ("allowed", auth.payment)
    assert len(signer.calls) == 1
    assert repo.writes == [(ident.job_id, 1, 0, "unknown", "allowed", auth.payment)]


async def test_a_held_allowed_key_replays_the_stored_payment_without_resigning() -> None:
    repo = FakeIntentRepo()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock())
    ident = identity()
    challenge_ = challenge()
    first = await provider.authorize(ident, challenge_, "cred-1")
    second = await provider.authorize(ident, challenge_, "cred-1")
    assert first.payment == second.payment
    assert len(signer.calls) == 1  # the signer was asked once, not once per repeat


async def test_a_refused_intent_refuses_every_repeat() -> None:
    repo = FakeIntentRepo()
    signer = FakeSigner("refused")
    provider = X402PaymentProvider(signer, repo, FixedClock())
    ident = identity()
    challenge_ = challenge()
    with pytest.raises(X402Refused):
        await provider.authorize(ident, challenge_, "cred-1")
    record = repo.records[(ident.job_id, 1, 0)]
    assert (record.state, record.payment) == ("refused", None)
    with pytest.raises(X402Refused):
        await provider.authorize(ident, challenge_, "cred-1")
    assert len(signer.calls) == 1  # a repeat of a refused key never re-signs


async def test_an_unknown_intent_never_resigns() -> None:
    repo = FakeIntentRepo()
    signer = FakeSigner("unknown")
    provider = X402PaymentProvider(signer, repo, FixedClock())
    ident = identity()
    challenge_ = challenge()
    with pytest.raises(X402SignerUnknown):
        await provider.authorize(ident, challenge_, "cred-1")
    record = repo.records[(ident.job_id, 1, 0)]
    assert (record.state, record.payment) == ("unknown", None)  # the signer may have charged
    with pytest.raises(X402SignerUnknown):
        await provider.authorize(ident, challenge_, "cred-1")
    assert len(signer.calls) == 1


async def test_a_changed_challenge_on_a_held_key_raises_mismatch() -> None:
    repo = FakeIntentRepo()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock())
    ident = identity()
    await provider.authorize(ident, challenge(), "cred-1")
    with pytest.raises(X402ChallengeMismatch):
        await provider.authorize(ident, challenge(nonce="n2"), "cred-1")
    with pytest.raises(X402ChallengeMismatch):
        await provider.authorize(ident, challenge(amount=Decimal("99999")), "cred-1")


async def test_renew_moves_to_the_next_cycle() -> None:
    repo = FakeIntentRepo()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock())
    ident = identity()
    await provider.authorize(ident, challenge(), "cred-1")  # the original sign (cycle 0)
    await provider.renew(ident, challenge(), "cred-1")
    record = repo.records[(ident.job_id, 1, 1)]
    assert (record.state, record.payment) == ("allowed", f"pay-x402:{ident.job_id}:1:1")
    assert len(signer.calls) == 2  # the original sign and the renewal


async def test_the_renewal_task_keeps_allowed_intents_renewed() -> None:
    repo = FakeIntentRepo()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), renewal_interval_s=0.05)
    ident = identity()
    await provider.authorize(ident, challenge(), "cred-1")
    provider.start_renewals()
    await asyncio.sleep(0.2)
    record = repo.records[(ident.job_id, 1, 1)]
    assert record.state == "allowed" and record.payment is not None
    await provider.stop_renewals()
    remaining = [t for t in asyncio.all_tasks() if not t.cancelled()]
    assert len(remaining) == 1  # only this test's task: no leaked renewal task
