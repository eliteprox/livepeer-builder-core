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
from pydantic import SecretStr

from livepeer_builder.contracts import (
    SecretRef,
    X402Authorization,
    X402Challenge,
    X402IntentIdentity,
    X402IntentRecord,
    X402StreamStatus,
)
from livepeer_builder.errors import (
    NotFound,
    X402ChallengeMismatch,
    X402Refused,
    X402SignerUnknown,
)
from livepeer_builder.services.x402 import X402PaymentProvider
from livepeer_builder.testing import FixedClock, MemorySecretStore


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
            payment=SecretStr(f"pay-{intent.intent_key}"),
            signed_at=datetime.now(UTC),
        )


class FakeIntentRepo:
    """In-memory ``X402IntentRepo`` with the same state semantics as ``SqlPaymentIntents``.

    The Payment header value never rides on the record: it lives in the encrypted secret store and the
    record carries only a ``SecretRef`` (ISS-68 review, priority C)."""

    def __init__(self) -> None:
        self.records: dict[tuple[UUID, int, int], X402IntentRecord] = {}
        self.writes: list[tuple[UUID, int, int, str, str, SecretRef | None]] = []

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
        challenge_digest: str = "",
        payment_url: str | None = None,
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
            payment_url=payment_url,
            challenge_digest=challenge_digest,
            created_at=now,
            updated_at=now,
        )
        self.records[key] = record
        return record, True

    async def get_payment_intent(self, job_id: UUID, attempt: int, cycle: int) -> X402IntentRecord | None:
        return self.records.get((job_id, attempt, cycle))

    async def intents_for_job(self, job_id: UUID) -> list[X402IntentRecord]:
        return [record for key, record in self.records.items() if key[0] == job_id]

    async def set_payment_intent_state(
        self,
        job_id: UUID,
        attempt: int,
        cycle: int,
        state: str,
        payment_ref: SecretRef | None,
        now: datetime,
        from_state: str = "unknown",
    ) -> bool:
        record = self.records.get((job_id, attempt, cycle))
        if record is None or record.state != from_state:
            return False
        self.records[(job_id, attempt, cycle)] = record.model_copy(
            update={"state": state, "payment_ref": payment_ref, "updated_at": now}
        )
        self.writes.append((job_id, attempt, cycle, from_state, state, payment_ref))
        return True

    async def unknown_payment_intents(self, limit: int = 100) -> list[X402IntentRecord]:
        return [record for record in self.records.values() if record.state == "unknown"][:limit]

    async def list_renewals(self, limit: int = 100) -> list[X402IntentRecord]:
        return [
            record
            for record in self.records.values()
            if record.state == "allowed" and record.payment_ref is not None
        ][:limit]


def identity() -> X402IntentIdentity:
    return X402IntentIdentity(job_id=uuid4(), attempt=1, cycle=0)


def challenge(nonce: str = "n1", amount: Decimal = Decimal("100000")) -> X402Challenge:
    return X402Challenge(
        origin="https://runner.example", pay_to="0xpay", asset="0xasset", amount=amount, nonce=nonce
    )


async def test_authorize_writes_the_unknown_intent_before_asking_the_signer() -> None:
    repo = FakeIntentRepo()
    secrets = MemorySecretStore()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), secrets)
    ident = identity()
    challenge_ = challenge()
    auth = await provider.authorize(ident, challenge_, "cred-1")
    record = repo.records[(ident.job_id, 1, 0)]
    assert record.state == "allowed" and record.payment_ref is not None
    # the value lives in the secret store, not the row
    assert secrets.values[record.payment_ref.ref] == auth.payment.get_secret_value()
    assert len(signer.calls) == 1
    (write,) = repo.writes
    assert write[:5] == (ident.job_id, 1, 0, "unknown", "allowed")


async def test_a_held_allowed_key_replays_the_stored_payment_without_resigning() -> None:
    repo = FakeIntentRepo()
    secrets = MemorySecretStore()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), secrets)
    ident = identity()
    challenge_ = challenge()
    first = await provider.authorize(ident, challenge_, "cred-1")
    second = await provider.authorize(ident, challenge_, "cred-1")
    assert first.payment == second.payment  # the replayed value came from the secret store
    assert len(signer.calls) == 1  # the signer was asked once, not once per repeat


async def test_a_refused_intent_refuses_every_repeat() -> None:
    repo = FakeIntentRepo()
    secrets = MemorySecretStore()
    signer = FakeSigner("refused")
    provider = X402PaymentProvider(signer, repo, FixedClock(), secrets)
    ident = identity()
    challenge_ = challenge()
    with pytest.raises(X402Refused):
        await provider.authorize(ident, challenge_, "cred-1")
    record = repo.records[(ident.job_id, 1, 0)]
    assert (record.state, record.payment_ref) == ("refused", None)  # no money moved
    with pytest.raises(X402Refused):
        await provider.authorize(ident, challenge_, "cred-1")
    assert len(signer.calls) == 1  # a repeat of a refused key never re-signs


async def test_an_unknown_intent_never_resigns() -> None:
    repo = FakeIntentRepo()
    secrets = MemorySecretStore()
    signer = FakeSigner("unknown")
    provider = X402PaymentProvider(signer, repo, FixedClock(), secrets)
    ident = identity()
    challenge_ = challenge()
    with pytest.raises(X402SignerUnknown):
        await provider.authorize(ident, challenge_, "cred-1")
    record = repo.records[(ident.job_id, 1, 0)]
    assert (record.state, record.payment_ref) == ("unknown", None)  # the signer may have charged
    with pytest.raises(X402SignerUnknown):
        await provider.authorize(ident, challenge_, "cred-1")
    assert len(signer.calls) == 1


async def test_a_changed_challenge_on_a_held_key_raises_mismatch() -> None:
    repo = FakeIntentRepo()
    secrets = MemorySecretStore()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), secrets)
    ident = identity()
    await provider.authorize(ident, challenge(), "cred-1")
    with pytest.raises(X402ChallengeMismatch):
        await provider.authorize(ident, challenge(nonce="n2"), "cred-1")  # a changed nonce changes the digest
    with pytest.raises(X402ChallengeMismatch):
        await provider.authorize(
            ident, challenge(amount=Decimal("99999")), "cred-1"
        )  # so does a changed amount


async def test_renew_moves_to_the_next_cycle() -> None:
    repo = FakeIntentRepo()
    secrets = MemorySecretStore()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), secrets)
    ident = identity()
    await provider.authorize(ident, challenge(), "cred-1")  # the original sign (cycle 0)
    auth = await provider.renew(ident, challenge(), "cred-1")
    record = repo.records[(ident.job_id, 1, 1)]
    assert record.state == "allowed" and record.payment_ref is not None
    # the renewed value is stored under the new key
    assert secrets.values[record.payment_ref.ref] == auth.payment.get_secret_value()
    assert len(signer.calls) == 2  # the original sign and the renewal


async def test_a_metered_stream_renews_only_itself() -> None:
    repo = FakeIntentRepo()
    secrets = MemorySecretStore()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), secrets)
    ident = identity()
    other = identity()
    challenge_ = challenge()
    await provider.open_stream(ident, challenge_, "cred-1", mode="metered")
    await provider.authorize(other, challenge_, "cred-1")  # allowed, but not this stream
    status = await provider.renew_stream(ident.job_id, ident.attempt)
    assert status.renewal == "allowed" and status.cycle == 1
    renewed = repo.records[(ident.job_id, 1, 1)]
    assert renewed.state == "allowed" and renewed.payment_ref is not None
    assert (other.job_id, 1, 1) not in repo.records  # another intent is not renewed
    assert len(signer.calls) == 3  # two opens plus this stream's renewal
    assert "pay-" not in str(status.model_dump(mode="json"))
    await provider.stop_renewals()


async def test_a_single_shot_payment_is_not_renewed_in_the_background() -> None:
    repo = FakeIntentRepo()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), MemorySecretStore())
    ident = identity()
    status = await provider.open_stream(ident, challenge(), "cred-secret", mode="single-shot")
    assert status.mode == "single-shot" and status.renewal == "none"
    assert provider.renewal_task_count() == 0
    provider.start_renewals()  # the engine start path must not arm a single-shot payment
    assert provider.renewal_task_count() == 0
    again = await provider.renew_stream(ident.job_id, ident.attempt)
    assert again.detail == "a single-shot payment is not renewed"
    assert len(signer.calls) == 1
    assert "cred-secret" not in str(again.model_dump(mode="json"))


async def test_a_closed_stream_is_not_renewed() -> None:
    repo = FakeIntentRepo()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), MemorySecretStore())
    ident = identity()
    await provider.open_stream(ident, challenge(), "cred-1", mode="metered")
    closed = await provider.close_stream(ident.job_id, ident.attempt)
    assert closed.phase == "closed" and provider.renewal_task_count() == 0
    with pytest.raises(X402Refused, match="a closed stream is not renewed"):
        await provider.renew_stream(ident.job_id, ident.attempt)
    assert len(signer.calls) == 1  # the close did not sign another cycle
    assert (ident.job_id, 1, 1) not in repo.records


async def test_a_refused_renewal_stays_visible_without_leaking_the_payment() -> None:
    repo = FakeIntentRepo()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), MemorySecretStore())
    ident = identity()
    await provider.open_stream(ident, challenge(), "cred-1", mode="metered")
    signer.behavior = "refused"
    with pytest.raises(X402Refused, match="the signer's params were rejected"):
        await provider.renew_stream(ident.job_id, ident.attempt)
    status = provider.stream_status(ident.job_id, ident.attempt)
    assert status.renewal == "refused"
    assert status.detail == "the signer refused the renewal"
    assert "pay-" not in repr(status) and "pay-" not in str(status.model_dump(mode="json"))
    original = repo.records[(ident.job_id, 1, 0)]
    assert original.state == "allowed"  # the current cycle stays valid
    await provider.stop_renewals()


async def test_an_unknown_renewal_stays_visible() -> None:
    repo = FakeIntentRepo()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), MemorySecretStore())
    ident = identity()
    await provider.open_stream(ident, challenge(), "cred-1", mode="metered")
    signer.behavior = "unknown"
    with pytest.raises(X402SignerUnknown, match="the signer's reply was lost"):
        await provider.renew_stream(ident.job_id, ident.attempt)
    status = provider.stream_status(ident.job_id, ident.attempt)
    assert (status.renewal, status.phase) == ("unknown", "open")
    assert status.detail == "the renewal is unknown; the signer may have charged"
    renewed = repo.records[(ident.job_id, 1, 1)]
    assert renewed.state == "unknown"  # it may have charged; the consumer can see that
    await provider.stop_renewals()


def test_stream_status_redacts_nothing_it_does_not_hold() -> None:
    status = X402StreamStatus(
        job_id=uuid4(),
        attempt=1,
        phase="open",
        mode="metered",
        cycle=2,
        renewal="unknown",
        detail="the renewal is unknown; the signer may have charged",
    )
    assert "pay-" not in repr(status)
    assert status.model_dump(mode="json")["renewal"] == "unknown"


async def test_opening_an_open_metered_stream_again_does_not_add_a_task() -> None:
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, FakeIntentRepo(), FixedClock(), MemorySecretStore())
    ident = identity()
    challenge_ = challenge()
    await provider.open_stream(ident, challenge_, "cred-1", mode="metered")
    again = await provider.open_stream(ident, challenge_, "cred-1", mode="metered")
    assert again.phase == "open" and again.cycle == 0
    assert provider.renewal_task_count() == 1
    assert len(signer.calls) == 1  # the second open replays the held payment
    await provider.stop_renewals()


async def test_a_changed_challenge_on_renewal_is_visible() -> None:
    repo = FakeIntentRepo()
    provider = X402PaymentProvider(FakeSigner(), repo, FixedClock(), MemorySecretStore())
    ident = identity()
    await provider.open_stream(ident, challenge(), "cred-1", mode="metered")
    next_cycle = X402IntentIdentity(job_id=ident.job_id, attempt=ident.attempt, cycle=1)
    await provider.authorize(next_cycle, challenge(nonce="other"), "cred-1")
    with pytest.raises(X402ChallengeMismatch, match="the challenge changed for an existing intent"):
        await provider.renew_stream(ident.job_id, ident.attempt)
    status = provider.stream_status(ident.job_id, ident.attempt)
    assert status.renewal == "refused"
    assert status.detail == "the renewal challenge did not match the open stream"
    await provider.stop_renewals()


async def test_a_missing_stream_is_not_found() -> None:
    provider = X402PaymentProvider(FakeSigner(), FakeIntentRepo(), FixedClock(), MemorySecretStore())
    ident = identity()
    with pytest.raises(NotFound, match="no x402 stream is open for this attempt"):
        provider.stream_status(ident.job_id, ident.attempt)


async def test_the_metered_loop_renews_its_own_stream_then_stops_when_closed() -> None:
    repo = FakeIntentRepo()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), MemorySecretStore(), renewal_interval_s=0.01)
    ident = identity()
    await provider.open_stream(ident, challenge(), "cred-1", mode="metered")
    for _ in range(100):
        if (ident.job_id, 1, 1) in repo.records:
            break
        await asyncio.sleep(0.01)
    renewed = repo.records[(ident.job_id, 1, 1)]
    assert renewed.state == "allowed"
    await provider.close_stream(ident.job_id, ident.attempt)
    signed = len(signer.calls)
    await asyncio.sleep(0.05)
    assert len(signer.calls) == signed  # a closed stream's loop does not sign again


async def test_stopping_the_provider_cancels_metered_renewal_tasks() -> None:
    provider = X402PaymentProvider(
        FakeSigner(), FakeIntentRepo(), FixedClock(), MemorySecretStore(), renewal_interval_s=60
    )
    ident = identity()
    await provider.open_stream(ident, challenge(), "cred-1", mode="metered")
    assert provider.renewal_task_count() == 1
    await provider.stop_renewals()
    assert provider.renewal_task_count() == 0
    status = provider.stream_status(ident.job_id, ident.attempt)
    assert status.phase == "closed"
    remaining = [task for task in asyncio.all_tasks() if not task.done()]
    assert len(remaining) == 1  # only this test's task: no leaked renewal task


def test_authorization_redacts_the_payment_in_repr_and_dumps() -> None:
    auth = X402Authorization(
        intent_key="k",
        payment=SecretStr("pay-k"),
        signed_at=datetime.now(UTC),
    )
    # the repr redacts the secret value
    assert "pay-k" not in repr(auth)
    assert "**********" in repr(auth)
    # the serialized (JSON) dump redacts the secret value (only a secret reference may ride on records)
    dumped = auth.model_dump(mode="json")
    assert "pay-k" not in str(dumped)
    assert dumped["payment"] == "**********"
    # the value is only reachable through the explicit unwrap at the secret store / HTTP boundary
    assert auth.payment.get_secret_value() == "pay-k"


class NonePaymentSigner:
    """A signer that answers with an ``X402Authorization`` whose ``payment`` is ``None``: the validated
    payment value is absent, so the intent must stay ``unknown``."""

    def __init__(self) -> None:
        self.calls: list[X402IntentIdentity] = []

    async def sign(
        self, intent: X402IntentIdentity, challenge: X402Challenge, credential: str | None
    ) -> X402Authorization:
        self.calls.append(intent)
        return X402Authorization(intent_key=intent.intent_key, payment=None, signed_at=datetime.now(UTC))


class MismatchedKeySigner:
    """A signer that answers for a *different* intent key: the reply is untrusted, the intent stays
    ``unknown`` (ISS-68 review, priority C)."""

    def __init__(self) -> None:
        self.calls: list[X402IntentIdentity] = []

    async def sign(
        self, intent: X402IntentIdentity, challenge: X402Challenge, credential: str | None
    ) -> X402Authorization:
        self.calls.append(intent)
        return X402Authorization(
            intent_key="a-different-key", payment=SecretStr("pay-other"), signed_at=datetime.now(UTC)
        )


class _FailingPutStore:
    """A ``SecretStore`` whose ``put`` fails after the signer was dispatched: the payment may have been
    charged, so the intent must stay ``unknown`` (never fabricate a spend)."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def put(self, name: str, secret: str) -> SecretRef:
        raise RuntimeError("the encrypted secret write failed")

    async def get(self, ref: SecretRef) -> str:
        if ref.ref not in self.values:
            raise NotFound("unknown secret reference")
        return self.values[ref.ref]

    async def delete(self, ref: SecretRef) -> None:
        self.values.pop(ref.ref, None)


async def test_a_signer_with_no_validated_payment_leaves_the_intent_unknown() -> None:
    repo = FakeIntentRepo()
    signer = NonePaymentSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), MemorySecretStore())
    ident = identity()
    with pytest.raises(X402SignerUnknown):
        await provider.authorize(ident, challenge(), "cred-1")
    record = repo.records[(ident.job_id, 1, 0)]
    assert (record.state, record.payment_ref) == ("unknown", None)  # it may have charged
    assert len(signer.calls) == 1


async def test_a_mismatched_signer_reply_key_leaves_the_intent_unknown() -> None:
    repo = FakeIntentRepo()
    signer = MismatchedKeySigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), MemorySecretStore())
    ident = identity()
    with pytest.raises(X402SignerUnknown):
        await provider.authorize(ident, challenge(), "cred-1")
    record = repo.records[(ident.job_id, 1, 0)]
    assert (record.state, record.payment_ref) == ("unknown", None)  # a mismatched reply is untrusted


async def test_a_failed_secret_write_leaves_the_intent_unknown() -> None:
    repo = FakeIntentRepo()
    signer = FakeSigner()
    provider = X402PaymentProvider(signer, repo, FixedClock(), _FailingPutStore())
    ident = identity()
    with pytest.raises(X402SignerUnknown):
        await provider.authorize(ident, challenge(), "cred-1")
    record = repo.records[(ident.job_id, 1, 0)]
    assert (record.state, record.payment_ref) == ("unknown", None)  # the secret write failed after dispatch


def _seeded_allowed_record(ident: X402IntentIdentity, payment_ref: SecretRef | None) -> X402IntentRecord:
    now = datetime.now(UTC)
    return X402IntentRecord(
        job_id=ident.job_id,
        attempt=1,
        cycle=0,
        intent_key=ident.intent_key,
        state="allowed",
        origin="https://runner.example",
        pay_to="0xpay",
        asset="0xasset",
        amount=Decimal("100000"),
        nonce="n1",
        payment_ref=payment_ref,
        created_at=now,
        updated_at=now,
    )


async def test_an_allowed_intent_without_a_secret_ref_stays_unknown() -> None:
    repo = FakeIntentRepo()
    ident = identity()
    repo.records[(ident.job_id, 1, 0)] = _seeded_allowed_record(ident, None)
    provider = X402PaymentProvider(FakeSigner(), repo, FixedClock(), MemorySecretStore())
    with pytest.raises(X402SignerUnknown):
        await provider.authorize(ident, challenge(), "cred-1")  # replays the held allowed intent
    record = repo.records[(ident.job_id, 1, 0)]
    assert (record.state, record.payment_ref) == (
        "allowed",
        None,
    )  # _settle refused to replay; state unchanged


async def test_a_missing_stored_payment_value_stays_unknown() -> None:
    repo = FakeIntentRepo()
    ident = identity()
    repo.records[(ident.job_id, 1, 0)] = _seeded_allowed_record(ident, SecretRef(ref="mem:missing"))
    provider = X402PaymentProvider(FakeSigner(), repo, FixedClock(), MemorySecretStore())
    with pytest.raises(X402SignerUnknown):
        await provider.authorize(ident, challenge(), "cred-1")


def test_challenge_timeout_property_exposes_max_timeout_seconds() -> None:
    assert challenge().timeout == 300
    assert challenge(amount=Decimal("200000")).timeout == 300
