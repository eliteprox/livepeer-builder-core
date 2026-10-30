"""The x402 payment intent store (ISS-68, PROTOTYPE.md #3.1): durable, atomic and restart-safe on both
SQLite and PostgreSQL (tests/store/conftest.py runs every store test on each).

The caller-owned ``intent_key`` is a unique constraint: two concurrent ``authorize``s for one intent cannot
both win. Every state write is a compare-and-set, so a racing renewal or close writes nothing. The ``unknown``
intents a crash left behind are listed so a restart sees the pending cost, never zeroes it.
"""

from collections.abc import AsyncIterator
from decimal import Decimal

import pytest

from livepeer_builder.contracts import Job, SecretRef
from livepeer_builder.store import SqlStore
from livepeer_builder.testing import FixedClock
from tests.store.conftest import make_job

REF = SecretRef(ref="pay-1")


@pytest.fixture
async def sql(database_url: str) -> AsyncIterator[SqlStore]:
    store = await SqlStore.connect(database_url)
    yield store
    await store.close()


async def make_job_row(sql: SqlStore, clock: FixedClock, actor: str = "alice") -> Job:
    job = make_job(clock, actor=actor)
    await sql.create_job(job, None)
    return job


async def test_a_second_insert_of_a_held_key_reads_the_winner(sql: SqlStore, clock: FixedClock) -> None:
    job = await make_job_row(sql, clock)
    first, created = await sql.insert_payment_intent(
        job.id, 1, 0, "https://runner.example", "0xpay", "0xasset", Decimal("100000"), "n1", clock.now()
    )
    assert created and first.state == "unknown"
    assert first.intent_key == f"x402:{job.id}:1:0"
    held, created_again = await sql.insert_payment_intent(
        job.id, 1, 0, "https://runner.example", "0xpay", "0xasset", Decimal("100000"), "n1", clock.now()
    )
    assert not created_again and held == first  # the loser reads the winner's row


async def test_state_writes_are_compare_and_set(sql: SqlStore, clock: FixedClock) -> None:
    job = await make_job_row(sql, clock)
    await sql.insert_payment_intent(
        job.id, 1, 0, "https://runner.example", "0xpay", "0xasset", Decimal("100000"), "n1", clock.now()
    )
    assert await sql.set_payment_intent_state(job.id, 1, 0, "allowed", REF, clock.now()) is True
    assert (
        await sql.set_payment_intent_state(job.id, 1, 0, "allowed", REF, clock.now()) is False
    )  # already allowed
    assert (
        await sql.set_payment_intent_state(job.id, 1, 0, "refused", None, clock.now()) is False
    )  # wrong from_state
    assert (
        await sql.set_payment_intent_state(job.id, 1, 0, "refused", None, clock.now(), from_state="allowed")
        is True
    )
    record = await sql.get_payment_intent(job.id, 1, 0)
    assert record is not None and record.state == "refused" and record.payment_ref is None


async def test_unknown_intents_are_recovered_after_a_crash(sql: SqlStore, clock: FixedClock) -> None:
    job = await make_job_row(sql, clock)
    await sql.insert_payment_intent(
        job.id, 1, 0, "https://runner.example", "0xpay", "0xasset", Decimal("100000"), "n1", clock.now()
    )
    [unknown] = await sql.unknown_payment_intents()
    assert (unknown.job_id, unknown.state) == (job.id, "unknown")
    assert await sql.set_payment_intent_state(job.id, 1, 0, "allowed", REF, clock.now())
    assert await sql.unknown_payment_intents() == []  # settled: no pending cost remains


async def test_renewals_list_the_allowed_intents_with_a_payment(sql: SqlStore, clock: FixedClock) -> None:
    job = await make_job_row(sql, clock)
    await sql.insert_payment_intent(
        job.id, 1, 0, "https://runner.example", "0xpay", "0xasset", Decimal("100000"), "n1", clock.now()
    )
    assert await sql.set_payment_intent_state(job.id, 1, 0, "allowed", REF, clock.now())
    [renewal] = await sql.list_renewals()
    assert (renewal.job_id, renewal.cycle, renewal.payment_ref) == (
        job.id,
        0,
        REF,
    )  # the value lives in the secret store

    other = await make_job_row(sql, clock, actor="bob")
    await sql.insert_payment_intent(
        other.id, 1, 0, "https://runner.example", "0xpay", "0xasset", Decimal("100000"), "n1", clock.now()
    )
    assert await sql.set_payment_intent_state(other.id, 1, 0, "allowed", None, clock.now())
    assert [renewal] == await sql.list_renewals()  # the payment-less intent is excluded


async def test_a_renewal_creates_the_next_cycle_row(sql: SqlStore, clock: FixedClock) -> None:
    job = await make_job_row(sql, clock)
    await sql.insert_payment_intent(
        job.id, 1, 0, "https://runner.example", "0xpay", "0xasset", Decimal("100000"), "n1", clock.now()
    )
    await sql.set_payment_intent_state(job.id, 1, 0, "allowed", REF, clock.now())
    second, created = await sql.insert_payment_intent(
        job.id, 1, 1, "https://runner.example", "0xpay", "0xasset", Decimal("100000"), "n1", clock.now()
    )
    assert created and second.intent_key == f"x402:{job.id}:1:1"
    assert await sql.set_payment_intent_state(job.id, 1, 1, "allowed", REF, clock.now(), from_state="unknown")
    assert len(await sql.list_renewals()) == 2  # both cycles are renewal candidates
