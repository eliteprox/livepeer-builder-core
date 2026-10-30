"""The x402 payment intents in SQL (ISS-68): ``lpb_payment_intents``, one row per (job, attempt, cycle).

The primary key is the unique constraint the caller-owned ``intent_key`` is stored under, so two
concurrent ``authorize``s for one intent cannot both win: the loser reads the row the winner stored.
Every state write is a compare-and-set on the current state, so a renewal or close that raced writes
nothing, and the finite ``unknown -> allowed | refused`` transitions stay explicit.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import and_, insert, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from livepeer_builder.contracts import X402IntentRecord
from livepeer_builder.store import schema as t
from livepeer_builder.utils import utc


def pk(job_id: UUID, attempt: int, cycle: int) -> Any:
    c = t.payment_intents.c
    return and_(c.job_id == str(job_id), c.attempt_number == attempt, c.cycle == cycle)


async def insert_payment_intent(
    conn: AsyncConnection,
    job_id: UUID,
    attempt: int,
    cycle: int,
    origin: str,
    pay_to: str,
    asset: str,
    amount: Decimal,
    nonce: str,
    now: datetime,
) -> X402IntentRecord:
    """Insert a new intent in state ``unknown``. A held key raises IntegrityError and the caller's
    transaction rolls back (the unique constraint is the atomic gate)."""
    now = utc(now)
    values = dict(
        job_id=str(job_id),
        attempt_number=attempt,
        cycle=cycle,
        intent_key=f"x402:{job_id}:{attempt}:{cycle}",
        state="unknown",
        origin=origin,
        pay_to=pay_to,
        asset=asset,
        amount=str(amount),
        nonce=nonce,
        payment=None,
        created_at=now,
        updated_at=now,
    )
    await conn.execute(insert(t.payment_intents).values(values))
    return X402IntentRecord(
        job_id=job_id,
        attempt=attempt,
        cycle=cycle,
        intent_key=values["intent_key"],
        state="unknown",
        origin=origin,
        pay_to=pay_to,
        asset=asset,
        amount=amount,
        nonce=nonce,
        created_at=now,
        updated_at=now,
    )


async def read_payment_intent(conn: AsyncConnection, job_id: UUID, attempt: int, cycle: int) -> X402IntentRecord | None:
    result = await conn.execute(select(t.payment_intents).where(pk(job_id, attempt, cycle)))
    row = result.mappings().first()
    return _record(row) if row is not None else None


class SqlPaymentIntents:
    """``X402IntentRepo`` for ``SqlStore`` (ISS-68): durable, atomic and restart-safe."""

    engine: AsyncEngine

    async def insert_payment_intent(
        self, job_id: UUID, attempt: int, cycle: int, origin: str, pay_to: str, asset: str, amount: Decimal,
        nonce: str, now: datetime,
    ) -> tuple[X402IntentRecord, bool]:
        """Atomically insert a new ``unknown`` intent. Returns the record and whether this call created it:
        when the key is held, the row another call stored comes back and ``created`` is ``False``."""
        try:
            async with self.engine.begin() as conn:
                record = await insert_payment_intent(conn, job_id, attempt, cycle, origin, pay_to, asset, amount, nonce, now)
            return record, True
        except IntegrityError:
            held = await self.get_payment_intent(job_id, attempt, cycle)
            if held is None:  # the holder was deleted between the insert and the read: try once more
                return await self.insert_payment_intent(job_id, attempt, cycle, origin, pay_to, asset, amount, nonce, now)
            return held, False

    async def get_payment_intent(self, job_id: UUID, attempt: int, cycle: int) -> X402IntentRecord | None:
        async with self.engine.connect() as conn:
            return await read_payment_intent(conn, job_id, attempt, cycle)

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
        """Move the intent to ``state`` and (re)write its payment value. A compare-and-set on the current
        state: a racing renewal or close writes nothing and this returns ``False``. ``None`` clears the
        payment column, so a ``refused`` intent never keeps a payment value."""
        values = {"state": state, "payment": payment, "updated_at": utc(now)}
        async with self.engine.begin() as conn:
            c = t.payment_intents.c
            moved = await conn.execute(
                update(t.payment_intents).where(and_(pk(job_id, attempt, cycle), c.state == from_state)).values(values)
            )
        return moved.rowcount == 1

    async def unknown_payment_intents(self, limit: int = 100) -> list[X402IntentRecord]:
        """The ``unknown`` intents a crash left behind (ISS-68, PROTOTYPE.md #3.1): recovery lists them so
        a restart sees the pending cost, never zeroes it."""
        async with self.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        select(t.payment_intents).where(t.payment_intents.c.state == "unknown").limit(limit)
                    )
                ).mappings().all()
            )
        return [_record(row) for row in rows]

    async def list_renewals(self, limit: int = 100) -> list[X402IntentRecord]:
        """The ``allowed`` intents whose voucher the provider renews (PROTOTYPE.md #3.3)."""
        async with self.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        select(t.payment_intents)
                    .where(and_(t.payment_intents.c.state == "allowed", t.payment_intents.c.payment.is_not(None)))
                    .limit(limit)
                    )
                ).mappings().all()
            )
        return [_record(row) for row in rows]


def _record(row: Any) -> X402IntentRecord:
    data = dict(row)
    return X402IntentRecord(
        job_id=UUID(data["job_id"]),
        attempt=data["attempt_number"],
        cycle=data["cycle"],
        intent_key=data["intent_key"],
        state=data["state"],
        origin=data["origin"],
        pay_to=data["pay_to"],
        asset=data["asset"],
        amount=Decimal(data["amount"]),
        nonce=data["nonce"],
        payment=data.get("payment"),
        created_at=data["created_at"],
        updated_at=data["updated_at"],
    )
