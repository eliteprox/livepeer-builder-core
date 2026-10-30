"""The x402 payment-plane protocols (ISS-68, PROTOTYPE.md #2/#3): the signer and the durable intent repo.

Split out of ``protocols/__init__.py`` to keep that file under the line limit.
"""

from datetime import datetime
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from livepeer_builder.contracts import (
    X402Authorization,
    X402Challenge,
    X402IntentIdentity,
    X402IntentRecord,
)


class X402Signer(Protocol):
    """The external signer/facilitator that answers x402 challenges (POST /generate-live-payment in the
    local prototype). The engine asks it with the caller-owned intent key; its validated answer becomes the
    ``Payment`` header value the media transport sends."""

    async def sign(
        self,
        intent: X402IntentIdentity,
        challenge: X402Challenge,
        credential: str | None,
    ) -> X402Authorization:
        """Return the signed payment value for ``challenge`` under ``intent``. A refused sign raises
        ``X402Refused``; a lost or malformed answer raises ``X402SignerUnknown`` (the intent stays
        ``unknown``). A success returns ``X402Authorization(payment=<Payment header value>)``."""
        ...


class X402IntentRepo(Protocol):
    """Durable x402 intent records: the state a restart recovers (PROTOTYPE.md #3.1)."""

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
        """Atomically insert an ``unknown`` intent. Returns the record and whether this call created it."""
        ...

    async def get_payment_intent(self, job_id: UUID, attempt: int, cycle: int) -> X402IntentRecord | None: ...

    async def set_payment_intent_state(
        self,
        job_id: UUID,
        attempt: int,
        cycle: int,
        state: str,
        payment: str | None,
        now: datetime,
        from_state: str = "unknown",
    ) -> bool: ...

    async def unknown_payment_intents(self, limit: int = 100) -> list[X402IntentRecord]: ...

    async def list_renewals(self, limit: int = 100) -> list[X402IntentRecord]:
        """The ``allowed`` intents due for a metered renewal (the voucher of an open stream,
        PROTOTYPE.md #3.3)."""
        ...
