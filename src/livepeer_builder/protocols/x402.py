"""The x402 payment-plane protocols (ISS-68, PROTOTYPE.md #2/#3): the signer, the payment authorizer and
the durable intent repo.

Split out of ``protocols/__init__.py`` to keep that file under the line limit.
"""

from datetime import datetime
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from livepeer_builder.contracts import (
    SecretRef,
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


class X402PaymentAuthorizer(Protocol):
    """The x402 payment plane (PROTOTYPE.md #2): owns the signer dispatch and the durable intent state.

    The media transport calls ``authorize`` on a 402 and attaches the returned header; it never calls the
    backend signer itself (ISS-68 review, priority A). The durable pre-dispatch journal lives inside this
    boundary, so a crash mid-flight never hides a charged payment (D54).
    """

    async def authorize(
        self,
        intent: X402IntentIdentity,
        challenge: X402Challenge,
        credential: str | None,
    ) -> X402Authorization:
        """Journal the intent, then dispatch the signer; for a held key it replays the persisted payment
        instead of re-signing. A ``refused`` intent raises ``X402Refused``; a lost or mismatched signer
        answer raises ``X402SignerUnknown`` and leaves the intent ``unknown`` (no failover, no re-sign)."""
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
        challenge_digest: str = "",
        payment_url: str | None = None,
    ) -> tuple[X402IntentRecord, bool]:
        """Atomically insert an ``unknown`` intent (and journal the challenge digest). Returns the record
        and whether this call created it; a duplicate key returns the held row."""
        ...

    async def get_payment_intent(self, job_id: UUID, attempt: int, cycle: int) -> X402IntentRecord | None: ...

    async def intents_for_job(self, job_id: UUID) -> list[X402IntentRecord]:
        """Every durable intent for one job, so cost evidence can keep an unresolved intent pending
        (ISS-68 review, priority B)."""
        ...

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
        """Compare-and-set the state (a racing renewal or close writes nothing). The Payment header value
        lives in the encrypted secret store; the row only carries its ``SecretRef``."""
        ...

    async def unknown_payment_intents(self, limit: int = 100) -> list[X402IntentRecord]: ...

    async def list_renewals(self, limit: int = 100) -> list[X402IntentRecord]:
        """The ``allowed`` intents due for a metered renewal (the voucher of an open stream,
        PROTOTYPE.md #3.3)."""
        ...
