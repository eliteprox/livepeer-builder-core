"""x402 payment boundary contracts (ISS-68, PROTOTYPE.md).

The x402 (HTTP 402 Payment Required) boundary between the engine and an external signer/facilitator:
the caller owns the idempotency key, the provider owns the signer dispatch, and the media transport
attaches the returned payment header.
"""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field

from livepeer_builder.contracts.common import Contract

# The finite intent states (PROTOTYPE.md #3.2): ``unknown`` before the signer answers (it may have
# charged), then ``allowed`` (a validated payment) or ``refused`` (an explicit refusal, no money moved).
X402IntentState = Literal["unknown", "allowed", "refused"]


class X402IntentIdentity(Contract):
    """The caller-owned, deterministic idempotency key material: which job, which attempt, which renewal
    cycle (PROTOTYPE.md #3.1)."""

    job_id: UUID
    attempt: Annotated[int, Field(gt=0)]
    cycle: Annotated[int, Field(ge=0)]

    @property
    def intent_key(self) -> str:
        return f"x402:{self.job_id}:{self.attempt}:{self.cycle}"


class X402Challenge(Contract):
    """A parsed 402 Payment Required response (the standard x402 wire format, distinct from the legacy
    Livepeer-Payment format). ``amount`` and ``min_fee`` are in the asset's atomic units; ``origin`` is
    the host of the URL that challenged, the authorization is bound to it."""

    origin: str
    pay_to: str
    asset: str
    amount: Decimal
    nonce: str
    response_format: str = "x402"
    exchange: str = "ethereum"
    min_fee: Decimal = Decimal(0)


class X402Authorization(Contract):
    """What the signer returned, or what a persisted intent remembers.

    ``payment`` is the value for the standard x402 ``Payment`` request header. ``None`` means the
    previous dispatch result is unknown (the signer was asked and gave no validated answer): the caller
    must not fail over or re-sign (PROTOTYPE.md #3.2).
    """

    intent_key: str
    payment: str | None
    signed_at: datetime


class X402IntentRecord(Contract):
    """One persisted x402 intent row (PROTOTYPE.md #3.1): the durable boundary state a restart recovers."""

    job_id: UUID
    attempt: int
    cycle: int
    intent_key: str
    state: X402IntentState
    origin: str
    pay_to: str
    asset: str
    amount: Decimal
    nonce: str
    payment: str | None = None  # the Payment header value once the state is ``allowed``
    created_at: datetime
    updated_at: datetime
