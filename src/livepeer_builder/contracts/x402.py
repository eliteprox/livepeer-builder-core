"""x402 payment boundary contracts (ISS-68, PROTOTYPE.md).

The x402 (HTTP 402 Payment Required) boundary between the engine and an external signer/facilitator:
the caller owns the idempotency key, the provider owns the signer dispatch, and the media transport
attaches the returned payment header.
"""

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import Field, SecretStr, computed_field

from livepeer_builder.contracts.common import Contract
from livepeer_builder.contracts.payments import SecretRef

# The finite intent states (PROTOTYPE.md #3.2): ``unknown`` before the signer answers (it may have
# charged), then ``allowed`` (a validated payment) or ``refused`` (an explicit refusal, no money moved).
X402IntentState = Literal["unknown", "allowed", "refused"]

# A stream is metered only when the caller opens it that way. The default payment is single-shot and
# must not grow a background renewal task.
X402StreamMode = Literal["single-shot", "metered"]
X402StreamPhase = Literal["open", "closed"]
X402RenewalOutcome = Literal["none", "allowed", "refused", "unknown"]


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
    """A parsed 402 (x402 V2, ``PAYMENT-REQUIRED`` header). ``amount`` is in the asset's atomic units;
    ``origin`` is the host of the URL that challenged, the authorization is bound to it."""

    x402_version: int = 2
    origin: str
    pay_to: str
    asset: str
    amount: Decimal
    nonce: str
    response_format: str = "x402"
    exchange: str = "ethereum"
    min_fee: Decimal = Decimal(0)
    resource_url: str = ""
    scheme: str = ""
    network: str = ""
    max_timeout_seconds: int = 300
    extra: dict[str, Any] = Field(default_factory=dict)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def timeout(self) -> int:
        return self.max_timeout_seconds

    @computed_field  # type: ignore[prop-decorator]
    @property
    def digest(self) -> str:
        """A canonical SHA-256 digest over every security-relevant challenge field.

        The digest is journaled with the intent (``X402IntentRecord.challenge_digest``) so that reusing
        an allowed identity against a *changed* challenge fails loudly instead of silently replaying an
        old authorization (ISS-68 review, priority C).
        """
        canonical = {
            "amount": str(self.amount),
            "asset": self.asset,
            "exchange": self.exchange,
            "max_timeout_seconds": self.max_timeout_seconds,
            "min_fee": str(self.min_fee),
            "network": self.network,
            "nonce": self.nonce,
            "origin": self.origin,
            "pay_to": self.pay_to,
            "resource_url": self.resource_url,
            "scheme": self.scheme,
            "x402_version": self.x402_version,
        }
        payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class X402Authorization(Contract):
    """What the signer returned, or what a persisted intent remembers.

    ``payment`` is the value for the x402 V2 ``PAYMENT-SIGNATURE`` request header, held as a ``SecretStr``:
    its ``repr`` and serialized dumps redact the value; unwrap with ``get_secret_value`` only at the
    encrypted secret store and the HTTP boundary. ``None`` means the previous dispatch result is unknown
    (the signer was asked and gave no validated answer): the caller must not fail over or re-sign
    (PROTOTYPE.md #3.2).
    """

    intent_key: str
    payment: SecretStr | None
    signed_at: datetime


class X402StreamStatus(Contract):
    """One stream's renewal as the consumer can see it.

    The provider owns this view. It names the stream, whether it is still open, and the last renewal
    outcome. It never carries a payment header, credential, or secret reference.
    """

    job_id: UUID
    attempt: Annotated[int, Field(gt=0)]
    phase: X402StreamPhase
    mode: X402StreamMode
    cycle: int
    renewal: X402RenewalOutcome
    detail: str = ""


class X402IntentRecord(Contract):
    """One persisted x402 intent row (PROTOTYPE.md #3.1): the durable boundary state a restart recovers.

    The payment header itself never rides on the record: it lives in the encrypted secret store and the
    row only holds a ``payment_ref`` (ISS-68 review, priority C).
    """

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
    payment_ref: SecretRef | None = None  # encrypted Payment header value, once the state is ``allowed``
    payment_url: str | None = None  # where a renewal POSTs the payment (PROTOTYPE.md #3.1)
    challenge_digest: str = ""  # the X402Challenge.digest journaled before dispatch
    created_at: datetime
    updated_at: datetime
