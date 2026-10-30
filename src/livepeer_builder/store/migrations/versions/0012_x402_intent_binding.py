"""Bind x402 intents to their challenge and to the encrypted payment secret (ISS-68 review, priority C).

Revision ID: 0012
Revises: 0011
Create Date: 2026-10-30

``challenge_digest`` is the canonical SHA-256 digest of the 402 challenge journaled before dispatch:
reusing a held key against a *changed* challenge must fail loudly instead of replaying an old
authorization. The plaintext ``payment`` column is dropped: the Payment header value lives in the
encrypted secret store and the row keeps only a ``payment_ref`` (SecretRef).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "lpb_payment_intents",
        sa.Column("challenge_digest", sa.String(64), nullable=False, server_default="#"),
    )
    op.add_column("lpb_payment_intents", sa.Column("payment_ref", sa.String(64), nullable=True))
    op.add_column("lpb_payment_intents", sa.Column("payment_url", sa.String(64), nullable=True))
    op.drop_column("lpb_payment_intents", "payment")


def downgrade() -> None:
    op.add_column("lpb_payment_intents", sa.Column("payment", sa.Text, nullable=True))
    op.drop_column("lpb_payment_intents", "payment_ref")
    op.drop_column("lpb_payment_intents", "payment_url")
    op.drop_column("lpb_payment_intents", "challenge_digest")
