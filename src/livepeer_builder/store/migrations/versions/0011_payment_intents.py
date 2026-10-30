"""The x402 payment intent ledger (ISS-68): one durable intent per (job, attempt, renewal cycle).

Revision ID: 0011
Revises: 0010
Create Date: 2026-10-30

A caller-owned idempotency key (job_id, attempt_number, cycle) is stored under a unique ``intent_key``,
so two concurrent authorizes for one intent cannot both win. The state moves ``unknown`` -> ``allowed``
(a validated signer answer) or ``refused`` (an explicit refusal, no money moved); an answer that was lost
or a 5xx leaves the intent ``unknown`` (it may have charged). The ``payment`` column holds the standard
x402 Payment header value once the state is ``allowed``, so an idempotent retry never re-signs.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from livepeer_builder.store.schema import UTCDateTime

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "lpb_payment_intents",
        sa.Column(
            "job_id", sa.String(36), sa.ForeignKey("lpb_jobs.id", ondelete="CASCADE"), primary_key=True
        ),
        sa.Column("attempt_number", sa.Integer, primary_key=True),
        sa.Column("cycle", sa.Integer, primary_key=True),
        sa.Column("intent_key", sa.String(200), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("origin", sa.Text, nullable=False),
        sa.Column("pay_to", sa.String(128), nullable=False),
        sa.Column("asset", sa.String(128), nullable=False),
        sa.Column("amount", sa.String(64), nullable=False),
        sa.Column("nonce", sa.String(128), nullable=False),
        sa.Column("payment", sa.Text, nullable=True),
        sa.Column("created_at", UTCDateTime(), nullable=False),
        sa.Column("updated_at", UTCDateTime(), nullable=False),
    )
    op.create_index("lpb_payment_intents_key", "lpb_payment_intents", ["intent_key"], unique=True)
    op.create_index("lpb_payment_intents_state", "lpb_payment_intents", ["state"])


def downgrade() -> None:
    op.drop_index("lpb_payment_intents_key", "lpb_payment_intents")
    op.drop_index("lpb_payment_intents_state", "lpb_payment_intents")
    op.drop_table("lpb_payment_intents")
