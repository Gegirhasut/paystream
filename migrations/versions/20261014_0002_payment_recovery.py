"""payment recovery: dead-letter mark, recovery attempts, indexes

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-14 21:00:00

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("payments", sa.Column("dead_lettered_at", sa.DateTime(timezone=True)))
    op.add_column(
        "payments",
        sa.Column("recovery_attempts", sa.Integer(), server_default="0", nullable=False),
    )
    op.create_index(
        "ix_payments_undelivered",
        "payments",
        ["created_at"],
        unique=False,
        postgresql_where=sa.text("webhook_delivered_at IS NULL"),
    )
    op.create_index("ix_outbox_aggregate_id", "outbox", ["aggregate_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_outbox_aggregate_id", table_name="outbox")
    op.drop_index(
        "ix_payments_undelivered",
        table_name="payments",
        postgresql_where=sa.text("webhook_delivered_at IS NULL"),
    )
    op.drop_column("payments", "recovery_attempts")
    op.drop_column("payments", "dead_lettered_at")
