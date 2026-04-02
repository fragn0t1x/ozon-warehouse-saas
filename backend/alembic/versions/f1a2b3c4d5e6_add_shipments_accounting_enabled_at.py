"""add shipments accounting enabled at

Revision ID: f1a2b3c4d5e6
Revises: e6a7b8c9d0f1
Create Date: 2026-03-24 22:25:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f1a2b3c4d5e6"
down_revision = "e6a7b8c9d0f1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "user_settings",
        sa.Column("shipments_accounting_enabled_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.execute(
        """
        UPDATE user_settings
        SET shipments_accounting_enabled_at = NOW()
        WHERE shipments_accounting_enabled = TRUE
          AND shipments_start_date IS NULL
        """
    )


def downgrade() -> None:
    op.drop_column("user_settings", "shipments_accounting_enabled_at")
