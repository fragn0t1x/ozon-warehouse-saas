"""add shipments accounting toggle

Revision ID: e6a7b8c9d0f1
Revises: d4b5e6f7a8b9
Create Date: 2026-03-24 22:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e6a7b8c9d0f1"
down_revision = "d4b5e6f7a8b9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "user_settings",
        sa.Column("shipments_accounting_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
    )

    op.execute(
        """
        UPDATE user_settings
        SET shipments_accounting_enabled = CASE
            WHEN is_first_login = TRUE THEN FALSE
            ELSE TRUE
        END
        """
    )

    op.alter_column("user_settings", "shipments_accounting_enabled", server_default=sa.false())


def downgrade() -> None:
    op.drop_column("user_settings", "shipments_accounting_enabled")
