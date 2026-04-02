"""add sync intervals to user settings

Revision ID: a9b8c7d6e5f4
Revises: f1a2b3c4d5e6
Create Date: 2026-03-24 18:05:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a9b8c7d6e5f4"
down_revision: Union[str, None] = "f1a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "user_settings",
        sa.Column("sync_products_interval_minutes", sa.Integer(), nullable=False, server_default="360"),
    )
    op.add_column(
        "user_settings",
        sa.Column("sync_supplies_interval_minutes", sa.Integer(), nullable=False, server_default="5"),
    )
    op.add_column(
        "user_settings",
        sa.Column("sync_stocks_interval_minutes", sa.Integer(), nullable=False, server_default="20"),
    )
    op.add_column(
        "user_settings",
        sa.Column("sync_reports_interval_minutes", sa.Integer(), nullable=False, server_default="180"),
    )
    op.add_column(
        "user_settings",
        sa.Column("sync_finance_interval_minutes", sa.Integer(), nullable=False, server_default="360"),
    )


def downgrade() -> None:
    op.drop_column("user_settings", "sync_finance_interval_minutes")
    op.drop_column("user_settings", "sync_reports_interval_minutes")
    op.drop_column("user_settings", "sync_stocks_interval_minutes")
    op.drop_column("user_settings", "sync_supplies_interval_minutes")
    op.drop_column("user_settings", "sync_products_interval_minutes")
