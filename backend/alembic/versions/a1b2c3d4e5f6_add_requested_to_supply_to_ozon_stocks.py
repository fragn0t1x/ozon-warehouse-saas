"""add requested_to_supply to ozon_stocks

Revision ID: a1b2c3d4e5f6
Revises: f3b4c5d6e7f8
Create Date: 2026-04-01 16:20:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "f3b4c5d6e7f8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("ozon_stocks")}
    if "requested_to_supply" not in columns:
        op.add_column(
            "ozon_stocks",
            sa.Column("requested_to_supply", sa.Integer(), nullable=False, server_default="0"),
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("ozon_stocks")}
    if "requested_to_supply" in columns:
        op.drop_column("ozon_stocks", "requested_to_supply")
