"""make product warehouse optional

Revision ID: c7d8e9f0a1b2
Revises: b1c2d3e4f5a6
Create Date: 2026-03-24 20:05:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c7d8e9f0a1b2"
down_revision: Union[str, None] = "b1c2d3e4f5a6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("products", "warehouse_product_id", existing_type=sa.Integer(), nullable=True)


def downgrade() -> None:
    op.alter_column("products", "warehouse_product_id", existing_type=sa.Integer(), nullable=False)
