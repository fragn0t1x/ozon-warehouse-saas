"""add store economics settings

Revision ID: a7c8d9e0f1a2
Revises: f3b4c5d6e7f8
Create Date: 2026-03-26 16:20:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a7c8d9e0f1a2"
down_revision: Union[str, None] = "f3b4c5d6e7f8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    store_columns = {column["name"] for column in inspector.get_columns("stores")}

    if "economics_vat_mode" not in store_columns:
        op.add_column(
            "stores",
            sa.Column("economics_vat_mode", sa.String(), nullable=False, server_default="none"),
        )

    if "economics_tax_mode" not in store_columns:
        op.add_column(
            "stores",
            sa.Column("economics_tax_mode", sa.String(), nullable=False, server_default="usn_income_expenses"),
        )

    if "economics_tax_rate" not in store_columns:
        op.add_column(
            "stores",
            sa.Column("economics_tax_rate", sa.Float(), nullable=False, server_default="15"),
        )

    if "economics_default_sale_price_gross" not in store_columns:
        op.add_column(
            "stores",
            sa.Column("economics_default_sale_price_gross", sa.Float(), nullable=True),
        )


def downgrade() -> None:
    op.drop_column("stores", "economics_default_sale_price_gross")
    op.drop_column("stores", "economics_tax_rate")
    op.drop_column("stores", "economics_tax_mode")
    op.drop_column("stores", "economics_vat_mode")
