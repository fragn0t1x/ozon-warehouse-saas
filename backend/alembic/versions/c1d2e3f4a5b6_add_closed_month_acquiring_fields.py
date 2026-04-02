"""add acquiring and other expense fields to closed month finance

Revision ID: c1d2e3f4a5b6
Revises: b9c8d7e6f5a4
Create Date: 2026-03-29 20:05:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c1d2e3f4a5b6"
down_revision: Union[str, None] = "b9c8d7e6f5a4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    return any(column.get("name") == column_name for column in inspector.get_columns(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())

    if "store_month_finance" in tables:
        if not _has_column(inspector, "store_month_finance", "ozon_acquiring"):
            op.add_column(
                "store_month_finance",
                sa.Column("ozon_acquiring", sa.Float(), nullable=False, server_default="0"),
            )
        if not _has_column(inspector, "store_month_finance", "ozon_other_expenses"):
            op.add_column(
                "store_month_finance",
                sa.Column("ozon_other_expenses", sa.Float(), nullable=False, server_default="0"),
            )

    if "store_month_offer_finance" in tables:
        if not _has_column(inspector, "store_month_offer_finance", "ozon_acquiring"):
            op.add_column(
                "store_month_offer_finance",
                sa.Column("ozon_acquiring", sa.Float(), nullable=False, server_default="0"),
            )
        if not _has_column(inspector, "store_month_offer_finance", "ozon_other_expenses"):
            op.add_column(
                "store_month_offer_finance",
                sa.Column("ozon_other_expenses", sa.Float(), nullable=False, server_default="0"),
            )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())

    if "store_month_offer_finance" in tables:
        if _has_column(inspector, "store_month_offer_finance", "ozon_other_expenses"):
            op.drop_column("store_month_offer_finance", "ozon_other_expenses")
        if _has_column(inspector, "store_month_offer_finance", "ozon_acquiring"):
            op.drop_column("store_month_offer_finance", "ozon_acquiring")

    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())

    if "store_month_finance" in tables:
        if _has_column(inspector, "store_month_finance", "ozon_other_expenses"):
            op.drop_column("store_month_finance", "ozon_other_expenses")
        if _has_column(inspector, "store_month_finance", "ozon_acquiring"):
            op.drop_column("store_month_finance", "ozon_acquiring")
