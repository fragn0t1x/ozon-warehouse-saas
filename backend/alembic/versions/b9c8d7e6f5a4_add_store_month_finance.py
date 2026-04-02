"""add store month finance tables

Revision ID: b9c8d7e6f5a4
Revises: a7c8d9e0f1a2
Create Date: 2026-03-29 19:20:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b9c8d7e6f5a4"
down_revision: Union[str, None] = "a7c8d9e0f1a2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())

    if "store_month_finance" not in tables:
        op.create_table(
            "store_month_finance",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("store_id", sa.Integer(), nullable=False),
            sa.Column("month", sa.String(length=7), nullable=False),
            sa.Column("status", sa.String(), nullable=False, server_default="pending"),
            sa.Column("is_final", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("realization_available", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("coverage_ratio", sa.Float(), nullable=False, server_default="0"),
            sa.Column("sold_units", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("sold_amount", sa.Float(), nullable=False, server_default="0"),
            sa.Column("returned_units", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("returned_amount", sa.Float(), nullable=False, server_default="0"),
            sa.Column("revenue_amount", sa.Float(), nullable=False, server_default="0"),
            sa.Column("revenue_net_of_vat", sa.Float(), nullable=False, server_default="0"),
            sa.Column("cogs", sa.Float(), nullable=False, server_default="0"),
            sa.Column("gross_profit", sa.Float(), nullable=False, server_default="0"),
            sa.Column("ozon_commission", sa.Float(), nullable=False, server_default="0"),
            sa.Column("ozon_logistics", sa.Float(), nullable=False, server_default="0"),
            sa.Column("ozon_services", sa.Float(), nullable=False, server_default="0"),
            sa.Column("ozon_incentives", sa.Float(), nullable=False, server_default="0"),
            sa.Column("ozon_compensation", sa.Float(), nullable=False, server_default="0"),
            sa.Column("ozon_decompensation", sa.Float(), nullable=False, server_default="0"),
            sa.Column("ozon_adjustments_net", sa.Float(), nullable=False, server_default="0"),
            sa.Column("profit_before_tax", sa.Float(), nullable=False, server_default="0"),
            sa.Column("tax_amount", sa.Float(), nullable=False, server_default="0"),
            sa.Column("net_profit", sa.Float(), nullable=False, server_default="0"),
            sa.Column("generated_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("source_payload", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
            sa.ForeignKeyConstraint(["store_id"], ["stores.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("store_id", "month", name="uq_store_month_finance_store_month"),
        )
        op.create_index("ix_store_month_finance_store_id", "store_month_finance", ["store_id"])
        op.create_index("ix_store_month_finance_month", "store_month_finance", ["month"])

    if "store_month_offer_finance" not in tables:
        op.create_table(
            "store_month_offer_finance",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("store_month_finance_id", sa.Integer(), nullable=False),
            sa.Column("store_id", sa.Integer(), nullable=False),
            sa.Column("month", sa.String(length=7), nullable=False),
            sa.Column("offer_id", sa.String(), nullable=False),
            sa.Column("title", sa.String(), nullable=True),
            sa.Column("basis", sa.String(), nullable=False, server_default="realization_closed_month"),
            sa.Column("sold_units", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("sold_amount", sa.Float(), nullable=False, server_default="0"),
            sa.Column("returned_units", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("returned_amount", sa.Float(), nullable=False, server_default="0"),
            sa.Column("net_units", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("revenue_amount", sa.Float(), nullable=False, server_default="0"),
            sa.Column("revenue_net_of_vat", sa.Float(), nullable=False, server_default="0"),
            sa.Column("ozon_commission", sa.Float(), nullable=False, server_default="0"),
            sa.Column("ozon_logistics", sa.Float(), nullable=False, server_default="0"),
            sa.Column("ozon_services", sa.Float(), nullable=False, server_default="0"),
            sa.Column("ozon_incentives", sa.Float(), nullable=False, server_default="0"),
            sa.Column("ozon_adjustments_net", sa.Float(), nullable=False, server_default="0"),
            sa.Column("unit_cost", sa.Float(), nullable=True),
            sa.Column("cogs", sa.Float(), nullable=True),
            sa.Column("gross_profit", sa.Float(), nullable=True),
            sa.Column("profit_before_tax", sa.Float(), nullable=True),
            sa.Column("tax_amount", sa.Float(), nullable=True),
            sa.Column("net_profit", sa.Float(), nullable=True),
            sa.Column("margin_ratio", sa.Float(), nullable=True),
            sa.Column("has_cost", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
            sa.ForeignKeyConstraint(["store_id"], ["stores.id"], ondelete="CASCADE"),
            sa.ForeignKeyConstraint(["store_month_finance_id"], ["store_month_finance.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "store_month_finance_id",
                "offer_id",
                name="uq_store_month_offer_finance_month_offer",
            ),
        )
        op.create_index("ix_store_month_offer_finance_store_month_finance_id", "store_month_offer_finance", ["store_month_finance_id"])
        op.create_index("ix_store_month_offer_finance_store_id", "store_month_offer_finance", ["store_id"])
        op.create_index("ix_store_month_offer_finance_month", "store_month_offer_finance", ["month"])
        op.create_index("ix_store_month_offer_finance_offer_id", "store_month_offer_finance", ["offer_id"])


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())

    if "store_month_offer_finance" in tables:
        op.drop_index("ix_store_month_offer_finance_offer_id", table_name="store_month_offer_finance")
        op.drop_index("ix_store_month_offer_finance_month", table_name="store_month_offer_finance")
        op.drop_index("ix_store_month_offer_finance_store_id", table_name="store_month_offer_finance")
        op.drop_index("ix_store_month_offer_finance_store_month_finance_id", table_name="store_month_offer_finance")
        op.drop_table("store_month_offer_finance")

    if "store_month_finance" in tables:
        op.drop_index("ix_store_month_finance_month", table_name="store_month_finance")
        op.drop_index("ix_store_month_finance_store_id", table_name="store_month_finance")
        op.drop_table("store_month_finance")
