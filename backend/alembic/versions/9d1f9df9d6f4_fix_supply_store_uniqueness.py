"""fix supply uniqueness per store

Revision ID: 9d1f9df9d6f4
Revises: 8c3d4a1e9f21
Create Date: 2026-03-17 16:30:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9d1f9df9d6f4"
down_revision = "8c3d4a1e9f21"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)

    unique_constraints = inspector.get_unique_constraints("supplies")
    for constraint in unique_constraints:
        columns = tuple(constraint.get("column_names") or [])
        if columns == ("ozon_order_id",):
            op.drop_constraint(constraint["name"], "supplies", type_="unique")

    refreshed_inspector = sa.inspect(connection)
    refreshed_constraints = {
        constraint["name"]: tuple(constraint.get("column_names") or [])
        for constraint in refreshed_inspector.get_unique_constraints("supplies")
    }
    if "uq_supplies_store_ozon_order_id" not in refreshed_constraints:
        op.create_unique_constraint(
            "uq_supplies_store_ozon_order_id",
            "supplies",
            ["store_id", "ozon_order_id"],
        )


def downgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    unique_constraints = {
        constraint["name"]: tuple(constraint.get("column_names") or [])
        for constraint in inspector.get_unique_constraints("supplies")
    }

    if "uq_supplies_store_ozon_order_id" in unique_constraints:
        op.drop_constraint("uq_supplies_store_ozon_order_id", "supplies", type_="unique")

    if not any(columns == ("ozon_order_id",) for columns in unique_constraints.values()):
        op.create_unique_constraint("uq_supplies_ozon_order_id", "supplies", ["ozon_order_id"])
