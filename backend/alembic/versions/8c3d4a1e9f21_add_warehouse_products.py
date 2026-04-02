"""add warehouse products

Revision ID: 8c3d4a1e9f21
Revises: 2f6b6b1b0e6d
Create Date: 2026-03-17 23:40:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8c3d4a1e9f21"
down_revision = "2f6b6b1b0e6d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)

    existing_tables = set(inspector.get_table_names())
    if "warehouse_products" not in existing_tables:
        op.create_table(
            "warehouse_products",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("user_id", sa.Integer(), nullable=False),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("is_archived", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
            sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
        )

    warehouse_product_indexes = {index["name"] for index in inspector.get_indexes("warehouse_products")}
    if "ix_warehouse_products_user_id" not in warehouse_product_indexes:
        op.create_index("ix_warehouse_products_user_id", "warehouse_products", ["user_id"], unique=False)
    if "ix_warehouse_products_user_name" not in warehouse_product_indexes:
        op.create_index("ix_warehouse_products_user_name", "warehouse_products", ["user_id", "name"], unique=False)

    product_columns = {column["name"] for column in inspector.get_columns("products")}
    if "warehouse_product_id" not in product_columns:
        op.add_column("products", sa.Column("warehouse_product_id", sa.Integer(), nullable=True))

    products = connection.execute(
        sa.text(
            """
            SELECT p.id AS product_id, p.warehouse_product_id AS warehouse_product_id, s.user_id AS user_id,
                   COALESCE(p.base_name, p.name) AS warehouse_product_name
            FROM products p
            JOIN stores s ON s.id = p.store_id
            ORDER BY p.id
            """
        )
    ).fetchall()

    for product_id, existing_warehouse_product_id, user_id, warehouse_product_name in products:
        if existing_warehouse_product_id:
            continue

        warehouse_product_id = connection.execute(
            sa.text(
                """
                INSERT INTO warehouse_products (user_id, name, is_archived)
                VALUES (:user_id, :name, false)
                RETURNING id
                """
            ),
            {"user_id": user_id, "name": warehouse_product_name},
        ).scalar_one()

        connection.execute(
            sa.text(
                """
                UPDATE products
                SET warehouse_product_id = :warehouse_product_id
                WHERE id = :product_id
                """
            ),
            {"warehouse_product_id": warehouse_product_id, "product_id": product_id},
        )

    inspector = sa.inspect(connection)
    product_columns = {column["name"]: column for column in inspector.get_columns("products")}
    if product_columns["warehouse_product_id"]["nullable"]:
        op.alter_column("products", "warehouse_product_id", existing_type=sa.Integer(), nullable=False)

    product_foreign_keys = {fk["name"] for fk in inspector.get_foreign_keys("products")}
    if "fk_products_warehouse_product_id" not in product_foreign_keys:
        op.create_foreign_key(
            "fk_products_warehouse_product_id",
            "products",
            "warehouse_products",
            ["warehouse_product_id"],
            ["id"],
        )

    product_indexes = {index["name"] for index in inspector.get_indexes("products")}
    if "ix_products_warehouse_product_id" not in product_indexes:
        op.create_index("ix_products_warehouse_product_id", "products", ["warehouse_product_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_products_warehouse_product_id", table_name="products")
    op.drop_constraint("fk_products_warehouse_product_id", "products", type_="foreignkey")
    op.drop_column("products", "warehouse_product_id")

    op.drop_index("ix_warehouse_products_user_name", table_name="warehouse_products")
    op.drop_index("ix_warehouse_products_user_id", table_name="warehouse_products")
    op.drop_table("warehouse_products")
