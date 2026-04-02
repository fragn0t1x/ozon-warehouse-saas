"""add economics history

Revision ID: d4e5f6a7b8c9
Revises: c1d2e3f4a5b6
Create Date: 2026-03-30 10:40:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d4e5f6a7b8c9"
down_revision = "c1d2e3f4a5b6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS store_economics_history (
            id SERIAL PRIMARY KEY,
            store_id INTEGER NOT NULL REFERENCES stores(id) ON DELETE CASCADE,
            effective_from DATE NOT NULL,
            vat_mode VARCHAR NOT NULL,
            tax_mode VARCHAR NOT NULL,
            tax_rate DOUBLE PRECISION NOT NULL,
            created_by_user_id INTEGER REFERENCES users(id),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_store_economics_history_store_id ON store_economics_history(store_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_store_economics_history_effective_from ON store_economics_history(effective_from)")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS variant_cost_history (
            id SERIAL PRIMARY KEY,
            variant_id INTEGER NOT NULL REFERENCES variants(id) ON DELETE CASCADE,
            store_id INTEGER NOT NULL REFERENCES stores(id) ON DELETE CASCADE,
            warehouse_product_id INTEGER REFERENCES warehouse_products(id) ON DELETE SET NULL,
            offer_id VARCHAR NOT NULL,
            pack_size INTEGER NOT NULL DEFAULT 1,
            color VARCHAR NULL,
            size VARCHAR NULL,
            unit_cost DOUBLE PRECISION NULL,
            effective_from DATE NOT NULL,
            created_by_user_id INTEGER REFERENCES users(id),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_variant_cost_history_variant_id ON variant_cost_history(variant_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_variant_cost_history_store_id ON variant_cost_history(store_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_variant_cost_history_warehouse_product_id ON variant_cost_history(warehouse_product_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_variant_cost_history_offer_id ON variant_cost_history(offer_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_variant_cost_history_effective_from ON variant_cost_history(effective_from)")

    op.execute("ALTER TABLE store_month_finance ADD COLUMN IF NOT EXISTS is_locked BOOLEAN NOT NULL DEFAULT false")
    op.execute("ALTER TABLE store_month_finance ADD COLUMN IF NOT EXISTS vat_mode_used VARCHAR NULL")
    op.execute("ALTER TABLE store_month_finance ADD COLUMN IF NOT EXISTS tax_mode_used VARCHAR NULL")
    op.execute("ALTER TABLE store_month_finance ADD COLUMN IF NOT EXISTS tax_rate_used DOUBLE PRECISION NULL")
    op.execute("ALTER TABLE store_month_finance ADD COLUMN IF NOT EXISTS tax_effective_from_used DATE NULL")
    op.execute("ALTER TABLE store_month_finance ADD COLUMN IF NOT EXISTS cost_basis VARCHAR NULL")
    op.execute("ALTER TABLE store_month_finance ADD COLUMN IF NOT EXISTS cost_snapshot_date DATE NULL")

    op.execute("ALTER TABLE store_month_offer_finance ADD COLUMN IF NOT EXISTS vat_mode_used VARCHAR NULL")
    op.execute("ALTER TABLE store_month_offer_finance ADD COLUMN IF NOT EXISTS tax_mode_used VARCHAR NULL")
    op.execute("ALTER TABLE store_month_offer_finance ADD COLUMN IF NOT EXISTS tax_rate_used DOUBLE PRECISION NULL")
    op.execute("ALTER TABLE store_month_offer_finance ADD COLUMN IF NOT EXISTS tax_effective_from_used DATE NULL")
    op.execute("ALTER TABLE store_month_offer_finance ADD COLUMN IF NOT EXISTS cost_effective_from_used DATE NULL")

    op.execute(
        """
        INSERT INTO store_economics_history (store_id, effective_from, vat_mode, tax_mode, tax_rate, created_by_user_id)
        SELECT
            s.id,
            COALESCE(DATE(s.created_at), CURRENT_DATE),
            COALESCE(s.economics_vat_mode, 'none'),
            COALESCE(s.economics_tax_mode, 'usn_income_expenses'),
            COALESCE(s.economics_tax_rate, 15),
            s.user_id
        FROM stores s
        WHERE NOT EXISTS (
            SELECT 1
            FROM store_economics_history h
            WHERE h.store_id = s.id
        )
        """
    )

    op.execute(
        """
        INSERT INTO variant_cost_history (
            variant_id,
            store_id,
            warehouse_product_id,
            offer_id,
            pack_size,
            color,
            size,
            unit_cost,
            effective_from,
            created_by_user_id
        )
        SELECT
            v.id,
            p.store_id,
            p.warehouse_product_id,
            COALESCE(v.offer_id, ''),
            COALESCE(v.pack_size, 1),
            LOWER(TRIM(COALESCE(MAX(CASE WHEN va.name = 'Цвет' THEN va.value END), ''))),
            LOWER(TRIM(COALESCE(MAX(CASE WHEN va.name = 'Размер' THEN va.value END), ''))),
            v.unit_cost,
            COALESCE(DATE(v.created_at), CURRENT_DATE),
            s.user_id
        FROM variants v
        JOIN products p ON p.id = v.product_id
        JOIN stores s ON s.id = p.store_id
        LEFT JOIN variant_attributes va ON va.variant_id = v.id
        WHERE NOT EXISTS (
            SELECT 1
            FROM variant_cost_history h
            WHERE h.variant_id = v.id
        )
        GROUP BY v.id, p.store_id, p.warehouse_product_id, v.offer_id, v.pack_size, v.unit_cost, v.created_at, s.user_id
        """
    )


def downgrade() -> None:
    op.drop_column("store_month_offer_finance", "cost_effective_from_used")
    op.drop_column("store_month_offer_finance", "tax_effective_from_used")
    op.drop_column("store_month_offer_finance", "tax_rate_used")
    op.drop_column("store_month_offer_finance", "tax_mode_used")
    op.drop_column("store_month_offer_finance", "vat_mode_used")

    op.drop_column("store_month_finance", "cost_snapshot_date")
    op.drop_column("store_month_finance", "cost_basis")
    op.drop_column("store_month_finance", "tax_effective_from_used")
    op.drop_column("store_month_finance", "tax_rate_used")
    op.drop_column("store_month_finance", "tax_mode_used")
    op.drop_column("store_month_finance", "vat_mode_used")
    op.drop_column("store_month_finance", "is_locked")

    op.drop_index(op.f("ix_variant_cost_history_effective_from"), table_name="variant_cost_history")
    op.drop_index(op.f("ix_variant_cost_history_offer_id"), table_name="variant_cost_history")
    op.drop_index(op.f("ix_variant_cost_history_warehouse_product_id"), table_name="variant_cost_history")
    op.drop_index(op.f("ix_variant_cost_history_store_id"), table_name="variant_cost_history")
    op.drop_index(op.f("ix_variant_cost_history_variant_id"), table_name="variant_cost_history")
    op.drop_table("variant_cost_history")

    op.drop_index(op.f("ix_store_economics_history_effective_from"), table_name="store_economics_history")
    op.drop_index(op.f("ix_store_economics_history_store_id"), table_name="store_economics_history")
    op.drop_table("store_economics_history")
