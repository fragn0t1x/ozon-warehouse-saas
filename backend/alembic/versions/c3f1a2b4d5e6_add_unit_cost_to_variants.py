"""add unit cost to variants

Revision ID: c3f1a2b4d5e6
Revises: b5e4a7c2d9f1
Create Date: 2026-03-18 18:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = "c3f1a2b4d5e6"
down_revision = "b5e4a7c2d9f1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("variants", sa.Column("unit_cost", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("variants", "unit_cost")
