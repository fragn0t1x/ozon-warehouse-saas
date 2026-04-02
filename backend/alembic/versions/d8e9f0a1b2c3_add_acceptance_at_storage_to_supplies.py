"""add acceptance at storage timestamp to supplies

Revision ID: d8e9f0a1b2c3
Revises: c7d8e9f0a1b2
Create Date: 2026-03-25 01:15:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d8e9f0a1b2c3"
down_revision: Union[str, None] = "c7d8e9f0a1b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("supplies", sa.Column("acceptance_at_storage_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("supplies", "acceptance_at_storage_at")
