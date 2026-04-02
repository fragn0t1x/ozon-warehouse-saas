"""add owner role to users

Revision ID: d4b5e6f7a8b9
Revises: c3f1a2b4d5e6
Create Date: 2026-03-23 20:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d4b5e6f7a8b9"
down_revision = "c3f1a2b4d5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("role", sa.String(), nullable=False, server_default="owner"))
    op.add_column("users", sa.Column("owner_user_id", sa.Integer(), nullable=True))
    op.create_index(op.f("ix_users_owner_user_id"), "users", ["owner_user_id"], unique=False)
    op.create_foreign_key(
        "fk_users_owner_user_id_users",
        "users",
        "users",
        ["owner_user_id"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint("fk_users_owner_user_id_users", "users", type_="foreignkey")
    op.drop_index(op.f("ix_users_owner_user_id"), table_name="users")
    op.drop_column("users", "owner_user_id")
    op.drop_column("users", "role")
