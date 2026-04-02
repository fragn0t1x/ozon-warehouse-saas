"""add web push channels

Revision ID: f3b4c5d6e7f8
Revises: e1f2a3b4c5d6
Create Date: 2026-03-25 21:10:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f3b4c5d6e7f8"
down_revision: Union[str, None] = "e1f2a3b4c5d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    user_settings_columns = {column["name"] for column in inspector.get_columns("user_settings")}
    if "web_push_notifications_enabled" not in user_settings_columns:
        op.add_column(
            "user_settings",
            sa.Column("web_push_notifications_enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
        )

    existing_tables = set(inspector.get_table_names())
    if "web_push_subscriptions" not in existing_tables:
        op.create_table(
            "web_push_subscriptions",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("user_id", sa.Integer(), nullable=False),
            sa.Column("endpoint", sa.String(), nullable=False),
            sa.Column("p256dh_key", sa.String(), nullable=False),
            sa.Column("auth_key", sa.String(), nullable=False),
            sa.Column("user_agent", sa.String(), nullable=True),
            sa.Column("last_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.Column("last_sent_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
            sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("endpoint", name="uq_web_push_subscription_endpoint"),
        )
        op.create_index(
            op.f("ix_web_push_subscriptions_user_id"),
            "web_push_subscriptions",
            ["user_id"],
            unique=False,
        )


def downgrade() -> None:
    op.drop_index(op.f("ix_web_push_subscriptions_user_id"), table_name="web_push_subscriptions")
    op.drop_table("web_push_subscriptions")
    op.drop_column("user_settings", "web_push_notifications_enabled")
