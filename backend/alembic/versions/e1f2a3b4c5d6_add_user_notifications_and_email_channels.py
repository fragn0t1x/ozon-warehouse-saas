"""add user notifications and email channels

Revision ID: e1f2a3b4c5d6
Revises: d8e9f0a1b2c3
Create Date: 2026-03-25 01:05:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "e1f2a3b4c5d6"
down_revision = "d8e9f0a1b2c3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if "user_notifications" not in inspector.get_table_names():
        op.create_table(
            "user_notifications",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("user_id", sa.Integer(), nullable=False),
            sa.Column("kind", sa.String(), nullable=False),
            sa.Column("title", sa.String(), nullable=False),
            sa.Column("body", sa.Text(), nullable=False),
            sa.Column("action_url", sa.String(), nullable=True),
            sa.Column("severity", sa.String(), nullable=False, server_default="info"),
            sa.Column("is_important", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
        )

    existing_indexes = {index["name"] for index in inspector.get_indexes("user_notifications")}
    for index_name, columns in (
        (op.f("ix_user_notifications_user_id"), ["user_id"]),
        (op.f("ix_user_notifications_kind"), ["kind"]),
        (op.f("ix_user_notifications_read_at"), ["read_at"]),
        (op.f("ix_user_notifications_created_at"), ["created_at"]),
    ):
        if index_name not in existing_indexes:
            op.create_index(index_name, "user_notifications", columns, unique=False)

    existing_columns = {column["name"] for column in inspector.get_columns("user_settings")}
    for column_name, server_default in (
        ("email_notifications_enabled", sa.text("false")),
        ("email_today_supplies", sa.text("true")),
        ("email_losses", sa.text("true")),
        ("email_daily_report", sa.text("true")),
        ("email_rejection", sa.text("true")),
        ("email_acceptance_status", sa.text("true")),
    ):
        if column_name not in existing_columns:
            op.add_column(
                "user_settings",
                sa.Column(column_name, sa.Boolean(), nullable=False, server_default=server_default),
            )


def downgrade() -> None:
    op.drop_column("user_settings", "email_acceptance_status")
    op.drop_column("user_settings", "email_rejection")
    op.drop_column("user_settings", "email_daily_report")
    op.drop_column("user_settings", "email_losses")
    op.drop_column("user_settings", "email_today_supplies")
    op.drop_column("user_settings", "email_notifications_enabled")

    op.drop_index(op.f("ix_user_notifications_created_at"), table_name="user_notifications")
    op.drop_index(op.f("ix_user_notifications_read_at"), table_name="user_notifications")
    op.drop_index(op.f("ix_user_notifications_kind"), table_name="user_notifications")
    op.drop_index(op.f("ix_user_notifications_user_id"), table_name="user_notifications")
    op.drop_table("user_notifications")
