"""add supply notification events

Revision ID: b5e4a7c2d9f1
Revises: a4c6f8d2e1b7
Create Date: 2026-03-18 07:25:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "b5e4a7c2d9f1"
down_revision = "a4c6f8d2e1b7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)

    if "supply_notification_events" not in inspector.get_table_names():
        op.create_table(
            "supply_notification_events",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("supply_id", sa.Integer(), nullable=False),
            sa.Column("event_type", sa.String(), nullable=False),
            sa.Column("dedupe_key", sa.String(), nullable=False),
            sa.Column("order_number", sa.String(), nullable=False),
            sa.Column("store_id", sa.Integer(), nullable=False),
            sa.Column("store_name", sa.String(), nullable=False),
            sa.Column("user_email", sa.String(), nullable=True),
            sa.Column("status_before", sa.String(), nullable=True),
            sa.Column("status_after", sa.String(), nullable=True),
            sa.Column("timeslot_from", sa.DateTime(), nullable=True),
            sa.Column("timeslot_to", sa.DateTime(), nullable=True),
            sa.Column("old_timeslot_from", sa.DateTime(), nullable=True),
            sa.Column("old_timeslot_to", sa.DateTime(), nullable=True),
            sa.Column("telegram_sent_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_error", sa.String(), nullable=True),
            sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.ForeignKeyConstraint(["supply_id"], ["supplies.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("dedupe_key", name="uq_supply_notification_events_dedupe_key"),
        )

    refreshed_inspector = sa.inspect(connection)
    existing_indexes = {
        index["name"] for index in refreshed_inspector.get_indexes("supply_notification_events")
    }

    if "ix_supply_notification_events_supply_id" not in existing_indexes:
        op.create_index("ix_supply_notification_events_supply_id", "supply_notification_events", ["supply_id"])
    if "ix_supply_notification_events_event_type" not in existing_indexes:
        op.create_index("ix_supply_notification_events_event_type", "supply_notification_events", ["event_type"])
    if "ix_supply_notification_events_telegram_sent_at" not in existing_indexes:
        op.create_index(
            "ix_supply_notification_events_telegram_sent_at",
            "supply_notification_events",
            ["telegram_sent_at"],
        )
    if "ix_supply_notification_events_store_id" not in existing_indexes:
        op.create_index("ix_supply_notification_events_store_id", "supply_notification_events", ["store_id"])
    if "ix_supply_notification_events_created_at" not in existing_indexes:
        op.create_index("ix_supply_notification_events_created_at", "supply_notification_events", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_supply_notification_events_created_at", table_name="supply_notification_events")
    op.drop_index("ix_supply_notification_events_store_id", table_name="supply_notification_events")
    op.drop_index("ix_supply_notification_events_telegram_sent_at", table_name="supply_notification_events")
    op.drop_index("ix_supply_notification_events_event_type", table_name="supply_notification_events")
    op.drop_index("ix_supply_notification_events_supply_id", table_name="supply_notification_events")
    op.drop_table("supply_notification_events")
