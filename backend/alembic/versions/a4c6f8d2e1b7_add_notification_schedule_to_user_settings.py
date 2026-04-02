"""add notification schedule to user settings

Revision ID: a4c6f8d2e1b7
Revises: 9d1f9df9d6f4
Create Date: 2026-03-18 04:05:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = 'a4c6f8d2e1b7'
down_revision = '9d1f9df9d6f4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('user_settings', sa.Column('notification_timezone', sa.String(), nullable=True))
    op.add_column('user_settings', sa.Column('today_supplies_time_local', sa.String(), nullable=True))
    op.add_column('user_settings', sa.Column('daily_report_time_local', sa.String(), nullable=True))

    op.execute("UPDATE user_settings SET notification_timezone = COALESCE(notification_timezone, 'Europe/Moscow')")
    op.execute("UPDATE user_settings SET today_supplies_time_local = COALESCE(today_supplies_time_local, '08:00')")
    op.execute("UPDATE user_settings SET daily_report_time_local = COALESCE(daily_report_time_local, '09:00')")

    op.alter_column('user_settings', 'notification_timezone', nullable=False)
    op.alter_column('user_settings', 'today_supplies_time_local', nullable=False)
    op.alter_column('user_settings', 'daily_report_time_local', nullable=False)


def downgrade() -> None:
    op.drop_column('user_settings', 'daily_report_time_local')
    op.drop_column('user_settings', 'today_supplies_time_local')
    op.drop_column('user_settings', 'notification_timezone')
