from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user_settings import UserSettings


USER_SETTINGS_INSERT_DEFAULTS = {
    "warehouse_mode": "shared",
    "packing_mode": "simple",
    "shipments_accounting_enabled": False,
    "sync_products_interval_minutes": 360,
    "sync_supplies_interval_minutes": 5,
    "sync_stocks_interval_minutes": 20,
    "sync_reports_interval_minutes": 180,
    "sync_finance_interval_minutes": 360,
    "notification_timezone": "Europe/Moscow",
    "today_supplies_time_local": "08:00",
    "daily_report_time_local": "09:00",
    "notify_today_supplies": True,
    "notify_losses": True,
    "notify_daily_report": True,
    "notify_rejection": True,
    "notify_acceptance_status": True,
    "email_notifications_enabled": False,
    "email_today_supplies": True,
    "email_losses": True,
    "email_daily_report": True,
    "email_rejection": True,
    "email_acceptance_status": True,
    "web_push_notifications_enabled": False,
    "discrepancy_mode": "loss",
    "is_first_login": True,
}


def ensure_user_settings_defaults(settings: UserSettings) -> bool:
    changed = False

    for field_name, default_value in USER_SETTINGS_INSERT_DEFAULTS.items():
        current_value = getattr(settings, field_name)
        if isinstance(default_value, str):
            needs_update = not current_value
        else:
            needs_update = current_value is None
        if needs_update:
            setattr(settings, field_name, default_value)
            changed = True

    return changed


async def get_or_create_user_settings(db: AsyncSession, user_id: int) -> tuple[UserSettings, bool]:
    settings = (
        await db.execute(select(UserSettings).where(UserSettings.user_id == user_id))
    ).scalar_one_or_none()

    changed = False
    if settings is None:
        settings = UserSettings(user_id=user_id, **USER_SETTINGS_INSERT_DEFAULTS)
        db.add(settings)
        changed = True

    if ensure_user_settings_defaults(settings):
        changed = True

    if changed:
        await db.flush()

    return settings, changed
