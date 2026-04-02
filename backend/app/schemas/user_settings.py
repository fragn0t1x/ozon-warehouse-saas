from pydantic import BaseModel, ConfigDict, field_validator
from datetime import datetime
from typing import Literal, Optional
from zoneinfo import ZoneInfo

from app.services.sync_intervals import SYNC_INTERVAL_MINIMUMS

class UserSettingsBase(BaseModel):
    warehouse_mode: str = "shared"
    packing_mode: str = "simple"
    shipments_start_date: Optional[datetime] = None
    shipments_accounting_enabled: bool = False
    sync_products_interval_minutes: int = 360
    sync_supplies_interval_minutes: int = 5
    sync_stocks_interval_minutes: int = 20
    sync_reports_interval_minutes: int = 180
    sync_finance_interval_minutes: int = 360
    telegram_chat_id: Optional[str] = None
    notification_timezone: str = "Europe/Moscow"
    today_supplies_time_local: str = "08:00"
    daily_report_time_local: str = "09:00"
    notify_today_supplies: bool = True
    notify_losses: bool = True
    notify_daily_report: bool = True
    notify_rejection: bool = True
    notify_acceptance_status: bool = True
    email_notifications_enabled: bool = False
    email_today_supplies: bool = True
    email_losses: bool = True
    email_daily_report: bool = True
    email_rejection: bool = True
    email_acceptance_status: bool = True
    web_push_notifications_enabled: bool = False
    discrepancy_mode: str = "loss"

    @field_validator("today_supplies_time_local", "daily_report_time_local")
    @classmethod
    def validate_time_format(cls, value: str) -> str:
        try:
            hours, minutes = value.split(":")
            hours_int = int(hours)
            minutes_int = int(minutes)
        except Exception as exc:
            raise ValueError("Время должно быть в формате HH:MM") from exc

        if not (0 <= hours_int <= 23 and 0 <= minutes_int <= 59):
            raise ValueError("Время должно быть в формате HH:MM")
        return f"{hours_int:02d}:{minutes_int:02d}"

    @field_validator("notification_timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except Exception as exc:
            raise ValueError("Неизвестный часовой пояс") from exc
        return value

    @field_validator(
        "sync_products_interval_minutes",
        "sync_supplies_interval_minutes",
        "sync_stocks_interval_minutes",
        "sync_reports_interval_minutes",
        "sync_finance_interval_minutes",
    )
    @classmethod
    def validate_sync_interval(cls, value: int, info) -> int:
        minimum = SYNC_INTERVAL_MINIMUMS[info.field_name.removeprefix("sync_").removesuffix("_interval_minutes")]  # type: ignore[index]
        if value < minimum:
            raise ValueError(f"Интервал не может быть меньше {minimum} минут")
        return value

class UserSettingsCreate(UserSettingsBase):
    pass

class UserSettingsResponse(UserSettingsBase):
    id: int
    user_id: int
    role: str = "owner"
    cabinet_owner_id: int
    can_manage_business_settings: bool = False
    shared_warehouse_id: Optional[int] = None
    shipments_accounting_enabled_at: Optional[datetime] = None
    is_first_login: bool
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class TelegramConnectStatusResponse(BaseModel):
    configured: bool = True
    status: Literal["not_configured", "not_connected", "pending", "connected"]
    bot_available: bool = False
    bot_last_seen_at: Optional[datetime] = None
    bot_status_message: Optional[str] = None
    bot_username: Optional[str] = None
    connect_url: Optional[str] = None
    qr_code_url: Optional[str] = None
    manual_code: Optional[str] = None
    manual_command: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    expires_at: Optional[datetime] = None
    connected_at: Optional[datetime] = None
    message: Optional[str] = None
