from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_NOTIFICATION_TIMEZONE = "Europe/Moscow"
DEFAULT_TODAY_SUPPLIES_TIME = "08:00"
DEFAULT_DAILY_REPORT_TIME = "09:00"


@dataclass(frozen=True)
class NotificationSchedule:
    timezone_name: str
    today_supplies_time_local: str
    daily_report_time_local: str


def safe_timezone_name(value: str | None) -> str:
    candidate = (value or DEFAULT_NOTIFICATION_TIMEZONE).strip()
    try:
        ZoneInfo(candidate)
    except ZoneInfoNotFoundError:
        return DEFAULT_NOTIFICATION_TIMEZONE
    return candidate


def safe_time_value(value: str | None, *, fallback: str) -> str:
    candidate = (value or fallback).strip()
    try:
        hours_text, minutes_text = candidate.split(":")
        hours = int(hours_text)
        minutes = int(minutes_text)
    except Exception:
        return fallback

    if not (0 <= hours <= 23 and 0 <= minutes <= 59):
        return fallback

    return f"{hours:02d}:{minutes:02d}"


def build_notification_schedule(settings) -> NotificationSchedule:
    return NotificationSchedule(
        timezone_name=safe_timezone_name(getattr(settings, "notification_timezone", None)),
        today_supplies_time_local=safe_time_value(
            getattr(settings, "today_supplies_time_local", None),
            fallback=DEFAULT_TODAY_SUPPLIES_TIME,
        ),
        daily_report_time_local=safe_time_value(
            getattr(settings, "daily_report_time_local", None),
            fallback=DEFAULT_DAILY_REPORT_TIME,
        ),
    )


def local_now(schedule: NotificationSchedule, now_utc: datetime | None = None) -> datetime:
    reference = now_utc or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return reference.astimezone(ZoneInfo(schedule.timezone_name))


def is_dispatch_due(*, scheduled_time: str, schedule: NotificationSchedule, now_utc: datetime | None = None) -> bool:
    now_local = local_now(schedule, now_utc)
    return now_local.strftime("%H:%M") == scheduled_time

