from datetime import datetime, timezone

from app.services.notification_schedule import (
    DEFAULT_DAILY_REPORT_TIME,
    DEFAULT_NOTIFICATION_TIMEZONE,
    DEFAULT_TODAY_SUPPLIES_TIME,
    build_notification_schedule,
    is_dispatch_due,
    local_now,
)


class _Settings:
    def __init__(self, *, timezone_name=None, today=None, daily=None):
        self.notification_timezone = timezone_name
        self.today_supplies_time_local = today
        self.daily_report_time_local = daily


def test_build_notification_schedule_uses_defaults_for_invalid_values():
    schedule = build_notification_schedule(
        _Settings(timezone_name="Bad/Timezone", today="25:99", daily="oops")
    )

    assert schedule.timezone_name == DEFAULT_NOTIFICATION_TIMEZONE
    assert schedule.today_supplies_time_local == DEFAULT_TODAY_SUPPLIES_TIME
    assert schedule.daily_report_time_local == DEFAULT_DAILY_REPORT_TIME


def test_local_now_uses_user_timezone():
    schedule = build_notification_schedule(_Settings(timezone_name="Asia/Yekaterinburg"))
    now_utc = datetime(2026, 3, 18, 6, 0, tzinfo=timezone.utc)

    converted = local_now(schedule, now_utc)

    assert converted.strftime("%Y-%m-%d %H:%M") == "2026-03-18 11:00"


def test_is_dispatch_due_matches_exact_local_minute():
    schedule = build_notification_schedule(_Settings(timezone_name="Europe/Moscow", today="09:15"))
    now_utc = datetime(2026, 3, 18, 6, 15, tzinfo=timezone.utc)

    assert is_dispatch_due(scheduled_time=schedule.today_supplies_time_local, schedule=schedule, now_utc=now_utc)
    assert not is_dispatch_due(scheduled_time="09:16", schedule=schedule, now_utc=now_utc)
