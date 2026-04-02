from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from app.services.sync_status import get_store_sync_status

BackgroundSyncKind = Literal["products", "supplies", "stocks", "reports", "finance"]

SYNC_INTERVAL_FIELD_BY_KIND: dict[BackgroundSyncKind, str] = {
    "products": "sync_products_interval_minutes",
    "supplies": "sync_supplies_interval_minutes",
    "stocks": "sync_stocks_interval_minutes",
    "reports": "sync_reports_interval_minutes",
    "finance": "sync_finance_interval_minutes",
}

SYNC_INTERVAL_MINIMUMS: dict[BackgroundSyncKind, int] = {
    "products": 360,
    "supplies": 5,
    "stocks": 20,
    "reports": 180,
    "finance": 360,
}

SYNC_INTERVAL_DEFAULTS: dict[BackgroundSyncKind, int] = {
    "products": 360,
    "supplies": 5,
    "stocks": 20,
    "reports": 180,
    "finance": 360,
}


def get_sync_interval_minutes(settings: Any, kind: BackgroundSyncKind) -> int:
    field_name = SYNC_INTERVAL_FIELD_BY_KIND[kind]
    minimum = SYNC_INTERVAL_MINIMUMS[kind]
    default = SYNC_INTERVAL_DEFAULTS[kind]
    raw_value = getattr(settings, field_name, None) if settings is not None else None

    try:
        value = int(raw_value) if raw_value is not None else default
    except (TypeError, ValueError):
        value = default

    return max(value, minimum)


def get_sync_interval_minutes_from_payload(payload: dict[str, Any], kind: BackgroundSyncKind) -> int:
    field_name = SYNC_INTERVAL_FIELD_BY_KIND[kind]
    minimum = SYNC_INTERVAL_MINIMUMS[kind]
    default = SYNC_INTERVAL_DEFAULTS[kind]

    try:
        value = int(payload.get(field_name, default))
    except (TypeError, ValueError):
        value = default

    return max(value, minimum)


def format_sync_interval_label(kind: BackgroundSyncKind, minutes: int) -> str:
    value = max(int(minutes), 1)
    if value % (24 * 60) == 0:
        days = value // (24 * 60)
        suffix = "день" if days == 1 else "дня" if 2 <= days <= 4 else "дней"
        return f"Фон: каждые {days} {suffix}"
    if value % 60 == 0:
        hours = value // 60
        suffix = "час" if hours == 1 else "часа" if 2 <= hours <= 4 else "часов"
        return f"Фон: каждые {hours} {suffix}"
    return f"Фон: каждые {value} мин."


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def get_last_background_sync_completion(store_id: int, kind: BackgroundSyncKind) -> datetime | None:
    status = get_store_sync_status(store_id)
    kind_status = (status.get("sync_kinds") or {}).get(kind) or {}
    return _parse_iso_datetime(
        kind_status.get("finished_at")
        or kind_status.get("last_success_at")
        or kind_status.get("last_failure_at")
    )


def is_background_sync_due(store_id: int, kind: BackgroundSyncKind, interval_minutes: int) -> bool:
    last_completed_at = get_last_background_sync_completion(store_id, kind)
    if last_completed_at is None:
        return True
    next_due_at = last_completed_at + timedelta(minutes=max(int(interval_minutes), 1))
    return datetime.now(timezone.utc) >= next_due_at
