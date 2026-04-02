from __future__ import annotations

from datetime import datetime, timezone

from app.models.user_settings import UserSettings


def _normalize_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def get_effective_shipments_accounting_start(
    settings: UserSettings | None,
) -> datetime | None:
    if settings is None or not settings.shipments_accounting_enabled:
        return None
    return _normalize_utc(
        settings.shipments_start_date or settings.shipments_accounting_enabled_at
    )


def get_supply_accounting_skip_reason(
    *,
    supply_created_at: datetime | None,
    settings: UserSettings | None,
) -> str | None:
    if settings is None:
        return None

    if not settings.shipments_accounting_enabled:
        return "shipment_skipped_accounting_disabled"

    effective_start = get_effective_shipments_accounting_start(settings)
    if effective_start is None or supply_created_at is None:
        return None

    created_at_utc = _normalize_utc(supply_created_at)
    if created_at_utc is not None and created_at_utc < effective_start:
        return "shipment_skipped_before_start_date"

    return None


def supply_affects_stock(
    *,
    supply_created_at: datetime | None,
    settings: UserSettings | None,
) -> bool:
    return get_supply_accounting_skip_reason(
        supply_created_at=supply_created_at,
        settings=settings,
    ) is None
