from __future__ import annotations

from datetime import datetime, timezone

from app.models.user_settings import UserSettings
from app.services.shipment_accounting import (
    get_effective_shipments_accounting_start,
    get_supply_accounting_skip_reason,
    supply_affects_stock,
)


def make_settings(**overrides) -> UserSettings:
    settings = UserSettings()
    settings.shipments_accounting_enabled = overrides.get("shipments_accounting_enabled", False)
    settings.shipments_start_date = overrides.get("shipments_start_date")
    settings.shipments_accounting_enabled_at = overrides.get("shipments_accounting_enabled_at")
    return settings


def test_disabled_accounting_skips_supply() -> None:
    settings = make_settings(shipments_accounting_enabled=False)

    assert get_supply_accounting_skip_reason(
        supply_created_at=datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc),
        settings=settings,
    ) == "shipment_skipped_accounting_disabled"
    assert supply_affects_stock(
        supply_created_at=datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc),
        settings=settings,
    ) is False


def test_explicit_start_date_is_used_as_accounting_anchor() -> None:
    start = datetime(2026, 3, 20, 0, 0, tzinfo=timezone.utc)
    settings = make_settings(
        shipments_accounting_enabled=True,
        shipments_start_date=start,
    )

    assert get_effective_shipments_accounting_start(settings) == start
    assert get_supply_accounting_skip_reason(
        supply_created_at=datetime(2026, 3, 19, 23, 59, tzinfo=timezone.utc),
        settings=settings,
    ) == "shipment_skipped_before_start_date"
    assert get_supply_accounting_skip_reason(
        supply_created_at=datetime(2026, 3, 20, 0, 0, tzinfo=timezone.utc),
        settings=settings,
    ) is None


def test_enabled_at_is_used_when_start_date_is_empty() -> None:
    enabled_at = datetime(2026, 3, 24, 10, 0, tzinfo=timezone.utc)
    settings = make_settings(
        shipments_accounting_enabled=True,
        shipments_accounting_enabled_at=enabled_at,
    )

    assert get_effective_shipments_accounting_start(settings) == enabled_at
    assert get_supply_accounting_skip_reason(
        supply_created_at=datetime(2026, 3, 24, 9, 59, tzinfo=timezone.utc),
        settings=settings,
    ) == "shipment_skipped_before_start_date"
    assert supply_affects_stock(
        supply_created_at=datetime(2026, 3, 24, 10, 0, tzinfo=timezone.utc),
        settings=settings,
    ) is True


def test_missing_settings_keeps_previous_fallback_behavior() -> None:
    assert get_supply_accounting_skip_reason(
        supply_created_at=datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc),
        settings=None,
    ) is None
