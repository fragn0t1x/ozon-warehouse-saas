from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.models.inventory_transaction import TransactionType
from app.models.supply import Supply
from app.models.variant import Variant
from app.models.warehouse import WarehouseStock
from app.services.warehouse_service import WarehouseService


class FakeResult:
    def __init__(self, value=None):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class FakeSession:
    def __init__(self, execute_value=None, objects=None):
        self.execute_value = execute_value
        self.objects = objects or {}
        self.added = []
        self.commits = 0
        self.rollbacks = 0
        self.flushes = 0

    async def execute(self, _stmt):
        return FakeResult(self.execute_value)

    async def get(self, model, object_id):
        return self.objects.get((model, object_id))

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1

    async def flush(self):
        self.flushes += 1

    async def refresh(self, _obj):
        return None


def make_stock(**kwargs) -> WarehouseStock:
    stock = WarehouseStock()
    stock.warehouse_id = kwargs.get("warehouse_id", 1)
    stock.variant_id = kwargs.get("variant_id", 101)
    stock.unpacked_quantity = kwargs.get("unpacked_quantity", 0)
    stock.packed_quantity = kwargs.get("packed_quantity", 0)
    stock.reserved_quantity = kwargs.get("reserved_quantity", 0)
    return stock


def make_variant(pack_size: int = 1, variant_id: int = 101) -> Variant:
    variant = Variant()
    variant.id = variant_id
    variant.pack_size = pack_size
    variant.sku = f"SKU-{variant_id}"
    return variant


def make_supply(supply_id: int = 501) -> Supply:
    supply = Supply()
    supply.id = supply_id
    supply.reserved_at = None
    return supply


@pytest.mark.asyncio
async def test_reserve_uses_packed_units_in_advanced_mode(monkeypatch):
    service = WarehouseService()
    stock = make_stock(unpacked_quantity=2, packed_quantity=2, reserved_quantity=0)
    variant = make_variant(pack_size=5)
    supply = make_supply()
    db = FakeSession(
        execute_value=None,
        objects={
            (type("Warehouse", (), {}), 1): object(),
            (Variant, 101): variant,
            (Supply, 501): supply,
        },
    )

    async def fake_get_stock(*_args, **_kwargs):
        return stock

    async def fake_get_group_stocks(*_args, **_kwargs):
        return [stock]

    monkeypatch.setattr(service, "_get_stock", fake_get_stock)
    monkeypatch.setattr(service, "_get_simple_group_stocks", fake_get_group_stocks)

    result = await service.reserve(
        db,
        warehouse_id=1,
        variant_id=101,
        quantity=8,
        supply_id=501,
        packing_mode="advanced",
        commit=False,
    )

    assert result["status"] == "ok"
    assert stock.reserved_quantity == 8
    assert isinstance(supply.reserved_at, datetime)
    assert supply.reserved_at.tzinfo == timezone.utc
    assert db.commits == 0
    assert any(tx.type == TransactionType.RESERVE for tx in db.added)


@pytest.mark.asyncio
async def test_ship_in_advanced_mode_opens_box_and_returns_leftover(monkeypatch):
    service = WarehouseService()
    stock = make_stock(unpacked_quantity=1, packed_quantity=2, reserved_quantity=6)
    variant = make_variant(pack_size=4)
    db = FakeSession(objects={(Variant, 101): variant})

    async def fake_get_stock(*_args, **_kwargs):
        return stock

    async def fake_get_group_stocks(*_args, **_kwargs):
        return [stock]

    monkeypatch.setattr(service, "_get_stock", fake_get_stock)
    monkeypatch.setattr(service, "_get_simple_group_stocks", fake_get_group_stocks)

    result = await service.ship(
        db,
        warehouse_id=1,
        variant_id=101,
        quantity=6,
        supply_id=501,
        packing_mode="advanced",
        commit=False,
    )

    assert result["status"] == "ok"
    assert stock.reserved_quantity == 0
    assert stock.packed_quantity == 0
    assert stock.unpacked_quantity == 3
    assert any(tx.type == TransactionType.SHIP for tx in db.added)


@pytest.mark.asyncio
async def test_cancel_reserve_releases_reserved_stock(monkeypatch):
    service = WarehouseService()
    stock = make_stock(unpacked_quantity=10, packed_quantity=0, reserved_quantity=7)
    db = FakeSession()

    async def fake_get_stock(*_args, **_kwargs):
        return stock

    async def fake_get_group_stocks(*_args, **_kwargs):
        return [stock]

    monkeypatch.setattr(service, "_get_stock", fake_get_stock)
    monkeypatch.setattr(service, "_get_simple_group_stocks", fake_get_group_stocks)

    result = await service.cancel_reserve(
        db,
        warehouse_id=1,
        variant_id=101,
        quantity=4,
        supply_id=501,
        commit=False,
    )

    assert result["status"] == "ok"
    assert stock.reserved_quantity == 3
    assert any(tx.type == TransactionType.UNRESERVE for tx in db.added)


@pytest.mark.asyncio
async def test_return_from_shipment_puts_units_back_to_unpacked(monkeypatch):
    service = WarehouseService()
    stock = make_stock(unpacked_quantity=3, packed_quantity=0, reserved_quantity=0)
    db = FakeSession()

    async def fake_get_stock(*_args, **_kwargs):
        return stock

    async def fake_get_group_stocks(*_args, **_kwargs):
        return [stock]

    monkeypatch.setattr(service, "_get_stock", fake_get_stock)
    monkeypatch.setattr(service, "_get_simple_group_stocks", fake_get_group_stocks)

    result = await service.return_from_shipment(
        db,
        warehouse_id=1,
        variant_id=101,
        quantity=5,
        supply_id=501,
        reason="acceptance_discrepancy",
        commit=False,
    )

    assert result["status"] == "ok"
    assert stock.unpacked_quantity == 8
    assert any(tx.type == TransactionType.RETURN for tx in db.added)


@pytest.mark.asyncio
async def test_duplicate_reserve_returns_existing_quantity(monkeypatch):
    service = WarehouseService()
    existing_tx = SimpleNamespace(quantity=12)
    stock = make_stock(unpacked_quantity=20, packed_quantity=0, reserved_quantity=12)
    variant = make_variant(pack_size=1)
    db = FakeSession(
        execute_value=existing_tx,
        objects={
            (Variant, 101): variant,
            (Supply, 501): make_supply(),
        },
    )

    async def fake_get_stock(*_args, **_kwargs):
        return stock

    monkeypatch.setattr(service, "_get_stock", fake_get_stock)

    result = await service.reserve(
        db,
        warehouse_id=1,
        variant_id=101,
        quantity=12,
        supply_id=501,
        packing_mode="simple",
        commit=False,
    )

    assert result == {"status": "already_reserved", "reserved": 12}
    assert stock.reserved_quantity == 12
    assert db.added == []
