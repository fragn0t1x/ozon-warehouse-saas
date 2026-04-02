from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.api.store_router import delete_store, patch_store, update_store
from app.api.supply_router import get_supplies, get_supply
from app.api.warehouse_product_router import relink_product_group, ProductRelinkRequest
from app.schemas.store import StorePatch, StoreUpdate
from app.models.product import Product
from app.models.store import Store
from app.models.supply import Supply
from app.services.warehouse_selector import resolve_warehouse


class ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class SequenceResult:
    def __init__(self, values):
        self._values = values

    def scalars(self):
        return self

    def all(self):
        return list(self._values)

    def first(self):
        return self._values[0] if self._values else None


class QueueDb:
    def __init__(self, *, execute_results=None, get_results=None):
        self.execute_results = list(execute_results or [])
        self.get_results = get_results or {}
        self.added = []

    async def execute(self, _stmt):
        if not self.execute_results:
            raise AssertionError("Unexpected execute call")
        return self.execute_results.pop(0)

    async def get(self, model, object_id):
        return self.get_results.get((model, object_id))

    async def flush(self):
        return None

    async def commit(self):
        return None

    async def delete(self, _obj):
        return None

    def add(self, obj):
        self.added.append(obj)


@pytest.mark.asyncio
async def test_get_supply_blocks_foreign_user_access():
    supply = Supply()
    supply.id = 55
    supply.store_id = 200

    foreign_store = Store()
    foreign_store.id = 200
    foreign_store.user_id = 2

    db = QueueDb(get_results={
        (Supply, 55): supply,
        (Store, 200): foreign_store,
    })
    current_user = SimpleNamespace(id=1)

    with pytest.raises(HTTPException) as exc_info:
        await get_supply(55, db=db, current_user=current_user)

    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_get_supplies_with_foreign_store_id_returns_empty_list():
    db = QueueDb(execute_results=[
        SequenceResult([]),  # store_ids for current user + foreign store_id filter
    ])
    current_user = SimpleNamespace(id=1)

    result = await get_supplies(store_id=999, db=db, current_user=current_user)

    assert result == {"items": [], "total": 0, "page": 1, "page_size": 20}


@pytest.mark.asyncio
async def test_relink_product_group_blocks_foreign_user_product():
    product = Product()
    product.id = 77
    store = Store()
    store.user_id = 2
    product.store = store
    product.warehouse_product_id = 10

    db = QueueDb(execute_results=[ScalarResult(product)])
    current_user = SimpleNamespace(id=1)

    with pytest.raises(HTTPException) as exc_info:
        await relink_product_group(
            77,
            ProductRelinkRequest(warehouse_product_name="Новый товар"),
            db=db,
            current_user=current_user,
        )

    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_delete_store_blocks_foreign_user_store():
    store = Store()
    store.id = 55
    store.user_id = 2

    db = QueueDb(get_results={(Store, 55): store})
    current_user = SimpleNamespace(id=1)

    with pytest.raises(HTTPException) as exc_info:
        await delete_store(55, db=db, current_user=current_user)

    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_update_store_rejects_duplicate_client_id():
    store = Store()
    store.id = 55
    store.user_id = 1
    store.client_id = "old-client"

    db = QueueDb(
        execute_results=[ScalarResult(77)],
        get_results={(Store, 55): store},
    )
    current_user = SimpleNamespace(id=1)

    with pytest.raises(HTTPException) as exc_info:
        await update_store(
            55,
            StoreUpdate(name="Updated", client_id="dup-client", api_key=None),
            db=db,
            current_user=current_user,
        )

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_patch_store_rejects_duplicate_client_id():
    store = Store()
    store.id = 55
    store.user_id = 1
    store.client_id = "old-client"

    db = QueueDb(
        execute_results=[ScalarResult(88)],
        get_results={(Store, 55): store},
    )
    current_user = SimpleNamespace(id=1)

    with pytest.raises(HTTPException) as exc_info:
        await patch_store(
            55,
            StorePatch(client_id="dup-client"),
            db=db,
            current_user=current_user,
        )

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_resolve_warehouse_blocks_foreign_warehouse_id():
    foreign_warehouse = SimpleNamespace(id=50, user_id=2)
    settings = SimpleNamespace(shared_warehouse_id=None, warehouse_mode="shared")
    from app.models.warehouse import Warehouse

    db = QueueDb(
        execute_results=[ScalarResult(settings)],
        get_results={(Warehouse, 50): foreign_warehouse},
    )

    with pytest.raises(ValueError, match="Warehouse not found or не доступен"):
        await resolve_warehouse(db, user_id=1, warehouse_id=50)
