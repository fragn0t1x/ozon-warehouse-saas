from datetime import datetime, timedelta

from app.services.sync_service import (
    SyncService,
    normalize_product_link_map,
    select_product_link_plan,
    should_notify_supply_created,
)


class _FakeSession:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1

    async def execute(self, _stmt):
        raise NotImplementedError

    def begin_nested(self):
        return _FakeNestedTransaction()


class _FakeNestedTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeStore:
    def __init__(self, store_id=1, name="Test Store"):
        self.id = store_id
        self.name = name


class _FakeClient:
    async def get_clusters(self, cluster_type):
        return [{"id": 1, "name": cluster_type, "logistic_clusters": []}]


class _DeadlockError(Exception):
    sqlstate = "40P01"


class _FakeTrackedResult:
    def __init__(self, values):
        self._values = values

    def scalars(self):
        return self

    def all(self):
        return self._values


async def test_refresh_clusters_before_stock_sync_rolls_back_and_retries_on_deadlock(monkeypatch):
    db = _FakeSession()
    service = SyncService(db)
    client = _FakeClient()
    calls = {"count": 0}

    async def fake_get_redis():
        return None

    async def fake_sync(_payloads):
        calls["count"] += 1
        if calls["count"] == 1:
            raise _DeadlockError("deadlock detected")

    monkeypatch.setattr("app.services.sync_service.get_redis", fake_get_redis)
    monkeypatch.setattr(service, "_sync_cluster_payloads", fake_sync)

    refreshed = await service._refresh_clusters_before_stock_sync(
        client,
        store_id=1,
        store_name="Test Store",
    )

    assert refreshed is True
    assert db.rollbacks == 1
    assert db.commits == 1


async def test_refresh_clusters_before_stock_sync_clears_failed_transaction(monkeypatch):
    db = _FakeSession()
    service = SyncService(db)
    client = _FakeClient()

    async def fake_get_redis():
        return None

    async def fake_sync(_payloads):
        raise RuntimeError("boom")

    monkeypatch.setattr("app.services.sync_service.get_redis", fake_get_redis)
    monkeypatch.setattr(service, "_sync_cluster_payloads", fake_sync)

    refreshed = await service._refresh_clusters_before_stock_sync(
        client,
        store_id=1,
        store_name="Test Store",
    )

    assert refreshed is False
    assert db.rollbacks == 1
    assert db.commits == 0


async def test_refresh_clusters_before_stock_sync_skips_when_ttl_guard_active(monkeypatch):
    db = _FakeSession()
    service = SyncService(db)
    client = _FakeClient()

    class _FakeRedis:
        async def set(self, *_args, **_kwargs):
            return False

    async def fake_get_redis():
        return _FakeRedis()

    async def fake_sync(_payloads):
        raise AssertionError("cluster refresh should have been skipped by TTL guard")

    monkeypatch.setattr("app.services.sync_service.get_redis", fake_get_redis)
    monkeypatch.setattr(service, "_sync_cluster_payloads", fake_sync)

    refreshed = await service._refresh_clusters_before_stock_sync(
        client,
        store_id=1,
        store_name="Test Store",
    )

    assert refreshed is False
    assert db.rollbacks == 0
    assert db.commits == 0


def test_is_deadlock_error_detects_nested_sqlalchemy_style_exception():
    class _Orig(Exception):
        sqlstate = "40P01"

    wrapped = RuntimeError("wrapper")
    wrapped.__cause__ = _Orig("deadlock detected")

    assert SyncService._is_deadlock_error(wrapped) is True
    assert SyncService._is_deadlock_error(RuntimeError("boom")) is False


async def test_refresh_tracked_supplies_for_store_updates_old_active_orders(monkeypatch):
    class _TrackedSession(_FakeSession):
        async def execute(self, _stmt):
            return _FakeTrackedResult(["12345", "abc", None])

    db = _TrackedSession()
    service = SyncService(db)
    client = _FakeClient()
    processed_order_ids = []

    async def fake_get_detail(_client, order_id, max_retries=3):
        return {"order_id": order_id, "state": "REPORTS_CONFIRMATION_AWAITING", "supplies": []}

    async def fake_process(store_id, order, _client, **_kwargs):
        processed_order_ids.append((store_id, str(order["order_id"])))
        return []

    monkeypatch.setattr(service, "_get_supply_order_detail_with_retry", fake_get_detail)
    monkeypatch.setattr(service, "_process_supply_order", fake_process)

    refreshed_ids, refreshed_count, queued_event_ids = await service._refresh_tracked_supplies_for_store(2, client)

    assert refreshed_ids == {"12345"}
    assert refreshed_count == 1
    assert queued_event_ids == set()
    assert processed_order_ids == [(2, "12345")]


def test_should_notify_supply_created_for_active_and_recent_final_supplies():
    now = datetime.now()

    assert should_notify_supply_created(status="READY_TO_SUPPLY", created_at=now - timedelta(days=30)) is True
    assert should_notify_supply_created(status="CANCELLED", created_at=now - timedelta(hours=2)) is True
    assert should_notify_supply_created(status="COMPLETED", created_at=now - timedelta(days=10)) is False
    assert should_notify_supply_created(status="CANCELLED", created_at=None) is False


def test_normalize_product_link_map_preserves_multiple_plans_for_same_group():
    normalized = normalize_product_link_map(
        {
            "group-1": [
                {"warehouse_product_name": "Носки белые", "offer_ids": ["wb-1", "wb-2"]},
                {"warehouse_product_name": "Носки черные", "offer_ids": ["blk-1", "", None]},
            ]
        }
    )

    assert list(normalized) == ["group-1"]
    assert len(normalized["group-1"]) == 2
    assert normalized["group-1"][0]["offer_ids"] == ["wb-1", "wb-2"]
    assert normalized["group-1"][1]["offer_ids"] == ["blk-1"]


def test_select_product_link_plan_prefers_offer_specific_plan_over_group_default():
    product_link_map = normalize_product_link_map(
        {
            "group-1": [
                {"warehouse_product_name": "Футболка белая", "offer_ids": ["sku-white"]},
                {"warehouse_product_name": "Футболка базовая"},
            ]
        }
    )

    exact = select_product_link_plan(
        product_link_map,
        group_key="group-1",
        base_name="Футболка",
        offer_id="sku-white",
    )
    fallback = select_product_link_plan(
        product_link_map,
        group_key="group-1",
        base_name="Футболка",
        offer_id="sku-black",
    )

    assert exact["warehouse_product_name"] == "Футболка белая"
    assert fallback["warehouse_product_name"] == "Футболка базовая"
