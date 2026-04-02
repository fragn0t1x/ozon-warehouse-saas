import json

import pytest

from app.services import supply_reservation_wait as reservation_wait


class FakeRedis:
    def __init__(self):
        self.values: dict[str, str] = {}

    async def exists(self, key: str) -> int:
        return 1 if key in self.values else 0

    async def set(self, key: str, value: str, ex: int | None = None):
        self.values[key] = value
        return True

    async def delete(self, *keys: str) -> int:
        removed = 0
        for key in keys:
            if key in self.values:
                removed += 1
                del self.values[key]
        return removed


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class FakeDb:
    def __init__(self, rows):
        self._rows = rows

    async def execute(self, _stmt):
        return FakeResult(self._rows)


@pytest.mark.asyncio
async def test_mark_and_clear_supply_wait_state(monkeypatch):
    redis = FakeRedis()

    async def fake_get_redis():
        return redis

    monkeypatch.setattr(reservation_wait, "get_redis", fake_get_redis)

    await reservation_wait.mark_supply_waiting_for_stock(
        42,
        variant_id=460,
        required_quantity=40,
        message="Not enough available stock. Have 0, need 40",
    )

    key = reservation_wait.supply_reservation_wait_key(42)
    assert key in redis.values
    assert await reservation_wait.is_supply_reservation_wait_active(42) is True

    payload = json.loads(redis.values[key])
    assert payload["supply_id"] == 42
    assert payload["variant_id"] == 460
    assert payload["required_quantity"] == 40

    await reservation_wait.clear_supply_reservation_wait(42)
    assert await reservation_wait.is_supply_reservation_wait_active(42) is False


@pytest.mark.asyncio
async def test_clear_wait_states_for_variants(monkeypatch):
    redis = FakeRedis()

    async def fake_get_redis():
        return redis

    monkeypatch.setattr(reservation_wait, "get_redis", fake_get_redis)

    redis.values[reservation_wait.supply_reservation_wait_key(10)] = "1"
    redis.values[reservation_wait.supply_reservation_wait_key(11)] = "1"
    redis.values[reservation_wait.supply_reservation_wait_key(99)] = "1"

    cleared = await reservation_wait.clear_supply_reservation_wait_for_variants(
        FakeDb([(10,), (11,)]),
        [460, 461],
    )

    assert cleared == 2
    assert reservation_wait.supply_reservation_wait_key(10) not in redis.values
    assert reservation_wait.supply_reservation_wait_key(11) not in redis.values
    assert reservation_wait.supply_reservation_wait_key(99) in redis.values
