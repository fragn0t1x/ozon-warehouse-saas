from __future__ import annotations

from redis import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import settings
from app.models.store import Store
from app.services.sync_dispatcher import enqueue_full_sync
from app.utils.redis_cache import get_redis


BOOTSTRAP_STATE_KEY = "store:{store_id}:bootstrap_state"
BOOTSTRAP_STATE_PENDING = "pending"
BOOTSTRAP_STATE_COMPLETED = "completed"
BOOTSTRAP_STATE_FAILED = "failed"
BOOTSTRAP_STATE_RUNNING = "running"

_redis_sync = Redis.from_url(settings.REDIS_URL, decode_responses=True)


def _key(store_id: int) -> str:
    return BOOTSTRAP_STATE_KEY.format(store_id=store_id)


def get_bootstrap_state_sync(store_id: int) -> str:
    value = _redis_sync.get(_key(store_id))
    return value or BOOTSTRAP_STATE_PENDING


def is_bootstrap_completed_sync(store_id: int) -> bool:
    return get_bootstrap_state_sync(store_id) == BOOTSTRAP_STATE_COMPLETED


def mark_bootstrap_state_sync(store_id: int, state: str) -> None:
    _redis_sync.set(_key(store_id), state)


def reset_bootstrap_state_sync(store_id: int) -> None:
    mark_bootstrap_state_sync(store_id, BOOTSTRAP_STATE_PENDING)


async def get_bootstrap_state(store_id: int) -> str:
    redis = await get_redis()
    if not redis:
        return BOOTSTRAP_STATE_PENDING
    value = await redis.get(_key(store_id))
    return value or BOOTSTRAP_STATE_PENDING


async def is_bootstrap_completed(store_id: int) -> bool:
    return await get_bootstrap_state(store_id) == BOOTSTRAP_STATE_COMPLETED


async def mark_bootstrap_state(store_id: int, state: str) -> None:
    redis = await get_redis()
    if not redis:
        return
    await redis.set(_key(store_id), state)


async def mark_bootstrap_pending(store_id: int) -> None:
    await mark_bootstrap_state(store_id, BOOTSTRAP_STATE_PENDING)


async def mark_bootstrap_running(store_id: int) -> None:
    await mark_bootstrap_state(store_id, BOOTSTRAP_STATE_RUNNING)


async def mark_bootstrap_completed(store_id: int) -> None:
    await mark_bootstrap_state(store_id, BOOTSTRAP_STATE_COMPLETED)


async def mark_bootstrap_failed(store_id: int) -> None:
    await mark_bootstrap_state(store_id, BOOTSTRAP_STATE_FAILED)


async def enqueue_startup_bootstrap_syncs(session_factory: async_sessionmaker) -> int:
    queued = 0
    async with session_factory() as db:
        result = await db.execute(
            select(Store.id).where(Store.is_active == True)  # noqa: E712
        )
        store_ids = [row[0] for row in result.all()]

    for store_id in store_ids:
        if is_bootstrap_completed_sync(store_id):
            continue
        enqueue_full_sync(store_id, bootstrap=True, trigger="startup")
        queued += 1

    return queued
