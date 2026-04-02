from __future__ import annotations

from app.utils.redis_cache import get_redis


SYNC_LOCK_TTL_SECONDS = 30 * 60


def sync_lock_key(kind: str, store_id: int) -> str:
    return f"sync:{kind}:{store_id}"


async def acquire_sync_lock(kind: str, store_id: int, ttl_seconds: int = SYNC_LOCK_TTL_SECONDS) -> bool:
    redis = await get_redis()
    if not redis:
        return True
    return bool(await redis.set(sync_lock_key(kind, store_id), "1", nx=True, ex=ttl_seconds))


async def release_sync_lock(kind: str, store_id: int) -> None:
    redis = await get_redis()
    if not redis:
        return
    await redis.delete(sync_lock_key(kind, store_id))


async def has_sync_lock(kind: str, store_id: int) -> bool:
    redis = await get_redis()
    if not redis:
        return False
    return bool(await redis.exists(sync_lock_key(kind, store_id)))


async def has_any_sync_lock(store_id: int, kinds: tuple[str, ...]) -> bool:
    redis = await get_redis()
    if not redis:
        return False
    keys = [sync_lock_key(kind, store_id) for kind in kinds]
    if not keys:
        return False
    return bool(await redis.exists(*keys))
