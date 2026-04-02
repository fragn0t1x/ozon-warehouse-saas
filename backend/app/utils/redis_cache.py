import asyncio
import json
import logging
from typing import Any, Optional

from redis.asyncio import Redis

from app.config import settings

logger = logging.getLogger(__name__)

_redis_client: Optional[Redis] = None
_redis_loop: Optional[asyncio.AbstractEventLoop] = None


async def get_redis() -> Optional[Redis]:
    global _redis_client, _redis_loop

    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    if _redis_client and _redis_loop is current_loop:
        return _redis_client

    if not settings.REDIS_URL:
        return None

    try:
        if _redis_client is not None and _redis_loop is not current_loop:
            try:
                await _redis_client.aclose()
            except Exception:
                pass
        _redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
        _redis_loop = current_loop
        return _redis_client
    except Exception as e:
        logger.error(f"❌ Failed to connect to Redis: {e}")
        return None


async def cache_get_json(key: str) -> Optional[Any]:
    client = await get_redis()
    if not client:
        return None
    try:
        value = await client.get(key)
        if value:
            return json.loads(value)
    except Exception as e:
        logger.error(f"❌ Redis get failed: {e}")
    return None


async def cache_set_json(key: str, value: Any, ttl_seconds: int) -> None:
    client = await get_redis()
    if not client:
        return
    try:
        await client.set(key, json.dumps(value, ensure_ascii=False), ex=ttl_seconds)
    except Exception as e:
        logger.error(f"❌ Redis set failed: {e}")


async def cache_delete(key: str) -> None:
    client = await get_redis()
    if not client:
        return
    try:
        await client.delete(key)
    except Exception as e:
        logger.error(f"❌ Redis delete failed: {e}")
