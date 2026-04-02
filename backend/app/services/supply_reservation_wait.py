import json

from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.supply import Supply, SupplyItem
from app.utils.redis_cache import get_redis

RESERVATION_WAIT_TTL_SECONDS = 15 * 60


def supply_reservation_wait_key(supply_id: int) -> str:
    return f"supply:{supply_id}:reservation_waiting_stock"


async def is_supply_reservation_wait_active(supply_id: int) -> bool:
    redis = await get_redis()
    if not redis:
        return False
    try:
        return bool(await redis.exists(supply_reservation_wait_key(supply_id)))
    except Exception as e:
        logger.warning("⚠️ Failed to read reservation wait state for supply {}: {}", supply_id, e)
        return False


async def mark_supply_waiting_for_stock(
    supply_id: int,
    *,
    variant_id: int | None = None,
    required_quantity: int | None = None,
    message: str | None = None,
    ttl_seconds: int = RESERVATION_WAIT_TTL_SECONDS,
) -> None:
    redis = await get_redis()
    if not redis:
        return

    payload = {
        "supply_id": supply_id,
        "variant_id": variant_id,
        "required_quantity": required_quantity,
        "message": message,
    }

    try:
        await redis.set(
            supply_reservation_wait_key(supply_id),
            json.dumps(payload, ensure_ascii=False),
            ex=max(int(ttl_seconds), 1),
        )
    except Exception as e:
        logger.warning("⚠️ Failed to mark supply {} as waiting for stock: {}", supply_id, e)


async def clear_supply_reservation_wait(supply_id: int) -> None:
    redis = await get_redis()
    if not redis:
        return
    try:
        await redis.delete(supply_reservation_wait_key(supply_id))
    except Exception as e:
        logger.warning("⚠️ Failed to clear reservation wait state for supply {}: {}", supply_id, e)


async def get_supply_reservation_wait_map(supply_ids: list[int]) -> dict[int, dict]:
    redis = await get_redis()
    if not redis or not supply_ids:
        return {}

    result: dict[int, dict] = {}
    for supply_id in supply_ids:
        try:
            raw = await redis.get(supply_reservation_wait_key(supply_id))
        except Exception as e:
            logger.warning("⚠️ Failed to read reservation wait payload for supply {}: {}", supply_id, e)
            continue
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            payload = {"message": raw}
        result[supply_id] = payload if isinstance(payload, dict) else {"message": str(payload)}
    return result


async def clear_supply_reservation_wait_for_variants(
    db: AsyncSession,
    variant_ids: list[int],
) -> int:
    redis = await get_redis()
    if not redis or not variant_ids:
        return 0

    stmt = (
        select(Supply.id)
        .join(SupplyItem, SupplyItem.supply_id == Supply.id)
        .where(
            Supply.status == "READY_TO_SUPPLY",
            Supply.reserved_at.is_(None),
            SupplyItem.variant_id.in_(variant_ids),
        )
        .distinct()
    )
    result = await db.execute(stmt)
    supply_ids = [row[0] for row in result.all()]
    if not supply_ids:
        return 0

    keys = [supply_reservation_wait_key(supply_id) for supply_id in supply_ids]
    try:
        await redis.delete(*keys)
    except Exception as e:
        logger.warning("⚠️ Failed to clear reservation wait states for variants {}: {}", variant_ids, e)
        return 0
    return len(keys)
