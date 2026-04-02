from app.utils.redis_cache import cache_delete


def shipments_response_cache_key(*, owner_user_id: int, store_id: int, order_window_days: int) -> str:
    return f"shipments:response:{owner_user_id}:{store_id}:{order_window_days}"


async def invalidate_shipments_response_cache(*, owner_user_id: int, store_id: int) -> None:
    for order_window_days in (7, 30, 90):
        await cache_delete(
            shipments_response_cache_key(
                owner_user_id=owner_user_id,
                store_id=store_id,
                order_window_days=order_window_days,
            )
        )
