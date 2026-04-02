from typing import Optional, Tuple

from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.store import Store
from app.models.user_settings import UserSettings
from app.models.warehouse import Warehouse
from app.services.user_settings_helper import get_or_create_user_settings as get_or_create_user_settings_row


async def get_or_create_settings(db: AsyncSession, user_id: int) -> UserSettings:
    settings, changed = await get_or_create_user_settings_row(db, user_id)
    if changed:
        logger.info(f"✅ Created settings for user {user_id}")
    return settings


async def ensure_shared_warehouse(db: AsyncSession, user_id: int, settings: UserSettings) -> Warehouse:
    if settings.shared_warehouse_id:
        warehouse = await db.get(Warehouse, settings.shared_warehouse_id)
        if warehouse and warehouse.user_id == user_id:
            return warehouse

    warehouse = Warehouse(
        user_id=user_id,
        store_id=None,
        name="Основной склад"
    )
    db.add(warehouse)
    await db.flush()

    settings.shared_warehouse_id = warehouse.id
    await db.flush()

    logger.info(f"✅ Created shared warehouse {warehouse.id} for user {user_id}")
    return warehouse


async def ensure_store_warehouse(db: AsyncSession, user_id: int, store_id: int) -> Warehouse:
    stmt = select(Warehouse).where(
        Warehouse.user_id == user_id,
        Warehouse.store_id == store_id
    )
    result = await db.execute(stmt)
    warehouse = result.scalar_one_or_none()
    if warehouse:
        return warehouse

    store = await db.get(Store, store_id)
    name = f"Склад {store.name}" if store else "Склад магазина"
    warehouse = Warehouse(
        user_id=user_id,
        store_id=store_id,
        name=name
    )
    db.add(warehouse)
    await db.flush()

    logger.info(f"✅ Created warehouse {warehouse.id} for store {store_id}")
    return warehouse


async def resolve_warehouse(
    db: AsyncSession,
    user_id: int,
    store_id: Optional[int] = None,
    warehouse_id: Optional[int] = None
) -> Tuple[Warehouse, UserSettings]:
    if warehouse_id is not None:
        warehouse = await db.get(Warehouse, warehouse_id)
        if not warehouse or warehouse.user_id != user_id:
            raise ValueError("Warehouse not found or не доступен")
        settings = await get_or_create_settings(db, user_id)
        return warehouse, settings

    settings = await get_or_create_settings(db, user_id)

    if settings.warehouse_mode == "shared":
        warehouse = await ensure_shared_warehouse(db, user_id, settings)
        await db.commit()
        await db.refresh(settings)
        return warehouse, settings

    if store_id is None:
        raise ValueError("store_id is required for per_store mode")

    # Проверяем, что магазин принадлежит пользователю
    store = await db.get(Store, store_id)
    if not store or store.user_id != user_id:
        raise ValueError("Store not found or не доступен")

    warehouse = await ensure_store_warehouse(db, user_id, store_id)
    await db.commit()
    await db.refresh(settings)
    return warehouse, settings
