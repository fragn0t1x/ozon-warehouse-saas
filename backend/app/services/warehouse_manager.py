# backend/app/services/warehouse_manager.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.user import User
from app.models.user_settings import UserSettings
from app.models.warehouse import Warehouse
from app.models.store import Store
from app.models.variant import Variant
from app.models.product import Product
from app.services.user_settings_helper import get_or_create_user_settings
from loguru import logger
from typing import Optional, List


class WarehouseManager:
    """
    Менеджер для работы со складами в зависимости от режима
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_warehouse_for_store(self, user: User, store_id: Optional[int] = None) -> Warehouse:
        """
        Получает склад для магазина в зависимости от режима
        """
        settings = await self._get_settings(user)

        if settings.warehouse_mode == "shared":
            # Режим одного склада
            if not settings.shared_warehouse_id:
                # Создаем общий склад, если его нет
                warehouse = await self._create_shared_warehouse(user)
                settings.shared_warehouse_id = warehouse.id
                await self.db.commit()
                return warehouse

            warehouse = await self.db.get(Warehouse, settings.shared_warehouse_id)
            if not warehouse:
                # Если склад удален, создаем новый
                warehouse = await self._create_shared_warehouse(user)
                settings.shared_warehouse_id = warehouse.id
                await self.db.commit()

            return warehouse

        else:
            # Режим отдельных складов
            if not store_id:
                raise ValueError("store_id is required in per_store mode")

            # Ищем склад для магазина
            stmt = select(Warehouse).where(
                Warehouse.user_id == user.id,
                Warehouse.store_id == store_id
            )
            result = await self.db.execute(stmt)
            warehouse = result.scalar_one_or_none()

            if not warehouse:
                # Создаем склад для магазина
                warehouse = await self._create_store_warehouse(user, store_id)

            return warehouse

    async def get_all_warehouses(self, user: User) -> List[Warehouse]:
        """
        Получает все склады пользователя
        """
        stmt = select(Warehouse).where(Warehouse.user_id == user.id)
        result = await self.db.execute(stmt)
        return result.scalars().all()

    async def get_warehouse_stock(self, user: User, warehouse_id: Optional[int] = None, store_id: Optional[int] = None):
        """
        Получает остатки со склада
        """
        if warehouse_id:
            warehouse = await self.db.get(Warehouse, warehouse_id)
            if not warehouse or warehouse.user_id != user.id:
                raise ValueError("Warehouse not found")
        else:
            warehouse = await self.get_warehouse_for_store(user, store_id)

        # Получаем остатки
        from app.models.warehouse import WarehouseStock
        from app.models.variant import Variant
        from app.models.product import Product

        stmt = select(
            WarehouseStock,
            Variant,
            Product
        ).join(
            Variant, Variant.id == WarehouseStock.variant_id
        ).join(
            Product, Product.id == Variant.product_id
        ).where(
            WarehouseStock.warehouse_id == warehouse.id
        )

        result = await self.db.execute(stmt)
        return result.all()

    async def _get_settings(self, user: User) -> UserSettings:
        """Получает настройки пользователя"""
        settings, _changed = await get_or_create_user_settings(self.db, user.id)
        return settings

    async def _create_shared_warehouse(self, user: User) -> Warehouse:
        """Создает общий склад"""
        warehouse = Warehouse(
            user_id=user.id,
            store_id=None,
            name="Основной склад"
        )
        self.db.add(warehouse)
        await self.db.flush()
        logger.info(f"✅ Created shared warehouse for user {user.id}")
        return warehouse

    async def _create_store_warehouse(self, user: User, store_id: int) -> Warehouse:
        """Создает склад для конкретного магазина"""
        store = await self.db.get(Store, store_id)
        if not store or store.user_id != user.id:
            raise ValueError("Store not found")

        warehouse = Warehouse(
            user_id=user.id,
            store_id=store_id,
            name=f"Склад для {store.name}"
        )
        self.db.add(warehouse)
        await self.db.flush()
        logger.info(f"✅ Created warehouse for store {store_id}")
        return warehouse
