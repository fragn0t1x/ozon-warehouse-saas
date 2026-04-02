import asyncio
import sys
import os
from sqlalchemy import select

sys.path.append('/app/backend')

from app.database import SessionLocal
from app.models.warehouse import Warehouse
from app.models.user import User
from app.models.store import Store

async def init_warehouse():
    async with SessionLocal() as db:
        stmt = select(Warehouse)
        result = await db.execute(stmt)
        warehouse = result.scalar_one_or_none()

        if warehouse:
            print(f"✅ Склад уже существует: ID={warehouse.id}, name={warehouse.name}")
            return warehouse.id

        stmt = select(User).limit(1)
        result = await db.execute(stmt)
        user = result.scalar_one_or_none()

        if not user:
            print("❌ Нет пользователей в БД. Создаем тестового пользователя...")
            from app.models.user import User
            from passlib.context import CryptContext

            pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
            user = User(
                email="admin@example.com",
                password_hash=pwd_context.hash("admin123"),
                is_admin=True,
                is_active=True
            )
            db.add(user)
            await db.flush()
            print(f"✅ Создан пользователь: ID={user.id}, email={user.email}")

        stmt = select(Store).limit(1)
        result = await db.execute(stmt)
        store = result.scalar_one_or_none()

        if not store:
            print("❌ Нет магазинов в БД")
            return None

        warehouse = Warehouse(
            user_id=user.id,
            store_id=store.id,
            name="Основной склад"
        )
        db.add(warehouse)
        await db.commit()
        await db.refresh(warehouse)

        print(f"✅ Склад создан: ID={warehouse.id}, name={warehouse.name}")
        return warehouse.id

if __name__ == "__main__":
    asyncio.run(init_warehouse())