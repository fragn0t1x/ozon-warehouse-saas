# backend/scripts/create_admin.py
import asyncio
import sys
import os

# Добавляем путь к корневой директории проекта
sys.path.insert(0, '/app')
sys.path.insert(0, '/app/backend')

from app.database import SessionLocal
from app.models.user import User
from app.core.security import get_password_hash
from sqlalchemy import select


async def create_admin(email: str, password: str):
    print(f"📧 Создание администратора: {email}")

    async with SessionLocal() as db:
        # Проверяем, есть ли уже такой пользователь
        result = await db.execute(
            select(User).where(User.email == email)
        )
        existing = result.scalar_one_or_none()

        if existing:
            print(f"❌ Пользователь с email {email} уже существует")
            return

        # Создаем админа
        admin = User(
            email=email,
            password_hash=get_password_hash(password),
            is_admin=True,
            is_active=True
        )

        db.add(admin)
        await db.commit()
        print(f"✅ Администратор создан: {email}")
        print(f"   ID: {admin.id}")
        print(f"   Email: {admin.email}")
        print(f"   Admin: {admin.is_admin}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Использование: python create_admin.py <email> <password>")
        print("Пример: python create_admin.py admin@example.com admin123")
        sys.exit(1)

    email = sys.argv[1]
    password = sys.argv[2]
    asyncio.run(create_admin(email, password))