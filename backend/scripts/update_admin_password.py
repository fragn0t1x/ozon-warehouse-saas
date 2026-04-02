# backend/scripts/update_admin_password.py
import asyncio
import sys
import os

sys.path.insert(0, '/app')
sys.path.insert(0, '/app/backend')

from app.database import SessionLocal
from app.models.user import User
from app.core.security import get_password_hash
from sqlalchemy import select


async def update_admin_password(email: str, new_password: str):
    print(f"📧 Обновление пароля для: {email}")

    async with SessionLocal() as db:
        # Ищем пользователя
        result = await db.execute(
            select(User).where(User.email == email)
        )
        user = result.scalar_one_or_none()

        if not user:
            print(f"❌ Пользователь с email {email} не найден")
            return

        # Обновляем пароль
        old_hash = user.password_hash
        user.password_hash = get_password_hash(new_password)

        await db.commit()

        print(f"✅ Пароль обновлен")
        print(f"   ID: {user.id}")
        print(f"   Email: {user.email}")
        print(f"   Старый хэш: {old_hash}")
        print(f"   Новый хэш: {user.password_hash}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Использование: python update_admin_password.py <email> <new_password>")
        print("Пример: python update_admin_password.py admin@example.com admin123")
        sys.exit(1)

    email = sys.argv[1]
    password = sys.argv[2]
    asyncio.run(update_admin_password(email, password))