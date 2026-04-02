import secrets
import string
from sqlalchemy import select

from app.config import settings
from app.core.security import get_password_hash
from app.models.user import User


async def ensure_admin_user(db_session_factory) -> None:
    admin_email = settings.ADMIN_EMAIL
    admin_password = settings.ADMIN_PASSWORD.get_secret_value() if settings.ADMIN_PASSWORD else None

    if not admin_email:
        return

    async with db_session_factory() as db:
        result = await db.execute(select(User).where(User.email == admin_email))
        existing = result.scalar_one_or_none()

        if existing:
            changed = False
            if not existing.is_admin:
                existing.is_admin = True
                changed = True
            if not existing.is_active:
                existing.is_active = True
                changed = True
            if changed:
                await db.commit()
            return

        if not admin_password:
            if settings.is_production:
                raise RuntimeError("ADMIN_PASSWORD is required in production for admin bootstrap")
            alphabet = string.ascii_letters + string.digits
            admin_password = "".join(secrets.choice(alphabet) for _ in range(12))
            print(f"[bootstrap] Generated admin password for {admin_email}: {admin_password}")

        user = User(
            email=admin_email,
            password_hash=get_password_hash(admin_password),
            is_admin=True,
            is_active=True,
        )
        db.add(user)
        await db.commit()
