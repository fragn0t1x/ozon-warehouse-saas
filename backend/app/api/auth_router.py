# backend/app/api/auth_router.py
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select

from app.config import settings
from app.core.dependencies import get_db, get_current_user, get_current_admin_user, get_current_owner_user
from app.core.security import (
    verify_password, get_password_hash,
    create_access_token, create_refresh_token,
    decode_token
)
from app.models.user import User
from app.models.store import Store
from app.models.user_notification import UserNotification
from app.models.web_push_subscription import WebPushSubscription
from app.models.warehouse import Warehouse
from app.models.warehouse_product import WarehouseProduct
from app.schemas.auth import (
    Token, LoginRequest, RefreshRequest,
    ChangePasswordRequest, TeamMemberCreate, UserCreate, UserResponse,
    AdminUserCreateResponse
)
from app.services.email_service import EmailService
from app.services.cabinet_access import get_cabinet_owner_id
from app.services.user_settings_helper import USER_SETTINGS_INSERT_DEFAULTS, get_or_create_user_settings
import secrets
import string

router = APIRouter(prefix="/auth", tags=["authentication"])


def _cookie_secure() -> bool:
    return bool(settings.is_production)


def _set_auth_cookies(response: Response, *, access_token: str, refresh_token: str) -> None:
    secure = _cookie_secure()
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=secure,
        samesite="lax",
        path="/",
        max_age=60 * int(settings.ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=secure,
        samesite="lax",
        path="/",
        max_age=60 * 60 * 24 * int(settings.REFRESH_TOKEN_EXPIRE_DAYS),
    )


def _clear_auth_cookies(response: Response) -> None:
    response.delete_cookie("access_token", path="/", samesite="lax")
    response.delete_cookie("refresh_token", path="/", samesite="lax")


@router.post("/login", response_model=Token)
async def login(
        login_data: LoginRequest,
        response: Response,
        db: AsyncSession = Depends(get_db)
):
    """Вход в систему"""
    # Ищем пользователя по email
    result = await db.execute(
        select(User).where(User.email == login_data.email)
    )
    user = result.scalar_one_or_none()

    if not user or not verify_password(login_data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )

    # Создаем токены
    access_token = create_access_token(
        data={"sub": str(user.id)}
    )
    refresh_token = create_refresh_token(
        data={"sub": str(user.id)}
    )
    _set_auth_cookies(response, access_token=access_token, refresh_token=refresh_token)

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer"
    }


@router.post("/refresh", response_model=Token)
async def refresh_token(
        refresh_data: RefreshRequest,
        request: Request,
        response: Response,
        db: AsyncSession = Depends(get_db)
):
    """Обновление access token по refresh token"""
    refresh_token_value = refresh_data.refresh_token or request.cookies.get("refresh_token")
    payload = decode_token(refresh_token_value or "")

    if not payload or payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token"
        )

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token"
        )

    # Проверяем, что пользователь существует
    user = await db.get(User, int(user_id))
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive"
        )

    # Создаем новые токены
    access_token = create_access_token(
        data={"sub": str(user.id)}
    )
    refresh_token = create_refresh_token(
        data={"sub": str(user.id)}
    )
    _set_auth_cookies(response, access_token=access_token, refresh_token=refresh_token)

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer"
    }


@router.post("/logout")
async def logout(response: Response):
    _clear_auth_cookies(response)
    return {"message": "Logged out"}


@router.post("/change-password")
async def change_password(
        password_data: ChangePasswordRequest,
        current_user: User = Depends(get_current_user),
        db: AsyncSession = Depends(get_db)
):
    """Смена пароля"""
    if not verify_password(password_data.old_password, current_user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Incorrect old password"
        )

    current_user.password_hash = get_password_hash(password_data.new_password)
    await db.commit()

    return {"message": "Password changed successfully"}


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
        current_user: User = Depends(get_current_user)
):
    """Информация о текущем пользователе"""
    return current_user


# Админские эндпоинты
@router.post("/admin/users", response_model=AdminUserCreateResponse)
async def create_user(
        user_data: UserCreate,
        db: AsyncSession = Depends(get_db),
        admin: User = Depends(get_current_admin_user)
):
    """Создание нового пользователя (только для админа)"""
    # Проверяем, не занят ли email
    result = await db.execute(
        select(User).where(User.email == user_data.email)
    )
    existing = result.scalar_one_or_none()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    # Создаем пользователя
    password = user_data.password
    generated_password = None
    if not password:
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        password = "".join(secrets.choice(alphabet) for _ in range(12))
        generated_password = password

    user = User(
        email=user_data.email,
        password_hash=get_password_hash(password),
        is_admin=user_data.is_admin,
        role="owner",
        is_active=True
    )

    db.add(user)
    await db.commit()
    await db.refresh(user)

    # Пробуем отправить письмо с паролем
    if generated_password:
        EmailService().send_password_email(user.email, generated_password)

    return {
        "user": user,
        "generated_password": generated_password
    }


@router.get("/admin/users", response_model=list[UserResponse])
async def get_users(
        db: AsyncSession = Depends(get_db),
        admin: User = Depends(get_current_admin_user)
):
    """Список всех пользователей (только для админа)"""
    result = await db.execute(select(User).order_by(User.created_at.desc(), User.email.asc()))
    users = result.scalars().all()
    return users


@router.patch("/admin/users/{user_id}/toggle-active")
async def toggle_user_active(
        user_id: int,
        db: AsyncSession = Depends(get_db),
        admin: User = Depends(get_current_admin_user)
):
    """Активация/деактивация пользователя (только для админа)"""
    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.id == admin.id:
        raise HTTPException(status_code=400, detail="You cannot deactivate your own admin account")

    user.is_active = not user.is_active
    await db.commit()

    return {
        "id": user.id,
        "email": user.email,
        "is_active": user.is_active
    }


@router.delete("/admin/users/{user_id}")
async def delete_user(
        user_id: int,
        db: AsyncSession = Depends(get_db),
        admin: User = Depends(get_current_admin_user)
):
    """Полное удаление пользователя и связанных данных (только для админа)"""
    target_user = await db.get(User, user_id)
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")

    if target_user.id == admin.id:
        raise HTTPException(status_code=400, detail="You cannot delete your own admin account")

    if target_user.is_admin:
        admin_count_result = await db.execute(
            select(func.count(User.id)).where(User.is_admin == True)  # noqa: E712
        )
        admin_count = int(admin_count_result.scalar() or 0)
        if admin_count <= 1:
            raise HTTPException(status_code=400, detail="You cannot delete the last admin account")

    cabinet_owner_id = target_user.owner_user_id or target_user.id
    cabinet_users_result = await db.execute(
        select(User)
        .where(
            User.is_admin == False,  # noqa: E712
            (User.id == cabinet_owner_id) | (User.owner_user_id == cabinet_owner_id),
        )
        .order_by(User.owner_user_id.is_not(None), User.id.desc())
    )
    cabinet_users = cabinet_users_result.scalars().all()

    if not cabinet_users:
        cabinet_users = [target_user]

    deleted_user_ids = [user.id for user in cabinet_users]
    deleted_user_emails = [user.email for user in cabinet_users]

    for user in cabinet_users:
        await db.delete(user)

    await db.commit()

    return {
        "deleted_user_ids": deleted_user_ids,
        "deleted_user_emails": deleted_user_emails,
        "deleted_count": len(deleted_user_ids),
    }


@router.post("/admin/users/{user_id}/reset")
async def reset_user(
        user_id: int,
        db: AsyncSession = Depends(get_db),
        admin: User = Depends(get_current_admin_user)
):
    """Сбросить кабинет пользователя до состояния первого входа"""
    target_user = await db.get(User, user_id)
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")

    if target_user.is_admin:
        raise HTTPException(status_code=400, detail="Admin account cannot be reset")

    if target_user.owner_user_id is not None or (target_user.role or "owner") != "owner":
        raise HTTPException(status_code=400, detail="Reset is available only for cabinet owners")

    owner = target_user

    cabinet_users_result = await db.execute(
        select(User)
        .where(
            User.is_admin == False,  # noqa: E712
            (User.id == owner.id) | (User.owner_user_id == owner.id),
        )
        .order_by(User.id.asc())
    )
    cabinet_users = cabinet_users_result.scalars().all()

    stores_result = await db.execute(select(Store).where(Store.user_id == owner.id))
    stores = stores_result.scalars().all()
    deleted_store_count = len(stores)
    for store in stores:
        await db.delete(store)

    await db.flush()

    warehouse_products_result = await db.execute(
        select(WarehouseProduct).where(WarehouseProduct.user_id == owner.id)
    )
    for warehouse_product in warehouse_products_result.scalars().all():
        await db.delete(warehouse_product)

    owner_settings, _ = await get_or_create_user_settings(db, owner.id)
    owner_settings.shared_warehouse_id = None
    await db.flush()

    warehouses_result = await db.execute(select(Warehouse).where(Warehouse.user_id == owner.id))
    for warehouse in warehouses_result.scalars().all():
        await db.delete(warehouse)

    cleared_notifications = 0
    cleared_push_subscriptions = 0

    for cabinet_user in cabinet_users:
        notifications_result = await db.execute(
            select(UserNotification).where(UserNotification.user_id == cabinet_user.id)
        )
        notifications = notifications_result.scalars().all()
        cleared_notifications += len(notifications)
        for notification in notifications:
            await db.delete(notification)

        subscriptions_result = await db.execute(
            select(WebPushSubscription).where(WebPushSubscription.user_id == cabinet_user.id)
        )
        subscriptions = subscriptions_result.scalars().all()
        cleared_push_subscriptions += len(subscriptions)
        for subscription in subscriptions:
            await db.delete(subscription)

        settings, _ = await get_or_create_user_settings(db, cabinet_user.id)
        for field_name, default_value in USER_SETTINGS_INSERT_DEFAULTS.items():
            setattr(settings, field_name, default_value)

        settings.shared_warehouse_id = None
        settings.shipments_start_date = None
        settings.shipments_accounting_enabled_at = None
        settings.telegram_chat_id = None

    await db.commit()

    return {
        "status": "reset",
        "user_id": owner.id,
        "email": owner.email,
        "deleted_stores": deleted_store_count,
        "cleared_notifications": cleared_notifications,
        "cleared_web_push_subscriptions": cleared_push_subscriptions,
        "cabinet_users_kept": len(cabinet_users),
    }


@router.get("/team/users", response_model=list[UserResponse])
async def get_team_users(
        db: AsyncSession = Depends(get_db),
        owner: User = Depends(get_current_owner_user)
):
    owner_id = get_cabinet_owner_id(owner)
    result = await db.execute(
        select(User)
        .where(
            User.is_admin == False,  # noqa: E712
            (User.id == owner_id) | (User.owner_user_id == owner_id),
        )
        .order_by(User.role.desc(), User.created_at.asc(), User.email.asc())
    )
    return result.scalars().all()


@router.post("/team/users", response_model=AdminUserCreateResponse)
async def create_team_user(
        user_data: TeamMemberCreate,
        db: AsyncSession = Depends(get_db),
        owner: User = Depends(get_current_owner_user)
):
    owner_id = get_cabinet_owner_id(owner)
    result = await db.execute(
        select(User).where(User.email == user_data.email)
    )
    existing = result.scalar_one_or_none()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    password = user_data.password
    generated_password = None
    if not password:
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        password = "".join(secrets.choice(alphabet) for _ in range(12))
        generated_password = password

    user = User(
        email=user_data.email,
        password_hash=get_password_hash(password),
        is_admin=False,
        role="member",
        owner_user_id=owner_id,
        is_active=True,
    )

    db.add(user)
    await db.commit()
    await db.refresh(user)

    if generated_password:
        EmailService().send_password_email(user.email, generated_password)

    return {
        "user": user,
        "generated_password": generated_password,
    }


@router.patch("/team/users/{user_id}/toggle-active")
async def toggle_team_user_active(
        user_id: int,
        db: AsyncSession = Depends(get_db),
        owner: User = Depends(get_current_owner_user)
):
    owner_id = get_cabinet_owner_id(owner)
    user = await db.get(User, user_id)
    if not user or user.is_admin:
        raise HTTPException(status_code=404, detail="User not found")

    if user.id == owner_id:
        raise HTTPException(status_code=400, detail="You cannot deactivate the owner account")

    if user.owner_user_id != owner_id:
        raise HTTPException(status_code=403, detail="This user does not belong to your cabinet")

    user.is_active = not user.is_active
    await db.commit()

    return {
        "id": user.id,
        "email": user.email,
        "is_active": user.is_active
    }
