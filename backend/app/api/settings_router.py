# backend/app/api/settings_router.py
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import SessionLocal
from app.models.user import User
from app.schemas.user_settings import (
    TelegramConnectStatusResponse,
    UserSettingsCreate,
    UserSettingsResponse,
)
from app.services.settings_service import SettingsService
from app.services.telegram_linking import (
    create_telegram_connect_link,
    disconnect_telegram,
    get_telegram_connect_status,
)
from app.core.dependencies import get_current_user
from typing import Optional
from datetime import datetime

router = APIRouter(prefix="/settings", tags=["settings"])

async def get_db():
    async with SessionLocal() as session:
        yield session

@router.get("", response_model=UserSettingsResponse)
@router.get("/", response_model=UserSettingsResponse, include_in_schema=False)
async def get_settings(
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Получить настройки текущего пользователя"""
    service = SettingsService(db)
    settings = await service.get_settings(current_user.id)
    return settings

@router.put("", response_model=UserSettingsResponse)
@router.put("/", response_model=UserSettingsResponse, include_in_schema=False)
async def update_settings(
        settings_data: UserSettingsCreate,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Обновить настройки пользователя (полное обновление)"""
    service = SettingsService(db)
    return await service.update_settings(current_user.id, settings_data)

@router.patch("", response_model=UserSettingsResponse)
@router.patch("/", response_model=UserSettingsResponse, include_in_schema=False)
async def patch_settings(
        settings_data: UserSettingsCreate,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Частичное обновление настроек пользователя"""
    service = SettingsService(db)
    return await service.update_settings(current_user.id, settings_data)

@router.post("/first-login")
async def first_login_setup(
        settings_data: UserSettingsCreate,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Первоначальная настройка при первом входе"""
    service = SettingsService(db)
    return await service.first_login_setup(current_user.id, settings_data)


@router.post("/complete-onboarding", response_model=UserSettingsResponse)
async def complete_onboarding(
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Завершить онбординг после первого входа"""
    service = SettingsService(db)
    return await service.complete_onboarding(current_user.id)


@router.get("/first-login-status")
async def check_first_login(
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Проверить, первый ли это вход пользователя"""
    if current_user.is_admin:
        return {
            "is_first_login": False,
            "has_settings": True,
        }

    service = SettingsService(db)
    settings = await service.get_settings(current_user.id)
    return {
        "is_first_login": settings.is_first_login if settings.can_manage_business_settings else False,
        "has_settings": settings.warehouse_mode is not None
    }

@router.patch("/shipments-start-date")
async def update_shipments_start_date(
        start_date: Optional[datetime] = None,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Обновить дату начала учета отправок"""
    service = SettingsService(db)
    settings = await service.update_settings(
        current_user.id,
        UserSettingsCreate(shipments_start_date=start_date),
    )

    return {
        "status": "updated",
        "shipments_start_date": settings.shipments_start_date,
        "shipments_accounting_enabled": settings.shipments_accounting_enabled,
    }


@router.get("/telegram/connect-status", response_model=TelegramConnectStatusResponse)
async def telegram_connect_status(
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    return await get_telegram_connect_status(db, current_user.id)


@router.post("/telegram/connect", response_model=TelegramConnectStatusResponse)
async def telegram_connect(
        force_new: bool = Query(False),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    return await create_telegram_connect_link(db, current_user.id, force_new=force_new)


@router.post("/telegram/disconnect", response_model=TelegramConnectStatusResponse)
async def telegram_disconnect(
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    return await disconnect_telegram(db, user_id=current_user.id)
