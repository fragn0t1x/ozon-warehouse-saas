from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_current_user
from app.database import SessionLocal
from app.models.user import User
from app.models.user_settings import UserSettings
from app.schemas.notification import (
    NotificationListResponse,
    NotificationUnreadCountResponse,
    UserNotificationResponse,
    WebPushStatusResponse,
    WebPushSubscriptionRequest,
    WebPushTestResponse,
    WebPushUnsubscribeRequest,
)
from app.services.notification_center import (
    get_unread_notification_count,
    list_user_notifications,
    mark_all_notifications_read,
    mark_notification_read,
)
from app.services.web_push_service import (
    build_web_push_status,
    remove_web_push_subscription,
    send_web_push_notifications_if_enabled,
    upsert_web_push_subscription,
)


router = APIRouter(prefix="/notifications", tags=["notifications"])


async def get_db():
    async with SessionLocal() as session:
        yield session


@router.get("", response_model=NotificationListResponse)
@router.get("/", response_model=NotificationListResponse, include_in_schema=False)
async def get_notifications(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    items, unread_count = await list_user_notifications(db, user_id=current_user.id)
    return NotificationListResponse(
        items=[UserNotificationResponse.model_validate(item) for item in items],
        unread_count=unread_count,
    )


@router.get("/unread-count", response_model=NotificationUnreadCountResponse)
async def get_notifications_unread_count(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    unread_count = await get_unread_notification_count(db, user_id=current_user.id)
    return NotificationUnreadCountResponse(unread_count=unread_count)


@router.post("/{notification_id}/read", response_model=NotificationUnreadCountResponse)
async def read_notification(
    notification_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    updated = await mark_notification_read(db, user_id=current_user.id, notification_id=notification_id)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Notification not found")
    await db.commit()
    unread_count = await get_unread_notification_count(db, user_id=current_user.id)
    return NotificationUnreadCountResponse(unread_count=unread_count)


@router.post("/read-all", response_model=NotificationUnreadCountResponse)
async def read_all_notifications(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    await mark_all_notifications_read(db, user_id=current_user.id)
    await db.commit()
    return NotificationUnreadCountResponse(unread_count=0)


async def _get_user_settings(db: AsyncSession, user_id: int) -> UserSettings | None:
    return (
        await db.execute(
            select(UserSettings).where(UserSettings.user_id == user_id)
        )
    ).scalar_one_or_none()


@router.get("/web-push/status", response_model=WebPushStatusResponse)
async def get_web_push_status(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    settings = await _get_user_settings(db, current_user.id)
    return await build_web_push_status(db, user_id=current_user.id, settings_row=settings)


@router.post("/web-push/subscribe", response_model=WebPushStatusResponse)
async def subscribe_web_push(
    payload: WebPushSubscriptionRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    await upsert_web_push_subscription(
        db,
        user_id=current_user.id,
        endpoint=payload.endpoint,
        p256dh_key=payload.p256dh_key,
        auth_key=payload.auth_key,
        user_agent=payload.user_agent,
    )
    await db.commit()
    settings = await _get_user_settings(db, current_user.id)
    return await build_web_push_status(db, user_id=current_user.id, settings_row=settings)


@router.post("/web-push/unsubscribe", response_model=WebPushStatusResponse)
async def unsubscribe_web_push(
    payload: WebPushUnsubscribeRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    await remove_web_push_subscription(db, user_id=current_user.id, endpoint=payload.endpoint)
    await db.commit()
    settings = await _get_user_settings(db, current_user.id)
    return await build_web_push_status(db, user_id=current_user.id, settings_row=settings)


@router.post("/web-push/test", response_model=WebPushTestResponse)
async def send_web_push_test(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    settings = await _get_user_settings(db, current_user.id)
    sent_count = await send_web_push_notifications_if_enabled(
        db,
        user=current_user,
        settings=settings,
        title="Тест web push",
        plain_text="Если ты видишь это уведомление, браузерный push в этом кабинете работает.",
        action_url="/settings",
        kind="web_push_test",
        severity="info",
    )
    await db.commit()
    return WebPushTestResponse(
        sent_count=sent_count,
        message=(
            "Тестовое уведомление отправлено."
            if sent_count > 0
            else "Тест не отправился. Сначала подключи браузер в настройках и разреши уведомления."
        ),
    )
