from __future__ import annotations

import json
from datetime import datetime, UTC

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings as app_settings
from app.models.user import User
from app.models.user_settings import UserSettings
from app.models.web_push_subscription import WebPushSubscription
from app.services.user_settings_helper import get_or_create_user_settings

try:
    from pywebpush import WebPushException, webpush
except Exception:  # pragma: no cover - graceful runtime fallback
    WebPushException = Exception
    webpush = None


def is_web_push_configured() -> bool:
    return bool(
        app_settings.WEB_PUSH_VAPID_PUBLIC_KEY
        and app_settings.WEB_PUSH_VAPID_PRIVATE_KEY
        and app_settings.WEB_PUSH_VAPID_SUBJECT
    )


def is_web_push_runtime_ready() -> bool:
    return is_web_push_configured() and webpush is not None


def get_web_push_status_message() -> str | None:
    if not is_web_push_configured():
        return "Web push еще не настроен на сервере. Добавьте VAPID-ключи в .env."
    if webpush is None:
        return "На сервере еще не установлен модуль web push. Пересоберите backend после обновления зависимостей."
    return "Можно подключить браузерные push-уведомления."


async def _get_or_create_settings(db: AsyncSession, user_id: int) -> UserSettings:
    settings_row, _changed = await get_or_create_user_settings(db, user_id)
    return settings_row


async def _get_subscription_count(db: AsyncSession, user_id: int) -> int:
    return int(
        await db.scalar(
            select(func.count(WebPushSubscription.id)).where(WebPushSubscription.user_id == user_id)
        )
        or 0
    )


async def build_web_push_status(
    db: AsyncSession,
    *,
    user_id: int,
    settings_row: UserSettings | None = None,
) -> dict:
    current_settings = settings_row or await _get_or_create_settings(db, user_id)
    subscription_count = await _get_subscription_count(db, user_id)
    return {
        "configured": is_web_push_configured(),
        "library_available": webpush is not None,
        "enabled": bool(current_settings.web_push_notifications_enabled and subscription_count > 0),
        "subscription_count": subscription_count,
        "public_key": app_settings.WEB_PUSH_VAPID_PUBLIC_KEY if is_web_push_configured() else None,
        "message": get_web_push_status_message(),
    }


async def upsert_web_push_subscription(
    db: AsyncSession,
    *,
    user_id: int,
    endpoint: str,
    p256dh_key: str,
    auth_key: str,
    user_agent: str | None = None,
) -> WebPushSubscription:
    settings_row = await _get_or_create_settings(db, user_id)
    subscription = (
        await db.execute(
            select(WebPushSubscription).where(WebPushSubscription.endpoint == endpoint)
        )
    ).scalar_one_or_none()

    if subscription is None:
        subscription = WebPushSubscription(
            user_id=user_id,
            endpoint=endpoint,
            p256dh_key=p256dh_key,
            auth_key=auth_key,
            user_agent=user_agent,
            last_seen_at=datetime.now(UTC),
        )
        db.add(subscription)
    else:
        subscription.user_id = user_id
        subscription.p256dh_key = p256dh_key
        subscription.auth_key = auth_key
        subscription.user_agent = user_agent
        subscription.last_seen_at = datetime.now(UTC)

    settings_row.web_push_notifications_enabled = True
    await db.flush()
    return subscription


async def remove_web_push_subscription(
    db: AsyncSession,
    *,
    user_id: int,
    endpoint: str | None = None,
) -> int:
    settings_row = await _get_or_create_settings(db, user_id)
    stmt = delete(WebPushSubscription).where(WebPushSubscription.user_id == user_id)
    if endpoint:
        stmt = stmt.where(WebPushSubscription.endpoint == endpoint)

    result = await db.execute(stmt)
    remaining = await _get_subscription_count(db, user_id)
    if remaining == 0:
        settings_row.web_push_notifications_enabled = False
    await db.flush()
    return int(result.rowcount or 0)


def _truncate_push_text(value: str, limit: int = 220) -> str:
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return f"{compact[: limit - 1].rstrip()}…"


async def send_web_push_notifications_if_enabled(
    db: AsyncSession,
    *,
    user: User,
    settings: UserSettings | None,
    title: str,
    plain_text: str,
    action_url: str | None = None,
    kind: str = "general",
    severity: str = "info",
) -> int:
    if settings is None or not settings.web_push_notifications_enabled:
        return 0
    if not is_web_push_runtime_ready():
        return 0

    subscriptions = (
        await db.execute(
            select(WebPushSubscription).where(WebPushSubscription.user_id == user.id)
        )
    ).scalars().all()
    if not subscriptions:
        return 0

    payload = json.dumps(
        {
            "title": title,
            "body": _truncate_push_text(plain_text),
            "url": action_url or "/notifications",
            "kind": kind,
            "severity": severity,
            "tag": kind,
        },
        ensure_ascii=False,
    )

    vapid_private_key = (
        app_settings.WEB_PUSH_VAPID_PRIVATE_KEY.get_secret_value()
        if app_settings.WEB_PUSH_VAPID_PRIVATE_KEY
        else None
    )
    vapid_claims = {"sub": app_settings.WEB_PUSH_VAPID_SUBJECT} if app_settings.WEB_PUSH_VAPID_SUBJECT else None
    if not vapid_private_key or not vapid_claims:
        return 0

    sent_count = 0
    stale_ids: list[int] = []
    for subscription in subscriptions:
        try:
            webpush(
                subscription_info={
                    "endpoint": subscription.endpoint,
                    "keys": {
                        "p256dh": subscription.p256dh_key,
                        "auth": subscription.auth_key,
                    },
                },
                data=payload,
                vapid_private_key=vapid_private_key,
                vapid_claims=vapid_claims,
            )
            subscription.last_sent_at = datetime.now(UTC)
            subscription.last_seen_at = datetime.now(UTC)
            sent_count += 1
        except WebPushException as exc:  # pragma: no cover - network dependent
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code in {404, 410}:
                stale_ids.append(subscription.id)
        except Exception:  # pragma: no cover - network dependent
            continue

    if stale_ids:
        await db.execute(
            delete(WebPushSubscription).where(WebPushSubscription.id.in_(stale_ids))
        )
        remaining = await _get_subscription_count(db, user.id)
        if remaining == 0 and settings:
            settings.web_push_notifications_enabled = False
    await db.flush()
    return sent_count
