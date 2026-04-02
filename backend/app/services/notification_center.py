from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import datetime, UTC

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user import User
from app.models.user_notification import UserNotification
from app.models.user_settings import UserSettings
from app.services.email_service import EmailService
from app.services.web_push_service import send_web_push_notifications_if_enabled


TAG_RE = re.compile(r"<[^>]+>")


def html_to_text(value: str) -> str:
    text = value.replace("<br/>", "\n").replace("<br>", "\n")
    text = TAG_RE.sub("", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


async def create_in_app_notification(
    db: AsyncSession,
    *,
    user_id: int,
    kind: str,
    title: str,
    body: str,
    action_url: str | None = None,
    severity: str = "info",
    is_important: bool = False,
) -> UserNotification:
    notification = UserNotification(
        user_id=user_id,
        kind=kind,
        title=title,
        body=body,
        action_url=action_url,
        severity=severity,
        is_important=is_important,
    )
    db.add(notification)
    await db.flush()
    return notification


async def create_in_app_notifications_for_users(
    db: AsyncSession,
    *,
    user_ids: Iterable[int],
    kind: str,
    title: str,
    body: str,
    action_url: str | None = None,
    severity: str = "info",
    is_important: bool = False,
) -> int:
    count = 0
    for user_id in set(user_ids):
        await create_in_app_notification(
            db,
            user_id=user_id,
            kind=kind,
            title=title,
            body=body,
            action_url=action_url,
            severity=severity,
            is_important=is_important,
        )
        count += 1
    return count


async def list_user_notifications(
    db: AsyncSession,
    *,
    user_id: int,
    limit: int = 50,
) -> tuple[list[UserNotification], int]:
    items = (
        await db.execute(
            select(UserNotification)
            .where(UserNotification.user_id == user_id)
            .order_by(UserNotification.created_at.desc(), UserNotification.id.desc())
            .limit(limit)
        )
    ).scalars().all()
    unread_count = await db.scalar(
        select(func.count(UserNotification.id)).where(
            UserNotification.user_id == user_id,
            UserNotification.read_at.is_(None),
        )
    ) or 0
    return items, int(unread_count)


async def mark_notification_read(db: AsyncSession, *, user_id: int, notification_id: int) -> bool:
    result = await db.execute(
        update(UserNotification)
        .where(
            UserNotification.id == notification_id,
            UserNotification.user_id == user_id,
            UserNotification.read_at.is_(None),
        )
        .values(read_at=datetime.now(UTC))
    )
    return bool(result.rowcount)


async def mark_all_notifications_read(db: AsyncSession, *, user_id: int) -> int:
    result = await db.execute(
        update(UserNotification)
        .where(
            UserNotification.user_id == user_id,
            UserNotification.read_at.is_(None),
        )
        .values(read_at=datetime.now(UTC))
    )
    return int(result.rowcount or 0)


async def get_unread_notification_count(db: AsyncSession, *, user_id: int) -> int:
    return int(
        await db.scalar(
            select(func.count(UserNotification.id)).where(
                UserNotification.user_id == user_id,
                UserNotification.read_at.is_(None),
            )
        ) or 0
    )


async def deliver_email_notification_if_enabled(
    *,
    user: User,
    settings: UserSettings | None,
    subject: str,
    html_text: str,
    plain_text: str | None = None,
    email_flag: str | None = None,
) -> bool:
    if settings is None or not settings.email_notifications_enabled:
        return False
    if email_flag and not getattr(settings, email_flag, False):
        return False
    email = (user.email or "").strip()
    if not email:
        return False
    text = plain_text or html_to_text(html_text)
    service = EmailService()
    return await service.send_message(
        to_email=email,
        subject=subject,
        text=text,
        html=f"<pre style=\"white-space:pre-wrap;font-family:Arial,sans-serif\">{text}</pre>",
    )


async def deliver_web_push_notification_if_enabled(
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
    return await send_web_push_notifications_if_enabled(
        db,
        user=user,
        settings=settings,
        title=title,
        plain_text=plain_text,
        action_url=action_url,
        kind=kind,
        severity=severity,
    )
