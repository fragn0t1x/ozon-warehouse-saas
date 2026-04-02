from __future__ import annotations

import html
import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user import User
from app.models.user_settings import UserSettings
from app.config import settings
from app.models.supply_notification_event import SupplyNotificationEvent
from app.services.notification_center import create_in_app_notifications_for_users
from app.services.telegram_service import TelegramService
from app.utils.redis_cache import cache_get_json, cache_set_json, get_redis


ADMIN_EVENT_LIST_KEY = "admin:events"
ADMIN_EVENT_MAX_ITEMS = 50


def _clip(value: Any, limit: int = 800) -> str:
    if value is None:
        return "-"
    if isinstance(value, Exception):
        message = str(value).strip()
        if message:
            text = f"{type(value).__name__}: {message}"
        else:
            text = type(value).__name__
    elif isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False, default=str)
    else:
        text = str(value)
    if len(text) <= limit:
        return text
    return f"{text[:limit]}..."


def _clip_html(value: Any, limit: int = 800) -> str:
    return html.escape(_clip(value, limit))


def _format_duration(seconds: float | int | None) -> str | None:
    if seconds is None:
        return None
    total_seconds = max(int(round(float(seconds))), 0)
    minutes, secs = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}ч {minutes}м {secs}с"
    if minutes:
        return f"{minutes}м {secs}с"
    return f"{secs}с"


async def send_admin_message(
    text: str,
    *,
    dedupe_key: str | None = None,
    dedupe_ttl_seconds: int = 300,
) -> bool:
    chat_id = settings.TELEGRAM_CHAT_ID
    if not chat_id:
        logger.warning("Админский TELEGRAM_CHAT_ID не настроен, сообщение пропущено")
        return False

    cache_key: str | None = None
    dedupe_reserved = False
    if dedupe_key:
        cache_key = f"admin-alert:{hashlib.sha1(dedupe_key.encode('utf-8')).hexdigest()}"
        redis = await get_redis()
        if redis:
            try:
                dedupe_reserved = bool(
                    await redis.set(
                        cache_key,
                        json.dumps({"pending": True}, ensure_ascii=False),
                        ex=dedupe_ttl_seconds,
                        nx=True,
                    )
                )
            except Exception as e:
                logger.error("Не удалось поставить dedupe-lock для admin alert: {}", e)
            else:
                if not dedupe_reserved:
                    logger.info("Пропускаем дублирующее админское уведомление: {}", dedupe_key)
                    return False
        elif await cache_get_json(cache_key) is not None:
            logger.info("Пропускаем дублирующее админское уведомление: {}", dedupe_key)
            return False

    telegram = TelegramService()
    try:
        delivered = await telegram.send_message(chat_id, text)
        if delivered and dedupe_key and cache_key:
            await cache_set_json(cache_key, {"sent": True}, dedupe_ttl_seconds)
        elif not delivered and dedupe_reserved and cache_key:
            redis = await get_redis()
            if redis:
                try:
                    await redis.delete(cache_key)
                except Exception as e:
                    logger.error("Не удалось снять dedupe-lock для admin alert: {}", e)
        return delivered
    finally:
        await telegram.close()


async def send_admin_broadcast_message(
    db: AsyncSession,
    text: str,
    *,
    dedupe_key: str | None = None,
    dedupe_ttl_seconds: int = 300,
) -> bool:
    cache_key: str | None = None
    dedupe_reserved = False
    if dedupe_key:
        cache_key = f"admin-alert:{hashlib.sha1(dedupe_key.encode('utf-8')).hexdigest()}"
        redis = await get_redis()
        if redis:
            try:
                dedupe_reserved = bool(
                    await redis.set(
                        cache_key,
                        json.dumps({"pending": True}, ensure_ascii=False),
                        ex=dedupe_ttl_seconds,
                        nx=True,
                    )
                )
            except Exception as e:
                logger.error("Не удалось поставить dedupe-lock для admin broadcast: {}", e)
            else:
                if not dedupe_reserved:
                    logger.info("Пропускаем дублирующий admin broadcast: {}", dedupe_key)
                    return False
        elif await cache_get_json(cache_key) is not None:
            logger.info("Пропускаем дублирующий admin broadcast: {}", dedupe_key)
            return False

    result = await db.execute(
        select(UserSettings.telegram_chat_id)
        .join(User, User.id == UserSettings.user_id)
        .where(
            User.is_admin == True,  # noqa: E712
            User.is_active == True,  # noqa: E712
            UserSettings.telegram_chat_id.is_not(None),
        )
    )
    chat_ids = {str(chat_id).strip() for chat_id in result.scalars().all() if str(chat_id).strip()}
    if settings.TELEGRAM_CHAT_ID:
        chat_ids.add(str(settings.TELEGRAM_CHAT_ID).strip())

    if not chat_ids:
        logger.warning("Не найдено admin Telegram chat_id для broadcast, сообщение пропущено")
        return False

    telegram = TelegramService()
    delivered_any = False
    try:
        for chat_id in sorted(chat_ids):
            try:
                delivered = await telegram.send_message(chat_id, text)
            except Exception as e:
                logger.error("Не удалось отправить admin broadcast в Telegram {}: {}", chat_id, e)
                delivered = False
            delivered_any = delivered_any or delivered
        if delivered_any and dedupe_key and cache_key:
            await cache_set_json(cache_key, {"sent": True}, dedupe_ttl_seconds)
        elif not delivered_any and dedupe_reserved and cache_key:
            redis = await get_redis()
            if redis:
                try:
                    await redis.delete(cache_key)
                except Exception as e:
                    logger.error("Не удалось снять dedupe-lock для admin broadcast: {}", e)
        return delivered_any
    finally:
        await telegram.close()


async def record_admin_event(
    event_type: str,
    title: str,
    *,
    severity: str,
    details: dict[str, Any] | None = None,
) -> None:
    redis = await get_redis()
    if not redis:
        return

    payload = {
        "event_type": event_type,
        "title": title,
        "severity": severity,
        "details": details or {},
    }
    try:
        await redis.lpush(ADMIN_EVENT_LIST_KEY, json.dumps(payload, ensure_ascii=False, default=str))
        await redis.ltrim(ADMIN_EVENT_LIST_KEY, 0, ADMIN_EVENT_MAX_ITEMS - 1)
    except Exception as e:
        logger.error("Не удалось записать admin event: {}", e)


async def get_recent_admin_events(limit: int = 10) -> list[dict[str, Any]]:
    redis = await get_redis()
    if not redis:
        return []
    try:
        raw_items = await redis.lrange(ADMIN_EVENT_LIST_KEY, 0, max(limit - 1, 0))
        return [json.loads(item) for item in raw_items]
    except Exception as e:
        logger.error("Не удалось прочитать admin events: {}", e)
        return []


async def notify_backend_error(source: str, error: Exception | str, *, details: dict[str, Any] | None = None) -> bool:
    details = details or {}
    await record_admin_event("backend_error", f"Ошибка backend: {source}", severity="error", details=details | {"error": _clip(error, 400)})
    text = (
        "🚨 <b>Ошибка backend</b>\n\n"
        f"<b>Источник:</b> {_clip_html(source, 300)}\n"
        f"<b>Ошибка:</b> {_clip_html(error, 1200)}\n"
    )
    if details:
        rendered = "\n".join(
            f"• <b>{html.escape(str(key))}:</b> {_clip_html(value, 400)}"
            for key, value in details.items()
        )
        text += f"\n<b>Детали:</b>\n{rendered}"

    dedupe_key = f"backend:{source}:{type(error).__name__ if isinstance(error, Exception) else 'text'}:{_clip(error, 200)}"
    return await send_admin_message(text, dedupe_key=dedupe_key)


async def _active_admin_user_ids(db: AsyncSession) -> list[int]:
    result = await db.execute(
        select(User.id).where(
            User.is_admin == True,  # noqa: E712
            User.is_active == True,  # noqa: E712
        )
    )
    return [int(user_id) for user_id in result.scalars().all()]


async def notify_closed_month_issue(
    db: AsyncSession,
    *,
    store_id: int,
    store_name: str,
    month: str,
    issue_type: str,
    title: str,
    summary: str,
    details: list[str] | None = None,
    action_url: str = "/closed-months",
) -> bool:
    issue_key = f"{store_id}:{month}:{issue_type}:{summary}"
    dedupe_key = f"closed-month:{hashlib.sha1(issue_key.encode('utf-8')).hexdigest()}"
    admin_user_ids = await _active_admin_user_ids(db)

    body_lines = [
        f"Магазин: {store_name}",
        f"Store ID: {store_id}",
        f"Месяц: {month}",
        summary,
    ]
    for line in (details or [])[:5]:
        body_lines.append(f"• {line}")
    body = "\n".join(body_lines)

    if admin_user_ids:
        await create_in_app_notifications_for_users(
            db,
            user_ids=admin_user_ids,
            kind="closed_month_issue",
            title=title,
            body=body,
            action_url=action_url,
            severity="warning" if issue_type == "ozon_warning" else "error",
            is_important=True,
        )

    await record_admin_event(
        "closed_month_issue",
        title,
        severity="warning" if issue_type == "ozon_warning" else "error",
        details={
            "store_id": store_id,
            "store_name": store_name,
            "month": month,
            "issue_type": issue_type,
            "summary": summary,
            "details": details or [],
        },
    )

    telegram_text = (
        "⚠️ <b>Проблема в закрытом месяце</b>\n\n"
        f"<b>Магазин:</b> {_clip_html(store_name, 200)}\n"
        f"<b>Store ID:</b> {store_id}\n"
        f"<b>Месяц:</b> {_clip_html(month, 20)}\n"
        f"<b>Тип:</b> {_clip_html(issue_type, 40)}\n"
        f"<b>Суть:</b> {_clip_html(summary, 600)}\n"
    )
    if details:
        rendered = "\n".join(f"• {_clip_html(item, 400)}" for item in details[:5])
        telegram_text += f"\n<b>Детали:</b>\n{rendered}"

    return await send_admin_broadcast_message(
        db,
        telegram_text,
        dedupe_key=dedupe_key,
        dedupe_ttl_seconds=6 * 60 * 60,
    )


def _format_timeslot_range(timeslot_from: datetime | None, timeslot_to: datetime | None) -> str:
    if not timeslot_from:
        return "Не указан"
    from_label = timeslot_from.strftime("%d.%m.%Y %H:%M")
    if not timeslot_to:
        return from_label
    return f"{from_label} - {timeslot_to.strftime('%H:%M')}"


def _supply_status_label(status: str | None) -> str:
    return {
        "DATA_FILLING": "Подготовка к поставкам",
        "READY_TO_SUPPLY": "Готова к отгрузке",
        "ACCEPTED_AT_SUPPLY_WAREHOUSE": "Принята на точке отгрузки",
        "IN_TRANSIT": "В пути",
        "COMPLETED": "Завершена",
        "CANCELLED": "Отменена",
        "REJECTED_AT_SUPPLY_WAREHOUSE": "Отказано в приемке",
        "ACCEPTANCE_AT_STORAGE_WAREHOUSE": "На приемке на складе Ozon",
        "REPORTS_CONFIRMATION_AWAITING": "Ожидает подтверждения актов",
        "REPORT_REJECTED": "Акт приемки отклонен",
        "OVERDUE": "Просрочена",
        None: "-",
        "": "-",
        "-": "-",
    }.get(status, status or "-")


def _format_supply_status(status: str | None) -> str:
    label = _supply_status_label(status)
    if not status or status == "-" or label == status:
        return label
    return f"{label} ({status})"


def _should_notify_timeslot_change(old_status: str | None, new_status: str | None) -> bool:
    immutable_statuses = {
        "ACCEPTED_AT_SUPPLY_WAREHOUSE",
        "IN_TRANSIT",
        "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
        "REPORTS_CONFIRMATION_AWAITING",
        "REPORT_REJECTED",
        "REJECTED_AT_SUPPLY_WAREHOUSE",
        "COMPLETED",
        "CANCELLED",
    }
    return (old_status not in immutable_statuses) and (new_status not in immutable_statuses)


def _build_supply_identity_block(event: SupplyNotificationEvent) -> str:
    return (
        f"<b>Магазин:</b> {_clip_html(event.store_name, 200)}\n"
        f"<b>Store ID:</b> {event.store_id}\n"
        f"<b>Поставка:</b> №{_clip_html(event.order_number or event.supply_id, 200)}\n"
        f"<b>Supply ID:</b> {event.supply_id}\n"
    )


def _build_supply_created_message(event: SupplyNotificationEvent) -> str:
    text = (
        "🆕 <b>Новая поставка</b>\n\n"
        f"{_build_supply_identity_block(event)}"
        f"<b>Статус:</b> {_clip_html(_format_supply_status(event.status_after), 120)}\n"
        f"<b>Таймслот:</b> {_clip_html(_format_timeslot_range(event.timeslot_from, event.timeslot_to), 120)}\n"
    )
    if event.user_email:
        text += f"<b>Пользователь:</b> {_clip_html(event.user_email, 200)}\n"
    return text


def _build_supply_status_changed_message(event: SupplyNotificationEvent) -> str:
    text = (
        "🔄 <b>Поставка обновлена</b>\n\n"
        f"{_build_supply_identity_block(event)}"
        f"<b>Было:</b> {_clip_html(_format_supply_status(event.status_before), 120)}\n"
        f"<b>Стало:</b> {_clip_html(_format_supply_status(event.status_after), 120)}\n"
        f"<b>Таймслот:</b> {_clip_html(_format_timeslot_range(event.timeslot_from, event.timeslot_to), 120)}\n"
    )
    if event.user_email:
        text += f"<b>Пользователь:</b> {_clip_html(event.user_email, 200)}\n"
    return text


def _build_supply_timeslot_changed_message(event: SupplyNotificationEvent) -> str:
    text = (
        "🕒 <b>Изменился таймслот поставки</b>\n\n"
        f"{_build_supply_identity_block(event)}"
        f"<b>Статус:</b> {_clip_html(_format_supply_status(event.status_after), 120)}\n"
        f"<b>Было:</b> {_clip_html(_format_timeslot_range(event.old_timeslot_from, event.old_timeslot_to), 120)}\n"
        f"<b>Стало:</b> {_clip_html(_format_timeslot_range(event.timeslot_from, event.timeslot_to), 120)}\n"
    )
    if event.user_email:
        text += f"<b>Пользователь:</b> {_clip_html(event.user_email, 200)}\n"
    return text


def _build_supply_event_message(event: SupplyNotificationEvent) -> str:
    if event.event_type == "supply_created":
        return _build_supply_created_message(event)
    if event.event_type == "supply_status_changed":
        return _build_supply_status_changed_message(event)
    if event.event_type == "supply_timeslot_changed":
        return _build_supply_timeslot_changed_message(event)
    raise ValueError(f"Unsupported supply notification event type: {event.event_type}")


async def _create_supply_notification_event(
    db: AsyncSession,
    *,
    event_type: str,
    dedupe_key: str,
    supply_id: int,
    order_number: str,
    store_id: int,
    store_name: str,
    user_email: str | None = None,
    status_before: str | None = None,
    status_after: str | None = None,
    timeslot_from: datetime | None = None,
    timeslot_to: datetime | None = None,
    old_timeslot_from: datetime | None = None,
    old_timeslot_to: datetime | None = None,
) -> int | None:
    existing_id = await db.scalar(
        select(SupplyNotificationEvent.id).where(SupplyNotificationEvent.dedupe_key == dedupe_key)
    )
    if existing_id is not None:
        return None

    event = SupplyNotificationEvent(
        supply_id=supply_id,
        event_type=event_type,
        dedupe_key=dedupe_key,
        order_number=str(order_number or supply_id),
        store_id=store_id,
        store_name=store_name,
        user_email=user_email,
        status_before=status_before,
        status_after=status_after,
        timeslot_from=timeslot_from,
        timeslot_to=timeslot_to,
        old_timeslot_from=old_timeslot_from,
        old_timeslot_to=old_timeslot_to,
    )
    db.add(event)
    await db.flush()
    return event.id


async def queue_supply_created(
    db: AsyncSession,
    *,
    supply_id: int,
    order_number: str,
    store_id: int,
    store_name: str,
    status: str,
    user_email: str | None = None,
    timeslot_from: datetime | None = None,
    timeslot_to: datetime | None = None,
) -> int | None:
    await record_admin_event(
        "supply_created",
        f"Создана поставка {order_number or supply_id}",
        severity="info",
        details={
            "supply_id": supply_id,
            "order_number": order_number,
            "store_id": store_id,
            "store_name": store_name,
            "status": status,
            "user_email": user_email,
            "timeslot": _format_timeslot_range(timeslot_from, timeslot_to),
        },
    )
    dedupe_key = f"supply-created:{supply_id}:{status}:{timeslot_from.isoformat() if timeslot_from else '-'}"
    return await _create_supply_notification_event(
        db,
        event_type="supply_created",
        dedupe_key=dedupe_key,
        supply_id=supply_id,
        order_number=order_number,
        store_id=store_id,
        store_name=store_name,
        user_email=user_email,
        status_after=status,
        timeslot_from=timeslot_from,
        timeslot_to=timeslot_to,
    )


async def queue_supply_status_changed(
    db: AsyncSession,
    *,
    supply_id: int,
    order_number: str,
    store_id: int,
    store_name: str,
    old_status: str,
    new_status: str,
    user_email: str | None = None,
    timeslot_from: datetime | None = None,
    timeslot_to: datetime | None = None,
) -> int | None:
    await record_admin_event(
        "supply_status_changed",
        f"Поставка {order_number or supply_id}: {old_status} -> {new_status}",
        severity="warning",
        details={
            "supply_id": supply_id,
            "order_number": order_number,
            "store_id": store_id,
            "store_name": store_name,
            "old_status": old_status,
            "new_status": new_status,
            "user_email": user_email,
            "timeslot": _format_timeslot_range(timeslot_from, timeslot_to),
        },
    )
    dedupe_key = (
        f"supply-status:{supply_id}:{old_status}:{new_status}:"
        f"{timeslot_from.isoformat() if timeslot_from else '-'}"
    )
    return await _create_supply_notification_event(
        db,
        event_type="supply_status_changed",
        dedupe_key=dedupe_key,
        supply_id=supply_id,
        order_number=order_number,
        store_id=store_id,
        store_name=store_name,
        user_email=user_email,
        status_before=old_status,
        status_after=new_status,
        timeslot_from=timeslot_from,
        timeslot_to=timeslot_to,
    )


async def queue_supply_timeslot_changed(
    db: AsyncSession,
    *,
    supply_id: int,
    order_number: str,
    store_id: int,
    store_name: str,
    status: str,
    old_timeslot_from: datetime | None = None,
    old_timeslot_to: datetime | None = None,
    new_timeslot_from: datetime | None = None,
    new_timeslot_to: datetime | None = None,
    user_email: str | None = None,
) -> int | None:
    await record_admin_event(
        "supply_timeslot_changed",
        f"Поставка {order_number or supply_id}: изменился таймслот",
        severity="warning",
        details={
            "supply_id": supply_id,
            "order_number": order_number,
            "store_id": store_id,
            "store_name": store_name,
            "status": status,
            "user_email": user_email,
            "old_timeslot": _format_timeslot_range(old_timeslot_from, old_timeslot_to),
            "new_timeslot": _format_timeslot_range(new_timeslot_from, new_timeslot_to),
        },
    )
    dedupe_key = (
        f"supply-timeslot:{supply_id}:"
        f"{old_timeslot_from.isoformat() if old_timeslot_from else '-'}:"
        f"{new_timeslot_from.isoformat() if new_timeslot_from else '-'}"
    )
    return await _create_supply_notification_event(
        db,
        event_type="supply_timeslot_changed",
        dedupe_key=dedupe_key,
        supply_id=supply_id,
        order_number=order_number,
        store_id=store_id,
        store_name=store_name,
        user_email=user_email,
        status_after=status,
        timeslot_from=new_timeslot_from,
        timeslot_to=new_timeslot_to,
        old_timeslot_from=old_timeslot_from,
        old_timeslot_to=old_timeslot_to,
    )


async def deliver_pending_supply_notification_events(
    db: AsyncSession,
    *,
    limit: int = 100,
    event_ids: set[int] | list[int] | tuple[int, ...] | None = None,
) -> tuple[int, int]:
    stmt = select(SupplyNotificationEvent).where(SupplyNotificationEvent.telegram_sent_at.is_(None))
    if event_ids:
        stmt = stmt.where(SupplyNotificationEvent.id.in_(list(event_ids)))
    stmt = stmt.order_by(SupplyNotificationEvent.created_at.asc()).limit(limit)

    result = await db.execute(stmt)
    events = result.scalars().all()

    if not events:
        return 0, 0

    sent_count = 0
    failed_count = 0

    for event in events:
        event.attempts = int(event.attempts or 0) + 1
        try:
            delivered = await send_admin_message(
                _build_supply_event_message(event),
                dedupe_key=event.dedupe_key,
                dedupe_ttl_seconds=60 * 60 * 24 * 30,
            )
        except Exception as e:
            delivered = False
            event.last_error = _clip(e, 800)

        if delivered:
            event.telegram_sent_at = datetime.now(UTC)
            event.last_error = None
            sent_count += 1
        else:
            event.last_error = event.last_error or "telegram_send_failed"
            failed_count += 1

    await db.commit()
    return sent_count, failed_count


async def notify_supply_created(
    *,
    supply_id: int,
    order_number: str,
    store_id: int,
    store_name: str,
    status: str,
    user_email: str | None = None,
    timeslot_from: datetime | None = None,
    timeslot_to: datetime | None = None,
) -> bool:
    event = SupplyNotificationEvent(
        supply_id=supply_id,
        event_type="supply_created",
        dedupe_key=f"supply-created:{supply_id}:{status}:{timeslot_from.isoformat() if timeslot_from else '-'}",
        order_number=str(order_number or supply_id),
        store_id=store_id,
        store_name=store_name,
        user_email=user_email,
        status_after=status,
        timeslot_from=timeslot_from,
        timeslot_to=timeslot_to,
    )
    await record_admin_event(
        "supply_created",
        f"Создана поставка {order_number or supply_id}",
        severity="info",
        details={
            "supply_id": supply_id,
            "order_number": order_number,
            "store_id": store_id,
            "store_name": store_name,
            "status": status,
            "user_email": user_email,
            "timeslot": _format_timeslot_range(timeslot_from, timeslot_to),
        },
    )
    return await send_admin_message(_build_supply_created_message(event), dedupe_key=event.dedupe_key, dedupe_ttl_seconds=3600)


async def notify_supply_status_changed(
    *,
    supply_id: int,
    order_number: str,
    store_id: int,
    store_name: str,
    old_status: str,
    new_status: str,
    user_email: str | None = None,
    timeslot_from: datetime | None = None,
    timeslot_to: datetime | None = None,
) -> bool:
    event = SupplyNotificationEvent(
        supply_id=supply_id,
        event_type="supply_status_changed",
        dedupe_key=(
            f"supply-status:{supply_id}:{old_status}:{new_status}:"
            f"{timeslot_from.isoformat() if timeslot_from else '-'}"
        ),
        order_number=str(order_number or supply_id),
        store_id=store_id,
        store_name=store_name,
        user_email=user_email,
        status_before=old_status,
        status_after=new_status,
        timeslot_from=timeslot_from,
        timeslot_to=timeslot_to,
    )
    await record_admin_event(
        "supply_status_changed",
        f"Поставка {order_number or supply_id}: {old_status} -> {new_status}",
        severity="warning",
        details={
            "supply_id": supply_id,
            "order_number": order_number,
            "store_id": store_id,
            "store_name": store_name,
            "old_status": old_status,
            "new_status": new_status,
            "user_email": user_email,
            "timeslot": _format_timeslot_range(timeslot_from, timeslot_to),
        },
    )
    return await send_admin_message(_build_supply_status_changed_message(event), dedupe_key=event.dedupe_key, dedupe_ttl_seconds=3600)


async def notify_supply_timeslot_changed(
    *,
    supply_id: int,
    order_number: str,
    store_id: int,
    store_name: str,
    status: str,
    old_timeslot_from: datetime | None = None,
    old_timeslot_to: datetime | None = None,
    new_timeslot_from: datetime | None = None,
    new_timeslot_to: datetime | None = None,
    user_email: str | None = None,
) -> bool:
    event = SupplyNotificationEvent(
        supply_id=supply_id,
        event_type="supply_timeslot_changed",
        dedupe_key=(
            f"supply-timeslot:{supply_id}:"
            f"{old_timeslot_from.isoformat() if old_timeslot_from else '-'}:"
            f"{new_timeslot_from.isoformat() if new_timeslot_from else '-'}"
        ),
        order_number=str(order_number or supply_id),
        store_id=store_id,
        store_name=store_name,
        user_email=user_email,
        status_after=status,
        timeslot_from=new_timeslot_from,
        timeslot_to=new_timeslot_to,
        old_timeslot_from=old_timeslot_from,
        old_timeslot_to=old_timeslot_to,
    )
    await record_admin_event(
        "supply_timeslot_changed",
        f"Поставка {order_number or supply_id}: изменился таймслот",
        severity="warning",
        details={
            "supply_id": supply_id,
            "order_number": order_number,
            "store_id": store_id,
            "store_name": store_name,
            "status": status,
            "user_email": user_email,
            "old_timeslot": _format_timeslot_range(old_timeslot_from, old_timeslot_to),
            "new_timeslot": _format_timeslot_range(new_timeslot_from, new_timeslot_to),
        },
    )
    return await send_admin_message(_build_supply_timeslot_changed_message(event), dedupe_key=event.dedupe_key, dedupe_ttl_seconds=3600)


async def notify_ozon_api_error(
    endpoint: str,
    client_id: str,
    *,
    error: Exception | str,
    payload: dict[str, Any] | None = None,
    status_code: int | None = None,
    response_text: str | None = None,
) -> bool:
    await record_admin_event(
        "ozon_api_error",
        f"Ozon API: {endpoint}",
        severity="error",
        details={
            "client_id": client_id,
            "status_code": status_code,
            "error": _clip(error, 400),
        },
    )
    text = (
        "📡 <b>Проблема с Ozon API</b>\n\n"
        f"<b>Метод:</b> {endpoint}\n"
        f"<b>Client-Id:</b> {client_id}\n"
    )
    if status_code is not None:
        text += f"<b>HTTP статус:</b> {status_code}\n"
    text += f"<b>Ошибка:</b> {_clip(error, 1000)}\n"
    if payload:
        text += f"\n<b>Payload:</b>\n<code>{_clip(payload, 1200)}</code>\n"
    if response_text:
        text += f"\n<b>Ответ:</b>\n<code>{_clip(response_text, 1200)}</code>"

    dedupe_key = f"ozon:{endpoint}:{status_code}:{_clip(error, 200)}"
    return await send_admin_message(text, dedupe_key=dedupe_key)


async def notify_ozon_schema_change(
    endpoint: str,
    client_id: str,
    *,
    expected_key: str,
    actual_keys: list[str],
    payload: dict[str, Any] | None = None,
) -> bool:
    await record_admin_event(
        "ozon_schema_change",
        f"Ozon schema changed: {endpoint}",
        severity="warning",
        details={
            "client_id": client_id,
            "expected_key": expected_key,
            "actual_keys": actual_keys,
        },
    )
    text = (
        "🧩 <b>Похоже, Ozon изменил схему ответа</b>\n\n"
        f"<b>Метод:</b> {endpoint}\n"
        f"<b>Client-Id:</b> {client_id}\n"
        f"<b>Ожидали ключ:</b> {expected_key}\n"
        f"<b>Получили ключи:</b> {_clip(actual_keys, 600)}\n"
    )
    if payload:
        text += f"\n<b>Payload:</b>\n<code>{_clip(payload, 1200)}</code>"

    dedupe_key = f"ozon-schema:{endpoint}:{expected_key}:{','.join(actual_keys)}"
    return await send_admin_message(text, dedupe_key=dedupe_key, dedupe_ttl_seconds=900)


async def notify_ozon_rate_limit_pressure(
    *,
    client_id: str,
    store_name: str | None = None,
    limited_hits_last_minute: int,
    limit_per_second: int,
) -> bool:
    await record_admin_event(
        "ozon_rate_pressure",
        f"Rate pressure: {store_name or client_id}",
        severity="warning",
        details={
            "client_id": client_id,
            "store_name": store_name,
            "limited_hits_last_minute": limited_hits_last_minute,
            "limit_per_second": limit_per_second,
        },
    )
    text = (
        "⏱️ <b>Наш limiter начал часто притормаживать Ozon API</b>\n\n"
        f"<b>Client-Id:</b> {client_id}\n"
        f"<b>Магазин:</b> {store_name or '-'}\n"
        f"<b>Лимит в приложении:</b> {limit_per_second} req/sec\n"
        f"<b>Срабатываний за последнюю минуту:</b> {limited_hits_last_minute}\n"
    )

    dedupe_key = f"ozon-rate-pressure:{client_id}:{limited_hits_last_minute}:{limit_per_second}"
    return await send_admin_message(text, dedupe_key=dedupe_key, dedupe_ttl_seconds=300)


async def notify_sync_success(
    *,
    sync_type: str,
    store_id: int,
    store_name: str,
    user_email: str | None = None,
    task_id: str | None = None,
    duration_seconds: float | int | None = None,
    details: dict[str, Any] | None = None,
    send_telegram: bool = False,
) -> bool:
    await record_admin_event(
        "sync_success",
        f"Успешная синхронизация: {sync_type}",
        severity="success",
        details={
            "store_id": store_id,
            "store_name": store_name,
            "user_email": user_email,
            "task_id": task_id,
        } | (details or {}),
    )
    text = (
        "✅ <b>Успешная синхронизация</b>\n\n"
        f"<b>Тип:</b> {sync_type}\n"
        f"<b>Магазин:</b> {store_name} (ID {store_id})\n"
    )
    if user_email:
        text += f"<b>Пользователь:</b> {user_email}\n"
    if task_id:
        text += f"<b>Task ID:</b> {task_id}\n"
    duration = _format_duration(duration_seconds)
    if duration:
        text += f"<b>Длительность:</b> {duration}\n"
    if details:
        rendered = "\n".join(f"• <b>{key}:</b> {_clip(value, 400)}" for key, value in details.items())
        text += f"\n<b>Детали:</b>\n{rendered}"

    if not send_telegram:
        logger.info(
            "Пропускаем Telegram-уведомление об успешной синхронизации: {} / магазин {}",
            sync_type,
            store_id,
        )
        return False

    return await send_admin_message(text)


async def notify_sync_skipped(
    *,
    sync_type: str,
    store_id: int,
    store_name: str,
    user_email: str | None = None,
    reason: str,
    task_id: str | None = None,
    details: dict[str, Any] | None = None,
    send_telegram: bool = False,
) -> bool:
    await record_admin_event(
        "sync_skipped",
        f"Синхронизация пропущена: {sync_type}",
        severity="warning",
        details={
            "store_id": store_id,
            "store_name": store_name,
            "user_email": user_email,
            "task_id": task_id,
            "reason": reason,
        } | (details or {}),
    )
    text = (
        "⏭️ <b>Синхронизация пропущена</b>\n\n"
        f"<b>Тип:</b> {sync_type}\n"
        f"<b>Магазин:</b> {store_name} (ID {store_id})\n"
        f"<b>Причина:</b> {reason}\n"
    )
    if user_email:
        text += f"<b>Пользователь:</b> {user_email}\n"
    if task_id:
        text += f"<b>Task ID:</b> {task_id}\n"
    if details:
        rendered = "\n".join(f"• <b>{key}:</b> {_clip(value, 400)}" for key, value in details.items())
        text += f"\n<b>Детали:</b>\n{rendered}"

    if not send_telegram:
        logger.info(
            "Пропускаем Telegram-уведомление о пропуске синхронизации: {} / магазин {}",
            sync_type,
            store_id,
        )
        return False

    dedupe_key = f"sync-skipped:{sync_type}:{store_id}:{reason}"
    return await send_admin_message(text, dedupe_key=dedupe_key, dedupe_ttl_seconds=180)


async def notify_sync_partial(
    *,
    sync_type: str,
    store_id: int,
    store_name: str,
    user_email: str | None = None,
    task_id: str | None = None,
    duration_seconds: float | int | None = None,
    successful_units: int | None = None,
    failed_units: int | None = None,
    details: dict[str, Any] | None = None,
) -> bool:
    await record_admin_event(
        "sync_partial",
        f"Частичная синхронизация: {sync_type}",
        severity="warning",
        details={
            "store_id": store_id,
            "store_name": store_name,
            "user_email": user_email,
            "task_id": task_id,
            "successful_units": successful_units,
            "failed_units": failed_units,
        } | (details or {}),
    )
    text = (
        "⚠️ <b>Синхронизация завершилась частично</b>\n\n"
        f"<b>Тип:</b> {sync_type}\n"
        f"<b>Магазин:</b> {store_name} (ID {store_id})\n"
    )
    if user_email:
        text += f"<b>Пользователь:</b> {user_email}\n"
    if task_id:
        text += f"<b>Task ID:</b> {task_id}\n"
    duration = _format_duration(duration_seconds)
    if duration:
        text += f"<b>Длительность:</b> {duration}\n"
    if successful_units is not None:
        text += f"<b>Успешно обработано:</b> {successful_units}\n"
    if failed_units is not None:
        text += f"<b>Ошибок:</b> {failed_units}\n"
    if details:
        rendered = "\n".join(f"• <b>{key}:</b> {_clip(value, 400)}" for key, value in details.items())
        text += f"\n<b>Детали:</b>\n{rendered}"

    dedupe_key = (
        f"sync-partial:{sync_type}:{store_id}:{successful_units}:{failed_units}:"
        f"{_clip(details or {}, 200)}"
    )
    return await send_admin_message(text, dedupe_key=dedupe_key)
