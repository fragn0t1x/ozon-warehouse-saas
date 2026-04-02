import json
import secrets
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user_settings import UserSettings
from app.schemas.user_settings import TelegramConnectStatusResponse
from app.services.user_settings_helper import get_or_create_user_settings
from app.services.telegram_service import TelegramService
from app.utils.redis_cache import get_redis

logger = logging.getLogger(__name__)

TELEGRAM_CONNECT_TTL_SECONDS = 15 * 60
TELEGRAM_BOT_HEARTBEAT_KEY = "telegram:bot:heartbeat"
TELEGRAM_BOT_HEARTBEAT_STALE_SECONDS = 90


def telegram_connect_token_key(token: str) -> str:
    return f"telegram:connect:token:{token}"


def telegram_connect_code_key(code: str) -> str:
    return f"telegram:connect:code:{code}"


def telegram_connect_pending_key(user_id: int) -> str:
    return f"telegram:connect:pending:{user_id}"


async def _get_or_create_user_settings(db: AsyncSession, user_id: int) -> UserSettings:
    settings, _changed = await get_or_create_user_settings(db, user_id)
    return settings


def _generate_manual_code() -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(8))


def _serialize_pending_payload(token: str, code: str, expires_at: datetime) -> str:
    payload = {
        "token": token,
        "code": code,
        "expires_at": expires_at.isoformat(),
    }
    return json.dumps(payload, ensure_ascii=False)


async def _read_pending_payload(user_id: int) -> dict | None:
    redis = await get_redis()
    if not redis:
        return None

    try:
        raw = await redis.get(telegram_connect_pending_key(user_id))
    except Exception as e:
        logger.warning("⚠️ Failed to read Telegram pending state for user {}: {}", user_id, e)
        return None

    if not raw:
        return None

    try:
        payload = json.loads(raw)
    except Exception:
        return None

    if not isinstance(payload, dict):
        return None

    token = payload.get("token")
    code = payload.get("code")
    expires_at = payload.get("expires_at")
    if not token or not code or not expires_at:
        return None

    return payload


async def _get_bot_runtime_status() -> tuple[bool, datetime | None, str]:
    redis = await get_redis()
    if not redis:
        return False, None, "Не удалось проверить состояние бота: Redis недоступен."

    try:
        raw = await redis.get(TELEGRAM_BOT_HEARTBEAT_KEY)
    except Exception as e:
        logger.warning("⚠️ Failed to read Telegram bot heartbeat: {}", e)
        return False, None, "Не удалось проверить состояние бота."

    if not raw:
        return False, None, "Бот сейчас недоступен. Если только что перезапускали систему, подождите несколько секунд."

    try:
        payload = json.loads(raw)
    except Exception:
        return False, None, "Не удалось прочитать состояние Telegram-бота."

    last_seen_raw = payload.get("last_seen_at")
    if not last_seen_raw:
        return False, None, "Не удалось определить время последнего сигнала от бота."

    try:
        last_seen = datetime.fromisoformat(last_seen_raw)
    except Exception:
        return False, None, "Не удалось определить время последнего сигнала от бота."

    now = datetime.now(timezone.utc)
    is_alive = (now - last_seen).total_seconds() <= TELEGRAM_BOT_HEARTBEAT_STALE_SECONDS
    if is_alive:
        return True, last_seen, "Бот в сети и готов принимать команды."
    return False, last_seen, "Бот давно не отвечал. Команды и подтверждение подключения могут не сработать."


async def get_telegram_connect_status(
    db: AsyncSession,
    user_id: int,
) -> TelegramConnectStatusResponse:
    settings = await _get_or_create_user_settings(db, user_id)
    bot_available, bot_last_seen_at, bot_status_message = await _get_bot_runtime_status()
    telegram_service = TelegramService()
    try:
        bot_username = await telegram_service.get_bot_username()

        if not telegram_service.bot or not bot_username:
            return TelegramConnectStatusResponse(
                configured=False,
                status="not_configured",
                bot_available=bot_available,
                bot_last_seen_at=bot_last_seen_at,
                bot_status_message=bot_status_message,
                message="Telegram-бот пока не настроен. Подключение временно недоступно.",
            )

        if settings.telegram_chat_id:
            return TelegramConnectStatusResponse(
                configured=True,
                status="connected",
                bot_available=bot_available,
                bot_last_seen_at=bot_last_seen_at,
                bot_status_message=bot_status_message,
                bot_username=bot_username,
                telegram_chat_id=settings.telegram_chat_id,
                connected_at=settings.updated_at or settings.created_at,
                message="Бот подключен. Уведомления будут приходить автоматически.",
            )

        pending = await _read_pending_payload(user_id)
        if not pending:
            return TelegramConnectStatusResponse(
                configured=True,
                status="not_connected",
                bot_available=bot_available,
                bot_last_seen_at=bot_last_seen_at,
                bot_status_message=bot_status_message,
                bot_username=bot_username,
                message="Нажмите кнопку, откройте Telegram и отправьте команду /start боту.",
            )

        token = str(pending["token"])
        code = str(pending["code"])
        expires_at_raw = pending["expires_at"]
        try:
            expires_at = datetime.fromisoformat(expires_at_raw)
        except Exception:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=TELEGRAM_CONNECT_TTL_SECONDS)
        connect_url = telegram_service.build_connect_url(bot_username, token)

        return TelegramConnectStatusResponse(
            configured=True,
            status="pending",
            bot_available=bot_available,
            bot_last_seen_at=bot_last_seen_at,
            bot_status_message=bot_status_message,
            bot_username=bot_username,
            connect_url=connect_url,
            qr_code_url=telegram_service.build_qr_code_url(connect_url),
            manual_code=code,
            manual_command=f"/connect {code}",
            expires_at=expires_at,
            message="Откройте Telegram по ссылке или QR-коду. Если не получается, отправьте боту команду /connect с кодом.",
        )
    finally:
        await telegram_service.close()


async def create_telegram_connect_link(
    db: AsyncSession,
    user_id: int,
    *,
    force_new: bool = False,
) -> TelegramConnectStatusResponse:
    current_status = await get_telegram_connect_status(db, user_id)
    if not current_status.configured:
        return current_status

    if current_status.status == "connected" and not force_new:
        return current_status

    redis = await get_redis()
    if not redis:
        return TelegramConnectStatusResponse(
            configured=True,
            status="not_connected",
            bot_available=False,
            bot_last_seen_at=None,
            bot_status_message="Не удалось проверить состояние бота.",
            bot_username=current_status.bot_username,
            message="Redis недоступен, поэтому безопасное подключение Telegram временно не работает.",
        )

    if current_status.status == "pending" and current_status.connect_url and current_status.expires_at and not force_new:
        return current_status

    token = secrets.token_urlsafe(24)
    code = _generate_manual_code()
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=TELEGRAM_CONNECT_TTL_SECONDS)

    try:
        await redis.set(
            telegram_connect_token_key(token),
            str(user_id),
            ex=TELEGRAM_CONNECT_TTL_SECONDS,
        )
        await redis.set(
            telegram_connect_code_key(code),
            str(user_id),
            ex=TELEGRAM_CONNECT_TTL_SECONDS,
        )
        await redis.set(
            telegram_connect_pending_key(user_id),
            _serialize_pending_payload(token, code, expires_at),
            ex=TELEGRAM_CONNECT_TTL_SECONDS,
        )
    except Exception as e:
        logger.warning("⚠️ Failed to create Telegram connect token for user {}: {}", user_id, e)
        return TelegramConnectStatusResponse(
            configured=True,
            status="not_connected",
            bot_username=current_status.bot_username,
            message="Не удалось подготовить ссылку для Telegram. Попробуйте еще раз.",
        )

    connect_url = TelegramService.build_connect_url(current_status.bot_username, token)
    return TelegramConnectStatusResponse(
        configured=True,
        status="pending",
        bot_available=current_status.bot_available,
        bot_last_seen_at=current_status.bot_last_seen_at,
        bot_status_message=current_status.bot_status_message,
        bot_username=current_status.bot_username,
        connect_url=connect_url,
        qr_code_url=TelegramService.build_qr_code_url(connect_url),
        manual_code=code,
        manual_command=f"/connect {code}",
        expires_at=expires_at,
        message="Ссылка и QR готовы. Откройте Telegram или отправьте боту команду /connect с кодом.",
    )


async def disconnect_telegram(
    db: AsyncSession,
    *,
    user_id: int,
) -> TelegramConnectStatusResponse:
    settings = await _get_or_create_user_settings(db, user_id)
    settings.telegram_chat_id = None
    await db.commit()
    return await get_telegram_connect_status(db, user_id)


async def consume_telegram_connect_token(
    db: AsyncSession,
    *,
    token: str,
    chat_id: str,
) -> tuple[bool, str, int | None]:
    redis = await get_redis()
    if not redis:
        return False, "Подключение временно недоступно: Redis не отвечает.", None

    try:
        user_id_raw = await redis.get(telegram_connect_token_key(token))
        token_key = telegram_connect_token_key(token)
        code_key = None
        if not user_id_raw:
            code_key = telegram_connect_code_key(token.upper())
            user_id_raw = await redis.get(code_key)
    except Exception as e:
        logger.warning("⚠️ Failed to read Telegram token {}: {}", token, e)
        return False, "Не удалось проверить ссылку подключения. Попробуйте снова.", None

    if not user_id_raw:
        return False, "Ссылка уже истекла. Вернитесь в приложение и запросите новую.", None

    try:
        user_id = int(user_id_raw)
    except Exception:
        return False, "Ссылка подключения повреждена. Запросите новую в приложении.", None

    settings = await _get_or_create_user_settings(db, user_id)
    existing_chat_links = await db.execute(
        select(UserSettings).where(
            UserSettings.telegram_chat_id == str(chat_id),
            UserSettings.user_id != user_id,
        )
    )
    for linked_settings in existing_chat_links.scalars().all():
        linked_settings.telegram_chat_id = None

    settings.telegram_chat_id = str(chat_id)
    await db.commit()
    await db.refresh(settings)

    pending = await _read_pending_payload(user_id)
    pending_code = str(pending["code"]) if pending and pending.get("code") else None

    try:
        await redis.delete(telegram_connect_pending_key(user_id))
        if token_key:
            await redis.delete(token_key)
        if code_key:
            await redis.delete(code_key)
        elif pending_code:
            await redis.delete(telegram_connect_code_key(pending_code))
    except Exception as e:
        logger.warning("⚠️ Failed to cleanup Telegram connect state for user {}: {}", user_id, e)

    return True, "Telegram успешно подключен. Можно возвращаться в кабинет.", user_id
