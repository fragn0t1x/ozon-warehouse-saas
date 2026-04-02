import os
import asyncio
import traceback
import logging
import json
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import BotCommand, Message
from sqlalchemy import select, desc

# Импортируем всё через единый модуль
from models_import import SessionLocal, User
from app.models.user_settings import UserSettings
from app.services.cabinet_access import get_cabinet_owner_id
from app.services.daily_report_service import DailyReportService
from app.services.notification_schedule import build_notification_schedule, local_now
from app.services.telegram_service import TelegramService
from app.services.telegram_linking import consume_telegram_connect_token
from app.services.telegram_reports import build_user_next_supplies, build_user_today_supplies
from app.utils.redis_cache import get_redis

TOKEN = os.getenv("TELEGRAM_TOKEN")
bot = Bot(token=TOKEN)
dp = Dispatcher()
COMMAND_TIMEOUT_SECONDS = 90
logger = logging.getLogger(__name__)
BOT_HEARTBEAT_KEY = "telegram:bot:heartbeat"
BOT_HEARTBEAT_INTERVAL_SECONDS = 15
BOT_HEARTBEAT_TTL_SECONDS = 45


async def _replace_status(message: Message, status_message: Message, text: str, *, parse_mode: str | None = None):
    try:
        await status_message.edit_text(text, parse_mode=parse_mode)
    except Exception:
        await message.answer(text, parse_mode=parse_mode)


async def _get_linked_user_and_settings(db, chat_id: int | str):
    result = await db.execute(
        select(User, UserSettings)
        .join(UserSettings, UserSettings.user_id == User.id)
        .where(UserSettings.telegram_chat_id == str(chat_id))
        .order_by(desc(UserSettings.updated_at), desc(UserSettings.id))
    )
    rows = result.all()
    if not rows:
        return None

    active_user, active_settings = rows[0]
    duplicate_settings = [settings for _user, settings in rows[1:]]
    if duplicate_settings:
        for settings in duplicate_settings:
            settings.telegram_chat_id = None
        await db.commit()
        await db.refresh(active_settings)

    return active_user, active_settings


def _command_lock_key(chat_id: int | str, command_name: str) -> str:
    return f"telegram:command:lock:{chat_id}:{command_name}"


async def _acquire_command_lock(chat_id: int | str, command_name: str) -> bool:
    redis = await get_redis()
    if not redis:
        return True

    try:
        return bool(
            await redis.set(
                _command_lock_key(chat_id, command_name),
                "1",
                ex=COMMAND_TIMEOUT_SECONDS,
                nx=True,
            )
        )
    except Exception:
        return True


async def _release_command_lock(chat_id: int | str, command_name: str):
    redis = await get_redis()
    if not redis:
        return

    try:
        await redis.delete(_command_lock_key(chat_id, command_name))
    except Exception:
        return


async def _heartbeat_loop():
    while True:
        try:
            redis = await get_redis()
            if redis:
                await redis.set(
                    BOT_HEARTBEAT_KEY,
                    json.dumps({"last_seen_at": datetime.now(timezone.utc).isoformat()}),
                    ex=BOT_HEARTBEAT_TTL_SECONDS,
                )
        except Exception as exc:
            logger.warning("Failed to update Telegram bot heartbeat: %s", exc)
        await asyncio.sleep(BOT_HEARTBEAT_INTERVAL_SECONDS)


@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject):
    print(f"📨 /start chat_id={message.chat.id} text={message.text!r}", flush=True)
    token = (command.args or "").strip()
    if token:
        async with SessionLocal() as db:
            linked, reply_text, _user_id = await consume_telegram_connect_token(
                db,
                token=token,
                chat_id=str(message.chat.id),
            )
        if linked:
            await message.answer(
                "Подключение готово.\n\n"
                f"{reply_text}\n"
                "Теперь сюда будут приходить уведомления и отчеты из кабинета."
            )
            return

        await message.answer(
            "Не удалось завершить подключение.\n\n"
            f"{reply_text}"
        )
        return

    await message.answer(
        "Бот склада Ozon работает.\n\n"
        "Если вы пришли из кабинета, откройте ссылку подключения, отсканируйте QR-код или отправьте сюда команду /connect ВАШ_КОД.\n\n"
        "Доступные команды:\n"
        "/today — поставки на сегодня\n"
        "/daily — ежедневный отчет за вчера"
    )


@dp.message(Command("connect"))
async def cmd_connect(message: Message, command: CommandObject):
    print(f"📨 /connect chat_id={message.chat.id} text={message.text!r}", flush=True)
    token = (command.args or "").strip()
    if not token:
        await message.answer(
            "Чтобы подключить уведомления, отправьте команду в формате:\n"
            "/connect ВАШ_КОД"
        )
        return

    async with SessionLocal() as db:
        linked, reply_text, _user_id = await consume_telegram_connect_token(
            db,
            token=token,
            chat_id=str(message.chat.id),
        )

    if linked:
        await message.answer(
            "Подключение готово.\n\n"
            f"{reply_text}\n"
            "Теперь сюда будут приходить уведомления и отчеты из кабинета."
        )
        return

    await message.answer(
        "Не удалось завершить подключение.\n\n"
        f"{reply_text}"
    )


async def _handle_today(message: Message):
    print(f"📨 /today chat_id={message.chat.id} text={message.text!r}", flush=True)
    logger.info("Telegram command /today received for chat_id=%s", message.chat.id)
    if not await _acquire_command_lock(message.chat.id, "today"):
        await message.answer("Список поставок уже готовится. Подожди, пожалуйста, предыдущий ответ.")
        return

    status_message = await message.answer("Ищу поставки. Это может занять до минуты.")
    try:
        async with SessionLocal() as db:
            row = await _get_linked_user_and_settings(db, message.chat.id)
            if not row:
                logger.info("Telegram /today rejected: chat_id=%s is not linked", message.chat.id)
                await _replace_status(
                    message,
                    status_message,
                    "Сначала подключи Telegram к кабинету через настройки.\n"
                    "После этого команды /today и /daily будут доступны.",
                )
                return

            user, settings = row
            logger.info("Telegram /today linked chat_id=%s to user_id=%s owner_id=%s", message.chat.id, user.id, get_cabinet_owner_id(user))
            schedule = build_notification_schedule(settings)
            today = local_now(schedule).date()

            async def _build_payload():
                supplies = await build_user_today_supplies(
                    db,
                    cabinet_owner_id=get_cabinet_owner_id(user),
                    target_date=today,
                )
                telegram = TelegramService()
                try:
                    if supplies:
                        return telegram.build_today_supplies_grouped_text(
                            supplies,
                            title="Поставки на сегодня",
                        )

                    next_supplies, next_date = await build_user_next_supplies(
                        db,
                        cabinet_owner_id=get_cabinet_owner_id(user),
                        from_date=today,
                    )
                    if next_supplies and next_date:
                        next_title = f"Ближайшие поставки на {next_date.strftime('%d.%m.%Y')}"
                        return telegram.build_today_supplies_grouped_text(
                            next_supplies,
                            title=next_title,
                        )

                    return "На сегодня и ближайшие даты поставок не найдено."
                finally:
                    await telegram.close()

            text = await asyncio.wait_for(_build_payload(), timeout=COMMAND_TIMEOUT_SECONDS)
            logger.info("Telegram /today completed for chat_id=%s", message.chat.id)
            await _replace_status(message, status_message, text, parse_mode="HTML")
    except asyncio.TimeoutError:
        logger.warning("Telegram /today timed out for chat_id=%s", message.chat.id)
        await _replace_status(
            message,
            status_message,
            "Не успел подготовить ответ вовремя. Попробуй еще раз чуть позже.",
        )
    except Exception:
        logger.exception("Telegram /today failed for chat_id=%s", message.chat.id)
        traceback.print_exc()
        await _replace_status(
            message,
            status_message,
            "Не получилось подготовить ответ. Попробуй еще раз через пару минут.",
        )
    finally:
        await _release_command_lock(message.chat.id, "today")


@dp.message(Command("today"))
@dp.message(F.text.regexp(r"^/today(?:@\w+)?$"))
async def cmd_today(message: Message):
    await _handle_today(message)


async def _handle_daily(message: Message):
    print(f"📨 /daily chat_id={message.chat.id} text={message.text!r}", flush=True)
    logger.info("Telegram command /daily received for chat_id=%s", message.chat.id)
    if not await _acquire_command_lock(message.chat.id, "daily"):
        await message.answer("Ежедневный отчет уже формируется. Подожди, пожалуйста, предыдущий ответ.")
        return

    status_message = await message.answer("Готовлю ежедневный отчет. Это может занять до минуты.")
    try:
        async with SessionLocal() as db:
            row = await _get_linked_user_and_settings(db, message.chat.id)
            if not row:
                logger.info("Telegram /daily rejected: chat_id=%s is not linked", message.chat.id)
                await _replace_status(
                    message,
                    status_message,
                    "Сначала подключи Telegram к кабинету через настройки.\n"
                    "После этого команды /today и /daily будут доступны.",
                )
                return

            user, settings = row
            logger.info("Telegram /daily linked chat_id=%s to user_id=%s owner_id=%s", message.chat.id, user.id, get_cabinet_owner_id(user))
            schedule = build_notification_schedule(settings)
            local_report_date = local_now(schedule).date()

            async def _build_payload():
                report_service = DailyReportService()
                stats, store_stats = await report_service.build_owner_daily_report(
                    db,
                    cabinet_owner_id=get_cabinet_owner_id(user),
                    report_date=local_report_date,
                    allow_external_fetch=True,
                )
                telegram = TelegramService()
                try:
                    return telegram.build_daily_report_text(
                        stats,
                        title="Ежедневный отчет",
                        store_stats=store_stats,
                    )
                finally:
                    await telegram.close()

            text = await asyncio.wait_for(_build_payload(), timeout=COMMAND_TIMEOUT_SECONDS)
            logger.info("Telegram /daily completed for chat_id=%s", message.chat.id)
            await _replace_status(message, status_message, text, parse_mode="HTML")
    except asyncio.TimeoutError:
        logger.warning("Telegram /daily timed out for chat_id=%s", message.chat.id)
        await _replace_status(
            message,
            status_message,
            "Отчет готовится дольше обычного. Попробуй еще раз через пару минут.",
        )
    except Exception:
        logger.exception("Telegram /daily failed for chat_id=%s", message.chat.id)
        traceback.print_exc()
        await _replace_status(
            message,
            status_message,
            "Не получилось собрать ежедневный отчет. Попробуй еще раз чуть позже.",
        )
    finally:
        await _release_command_lock(message.chat.id, "daily")


@dp.message(Command("daily"))
@dp.message(F.text.regexp(r"^/daily(?:@\w+)?$"))
async def cmd_daily(message: Message):
    await _handle_daily(message)


@dp.message()
async def fallback_message(message: Message):
    print(f"📩 unhandled chat_id={message.chat.id} text={message.text!r}", flush=True)
    await message.answer(
        "Команда не распознана.\n\n"
        "Доступно:\n"
        "/today — поставки на сегодня\n"
        "/daily — ежедневный отчет за вчера\n"
        "/connect КОД — подключить Telegram к кабинету"
    )


async def main():
    print("🤖 Bot init", flush=True)
    try:
        await asyncio.wait_for(bot.delete_webhook(drop_pending_updates=False), timeout=10)
        print("✅ Webhook cleared", flush=True)
    except Exception as exc:
        print(f"⚠️ Failed to clear webhook: {exc}", flush=True)

    try:
        await asyncio.wait_for(
            bot.set_my_commands(
                [
                    BotCommand(command="today", description="Поставки на сегодня"),
                    BotCommand(command="daily", description="Ежедневный отчет за вчера"),
                    BotCommand(command="connect", description="Подключить Telegram к кабинету"),
                ]
            ),
            timeout=10,
        )
        print("✅ Bot commands configured", flush=True)
    except Exception as exc:
        print(f"⚠️ Failed to set bot commands: {exc}", flush=True)

    heartbeat_task = asyncio.create_task(_heartbeat_loop())
    print("🚀 Starting polling", flush=True)
    try:
        await dp.start_polling(bot)
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
