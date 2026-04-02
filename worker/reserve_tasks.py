from worker.worker import celery, get_event_loop
import asyncio
import sys
from loguru import logger
from datetime import date, datetime, timedelta, timezone
import os
from collections import defaultdict

# Добавляем пути
sys.path.insert(0, '/app')
sys.path.insert(0, '/app/backend')

try:
    from app.models.supply import Supply, SupplyItem
    from app.models.variant import Variant
    from app.models.product import Product
    from app.models.store import Store
    from app.models.user import User
    from app.models.user_settings import UserSettings
    from app.models.store_month_finance import StoreMonthFinance
    from app.models.supply_processing import SupplyProcessing
    from app.models.inventory_transaction import InventoryTransaction, TransactionType
    from app.services.cabinet_access import get_cabinet_owner_id
    from app.services.warehouse_service import WarehouseService
    from app.services.shipment_accounting import (
        get_supply_accounting_skip_reason,
    )
    from app.services.supply_reservation_wait import (
        clear_supply_reservation_wait,
        is_supply_reservation_wait_active,
        mark_supply_waiting_for_stock,
    )
    from app.services.warehouse_selector import resolve_warehouse
    from app.services.telegram_service import TelegramService
    from app.services.daily_report_service import DailyReportService
    from app.services.closed_month_history_service import ClosedMonthHistoryService
    from app.services.price_risk_alerts_service import PriceRiskAlertsService
    from app.services.ozon.finance_snapshot_service import OzonFinanceSnapshotService
    from app.services.notification_schedule import (
        DEFAULT_DAILY_REPORT_TIME,
        DEFAULT_TODAY_SUPPLIES_TIME,
        build_notification_schedule,
        is_dispatch_due,
        local_now,
    )
    from app.services.telegram_reports import (
        build_user_next_supplies,
        build_user_today_supplies,
    )
    from app.services.notification_center import (
        create_in_app_notification,
        create_in_app_notifications_for_users,
        deliver_email_notification_if_enabled,
        deliver_web_push_notification_if_enabled,
        html_to_text,
    )
    from app.services.admin_notifications import (
        notify_backend_error,
        record_admin_event,
        send_admin_broadcast_message,
    )
    from app.services.sync_dispatcher import enqueue_closed_month_history_sync
    from app.services.sync_scheduler import StoreSyncScheduler
    from app.services.sync_status import get_store_sync_status
    from app.utils.redis_cache import cache_get_json, get_redis
    from app.database import SessionLocal
    from sqlalchemy import select, and_, func, or_

    logger.info("✅ Successfully imported reserve task modules")
except Exception as e:
    logger.error(f"❌ Failed to import reserve task modules: {e}")
    import traceback
    traceback.print_exc()


def format_variant_attributes(variant):
    """Форматирует характеристики вариации в читаемый вид"""
    attributes = []
    for attr in variant.attributes:
        if attr.name.lower() in ['цвет', 'color', 'размер', 'size', 'кол-во пар', 'количество пар']:
            attributes.append(f"{attr.name}: {attr.value}")
    return ", ".join(attributes) if attributes else ""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _is_expected_stock_wait(error: Exception) -> bool:
    return isinstance(error, ValueError) and str(error).startswith("Not enough available stock.")


async def _acquire_notification_delivery(
    *,
    user_id: int,
    report_type: str,
    report_date: str,
) -> bool:
    redis = await get_redis()
    if not redis:
        return True

    sent_key = f"notifications:sent:{report_type}:{user_id}:{report_date}"
    processing_key = f"notifications:processing:{report_type}:{user_id}:{report_date}"

    if await redis.exists(sent_key):
        return False

    acquired = await redis.set(processing_key, "1", ex=300, nx=True)
    return bool(acquired)


async def _mark_notification_delivery(
    *,
    user_id: int,
    report_type: str,
    report_date: str,
    success: bool,
) -> None:
    redis = await get_redis()
    if not redis:
        return

    sent_key = f"notifications:sent:{report_type}:{user_id}:{report_date}"
    processing_key = f"notifications:processing:{report_type}:{user_id}:{report_date}"

    if success:
        await redis.set(sent_key, "1", ex=60 * 60 * 48)

    await redis.delete(processing_key)


def _resolve_schedule(settings: UserSettings | None):
    return build_notification_schedule(settings)


def _today_supplies_due(settings: UserSettings | None, now_utc: datetime | None = None) -> tuple[bool, str]:
    schedule = _resolve_schedule(settings)
    due = is_dispatch_due(
        scheduled_time=schedule.today_supplies_time_local or DEFAULT_TODAY_SUPPLIES_TIME,
        schedule=schedule,
        now_utc=now_utc,
    )
    report_date = local_now(schedule, now_utc).date().isoformat()
    return due, report_date


def _daily_report_due(settings: UserSettings | None, now_utc: datetime | None = None) -> tuple[bool, str]:
    schedule = _resolve_schedule(settings)
    due = is_dispatch_due(
        scheduled_time=schedule.daily_report_time_local or DEFAULT_DAILY_REPORT_TIME,
        schedule=schedule,
        now_utc=now_utc,
    )
    report_date = local_now(schedule, now_utc).date().isoformat()
    return due, report_date


def _previous_month_key(local_dt: datetime) -> str:
    current_month_start = local_dt.date().replace(day=1)
    previous_month_last_day = current_month_start - timedelta(days=1)
    return previous_month_last_day.strftime("%Y-%m")


def _monthly_closed_month_report_due(settings: UserSettings | None, now_utc: datetime | None = None) -> tuple[bool, str]:
    schedule = _resolve_schedule(settings)
    now_local = local_now(schedule, now_utc)
    due = 1 <= now_local.day <= 8
    return due, _previous_month_key(now_local)


def _month_label(month_key: str) -> str:
    try:
        year_text, month_text = str(month_key).split("-", 1)
        year = int(year_text)
        month = int(month_text)
    except Exception:
        return month_key
    names = [
        "январь",
        "февраль",
        "март",
        "апрель",
        "май",
        "июнь",
        "июль",
        "август",
        "сентябрь",
        "октябрь",
        "ноябрь",
        "декабрь",
    ]
    if 1 <= month <= 12:
        return f"{names[month - 1]} {year}"
    return month_key


def _closed_month_status_label(status: str) -> str:
    return {
        "ready": "Готов",
        "ozon_warning": "Есть ограничения Ozon",
        "needs_cost": "Нужна себестоимость",
        "error": "Ошибка",
        "pending": "Ожидает",
    }.get(str(status or "").strip(), str(status or "-").strip() or "-")


def _price_risk_status_label(status: str) -> str:
    return {
        "low_margin": "Очень низкая маржа",
        "break_even": "Почти в ноль",
        "loss": "В минус",
        "critical_loss": "Сильный минус",
    }.get(str(status or "").strip(), str(status or "-").strip() or "-")


async def _record_notification_delivery_admin_event(
    *,
    kind: str,
    title: str,
    user_email: str,
    in_app_count: int,
    email_count: int,
    web_push_count: int,
    telegram_count: int,
    admin_telegram_count: int = 0,
    details: dict | None = None,
) -> None:
    await record_admin_event(
        "notification_delivery",
        f"Доставка уведомления: {title}",
        severity="info",
        details={
            "kind": kind,
            "user_email": user_email,
            "in_app": int(in_app_count or 0),
            "email": int(email_count or 0),
            "web_push": int(web_push_count or 0),
            "telegram": int(telegram_count or 0),
            "admin_telegram": int(admin_telegram_count or 0),
        }
        | (details or {}),
    )


def get_internal_units(quantity: int, variant: Variant | None) -> int:
    """Converts supply quantity into internal stock units."""
    pack_size = variant.pack_size if variant and variant.pack_size else 1
    return quantity * pack_size


def supply_status_ru(status: str) -> str:
    return {
        'READY_TO_SUPPLY': 'Готова к отгрузке',
        'ACCEPTED_AT_SUPPLY_WAREHOUSE': 'Принята на точке отгрузки',
        'IN_TRANSIT': 'В пути',
        'COMPLETED': 'Завершена',
        'CANCELLED': 'Отменена',
        'REJECTED_AT_SUPPLY_WAREHOUSE': 'Отказано в приемке',
        'ACCEPTANCE_AT_STORAGE_WAREHOUSE': 'На приемке на складе OZON',
        'REPORTS_CONFIRMATION_AWAITING': 'Ожидает подтверждения актов',
        'REPORT_REJECTED': 'Акт приемки отклонен',
        'OVERDUE': 'Просрочена',
    }.get(status, status)


def _format_supply_timeslot_short(timeslot_from: datetime | None, timeslot_to: datetime | None) -> str:
    if not timeslot_from:
        return "Не указан"
    from_label = timeslot_from.strftime("%d.%m %H:%M")
    if not timeslot_to:
        return from_label
    return f"{from_label} - {timeslot_to.strftime('%H:%M')}"


def _format_overdue_duration(overdue_since: datetime | None, now: datetime | None = None) -> str:
    if not overdue_since:
        return "-"
    current = now or datetime.now()
    delta = current - overdue_since
    total_seconds = max(int(delta.total_seconds()), 0)
    minutes, secs = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    if days:
        return f"{days}д {hours}ч {minutes}м"
    if hours:
        return f"{hours}ч {minutes}м"
    if minutes:
        return f"{minutes}м"
    return f"{secs}с"


def _pluralize_units(count: int) -> str:
    value = abs(int(count))
    if value % 10 == 1 and value % 100 != 11:
        return "шт."
    return "шт."


def _build_compact_items_preview(items: list[dict], *, limit: int = 5) -> str:
    if not items:
        return ""
    lines: list[str] = []
    for item in items[:limit]:
        title = item.get("product_name") or "Без названия"
        sku = item.get("sku") or "-"
        quantity = int(item.get("quantity") or 0)
        lines.append(f"• {title} — <code>{sku}</code> — {quantity} шт.")
    if len(items) > limit:
        lines.append(f"• И еще {len(items) - limit} поз.")
    return "\n".join(lines)


async def _load_notification_users(db, *, notification_field: str) -> list[tuple[User, UserSettings | None]]:
    result = await db.execute(
        select(User, UserSettings)
        .outerjoin(UserSettings, UserSettings.user_id == User.id)
        .where(
            User.is_active == True,  # noqa: E712
            User.is_admin == False,  # noqa: E712
        )
        .order_by(User.id)
    )
    rows = result.all()
    users: list[tuple[User, UserSettings | None]] = []
    for user, settings in rows:
        if settings is None or getattr(settings, notification_field, False):
            users.append((user, settings))
    return users


async def _load_cabinet_notification_targets(
    db,
    *,
    owner_user_id: int,
    notification_field: str,
) -> list[tuple[User, UserSettings | None]]:
    result = await db.execute(
        select(User, UserSettings)
        .outerjoin(UserSettings, UserSettings.user_id == User.id)
        .where(
            User.is_active == True,  # noqa: E712
            User.is_admin == False,  # noqa: E712
            or_(User.id == owner_user_id, User.owner_user_id == owner_user_id),
        )
        .order_by(User.id)
    )
    rows = result.all()
    users: list[tuple[User, UserSettings | None]] = []
    for user, settings in rows:
        if settings is None or getattr(settings, notification_field, False):
            users.append((user, settings))
    return users


def _notification_recipients(
    targets: list[tuple[User, UserSettings | None]],
) -> list[tuple[User, str]]:
    recipients: list[tuple[User, str]] = []
    seen_chat_ids: set[str] = set()
    for user, settings in targets:
        chat_id = settings.telegram_chat_id if settings and settings.telegram_chat_id else None
        if not chat_id or chat_id in seen_chat_ids:
            continue
        seen_chat_ids.add(chat_id)
        recipients.append((user, chat_id))
    return recipients


async def _send_notification_to_recipients(
    telegram: TelegramService,
    recipients: list[tuple[User, str]],
    text: str,
) -> int:
    sent_count = 0
    for user, chat_id in recipients:
        try:
            await telegram.send_message(chat_id, text)
            sent_count += 1
            logger.info(f"   ✅ Notification sent to user {user.id}")
        except Exception as exc:
            logger.error(f"   ❌ Failed to send notification to user {user.id}: {exc}")
    return sent_count


async def _deliver_multichannel_notification(
    db,
    *,
    targets: list[tuple[User, UserSettings | None]],
    telegram: TelegramService | None,
    telegram_recipients: list[tuple[User, str]],
    title: str,
    text: str,
    kind: str,
    action_url: str | None = None,
    severity: str = "info",
    is_important: bool = False,
    email_flag: str | None = None,
) -> tuple[int, int, int, int]:
    plain_text = html_to_text(text)
    in_app_count = await create_in_app_notifications_for_users(
        db,
        user_ids=[user.id for user, _ in targets],
        kind=kind,
        title=title,
        body=plain_text,
        action_url=action_url,
        severity=severity,
        is_important=is_important,
    )

    email_count = 0
    push_count = 0
    for user, settings in targets:
        if await deliver_email_notification_if_enabled(
            user=user,
            settings=settings,
            subject=title,
            html_text=text,
            plain_text=plain_text,
            email_flag=email_flag,
        ):
            email_count += 1
        push_count += await deliver_web_push_notification_if_enabled(
            db,
            user=user,
            settings=settings,
            title=title,
            plain_text=plain_text,
            action_url=action_url,
            kind=kind,
            severity=severity,
        )

    telegram_count = 0
    if telegram and telegram_recipients:
        telegram_count = await _send_notification_to_recipients(telegram, telegram_recipients, text)

    return in_app_count, email_count, push_count, telegram_count


async def _build_user_daily_report(db, cabinet_owner_id: int, report_date: str | None = None) -> tuple[dict, list[dict]]:
    service = DailyReportService()
    parsed_report_date = None
    if report_date:
        try:
            parsed_report_date = date.fromisoformat(report_date)
        except ValueError:
            parsed_report_date = None
    return await service.build_owner_daily_report(
        db,
        cabinet_owner_id=cabinet_owner_id,
        report_date=parsed_report_date,
    )


async def _build_user_today_supplies(db, cabinet_owner_id: int) -> list[dict]:
    today = datetime.now().date()
    return await build_user_today_supplies(db, cabinet_owner_id=cabinet_owner_id, target_date=today)


async def _build_user_next_supplies(db, cabinet_owner_id: int) -> tuple[list[dict], date | None]:
    tomorrow = datetime.now().date() + timedelta(days=1)
    return await build_user_next_supplies(db, cabinet_owner_id=cabinet_owner_id, from_date=tomorrow)


async def get_products_grouped_by_variants(db, supply_id: int):
    """Получает товары, сгруппированные по продуктам с вариациями"""
    # Получаем все товары в поставке
    items_stmt = select(SupplyItem).where(SupplyItem.supply_id == supply_id)
    items_result = await db.execute(items_stmt)
    items = items_result.scalars().all()

    # Группируем по продуктам
    products_dict = defaultdict(list)

    for item in items:
        variant = await db.get(Variant, item.variant_id)
        product = await db.get(Product, variant.product_id)

        # Получаем характеристики вариации
        attributes = []
        for attr in variant.attributes:
            if attr.name.lower() in ['цвет', 'color', 'размер', 'size']:
                attributes.append(f"{attr.value}")

        variant_info = {
            'variant_id': variant.id,
            'sku': variant.sku,
            'pack_size': variant.pack_size,
            'quantity': item.quantity,
            'accepted_quantity': item.accepted_quantity,
            'attributes': " / ".join(attributes) if attributes else "",
            'color': next((attr.value for attr in variant.attributes if attr.name.lower() in ['цвет', 'color']), ""),
            'size': next((attr.value for attr in variant.attributes if attr.name.lower() in ['размер', 'size']), ""),
        }

        products_dict[product.id].append({
            'product_id': product.id,
            'product_name': product.name,
            'variant': variant_info
        })

    return products_dict


async def get_products_with_losses(db, supply_id: int):
    """Получает товары с потерями, сгруппированные по продуктам"""
    items_stmt = select(SupplyItem).where(
        SupplyItem.supply_id == supply_id,
        SupplyItem.accepted_quantity.isnot(None),
        SupplyItem.accepted_quantity < SupplyItem.quantity
    )
    items_result = await db.execute(items_stmt)
    items = items_result.scalars().all()

    products_dict = defaultdict(list)

    for item in items:
        variant = await db.get(Variant, item.variant_id)
        product = await db.get(Product, variant.product_id)

        loss = item.quantity - item.accepted_quantity

        # Получаем характеристики
        color = next((attr.value for attr in variant.attributes if attr.name.lower() in ['цвет', 'color']), "")
        size = next((attr.value for attr in variant.attributes if attr.name.lower() in ['размер', 'size']), "")
        pack_size = variant.pack_size

        variant_info = {
            'variant_id': variant.id,
            'sku': variant.sku,
            'quantity': item.quantity,
            'accepted_quantity': item.accepted_quantity,
            'loss': loss,
            'color': color,
            'size': size,
            'pack_size': pack_size,
            'attributes': f"{color} {size}".strip() if color or size else ""
        }

        products_dict[product.id].append({
            'product_id': product.id,
            'product_name': product.name,
            'variant': variant_info
        })

    return products_dict


def format_product_section(product_name: str, variants: list, show_accepted: bool = False):
    """Форматирует секцию товара с вариациями"""
    text = f"\n<b>{product_name}</b>\n"

    for item in variants:
        variant = item['variant']
        attr_text = f" ({variant['attributes']})" if variant['attributes'] else ""

        if show_accepted and variant.get('accepted_quantity'):
            text += f"  • {attr_text[2:] if attr_text else 'Без характеристик'}: отправлено {variant['quantity']}, принято {variant['accepted_quantity']}"
            if variant.get('loss'):
                text += f" (потеря <b>{variant['loss']}</b> шт.)"
            text += "\n"
        else:
            text += f"  • {attr_text[2:] if attr_text else 'Без характеристик'}: {variant['quantity']} шт.\n"

    return text


async def mark_processed(db, supply_id: int, processed_type: str):
    """Отметить поставку как обработанную"""
    try:
        # Проверяем, нет ли уже такой записи
        existing = await db.execute(
            select(SupplyProcessing).where(
                SupplyProcessing.supply_id == supply_id,
                SupplyProcessing.processed_type == processed_type
            )
        )
        if not existing.scalar_one_or_none():
            processed = SupplyProcessing(
                supply_id=supply_id,
                processed_type=processed_type,
                processed_at=utcnow()
            )
            db.add(processed)
            logger.info(f"   ✅ Marked supply {supply_id} as processed ({processed_type})")
        else:
            logger.info(f"   ℹ️ Supply {supply_id} already marked as processed ({processed_type})")
    except Exception as e:
        logger.error(f"   ❌ Failed to mark as processed: {e}")


async def _release_supply_reserves(
    *,
    db,
    service: WarehouseService,
    warehouse_id: int,
    supply: Supply,
    reserves: list[InventoryTransaction],
) -> int:
    released_count = 0
    for reserve in reserves:
        await service.cancel_reserve(
            db,
            warehouse_id=warehouse_id,
            variant_id=reserve.variant_id,
            quantity=reserve.quantity,
            supply_id=supply.id,
            commit=False,
        )
        logger.info(f"   🔓 Released {reserve.quantity} reserved units for variant {reserve.variant_id}")
        released_count += 1

    supply.reserved_at = None
    return released_count


async def _get_supply_processed_types(db, supply_id: int) -> set[str]:
    result = await db.execute(
        select(SupplyProcessing.processed_type).where(
            SupplyProcessing.supply_id == supply_id
        )
    )
    return {value for value in result.scalars().all() if value}


@celery.task(name='worker.tasks.reserve_ready_supplies_task', bind=True)
def reserve_ready_supplies_task(self):
    """Автоматическое резервирование товаров для всех поставок READY_TO_SUPPLY"""
    logger.info("🔍 Checking for READY_TO_SUPPLY supplies to reserve")

    async def _run():
        try:
            async with SessionLocal() as db:
                # Находим все поставки READY_TO_SUPPLY, которые еще не резервировались
                stmt = select(Supply).where(
                    Supply.status == "READY_TO_SUPPLY",
                    Supply.reserved_at.is_(None)
                )
                result = await db.execute(stmt)
                supplies = result.scalars().all()

                logger.info(f"📊 Found {len(supplies)} READY_TO_SUPPLY supplies")

                if not supplies:
                    logger.info("✅ No new READY_TO_SUPPLY supplies found")
                    return

                service = WarehouseService()
                reserved_count = 0
                failed_count = 0
                waiting_for_stock_count = 0

                for supply in supplies:
                    supply_id = supply.id
                    logger.info(f"📦 Processing supply {supply_id} ({supply.order_number})")

                    if await is_supply_reservation_wait_active(supply_id):
                        logger.info(f"   ⏸️ Supply {supply_id} is waiting for stock income, skipping for now")
                        waiting_for_stock_count += 1
                        continue

                    # Получаем магазин и склад
                    store = await db.get(Store, supply.store_id)
                    if not store:
                        logger.warning(f"   ⚠️ Store not found for supply {supply_id}")
                        continue

                    owner_settings_result = await db.execute(
                        select(UserSettings).where(UserSettings.user_id == store.user_id)
                    )
                    owner_settings = owner_settings_result.scalar_one_or_none()
                    skip_reason = get_supply_accounting_skip_reason(
                        supply_created_at=supply.created_at,
                        settings=owner_settings,
                    )
                    if skip_reason:
                        logger.info(
                            f"   ⏭️ Supply {supply_id} is outside accounting window "
                            f"({skip_reason}), skipping reserve"
                        )
                        continue

                    try:
                        warehouse, settings = await resolve_warehouse(
                            db,
                            user_id=store.user_id,
                            store_id=store.id,
                            warehouse_id=None
                        )
                    except Exception as e:
                        logger.error(f"   ❌ Failed to resolve warehouse: {e}")
                        continue

                    # Получаем товары в поставке
                    items_stmt = select(SupplyItem).where(SupplyItem.supply_id == supply_id)
                    items_result = await db.execute(items_stmt)
                    items = items_result.scalars().all()

                    logger.info(f"   Found {len(items)} items in supply")

                    supply_success = True
                    current_variant_id: int | None = None
                    current_required_units: int | None = None
                    try:
                        for item in items:
                            current_variant_id = item.variant_id
                            variant = await db.get(Variant, item.variant_id)
                            reserve_units = get_internal_units(item.quantity, variant)
                            current_required_units = reserve_units
                            logger.info(
                                f"   🔄 Attempting to reserve variant_id={item.variant_id}, "
                                f"packages={item.quantity}, internal_units={reserve_units}"
                            )
                            await service.reserve(
                                db,
                                warehouse_id=warehouse.id,
                                variant_id=item.variant_id,
                                quantity=reserve_units,
                                supply_id=supply_id,
                                packing_mode=settings.packing_mode if settings else None,
                                commit=False
                            )
                            logger.info(f"   ✅ Reserved {reserve_units} units for variant {item.variant_id}")
                            reserved_count += 1

                        logger.info(f"   ✅ Successfully reserved all items for supply {supply_id}")
                        supply = await db.get(Supply, supply_id)
                        if supply:
                            supply.reserved_at = utcnow()
                        await db.commit()
                        await clear_supply_reservation_wait(supply_id)
                    except ValueError as e:
                        await db.rollback()
                        supply_success = False
                        if _is_expected_stock_wait(e):
                            logger.warning(f"   ⏸️ Supply {supply_id} is waiting for stock income: {e}")
                            await mark_supply_waiting_for_stock(
                                supply_id,
                                variant_id=current_variant_id,
                                required_quantity=current_required_units,
                                message=str(e),
                            )
                            waiting_for_stock_count += 1
                        else:
                            logger.error(f"   ❌ Reservation rolled back for supply {supply_id}: {e}")
                            failed_count += 1
                    except Exception as e:
                        logger.error(f"   ❌ Failed to reserve supply {supply_id}: {e}")
                        await db.rollback()
                        supply_success = False
                        failed_count += 1
                        import traceback
                        traceback.print_exc()

                    if not supply_success:
                        logger.warning(f"   ⚠️ Reservation for supply {supply_id} was not committed")

                logger.info(
                    f"✅ Reservation summary: {reserved_count} items reserved, "
                    f"{waiting_for_stock_count} waiting for stock, {failed_count} failed"
                )

        except Exception as e:
            logger.error(f"❌ Error in reserve_ready_supplies_task: {e}")
            import traceback
            traceback.print_exc()

    return get_event_loop().run_until_complete(_run())


@celery.task(name='worker.tasks.check_supplies_status_task', bind=True)
def check_supplies_status_task(self):
    """Проверка статусов поставок и обновление резервов"""
    logger.info("🔍 Checking supplies status changes")

    async def _run():
        telegram = None
        shipment_statuses = {
            "ACCEPTED_AT_SUPPLY_WAREHOUSE",
            "IN_TRANSIT",
            "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
            "REPORTS_CONFIRMATION_AWAITING",
        }
        try:
            async with SessionLocal() as db:
                # Находим поставки, которые были зарезервированы, но изменили статус
                stmt = select(Supply).where(
                    Supply.status.in_([
                        "ACCEPTED_AT_SUPPLY_WAREHOUSE",  # Отгружено - можно списать
                        "IN_TRANSIT",  # Уже ушло со склада OZON, если пропустили промежуточный статус
                        "ACCEPTANCE_AT_STORAGE_WAREHOUSE",  # Приемка на складе хранения OZON
                        "REPORTS_CONFIRMATION_AWAITING",  # Ожидается подтверждение актов приемки
                        "CANCELLED",  # Отменено ДО отправки - снять резерв
                        "REPORT_REJECTED",  # Проблема с актом приемки - ручная проверка
                        "REJECTED_AT_SUPPLY_WAREHOUSE",  # Отказано в приемке - ТОЛЬКО УВЕДОМЛЕНИЕ!
                        "OVERDUE",  # Просрочена - уведомление и ручная проверка
                        "COMPLETED"  # Завершена - проверяем расхождения
                    ]),
                    or_(
                        Supply.reserved_at.is_not(None),
                        Supply.status.in_(shipment_statuses),
                        Supply.status == "CANCELLED",
                    )
                )

                result = await db.execute(stmt)
                all_supplies = result.scalars().all()

                # Фильтруем уже обработанные поставки
                supplies = []
                for supply in all_supplies:
                    if supply.status == "COMPLETED":
                        # Проверяем, не обрабатывали ли уже эту поставку для correction
                        processed = await db.execute(
                            select(SupplyProcessing).where(
                                SupplyProcessing.supply_id == supply.id,
                                SupplyProcessing.processed_type.in_(["correction", "correction_check"])
                            )
                        )
                        if not processed.scalar_one_or_none():
                            supplies.append(supply)
                        else:
                            logger.info(f"   ⏭️ Supply {supply.id} already processed for correction, skipping")
                    else:
                        # Для других статусов проверяем соответствующие типы обработки
                        processed_type = {
                            "ACCEPTED_AT_SUPPLY_WAREHOUSE": "shipment",
                            "IN_TRANSIT": "shipment",
                            "ACCEPTANCE_AT_STORAGE_WAREHOUSE": "shipment",
                            "REPORTS_CONFIRMATION_AWAITING": "shipment",
                            "CANCELLED": "cancellation",
                            "REPORT_REJECTED": "rejection_notification",
                            "REJECTED_AT_SUPPLY_WAREHOUSE": "rejection_notification",
                            "OVERDUE": "overdue_notification",
                        }.get(supply.status)

                        if processed_type:
                            processed_types_found = await _get_supply_processed_types(db, supply.id)
                            if processed_type == "shipment":
                                terminal_types = {
                                    "shipment",
                                    "shipment_skipped_before_start_date",
                                    "shipment_skipped_accounting_disabled",
                                }
                                recovery_types = {
                                    "overdue_reserve_released",
                                    "overdue_recovery_notification",
                                }
                                if not processed_types_found.intersection(terminal_types) and (
                                    supply.reserved_at is not None
                                    or processed_types_found.intersection(recovery_types)
                                ):
                                    supplies.append(supply)
                                else:
                                    logger.info(
                                        f"   ⏭️ Supply {supply.id} already processed for shipment, skipping"
                                    )
                            elif processed_type == "cancellation":
                                if (
                                    "cancellation" not in processed_types_found
                                    and (
                                        supply.reserved_at is not None
                                        or "overdue_reserve_released" in processed_types_found
                                    )
                                ):
                                    supplies.append(supply)
                                else:
                                    logger.info(
                                        f"   ⏭️ Supply {supply.id} already processed for cancellation, skipping"
                                    )
                            elif processed_type not in processed_types_found:
                                supplies.append(supply)
                            else:
                                logger.info(
                                    f"   ⏭️ Supply {supply.id} already processed for {processed_type}, skipping")
                        else:
                            supplies.append(supply)

                logger.info(f"📊 Found {len(supplies)} supplies with status changes (filtered from {len(all_supplies)})")

                if not supplies:
                    logger.info("✅ No new supplies with status changes found")
                    return

                service = WarehouseService()
                telegram = TelegramService()

                for supply in supplies:
                    logger.info(f"📦 Processing status change for supply {supply.id} -> {supply.status}")

                    # Получаем магазин, общие настройки кабинета и получателей уведомлений
                    store = await db.get(Store, supply.store_id)
                    owner_user_id = store.user_id
                    owner_settings = await db.execute(
                        select(UserSettings).where(UserSettings.user_id == owner_user_id)
                    )
                    owner_settings = owner_settings.scalar_one_or_none()
                    acceptance_targets = await _load_cabinet_notification_targets(
                        db,
                        owner_user_id=owner_user_id,
                        notification_field="notify_acceptance_status",
                    )
                    rejection_targets = await _load_cabinet_notification_targets(
                        db,
                        owner_user_id=owner_user_id,
                        notification_field="notify_rejection",
                    )
                    acceptance_recipients = _notification_recipients(acceptance_targets)
                    rejection_recipients = _notification_recipients(rejection_targets)

                    try:
                        warehouse, _ = await resolve_warehouse(
                            db,
                            user_id=owner_user_id,
                            store_id=store.id,
                            warehouse_id=None
                        )
                    except Exception as e:
                        logger.error(f"   ❌ Failed to resolve warehouse for supply {supply.id}: {e}")
                        continue

                    logger.info(
                        f"   Notification recipients: acceptance={len(acceptance_recipients)},"
                        f" rejection={len(rejection_recipients)}"
                    )

                    # Получаем все резервы для этой поставки
                    reserves_stmt = select(InventoryTransaction).where(
                        InventoryTransaction.reference_type == "SUPPLY",
                        InventoryTransaction.reference_id == supply.id,
                        InventoryTransaction.type == TransactionType.RESERVE
                    )
                    reserves_result = await db.execute(reserves_stmt)
                    reserves = reserves_result.scalars().all()
                    processed_types_found = await _get_supply_processed_types(db, supply.id)
                    overdue_reserve_released = "overdue_reserve_released" in processed_types_found

                    logger.info(f"   Found {len(reserves)} reserve transactions")

                    # Обработка разных статусов
                    if supply.status in {
                        "ACCEPTED_AT_SUPPLY_WAREHOUSE",
                        "IN_TRANSIT",
                        "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
                        "REPORTS_CONFIRMATION_AWAITING",
                    }:
                        # Поставка принята на точке отгрузки - списываем товары
                        logger.info(f"   🚚 Supply reached post-shipment state {supply.status} - shipping items if needed")

                        skip_reason = get_supply_accounting_skip_reason(
                            supply_created_at=supply.created_at,
                            settings=owner_settings,
                        )
                        if skip_reason:
                            logger.info(
                                f"   ⏭️ Supply is outside accounting window ({skip_reason}) - "
                                f"releasing reserve and skipping shipment"
                            )
                            try:
                                await _release_supply_reserves(
                                    db=db,
                                    service=service,
                                    warehouse_id=warehouse.id,
                                    supply=supply,
                                    reserves=reserves,
                                )
                                await mark_processed(db, supply.id, skip_reason)
                                await db.commit()
                            except Exception as e:
                                logger.error(
                                    f"   ❌ Failed to release skipped shipment reserve for supply {supply.id}: {e}"
                                )
                                await db.rollback()
                            continue

                        shipped_count = 0
                        shipped_items = []
                        recovery_notified = "overdue_recovery_notification" in processed_types_found
                        recovery_shortage = False
                        shipment_success = True
                        recovered_from_overdue = False
                        try:
                            if not reserves and overdue_reserve_released:
                                recovered_from_overdue = True
                                logger.info(
                                    f"   🔁 Supply {supply.id} revived after OVERDUE - "
                                    f"re-reserving items before shipment"
                                )
                                items_stmt = select(SupplyItem).where(SupplyItem.supply_id == supply.id)
                                items_result = await db.execute(items_stmt)
                                items = items_result.scalars().all()
                                if not items:
                                    raise ValueError("No supply items found for overdue recovery")

                                for item in items:
                                    variant = await db.get(Variant, item.variant_id)
                                    reserve_units = get_internal_units(item.quantity, variant)
                                    await service.reserve(
                                        db,
                                        warehouse_id=warehouse.id,
                                        variant_id=item.variant_id,
                                        quantity=reserve_units,
                                        supply_id=supply.id,
                                        packing_mode=owner_settings.packing_mode if owner_settings else None,
                                        commit=False,
                                    )
                                    await service.ship(
                                        db,
                                        warehouse_id=warehouse.id,
                                        variant_id=item.variant_id,
                                        quantity=reserve_units,
                                        supply_id=supply.id,
                                        packing_mode=owner_settings.packing_mode if owner_settings else None,
                                        commit=False,
                                    )
                                    logger.info(
                                        f"   ✅ Recovered and shipped {reserve_units} units for variant {item.variant_id}"
                                    )
                                    shipped_count += 1
                                    product = await db.get(Product, variant.product_id) if variant else None
                                    shipped_items.append({
                                        'product_name': product.name if product else 'Unknown',
                                        'sku': variant.sku if variant else 'Unknown',
                                        'quantity': reserve_units
                                    })
                            else:
                                if not reserves:
                                    raise ValueError("Supply reached shipment state without active reserve")
                                for reserve in reserves:
                                    await service.ship(
                                        db,
                                        warehouse_id=warehouse.id,
                                        variant_id=reserve.variant_id,
                                        quantity=reserve.quantity,
                                        supply_id=supply.id,
                                        packing_mode=owner_settings.packing_mode if owner_settings else None,
                                        commit=False
                                    )
                                    logger.info(f"   ✅ Shipped {reserve.quantity} units for variant {reserve.variant_id}")
                                    shipped_count += 1

                                    variant = await db.get(Variant, reserve.variant_id)
                                    product = await db.get(Product, variant.product_id) if variant else None
                                    shipped_items.append({
                                        'product_name': product.name if product else 'Unknown',
                                        'sku': variant.sku if variant else 'Unknown',
                                        'quantity': reserve.quantity
                                    })

                            await mark_processed(db, supply.id, "shipment")
                            await db.commit()
                            await clear_supply_reservation_wait(supply.id)
                        except Exception as e:
                            if recovered_from_overdue and _is_expected_stock_wait(e):
                                recovery_shortage = True
                            logger.error(f"   ❌ Shipment rolled back for supply {supply.id}: {e}")
                            shipment_success = False
                            shipped_count = 0
                            shipped_items = []
                            await db.rollback()

                        # Отправляем уведомление о начале приемки (если включено)
                        if shipment_success and shipped_count > 0 and acceptance_targets:
                            items_text = _build_compact_items_preview(shipped_items)
                            status_title = supply_status_ru(supply.status)
                            title = f"Поставка #{supply.order_number} обновлена"
                            text = (
                                f"📦 <b>Поставка обновлена</b>\n\n"
                                f"Магазин: <b>{store.name}</b>\n"
                                f"Поставка: <b>№{supply.order_number}</b>\n"
                                f"Статус: <b>{status_title}</b>\n"
                                f"Таймслот: {_format_supply_timeslot_short(supply.timeslot_from, supply.timeslot_to)}\n"
                            )
                            if items_text:
                                text += f"\n\nОтгружено:\n{items_text}"
                            text += "\n\nПроверь раздел «Поставки», если нужен полный состав."
                            _in_app, email_count, push_count, sent_count = await _deliver_multichannel_notification(
                                db,
                                targets=acceptance_targets,
                                telegram=telegram,
                                telegram_recipients=acceptance_recipients,
                                title=title,
                                text=text,
                                kind="supply_status",
                                action_url="/supplies",
                                severity="info",
                                is_important=False,
                                email_flag="email_acceptance_status",
                            )
                            logger.info(
                                f"   ✅ Shipment notifications delivered: telegram={sent_count}, email={email_count}, web_push={push_count}"
                            )
                        elif recovery_shortage and acceptance_targets and not recovery_notified:
                            text = (
                                f"⚠️ <b>Поставка снова пошла в путь после статуса «Просрочена»</b>\n\n"
                                f"Магазин: <b>{store.name}</b>\n"
                                f"Поставка: <b>№{supply.order_number}</b>\n"
                                f"Резерв ранее вернули на склад, но сейчас свободного остатка "
                                f"не хватает для повторного списания.\n\n"
                                f"Проверь фактическое движение товара и остатки на складе."
                            )
                            _in_app, email_count, push_count, sent_count = await _deliver_multichannel_notification(
                                db,
                                targets=acceptance_targets,
                                telegram=telegram,
                                telegram_recipients=acceptance_recipients,
                                title=f"Поставка #{supply.order_number} требует проверки",
                                text=text,
                                kind="supply_overdue_recovery",
                                action_url="/supplies",
                                severity="warning",
                                is_important=True,
                                email_flag="email_acceptance_status",
                            )
                            logger.info(
                                f"   ✅ Overdue recovery shortage notifications delivered: telegram={sent_count}, email={email_count}, web_push={push_count}"
                            )
                            await mark_processed(db, supply.id, "overdue_recovery_notification")
                            await db.commit()

                    elif supply.status == "CANCELLED":
                        # Поставка отменена ДО отправки - снимаем резерв
                        logger.info(f"   ❌ Supply cancelled - cancelling reservation")

                        cancelled_count = 0
                        cancellation_success = True
                        try:
                            if reserves:
                                for reserve in reserves:
                                    await service.cancel_reserve(
                                        db,
                                        warehouse_id=warehouse.id,
                                        variant_id=reserve.variant_id,
                                        quantity=reserve.quantity,
                                        supply_id=supply.id,
                                        commit=False
                                    )
                                    logger.info(f"   ✅ Cancelled reserve for {reserve.quantity} units")
                                    cancelled_count += 1
                            else:
                                logger.info(
                                    f"   ℹ️ Supply {supply.id} has no active reserve to cancel "
                                    f"(possibly released earlier after OVERDUE)"
                                )
                            supply.reserved_at = None

                            await mark_processed(db, supply.id, "cancellation")
                            await db.commit()
                        except Exception as e:
                            logger.error(f"   ❌ Cancellation rolled back for supply {supply.id}: {e}")
                            cancellation_success = False
                            cancelled_count = 0
                            await db.rollback()

                        # Отправляем уведомление об отмене
                        if cancellation_success and acceptance_targets:
                            if cancelled_count > 0:
                                text = (
                                    f"❌ <b>Поставка отменена</b>\n\n"
                                    f"Магазин: <b>{store.name}</b>\n"
                                    f"Поставка: <b>№{supply.order_number}</b>\n"
                                    f"Резерв снят, товары снова доступны на складе."
                                )
                            else:
                                text = (
                                    f"❌ <b>Поставка отменена</b>\n\n"
                                    f"Магазин: <b>{store.name}</b>\n"
                                    f"Поставка: <b>№{supply.order_number}</b>\n"
                                    f"Резерв уже был возвращен на склад ранее."
                                )
                            _in_app, email_count, push_count, sent_count = await _deliver_multichannel_notification(
                                db,
                                targets=acceptance_targets,
                                telegram=telegram,
                                telegram_recipients=acceptance_recipients,
                                title=f"Поставка #{supply.order_number} отменена",
                                text=text,
                                kind="supply_cancelled",
                                action_url="/supplies",
                                severity="warning",
                                is_important=True,
                                email_flag="email_acceptance_status",
                            )
                            logger.info(
                                f"   ✅ Cancellation notifications delivered: telegram={sent_count}, email={email_count}, web_push={push_count}"
                            )

                    elif supply.status in {"REJECTED_AT_SUPPLY_WAREHOUSE", "REPORT_REJECTED"}:
                        # Отказано в приемке на складе OZON - ТОЛЬКО УВЕДОМЛЕНИЕ!
                        logger.info(f"   ⚠️ Supply rejected/report issue at OZON warehouse - sending notification only")

                        # Проверяем, не отправляли ли уже уведомление
                        processed = await db.execute(
                            select(SupplyProcessing).where(
                                SupplyProcessing.supply_id == supply.id,
                                SupplyProcessing.processed_type == "rejection_notification"
                            )
                        )

                        if not processed.scalar_one_or_none():
                            # Получаем детали по товарам
                            items_text = ""
                            total_items = 0
                            total_quantity = 0

                            for reserve in reserves:
                                variant = await db.get(Variant, reserve.variant_id)
                                product = await db.get(Product, variant.product_id) if variant else None
                                items_text += f"  • {product.name if product else 'Unknown'} ({variant.sku if variant else 'Unknown'}): {reserve.quantity} шт.\n"
                                total_items += 1
                                total_quantity += reserve.quantity

                            if total_items > 0:
                                text = (
                                    f"⚠️ <b>Отказ в приемке поставки</b>\n\n"
                                    f"Магазин: <b>{store.name}</b>\n"
                                    f"Поставка: <b>№{supply.order_number}</b>\n"
                                    f"Склад Ozon: <b>{supply.storage.name if supply.storage else 'Неизвестно'}</b>\n"
                                    f"Позиций: <b>{total_items}</b>\n"
                                    f"Штук: <b>{total_quantity}</b>\n\n"
                                    f"Товары не возвращаем на склад автоматически.\n"
                                    f"Проверь фактический возврат и при необходимости сделай ручной приход.\n\n"
                                    f"Состав:\n{items_text}"
                                )
                                if rejection_targets:
                                    _in_app, email_count, push_count, sent_count = await _deliver_multichannel_notification(
                                        db,
                                        targets=rejection_targets,
                                        telegram=telegram,
                                        telegram_recipients=rejection_recipients,
                                        title=f"Отказ в приемке поставки #{supply.order_number}",
                                        text=text,
                                        kind="supply_rejection",
                                        action_url="/supplies",
                                        severity="warning",
                                        is_important=True,
                                        email_flag="email_rejection",
                                    )
                                    logger.info(
                                        f"   ✅ Rejection notifications delivered: telegram={sent_count}, email={email_count}, web_push={push_count}"
                                    )
                                else:
                                    logger.info("   ℹ️ No recipients subscribed to rejection notifications")

                                await mark_processed(db, supply.id, "rejection_notification")
                                await db.commit()

                    elif supply.status == "OVERDUE":
                        logger.info(f"   ⏰ Supply overdue - releasing reserve and sending notification")

                        processed = await db.execute(
                            select(SupplyProcessing).where(
                                SupplyProcessing.supply_id == supply.id,
                                SupplyProcessing.processed_type == "overdue_notification"
                            )
                        )

                        if not processed.scalar_one_or_none():
                            overdue_success = True
                            try:
                                released_count = 0
                                if reserves:
                                    released_count = await _release_supply_reserves(
                                        db=db,
                                        service=service,
                                        warehouse_id=warehouse.id,
                                        supply=supply,
                                        reserves=reserves,
                                    )
                                await mark_processed(db, supply.id, "overdue_reserve_released")
                                await mark_processed(db, supply.id, "overdue_notification")
                                await db.commit()
                            except Exception as e:
                                overdue_success = False
                                logger.error(f"   ❌ Overdue reserve release rolled back for supply {supply.id}: {e}")
                                await db.rollback()

                            if overdue_success:
                                text = (
                                    f"⏰ <b>Поставка просрочена</b>\n\n"
                                    f"Магазин: <b>{store.name}</b>\n"
                                    f"Поставка: <b>№{supply.order_number}</b>\n"
                                    f"Время отгрузки уже прошло, но поставка все еще не изменила статус "
                                    f"на «Принята на точке отгрузки».\n\n"
                                    f"Резерв вернули на склад автоматически."
                                )
                                if released_count > 0:
                                    text += f"\nОсвободили позиций: <b>{released_count}</b>."
                                text += (
                                    f"\n\nЕсли поставка позже все же уйдет в путь, "
                                    f"спишем товар со склада автоматически."
                                )
                                if acceptance_targets:
                                    _in_app, email_count, push_count, sent_count = await _deliver_multichannel_notification(
                                        db,
                                        targets=acceptance_targets,
                                        telegram=telegram,
                                        telegram_recipients=acceptance_recipients,
                                        title=f"Поставка #{supply.order_number} просрочена",
                                        text=text,
                                        kind="supply_overdue",
                                        action_url="/supplies",
                                        severity="warning",
                                        is_important=True,
                                        email_flag="email_acceptance_status",
                                    )
                                    logger.info(
                                        f"   ✅ Overdue notifications delivered: telegram={sent_count}, email={email_count}, web_push={push_count}"
                                    )
                                else:
                                    logger.info("   ℹ️ No recipients subscribed to acceptance-status notifications")

                    elif supply.status == "COMPLETED":
                        # Поставка завершена - проверяем расхождения
                        logger.info(f"   ✅ Supply completed - checking discrepancies")

                        # Получаем товары в поставке с accepted_quantity
                        items_stmt = select(SupplyItem).where(
                            SupplyItem.supply_id == supply.id,
                            SupplyItem.accepted_quantity.isnot(None)
                        )
                        items_result = await db.execute(items_stmt)
                        items = items_result.scalars().all()

                        logger.info(f"   Found {len(items)} items with accepted_quantity")

                        corrections = []
                        total_return = 0
                        total_loss = 0
                        correction_success = True

                        try:
                            for item in items:
                                if item.accepted_quantity < item.quantity:
                                    variant = await db.get(Variant, item.variant_id)
                                    diff = get_internal_units(item.quantity - item.accepted_quantity, variant)
                                    logger.info(
                                        f"      Item {item.variant_id}: sent={item.quantity}, accepted={item.accepted_quantity}, diff={diff}"
                                    )

                                    product = await db.get(Product, variant.product_id) if variant else None

                                    if owner_settings and owner_settings.discrepancy_mode == "correction":
                                        await service.return_from_shipment(
                                            db,
                                            warehouse_id=warehouse.id,
                                            variant_id=item.variant_id,
                                            quantity=diff,
                                            supply_id=supply.id,
                                            reason="acceptance_discrepancy",
                                            commit=False
                                        )
                                        total_return += diff

                                        corrections.append({
                                            "product": product.name if product else "Unknown",
                                            "sku": variant.sku if variant else "Unknown",
                                            "sent": item.quantity,
                                            "accepted": item.accepted_quantity,
                                            "returned": diff
                                        })
                                    else:
                                        total_loss += diff
                        except Exception as e:
                            logger.error(f"   ❌ Correction rolled back for supply {supply.id}: {e}")
                            correction_success = False
                            corrections = []
                            total_return = 0
                            total_loss = 0
                            await db.rollback()

                        # Отправляем уведомление о результатах приемки
                        if correction_success and (corrections or total_loss > 0):
                            if owner_settings and owner_settings.discrepancy_mode == "correction" and corrections:
                                text = f"🔄 <b>Расхождение по приемке</b>\n\n"
                                text += f"Магазин: <b>{store.name}</b>\n"
                                text += f"Поставка: <b>№{supply.order_number}</b>\n"
                                text += f"Недостающее количество вернули на склад.\n\n"
                                text += f"Детали:\n"

                                for c in corrections:
                                    text += (
                                        f"• {c['product']} — <code>{c['sku']}</code>\n"
                                        f"  Отправлено {c['sent']}, принято {c['accepted']}, "
                                        f"вернули {c['returned']} шт.\n"
                                    )

                                text += f"\n<b>Всего вернули: {total_return} шт.</b>"

                                if acceptance_targets:
                                    _in_app, email_count, push_count, sent_count = await _deliver_multichannel_notification(
                                        db,
                                        targets=acceptance_targets,
                                        telegram=telegram,
                                        telegram_recipients=acceptance_recipients,
                                        title=f"Расхождение по поставке #{supply.order_number}",
                                        text=text,
                                        kind="supply_discrepancy",
                                        action_url="/supplies",
                                        severity="warning",
                                        is_important=True,
                                        email_flag="email_losses",
                                    )
                                    logger.info(
                                        f"   ✅ Correction notifications delivered: telegram={sent_count}, email={email_count}, web_push={push_count}"
                                    )
                                else:
                                    logger.info("   ℹ️ No recipients subscribed to acceptance-status notifications")

                                await mark_processed(db, supply.id, "correction")
                                await db.commit()

                            elif owner_settings and owner_settings.discrepancy_mode == "loss" and total_loss > 0:
                                logger.info(
                                    f"   ℹ️ Loss mode - found {total_loss} units loss, will be sent by check_losses_task")
                                await db.commit()
                            else:
                                logger.info(f"   ✅ No discrepancies found")
                                await mark_processed(db, supply.id, "correction_check")
                                await db.commit()
                        elif correction_success:
                            if owner_settings and owner_settings.discrepancy_mode == "correction":
                                await mark_processed(db, supply.id, "correction_check")
                            await db.commit()

                await db.commit()
                logger.info(f"✅ Status check completed")

        except Exception as e:
            logger.error(f"❌ Error in check_supplies_status_task: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if telegram:
                await telegram.close()

    return get_event_loop().run_until_complete(_run())


@celery.task(name='worker.tasks.check_losses_task', bind=True)
def check_losses_task(self):
    """Проверка потерь при приемке товаров"""
    logger.info("🔍 Checking for losses in completed supplies")

    async def _run():
        telegram = None
        try:
            async with SessionLocal() as db:
                # Ищем за последние 7 дней
                seven_days_ago = datetime.now() - timedelta(days=7)

                logger.info(f"📅 Looking for completed supplies since: {seven_days_ago}")

                stmt = select(Supply).where(
                    Supply.status == "COMPLETED",
                    Supply.completed_at >= seven_days_ago
                ).order_by(Supply.completed_at.desc())

                result = await db.execute(stmt)
                all_supplies = result.scalars().all()

                # Фильтруем уже обработанные поставки
                supplies = []
                for supply in all_supplies:
                    # Проверяем различные типы обработки
                    processed = await db.execute(
                        select(SupplyProcessing).where(
                            SupplyProcessing.supply_id == supply.id,
                            SupplyProcessing.processed_type.in_([
                                "loss_notification", "correction", "loss_check"
                            ])
                        )
                    )
                    if not processed.scalar_one_or_none():
                        supplies.append(supply)
                    else:
                        logger.info(f"   ⏭️ Supply {supply.id} already processed, skipping")

                logger.info(f"📊 Found {len(supplies)} unprocessed completed supplies in last 7 days")

                if not supplies:
                    logger.info("✅ No unprocessed supplies found")
                    return

                telegram = TelegramService()

                for supply in supplies:
                    # Получаем участников кабинета, подписанных на уведомления о потерях
                    store = await db.get(Store, supply.store_id)
                    loss_targets = await _load_cabinet_notification_targets(
                        db,
                        owner_user_id=store.user_id,
                        notification_field="notify_losses",
                    )
                    loss_recipients = _notification_recipients(loss_targets)

                    logger.info(f"   Checking supply {supply.id} (completed at: {supply.completed_at})")

                    # Получаем товары с потерями, сгруппированные по продуктам
                    products_with_losses = await get_products_with_losses(db, supply.id)

                    if products_with_losses:
                        total_loss = sum(
                            item['variant']['loss']
                            for product_variants in products_with_losses.values()
                            for item in product_variants
                        )

                        logger.warning(f"⚠️ Losses detected in supply {supply.id}: total loss: {total_loss} pcs")

                        text = f"⚠️ <b>Обнаружены потери в поставке</b>\n\n"
                        text += f"Магазин: <b>{store.name}</b>\n"
                        text += f"Поставка: <b>№{supply.order_number}</b>\n"
                        text += f"Всего потеряно: <b>{total_loss} шт.</b>\n\n"
                        text += f"Детали:"

                        for product_id, variants in products_with_losses.items():
                            product_name = variants[0]['product_name']
                            text += f"\n\n<b>{product_name}</b>"

                            for item in variants:
                                variant = item['variant']
                                attr_parts = []
                                if variant['color']:
                                    attr_parts.append(f"цвет: {variant['color']}")
                                if variant['size']:
                                    attr_parts.append(f"размер: {variant['size']}")
                                if variant['pack_size'] > 1:
                                    attr_parts.append(f"упаковка: {variant['pack_size']} шт")

                                attr_text = f" ({', '.join(attr_parts)})" if attr_parts else ""

                                text += (
                                    f"\n• {attr_text[2:] if attr_text else 'Без характеристик'}: "
                                    f"отправлено {variant['quantity']}, принято {variant['accepted_quantity']}, "
                                    f"потеря <b>{variant['loss']}</b> шт."
                                )

                        text += f"\n\nПроверь поставку и фактическое расхождение на стороне Ozon."

                        if loss_targets:
                            _in_app, email_count, push_count, sent_count = await _deliver_multichannel_notification(
                                db,
                                targets=loss_targets,
                                telegram=telegram,
                                telegram_recipients=loss_recipients,
                                title=f"Потери по поставке #{supply.order_number}",
                                text=text,
                                kind="supply_loss",
                                action_url="/supplies",
                                severity="warning",
                                is_important=True,
                                email_flag="email_losses",
                            )
                            logger.info(
                                f"   ✅ Loss notifications delivered: telegram={sent_count}, email={email_count}, web_push={push_count}"
                            )
                        else:
                            logger.info("   ℹ️ No recipients subscribed to loss notifications")

                        # Отмечаем как обработанное
                        await mark_processed(db, supply.id, "loss_notification")
                    else:
                        logger.info(f"   ✅ No losses in supply {supply.id}")
                        await mark_processed(db, supply.id, "loss_check")

                await db.commit()
                logger.info(f"✅ Loss check completed for {len(supplies)} supplies")

        except Exception as e:
            logger.error(f"❌ Error in check_losses_task: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if telegram:
                await telegram.close()

    return get_event_loop().run_until_complete(_run())


@celery.task(name='worker.tasks.today_supplies_task')
def today_supplies_task():
    """Минутный диспетчер уведомлений о поставках на сегодня"""
    logger.info("📅 Dispatching today's supplies notifications")

    async def _run():
        telegram = None
        try:
            async with SessionLocal() as db:
                telegram = TelegramService()
                admin_chat_id = os.getenv("TELEGRAM_CHAT_ID")
                users = await _load_notification_users(db, notification_field="notify_today_supplies")

                for user, settings in users:
                    due, report_date = _today_supplies_due(settings)
                    if not due:
                        continue

                    if not await _acquire_notification_delivery(
                        user_id=user.id,
                        report_type="today_supplies",
                        report_date=report_date,
                    ):
                        continue

                    delivery_success = not bool(admin_chat_id or (settings and settings.telegram_chat_id))
                    in_app_count = 0
                    email_count = 0
                    web_push_count = 0
                    telegram_count = 0
                    admin_telegram_count = 0
                    supplies_list = await _build_user_today_supplies(db, get_cabinet_owner_id(user))
                    if not supplies_list:
                        empty_message = "📅 <b>Поставки на сегодня</b>\n\nНа сегодня поставок нет."
                        await create_in_app_notification(
                            db,
                            user_id=user.id,
                            kind="today_supplies",
                            title="Поставки на сегодня",
                            body="На сегодня поставок нет.",
                            action_url="/supplies",
                            severity="info",
                            is_important=False,
                        )
                        in_app_count += 1
                        push_count = await deliver_web_push_notification_if_enabled(
                            db,
                            user=user,
                            settings=settings,
                            title="Поставки на сегодня",
                            plain_text="На сегодня поставок нет.",
                            action_url="/supplies",
                            kind="today_supplies",
                            severity="info",
                        )
                        web_push_count += push_count
                        email_sent = await deliver_email_notification_if_enabled(
                            user=user,
                            settings=settings,
                            subject="Поставки на сегодня",
                            html_text=empty_message,
                            plain_text=html_to_text(empty_message),
                            email_flag="email_today_supplies",
                        )
                        email_count += 1 if email_sent else 0
                        delivery_success = delivery_success or email_sent or bool(push_count)

                        if settings and settings.telegram_chat_id:
                            user_sent = await telegram.send_message(settings.telegram_chat_id, empty_message)
                            delivery_success = delivery_success or user_sent
                            telegram_count += 1 if user_sent else 0
                            logger.info("✅ Sent empty today supplies report to user {}", user.email)

                        if admin_chat_id:
                            admin_sent = await telegram.send_message(
                                admin_chat_id,
                                f"{empty_message}\n\nПользователь: <b>{user.email}</b>",
                            )
                            delivery_success = delivery_success or admin_sent
                            admin_telegram_count += 1 if admin_sent else 0
                            logger.info("✅ Sent empty today supplies report copy to admin for user {}", user.email)
                        else:
                            logger.warning("⚠️ TELEGRAM_CHAT_ID not set")

                        next_supplies_list, next_report_date = await _build_user_next_supplies(db, get_cabinet_owner_id(user))
                        if next_supplies_list and next_report_date:
                            next_date_label = next_report_date.strftime("%d.%m.%Y")
                            next_title = f"Ближайшие поставки на {next_date_label}"
                            next_text = telegram.build_today_supplies_grouped_text(
                                next_supplies_list,
                                title=next_title,
                            )
                            await create_in_app_notification(
                                db,
                                user_id=user.id,
                                kind="next_supplies",
                                title=next_title,
                                body=html_to_text(next_text),
                                action_url="/supplies",
                                severity="info",
                                is_important=False,
                            )
                            in_app_count += 1
                            next_push_count = await deliver_web_push_notification_if_enabled(
                                db,
                                user=user,
                                settings=settings,
                                title=next_title,
                                plain_text=html_to_text(next_text),
                                action_url="/supplies",
                                kind="next_supplies",
                                severity="info",
                            )
                            web_push_count += next_push_count
                            next_email_sent = await deliver_email_notification_if_enabled(
                                user=user,
                                settings=settings,
                                subject=next_title,
                                html_text=next_text,
                                plain_text=html_to_text(next_text),
                                email_flag="email_today_supplies",
                            )
                            email_count += 1 if next_email_sent else 0
                            delivery_success = delivery_success or next_email_sent or bool(next_push_count)

                            if settings and settings.telegram_chat_id:
                                user_sent = await telegram.send_today_supplies_grouped(
                                    settings.telegram_chat_id,
                                    next_supplies_list,
                                    title=next_title,
                                )
                                delivery_success = delivery_success or user_sent
                                telegram_count += 1 if user_sent else 0
                                logger.info("✅ Sent nearest supplies report to user {}", user.email)

                            if admin_chat_id:
                                admin_sent = await telegram.send_today_supplies_grouped(
                                    admin_chat_id,
                                    next_supplies_list,
                                    title=f"{next_title} — {user.email}",
                                )
                                delivery_success = delivery_success or admin_sent
                                admin_telegram_count += 1 if admin_sent else 0
                                logger.info("✅ Sent nearest supplies report copy to admin for user {}", user.email)

                        await _record_notification_delivery_admin_event(
                            kind="today_supplies",
                            title="Поставки на сегодня",
                            user_email=user.email,
                            in_app_count=in_app_count,
                            email_count=email_count,
                            web_push_count=web_push_count,
                            telegram_count=telegram_count,
                            admin_telegram_count=admin_telegram_count,
                            details={
                                "report_date": report_date,
                                "has_today_supplies": False,
                                "has_next_supplies": bool(next_supplies_list and next_report_date),
                            },
                        )
                        await _mark_notification_delivery(
                            user_id=user.id,
                            report_type="today_supplies",
                            report_date=report_date,
                            success=delivery_success,
                        )
                        await db.commit()
                        continue

                    title = f"Поставки на сегодня — {user.email}"
                    text = telegram.build_today_supplies_grouped_text(
                        supplies_list,
                        title="Поставки на сегодня",
                    )
                    await create_in_app_notification(
                        db,
                        user_id=user.id,
                        kind="today_supplies",
                        title="Поставки на сегодня",
                        body=html_to_text(text),
                        action_url="/supplies",
                        severity="info",
                        is_important=True,
                    )
                    in_app_count += 1
                    push_count = await deliver_web_push_notification_if_enabled(
                        db,
                        user=user,
                        settings=settings,
                        title="Поставки на сегодня",
                        plain_text=html_to_text(text),
                        action_url="/supplies",
                        kind="today_supplies",
                        severity="info",
                    )
                    web_push_count += push_count
                    email_sent = await deliver_email_notification_if_enabled(
                        user=user,
                        settings=settings,
                        subject="Поставки на сегодня",
                        html_text=text,
                        plain_text=html_to_text(text),
                        email_flag="email_today_supplies",
                    )
                    email_count += 1 if email_sent else 0
                    delivery_success = delivery_success or email_sent or bool(push_count)
                    if settings and settings.telegram_chat_id:
                        user_sent = await telegram.send_today_supplies_grouped(
                            settings.telegram_chat_id,
                            supplies_list,
                            title="Поставки на сегодня",
                        )
                        delivery_success = delivery_success or user_sent
                        telegram_count += 1 if user_sent else 0
                        logger.info("✅ Sent today supplies report to user {}", user.email)

                    if admin_chat_id:
                        admin_sent = await telegram.send_today_supplies_grouped(
                            admin_chat_id,
                            supplies_list,
                            title=title,
                        )
                        delivery_success = delivery_success or admin_sent
                        admin_telegram_count += 1 if admin_sent else 0
                        logger.info("✅ Sent today supplies report copy to admin for user {}", user.email)
                    else:
                        logger.warning("⚠️ TELEGRAM_CHAT_ID not set")

                    await _record_notification_delivery_admin_event(
                        kind="today_supplies",
                        title="Поставки на сегодня",
                        user_email=user.email,
                        in_app_count=in_app_count,
                        email_count=email_count,
                        web_push_count=web_push_count,
                        telegram_count=telegram_count,
                        admin_telegram_count=admin_telegram_count,
                        details={
                            "report_date": report_date,
                            "has_today_supplies": True,
                            "supplies_count": len(supplies_list),
                        },
                    )
                    await _mark_notification_delivery(
                        user_id=user.id,
                        report_type="today_supplies",
                        report_date=report_date,
                        success=delivery_success,
                    )
                    await db.commit()

        except Exception as e:
            logger.error(f"❌ Error in today_supplies_task: {e}")
            await notify_backend_error("today_supplies_task", e)
            import traceback
            traceback.print_exc()
        finally:
            if telegram:
                await telegram.close()

    return get_event_loop().run_until_complete(_run())


async def _build_owner_monthly_closed_month_report(
    db,
    *,
    cabinet_owner_id: int,
    month_key: str,
) -> dict | None:
    stores_result = await db.execute(
        select(Store).where(Store.user_id == cabinet_owner_id, Store.is_active == True)  # noqa: E712
    )
    stores = list(stores_result.scalars().all())
    if not stores:
        return None

    store_ids = [store.id for store in stores]
    month_rows_result = await db.execute(
        select(StoreMonthFinance, Store)
        .join(Store, Store.id == StoreMonthFinance.store_id)
        .where(
            StoreMonthFinance.store_id.in_(store_ids),
            StoreMonthFinance.month == month_key,
            StoreMonthFinance.realization_available == True,  # noqa: E712
        )
    )
    month_rows = month_rows_result.all()
    if not month_rows:
        return None

    by_store_id = {store.id: (month_row, store) for month_row, store in month_rows}
    stores_payload: list[dict] = []
    sold_units = 0
    returned_units = 0
    returned_amount = 0.0
    revenue_amount = 0.0
    cogs = 0.0
    net_profit = 0.0
    stores_with_warning = 0
    stores_need_cost = 0
    stores_pending = 0

    for store in stores:
        pair = by_store_id.get(store.id)
        if not pair:
            stores_pending += 1
            stores_payload.append(
                {
                    "store_id": store.id,
                    "store_name": store.name,
                    "status": "pending",
                    "status_label": "Нет закрытого месяца",
                    "sold_units": 0,
                    "revenue_amount": 0.0,
                    "net_profit": 0.0,
                }
            )
            continue

        month_row, _store = pair
        sold_units += int(month_row.sold_units or 0)
        returned_units += int(month_row.returned_units or 0)
        returned_amount += float(month_row.returned_amount or 0)
        revenue_amount += float(month_row.revenue_amount or 0)
        cogs += float(month_row.cogs or 0)
        net_profit += float(month_row.net_profit or 0)
        if month_row.status == "ozon_warning":
            stores_with_warning += 1
        if month_row.status == "needs_cost":
            stores_need_cost += 1

        stores_payload.append(
            {
                "store_id": store.id,
                "store_name": store.name,
                "status": month_row.status,
                "status_label": _closed_month_status_label(month_row.status),
                "sold_units": int(month_row.sold_units or 0),
                "revenue_amount": round(float(month_row.revenue_amount or 0), 2),
                "net_profit": round(float(month_row.net_profit or 0), 2),
            }
        )

    return {
        "month": month_key,
        "month_label": _month_label(month_key),
        "stores_total": len(stores),
        "stores_included": len(month_rows),
        "stores_pending": stores_pending,
        "stores_with_warning": stores_with_warning,
        "stores_need_cost": stores_need_cost,
        "sold_units": sold_units,
        "returned_units": returned_units,
        "returned_amount": round(returned_amount, 2),
        "revenue_amount": round(revenue_amount, 2),
        "cogs": round(cogs, 2),
        "net_profit": round(net_profit, 2),
        "stores": stores_payload,
    }


async def _ensure_owner_closed_month_history_ready(
    db,
    *,
    cabinet_owner_id: int,
    month_key: str,
) -> None:
    stores_result = await db.execute(
        select(Store).where(Store.user_id == cabinet_owner_id, Store.is_active == True)  # noqa: E712
    )
    stores = list(stores_result.scalars().all())
    if not stores:
        return

    history_service = ClosedMonthHistoryService(db)
    scheduler = StoreSyncScheduler()

    for store in stores:
        existing_row = await history_service.get_month(
            store_id=store.id,
            owner_user_id=cabinet_owner_id,
            month=month_key,
        )
        if existing_row and existing_row.realization_available and existing_row.status not in {"pending", "error"}:
            continue

        snapshot = await cache_get_json(
            OzonFinanceSnapshotService.cache_key_for(str(store.client_id))
        )
        realization_snapshot = (snapshot or {}).get("realization_closed_month") or {}
        realization_period = str(realization_snapshot.get("period") or "").strip()
        realization_error = realization_snapshot.get("error")

        if realization_period != month_key or realization_error:
            logger.info(
                "⏳ Monthly closed month {} for store {} ({}) is not ready in finance snapshot yet",
                month_key,
                store.name,
                store.id,
            )
            continue

        scheduler_state = await scheduler.get_state(store.id)
        active = dict(scheduler_state.get("active") or {})
        closed_month_state = (get_store_sync_status(store.id).get("sync_kinds", {}) or {}).get("closed_months") or {}
        closed_month_status = str(closed_month_state.get("status") or "")

        if str(active.get("kind") or "").strip():
            logger.info(
                "⏳ Monthly report dispatcher waits to sync closed month {} for store {} ({}) because {} is active",
                month_key,
                store.name,
                store.id,
                active.get("kind"),
            )
            continue

        if closed_month_status in {"queued", "running"}:
            logger.info(
                "⏳ Monthly report dispatcher sees closed month {} already {} for store {} ({})",
                month_key,
                closed_month_status,
                store.name,
                store.id,
            )
            continue

        try:
            enqueue_closed_month_history_sync(store.id, months_back=1, start_month=month_key)
            logger.info(
                "📦 Monthly report dispatcher queued closed month {} sync for store {} ({})",
                month_key,
                store.name,
                store.id,
            )
        except Exception as exc:
            logger.warning(
                "⚠️ Failed to queue closed month {} sync for store {} ({}): {}",
                month_key,
                store.name,
                store.id,
                exc,
            )
            await notify_backend_error(
                "monthly_closed_month_history_sync_enqueue",
                exc,
                details={
                    "store_id": store.id,
                    "store_name": store.name,
                    "month": month_key,
                    "owner_user_id": cabinet_owner_id,
                },
            )


@celery.task(name='worker.tasks.monthly_closed_month_report_task')
def monthly_closed_month_report_task():
    """Проверяет с 1 по 8 число появление закрытого прошлого месяца и рассылает его сразу после готовности."""
    logger.info("🗓️ Dispatching monthly closed-month reports")

    async def _run():
        telegram = None
        try:
            async with SessionLocal() as db:
                telegram = TelegramService()
                admin_chat_id = os.getenv("TELEGRAM_CHAT_ID")
                users = await _load_notification_users(db, notification_field="notify_daily_report")
                owner_report_cache: dict[tuple[int, str], dict | None] = {}

                for user, settings in users:
                    due, month_key = _monthly_closed_month_report_due(settings)
                    if not due:
                        continue

                    owner_id = get_cabinet_owner_id(user)
                    cache_key = (owner_id, month_key)
                    if cache_key not in owner_report_cache:
                        await _ensure_owner_closed_month_history_ready(
                            db,
                            cabinet_owner_id=owner_id,
                            month_key=month_key,
                        )
                        owner_report_cache[cache_key] = await _build_owner_monthly_closed_month_report(
                            db,
                            cabinet_owner_id=owner_id,
                            month_key=month_key,
                        )

                    stats = owner_report_cache[cache_key]

                    if not await _acquire_notification_delivery(
                        user_id=user.id,
                        report_type="monthly_closed_month_report",
                        report_date=month_key,
                    ):
                        continue

                    delivery_success = not bool(admin_chat_id or (settings and settings.telegram_chat_id))
                    stores_pending = int(stats.get("stores_pending") or 0) if stats else 0
                    stores_with_warning = int(stats.get("stores_with_warning") or 0) if stats else 0
                    stores_need_cost = int(stats.get("stores_need_cost") or 0) if stats else 0

                    if (
                        not stats
                        or stores_pending > 0
                        or stores_with_warning > 0
                        or stores_need_cost > 0
                    ):
                        if stats:
                            logger.info(
                                "⏳ Monthly closed-month report for owner {} month {} is not user-ready yet: pending={}, warning={}, needs_cost={}",
                                owner_id,
                                month_key,
                                stores_pending,
                                stores_with_warning,
                                stores_need_cost,
                            )
                        await _mark_notification_delivery(
                            user_id=user.id,
                            report_type="monthly_closed_month_report",
                            report_date=month_key,
                            success=False,
                        )
                        continue

                    title = f"Ежемесячный отчет за {stats['month_label']}"
                    report_text = telegram.build_monthly_closed_month_report_text(stats, title=title)
                    await create_in_app_notification(
                        db,
                        user_id=user.id,
                        kind="monthly_report",
                        title=title,
                        body=html_to_text(report_text),
                        action_url="/closed-months",
                        severity="info",
                        is_important=True,
                    )
                    in_app_count = 1
                    push_count = await deliver_web_push_notification_if_enabled(
                        db,
                        user=user,
                        settings=settings,
                        title=title,
                        plain_text=html_to_text(report_text),
                        action_url="/closed-months",
                        kind="monthly_report",
                        severity="info",
                    )
                    web_push_count = push_count
                    email_sent = await deliver_email_notification_if_enabled(
                        user=user,
                        settings=settings,
                        subject=title,
                        html_text=report_text,
                        plain_text=html_to_text(report_text),
                        email_flag="email_daily_report",
                    )
                    email_count = 1 if email_sent else 0
                    delivery_success = delivery_success or email_sent or bool(push_count)

                    telegram_count = 0
                    if settings and settings.telegram_chat_id:
                        user_sent = await telegram.send_monthly_closed_month_report(
                            settings.telegram_chat_id,
                            stats,
                            title=title,
                        )
                        delivery_success = delivery_success or user_sent
                        telegram_count += 1 if user_sent else 0

                    admin_telegram_count = 0
                    if admin_chat_id:
                        admin_sent = await telegram.send_monthly_closed_month_report(
                            admin_chat_id,
                            stats,
                            title=f"{title} — {user.email}",
                        )
                        delivery_success = delivery_success or admin_sent
                        admin_telegram_count += 1 if admin_sent else 0

                    await _record_notification_delivery_admin_event(
                        kind="monthly_closed_month_report",
                        title=title,
                        user_email=user.email,
                        in_app_count=in_app_count,
                        email_count=email_count,
                        web_push_count=web_push_count,
                        telegram_count=telegram_count,
                        admin_telegram_count=admin_telegram_count,
                        details={
                            "month_key": month_key,
                            "stores_ready": int(stats.get("stores_ready") or 0),
                            "stores_total": int(stats.get("stores_total") or 0),
                        },
                    )

                    await _mark_notification_delivery(
                        user_id=user.id,
                        report_type="monthly_closed_month_report",
                        report_date=month_key,
                        success=delivery_success,
                    )
                    await db.commit()
        except Exception as e:
            logger.error(f"❌ Error in monthly_closed_month_report_task: {e}")
            await notify_backend_error("monthly_closed_month_report_task", e)
            import traceback
            traceback.print_exc()
        finally:
            if telegram:
                await telegram.close()

    return get_event_loop().run_until_complete(_run())


@celery.task(name='worker.tasks.price_risk_alerts_task')
def price_risk_alerts_task(store_id: int | None = None):
    """Проверяет текущую юнит-экономику SKU и предупреждает, если товар ушел в низкую маржу/ноль/минус."""
    logger.info("💸 Checking price risk alerts{}", f" for store {store_id}" if store_id else "")

    async def _run():
        telegram = None
        try:
            async with SessionLocal() as db:
                telegram = TelegramService()
                service = PriceRiskAlertsService(db)
                candidates = await service.evaluate_all(store_id=store_id)
                logger.info("📊 Price risk candidates ready: {}", len(candidates))

                for candidate in candidates:
                    store = candidate.store
                    owner_id = int(store.user_id)
                    targets = await _load_cabinet_notification_targets(
                        db,
                        owner_user_id=owner_id,
                        notification_field="notify_losses",
                    )
                    if not targets:
                        logger.info(
                            "ℹ️ No recipients subscribed to price-risk alerts for store {} ({})",
                            store.name,
                            store.id,
                        )
                        continue

                    recipients = _notification_recipients(targets)
                    title = f"{candidate.title}: {candidate.row.get('title') or candidate.row.get('offer_id')}"
                    _in_app, email_count, push_count, telegram_count = await _deliver_multichannel_notification(
                        db,
                        targets=targets,
                        telegram=telegram,
                        telegram_recipients=recipients,
                        title=title,
                        text=candidate.html_text,
                        kind="unit_price_risk",
                        action_url=candidate.action_url,
                        severity=candidate.severity,
                        is_important=True,
                        email_flag="email_losses",
                    )
                    await _record_notification_delivery_admin_event(
                        kind="unit_price_risk",
                        title=title,
                        user_email=targets[0][0].email if targets else store.user.email if getattr(store, "user", None) else f"user:{owner_id}",
                        in_app_count=_in_app,
                        email_count=email_count,
                        web_push_count=push_count,
                        telegram_count=telegram_count,
                        admin_telegram_count=0,
                        details={
                            "store_id": store.id,
                            "store_name": store.name,
                            "offer_id": str(candidate.row.get("offer_id") or ""),
                            "status": _price_risk_status_label(candidate.status),
                        },
                    )
                    logger.info(
                        "✅ Price risk alert delivered for store {} offer {}: status={}, telegram={}, email={}, web_push={}",
                        store.id,
                        candidate.row.get("offer_id"),
                        _price_risk_status_label(candidate.status),
                        telegram_count,
                        email_count,
                        push_count,
                    )

                await db.commit()
        except Exception as e:
            logger.error(f"❌ Error in price_risk_alerts_task: {e}")
            await notify_backend_error("price_risk_alerts_task", e, details={"store_id": store_id})
            import traceback
            traceback.print_exc()
        finally:
            if telegram:
                await telegram.close()

    return get_event_loop().run_until_complete(_run())


@celery.task(name='worker.tasks.admin_overdue_supplies_digest_task')
def admin_overdue_supplies_digest_task():
    """Шлет в админский Telegram конкретные просроченные активные поставки по магазинам."""
    logger.info("🚨 Building admin overdue supplies digest")

    async def _run():
        try:
            now = datetime.now()
            async with SessionLocal() as db:
                stmt = (
                    select(Supply, Store)
                    .join(Store, Store.id == Supply.store_id)
                    .where(
                        Supply.status == "READY_TO_SUPPLY",
                        Supply.timeslot_from.is_not(None),
                        Supply.timeslot_from < now,
                    )
                    .order_by(Store.id.asc(), Supply.timeslot_from.asc(), Supply.id.asc())
                )
                rows = (await db.execute(stmt)).all()

                if not rows:
                    logger.info("✅ No overdue active supplies found for admin digest")
                    return

                grouped: dict[int, dict] = {}
                for supply, store in rows:
                    bucket = grouped.setdefault(
                        int(store.id),
                        {
                            "store": store,
                            "supplies": [],
                        },
                    )
                    bucket["supplies"].append(supply)

                for store_id, payload in grouped.items():
                    store: Store = payload["store"]
                    supplies: list[Supply] = payload["supplies"]
                    top_supplies = supplies[:5]

                    lines = [
                        "⚠️ <b>Поставки не отгружены вовремя</b>",
                        "",
                        f"<b>Магазин:</b> {store.name}",
                        f"<b>Store ID:</b> {store.id}",
                        f"<b>Client ID:</b> {store.client_id}",
                        f"<b>Всего проблемных:</b> {len(supplies)}",
                        "",
                        "<b>Первые проблемные поставки:</b>",
                    ]

                    for supply in top_supplies:
                        lines.extend(
                            [
                                f"• <b>№{supply.order_number}</b> · Supply ID: {supply.id}",
                                "  Статус: Готова к отгрузке",
                                f"  Таймслот: {_format_supply_timeslot_short(supply.timeslot_from, supply.timeslot_to)}",
                                f"  Не отгружена уже: {_format_overdue_duration(supply.timeslot_from, now)}",
                            ]
                        )

                    if len(supplies) > len(top_supplies):
                        lines.append(f"")
                        lines.append(f"И еще {len(supplies) - len(top_supplies)} поставок.")

                    text = "\n".join(lines)
                    top_ids = ",".join(str(s.id) for s in top_supplies)
                    dedupe_key = f"overdue-digest:{store.id}:{len(supplies)}:{top_ids}"

                    await record_admin_event(
                        "overdue_supplies_digest",
                        f"Поставки не отгружены вовремя: {store.name}",
                        severity="warning",
                        details={
                            "store_id": store.id,
                            "store_name": store.name,
                            "client_id": store.client_id,
                            "overdue_count": len(supplies),
                            "status": "READY_TO_SUPPLY",
                            "top_supply_ids": [int(s.id) for s in top_supplies],
                            "top_order_numbers": [str(s.order_number) for s in top_supplies],
                        },
                    )

                    sent = await send_admin_broadcast_message(
                        db,
                        text,
                        dedupe_key=dedupe_key,
                        dedupe_ttl_seconds=2 * 60 * 60,
                    )
                    logger.info(
                        "✅ Overdue supplies digest processed for store {}: sent={}, overdue_count={}",
                        store.id,
                        sent,
                        len(supplies),
                    )

        except Exception as e:
            logger.error(f"❌ Error in admin_overdue_supplies_digest_task: {e}")
            await notify_backend_error("admin_overdue_supplies_digest_task", e)
            import traceback
            traceback.print_exc()

    return get_event_loop().run_until_complete(_run())


@celery.task(name='worker.tasks.daily_report_task')
def daily_report_task():
    """Минутный диспетчер ежедневных отчетов"""
    logger.info("📊 Dispatching daily reports")

    async def _run():
        telegram = None
        try:
            async with SessionLocal() as db:
                telegram = TelegramService()
                admin_chat_id = os.getenv("TELEGRAM_CHAT_ID")
                users = await _load_notification_users(db, notification_field="notify_daily_report")
                owner_report_cache: dict[tuple[int, str], tuple[dict, list[dict]]] = {}

                for user, settings in users:
                    due, report_date = _daily_report_due(settings)
                    if not due:
                        continue

                    if not await _acquire_notification_delivery(
                        user_id=user.id,
                        report_type="daily_report",
                        report_date=report_date,
                    ):
                        continue

                    delivery_success = not bool(admin_chat_id or (settings and settings.telegram_chat_id))
                    owner_id = get_cabinet_owner_id(user)
                    cache_key = (owner_id, report_date)
                    if cache_key not in owner_report_cache:
                        owner_report_cache[cache_key] = await _build_user_daily_report(db, owner_id, report_date)
                    stats, store_stats = owner_report_cache[cache_key]
                    report_text = telegram.build_daily_report_text(
                        stats,
                        title="Ежедневный отчет",
                        store_stats=store_stats,
                    )
                    await create_in_app_notification(
                        db,
                        user_id=user.id,
                        kind="daily_report",
                        title="Ежедневный отчет",
                        body=html_to_text(report_text),
                        action_url="/notifications",
                        severity="info",
                        is_important=True,
                    )
                    in_app_count = 1
                    push_count = await deliver_web_push_notification_if_enabled(
                        db,
                        user=user,
                        settings=settings,
                        title="Ежедневный отчет",
                        plain_text=html_to_text(report_text),
                        action_url="/notifications",
                        kind="daily_report",
                        severity="info",
                    )
                    web_push_count = push_count
                    email_sent = await deliver_email_notification_if_enabled(
                        user=user,
                        settings=settings,
                        subject="Ежедневный отчет",
                        html_text=report_text,
                        plain_text=html_to_text(report_text),
                        email_flag="email_daily_report",
                    )
                    email_count = 1 if email_sent else 0
                    delivery_success = delivery_success or email_sent or bool(push_count)

                    telegram_count = 0
                    if settings and settings.telegram_chat_id:
                        user_sent = await telegram.send_daily_report(
                            settings.telegram_chat_id,
                            stats,
                            title="Ежедневный отчет",
                            store_stats=store_stats,
                        )
                        delivery_success = delivery_success or user_sent
                        telegram_count += 1 if user_sent else 0
                        logger.info("✅ Daily report sent to user {}", user.email)

                    admin_telegram_count = 0
                    if admin_chat_id:
                        admin_sent = await telegram.send_daily_report(
                            admin_chat_id,
                            stats,
                            title=f"Ежедневный отчет пользователя {user.email}",
                            store_stats=store_stats,
                        )
                        delivery_success = delivery_success or admin_sent
                        admin_telegram_count += 1 if admin_sent else 0
                        logger.info("✅ Daily report copy sent to admin for user {}", user.email)
                    else:
                        logger.warning("⚠️ TELEGRAM_CHAT_ID not set")

                    await _record_notification_delivery_admin_event(
                        kind="daily_report",
                        title="Ежедневный отчет",
                        user_email=user.email,
                        in_app_count=in_app_count,
                        email_count=email_count,
                        web_push_count=web_push_count,
                        telegram_count=telegram_count,
                        admin_telegram_count=admin_telegram_count,
                        details={
                            "report_date": report_date,
                            "stores_count": len(store_stats or []),
                        },
                    )
                    await _mark_notification_delivery(
                        user_id=user.id,
                        report_type="daily_report",
                        report_date=report_date,
                        success=delivery_success,
                    )
                    await db.commit()

        except Exception as e:
            logger.error(f"❌ Error in daily_report_task: {e}")
            await notify_backend_error("daily_report_task", e)
            import traceback
            traceback.print_exc()
        finally:
            if telegram:
                await telegram.close()

    return get_event_loop().run_until_complete(_run())
