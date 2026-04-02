from worker.worker import celery, get_event_loop
import asyncio
import os
import sys
import time
import threading
from types import SimpleNamespace

from billiard.exceptions import SoftTimeLimitExceeded
from loguru import logger
from redis import Redis
from sqlalchemy import select

sys.path.insert(0, '/app')
sys.path.insert(0, '/app/backend')

logger.info(f"Python path: {sys.path}")
logger.info(f"Loading tasks from {__file__}")

redis_client = Redis.from_url(
    os.getenv("REDIS_URL", "redis://redis:6379"),
    decode_responses=True,
)

BACKGROUND_SYNC_KINDS = ("products", "supplies", "stocks", "reports", "finance")
BACKGROUND_COOLDOWN_SECONDS = 15 * 60


def scheduler() -> "StoreSyncScheduler":
    return StoreSyncScheduler(cooldown_seconds=BACKGROUND_COOLDOWN_SECONDS)


try:
    from app.services.sync_service import SyncService
    from app.services.ozon.client import OzonClient
    from app.services.ozon.finance_snapshot_service import OzonFinanceSnapshotService
    from app.services.ozon.report_service import OzonReportService
    from app.services.ozon.report_snapshot_service import OzonReportSnapshotService
    from app.services.closed_month_history_service import ClosedMonthHistoryService
    from app.services.export_excel_service import (
        build_closed_months_workbook,
        build_shipments_workbook,
        build_warehouse_workbook,
        export_file_path,
    )
    from app.services.export_status import (
        acquire_export_lock,
        mark_export_failed,
        mark_export_queued,
        mark_export_running,
        mark_export_success,
        release_export_lock,
    )
    from app.api.warehouse_router import get_warehouse_overview
    from app.api.shipments_router import get_shipments
    from app.core.dependencies import get_current_user
    from app.services.cabinet_access import get_cabinet_owner_id
    from app.services.shipments_cache import invalidate_shipments_response_cache
    from app.services.sync_status import (
        get_store_sync_status,
        mark_store_kind_failed,
        mark_store_kind_progress,
        mark_store_kind_queued,
        mark_store_kind_running,
        mark_store_kind_skipped,
        mark_store_kind_success,
        mark_store_sync_failed,
        mark_store_sync_queued,
        mark_store_sync_running,
        mark_store_sync_success,
    )
    from app.services.sync_locks import sync_lock_key
    from app.services.sync_scheduler import StoreSyncScheduler
    from app.services.sync_dispatcher import enqueue_post_full_follow_up_if_any
    from app.services.sync_intervals import (
        BackgroundSyncKind,
        SYNC_INTERVAL_DEFAULTS,
        SYNC_INTERVAL_FIELD_BY_KIND,
        format_sync_interval_label,
        is_background_sync_due,
    )
    from app.services.admin_notifications import (
        deliver_pending_supply_notification_events,
        notify_sync_partial,
        notify_sync_skipped,
        notify_sync_success,
    )
    from app.services.bootstrap_sync import (
        BOOTSTRAP_STATE_COMPLETED,
        BOOTSTRAP_STATE_FAILED,
        is_bootstrap_completed_sync,
        mark_bootstrap_state_sync,
    )
    from app.models.store import Store
    from app.models.user import User
    from app.database import SessionLocal
    from app.models.user_settings import UserSettings
    from app.utils.encryption import decrypt_api_key

    logger.info("✅ Successfully imported backend modules")
except Exception as e:
    logger.error(f"❌ Failed to import backend modules: {e}")
    import traceback
    traceback.print_exc()


def run_async(coro):
    result: dict[str, object] = {}
    error: dict[str, BaseException] = {}

    def _runner():
        try:
            loop = get_event_loop()
            if loop.is_running():
                raise RuntimeError("This event loop is already running")
            result["value"] = loop.run_until_complete(coro)
        except Exception as e:
            error["value"] = e

    try:
        current_loop = asyncio.get_event_loop()
        loop_is_running = current_loop.is_running()
    except RuntimeError:
        loop_is_running = False

    if loop_is_running:
        thread = threading.Thread(target=_runner, daemon=True)
        thread.start()
        thread.join()
    else:
        _runner()

    if "value" in error:
        logger.error(f"Error in async task: {error['value']}")
        raise error["value"]
    return result.get("value")


def acquire_store_sync_lock(store_id: int, ttl_seconds: int = 90 * 60) -> bool:
    return bool(redis_client.set(sync_lock_key("full", store_id), "1", nx=True, ex=ttl_seconds))


def release_store_sync_lock(store_id: int) -> None:
    redis_client.delete(sync_lock_key("full", store_id))


def acquire_named_sync_lock(kind: str, store_id: int, ttl_seconds: int = 45 * 60) -> bool:
    return bool(redis_client.set(sync_lock_key(kind, store_id), "1", nx=True, ex=ttl_seconds))


def release_named_sync_lock(kind: str, store_id: int) -> None:
    redis_client.delete(sync_lock_key(kind, store_id))


def has_named_sync_lock(kind: str, store_id: int) -> bool:
    return bool(redis_client.exists(sync_lock_key(kind, store_id)))


async def _scheduler_try_start(store_id: int, kind: str, task_id: str | None, source: str):
    return await scheduler().try_start(store_id, kind=kind, task_id=task_id, source=source)


async def _scheduler_finish(store_id: int, kind: str, task_id: str | None, success: bool, error: str | None = None):
    await scheduler().finish(store_id, kind=kind, task_id=task_id, success=success, error=error)


async def _scheduler_get_state(store_id: int):
    return await scheduler().get_state(store_id)


async def _scheduler_mark_background_requested(store_id: int, kind: str):
    return await scheduler().mark_background_requested(store_id, kind=kind)


def background_cooldown_key(store_id: int) -> str:
    return f"sync:background:cooldown:{store_id}"


def set_background_cooldown(store_id: int, ttl_seconds: int = BACKGROUND_COOLDOWN_SECONDS) -> None:
    try:
        redis_client.set(background_cooldown_key(store_id), "1", ex=max(int(ttl_seconds), 1))
    except Exception as e:
        logger.warning(f"Failed to set background sync cooldown for store {store_id}: {e}")


def clear_background_cooldown(store_id: int) -> None:
    try:
        redis_client.delete(background_cooldown_key(store_id))
    except Exception as e:
        logger.warning(f"Failed to clear background sync cooldown for store {store_id}: {e}")


def background_cooldown_remaining_seconds(store_id: int) -> int:
    try:
        ttl = redis_client.ttl(background_cooldown_key(store_id))
        return max(int(ttl or 0), 0) if ttl and ttl > 0 else 0
    except Exception as e:
        logger.warning(f"Failed to read background sync cooldown for store {store_id}: {e}")
        return 0


def should_delay_background_sync(store_id: int) -> tuple[bool, int]:
    remaining = background_cooldown_remaining_seconds(store_id)
    return remaining > 0, remaining


def _cooldown_message(remaining_seconds: int) -> str:
    remaining_minutes = max(1, (remaining_seconds + 59) // 60)
    return f"Ожидает запуск после полной синхронизации (~{remaining_minutes} мин.)"


def mark_background_kind_waiting(kind: str, store_id: int, remaining_seconds: int) -> None:
    try:
        mark_store_kind_queued(store_id, kind, _cooldown_message(remaining_seconds))
    except TypeError:
        mark_store_kind_queued(store_id, kind)


def mark_background_kind_queued_or_running(kind: str, store_id: int, message: str) -> None:
    current = get_store_sync_status(store_id).get("sync_kinds", {}).get(kind) or {}
    if has_named_sync_lock(kind, store_id) or current.get("status") == "running":
        current_message = current.get("message") or "Синхронизация выполняется"
        mark_store_kind_running(store_id, kind, current_message)
        return

    mark_store_kind_queued(store_id, kind, message)


def closed_months_are_pending_or_running(store_id: int) -> bool:
    if has_named_sync_lock("closed_months", store_id):
        return True

    state = run_async(_scheduler_get_state(store_id))
    active = dict(state.get("active") or {})
    if str(active.get("kind") or "") == "closed_months":
        return True

    queued_after_full = dict(state.get("queued_after_full") or {})
    return str(queued_after_full.get("kind") or "") == "closed_months"


def closed_months_wait_message() -> str:
    return "Ожидает завершения выгрузки закрытых месяцев"


def mark_background_kind_waiting_for_closed_months(kind: str, store_id: int) -> None:
    mark_store_kind_queued(store_id, kind, closed_months_wait_message())


def dispatch_deferred_background_syncs(store_id: int, kinds: list[str]) -> list[str]:
    resumed: list[str] = []
    for kind in kinds:
        if has_named_sync_lock("full", store_id) or has_named_sync_lock("closed_months", store_id):
            break
        if kind == "products":
            mark_store_kind_queued(store_id, "products", "Отложенная синхронизация товаров в очереди")
            sync_products_task.delay(store_id, "background")
        elif kind == "supplies":
            mark_store_kind_queued(store_id, "supplies", "Отложенная синхронизация поставок в очереди")
            sync_supplies_task.delay(store_id, 1, "background")
        elif kind == "stocks":
            mark_store_kind_queued(store_id, "stocks", "Отложенная синхронизация остатков в очереди")
            sync_stocks_task.delay(store_id, "background")
        elif kind == "reports":
            mark_store_kind_queued(store_id, "reports", "Отложенная синхронизация отчетов в очереди")
            sync_report_snapshots_task.delay(store_id, 30)
        elif kind == "finance":
            mark_store_kind_queued(store_id, "finance", "Отложенная синхронизация финансов в очереди")
            sync_finance_snapshots_task.delay(store_id, 62)
        else:
            continue
        resumed.append(kind)
    return resumed


async def _resume_deferred_background_syncs_for_store(store_id: int) -> list[str]:
    queued_kinds = await scheduler().drain_ready_background_queue(store_id)
    if not queued_kinds:
        return []
    resumed = dispatch_deferred_background_syncs(store_id, list(queued_kinds))
    if resumed:
        logger.info(
            "▶️ Resumed deferred background syncs for store {} after closed month export: {}",
            store_id,
            ", ".join(resumed),
        )
    return resumed


async def _get_store_context(db, store_id: int):
    result = await db.execute(
        select(Store, User.email)
        .join(User, User.id == Store.user_id)
        .where(Store.id == store_id)
    )
    row = result.first()
    if not row:
        return None, None, None

    store, user_email = row
    return store, store.name, user_email


async def _get_user_context(db, user_id: int):
    result = await db.execute(select(User).where(User.id == user_id))
    return result.scalar_one_or_none()


async def _invalidate_shipments_cache_for_store(store: Store) -> None:
    try:
        await invalidate_shipments_response_cache(owner_user_id=store.user_id, store_id=store.id)
    except Exception as e:
        logger.warning(f"Failed to invalidate shipments cache for store {store.id}: {e}")


async def _get_active_store_ids():
    async with SessionLocal() as db:
        result = await db.execute(
            select(Store.id).where(Store.is_active == True)  # noqa: E712
        )
        return [row[0] for row in result.all()]


async def _get_background_ready_store_ids():
    store_ids = await _get_active_store_ids()
    return [store_id for store_id in store_ids if is_bootstrap_completed_sync(store_id)]


async def _get_background_ready_store_candidates(kind: "BackgroundSyncKind") -> list[tuple[int, int]]:
    field_name = SYNC_INTERVAL_FIELD_BY_KIND[kind]
    default_interval = SYNC_INTERVAL_DEFAULTS[kind]
    interval_column = getattr(UserSettings, field_name)

    async with SessionLocal() as db:
        result = await db.execute(
            select(Store.id, interval_column)
            .outerjoin(UserSettings, UserSettings.user_id == Store.user_id)
            .where(Store.is_active == True)  # noqa: E712
        )
        rows = result.all()

    candidates: list[tuple[int, int]] = []
    for store_id, interval_minutes in rows:
        if not is_bootstrap_completed_sync(store_id):
            continue
        effective_interval = int(interval_minutes or default_interval)
        candidates.append((store_id, effective_interval))
    return candidates


async def _notify_user_full_sync_success(store, *, trigger: str, duration_seconds: float | int | None = None, send_telegram: bool = True):
    if not send_telegram:
        return False

    async with SessionLocal() as db:
        result = await db.execute(select(UserSettings).where(UserSettings.user_id == store.user_id))
        settings_row = result.scalar_one_or_none()
    chat_id = getattr(settings_row, "telegram_chat_id", None) if settings_row else None
    if not chat_id:
        return False

    from app.services.telegram_service import TelegramService

    trigger_label = {
        "store_created": "после подключения магазина",
        "startup": "после перезапуска проекта",
        "manual": "по ручному запуску",
    }.get(trigger, "после полной синхронизации")
    duration_label = f"\n<b>Длительность:</b> {round(float(duration_seconds), 1)} сек." if duration_seconds is not None else ""
    text = (
        "✅ <b>Полная синхронизация завершена успешно</b>\n\n"
        f"<b>Магазин:</b> {store.name}\n"
        f"<b>Client-ID:</b> {store.client_id}\n"
        f"<b>Сценарий:</b> {trigger_label}{duration_label}\n\n"
        "Загружены товары, остатки, поставки, отчёты и финансы."
    )
    telegram = TelegramService()
    try:
        return await telegram.send_message(chat_id, text)
    finally:
        await telegram.close()


try:
    from worker.reserve_tasks import (
        reserve_ready_supplies_task,
        check_supplies_status_task,
        check_losses_task,
        price_risk_alerts_task,
        today_supplies_task,
        daily_report_task,
        monthly_closed_month_report_task,
    )
    logger.info("✅ Successfully imported reserve tasks")
except Exception as e:
    logger.error(f"❌ Failed to import reserve tasks: {e}")
    import traceback
    traceback.print_exc()


@celery.task(name='worker.tasks.test_task')
def test_task():
    logger.info("🧪 Test task is working!")
    return {"status": "ok", "message": "Test task completed"}


@celery.task(
    name='worker.tasks.sync_products_task',
    bind=True,
    max_retries=3,
    soft_time_limit=15 * 60,
    time_limit=18 * 60,
)
def sync_products_task(self, store_id: int, source: str = "background"):
    logger.info(f"📦 Starting products sync task for store {store_id}")
    started_at = time.perf_counter()
    task_id = self.request.id
    sync_label = "Ручная синхронизация товаров" if source == "manual" else "Фоновая синхронизация товаров"

    decision = run_async(_scheduler_try_start(store_id, "products", task_id, "manual" if source == "manual" else "background"))
    if not decision.allowed or not acquire_named_sync_lock("products", store_id):
        logger.warning(f"⏳ Products sync blocked for store {store_id}: {decision.message}")
        mark_store_kind_skipped(store_id, "products", decision.message)
        return {"status": "skipped", "store_id": store_id, "reason": decision.action}

    mark_store_kind_running(store_id, "products")

    async def _run():
        async with SessionLocal() as db:
            store, store_name, user_email = await _get_store_context(db, store_id)
            if not store:
                logger.error(f"❌ Store {store_id} not found")
                return {"status": "missing_store", "store_id": store_id}

            logger.info(f"✅ Found store: {store_name} (ID: {store.id})")
            service = SyncService(db)
            await service.sync_products_for_store(store)
            await _invalidate_shipments_cache_for_store(store)

            await notify_sync_success(
                sync_type=sync_label,
                store_id=store_id,
                store_name=store_name,
                user_email=user_email,
                task_id=task_id,
                duration_seconds=time.perf_counter() - started_at,
                send_telegram=False,
            )
            price_risk_alerts_task.delay(store_id)

            mark_store_kind_success(store_id, "products")
            logger.info(f"✅ Products sync completed for store {store_id}")
            return {"status": "success", "store_id": store_id}

    try:
        result = run_async(_run())
        run_async(_scheduler_finish(store_id, "products", task_id, True))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        return result
    except SoftTimeLimitExceeded as e:
        logger.error(f"⏰ Products sync soft time limit exceeded for store {store_id}")
        mark_store_kind_failed(store_id, "products", "SoftTimeLimitExceeded()")
        run_async(_scheduler_finish(store_id, "products", task_id, False, "SoftTimeLimitExceeded()"))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        raise e
    except Exception as e:
        logger.error(f"❌ Task failed: {e}")
        mark_store_kind_failed(store_id, "products", str(e))
        run_async(_scheduler_finish(store_id, "products", task_id, False, str(e)))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        self.retry(countdown=60, exc=e)
    finally:
        release_named_sync_lock("products", store_id)


@celery.task(name='worker.tasks.sync_products_all')
def sync_products_all():
    async def _run():
        store_candidates = await _get_background_ready_store_candidates("products")
        if not store_candidates:
            logger.info("ℹ️ No active stores found for products sync")
            return
        for store_id, interval_minutes in store_candidates:
            if has_named_sync_lock("full", store_id):
                logger.info(f"ℹ️ Skipping background products scheduling for store {store_id} while full sync is active")
                continue
            if closed_months_are_pending_or_running(store_id):
                logger.info(
                    f"ℹ️ Skipping background products scheduling for store {store_id} while closed month export is queued/running"
                )
                mark_background_kind_waiting_for_closed_months("products", store_id)
                continue

            if not is_background_sync_due(store_id, "products", interval_minutes):
                logger.debug(
                    f"⏭️ Skipping background products scheduling for store {store_id}: "
                    f"owner cadence not due yet ({format_sync_interval_label('products', interval_minutes)})"
                )
                continue

            should_wait, remaining_seconds = should_delay_background_sync(store_id)
            if should_wait:
                logger.info(
                    f"⏱️ Delaying background products scheduling for store {store_id} for {remaining_seconds}s after full sync"
                )
                mark_background_kind_waiting("products", store_id, remaining_seconds)
                continue

            decision = await _scheduler_mark_background_requested(store_id, "products")
            if decision.allowed:
                mark_store_kind_queued(store_id, "products")
                sync_products_task.delay(store_id, "background")
            else:
                message = decision.message
                if decision.remaining_seconds > 0:
                    message = _cooldown_message(decision.remaining_seconds)
                mark_background_kind_queued_or_running("products", store_id, message)

    return run_async(_run())


@celery.task(
    name='worker.tasks.sync_full_task',
    bind=True,
    max_retries=1,
    soft_time_limit=60 * 60,
    time_limit=65 * 60,
)
def sync_full_task(
    self,
    store_id: int,
    months_back: int = 2,
    skip_products: bool = False,
    bootstrap: bool = False,
    trigger: str = 'manual',
):
    logger.info(f"🚚 Starting full sync task for store {store_id}")
    started_at = time.perf_counter()
    task_id = self.request.id
    effective_months_back = months_back if bootstrap else min(months_back, 1)
    bootstrap_in_effect = bootstrap or not is_bootstrap_completed_sync(store_id)

    decision = run_async(_scheduler_try_start(store_id, "full", task_id, trigger if trigger in ("manual", "startup", "store_created") else "system"))
    if not decision.allowed:
        logger.warning(f"⏳ Full sync blocked for store {store_id}: {decision.message}")
        mark_store_kind_skipped(store_id, "full", decision.message)
        return {"status": "skipped", "store_id": store_id, "reason": decision.action}

    if not acquire_store_sync_lock(store_id):
        logger.warning(f"⏳ Full sync already running for store {store_id}, skipping duplicate task")
        mark_store_kind_skipped(store_id, "full", "Полная синхронизация уже выполняется")
        return {"status": "skipped", "store_id": store_id, "reason": "already_running"}

    clear_background_cooldown(store_id)
    mark_store_sync_queued(store_id, task_id)
    mark_store_kind_queued(
        store_id,
        "full",
        "Первая полная синхронизация в очереди" if bootstrap_in_effect else "Полная синхронизация в очереди",
    )
    mark_store_sync_running(store_id, task_id)
    mark_store_kind_running(
        store_id,
        "full",
        "Первая полная синхронизация выполняется" if bootstrap_in_effect else "Полная синхронизация выполняется",
    )
    mark_store_kind_progress(
        store_id,
        "full",
        progress_percent=10,
        message="Подготавливаем первую синхронизацию магазина" if bootstrap_in_effect else "Подготавливаем полную синхронизацию",
        phase="prepare",
        phase_label="Подготовка",
    )
    if bootstrap_in_effect:
        mark_bootstrap_state_sync(store_id, "running")

    async def _run():
        async with SessionLocal() as db:
            store, store_name, user_email = await _get_store_context(db, store_id)
            if not store:
                logger.error(f"❌ Store {store_id} not found")
                return {"status": "missing_store", "store_id": store_id}

            logger.info(f"✅ Found store: {store_name} (ID: {store.id})")

            store_snapshot = SimpleNamespace(
                id=store.id,
                user_id=store.user_id,
                name=store.name,
                client_id=store.client_id,
                api_key_encrypted=store.api_key_encrypted,
            )

            service = SyncService(db)
            phases_completed = []

            if skip_products:
                logger.info(
                    f"♻️ Skipping products phase in full sync for store {store_id}: catalog was just imported"
                )
                phases_completed.append("products:skipped")
                mark_store_kind_progress(
                    store_id,
                    "full",
                    progress_percent=20,
                    message="Каталог уже импортирован, пропускаем этап товаров",
                    phase="products",
                    phase_label="Товары",
                )
            else:
                mark_store_kind_progress(
                    store_id,
                    "full",
                    progress_percent=20,
                    message="Загружаем товары и вариации из Ozon",
                    phase="products",
                    phase_label="Товары",
                )
                await service.sync_products_for_store(store_snapshot)
                phases_completed.append("products")
                logger.info(f"✅ Products phase completed for store {store_id}")

            mark_store_kind_progress(
                store_id,
                "full",
                progress_percent=40,
                message="Сверяем остатки по складам",
                phase="stocks",
                phase_label="Остатки",
            )
            stocks_result = await service.sync_stocks_for_store(store_snapshot)
            phases_completed.append("stocks")
            logger.info(f"✅ Stocks phase completed for store {store_id}")

            mark_store_kind_progress(
                store_id,
                "full",
                progress_percent=60,
                message="Загружаем поставки и статусы отправок",
                phase="supplies",
                phase_label="Поставки",
            )
            await service.sync_supplies_for_store(store_snapshot, effective_months_back)
            phases_completed.append("supplies")
            logger.info(f"✅ Supplies phase completed for store {store_id}")

            api_key = decrypt_api_key(store.api_key_encrypted)
            client = OzonClient(
                store.client_id,
                api_key,
                store_name=store_name,
                emit_notifications=False,
            )
            try:
                mark_store_kind_progress(
                    store_id,
                    "full",
                    progress_percent=78,
                    message="Обновляем отчеты Ozon",
                    phase="reports",
                    phase_label="Отчеты",
                )
                logger.info(f"📊 Starting report snapshots sync for store {store_id} ({store_name})")
                report_service = OzonReportService(client)
                report_snapshot_service = OzonReportSnapshotService(report_service)
                await report_snapshot_service.refresh_products_snapshot(client_id=store.client_id)
                logger.info(f"📦 Product report snapshot completed for store {store_id}")
                await report_snapshot_service.refresh_fbo_postings_snapshot(
                    client_id=store.client_id,
                    days_back=62,
                )
                logger.info(f"📑 FBO postings report snapshot completed for store {store_id}")
                phases_completed.append("reports")
                logger.info(f"✅ Reports phase completed for store {store_id}")

                mark_store_kind_progress(
                    store_id,
                    "full",
                    progress_percent=90,
                    message="Обновляем финансовые данные",
                    phase="finance",
                    phase_label="Финансы",
                )
                logger.info(f"💰 Starting finance snapshot sync for store {store_id} ({store_name})")
                finance_snapshot_service = OzonFinanceSnapshotService(client)
                await finance_snapshot_service.refresh_cash_flow_snapshot(
                    client_id=store.client_id,
                    days_back=62,
                )
                phases_completed.append("finance")
                logger.info(f"✅ Finance phase completed for store {store_id}")
            finally:
                await client.close()

            await _invalidate_shipments_cache_for_store(store)

            if stocks_result.get("failed_batches", 0) > 0:
                await notify_sync_partial(
                    sync_type="Полная синхронизация магазина",
                    store_id=store_id,
                    store_name=store_name,
                    user_email=user_email,
                    task_id=task_id,
                    duration_seconds=time.perf_counter() - started_at,
                    successful_units=stocks_result.get("successful_batches", 0),
                    failed_units=stocks_result.get("failed_batches", 0),
                    details={
                        "months_back": effective_months_back,
                        "skip_products": skip_products,
                        "trigger": trigger,
                        "phases_completed": ", ".join(phases_completed),
                        "проблемный метод OZON": "/v1/analytics/stocks",
                        "ошибки батчей": "; ".join(stocks_result.get("batch_errors", [])[:3]) or "-",
                    },
                )
            else:
                await notify_sync_success(
                    sync_type="Полная синхронизация магазина",
                    store_id=store_id,
                    store_name=store_name,
                    user_email=user_email,
                    task_id=task_id,
                    duration_seconds=time.perf_counter() - started_at,
                    details={
                        "months_back": effective_months_back,
                        "skip_products": skip_products,
                        "trigger": trigger,
                        "phases_completed": ", ".join(phases_completed),
                    },
                    send_telegram=True,
                )

            await _notify_user_full_sync_success(
                store_snapshot,
                trigger=trigger,
                duration_seconds=time.perf_counter() - started_at,
                send_telegram=True,
            )
            set_background_cooldown(store_id)
            mark_store_kind_progress(
                store_id,
                "full",
                progress_percent=100,
                message="Первая синхронизация завершена, открываем кабинет" if bootstrap_in_effect else "Полная синхронизация завершена",
                phase="completed",
                phase_label="Завершение",
            )
            if bootstrap_in_effect:
                mark_bootstrap_state_sync(store_id, BOOTSTRAP_STATE_COMPLETED)
            logger.info(
                f"✅ Full sync completed for store {store_id}; background sync cooldown set for {BACKGROUND_COOLDOWN_SECONDS // 60} minutes"
            )
            return {"status": "success", "store_id": store_id, "phases_completed": phases_completed}

    try:
        run_async(_run())
        mark_store_sync_success(store_id, task_id)
        mark_store_kind_success(
            store_id,
            "full",
            "Первая полная синхронизация завершена" if bootstrap_in_effect else "Полная синхронизация завершена",
        )
        run_async(_scheduler_finish(store_id, "full", task_id, True))
        release_store_sync_lock(store_id)
        run_async(enqueue_post_full_follow_up_if_any(store_id))
        if bootstrap_in_effect:
            mark_bootstrap_state_sync(store_id, BOOTSTRAP_STATE_COMPLETED)
        return {"status": "success", "store_id": store_id}
    except SoftTimeLimitExceeded as e:
        logger.error(f"❌ Error in full sync for store {store_id}: SoftTimeLimitExceeded()")
        mark_store_sync_failed(store_id, task_id, "SoftTimeLimitExceeded()")
        mark_store_kind_failed(store_id, "full", "SoftTimeLimitExceeded()")
        run_async(_scheduler_finish(store_id, "full", task_id, False, "SoftTimeLimitExceeded()"))
        release_store_sync_lock(store_id)
        run_async(enqueue_post_full_follow_up_if_any(store_id))
        if bootstrap_in_effect:
            mark_bootstrap_state_sync(store_id, BOOTSTRAP_STATE_FAILED)
        raise e
    except Exception as e:
        logger.error(f"❌ Task failed: {e}")
        mark_store_sync_failed(store_id, task_id, str(e))
        mark_store_kind_failed(store_id, "full", str(e))
        run_async(_scheduler_finish(store_id, "full", task_id, False, str(e)))
        release_store_sync_lock(store_id)
        run_async(enqueue_post_full_follow_up_if_any(store_id))
        if bootstrap_in_effect:
            mark_bootstrap_state_sync(store_id, BOOTSTRAP_STATE_FAILED)
        raise
    finally:
        release_store_sync_lock(store_id)


@celery.task(
    name='worker.tasks.sync_supplies_task',
    bind=True,
    max_retries=2,
    soft_time_limit=45 * 60,
    time_limit=50 * 60,
)
def sync_supplies_task(self, store_id: int, months_back: int = 1, source: str = "background"):
    logger.info(f"📦 Starting supplies sync task for store {store_id}")
    started_at = time.perf_counter()
    task_id = self.request.id
    sync_label = "Ручная синхронизация поставок" if source == "manual" else "Фоновая синхронизация поставок"
    decision = run_async(_scheduler_try_start(store_id, "supplies", task_id, "manual" if source == "manual" else "background"))

    if not decision.allowed or has_named_sync_lock("full", store_id) or not acquire_named_sync_lock("supplies", store_id, ttl_seconds=50 * 60):
        logger.warning(
            f"⏳ Supplies sync already running or blocked by full sync for store {store_id}, skipping duplicate task"
        )
        skip_reason = decision.message if not decision.allowed else "Для магазина уже выполняется другая синхронизация"

        async def _notify_skip():
            async with SessionLocal() as db:
                store, store_name, user_email = await _get_store_context(db, store_id)
                if store:
                    await notify_sync_skipped(
                        sync_type=sync_label,
                        store_id=store.id,
                        store_name=store_name,
                        user_email=user_email,
                        task_id=task_id,
                        reason=skip_reason,
                    )

        mark_store_kind_skipped(store_id, "supplies", skip_reason)
        run_async(_notify_skip())
        return {"status": "skipped", "store_id": store_id, "reason": decision.action if not decision.allowed else "already_running"}

    mark_store_kind_running(store_id, "supplies")

    async def _run():
        async with SessionLocal() as db:
            store, store_name, user_email = await _get_store_context(db, store_id)
            if not store:
                logger.error(f"❌ Store {store_id} not found")
                return {"status": "missing_store", "store_id": store_id}

            service = SyncService(db)
            await service.sync_supplies_for_store(store, months_back)
            await _invalidate_shipments_cache_for_store(store)

            await notify_sync_success(
                sync_type=sync_label,
                store_id=store_id,
                store_name=store_name,
                user_email=user_email,
                task_id=task_id,
                duration_seconds=time.perf_counter() - started_at,
                details={"months_back": months_back},
                send_telegram=False,
            )

            mark_store_kind_success(store_id, "supplies")
            logger.info(f"✅ Supplies sync completed for store {store_id}")
            return {"status": "success", "store_id": store_id}

    try:
        result = run_async(_run())
        run_async(_scheduler_finish(store_id, "supplies", task_id, True))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        return result
    except SoftTimeLimitExceeded as e:
        logger.error(f"❌ Supplies sync soft time limit exceeded for store {store_id}")
        mark_store_kind_failed(store_id, "supplies", "SoftTimeLimitExceeded()")
        run_async(_scheduler_finish(store_id, "supplies", task_id, False, "SoftTimeLimitExceeded()"))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        raise e
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        mark_store_kind_failed(store_id, "supplies", str(e))
        run_async(_scheduler_finish(store_id, "supplies", task_id, False, str(e)))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        try:
            self.retry(countdown=180, exc=e)
        except Exception:
            raise
    finally:
        release_named_sync_lock("supplies", store_id)


@celery.task(name='worker.tasks.sync_supplies_all')
def sync_supplies_all(months_back: int = 1):
    async def _run():
        store_candidates = await _get_background_ready_store_candidates("supplies")
        if not store_candidates:
            logger.info("ℹ️ No active stores found for supplies sync")
            return
        for store_id, interval_minutes in store_candidates:
            if has_named_sync_lock("full", store_id):
                logger.info(f"ℹ️ Skipping background supplies scheduling for store {store_id} while full sync is active")
                continue
            if closed_months_are_pending_or_running(store_id):
                logger.info(
                    f"ℹ️ Skipping background supplies scheduling for store {store_id} while closed month export is queued/running"
                )
                mark_background_kind_waiting_for_closed_months("supplies", store_id)
                continue

            if not is_background_sync_due(store_id, "supplies", interval_minutes):
                logger.debug(
                    f"⏭️ Skipping background supplies scheduling for store {store_id}: "
                    f"owner cadence not due yet ({format_sync_interval_label('supplies', interval_minutes)})"
                )
                continue

            should_wait, remaining_seconds = should_delay_background_sync(store_id)
            if should_wait:
                logger.info(
                    f"⏱️ Delaying background supplies scheduling for store {store_id} for {remaining_seconds}s after full sync"
                )
                mark_background_kind_waiting("supplies", store_id, remaining_seconds)
                continue

            decision = await _scheduler_mark_background_requested(store_id, "supplies")
            if decision.allowed:
                mark_store_kind_queued(store_id, "supplies")
                sync_supplies_task.delay(store_id, months_back, "background")
            else:
                message = decision.message
                if decision.remaining_seconds > 0:
                    message = _cooldown_message(decision.remaining_seconds)
                mark_background_kind_queued_or_running("supplies", store_id, message)

    return run_async(_run())


@celery.task(name='worker.tasks.deliver_supply_notifications_task')
def deliver_supply_notifications_task():
    logger.info("📨 Delivering pending supply notifications")

    async def _run():
        async with SessionLocal() as db:
            sent_count, failed_count = await deliver_pending_supply_notification_events(db, limit=100)
            logger.info(
                f"✅ Supply notifications delivery completed: sent={sent_count}, failed={failed_count}"
            )

    return run_async(_run())


@celery.task(
    name='worker.tasks.sync_stocks_task',
    bind=True,
    max_retries=3,
    soft_time_limit=15 * 60,
    time_limit=18 * 60,
)
def sync_stocks_task(self, store_id: int, source: str = "background"):
    logger.info(f"📦 Starting stocks sync task for store {store_id}")
    started_at = time.perf_counter()
    task_id = self.request.id
    sync_label = "Ручная синхронизация остатков" if source == "manual" else "Фоновая синхронизация остатков"
    decision = run_async(_scheduler_try_start(store_id, "stocks", task_id, "manual" if source == "manual" else "background"))

    if not decision.allowed or has_named_sync_lock("full", store_id) or not acquire_named_sync_lock("stocks", store_id):
        logger.warning(
            f"⏳ Stocks sync already running or blocked by full sync for store {store_id}, skipping duplicate task"
        )
        skip_reason = decision.message if not decision.allowed else "Для магазина уже выполняется другая синхронизация"

        async def _notify_skip():
            async with SessionLocal() as db:
                store, store_name, user_email = await _get_store_context(db, store_id)
                if store:
                    await notify_sync_skipped(
                        sync_type=sync_label,
                        store_id=store.id,
                        store_name=store_name,
                        user_email=user_email,
                        task_id=task_id,
                        reason=skip_reason,
                    )

        mark_store_kind_skipped(store_id, "stocks", skip_reason)
        run_async(_notify_skip())
        return {"status": "skipped", "store_id": store_id, "reason": decision.action if not decision.allowed else "already_running"}

    mark_store_kind_running(store_id, "stocks")

    async def _run():
        async with SessionLocal() as db:
            store, store_name, user_email = await _get_store_context(db, store_id)
            if not store:
                logger.error(f"❌ Store {store_id} not found")
                return {"status": "missing_store", "store_id": store_id}

            service = SyncService(db)
            sync_result = await service.sync_stocks_for_store(store)
            await _invalidate_shipments_cache_for_store(store)

            failed_batches = sync_result.get("failed_batches", 0)
            successful_batches = sync_result.get("successful_batches", 0)
            batch_errors = sync_result.get("batch_errors", [])

            if failed_batches > 0:
                await notify_sync_partial(
                    sync_type=sync_label,
                    store_id=store_id,
                    store_name=store_name,
                    user_email=user_email,
                    task_id=task_id,
                    duration_seconds=time.perf_counter() - started_at,
                    successful_units=successful_batches,
                    failed_units=failed_batches,
                    details={
                        "проблемный метод OZON": "/v1/analytics/stocks",
                        "ошибки батчей": "; ".join(batch_errors[:3]) or "-",
                    },
                )
                mark_store_kind_failed(store_id, "stocks", "Часть батчей остатков завершилась с ошибкой")
                logger.warning(
                    f"⚠️ Stocks sync completed with partial failures for store {store_id}: "
                    f"{failed_batches} failed batches"
                )
                return {
                    "status": "partial",
                    "store_id": store_id,
                    "successful_batches": successful_batches,
                    "failed_batches": failed_batches,
                    "batch_errors": batch_errors[:5],
                }

            await notify_sync_success(
                sync_type=sync_label,
                store_id=store_id,
                store_name=store_name,
                user_email=user_email,
                task_id=task_id,
                duration_seconds=time.perf_counter() - started_at,
                send_telegram=False,
            )
            price_risk_alerts_task.delay(store_id)
            mark_store_kind_success(store_id, "stocks")
            logger.info(f"✅ Stocks sync completed for store {store_id}")

            return {
                "status": "success",
                "store_id": store_id,
                "successful_batches": successful_batches,
                "failed_batches": failed_batches,
            }

    try:
        result = run_async(_run())
        run_async(_scheduler_finish(store_id, "stocks", task_id, True))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        return result
    except SoftTimeLimitExceeded as e:
        logger.error(f"❌ Stocks sync soft time limit exceeded for store {store_id}")
        mark_store_kind_failed(store_id, "stocks", "SoftTimeLimitExceeded()")
        run_async(_scheduler_finish(store_id, "stocks", task_id, False, "SoftTimeLimitExceeded()"))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        raise e
    except Exception as e:
        logger.error(f"❌ Stocks sync task failed: {e}")
        mark_store_kind_failed(store_id, "stocks", str(e))
        run_async(_scheduler_finish(store_id, "stocks", task_id, False, str(e)))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        self.retry(countdown=60, exc=e)
    finally:
        release_named_sync_lock("stocks", store_id)


@celery.task(name='worker.tasks.sync_stocks_all')
def sync_stocks_all():
    async def _run():
        store_candidates = await _get_background_ready_store_candidates("stocks")
        if not store_candidates:
            logger.info("ℹ️ No active stores found for stocks sync")
            return
        for store_id, interval_minutes in store_candidates:
            if has_named_sync_lock("full", store_id):
                logger.info(f"ℹ️ Skipping background stocks scheduling for store {store_id} while full sync is active")
                continue
            if closed_months_are_pending_or_running(store_id):
                logger.info(
                    f"ℹ️ Skipping background stocks scheduling for store {store_id} while closed month export is queued/running"
                )
                mark_background_kind_waiting_for_closed_months("stocks", store_id)
                continue

            if not is_background_sync_due(store_id, "stocks", interval_minutes):
                logger.debug(
                    f"⏭️ Skipping background stocks scheduling for store {store_id}: "
                    f"owner cadence not due yet ({format_sync_interval_label('stocks', interval_minutes)})"
                )
                continue

            should_wait, remaining_seconds = should_delay_background_sync(store_id)
            if should_wait:
                logger.info(
                    f"⏱️ Delaying background stocks scheduling for store {store_id} for {remaining_seconds}s after full sync"
                )
                mark_background_kind_waiting("stocks", store_id, remaining_seconds)
                continue

            decision = await _scheduler_mark_background_requested(store_id, "stocks")
            if decision.allowed:
                mark_store_kind_queued(store_id, "stocks")
                sync_stocks_task.delay(store_id, "background")
            else:
                message = decision.message
                if decision.remaining_seconds > 0:
                    message = _cooldown_message(decision.remaining_seconds)
                mark_background_kind_queued_or_running("stocks", store_id, message)

    return run_async(_run())


@celery.task(
    name='worker.tasks.sync_report_snapshots_task',
    bind=True,
    max_retries=2,
    soft_time_limit=25 * 60,
    time_limit=30 * 60,
)
def sync_report_snapshots_task(self, store_id: int, postings_days_back: int = 30, source: str = "background"):
    logger.info(f"🧾 Starting Ozon report snapshot sync for store {store_id}")
    started_at = time.perf_counter()
    task_id = self.request.id
    sync_label = "Ручная синхронизация отчётов" if source == "manual" else "Фоновая синхронизация отчётов"

    decision = run_async(_scheduler_try_start(store_id, "reports", task_id, "manual" if source == "manual" else "background"))
    ttl_kwargs = {}
    if "reports" in ("reports", "finance"):
        ttl_kwargs = {"ttl_seconds": 30 * 60}
    if not decision.allowed or not acquire_named_sync_lock("reports", store_id, **ttl_kwargs):
        logger.warning(f"⏳ Reports sync blocked for store {store_id}: {decision.message}")
        mark_store_kind_skipped(store_id, "reports", decision.message)
        return {"status": "skipped", "store_id": store_id, "reason": decision.action}

    mark_store_kind_running(store_id, "reports")

    async def _run():
        async with SessionLocal() as db:
            store, store_name, user_email = await _get_store_context(db, store_id)
            if not store:
                logger.error(f"❌ Store {store_id} not found")
                return {"status": "missing_store", "store_id": store_id}

            api_key = decrypt_api_key(store.api_key_encrypted)
            client = OzonClient(
                store.client_id,
                api_key,
                store_name=store_name,
                emit_notifications=False,
            )
            try:
                report_service = OzonReportService(client)
                snapshot_service = OzonReportSnapshotService(report_service)

                products_snapshot = await snapshot_service.refresh_products_snapshot(
                    client_id=store.client_id
                )
                postings_snapshot = await snapshot_service.refresh_fbo_postings_snapshot(
                    client_id=store.client_id,
                    days_back=max(postings_days_back, 90),
                )
            finally:
                await client.close()

            await _invalidate_shipments_cache_for_store(store)
            await notify_sync_success(
                sync_type=sync_label,
                store_id=store_id,
                store_name=store_name,
                user_email=user_email,
                task_id=task_id,
                duration_seconds=time.perf_counter() - started_at,
                details={"postings_days_back": postings_days_back},
                send_telegram=False,
            )
            price_risk_alerts_task.delay(store_id)
            mark_store_kind_success(store_id, "reports")
            logger.info(
                "✅ Report snapshots ready for store {} in {:.2f}s (products rows={}, postings rows={})",
                store_id,
                time.perf_counter() - started_at,
                ((products_snapshot.get("preview") or {}).get("summary") or {}).get("total_rows", 0),
                ((postings_snapshot.get("preview") or {}).get("summary") or {}).get("total_rows", 0),
            )
            return {
                "status": "success",
                "store_id": store_id,
                "products_rows": ((products_snapshot.get("preview") or {}).get("summary") or {}).get("total_rows", 0),
                "postings_rows": ((postings_snapshot.get("preview") or {}).get("summary") or {}).get("total_rows", 0),
            }

    try:
        result = run_async(_run())
        run_async(_scheduler_finish(store_id, "reports", task_id, True))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        return result
    except SoftTimeLimitExceeded as e:
        logger.error(f"❌ Report snapshot task soft time limit exceeded for store {store_id}")
        mark_store_kind_failed(store_id, "reports", "SoftTimeLimitExceeded()")
        run_async(_scheduler_finish(store_id, "reports", task_id, False, "SoftTimeLimitExceeded()"))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        raise e
    except Exception as e:
        logger.error(f"❌ Report snapshot task failed: {e}")
        mark_store_kind_failed(store_id, "reports", str(e))
        run_async(_scheduler_finish(store_id, "reports", task_id, False, str(e)))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        self.retry(countdown=120, exc=e)
    finally:
        release_named_sync_lock("reports", store_id)


@celery.task(name='worker.tasks.sync_report_snapshots_all')
def sync_report_snapshots_all(postings_days_back: int = 30):
    async def _run():
        store_candidates = await _get_background_ready_store_candidates("reports")
        if not store_candidates:
            logger.info("ℹ️ No active stores found for report snapshot sync")
            return
        for store_id, interval_minutes in store_candidates:
            if has_named_sync_lock("full", store_id):
                logger.info(f"ℹ️ Skipping background reports scheduling for store {store_id} while full sync is active")
                continue
            if closed_months_are_pending_or_running(store_id):
                logger.info(
                    f"ℹ️ Skipping background reports scheduling for store {store_id} while closed month export is queued/running"
                )
                mark_background_kind_waiting_for_closed_months("reports", store_id)
                continue

            if not is_background_sync_due(store_id, "reports", interval_minutes):
                logger.debug(
                    f"⏭️ Skipping background reports scheduling for store {store_id}: "
                    f"owner cadence not due yet ({format_sync_interval_label('reports', interval_minutes)})"
                )
                continue

            should_wait, remaining_seconds = should_delay_background_sync(store_id)
            if should_wait:
                logger.info(
                    f"⏱️ Delaying background reports scheduling for store {store_id} for {remaining_seconds}s after full sync"
                )
                mark_background_kind_waiting("reports", store_id, remaining_seconds)
                continue

            decision = await _scheduler_mark_background_requested(store_id, "reports")
            if decision.allowed:
                mark_store_kind_queued(store_id, "reports")
                sync_report_snapshots_task.delay(store_id, postings_days_back)
            else:
                message = decision.message
                if decision.remaining_seconds > 0:
                    message = _cooldown_message(decision.remaining_seconds)
                mark_background_kind_queued_or_running("reports", store_id, message)

    return run_async(_run())


@celery.task(
    name='worker.tasks.sync_finance_snapshots_task',
    bind=True,
    max_retries=2,
    soft_time_limit=25 * 60,
    time_limit=30 * 60,
)
def sync_finance_snapshots_task(self, store_id: int, days_back: int = 62, source: str = "background"):
    logger.info(f"💸 Starting Ozon finance snapshot sync for store {store_id}")
    started_at = time.perf_counter()
    task_id = self.request.id
    sync_label = "Ручная синхронизация текущих финансов" if source == "manual" else "Фоновая синхронизация финансов"

    decision = run_async(_scheduler_try_start(store_id, "finance", task_id, "manual" if source == "manual" else "background"))
    ttl_kwargs = {}
    if "finance" in ("reports", "finance"):
        ttl_kwargs = {"ttl_seconds": 30 * 60}
    if not decision.allowed or not acquire_named_sync_lock("finance", store_id, **ttl_kwargs):
        logger.warning(f"⏳ Finance sync blocked for store {store_id}: {decision.message}")
        mark_store_kind_skipped(store_id, "finance", decision.message)
        return {"status": "skipped", "store_id": store_id, "reason": decision.action}

    mark_store_kind_running(store_id, "finance")

    async def _run():
        async with SessionLocal() as db:
            store, store_name, user_email = await _get_store_context(db, store_id)
            if not store:
                logger.error(f"❌ Store {store_id} not found")
                return {"status": "missing_store", "store_id": store_id}

            api_key = decrypt_api_key(store.api_key_encrypted)
            client = OzonClient(
                store.client_id,
                api_key,
                store_name=store_name,
                emit_notifications=False,
            )
            try:
                snapshot_service = OzonFinanceSnapshotService(client)
                snapshot = await snapshot_service.refresh_cash_flow_snapshot(
                    client_id=store.client_id,
                    days_back=days_back,
                )
            finally:
                await client.close()

            summary = snapshot.get("summary") or {}
            await notify_sync_success(
                sync_type=sync_label,
                store_id=store_id,
                store_name=store_name,
                user_email=user_email,
                task_id=task_id,
                duration_seconds=time.perf_counter() - started_at,
                details={"days_back": days_back},
                send_telegram=False,
            )
            price_risk_alerts_task.delay(store_id)
            mark_store_kind_success(store_id, "finance")
            logger.info(
                "✅ Finance snapshot ready for store {} in {:.2f}s (net_payout={}, orders_amount={})",
                store_id,
                time.perf_counter() - started_at,
                summary.get("net_payout", 0),
                summary.get("orders_amount", 0),
            )
            return {
                "status": "success",
                "store_id": store_id,
                "net_payout": summary.get("net_payout", 0),
                "orders_amount": summary.get("orders_amount", 0),
            }

    try:
        result = run_async(_run())
        run_async(_scheduler_finish(store_id, "finance", task_id, True))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        return result
    except SoftTimeLimitExceeded as e:
        logger.error(f"❌ Finance snapshot task soft time limit exceeded for store {store_id}")
        mark_store_kind_failed(store_id, "finance", "SoftTimeLimitExceeded()")
        run_async(_scheduler_finish(store_id, "finance", task_id, False, "SoftTimeLimitExceeded()"))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        raise e
    except Exception as e:
        logger.error(f"❌ Finance snapshot task failed: {e}")
        mark_store_kind_failed(store_id, "finance", str(e))
        run_async(_scheduler_finish(store_id, "finance", task_id, False, str(e)))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        self.retry(countdown=120, exc=e)
    finally:
        release_named_sync_lock("finance", store_id)


@celery.task(
    name='worker.tasks.sync_closed_month_history_task',
    bind=True,
    max_retries=1,
    soft_time_limit=5 * 60 * 60,
    time_limit=5 * 60 * 60 + 10 * 60,
)
def sync_closed_month_history_task(self, store_id: int, months_back: int = 3, start_month: str | None = None):
    logger.info(
        f"🗓️ Starting closed month history sync for store {store_id}, months_back={months_back}, start_month={start_month}"
    )
    task_id = getattr(self.request, "id", None) or f"closed-months-{store_id}"

    decision = run_async(_scheduler_try_start(store_id, "closed_months", task_id, "manual"))
    if not decision.allowed:
        logger.warning(f"⏳ Closed month history sync blocked for store {store_id}: {decision.message}")
        mark_store_kind_skipped(store_id, "closed_months", decision.message)
        return {"status": "skipped", "store_id": store_id, "reason": decision.action}

    if not acquire_named_sync_lock("closed_months", store_id, ttl_seconds=50 * 60):
        current_kind = (get_store_sync_status(store_id).get("sync_kinds", {}) or {}).get("closed_months") or {}
        current_status = str(current_kind.get("status") or "")
        if current_status not in {"queued", "running"}:
            mark_store_kind_skipped(store_id, "closed_months", "Выгрузка истории закрытых месяцев уже выполняется")
        return {
            "status": "skipped",
            "store_id": store_id,
            "months_requested": months_back,
            "start_month": start_month,
            "message": "Выгрузка истории закрытых месяцев уже выполняется",
        }

    latest_closed_month = None
    if start_month:
        latest_closed_month = ClosedMonthHistoryService._latest_closed_month()
        queue_message = f"Собираем историю закрытых месяцев: с {start_month}"
        prepare_message = f"Готовим выгрузку истории: с {start_month}"
    else:
        queue_message = f"Собираем историю закрытых месяцев: {months_back} мес."
        prepare_message = f"Готовим выгрузку истории: {months_back} мес."

    mark_store_kind_running(
        store_id,
        "closed_months",
        queue_message,
        task_id=task_id,
    )
    mark_store_kind_progress(
        store_id,
        "closed_months",
        phase="prepare",
        phase_label="Подготовка",
        progress_percent=5,
        message=prepare_message,
        months_requested=months_back,
        months_completed=0,
        start_month=start_month,
        end_month=latest_closed_month,
        current_month=None,
        task_id=task_id,
    )

    async def _run():
        async with SessionLocal() as db:
            store, store_name, _user_email = await _get_store_context(db, store_id)
            if not store:
                logger.error(f"❌ Store {store_id} not found")
                return {"status": "missing_store", "store_id": store_id}

            service = ClosedMonthHistoryService(db)
            months = service._closed_months_from_start(start_month) if start_month else service._previous_closed_months(months_back)
            total_months = max(len(months), 1)
            batch_size = max(int(ClosedMonthHistoryService.SYNC_BATCH_MONTHS), 1)
            total_batches = max((len(months) + batch_size - 1) // batch_size, 1)
            results = []
            for batch_number, batch_start in enumerate(range(0, len(months), batch_size), start=1):
                batch_months = months[batch_start: batch_start + batch_size]
                for local_index, month in enumerate(batch_months, start=1):
                    index = batch_start + local_index
                    progress_before = min(10 + int(((index - 1) / total_months) * 80), 95)
                    mark_store_kind_progress(
                        store_id,
                        "closed_months",
                        phase="month",
                        phase_label=f"Пакет {batch_number} из {total_batches} · месяц {index} из {total_months}",
                        progress_percent=progress_before,
                        message=f"Собираем {month} ({index}/{total_months})",
                        months_requested=total_months,
                        months_completed=index - 1,
                        start_month=start_month,
                        end_month=latest_closed_month,
                        current_month=month,
                        task_id=task_id,
                    )
                    try:
                        result = await service.sync_store_month(
                            store=store,
                            month=month,
                            commit=True,
                        )
                    except Exception as exc:
                        result = await service._record_failed_month(
                            store=store,
                            month=month,
                            message=service._friendly_month_error_message(exc),
                        )
                    results.append(result)
                    progress_after = min(10 + int((index / total_months) * 85), 99)
                    mark_store_kind_progress(
                        store_id,
                        "closed_months",
                        phase="month",
                        phase_label=f"Пакет {batch_number} из {total_batches} · месяц {index} из {total_months}",
                        progress_percent=progress_after,
                        message=f"Готово: {month} ({index}/{total_months})",
                        months_requested=total_months,
                        months_completed=index,
                        start_month=start_month,
                        end_month=latest_closed_month,
                        current_month=month,
                        task_id=task_id,
                    )
            logger.info(
                "✅ Closed month history sync completed for store {} ({}): {}",
                store_id,
                store_name,
                ", ".join(f"{item.month}:{item.status}" for item in results) or "-",
            )
            return {
                "status": "success",
                "store_id": store_id,
                "months_requested": total_months,
                "start_month": start_month,
                "end_month": latest_closed_month,
                "months_synced": [item.month for item in results],
                "results": [
                    {
                        "month": item.month,
                        "status": item.status,
                        "is_final": item.is_final,
                        "offers_total": item.offers_total,
                        "offers_with_cost": item.offers_with_cost,
                    }
                    for item in results
                ],
            }

    try:
        result = run_async(_run())
        result_items = list(result.get("results") or [])
        has_errors = any(str(item.get("status") or "") == "error" for item in result_items)
        effective_months_requested = int(result.get("months_requested") or months_back)
        final_message = (
            f"История закрытых месяцев загружена частично: {effective_months_requested} мес."
            if has_errors
            else f"История закрытых месяцев обновлена: {effective_months_requested} мес."
        )
        mark_store_kind_success(
            store_id,
            "closed_months",
            final_message,
            task_id=task_id,
            months_requested=effective_months_requested,
            months_completed=effective_months_requested,
            start_month=start_month,
            end_month=latest_closed_month,
            current_month=None,
        )
        run_async(_scheduler_finish(store_id, "closed_months", task_id, True))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        return result
    except SoftTimeLimitExceeded as exc:
        logger.error(f"❌ Closed month history sync timed out for store {store_id}")
        mark_store_kind_failed(store_id, "closed_months", "Выгрузка истории закрытых месяцев превысила лимит времени")
        run_async(_scheduler_finish(store_id, "closed_months", task_id, False, "SoftTimeLimitExceeded()"))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        raise exc
    except Exception as exc:
        logger.error(f"❌ Closed month history sync failed for store {store_id}: {exc}")
        friendly_message = ClosedMonthHistoryService._friendly_month_error_message(exc)
        mark_store_kind_failed(store_id, "closed_months", friendly_message)
        run_async(_scheduler_finish(store_id, "closed_months", task_id, False, friendly_message))
        run_async(_resume_deferred_background_syncs_for_store(store_id))
        raise
    finally:
        release_named_sync_lock("closed_months", store_id)


@celery.task(
    name='worker.tasks.export_warehouse_excel_task',
    bind=True,
    max_retries=0,
    soft_time_limit=20 * 60,
    time_limit=25 * 60,
)
def export_warehouse_excel_task(self, store_id: int, user_id: int, order_window_days: int = 7):
    task_id = getattr(self.request, "id", None) or f"warehouse-export-{store_id}"
    if not acquire_export_lock("warehouse", store_id, ttl_seconds=30 * 60):
        return {"status": "skipped", "reason": "already_running", "store_id": store_id}

    mark_export_running(
        "warehouse",
        store_id,
        user_id,
        task_id=task_id,
        message="Собираем данные склада",
        phase="load",
        phase_label="Собираем данные",
        progress_percent=15,
        order_window_days=order_window_days,
    )

    async def _run():
        async with SessionLocal() as db:
            user = await _get_user_context(db, user_id)
            if not user:
                raise ValueError("Пользователь не найден")

            payload = await get_warehouse_overview(
                store_id=store_id,
                order_window_days=order_window_days,
                db=db,
                current_user=user,
            )
            total_products = len(payload.get("products") or [])
            mark_export_running(
                "warehouse",
                store_id,
                user_id,
                task_id=task_id,
                message=f"Формируем Excel по складу ({total_products} товаров)",
                phase="build",
                phase_label="Формируем Excel",
                progress_percent=65,
                order_window_days=order_window_days,
                processed_items=0,
                total_items=total_products,
            )
            workbook = build_warehouse_workbook(payload, order_window_days=order_window_days)
            file_path, file_name = export_file_path("warehouse", store_id)
            workbook.save(file_path)
            return {
                "file_path": file_path,
                "file_name": file_name,
                "total_products": total_products,
            }

    try:
        result = run_async(_run())
        mark_export_success(
            "warehouse",
            store_id,
            user_id,
            task_id=task_id,
            message="Excel по складу готов",
            file_path=result["file_path"],
            file_name=result["file_name"],
            download_url=f"/warehouse/export/download?store_id={store_id}",
            order_window_days=order_window_days,
            processed_items=result["total_products"],
            total_items=result["total_products"],
        )
        return {"status": "success", **result}
    except Exception as exc:
        logger.error(f"❌ Warehouse export failed for store {store_id}: {exc}")
        mark_export_failed(
            "warehouse",
            store_id,
            user_id,
            task_id=task_id,
            message="Не удалось сформировать Excel по складу",
            error=str(exc),
        )
        raise
    finally:
        release_export_lock("warehouse", store_id)


@celery.task(
    name='worker.tasks.export_shipments_excel_task',
    bind=True,
    max_retries=0,
    soft_time_limit=25 * 60,
    time_limit=30 * 60,
)
def export_shipments_excel_task(
    self,
    store_id: int,
    user_id: int,
    order_window_days: int = 30,
    product_filter: str | None = None,
    selected_product_names: list[str] | None = None,
):
    task_id = getattr(self.request, "id", None) or f"shipments-export-{store_id}"
    if not acquire_export_lock("shipments", store_id, ttl_seconds=35 * 60):
        return {"status": "skipped", "reason": "already_running", "store_id": store_id}

    mark_export_running(
        "shipments",
        store_id,
        user_id,
        task_id=task_id,
        message="Собираем данные отправок",
        phase="load",
        phase_label="Собираем данные",
        progress_percent=15,
        order_window_days=order_window_days,
    )

    async def _run():
        async with SessionLocal() as db:
            user = await _get_user_context(db, user_id)
            if not user:
                raise ValueError("Пользователь не найден")

            payload = await get_shipments(
                store_id=store_id,
                order_window_days=order_window_days,
                product_filter=product_filter,
                selected_product_names=selected_product_names or None,
                db=db,
                current_user=user,
            )
            total_clusters = len(payload.get("clusters") or [])
            mark_export_running(
                "shipments",
                store_id,
                user_id,
                task_id=task_id,
                message=f"Формируем Excel по отправкам ({total_clusters} кластеров)",
                phase="build",
                phase_label="Формируем Excel",
                progress_percent=65,
                order_window_days=order_window_days,
                processed_items=0,
                total_items=total_clusters,
            )
            workbook = build_shipments_workbook(payload, order_window_days=order_window_days)
            file_path, file_name = export_file_path("shipments", store_id)
            workbook.save(file_path)
            return {
                "file_path": file_path,
                "file_name": file_name,
                "total_clusters": total_clusters,
            }

    try:
        result = run_async(_run())
        mark_export_success(
            "shipments",
            store_id,
            user_id,
            task_id=task_id,
            message="Excel по отправкам готов",
            file_path=result["file_path"],
            file_name=result["file_name"],
            download_url=f"/shipments/export/download?store_id={store_id}",
            order_window_days=order_window_days,
            processed_items=result["total_clusters"],
            total_items=result["total_clusters"],
        )
        return {"status": "success", **result}
    except Exception as exc:
        logger.error(f"❌ Shipments export failed for store {store_id}: {exc}")
        mark_export_failed(
            "shipments",
            store_id,
            user_id,
            task_id=task_id,
            message="Не удалось сформировать Excel по отправкам",
            error=str(exc),
        )
        raise
    finally:
        release_export_lock("shipments", store_id)


@celery.task(
    name='worker.tasks.export_closed_months_excel_task',
    bind=True,
    max_retries=0,
    soft_time_limit=25 * 60,
    time_limit=30 * 60,
)
def export_closed_months_excel_task(self, store_id: int, user_id: int, year: int):
    task_id = getattr(self.request, "id", None) or f"closed-months-export-{store_id}-{year}"
    selection_label = f"{year} г."
    if not acquire_export_lock("closed_months", store_id, ttl_seconds=35 * 60):
        return {"status": "skipped", "reason": "already_running", "store_id": store_id, "year": year}

    mark_export_running(
        "closed_months",
        store_id,
        user_id,
        task_id=task_id,
        message=f"Собираем закрытые месяцы за {year} год",
        phase="load",
        phase_label="Собираем данные",
        progress_percent=15,
        selection_label=selection_label,
    )

    async def _run():
        async with SessionLocal() as db:
            user = await _get_user_context(db, user_id)
            if not user:
                raise ValueError("Пользователь не найден")

            owner_user_id = get_cabinet_owner_id(user)
            service = ClosedMonthHistoryService(db)
            store = await service._get_store(store_id, owner_user_id)
            months = await service.list_months(store_id=store_id, owner_user_id=owner_user_id, limit=24)
            year_months = [month for month in months if str(month.month).startswith(f"{year}-")]
            if not year_months:
                raise ValueError(f"За {year} год нет закрытых месяцев для экспорта")

            mark_export_running(
                "closed_months",
                store_id,
                user_id,
                task_id=task_id,
                message=f"Формируем Excel по закрытым месяцам ({len(year_months)} мес.)",
                phase="build",
                phase_label="Формируем Excel",
                progress_percent=65,
                selection_label=selection_label,
                processed_items=0,
                total_items=len(year_months),
            )

            export_months: list[dict] = []
            for month_row in year_months:
                offers = await service.list_month_offers(
                    store_id=store_id,
                    owner_user_id=owner_user_id,
                    month=month_row.month,
                )
                export_months.append(
                    {
                        "month": {
                            "month": month_row.month,
                            "status": month_row.status,
                            "coverage_ratio": float(month_row.coverage_ratio or 0),
                            "sold_units": int(month_row.sold_units or 0),
                            "returned_units": int(month_row.returned_units or 0),
                            "revenue_amount": float(month_row.revenue_amount or 0),
                            "cogs": float(month_row.cogs or 0),
                            "gross_profit": float(month_row.gross_profit or 0),
                            "ozon_commission": float(month_row.ozon_commission or 0),
                            "ozon_logistics": float(month_row.ozon_logistics or 0),
                            "ozon_services": float(month_row.ozon_services or 0),
                            "ozon_acquiring": float(month_row.ozon_acquiring or 0),
                            "ozon_other_expenses": float(month_row.ozon_other_expenses or 0),
                            "ozon_incentives": float(month_row.ozon_incentives or 0),
                            "ozon_adjustments_net": float(month_row.ozon_adjustments_net or 0),
                            "profit_before_tax": float(month_row.profit_before_tax or 0),
                            "tax_amount": float(month_row.tax_amount or 0),
                            "net_profit": float(month_row.net_profit or 0),
                            "tax_mode_used": month_row.tax_mode_used,
                            "vat_mode_used": month_row.vat_mode_used,
                        },
                        "offers": [
                            {
                                "offer_id": offer.offer_id,
                                "title": offer.title,
                                "sold_units": int(offer.sold_units or 0),
                                "returned_units": int(offer.returned_units or 0),
                                "revenue_amount": float(offer.revenue_amount or 0),
                                "cogs": None if offer.cogs is None else float(offer.cogs or 0),
                                "gross_profit": None if offer.gross_profit is None else float(offer.gross_profit or 0),
                                "profit_before_tax": None if offer.profit_before_tax is None else float(offer.profit_before_tax or 0),
                                "tax_amount": None if offer.tax_amount is None else float(offer.tax_amount or 0),
                                "net_profit": None if offer.net_profit is None else float(offer.net_profit or 0),
                                "has_cost": bool(offer.has_cost),
                            }
                            for offer in offers
                        ],
                    }
                )

            workbook = build_closed_months_workbook(
                {
                    "store_name": store.name,
                    "year": year,
                    "months": export_months,
                },
                year=year,
            )
            file_path, file_name = export_file_path("closed_months", store_id)
            workbook.save(file_path)
            return {
                "file_path": file_path,
                "file_name": file_name,
                "months_count": len(year_months),
            }

    try:
        result = run_async(_run())
        mark_export_success(
            "closed_months",
            store_id,
            user_id,
            task_id=task_id,
            message=f"Excel по закрытым месяцам за {year} год готов",
            file_path=result["file_path"],
            file_name=result["file_name"],
            download_url=f"/stores/{store_id}/closed-months/actions/export/download",
            selection_label=selection_label,
            processed_items=result["months_count"],
            total_items=result["months_count"],
        )
        return {"status": "success", **result}
    except Exception as exc:
        logger.error(f"❌ Closed months export failed for store {store_id}, year {year}: {exc}")
        mark_export_failed(
            "closed_months",
            store_id,
            user_id,
            task_id=task_id,
            message="Не удалось сформировать Excel по закрытым месяцам",
            error=str(exc),
        )
        raise
    finally:
        release_export_lock("closed_months", store_id)


@celery.task(name='worker.tasks.sync_finance_snapshots_all')
def sync_finance_snapshots_all(days_back: int = 62):
    async def _run():
        store_candidates = await _get_background_ready_store_candidates("finance")
        if not store_candidates:
            logger.info("ℹ️ No active stores found for finance snapshot sync")
            return
        for store_id, interval_minutes in store_candidates:
            if has_named_sync_lock("full", store_id):
                logger.info(f"ℹ️ Skipping background finance scheduling for store {store_id} while full sync is active")
                continue
            if closed_months_are_pending_or_running(store_id):
                logger.info(
                    f"ℹ️ Skipping background finance scheduling for store {store_id} while closed month export is queued/running"
                )
                mark_background_kind_waiting_for_closed_months("finance", store_id)
                continue

            if not is_background_sync_due(store_id, "finance", interval_minutes):
                logger.debug(
                    f"⏭️ Skipping background finance scheduling for store {store_id}: "
                    f"owner cadence not due yet ({format_sync_interval_label('finance', interval_minutes)})"
                )
                continue

            should_wait, remaining_seconds = should_delay_background_sync(store_id)
            if should_wait:
                logger.info(
                    f"⏱️ Delaying background finance scheduling for store {store_id} for {remaining_seconds}s after full sync"
                )
                mark_background_kind_waiting("finance", store_id, remaining_seconds)
                continue

            decision = await _scheduler_mark_background_requested(store_id, "finance")
            if decision.allowed:
                mark_store_kind_queued(store_id, "finance")
                sync_finance_snapshots_task.delay(store_id, days_back)
            else:
                message = decision.message
                if decision.remaining_seconds > 0:
                    message = _cooldown_message(decision.remaining_seconds)
                mark_background_kind_queued_or_running("finance", store_id, message)

    return run_async(_run())


@celery.task(name='worker.tasks.check_supplies_status')
def check_supplies_status():
    logger.info("🔍 Calling check_supplies_status_task")
    return check_supplies_status_task()
