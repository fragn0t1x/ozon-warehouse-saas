# backend/app/api/sync_router.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import SessionLocal
from app.models.store import Store
from app.models.user_settings import UserSettings
from app.models.user import User
from app.core.dependencies import get_current_user
from app.services.sync_dispatcher import (
    cancel_sync_kind,
    enqueue_full_sync,
    enqueue_finance_sync,
    enqueue_products_sync,
    enqueue_reports_sync,
    enqueue_stocks_sync,
    enqueue_supplies_sync,
    preempt_background_syncs_for_manual_kind,
    preempt_syncs_for_manual_full,
)
from app.services.bootstrap_sync import get_bootstrap_state
from app.services.sync_locks import has_any_sync_lock
from app.services.sync_scheduler import StoreSyncScheduler
from app.services.sync_status import get_store_sync_status, reconcile_store_sync_runtime_state
from app.services.admin_notifications import notify_backend_error
from app.services.cabinet_access import get_cabinet_owner_id
from app.services.telegram_service import TelegramService
from app.utils.redis_cache import get_redis
from loguru import logger

router = APIRouter(prefix="/sync", tags=["sync"])


INITIAL_FULL_SYNC_NOTICE_PENDING_KEY = "store:{store_id}:initial_full_sync_notice_pending"
INITIAL_FULL_SYNC_NOTICE_SENT_KEY = "store:{store_id}:initial_full_sync_notice_sent"


async def _maybe_send_initial_full_sync_notice(db: AsyncSession, current_user: User, store: Store, status_payload: dict) -> None:
    if status_payload.get("status") != "success" or not status_payload.get("finished_at"):
        return

    redis = await get_redis()
    if not redis:
        return

    pending_key = INITIAL_FULL_SYNC_NOTICE_PENDING_KEY.format(store_id=store.id)
    sent_key = INITIAL_FULL_SYNC_NOTICE_SENT_KEY.format(store_id=store.id)

    if not await redis.get(pending_key):
        return
    if await redis.get(sent_key):
        return

    settings_result = await db.execute(select(UserSettings).where(UserSettings.user_id == current_user.id))
    user_settings = settings_result.scalar_one_or_none()
    chat_id = getattr(user_settings, "telegram_chat_id", None)
    if not chat_id:
        return

    text = (
        "🎉 <b>Первая полная синхронизация завершена</b>\n\n"
        f"<b>Магазин:</b> {store.name}\n"
        f"<b>Client-ID:</b> {store.client_id}\n"
        f"<b>Завершена:</b> {status_payload.get('finished_at')}\n\n"
        "Товары, поставки и связанные данные уже подгружены в кабинет."
    )

    telegram = TelegramService()
    delivered = False
    try:
        delivered = await telegram.send_message(chat_id, text)
    finally:
        await telegram.close()

    if delivered:
        await redis.set(sent_key, "1")
        await redis.delete(pending_key)



async def get_db():
    async with SessionLocal() as session:
        yield session


async def _get_active_sync_entry(store_id: int) -> dict | None:
    await reconcile_store_sync_runtime_state(store_id)
    return await StoreSyncScheduler().get_active_entry(store_id)


def _kind_display(kind: str) -> str:
    return {
        "full": "полная синхронизация",
        "products": "синхронизация товаров",
        "stocks": "синхронизация остатков",
        "supplies": "синхронизация поставок",
        "reports": "синхронизация отчётов",
        "finance": "синхронизация текущих финансов",
        "closed_months": "выгрузка закрытых месяцев",
    }.get(kind, kind)


async def _ensure_manual_kind_can_start(store_id: int, kind: str) -> None:
    status_payload = await reconcile_store_sync_runtime_state(store_id)
    sync_kinds = (status_payload.get("sync_kinds") or {})
    active = await _get_active_sync_entry(store_id)
    active_kind = str((active or {}).get("kind") or "")
    active_source = str((active or {}).get("source") or "")
    current_status = str(((sync_kinds.get(kind) or {}).get("status")) or "")
    full_status = str(((sync_kinds.get("full") or {}).get("status")) or "")
    closed_months_status = str(((sync_kinds.get("closed_months") or {}).get("status")) or "")

    if full_status in {"queued", "running"}:
        raise HTTPException(status_code=409, detail="Сейчас выполняется полная синхронизация магазина")
    if closed_months_status in {"queued", "running"}:
        raise HTTPException(status_code=409, detail="Сейчас выполняется выгрузка закрытых месяцев")

    if active_kind == "full":
        raise HTTPException(status_code=409, detail="Сейчас выполняется полная синхронизация магазина")
    if active_kind == "closed_months":
        raise HTTPException(status_code=409, detail="Сейчас выполняется выгрузка закрытых месяцев")

    if current_status in {"queued", "running"}:
        if active_kind == kind and active_source == "background":
            return
        raise HTTPException(status_code=409, detail=f"{_kind_display(kind).capitalize()} уже выполняется для этого магазина")

    if active_kind and active_source == "manual" and active_kind != kind:
        raise HTTPException(status_code=409, detail=f"Сейчас выполняется {_kind_display(active_kind)}")


@router.post("/products/{store_id}")
async def sync_products(
        store_id: int,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """
    Запустить синхронизацию товаров для магазина
    """
    logger.info(f"🚀 Manual products sync requested for store {store_id}")
    cabinet_owner_id = get_cabinet_owner_id(current_user)

    # Проверяем, что магазин принадлежит пользователю
    store = await db.get(Store, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    if store.user_id != cabinet_owner_id:
        raise HTTPException(status_code=403, detail="Not enough permissions")

    await _ensure_manual_kind_can_start(store_id, "products")

    try:
        await preempt_background_syncs_for_manual_kind(store_id, "products")
        task = enqueue_products_sync(store_id, source="manual")
        return {
            "status": "queued",
            "message": f"Products sync queued for store {store_id}",
            "store_id": store_id,
            "store_name": store.name,
            "task_id": task.id,
        }
    except Exception as e:
        logger.error(f"❌ Products sync queueing failed for store {store_id}: {e}")
        await notify_backend_error(
            "manual_products_sync_queue",
            e,
            details={"store_id": store_id, "store_name": store.name, "user_email": current_user.email},
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/full/{store_id}", status_code=202)
async def sync_full(
        store_id: int,
        months_back: int = 2,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """
    Полная синхронизация магазина: товары, остатки и поставки
    """
    logger.info(f"🚀 Manual full sync requested for store {store_id}")
    cabinet_owner_id = get_cabinet_owner_id(current_user)

    store = await db.get(Store, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    if store.user_id != cabinet_owner_id:
        raise HTTPException(status_code=403, detail="Not enough permissions")

    status_payload = await reconcile_store_sync_runtime_state(store_id)
    full_status = str((((status_payload.get("sync_kinds") or {}).get("full") or {}).get("status")) or "")
    if full_status in {"queued", "running"} or await has_any_sync_lock(store_id, ("full",)):
        raise HTTPException(status_code=409, detail="Полная синхронизация уже выполняется для этого магазина")

    try:
        await preempt_syncs_for_manual_full(store_id)
        task = enqueue_full_sync(store_id, months_back, trigger="manual")

        return {
            "status": "queued",
            "message": f"Full sync queued for store {store_id}",
            "store_id": store_id,
            "store_name": store.name,
            "task_id": task.id,
        }
    except Exception as e:
        logger.error(f"❌ Full sync failed for store {store_id}: {e}")
        await notify_backend_error(
            "manual_full_sync_queue",
            e,
            details={"store_id": store_id, "store_name": store.name, "user_email": current_user.email},
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/supplies/{store_id}")
async def sync_supplies(
        store_id: int,
        months_back: int = 2,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """
    Запустить синхронизацию поставок для магазина
    """
    logger.info(f"🚀 Manual supplies sync requested for store {store_id}")
    cabinet_owner_id = get_cabinet_owner_id(current_user)

    store = await db.get(Store, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    if store.user_id != cabinet_owner_id:
        raise HTTPException(status_code=403, detail="Not enough permissions")

    await _ensure_manual_kind_can_start(store_id, "supplies")

    try:
        await preempt_background_syncs_for_manual_kind(store_id, "supplies")
        task = enqueue_supplies_sync(store_id, months_back=min(months_back, 1), source="manual")
        return {
            "status": "queued",
            "message": f"Supplies sync queued for store {store_id}",
            "store_id": store_id,
            "store_name": store.name,
            "task_id": task.id,
        }
    except Exception as e:
        logger.error(f"❌ Supplies sync queueing failed for store {store_id}: {e}")
        await notify_backend_error(
            "manual_supplies_sync_queue",
            e,
            details={"store_id": store_id, "store_name": store.name, "user_email": current_user.email},
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stocks/{store_id}")
async def sync_stocks(
        store_id: int,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """
    Запустить синхронизацию остатков для магазина
    """
    logger.info(f"🚀 Manual stocks sync requested for store {store_id}")
    cabinet_owner_id = get_cabinet_owner_id(current_user)

    store = await db.get(Store, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    if store.user_id != cabinet_owner_id:
        raise HTTPException(status_code=403, detail="Not enough permissions")

    await _ensure_manual_kind_can_start(store_id, "stocks")

    try:
        await preempt_background_syncs_for_manual_kind(store_id, "stocks")
        task = enqueue_stocks_sync(store_id, source="manual")
        return {
            "status": "queued",
            "message": f"Stocks sync queued for store {store_id}",
            "store_id": store_id,
            "store_name": store.name,
            "task_id": task.id,
        }
    except Exception as e:
        logger.error(f"❌ Stocks sync queueing failed for store {store_id}: {e}")
        await notify_backend_error(
            "manual_stocks_sync_queue",
            e,
            details={"store_id": store_id, "store_name": store.name, "user_email": current_user.email},
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reports/{store_id}")
async def sync_reports(
        store_id: int,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """
    Запустить синхронизацию отчётов для магазина
    """
    logger.info(f"🚀 Manual reports sync requested for store {store_id}")
    cabinet_owner_id = get_cabinet_owner_id(current_user)

    store = await db.get(Store, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    if store.user_id != cabinet_owner_id:
        raise HTTPException(status_code=403, detail="Not enough permissions")

    await _ensure_manual_kind_can_start(store_id, "reports")

    try:
        await preempt_background_syncs_for_manual_kind(store_id, "reports")
        task = enqueue_reports_sync(store_id, source="manual")
        return {
            "status": "queued",
            "message": f"Reports sync queued for store {store_id}",
            "store_id": store_id,
            "store_name": store.name,
            "task_id": task.id,
        }
    except Exception as e:
        logger.error(f"❌ Reports sync queueing failed for store {store_id}: {e}")
        await notify_backend_error(
            "manual_reports_sync_queue",
            e,
            details={"store_id": store_id, "store_name": store.name, "user_email": current_user.email},
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/finance/{store_id}")
async def sync_finance(
        store_id: int,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """
    Запустить синхронизацию финансов для магазина
    """
    logger.info(f"🚀 Manual finance sync requested for store {store_id}")
    cabinet_owner_id = get_cabinet_owner_id(current_user)

    store = await db.get(Store, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    if store.user_id != cabinet_owner_id:
        raise HTTPException(status_code=403, detail="Not enough permissions")

    await _ensure_manual_kind_can_start(store_id, "finance")

    try:
        await preempt_background_syncs_for_manual_kind(store_id, "finance")
        task = enqueue_finance_sync(store_id, source="manual")
        return {
            "status": "queued",
            "message": f"Finance sync queued for store {store_id}",
            "store_id": store_id,
            "store_name": store.name,
            "task_id": task.id,
        }
    except Exception as e:
        logger.error(f"❌ Finance sync queueing failed for store {store_id}: {e}")
        await notify_backend_error(
            "manual_finance_sync_queue",
            e,
            details={"store_id": store_id, "store_name": store.name, "user_email": current_user.email},
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{kind}/{store_id}/cancel", status_code=202)
async def cancel_sync(
        kind: str,
        store_id: int,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    allowed_kinds = {"full", "products", "stocks", "supplies", "reports", "finance"}
    if kind not in allowed_kinds:
        raise HTTPException(status_code=404, detail="Unknown sync kind")

    store = await db.get(Store, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    if store.user_id != get_cabinet_owner_id(current_user):
        raise HTTPException(status_code=403, detail="Not enough permissions")

    status_payload = await reconcile_store_sync_runtime_state(store_id)
    sync_kinds = status_payload.get("sync_kinds") or {}
    active = await _get_active_sync_entry(store_id)
    active_kind = str((active or {}).get("kind") or "")
    active_source = str((active or {}).get("source") or "")
    current_status = str(((sync_kinds.get(kind) or {}).get("status")) or "")

    if kind == "full" and active_kind == "full" and active_source in {"startup", "store_created"}:
        raise HTTPException(status_code=409, detail="Первую полную синхронизацию нельзя останавливать вручную")

    if kind == "full" and active_kind == "full" and active_source != "manual":
        raise HTTPException(status_code=409, detail="Можно остановить только ручную полную синхронизацию")

    if kind != "full":
        if active_kind != kind or active_source != "manual":
            if current_status not in {"queued", "running"}:
                raise HTTPException(status_code=409, detail="Можно остановить только ручную синхронизацию этого типа")
    elif active_kind != "full" and current_status not in {"queued", "running"}:
        raise HTTPException(status_code=409, detail="Можно остановить только ручную полную синхронизацию")

    try:
        return await cancel_sync_kind(store_id, kind)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/status/{store_id}")
async def get_sync_status(
        store_id: int,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """
    Получить статус последней синхронизации для магазина
    """
    store = await db.get(Store, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    if store.user_id != get_cabinet_owner_id(current_user):
        raise HTTPException(status_code=403, detail="Not enough permissions")

    status_payload = await reconcile_store_sync_runtime_state(store_id)
    active_entry = await StoreSyncScheduler().get_active_entry(store_id)
    active_kinds = []
    kind_labels = {
        "full": "полная",
        "products": "товары",
        "stocks": "остатки",
        "supplies": "поставки",
        "reports": "отчеты",
        "finance": "финансы",
        "closed_months": "закрытые месяцы",
    }
    if active_entry and active_entry.get("kind"):
        active_kinds.append(str(active_entry.get("kind")))

    for kind in ("full", "products", "stocks", "supplies", "reports", "finance", "closed_months"):
        if await has_any_sync_lock(store_id, (kind,)):
            active_kinds.append(kind)
            sync_kinds = dict(status_payload.get("sync_kinds") or {})
            current_kind = dict(sync_kinds.get(kind) or {})
            current_kind.update(
                {
                    "kind": kind,
                    "status": "running",
                    "message": current_kind.get("message") or f"Сейчас обновляются: {kind_labels.get(kind, kind)}",
                    "phase": current_kind.get("phase") or "running",
                    "phase_label": current_kind.get("phase_label") or "Выполняется",
                    "progress_percent": current_kind.get("progress_percent")
                    if current_kind.get("progress_percent") is not None
                    else (5 if kind == "full" else 10),
                    "queued_at": current_kind.get("queued_at") or status_payload.get("queued_at"),
                    "started_at": current_kind.get("started_at") or status_payload.get("started_at") or status_payload.get("updated_at"),
                    "finished_at": None,
                }
            )
            sync_kinds[kind] = current_kind
            status_payload["sync_kinds"] = sync_kinds

    if active_kinds and status_payload.get("status") != "running":
        status_payload["status"] = "running"
        status_payload["message"] = (
            "Синхронизация уже выполняется"
            if len(active_kinds) != 1
            else f"Сейчас обновляются: {kind_labels.get(active_kinds[0], active_kinds[0])}"
        )
    elif not active_kinds and status_payload.get("status") == "running":
        queued_kinds = [
            kind
            for kind, kind_status in (status_payload.get("sync_kinds") or {}).items()
            if (kind_status or {}).get("status") == "queued"
        ]
        if queued_kinds:
            status_payload["status"] = "queued"
            status_payload["message"] = "Фоновые синхронизации ждут запуска"

    status_payload["active_sync_kinds"] = sorted(set(active_kinds))
    status_payload["active_sync"] = active_entry
    status_payload["store_name"] = store.name
    status_payload["bootstrap_state"] = await get_bootstrap_state(store_id)
    await _maybe_send_initial_full_sync_notice(db, current_user, store, status_payload)
    return status_payload
