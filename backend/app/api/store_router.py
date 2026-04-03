# backend/app/api/store_router.py
from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger
from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_current_owner_user, get_current_user
from app.database import SessionLocal
from app.models.base_models import ProductMatch, VariantMatch
from app.models.ozon_warehouse import OzonStock
from app.models.product import Product
from app.models.store import Store
from app.models.store_economics_history import StoreEconomicsHistory
from app.models.supply import Supply
from app.models.supply_notification_event import SupplyNotificationEvent
from app.models.user import User
from app.models.variant import Variant
from app.models.variant_attribute import VariantAttribute
from app.models.warehouse import Warehouse
from app.models.warehouse_product import WarehouseProduct
from app.schemas.economics_history import StoreEconomicsHistoryEntryResponse
from app.schemas.store import StoreCreate, StoreImportPreviewResponse, StorePatch, StoreResponse, StoreUpdate, StoreValidate
from app.services.bootstrap_sync import (
    BOOTSTRAP_STATE_COMPLETED,
    get_bootstrap_state_sync,
    mark_bootstrap_state_sync,
    reset_bootstrap_state_sync,
)
from app.services.cabinet_access import get_cabinet_owner_id
from app.services.closed_month_history_service import ClosedMonthHistoryService
from app.services.closed_months_recalc_queue import ClosedMonthsRecalcQueue
from app.services.economics_history_service import EconomicsHistoryService
from app.services.export_status import mark_store_exports_stale
from app.services.settings_service import SettingsService
from app.services.store_linking_service import StoreLinkingService
from app.services.sync_dispatcher import enqueue_full_sync
from app.services.sync_scheduler import StoreSyncScheduler
from app.services.sync_service import SyncService
from app.services.warehouse_selector import ensure_shared_warehouse, ensure_store_warehouse
from app.services.ozon.validation_service import OzonValidationService
from app.utils.encryption import encrypt_api_key
from app.utils.redis_cache import get_redis

router = APIRouter(prefix="/stores", tags=["stores"])

_scheduler = StoreSyncScheduler()

INITIAL_FULL_SYNC_NOTICE_PENDING_KEY = "store:{store_id}:initial_full_sync_notice_pending"


async def _mark_initial_full_sync_notice_pending(store_id: int) -> None:
    redis = await get_redis()
    if not redis:
        return
    await redis.set(INITIAL_FULL_SYNC_NOTICE_PENDING_KEY.format(store_id=store_id), "1")


async def get_db():
    async with SessionLocal() as session:
        yield session


async def _get_store_warehouse_id(db: AsyncSession, store_id: int) -> int | None:
    result = await db.execute(select(Warehouse.id).where(Warehouse.store_id == store_id))
    return result.scalar_one_or_none()


async def _get_store_economics_effective_from(db: AsyncSession, store_id: int):
    result = await db.execute(
        select(StoreEconomicsHistory.effective_from)
        .where(StoreEconomicsHistory.store_id == store_id)
        .order_by(StoreEconomicsHistory.effective_from.desc(), StoreEconomicsHistory.id.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _get_current_store_economics_snapshot(db: AsyncSession, store: Store):
    return await EconomicsHistoryService(db).get_store_economics_for_date(
        store=store,
        as_of=datetime.now(timezone.utc).date(),
    )


def _serialize_store(
    store: Store,
    warehouse_id: int | None,
    economics_effective_from=None,
    economics_vat_mode: str | None = None,
    economics_tax_mode: str | None = None,
    economics_tax_rate: float | None = None,
) -> dict:
    return {
        "id": store.id,
        "user_id": store.user_id,
        "name": store.name,
        "client_id": store.client_id,
        "is_active": store.is_active,
        "created_at": store.created_at,
        "warehouse_id": warehouse_id,
        "bootstrap_state": get_bootstrap_state_sync(store.id) if store.is_active else None,
        "economics_vat_mode": economics_vat_mode or store.economics_vat_mode or "none",
        "economics_tax_mode": economics_tax_mode or store.economics_tax_mode or "usn_income_expenses",
        "economics_tax_rate": float(
            economics_tax_rate
            if economics_tax_rate is not None
            else (store.economics_tax_rate or 15.0)
        ),
        "economics_default_sale_price_gross": (
            float(store.economics_default_sale_price_gross)
            if store.economics_default_sale_price_gross is not None
            else None
        ),
        "economics_effective_from": economics_effective_from,
    }


async def _get_owned_store(db: AsyncSession, store_id: int, user_id: int) -> Store:
    store = await db.get(Store, store_id)
    if not store:
        raise HTTPException(404, "Store not found")
    if store.user_id != user_id:
        raise HTTPException(403, "Not enough permissions")
    return store


async def _ensure_unique_client_id(
    db: AsyncSession,
    user_id: int,
    client_id: str,
    excluded_store_id: int | None = None,
) -> None:
    stmt = select(Store.id).where(Store.user_id == user_id, Store.client_id == client_id)
    if excluded_store_id is not None:
        stmt = stmt.where(Store.id != excluded_store_id)
    result = await db.execute(stmt)
    if result.scalar_one_or_none() is not None:
        raise HTTPException(status_code=400, detail="Магазин с таким Client-ID уже существует")


def _log_deleted_rows(message: str, rowcount: int | None, store_id: int, *args) -> None:
    logger.info(message, rowcount or 0, *args, store_id)


def _closed_months_start_month(effective_from: datetime | None = None, *, effective_date=None) -> str:
    value = effective_date or (effective_from.date() if effective_from else None) or datetime.now(timezone.utc).date()
    return f"{value.year:04d}-{value.month:02d}"


def _is_month_after_latest_closed(effective_date) -> bool:
    value = effective_date or datetime.now(timezone.utc).date()
    latest_closed_month = ClosedMonthHistoryService._latest_closed_month()
    return _closed_months_start_month(effective_date=value) > latest_closed_month


def _closed_months_export_stale_message() -> str:
    return "Налоговая схема изменилась. Excel по закрытым месяцам устарел, сформируй его заново."


async def _queue_closed_months_recalc_after_store_economics_change(store_id: int, *, effective_date) -> dict:
    if _is_month_after_latest_closed(effective_date):
        return {
            "status": "skipped_future",
            "store_id": store_id,
            "start_month": _closed_months_start_month(effective_date=effective_date),
        }
    mark_store_exports_stale(
        "closed_months",
        store_id,
        message=_closed_months_export_stale_message(),
    )
    return await ClosedMonthsRecalcQueue().queue(
        store_id,
        start_month=_closed_months_start_month(effective_date=effective_date),
    )


async def _cleanup_supply_related_rows(db: AsyncSession, store_id: int) -> None:
    supply_cleanup_tables = (
        "supply_notification_events",
        "supply_processing",
        "supply_items",
    )
    for table_name in supply_cleanup_tables:
        deleted_rows = await db.execute(
            text(
                f"""
                DELETE FROM {table_name}
                WHERE supply_id IN (
                    SELECT id FROM supplies WHERE store_id = :store_id
                )
                """
            ),
            {"store_id": store_id},
        )
        _log_deleted_rows(
            "🧹 Deleted {} rows from {} for store {} via supply cleanup",
            deleted_rows.rowcount,
            store_id,
            table_name,
        )

    deleted_events = await db.execute(
        delete(SupplyNotificationEvent).where(SupplyNotificationEvent.store_id == store_id)
    )
    _log_deleted_rows(
        "🧹 Deleted {} supply notification events by store_id before removing store {}",
        deleted_events.rowcount,
        store_id,
    )


async def _cleanup_store_entities(db: AsyncSession, store_id: int) -> int | None:
    await db.execute(delete(ProductMatch).where(ProductMatch.store_id == store_id))
    await db.execute(delete(VariantMatch).where(VariantMatch.store_id == store_id))

    deleted_supplies = await db.execute(delete(Supply).where(Supply.store_id == store_id))
    _log_deleted_rows("🧹 Deleted {} supplies for store {}", deleted_supplies.rowcount, store_id)

    product_ids_subquery = select(Product.id).where(Product.store_id == store_id)
    variant_ids_subquery = select(Variant.id).where(Variant.product_id.in_(product_ids_subquery))

    deleted_ozon_stocks = await db.execute(
        delete(OzonStock).where(OzonStock.variant_id.in_(variant_ids_subquery))
    )
    _log_deleted_rows(
        "🧹 Deleted {} rows from ozon_stocks for store {} via variant cleanup",
        deleted_ozon_stocks.rowcount,
        store_id,
    )

    deleted_variant_attributes = await db.execute(
        delete(VariantAttribute).where(VariantAttribute.variant_id.in_(variant_ids_subquery))
    )
    _log_deleted_rows(
        "🧹 Deleted {} rows from variant_attributes for store {} via variant cleanup",
        deleted_variant_attributes.rowcount,
        store_id,
    )

    deleted_variants = await db.execute(
        delete(Variant).where(Variant.product_id.in_(product_ids_subquery))
    )
    _log_deleted_rows("🧹 Deleted {} variants for store {}", deleted_variants.rowcount, store_id)

    deleted_products = await db.execute(delete(Product).where(Product.store_id == store_id))
    _log_deleted_rows("🧹 Deleted {} products for store {}", deleted_products.rowcount, store_id)

    deleted_warehouses = await db.execute(delete(Warehouse).where(Warehouse.store_id == store_id))
    _log_deleted_rows("🧹 Deleted {} warehouses for store {}", deleted_warehouses.rowcount, store_id)

    deleted_store = await db.execute(delete(Store).where(Store.id == store_id))
    _log_deleted_rows("🧹 Deleted {} store rows for store {}", deleted_store.rowcount, store_id)
    return deleted_store.rowcount


async def _delete_orphaned_warehouse_products(
    db: AsyncSession,
    user_id: int,
    store_id: int,
    linked_warehouse_product_ids: set[int],
) -> None:
    if not linked_warehouse_product_ids:
        return

    still_linked_result = await db.execute(
        select(Product.warehouse_product_id).where(Product.warehouse_product_id.in_(linked_warehouse_product_ids))
    )
    still_linked_ids = {
        warehouse_product_id
        for warehouse_product_id in still_linked_result.scalars().all()
        if warehouse_product_id is not None
    }
    orphaned_warehouse_product_ids = linked_warehouse_product_ids - still_linked_ids
    if not orphaned_warehouse_product_ids:
        return

    deleted_warehouse_products = await db.execute(
        delete(WarehouseProduct).where(
            WarehouseProduct.user_id == user_id,
            WarehouseProduct.id.in_(orphaned_warehouse_product_ids),
        )
    )
    _log_deleted_rows(
        "🧹 Deleted {} orphaned warehouse_products after store {} removal",
        deleted_warehouse_products.rowcount,
        store_id,
    )


async def _cleanup_store_redis_flags(store_id: int) -> None:
    redis = await get_redis()
    if not redis:
        return

    try:
        await redis.delete(INITIAL_FULL_SYNC_NOTICE_PENDING_KEY.format(store_id=store_id))
    except Exception as e:
        logger.warning("⚠️ Failed to cleanup redis flags for store {}: {}", store_id, e)


@router.post("/validate")
async def validate_store(
    store_data: StoreValidate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_owner_user),
):
    validation_service = OzonValidationService()
    result = await validation_service.validate_store(store_data.client_id, store_data.api_key)

    if result["valid"]:
        store_info = await validation_service.get_store_info(store_data.client_id, store_data.api_key)
        if store_info:
            result["store_info"] = store_info

    return result


@router.post("/product-link-preview", response_model=StoreImportPreviewResponse)
async def get_store_product_link_preview(
    store_data: StoreValidate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_owner_user),
):
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    validation_service = OzonValidationService()
    result = await validation_service.validate_store(store_data.client_id, store_data.api_key)

    if not result["valid"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result.get("message") or "Не удалось проверить магазин",
        )

    linking_service = StoreLinkingService(db)
    return await linking_service.build_preview(cabinet_owner_id, store_data.client_id, store_data.api_key)


@router.post("", response_model=StoreResponse, status_code=status.HTTP_201_CREATED)
@router.post("/", response_model=StoreResponse, status_code=status.HTTP_201_CREATED, include_in_schema=False)
async def create_store(
    store_data: StoreCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_owner_user),
):
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    await _ensure_unique_client_id(db, cabinet_owner_id, store_data.client_id)
    existing_store_count = await db.scalar(
        select(func.count(Store.id)).where(Store.user_id == cabinet_owner_id)
    )
    is_first_store_in_cabinet = (existing_store_count or 0) == 0

    if store_data.product_links:
        warehouse_product_ids = {item.warehouse_product_id for item in store_data.product_links if item.warehouse_product_id}
        if warehouse_product_ids:
            result = await db.execute(
                select(WarehouseProduct.id).where(
                    WarehouseProduct.user_id == cabinet_owner_id,
                    WarehouseProduct.id.in_(warehouse_product_ids),
                )
            )
            allowed_ids = set(result.scalars().all())
            if allowed_ids != warehouse_product_ids:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Выбран чужой или несуществующий товар склада",
                )

    encrypted_key = encrypt_api_key(store_data.api_key)

    store = Store(
        user_id=cabinet_owner_id,
        name=store_data.name,
        client_id=store_data.client_id,
        api_key_encrypted=encrypted_key,
        is_active=True,
        economics_vat_mode=store_data.economics_vat_mode,
        economics_tax_mode=store_data.economics_tax_mode,
        economics_tax_rate=float(store_data.economics_tax_rate),
    )

    db.add(store)
    await db.commit()
    await db.refresh(store)

    await EconomicsHistoryService(db).ensure_store_history_entry(
        store=store,
        effective_from=store.created_at.date() if store.created_at else None,
        created_by_user_id=current_user.id,
    )
    await EconomicsHistoryService(db).sync_store_current_economics_from_history(store=store)
    await db.commit()

    settings_service = SettingsService(db)
    settings = await settings_service.get_settings(current_user.id)
    if settings.warehouse_mode == "per_store":
        await ensure_store_warehouse(db, cabinet_owner_id, store.id)
        await db.commit()
    elif settings.warehouse_mode == "shared" and not settings.shared_warehouse_id:
        await ensure_shared_warehouse(db, cabinet_owner_id, settings)
        await db.commit()

    warehouse_id = None
    if settings.warehouse_mode == "per_store":
        warehouse_id = await _get_store_warehouse_id(db, store.id)

    try:
        linking_service = StoreLinkingService(db)
        cached_attributes_payloads = None
        if store_data.product_links:
            cached_attributes_payloads = await linking_service.get_cached_catalog_snapshot(
                cabinet_owner_id,
                store_data.client_id,
                store_data.api_key,
            )

        if store_data.product_links:
            sync_service = SyncService(db)
            product_link_map: dict[str, list[dict[str, object | list[str] | None]]] = {}
            for item in store_data.product_links:
                link_key = item.group_key or item.base_name
                product_link_map.setdefault(link_key, []).append({
                    "warehouse_product_id": item.warehouse_product_id,
                    "warehouse_product_name": item.warehouse_product_name,
                    "offer_ids": item.offer_ids,
                })
            await sync_service.sync_products_for_store(
                store,
                product_link_map=product_link_map,
                preloaded_attributes_payloads=cached_attributes_payloads,
            )

        await _scheduler.clear_store_state(store.id)
        if is_first_store_in_cabinet:
            reset_bootstrap_state_sync(store.id)
            enqueue_full_sync(
                store.id,
                skip_products=bool(store_data.product_links),
                bootstrap=True,
                trigger="store_created",
            )
            await _mark_initial_full_sync_notice_pending(store.id)
            logger.info(f"📬 Initial bootstrap sync queued for first store {store.id}")
        else:
            mark_bootstrap_state_sync(store.id, BOOTSTRAP_STATE_COMPLETED)
            enqueue_full_sync(
                store.id,
                skip_products=bool(store_data.product_links),
                bootstrap=False,
                trigger="store_created",
            )
            logger.info(f"📬 Background full sync queued for additional store {store.id}")
    except Exception as e:
        logger.error(f"❌ Failed to prepare import for store {store.id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Не удалось подготовить товары нового магазина",
        )

    current_snapshot = await _get_current_store_economics_snapshot(db, store)
    return _serialize_store(
        store,
        warehouse_id,
        current_snapshot.effective_from,
        economics_vat_mode=current_snapshot.vat_mode,
        economics_tax_mode=current_snapshot.tax_mode,
        economics_tax_rate=current_snapshot.tax_rate,
    )


@router.get("", response_model=List[StoreResponse])
@router.get("/", response_model=List[StoreResponse], include_in_schema=False)
async def get_stores(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    latest_economics_subquery = (
        select(
            StoreEconomicsHistory.store_id.label("store_id"),
            StoreEconomicsHistory.effective_from.label("economics_effective_from"),
        )
        .distinct(StoreEconomicsHistory.store_id)
        .order_by(
            StoreEconomicsHistory.store_id,
            StoreEconomicsHistory.effective_from.desc(),
            StoreEconomicsHistory.id.desc(),
        )
        .subquery()
    )
    result = await db.execute(
        select(Store, Warehouse.id, latest_economics_subquery.c.economics_effective_from)
        .outerjoin(Warehouse, Warehouse.store_id == Store.id)
        .outerjoin(latest_economics_subquery, latest_economics_subquery.c.store_id == Store.id)
        .where(Store.user_id == get_cabinet_owner_id(current_user))
    )
    rows = result.all()
    serialized = []
    for store, warehouse_id, _latest_effective_from in rows:
        current_snapshot = await _get_current_store_economics_snapshot(db, store)
        serialized.append(
            _serialize_store(
                store,
                warehouse_id,
                current_snapshot.effective_from,
                economics_vat_mode=current_snapshot.vat_mode,
                economics_tax_mode=current_snapshot.tax_mode,
                economics_tax_rate=current_snapshot.tax_rate,
            )
        )
    return serialized


@router.get("/{store_id}")
async def get_store(
    store_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    store = await _get_owned_store(db, store_id, get_cabinet_owner_id(current_user))
    warehouse_id = await _get_store_warehouse_id(db, store.id)
    current_snapshot = await _get_current_store_economics_snapshot(db, store)
    return _serialize_store(
        store,
        warehouse_id,
        current_snapshot.effective_from,
        economics_vat_mode=current_snapshot.vat_mode,
        economics_tax_mode=current_snapshot.tax_mode,
        economics_tax_rate=current_snapshot.tax_rate,
    )


@router.get("/{store_id}/economics-history", response_model=List[StoreEconomicsHistoryEntryResponse])
async def get_store_economics_history(
    store_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    store = await _get_owned_store(db, store_id, get_cabinet_owner_id(current_user))
    result = await db.execute(
        select(StoreEconomicsHistory)
        .where(StoreEconomicsHistory.store_id == store.id)
        .order_by(StoreEconomicsHistory.effective_from.desc(), StoreEconomicsHistory.id.desc())
    )
    rows = result.scalars().all()
    return [
        StoreEconomicsHistoryEntryResponse(
            id=item.id,
            store_id=item.store_id,
            effective_from=item.effective_from,
            vat_mode=item.vat_mode,
            tax_mode=item.tax_mode,
            tax_rate=float(item.tax_rate or 0),
            created_at=item.created_at,
        )
        for item in rows
    ]


@router.delete("/{store_id}/economics-history/{history_id}")
async def delete_store_economics_history(
    store_id: int,
    history_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_owner_user),
):
    store = await _get_owned_store(db, store_id, get_cabinet_owner_id(current_user))
    history_rows = (
        await db.execute(
            select(StoreEconomicsHistory)
            .where(StoreEconomicsHistory.store_id == store.id)
            .order_by(StoreEconomicsHistory.effective_from.desc(), StoreEconomicsHistory.id.desc())
        )
    ).scalars().all()
    if not history_rows:
        raise HTTPException(status_code=404, detail="История налогов не найдена")

    target = next((item for item in history_rows if item.id == history_id), None)
    if target is None:
        raise HTTPException(status_code=404, detail="Запись истории не найдена")
    if len(history_rows) <= 1:
        raise HTTPException(status_code=400, detail="Нельзя удалить последнюю налоговую схему магазина")

    await db.delete(target)
    await db.flush()

    remaining = (
        await db.execute(
            select(StoreEconomicsHistory.id)
            .where(StoreEconomicsHistory.store_id == store.id)
            .limit(1)
        )
    ).scalar_one_or_none()
    if remaining is None:
        raise HTTPException(status_code=400, detail="После удаления не осталось налоговой схемы")

    await EconomicsHistoryService(db).sync_store_current_economics_from_history(store=store)
    await EconomicsHistoryService(db).unlock_store_months_from_date(
        store_id=store.id,
        effective_from=target.effective_from,
    )
    await db.commit()
    await _queue_closed_months_recalc_after_store_economics_change(
        store.id,
        effective_date=target.effective_from,
    )
    return {"status": "success", "message": "Запись истории удалена"}


@router.put("/{store_id}", response_model=StoreResponse)
async def update_store(
    store_id: int,
    store_data: StoreUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_owner_user),
):
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    store = await _get_owned_store(db, store_id, cabinet_owner_id)
    await _ensure_unique_client_id(db, cabinet_owner_id, store_data.client_id, excluded_store_id=store.id)

    store.name = store_data.name
    store.client_id = store_data.client_id
    store.economics_vat_mode = store_data.economics_vat_mode
    store.economics_tax_mode = store_data.economics_tax_mode
    store.economics_tax_rate = float(store_data.economics_tax_rate)
    store.economics_default_sale_price_gross = (
        float(store_data.economics_default_sale_price_gross)
        if store_data.economics_default_sale_price_gross is not None
        else None
    )
    if store_data.api_key:
        store.api_key_encrypted = encrypt_api_key(store_data.api_key)

    effective_from = store_data.economics_effective_from
    await EconomicsHistoryService(db).ensure_store_history_entry(
        store=store,
        effective_from=effective_from,
        created_by_user_id=current_user.id,
    )
    current_snapshot = await EconomicsHistoryService(db).sync_store_current_economics_from_history(store=store)
    await EconomicsHistoryService(db).unlock_store_months_from_date(
        store_id=store.id,
        effective_from=effective_from or datetime.now(timezone.utc).date(),
    )

    await db.commit()
    await db.refresh(store)

    await _queue_closed_months_recalc_after_store_economics_change(
        store.id,
        effective_date=effective_from,
    )

    warehouse_id = await _get_store_warehouse_id(db, store.id)
    return _serialize_store(
        store,
        warehouse_id,
        current_snapshot.effective_from,
        economics_vat_mode=current_snapshot.vat_mode,
        economics_tax_mode=current_snapshot.tax_mode,
        economics_tax_rate=current_snapshot.tax_rate,
    )


@router.patch("/{store_id}", response_model=StoreResponse)
async def patch_store(
    store_id: int,
    store_data: StorePatch,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_owner_user),
):
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    store = await _get_owned_store(db, store_id, cabinet_owner_id)
    payload = store_data.model_dump(exclude_unset=True)

    if "client_id" in payload:
        await _ensure_unique_client_id(db, cabinet_owner_id, payload["client_id"], excluded_store_id=store.id)

    if "name" in payload:
        store.name = payload["name"]
    if "client_id" in payload:
        store.client_id = payload["client_id"]
    if payload.get("api_key"):
        store.api_key_encrypted = encrypt_api_key(payload["api_key"])
    if "is_active" in payload:
        store.is_active = payload["is_active"]
    if "economics_vat_mode" in payload:
        store.economics_vat_mode = payload["economics_vat_mode"]
    if "economics_tax_mode" in payload:
        store.economics_tax_mode = payload["economics_tax_mode"]
    if "economics_tax_rate" in payload:
        store.economics_tax_rate = float(payload["economics_tax_rate"])
    if "economics_default_sale_price_gross" in payload:
        store.economics_default_sale_price_gross = (
            float(payload["economics_default_sale_price_gross"])
            if payload["economics_default_sale_price_gross"] is not None
            else None
        )

    if (
        "economics_vat_mode" in payload
        or "economics_tax_mode" in payload
        or "economics_tax_rate" in payload
        or "economics_effective_from" in payload
    ):
        effective_from = payload.get("economics_effective_from")
        await EconomicsHistoryService(db).ensure_store_history_entry(
            store=store,
            effective_from=effective_from,
            created_by_user_id=current_user.id,
        )
        current_snapshot = await EconomicsHistoryService(db).sync_store_current_economics_from_history(store=store)
        await EconomicsHistoryService(db).unlock_store_months_from_date(
            store_id=store.id,
            effective_from=effective_from or datetime.now(timezone.utc).date(),
        )
    else:
        current_snapshot = await _get_current_store_economics_snapshot(db, store)

    await db.commit()
    await db.refresh(store)

    if (
        "economics_vat_mode" in payload
        or "economics_tax_mode" in payload
        or "economics_tax_rate" in payload
        or "economics_effective_from" in payload
    ):
        await _queue_closed_months_recalc_after_store_economics_change(
            store.id,
            effective_date=payload.get("economics_effective_from"),
        )

    warehouse_id = await _get_store_warehouse_id(db, store.id)
    return _serialize_store(
        store,
        warehouse_id,
        current_snapshot.effective_from,
        economics_vat_mode=current_snapshot.vat_mode,
        economics_tax_mode=current_snapshot.tax_mode,
        economics_tax_rate=current_snapshot.tax_rate,
    )


@router.delete("/{store_id}")
async def delete_store(
    store_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_owner_user),
):
    """Удалить магазин вместе со всеми связанными данными."""
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    store = await _get_owned_store(db, store_id, cabinet_owner_id)

    linked_warehouse_products_result = await db.execute(
        select(Product.warehouse_product_id).where(Product.store_id == store.id)
    )
    linked_warehouse_product_ids = {
        warehouse_product_id
        for warehouse_product_id in linked_warehouse_products_result.scalars().all()
        if warehouse_product_id is not None
    }

    store.is_active = False
    await db.flush()
    await _scheduler.clear_store_state(store.id)

    await _cleanup_supply_related_rows(db, store.id)
    deleted_store_rowcount = await _cleanup_store_entities(db, store.id)

    if deleted_store_rowcount != 1:
        raise HTTPException(status_code=500, detail="Store delete was not completed")

    await _delete_orphaned_warehouse_products(db, cabinet_owner_id, store.id, linked_warehouse_product_ids)
    await _cleanup_store_redis_flags(store.id)

    await db.commit()
    return {"status": "deleted", "id": store_id}
