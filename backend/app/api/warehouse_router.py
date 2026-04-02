import random
import os
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import SessionLocal
from app.core.dependencies import get_current_user
from app.models.inventory_transaction import InventoryTransaction, TransactionType
from app.models.product import Product
from app.models.store import Store
from app.models.ozon_warehouse import OzonStock
from app.models.user import User
from app.models.variant import Variant
from app.models.variant_attribute import VariantAttribute
from app.models.warehouse import Warehouse, WarehouseStock
from app.schemas.warehouse import IncomeBatchRequest, IncomeRequest, PackBatchRequest, PackRequest, ReserveRequest
from app.services.cabinet_access import get_cabinet_owner_id
from app.services.export_status import (
    clear_export_status,
    get_export_status,
    has_export_lock,
    mark_export_queued,
)
from app.services.ozon.client import OzonClient
from app.services.ozon.report_snapshot_service import OzonReportSnapshotService
from app.services.product_grouping import extract_base_product_name
from app.services.settings_service import SettingsService
from app.services.sync_dispatcher import celery_app
from app.services.supply_reservation_wait import clear_supply_reservation_wait_for_variants
from app.services.warehouse_selector import resolve_warehouse
from app.services.warehouse_service import WarehouseService
from app.utils.encryption import decrypt_api_key

router = APIRouter(prefix="/warehouse", tags=["warehouse"])
service = WarehouseService()

async def get_db():
    async with SessionLocal() as session:
        yield session


async def _get_store_names_map(
    db: AsyncSession,
    user_id: int,
    store_ids: list[int] | None = None,
) -> dict[int, str]:
    stmt = select(Store.id, Store.name).where(Store.user_id == user_id)
    if store_ids:
        stmt = stmt.where(Store.id.in_(store_ids))
    result = await db.execute(stmt)
    return {row[0]: row[1] for row in result.all()}


def _coerce_snapshot_date(value: str | None) -> date | None:
    if not value:
        return None
    raw = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).date()
    except ValueError:
        return None


def _warehouse_group_key(product: Product, attrs: dict[str, str]) -> tuple[int, str, str]:
    return (
        int(product.warehouse_product_id or product.id),
        str(attrs.get("Цвет") or "").strip().lower(),
        str(attrs.get("Размер") or "").strip().lower(),
    )


async def _load_current_prices_by_offer_id(store: Store, offer_ids: list[str]) -> dict[str, float]:
    normalized_offer_ids = [str(offer_id).strip() for offer_id in offer_ids if str(offer_id).strip()]
    if not normalized_offer_ids:
        return {}

    client = OzonClient(
        store.client_id,
        decrypt_api_key(store.api_key_encrypted),
        store.name,
        emit_notifications=False,
    )
    prices_by_offer_id: dict[str, float] = {}
    try:
        try:
            chunk_size = 1000
            for chunk_start in range(0, len(normalized_offer_ids), chunk_size):
                chunk = normalized_offer_ids[chunk_start:chunk_start + chunk_size]
                items = await client.get_product_prices(offer_ids=chunk, visibility="ALL", limit=min(len(chunk), 1000))
                for item in items:
                    offer_id = str(item.get("offer_id") or "").strip()
                    if not offer_id:
                        continue
                    price_info = item.get("price") or {}
                    raw_price = (
                        price_info.get("price")
                        or price_info.get("marketing_seller_price")
                        or price_info.get("net_price")
                        or price_info.get("old_price")
                    )
                    if raw_price in (None, ""):
                        continue
                    try:
                        prices_by_offer_id[offer_id] = float(raw_price)
                    except (TypeError, ValueError):
                        continue
        except Exception as exc:
            logger.warning(
                "⚠️ Failed to load current Ozon prices for store {} ({}): {}",
                store.id,
                store.name,
                exc,
            )
    finally:
        await client.close()

    return prices_by_offer_id


@router.get("/info")
async def get_warehouse_info(
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Получить информацию о складах пользователя"""
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    settings = await SettingsService(db).get_settings(current_user.id)

    # Получаем все склады пользователя
    warehouses_stmt = select(Warehouse).where(Warehouse.user_id == cabinet_owner_id)
    warehouses_result = await db.execute(warehouses_stmt)
    warehouses = warehouses_result.scalars().all()
    store_ids = [warehouse.store_id for warehouse in warehouses if warehouse.store_id]
    store_names_map = await _get_store_names_map(db, cabinet_owner_id, store_ids)
    stores_result = await db.execute(select(Store.id, Store.client_id).where(Store.user_id == cabinet_owner_id))
    store_client_ids = {int(store_id): str(client_id) for store_id, client_id in stores_result.all()}

    warehouses_list = []
    for w in warehouses:
        warehouses_list.append({
            "id": w.id,
            "name": w.name,
            "store_id": w.store_id,
            "store_name": store_names_map.get(w.store_id),
        })

    order_snapshots = []
    for client_id in store_client_ids.values():
        snapshot = await OzonReportSnapshotService.get_cached_snapshot_for_client(client_id=client_id, kind="postings")
        if snapshot:
            order_snapshots.append(snapshot)

    period_from_values = [
        snapshot.get("filters", {}).get("processed_at_from")
        for snapshot in order_snapshots
        if (snapshot.get("filters") or {}).get("processed_at_from")
    ]
    period_to_values = [
        snapshot.get("filters", {}).get("processed_at_to")
        for snapshot in order_snapshots
        if (snapshot.get("filters") or {}).get("processed_at_to")
    ]
    updated_values = [snapshot.get("refreshed_at") for snapshot in order_snapshots if snapshot.get("refreshed_at")]

    return {
        "mode": settings.warehouse_mode if settings else "shared",
        "packing_mode": settings.packing_mode if settings else "simple",
        "orders_period_from": min(period_from_values, default=None),
        "orders_period_to": max(period_to_values, default=None),
        "orders_updated_at": max(updated_values, default=None),
        "orders_stores_covered": len(order_snapshots),
        "orders_stores_missing": max(len(store_client_ids) - len(order_snapshots), 0),
        "warehouses": warehouses_list
    }

@router.get("/stocks")
async def get_stocks(
        store_id: Optional[int] = Query(None, description="ID магазина (для per_store mode)"),
        warehouse_id: Optional[int] = Query(None, description="ID склада (если известен)"),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """
    Получить остатки на складе.
    Если warehouse_id указан - используем его.
    Иначе определяем склад по режиму и store_id.
    """
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    try:
        if warehouse_id or store_id is not None:
            warehouse, settings = await resolve_warehouse(
                db,
                user_id=cabinet_owner_id,
                store_id=store_id,
                warehouse_id=warehouse_id
            )
            warehouse_ids = [warehouse.id]
        else:
            # Если склад не указан, показываем все склады пользователя
            settings = await SettingsService(db).get_settings(current_user.id)
            warehouses_stmt = select(Warehouse.id).where(Warehouse.user_id == cabinet_owner_id)
            warehouses_result = await db.execute(warehouses_stmt)
            warehouse_ids = [row[0] for row in warehouses_result.all()]
    except ValueError as e:
        raise HTTPException(400, str(e))

    # Получаем остатки
    warehouses_map = {}
    if not warehouse_ids:
        return []

    store_names_map = await _get_store_names_map(db, cabinet_owner_id)
    store_client_ids_result = await db.execute(select(Store.id, Store.client_id).where(Store.user_id == cabinet_owner_id))
    postings_snapshot_by_store = {
        int(store_id): await OzonReportSnapshotService.get_cached_snapshot_for_client(client_id=str(client_id), kind="postings")
        for store_id, client_id in store_client_ids_result.all()
    }
    warehouses_stmt = select(Warehouse).where(Warehouse.id.in_(warehouse_ids))
    warehouses_result = await db.execute(warehouses_stmt)
    for w in warehouses_result.scalars().all():
        warehouses_map[w.id] = {
            "name": w.name,
            "store_id": w.store_id,
            "store_name": store_names_map.get(w.store_id),
        }

    stmt = select(
        WarehouseStock,
        Variant,
        Product
    ).join(
        Variant, Variant.id == WarehouseStock.variant_id
    ).join(
        Product, Product.id == Variant.product_id
    ).where(
        WarehouseStock.warehouse_id.in_(warehouse_ids)
    )

    result = await db.execute(stmt)
    stocks = result.all()

    # Загружаем характеристики вариаций
    variant_ids = [stock.WarehouseStock.variant_id for stock in stocks]
    attrs_map = {}
    if variant_ids:
        attrs_stmt = select(VariantAttribute).where(VariantAttribute.variant_id.in_(variant_ids))
        attrs_result = await db.execute(attrs_stmt)
        for attr in attrs_result.scalars().all():
            attrs_map.setdefault(attr.variant_id, {})[attr.name] = attr.value

    response = []
    for stock in stocks:
        pack_size = stock.Variant.pack_size or 1
        attributes = attrs_map.get(stock.WarehouseStock.variant_id, {})
        color = attributes.get("Цвет", "")
        size = attributes.get("Размер", "")
        packed_units = 0 if settings.packing_mode == "simple" else stock.WarehouseStock.packed_quantity * pack_size
        available = stock.WarehouseStock.unpacked_quantity + packed_units - stock.WarehouseStock.reserved_quantity
        product_name = stock.Product.base_name or extract_base_product_name(stock.Product.name)

        response.append({
            "variant_id": stock.WarehouseStock.variant_id,
            "sku": stock.Variant.sku,
            "offer_id": stock.Variant.offer_id,
            "product_name": product_name,
            "pack_size": pack_size,
            "color": color,
            "size": size,
            "unpacked": stock.WarehouseStock.unpacked_quantity,
            "packed": stock.WarehouseStock.packed_quantity,
            "reserved": stock.WarehouseStock.reserved_quantity,
            "available": available,
            "warehouse_id": stock.WarehouseStock.warehouse_id,
            "warehouse_name": warehouses_map.get(stock.WarehouseStock.warehouse_id, {}).get("name"),
            "store_id": warehouses_map.get(stock.WarehouseStock.warehouse_id, {}).get("store_id"),
            "store_name": warehouses_map.get(stock.WarehouseStock.warehouse_id, {}).get("store_name"),
            "product_store_id": stock.Product.store_id,
            "product_store_name": store_names_map.get(stock.Product.store_id),
            "ordered_30d": OzonReportSnapshotService.get_storewide_postings_order_units(
                postings_snapshot_by_store.get(int(stock.Product.store_id)),
                sku=stock.Variant.sku,
                offer_id=stock.Variant.offer_id,
            ),
            "attributes": attributes
        })

    group_totals = {}
    for item in response:
        group_key = (
            item["product_name"].strip().lower(),
            (item.get("color") or "").strip().lower(),
            (item.get("size") or "").strip().lower(),
        )
        group = group_totals.setdefault(group_key, {"unpacked": 0, "reserved": 0, "packed_units": 0, "ordered_30d": 0})
        group["unpacked"] += item["unpacked"]
        group["reserved"] += item["reserved"]
        group["ordered_30d"] += item.get("ordered_30d", 0)
        if settings.packing_mode == "advanced":
            group["packed_units"] += item["packed"] * item["pack_size"]

    for item in response:
        group_key = (
            item["product_name"].strip().lower(),
            (item.get("color") or "").strip().lower(),
            (item.get("size") or "").strip().lower(),
        )
        group = group_totals[group_key]
        item["group_unpacked"] = group["unpacked"]
        item["group_reserved"] = group["reserved"]
        item["group_packed_units"] = group["packed_units"]
        item["group_ordered_30d"] = group["ordered_30d"]
        if settings.packing_mode == "simple":
            item["group_available"] = group["unpacked"] - group["reserved"]
        else:
            item["group_available"] = group["unpacked"] + group["packed_units"] - group["reserved"]
        item["available"] = item["group_available"]

    return response


@router.get("/overview")
async def get_warehouse_overview(
        store_id: int = Query(..., description="ID активного магазина"),
        order_window_days: int = Query(7, description="Период заказов: 7, 30 или 90 дней"),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    normalized_window_days = 90 if order_window_days >= 90 else (30 if order_window_days >= 30 else 7)

    store_result = await db.execute(
        select(Store).where(Store.user_id == cabinet_owner_id, Store.id == store_id)
    )
    store = store_result.scalar_one_or_none()
    if store is None:
        raise HTTPException(404, "Магазин не найден")

    warehouse, settings = await resolve_warehouse(
        db,
        user_id=cabinet_owner_id,
        store_id=store_id,
    )

    variant_rows_result = await db.execute(
        select(Variant, Product)
        .join(Product, Product.id == Variant.product_id)
        .where(
            Product.store_id == store_id,
            Variant.is_archived.is_(False),
        )
    )
    variant_rows = variant_rows_result.all()

    variant_ids = [row.Variant.id for row in variant_rows]
    if not variant_ids:
        return {
            "store_id": store.id,
            "store_name": store.name,
            "warehouse_mode": settings.warehouse_mode,
            "packing_mode": settings.packing_mode,
            "warehouse_name": warehouse.name,
            "warehouse_scope": "shared" if settings.warehouse_mode == "shared" else "per_store",
            "order_window_days": normalized_window_days,
            "available_order_windows": [7, 30],
            "products": [],
        }

    attrs_map: dict[int, dict[str, str]] = {}
    attrs_result = await db.execute(select(VariantAttribute).where(VariantAttribute.variant_id.in_(variant_ids)))
    for attr in attrs_result.scalars().all():
        attrs_map.setdefault(attr.variant_id, {})[attr.name] = attr.value

    stock_result = await db.execute(
        select(WarehouseStock).where(
            WarehouseStock.warehouse_id == warehouse.id,
            WarehouseStock.variant_id.in_(variant_ids),
        )
    )
    stock_map = {item.variant_id: item for item in stock_result.scalars().all()}

    group_warehouse_totals: dict[tuple[int, str, str], dict[str, int]] = defaultdict(
        lambda: {"unpacked": 0, "reserved": 0, "available": 0}
    )
    for row in variant_rows:
        variant = row.Variant
        product = row.Product
        stock = stock_map.get(variant.id)
        attrs = attrs_map.get(variant.id, {})
        pack_size = variant.pack_size or 1
        packed_units = 0 if settings.packing_mode == "simple" else ((stock.packed_quantity if stock else 0) * pack_size)
        unpacked = int(stock.unpacked_quantity if stock else 0)
        reserved = int(stock.reserved_quantity if stock else 0)
        available = int(unpacked + packed_units - reserved)
        group = group_warehouse_totals[_warehouse_group_key(product, attrs)]
        group["unpacked"] += unpacked
        group["reserved"] += reserved
        group["available"] += available

    ozon_result = await db.execute(
        select(OzonStock).where(OzonStock.variant_id.in_(variant_ids))
    )
    ozon_totals_by_variant: dict[int, dict[str, int]] = defaultdict(
        lambda: {
            "ozon_available": 0,
            "ozon_ready_for_sale": 0,
            "ozon_requested_to_supply": 0,
            "ozon_in_transit": 0,
            "ozon_returning": 0,
        }
    )
    for item in ozon_result.scalars().all():
        bucket = ozon_totals_by_variant[item.variant_id]
        bucket["ozon_available"] += int(item.available_to_sell or 0)
        bucket["ozon_ready_for_sale"] += int(item.in_supply or 0)
        bucket["ozon_requested_to_supply"] += int(item.requested_to_supply or 0)
        bucket["ozon_in_transit"] += int(item.in_transit or 0)
        bucket["ozon_returning"] += int(item.returning or 0)

    postings_snapshot = await OzonReportSnapshotService.get_cached_snapshot_for_client(
        client_id=str(store.client_id),
        kind="postings",
    )
    current_prices_by_offer_id = await _load_current_prices_by_offer_id(
        store,
        [row.Variant.offer_id for row in variant_rows],
    )
    snapshot_filters = (postings_snapshot or {}).get("filters") or {}
    snapshot_start = _coerce_snapshot_date(snapshot_filters.get("processed_at_from"))
    snapshot_end = _coerce_snapshot_date(snapshot_filters.get("processed_at_to"))
    today = datetime.now(timezone.utc).date()
    order_window_start = today - timedelta(days=max(normalized_window_days - 1, 0))
    available_order_windows = [7, 30]
    if snapshot_start and (today - snapshot_start).days >= 89:
        available_order_windows.append(90)

    products_map: dict[int, dict] = {}
    for row in variant_rows:
        variant = row.Variant
        product = row.Product
        attrs = attrs_map.get(variant.id, {})
        warehouse_group_key = _warehouse_group_key(product, attrs)
        group_totals = group_warehouse_totals[warehouse_group_key]
        ozon_totals = ozon_totals_by_variant[variant.id]
        order_window = OzonReportSnapshotService.get_storewide_postings_offer_window_summary(
            postings_snapshot,
            offer_id=variant.offer_id,
            start=order_window_start,
            end=today,
        )
        current_price = current_prices_by_offer_id.get(str(variant.offer_id).strip())
        product_bucket = products_map.setdefault(
            product.id,
            {
                "product_id": product.id,
                "product_name": product.base_name or extract_base_product_name(product.name),
                "image_url": product.image_url,
                "warehouse_unpacked": 0,
                "warehouse_reserved": 0,
                "warehouse_available": 0,
                "ozon_available": 0,
                "ozon_ready_for_sale": 0,
                "ozon_requested_to_supply": 0,
                "ozon_in_transit": 0,
                "ozon_returning": 0,
                "ordered_units": 0,
                "colors": defaultdict(list),
                "_warehouse_group_keys": set(),
            },
        )

        variant_payload = {
            "variant_id": variant.id,
            "offer_id": variant.offer_id,
            "sku": variant.sku,
            "pack_size": int(variant.pack_size or 1),
            "color": attrs.get("Цвет") or "",
            "size": attrs.get("Размер") or "",
            "attributes": attrs,
            "warehouse_unpacked": int(group_totals["unpacked"]),
            "warehouse_reserved": int(group_totals["reserved"]),
            "warehouse_available": int(group_totals["available"]),
            "ozon_available": int(ozon_totals["ozon_available"]),
            "ozon_ready_for_sale": int(ozon_totals["ozon_ready_for_sale"]),
            "ozon_requested_to_supply": int(ozon_totals["ozon_requested_to_supply"]),
            "ozon_in_transit": int(ozon_totals["ozon_in_transit"]),
            "ozon_returning": int(ozon_totals["ozon_returning"]),
            "ordered_units": int(round(float(order_window.get("units") or 0))),
            "current_price": current_price,
            "total_units": int(group_totals["available"]) + int(ozon_totals["ozon_available"]),
        }

        if not any([
            variant_payload["warehouse_unpacked"],
            variant_payload["warehouse_reserved"],
            variant_payload["warehouse_available"],
            variant_payload["ozon_available"],
            variant_payload["ozon_ready_for_sale"],
            variant_payload["ozon_requested_to_supply"],
            variant_payload["ozon_in_transit"],
            variant_payload["ozon_returning"],
            variant_payload["ordered_units"],
        ]):
            continue

        if warehouse_group_key not in product_bucket["_warehouse_group_keys"]:
            product_bucket["_warehouse_group_keys"].add(warehouse_group_key)
            product_bucket["warehouse_unpacked"] = int(product_bucket["warehouse_unpacked"]) + variant_payload["warehouse_unpacked"]
            product_bucket["warehouse_reserved"] = int(product_bucket["warehouse_reserved"]) + variant_payload["warehouse_reserved"]
            product_bucket["warehouse_available"] = int(product_bucket["warehouse_available"]) + variant_payload["warehouse_available"]
        product_bucket["ozon_available"] = int(product_bucket["ozon_available"]) + variant_payload["ozon_available"]
        product_bucket["ozon_ready_for_sale"] = int(product_bucket["ozon_ready_for_sale"]) + variant_payload["ozon_ready_for_sale"]
        product_bucket["ozon_requested_to_supply"] = int(product_bucket["ozon_requested_to_supply"]) + variant_payload["ozon_requested_to_supply"]
        product_bucket["ozon_in_transit"] = int(product_bucket["ozon_in_transit"]) + variant_payload["ozon_in_transit"]
        product_bucket["ozon_returning"] = int(product_bucket["ozon_returning"]) + variant_payload["ozon_returning"]
        product_bucket["ordered_units"] = int(product_bucket["ordered_units"]) + variant_payload["ordered_units"]
        product_bucket["colors"][variant_payload["color"] or "Без цвета"].append(variant_payload)

    products: list[dict] = []
    for product in sorted(products_map.values(), key=lambda item: str(item["product_name"]).lower()):
        colors = []
        for color, variants_list in sorted(product["colors"].items(), key=lambda pair: pair[0].lower()):
            colors.append(
                {
                    "color": color,
                    "variants": sorted(
                        variants_list,
                        key=lambda item: (
                            str(item.get("size") or "").lower(),
                            str(item.get("offer_id") or "").lower(),
                        ),
                    ),
                }
            )
        products.append(
            {
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "image_url": product["image_url"],
                "warehouse_unpacked": product["warehouse_unpacked"],
                "warehouse_reserved": product["warehouse_reserved"],
                "warehouse_available": product["warehouse_available"],
                "ozon_available": product["ozon_available"],
                "ozon_ready_for_sale": product["ozon_ready_for_sale"],
                "ozon_requested_to_supply": product["ozon_requested_to_supply"],
                "ozon_in_transit": product["ozon_in_transit"],
                "ozon_returning": product["ozon_returning"],
                "ordered_units": product["ordered_units"],
                "total_units": int(product["warehouse_available"]) + int(product["ozon_available"]),
                "colors": colors,
            }
        )

    return {
        "store_id": store.id,
        "store_name": store.name,
        "warehouse_mode": settings.warehouse_mode,
        "packing_mode": settings.packing_mode,
        "warehouse_name": warehouse.name,
        "warehouse_scope": "shared" if settings.warehouse_mode == "shared" else "per_store",
        "order_window_days": normalized_window_days,
        "available_order_windows": available_order_windows,
        "orders_period_from": snapshot_filters.get("processed_at_from"),
        "orders_period_to": snapshot_filters.get("processed_at_to"),
        "orders_updated_at": (postings_snapshot or {}).get("refreshed_at"),
        "products": products,
    }


@router.post("/export")
async def start_warehouse_export(
        store_id: int = Query(..., description="ID активного магазина"),
        order_window_days: int = Query(7, description="Период заказов: 7, 30 или 90 дней"),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    store_result = await db.execute(
        select(Store.id).where(Store.user_id == cabinet_owner_id, Store.id == store_id)
    )
    if store_result.scalar_one_or_none() is None:
        raise HTTPException(404, "Магазин не найден")

    normalized_window_days = 90 if order_window_days >= 90 else (30 if order_window_days >= 30 else 7)
    current_status = get_export_status("warehouse", store_id, current_user.id)
    if has_export_lock("warehouse", store_id) or current_status.get("status") in {"queued", "running"}:
        return {
            **current_status,
            "status": current_status.get("status") if current_status.get("status") in {"queued", "running"} else "running",
            "phase": current_status.get("phase") or "queued",
            "phase_label": current_status.get("phase_label") or "Уже формируется",
            "progress_percent": current_status.get("progress_percent") or 5,
            "message": current_status.get("message") or "Excel по складу уже формируется для этого магазина",
            "duplicate_request": True,
        }

    task = celery_app.send_task(
        "worker.tasks.export_warehouse_excel_task",
        args=[store_id, current_user.id, normalized_window_days],
    )
    return mark_export_queued(
        "warehouse",
        store_id,
        current_user.id,
        task_id=str(task.id),
        message="Excel по складу поставлен в очередь",
        order_window_days=normalized_window_days,
    )


@router.get("/export/status")
async def get_warehouse_export_status(
        store_id: int = Query(..., description="ID активного магазина"),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    store_result = await db.execute(
        select(Store.id).where(Store.user_id == cabinet_owner_id, Store.id == store_id)
    )
    if store_result.scalar_one_or_none() is None:
        raise HTTPException(404, "Магазин не найден")
    return get_export_status("warehouse", store_id, current_user.id)


@router.get("/export/download")
async def download_warehouse_export(
        store_id: int = Query(..., description="ID активного магазина"),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    store_result = await db.execute(
        select(Store.id).where(Store.user_id == cabinet_owner_id, Store.id == store_id)
    )
    if store_result.scalar_one_or_none() is None:
        raise HTTPException(404, "Магазин не найден")

    status = get_export_status("warehouse", store_id, current_user.id)
    file_path = str(status.get("file_path") or "")
    file_name = str(status.get("file_name") or "warehouse.xlsx")
    if status.get("status") != "success" or not file_path:
        raise HTTPException(404, "Готовый Excel пока не найден")
    if not os.path.exists(file_path):
        raise HTTPException(404, "Файл выгрузки устарел или недоступен. Сформируй Excel заново.")
    return FileResponse(path=file_path, filename=file_name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@router.delete("/export")
async def clear_warehouse_export(
        store_id: int = Query(..., description="ID активного магазина"),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    store_result = await db.execute(
        select(Store.id).where(Store.user_id == cabinet_owner_id, Store.id == store_id)
    )
    if store_result.scalar_one_or_none() is None:
        raise HTTPException(404, "Магазин не найден")
    if has_export_lock("warehouse", store_id):
        raise HTTPException(409, "Нельзя очистить отчет, пока он формируется")
    return clear_export_status("warehouse", store_id, current_user.id)


@router.get("/stock/{warehouse_id}")
async def get_stock_by_id(
        warehouse_id: int,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Совместимость: получить остатки по ID склада"""
    return await get_stocks(warehouse_id=warehouse_id, db=db, current_user=current_user)

@router.post("/income")
async def income(
        data: IncomeRequest,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Приход товара на склад"""
    try:
            warehouse, _ = await resolve_warehouse(
                db,
                user_id=get_cabinet_owner_id(current_user),
                store_id=data.store_id,
                warehouse_id=data.warehouse_id
            )
    except ValueError as e:
        raise HTTPException(400, str(e))

    result = await service.income(db, warehouse.id, data.variant_id, data.quantity)
    try:
        await clear_supply_reservation_wait_for_variants(db, [data.variant_id])
    except Exception as e:
        logger.warning("⚠️ Failed to refresh waiting reservations after income for variant {}: {}", data.variant_id, e)
    return result


@router.post("/income-batch")
async def income_batch(
        data: IncomeBatchRequest,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Пакетный приход товара на склад"""
    if not data.items:
        raise HTTPException(400, "Нет строк для прихода")

    try:
            warehouse, _ = await resolve_warehouse(
                db,
                user_id=get_cabinet_owner_id(current_user),
                store_id=data.store_id,
                warehouse_id=data.warehouse_id
            )
    except ValueError as e:
        raise HTTPException(400, str(e))

    batch_id = random.randint(100000000, 999999999)

    try:
        for item in data.items:
            await service.income(
                db,
                warehouse.id,
                item.variant_id,
                item.quantity,
                reference_type="MANUAL_BATCH",
                reference_id=batch_id,
                commit=False,
            )
        await db.commit()
        variant_ids = list({item.variant_id for item in data.items})
        try:
            await clear_supply_reservation_wait_for_variants(db, variant_ids)
        except Exception as e:
            logger.warning("⚠️ Failed to refresh waiting reservations after income batch: {}", e)
        return {"status": "ok", "batch_id": batch_id, "items": len(data.items)}
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, str(e))


@router.post("/pack")
async def pack(
        data: PackRequest,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Упаковка товара"""
    try:
            warehouse, settings = await resolve_warehouse(
                db,
                user_id=get_cabinet_owner_id(current_user),
                store_id=data.store_id,
                warehouse_id=data.warehouse_id
            )
    except ValueError as e:
        raise HTTPException(400, str(e))

    return await service.pack(
        db,
        warehouse.id,
        data.variant_id,
        data.boxes,
        packing_mode=settings.packing_mode
    )


@router.post("/pack-batch")
async def pack_batch(
        data: PackBatchRequest,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Пакетная упаковка товара"""
    if not data.items:
        raise HTTPException(400, "Нет строк для упаковки")

    try:
            warehouse, settings = await resolve_warehouse(
                db,
                user_id=get_cabinet_owner_id(current_user),
                store_id=data.store_id,
                warehouse_id=data.warehouse_id
            )
    except ValueError as e:
        raise HTTPException(400, str(e))

    batch_id = random.randint(100000000, 999999999)
    aggregated_items: dict[int, int] = {}
    for item in data.items:
        aggregated_items[item.variant_id] = aggregated_items.get(item.variant_id, 0) + item.boxes

    try:
        for variant_id, boxes in aggregated_items.items():
            availability = await service.get_pack_availability(db, warehouse.id, variant_id)
            required_units = boxes * int(availability["pack_size"])
            available_units = int(availability["unpacked_quantity"])
            if available_units < required_units:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Недостаточно неупакованного остатка для {availability['variant_label']}. "
                        f"Доступно: {available_units} шт, нужно: {required_units} шт."
                    ),
                )

        for variant_id, boxes in aggregated_items.items():
            await service.pack(
                db,
                warehouse.id,
                variant_id,
                boxes,
                packing_mode=settings.packing_mode,
                reference_type="PACKING_BATCH",
                reference_id=batch_id,
                commit=False,
            )
        await db.commit()
        return {"status": "ok", "batch_id": batch_id, "items": len(aggregated_items)}
    except HTTPException:
        await db.rollback()
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, str(e))


@router.post("/reserve")
async def reserve(
        data: ReserveRequest,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Резервирование товара под поставку"""
    try:
            warehouse, settings = await resolve_warehouse(
                db,
                user_id=get_cabinet_owner_id(current_user),
                store_id=data.store_id,
                warehouse_id=data.warehouse_id
            )
    except ValueError as e:
        raise HTTPException(400, str(e))

    return await service.reserve(
        db,
        warehouse.id,
        data.variant_id,
        data.quantity,
        data.supply_id,
        packing_mode=settings.packing_mode
    )


@router.get("/transactions")
async def get_transactions(
        store_id: Optional[int] = Query(None),
        warehouse_id: Optional[int] = Query(None),
        limit: int = 50,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Получить транзакции склада"""
    try:
        if warehouse_id or store_id:
            warehouse, _ = await resolve_warehouse(
                db,
                user_id=get_cabinet_owner_id(current_user),
                store_id=store_id,
                warehouse_id=warehouse_id
            )
            warehouse_ids = [warehouse.id]
        else:
            warehouses_stmt = select(Warehouse.id).where(Warehouse.user_id == get_cabinet_owner_id(current_user))
            warehouses_result = await db.execute(warehouses_stmt)
            warehouse_ids = [row[0] for row in warehouses_result.all()]
    except ValueError as e:
        raise HTTPException(400, str(e))

    if not warehouse_ids:
        return []

    stmt = select(
        InventoryTransaction,
        Variant,
        Product,
    ).join(
        Variant, Variant.id == InventoryTransaction.variant_id
    ).join(
        Product, Product.id == Variant.product_id
    ).where(
        InventoryTransaction.warehouse_id.in_(warehouse_ids)
    ).order_by(
        InventoryTransaction.created_at.desc()
    ).limit(limit)

    result = await db.execute(stmt)
    transactions = result.all()
    store_names_map = await _get_store_names_map(db, get_cabinet_owner_id(current_user))
    variant_ids = [t.InventoryTransaction.variant_id for t in transactions]
    attrs_map = {}
    if variant_ids:
        attrs_stmt = select(VariantAttribute).where(VariantAttribute.variant_id.in_(variant_ids))
        attrs_result = await db.execute(attrs_stmt)
        for attr in attrs_result.scalars().all():
            attrs_map.setdefault(attr.variant_id, {})[attr.name] = attr.value

    response = []
    for t in transactions:
        attributes = attrs_map.get(t.InventoryTransaction.variant_id, {})
        reference_type = t.InventoryTransaction.reference_type
        reference_id = t.InventoryTransaction.reference_id
        batch_key = f"{reference_type}:{reference_id}" if reference_id is not None else f"tx:{t.InventoryTransaction.id}"

        response.append({
            "id": t.InventoryTransaction.id,
            "type": t.InventoryTransaction.type.value,
            "quantity": t.InventoryTransaction.quantity,
            "variant_id": t.InventoryTransaction.variant_id,
            "offer_id": t.Variant.offer_id,
            "pack_size": t.Variant.pack_size or 1,
            "product_name": t.Product.base_name or extract_base_product_name(t.Product.name),
            "product_store_id": t.Product.store_id,
            "product_store_name": store_names_map.get(t.Product.store_id),
            "color": attributes.get("Цвет", ""),
            "size": attributes.get("Размер", ""),
            "attributes": attributes,
            "created_at": t.InventoryTransaction.created_at.isoformat(),
            "reference_type": reference_type,
            "reference_id": reference_id,
            "batch_key": batch_key,
            "can_delete": t.InventoryTransaction.type in {TransactionType.INCOME, TransactionType.PACK}
                          and reference_type in {"MANUAL", "MANUAL_BATCH", "PACKING", "PACKING_BATCH"},
        })

    return response


@router.delete("/transactions/{transaction_id}")
async def delete_transaction(
        transaction_id: int,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    transaction = await db.get(InventoryTransaction, transaction_id)
    if not transaction:
        raise HTTPException(404, "Операция не найдена")

    warehouse = await db.get(Warehouse, transaction.warehouse_id)
    if not warehouse or warehouse.user_id != get_cabinet_owner_id(current_user):
        raise HTTPException(403, "Нет доступа к операции")

    settings = await SettingsService(db).get_settings(current_user.id)

    if transaction.type not in {TransactionType.INCOME, TransactionType.PACK}:
        raise HTTPException(400, "Удалять можно только приход и упаковку")

    try:
        return await service.delete_manual_transaction(
            db,
            transaction,
            packing_mode=settings.packing_mode if settings else None,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.get("/variant/{variant_id}/attributes")
async def get_variant_attributes(
        variant_id: int,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Получить характеристики вариации (только для товаров пользователя)"""
    variant = await db.get(Variant, variant_id)
    if not variant:
        raise HTTPException(404, "Variant not found")

    product = await db.get(Product, variant.product_id)
    if not product:
        raise HTTPException(404, "Product not found")

    store = await db.get(Store, product.store_id)
    if not store or store.user_id != get_cabinet_owner_id(current_user):
        raise HTTPException(403, "Not enough permissions")

    stmt = select(VariantAttribute).where(VariantAttribute.variant_id == variant_id)
    result = await db.execute(stmt)
    attributes = result.scalars().all()

    return [{
        "id": attr.id,
        "name": attr.name,
        "value": attr.value
    } for attr in attributes]
