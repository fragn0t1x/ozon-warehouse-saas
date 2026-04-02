from datetime import datetime, timedelta, timezone
import os
from typing import Optional
from collections import defaultdict
from statistics import median

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_current_user
from app.database import SessionLocal
from app.models.ozon_warehouse import Cluster, OzonWarehouse, OzonStock
from app.models.product import Product
from app.models.store import Store
from app.models.supply import Supply
from app.models.supply import SupplyItem
from app.models.user import User
from app.models.variant import Variant
from app.models.variant_attribute import VariantAttribute
from app.models.warehouse import WarehouseStock
from app.services.cabinet_access import get_cabinet_owner_id
from app.services.export_status import (
    clear_export_status,
    get_export_status,
    has_export_lock,
    mark_export_queued,
)
from app.services.ozon.report_snapshot_service import OzonReportSnapshotService
from app.services.product_grouping import extract_base_product_name
from app.services.shipments_cache import shipments_response_cache_key
from app.services.sync_dispatcher import celery_app
from app.services.warehouse_selector import resolve_warehouse
from app.services.ozon.client import OzonClient
from app.utils.redis_cache import cache_get_json, cache_set_json
from app.utils.encryption import decrypt_api_key

router = APIRouter(prefix="/shipments", tags=["shipments"])
SHIPMENTS_RESPONSE_CACHE_TTL_SECONDS = 120


async def get_db():
    async with SessionLocal() as session:
        yield session


async def _avg_delivery_days(db: AsyncSession, store_id: int, storage_warehouse_id: Optional[int]) -> int:
    stmt = select(Supply).where(
        Supply.store_id == store_id,
        Supply.timeslot_from.is_not(None),
        func.coalesce(Supply.acceptance_at_storage_at, Supply.completed_at).is_not(None),
        func.coalesce(Supply.acceptance_at_storage_at, Supply.completed_at) >= Supply.timeslot_from,
    ).order_by(func.coalesce(Supply.acceptance_at_storage_at, Supply.completed_at).desc()).limit(20)

    if storage_warehouse_id:
        stmt = stmt.where(Supply.storage_warehouse_id == storage_warehouse_id)

    result = await db.execute(stmt)
    supplies = result.scalars().all()

    if not supplies:
        return 2

    deltas = []
    for supply in supplies:
        try:
            arrival_at = supply.acceptance_at_storage_at or supply.completed_at
            if arrival_at is None:
                continue
            delta = (arrival_at.date() - supply.timeslot_from.date()).days
            deltas.append(max(delta, 0))
        except Exception:
            continue

    if not deltas:
        return 2

    return max(1, round(median(deltas)))


def _derive_cluster_name(warehouse_name: Optional[str]) -> str:
    if not warehouse_name:
        return "Без кластера"

    normalized = warehouse_name.strip()
    if not normalized:
        return "Без кластера"

    first_part = next((part for part in normalized.split("_") if part), None)
    return first_part or normalized


def _stock_group_key(warehouse_product_id: Optional[int], variant_id: int, attributes: dict[str, str]) -> tuple[int, str, str]:
    color = (attributes.get("Цвет") or "").strip().lower()
    size = (attributes.get("Размер") or "").strip().lower()
    return (warehouse_product_id or variant_id, color, size)


def _product_bucket_key(store_id: int, product: Product) -> str:
    base_product_name = product.base_name or extract_base_product_name(product.name)
    return f"{store_id}:{base_product_name.lower()}"


def _ensure_product_bucket(warehouse_bucket: dict, store_id: int, product: Product) -> dict:
    product_bucket = warehouse_bucket["products"]
    product_key = _product_bucket_key(store_id, product)
    if product_key not in product_bucket:
        product_bucket[product_key] = {
            "product_id": product.id,
            "product_name": product.base_name or extract_base_product_name(product.name),
            "variants": [],
        }
    return product_bucket[product_key]


def _warehouse_has_ozon_signal(warehouse: dict) -> bool:
    return (
        int(warehouse.get("total_ozon_available") or 0) > 0
        or int(warehouse.get("total_in_pipeline") or 0) > 0
        or int(warehouse.get("total_ordered_30d") or 0) > 0
        or len(warehouse.get("pending_departure_supplies") or []) > 0
        or len(warehouse.get("in_transit_supplies") or []) > 0
    )


def _looks_unknown_warehouse(warehouse: dict) -> bool:
    normalized_name = str(warehouse.get("name") or "").strip().lower()
    return not warehouse.get("ozon_id") or normalized_name == "unknown"


def _filter_shipments_clusters(
    clusters: list[dict],
    *,
    product_filter: str | None = None,
    selected_product_names: list[str] | None = None,
) -> list[dict]:
    normalized_filter = str(product_filter or "").strip().lower()
    selected_names = {str(name).strip() for name in (selected_product_names or []) if str(name).strip()}

    if not normalized_filter and not selected_names:
        result: list[dict] = []
        for cluster in clusters:
            visible_warehouses = [
                warehouse
                for warehouse in (cluster.get("warehouses") or [])
                if (not _looks_unknown_warehouse(warehouse)) or _warehouse_has_ozon_signal(warehouse)
            ]
            if not visible_warehouses:
                continue
            result.append(
                {
                    **cluster,
                    "warehouses": visible_warehouses,
                    "total_ozon_available": sum(int(warehouse.get("total_ozon_available") or 0) for warehouse in visible_warehouses),
                    "total_in_pipeline": sum(int(warehouse.get("total_in_pipeline") or 0) for warehouse in visible_warehouses),
                    "total_ordered_30d": sum(int(warehouse.get("total_ordered_30d") or 0) for warehouse in visible_warehouses),
                }
            )
        return result

    filtered_clusters: list[dict] = []
    for cluster in clusters:
        filtered_warehouses: list[dict] = []
        for warehouse in (cluster.get("warehouses") or []):
            if _looks_unknown_warehouse(warehouse) and not _warehouse_has_ozon_signal(warehouse):
                continue

            filtered_products: list[dict] = []
            for product in (warehouse.get("products") or []):
                product_name = str(product.get("product_name") or "")
                if selected_names and product_name not in selected_names:
                    continue

                if normalized_filter:
                    product_name_match = product_name.lower().find(normalized_filter) >= 0
                    offer_match = any(
                        normalized_filter in str(variant.get("offer_id") or "").lower()
                        for variant in (product.get("variants") or [])
                    )
                    if not product_name_match and not offer_match:
                        continue

                filtered_products.append(product)

            if not filtered_products:
                continue

            filtered_warehouses.append(
                {
                    **warehouse,
                    "products": filtered_products,
                    "total_ozon_available": sum(
                        int(variant.get("ozon_available") or 0)
                        for product in filtered_products
                        for variant in (product.get("variants") or [])
                    ),
                    "total_in_pipeline": sum(
                        int(variant.get("ozon_in_pipeline") or 0)
                        for product in filtered_products
                        for variant in (product.get("variants") or [])
                    ),
                    "total_ordered_30d": sum(
                        int(variant.get("ordered_30d") or 0)
                        for product in filtered_products
                        for variant in (product.get("variants") or [])
                    ),
                }
            )

        if not filtered_warehouses:
            continue

        filtered_clusters.append(
            {
                **cluster,
                "warehouses": filtered_warehouses,
                "total_ozon_available": sum(int(warehouse.get("total_ozon_available") or 0) for warehouse in filtered_warehouses),
                "total_in_pipeline": sum(int(warehouse.get("total_in_pipeline") or 0) for warehouse in filtered_warehouses),
                "total_ordered_30d": sum(int(warehouse.get("total_ordered_30d") or 0) for warehouse in filtered_warehouses),
            }
        )

    return filtered_clusters


async def _get_avg_delivery_days_cached(
    db: AsyncSession,
    store_id: int,
    warehouse_id: int,
    cache: dict[int, int],
) -> int:
    if warehouse_id not in cache:
        cache[warehouse_id] = await _avg_delivery_days(db, store_id, warehouse_id)
    return cache[warehouse_id]


def _build_variant_payload(
    *,
    product: Product,
    variant: Variant,
    settings,
    attrs_map: dict[int, dict[str, str]],
    stock_map: dict[int, WarehouseStock],
    group_stock_totals: dict[tuple[int, str, str], dict[str, int]],
    ozon_available: int,
    ozon_requested_to_supply: int,
    ozon_in_pipeline: int,
    ozon_returning: int,
    ordered_30d: int,
) -> dict:
    our_stock = stock_map.get(variant.id)
    pack_size = variant.pack_size or 1
    group_key = _stock_group_key(product.warehouse_product_id, variant.id, attrs_map.get(variant.id, {}))
    group_unpacked = group_stock_totals[group_key]["unpacked"]
    group_reserved = group_stock_totals[group_key]["reserved"]
    packed_units = 0 if settings.packing_mode == "simple" else (our_stock.packed_quantity * pack_size if our_stock else 0)
    available = group_unpacked + packed_units - group_reserved

    return {
        "variant_id": variant.id,
        "sku": variant.sku,
        "offer_id": variant.offer_id,
        "pack_size": pack_size,
        "attributes": attrs_map.get(variant.id, {}),
        "our_unpacked": group_unpacked,
        "our_packed": our_stock.packed_quantity if our_stock else 0,
        "our_reserved": group_reserved,
        "our_available": available,
        "ozon_available": ozon_available,
        "ozon_requested_to_supply": ozon_requested_to_supply,
        "ozon_in_pipeline": ozon_in_pipeline,
        "ozon_returning": ozon_returning,
        "ordered_30d": ordered_30d,
    }


def _build_supply_summary_payload(supply: Supply) -> dict:
    return {
        "id": supply.id,
        "order_number": supply.order_number,
        "status": supply.status,
        "timeslot_from": supply.timeslot_from.isoformat() if supply.timeslot_from else None,
        "timeslot_to": supply.timeslot_to.isoformat() if supply.timeslot_to else None,
        "eta_date": supply.eta_date.isoformat() if supply.eta_date else None,
    }


async def _repair_store_cluster_links(db: AsyncSession, store: Store) -> None:
    decrypted_key = decrypt_api_key(store.api_key_encrypted)
    client = OzonClient(store.client_id, decrypted_key, store.name)
    try:
        cluster_payloads = []
        cluster_payloads.extend(await client.get_clusters("CLUSTER_TYPE_OZON"))
        cluster_payloads.extend(await client.get_clusters("CLUSTER_TYPE_CIS"))
    except Exception:
        return
    finally:
        await client.close()

    if not cluster_payloads:
        return

    cluster_name_by_warehouse_ozon_id: dict[str, str] = {}
    for cluster_payload in cluster_payloads:
        cluster_name = cluster_payload.get("name")
        if not cluster_name:
            continue

        warehouse_payloads: list[dict] = []
        direct_warehouses = cluster_payload.get("warehouses") or []
        if isinstance(direct_warehouses, list):
            warehouse_payloads.extend(
                warehouse_payload
                for warehouse_payload in direct_warehouses
                if isinstance(warehouse_payload, dict)
            )

        logistic_clusters = cluster_payload.get("logistic_clusters") or []
        if isinstance(logistic_clusters, list):
            for logistic_cluster in logistic_clusters:
                if not isinstance(logistic_cluster, dict):
                    continue
                nested_warehouses = logistic_cluster.get("warehouses") or []
                if not isinstance(nested_warehouses, list):
                    continue
                warehouse_payloads.extend(
                    warehouse_payload
                    for warehouse_payload in nested_warehouses
                    if isinstance(warehouse_payload, dict)
                )

        for warehouse_payload in warehouse_payloads:
            warehouse_ozon_id = warehouse_payload.get("warehouse_id") or warehouse_payload.get("id")
            if warehouse_ozon_id is None:
                continue
            cluster_name_by_warehouse_ozon_id[str(warehouse_ozon_id)] = cluster_name

    if not cluster_name_by_warehouse_ozon_id:
        return

    clusters_result = await db.execute(select(Cluster))
    clusters_by_name = {cluster.name: cluster for cluster in clusters_result.scalars().all()}

    warehouses_result = await db.execute(select(OzonWarehouse))
    warehouses = warehouses_result.scalars().all()

    changed = False
    for warehouse in warehouses:
        cluster_name = cluster_name_by_warehouse_ozon_id.get(str(warehouse.ozon_id))
        if not cluster_name:
            continue

        cluster = clusters_by_name.get(cluster_name)
        if not cluster:
            cluster = Cluster(name=cluster_name)
            db.add(cluster)
            await db.flush()
            clusters_by_name[cluster_name] = cluster

        if warehouse.cluster_id != cluster.id:
            warehouse.cluster_id = cluster.id
            changed = True

    if changed:
        await db.flush()


def _ensure_cluster_bucket(
    clusters: dict,
    cluster_row: Optional[Cluster],
    warehouse_row: OzonWarehouse,
    avg_days: int,
):
    def cluster_key(cluster_value, warehouse_name: Optional[str]):
        if cluster_value and cluster_value.id:
            return f"cluster-{cluster_value.id}", cluster_value.name
        derived_name = _derive_cluster_name(warehouse_name)
        return f"derived-{derived_name}", derived_name

    c_key, c_name = cluster_key(cluster_row, warehouse_row.name)
    if c_key not in clusters:
        clusters[c_key] = {
            "key": c_key,
            "id": cluster_row.id if cluster_row else None,
            "name": c_name,
            "total_ozon_available": 0,
            "total_in_pipeline": 0,
            "total_ordered_30d": 0,
            "warehouses": {}
        }

    if warehouse_row.id not in clusters[c_key]["warehouses"]:
        clusters[c_key]["warehouses"][warehouse_row.id] = {
            "id": warehouse_row.id,
            "name": warehouse_row.name,
            "ozon_id": warehouse_row.ozon_id,
            "avg_delivery_days": avg_days,
            "total_ozon_available": 0,
            "total_in_pipeline": 0,
            "total_ordered_30d": 0,
            "pending_departure_supplies": [],
            "in_transit_supplies": [],
            "products": {}
        }

    return clusters[c_key], clusters[c_key]["warehouses"][warehouse_row.id]


@router.get("")
@router.get("/", include_in_schema=False)
async def get_shipments(
        store_id: Optional[int] = Query(None, description="ID магазина"),
        order_window_days: int = Query(30, description="Период заказов: 7, 30 или 90 дней"),
        product_filter: Optional[str] = Query(None, description="Фильтр по названию товара или offer_id"),
        selected_product_names: Optional[list[str]] = Query(None, description="Выбранные товары"),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Страница отправок: кластеры/склады OZON и остатки"""
    normalized_window_days = 90 if order_window_days >= 90 else (30 if order_window_days >= 30 else 7)
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    stores_stmt = select(Store).where(Store.user_id == cabinet_owner_id)
    if store_id:
        stores_stmt = stores_stmt.where(Store.id == store_id)
    stores_result = await db.execute(stores_stmt)
    stores = stores_result.scalars().all()

    if not stores:
        return {
            "clusters": [],
            "warehouse_mode": None,
            "packing_mode": None,
            "our_warehouse_name": None,
            "data_source": None,
            "data_note": None,
            "order_window_days": normalized_window_days,
            "available_order_windows": [7, 30],
        }

    if store_id is None:
        if len(stores) == 1:
            store_id = stores[0].id
        else:
            raise HTTPException(status_code=400, detail="store_id is required")

    selected_store = next((store for store in stores if store.id == store_id), None)
    if not selected_store:
        raise HTTPException(status_code=404, detail="Store not found")

    use_cache = not (str(product_filter or "").strip() or (selected_product_names or []))
    if use_cache:
        cached_response = await cache_get_json(
            shipments_response_cache_key(
                owner_user_id=cabinet_owner_id,
                store_id=selected_store.id,
                order_window_days=normalized_window_days,
            )
        )
        if isinstance(cached_response, dict):
            return cached_response

    await _repair_store_cluster_links(db, selected_store)
    postings_snapshot = await OzonReportSnapshotService.get_cached_snapshot_for_client(
        client_id=selected_store.client_id,
        kind="postings",
    )
    postings_filters = (postings_snapshot or {}).get("filters") or {}
    analytics = ((postings_snapshot or {}).get("analytics") or {}).get("shipment_orders") or {}
    has_daily_warehouse_orders = bool(analytics.get("daily_warehouse_offer_units"))
    orders_data_note = None
    if not postings_snapshot:
        orders_data_note = "Отчет заказов Ozon еще не готов, поэтому колонка заказов временно показывает 0."
    elif not has_daily_warehouse_orders:
        orders_data_note = "Для поскладовых заказов за неделю, месяц и 3 месяца нужно один раз обновить синхронизацию «Отчёты»."
    snapshot_start_raw = postings_filters.get("processed_at_from")
    snapshot_start = None
    if snapshot_start_raw:
        try:
            snapshot_start = datetime.fromisoformat(str(snapshot_start_raw).replace("Z", "+00:00")).astimezone(timezone.utc).date()
        except ValueError:
            snapshot_start = None
    today = datetime.now(timezone.utc).date()
    order_window_start = today - timedelta(days=max(normalized_window_days - 1, 0))
    available_order_windows = [7, 30]
    if snapshot_start and (today - snapshot_start).days >= 89:
        available_order_windows.append(90)

    # Определяем наш склад
    warehouse, settings = await resolve_warehouse(
        db,
        user_id=cabinet_owner_id,
        store_id=store_id,
        warehouse_id=None
    )

    # Загружаем вариации магазина
    variants_stmt = select(Variant, Product).join(Product, Product.id == Variant.product_id).where(
        Product.store_id == store_id
    )
    variants_result = await db.execute(variants_stmt)
    variants = variants_result.all()
    variant_ids = [v.Variant.id for v in variants]
    product_ids = [v.Product.id for v in variants]
    warehouse_product_ids = {
        v.Product.warehouse_product_id
        for v in variants
        if v.Product.warehouse_product_id is not None
    }

    if not variant_ids:
        return {
            "clusters": [],
            "warehouse_mode": settings.warehouse_mode,
            "packing_mode": settings.packing_mode,
            "our_warehouse_name": warehouse.name,
            "data_source": None,
            "data_note": None,
            "order_window_days": normalized_window_days,
            "available_order_windows": available_order_windows,
        }

    pipeline_statuses = [
        "DATA_FILLING",
        "READY_TO_SUPPLY",
        "ACCEPTED_AT_SUPPLY_WAREHOUSE",
        "IN_TRANSIT",
        "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
        "REPORTS_CONFIRMATION_AWAITING",
    ]
    pending_departure_statuses = {
        "DATA_FILLING",
        "READY_TO_SUPPLY",
        "ACCEPTED_AT_SUPPLY_WAREHOUSE",
    }
    in_transit_modal_statuses = {
        "IN_TRANSIT",
    }

    pipeline_stmt = select(
        SupplyItem.variant_id,
        Supply.storage_warehouse_id,
        SupplyItem.quantity,
    ).join(
        Supply, Supply.id == SupplyItem.supply_id
    ).where(
        Supply.store_id == store_id,
        Supply.storage_warehouse_id.is_not(None),
        Supply.status.in_(pipeline_statuses),
        SupplyItem.variant_id.in_(variant_ids),
    )
    pipeline_result = await db.execute(pipeline_stmt)
    pipeline_map = defaultdict(int)
    for variant_id, storage_warehouse_id, quantity in pipeline_result.all():
        pipeline_map[(variant_id, storage_warehouse_id)] += quantity or 0

    supplies_stmt = (
        select(Supply, OzonWarehouse, Cluster)
        .outerjoin(OzonWarehouse, OzonWarehouse.id == Supply.storage_warehouse_id)
        .outerjoin(Cluster, Cluster.id == OzonWarehouse.cluster_id)
        .where(
            Supply.store_id == store_id,
            Supply.storage_warehouse_id.is_not(None),
            Supply.status.in_(pipeline_statuses),
        )
        .order_by(Supply.timeslot_from.asc().nullslast(), Supply.created_at.asc())
    )
    supplies_result = await db.execute(supplies_stmt)
    active_supply_rows = supplies_result.all()

    # Характеристики вариаций
    attrs_map = {}
    attrs_stmt = select(VariantAttribute).where(VariantAttribute.variant_id.in_(variant_ids))
    attrs_result = await db.execute(attrs_stmt)
    for attr in attrs_result.scalars().all():
        attrs_map.setdefault(attr.variant_id, {})[attr.name] = attr.value

    all_group_variants_stmt = select(Variant, Product).join(Product, Product.id == Variant.product_id)
    if warehouse_product_ids:
        all_group_variants_stmt = all_group_variants_stmt.where(
            Product.warehouse_product_id.in_(warehouse_product_ids)
        )
    else:
        all_group_variants_stmt = all_group_variants_stmt.where(Product.id.in_(product_ids))
    all_group_variants_result = await db.execute(all_group_variants_stmt)
    all_group_variants = all_group_variants_result.all()
    all_group_variant_ids = [row.Variant.id for row in all_group_variants]

    all_attrs_map: dict[int, dict[str, str]] = {}
    if all_group_variant_ids:
        all_attrs_stmt = select(VariantAttribute).where(VariantAttribute.variant_id.in_(all_group_variant_ids))
        all_attrs_result = await db.execute(all_attrs_stmt)
        for attr in all_attrs_result.scalars().all():
            all_attrs_map.setdefault(attr.variant_id, {})[attr.name] = attr.value

    # Наши остатки
    stock_stmt = select(WarehouseStock).where(
        WarehouseStock.warehouse_id == warehouse.id,
        WarehouseStock.variant_id.in_(all_group_variant_ids or variant_ids)
    )
    stock_result = await db.execute(stock_stmt)
    stock_map = {s.variant_id: s for s in stock_result.scalars().all()}
    group_stock_totals: dict[tuple[int, str, str], dict[str, int]] = defaultdict(
        lambda: {"unpacked": 0, "reserved": 0}
    )
    product_by_variant_id = {row.Variant.id: row.Product for row in all_group_variants}
    for current_variant_id, stock in stock_map.items():
        current_product = product_by_variant_id.get(current_variant_id)
        current_attrs = all_attrs_map.get(current_variant_id, {})
        group_key = _stock_group_key(
            current_product.warehouse_product_id if current_product else None,
            current_variant_id,
            current_attrs,
        )
        group_stock_totals[group_key]["unpacked"] += stock.unpacked_quantity
        group_stock_totals[group_key]["reserved"] += stock.reserved_quantity

    # Остатки OZON
    ozon_stmt = select(
        OzonStock,
        OzonWarehouse,
        Cluster,
        Variant,
        Product
    ).join(
        OzonWarehouse, OzonWarehouse.id == OzonStock.warehouse_id
    ).outerjoin(
        Cluster, Cluster.id == OzonWarehouse.cluster_id
    ).join(
        Variant, Variant.id == OzonStock.variant_id
    ).join(
        Product, Product.id == Variant.product_id
    ).where(
        OzonStock.variant_id.in_(variant_ids)
    )

    ozon_result = await db.execute(ozon_stmt)
    rows = ozon_result.all()

    clusters = {}
    data_source = "ozon_stocks"
    data_note: Optional[str] = None
    variant_product_map = {row.Variant.id: (row.Variant, row.Product) for row in variants}
    avg_delivery_days_cache: dict[int, int] = {}

    for row in rows:
        ozon_stock = row.OzonStock
        wh = row.OzonWarehouse
        cluster = row.Cluster
        variant = row.Variant
        product = row.Product

        avg_days = await _get_avg_delivery_days_cached(db, store_id, wh.id, avg_delivery_days_cache)
        cluster_bucket, warehouse_bucket = _ensure_cluster_bucket(clusters, cluster, wh, avg_days)
        product_bucket = _ensure_product_bucket(warehouse_bucket, store_id, product)
        in_pipeline = ozon_stock.in_transit
        ordered_30d = OzonReportSnapshotService.get_warehouse_postings_offer_window_units(
            postings_snapshot,
            warehouse_name=wh.name,
            offer_id=variant.offer_id,
            start=order_window_start,
            end=today,
        )
        product_bucket["variants"].append(
            _build_variant_payload(
                product=product,
                variant=variant,
                settings=settings,
                attrs_map=attrs_map,
                stock_map=stock_map,
                group_stock_totals=group_stock_totals,
                ozon_available=ozon_stock.available_to_sell,
                ozon_requested_to_supply=ozon_stock.requested_to_supply,
                ozon_in_pipeline=in_pipeline,
                ozon_returning=ozon_stock.returning,
                ordered_30d=ordered_30d,
            )
        )
        warehouse_bucket["total_ozon_available"] += ozon_stock.available_to_sell
        warehouse_bucket["total_in_pipeline"] += in_pipeline
        warehouse_bucket["total_ordered_30d"] += ordered_30d
        cluster_bucket["total_ozon_available"] += ozon_stock.available_to_sell
        cluster_bucket["total_in_pipeline"] += in_pipeline
        cluster_bucket["total_ordered_30d"] += ordered_30d

    if not rows and pipeline_map:
        data_source = "pipeline_only"
        data_note = "Показываем активные поставки и остатки нашего склада. Аналитика остатков Ozon еще не успела прогреться."

        needed_warehouse_ids = sorted({
            warehouse_id
            for (_, warehouse_id), quantity in pipeline_map.items()
            if warehouse_id and quantity > 0
        })

        if needed_warehouse_ids:
            warehouse_rows = await db.execute(
                select(OzonWarehouse, Cluster)
                .outerjoin(Cluster, Cluster.id == OzonWarehouse.cluster_id)
                .where(OzonWarehouse.id.in_(needed_warehouse_ids))
            )
            warehouse_map = {
                warehouse.id: (warehouse, cluster)
                for warehouse, cluster in warehouse_rows.all()
            }

            for (variant_id, warehouse_id), in_pipeline in pipeline_map.items():
                if in_pipeline <= 0:
                    continue

                variant_product = variant_product_map.get(variant_id)
                warehouse_cluster = warehouse_map.get(warehouse_id)
                if not variant_product or not warehouse_cluster:
                    continue

                variant, product = variant_product
                wh, cluster = warehouse_cluster
                avg_days = await _get_avg_delivery_days_cached(db, store_id, wh.id, avg_delivery_days_cache)
                cluster_bucket, warehouse_bucket = _ensure_cluster_bucket(clusters, cluster, wh, avg_days)
                product_bucket = _ensure_product_bucket(warehouse_bucket, store_id, product)
                ordered_30d = OzonReportSnapshotService.get_warehouse_postings_offer_window_units(
                    postings_snapshot,
                    warehouse_name=wh.name,
                    offer_id=variant.offer_id,
                    start=order_window_start,
                    end=today,
                )
                product_bucket["variants"].append(
                    _build_variant_payload(
                        product=product,
                        variant=variant,
                        settings=settings,
                        attrs_map=attrs_map,
                        stock_map=stock_map,
                        group_stock_totals=group_stock_totals,
                        ozon_available=0,
                        ozon_requested_to_supply=0,
                        ozon_in_pipeline=in_pipeline,
                        ozon_returning=0,
                        ordered_30d=ordered_30d,
                    )
                )
                warehouse_bucket["total_in_pipeline"] += in_pipeline
                warehouse_bucket["total_ordered_30d"] += ordered_30d
                cluster_bucket["total_in_pipeline"] += in_pipeline
                cluster_bucket["total_ordered_30d"] += ordered_30d

    for supply, supply_warehouse, supply_cluster in active_supply_rows:
        if not supply_warehouse:
            continue

        avg_days = await _get_avg_delivery_days_cached(db, store_id, supply_warehouse.id, avg_delivery_days_cache)
        _, warehouse_bucket = _ensure_cluster_bucket(clusters, supply_cluster, supply_warehouse, avg_days)
        if supply.status in pending_departure_statuses:
            target_key = "pending_departure_supplies"
        elif supply.status in in_transit_modal_statuses:
            target_key = "in_transit_supplies"
        else:
            continue
        warehouse_bucket[target_key].append(_build_supply_summary_payload(supply))

    # Преобразуем в списки
    clusters_list = []
    for cluster in clusters.values():
        warehouses_list = []
        for wh in cluster["warehouses"].values():
            products_list = list(wh["products"].values())
            warehouses_list.append({
                **wh,
                "products": products_list
            })
        warehouses_list.sort(
            key=lambda warehouse: warehouse["total_ozon_available"] + warehouse["total_in_pipeline"],
            reverse=True,
        )
        clusters_list.append({
            "key": cluster["key"],
            "id": cluster["id"],
            "name": cluster["name"],
            "total_ozon_available": cluster["total_ozon_available"],
            "total_in_pipeline": cluster["total_in_pipeline"],
            "total_ordered_30d": cluster["total_ordered_30d"],
            "warehouses": warehouses_list
        })

    clusters_list.sort(key=lambda item: item["total_ozon_available"], reverse=True)

    filtered_clusters = _filter_shipments_clusters(
        clusters_list,
        product_filter=product_filter,
        selected_product_names=selected_product_names,
    )

    response = {
        "clusters": filtered_clusters,
        "warehouse_mode": settings.warehouse_mode,
        "packing_mode": settings.packing_mode,
        "our_warehouse_name": warehouse.name,
        "data_source": data_source,
        "data_note": data_note,
        "orders_data_note": orders_data_note,
        "orders_period_from": postings_filters.get("processed_at_from"),
        "orders_period_to": postings_filters.get("processed_at_to"),
        "orders_updated_at": (postings_snapshot or {}).get("refreshed_at"),
        "order_window_days": normalized_window_days,
        "available_order_windows": available_order_windows,
    }

    if use_cache:
        await cache_set_json(
            shipments_response_cache_key(
                owner_user_id=cabinet_owner_id,
                store_id=selected_store.id,
                order_window_days=normalized_window_days,
            ),
            response,
            SHIPMENTS_RESPONSE_CACHE_TTL_SECONDS,
        )
    return response


@router.post("/export")
async def start_shipments_export(
        store_id: int = Query(..., description="ID магазина"),
        order_window_days: int = Query(30, description="Период заказов: 7, 30 или 90 дней"),
        product_filter: str | None = Query(None, description="Фильтр по названию товара или offer_id"),
        selected_product_names: list[str] | None = Query(None, description="Выбранные товары"),
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
    current_status = get_export_status("shipments", store_id, current_user.id)
    if has_export_lock("shipments", store_id) or current_status.get("status") in {"queued", "running"}:
        return {
            **current_status,
            "status": current_status.get("status") if current_status.get("status") in {"queued", "running"} else "running",
            "phase": current_status.get("phase") or "queued",
            "phase_label": current_status.get("phase_label") or "Уже формируется",
            "progress_percent": current_status.get("progress_percent") or 5,
            "message": current_status.get("message") or "Excel по отправкам уже формируется для этого магазина",
            "duplicate_request": True,
        }

    task = celery_app.send_task(
        "worker.tasks.export_shipments_excel_task",
        args=[store_id, current_user.id, normalized_window_days, product_filter, selected_product_names or []],
    )
    return mark_export_queued(
        "shipments",
        store_id,
        current_user.id,
        task_id=str(task.id),
        message="Excel по отправкам поставлен в очередь",
        order_window_days=normalized_window_days,
    )


@router.get("/export/status")
async def get_shipments_export_status(
        store_id: int = Query(..., description="ID магазина"),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    store_result = await db.execute(
        select(Store.id).where(Store.user_id == cabinet_owner_id, Store.id == store_id)
    )
    if store_result.scalar_one_or_none() is None:
        raise HTTPException(404, "Магазин не найден")
    return get_export_status("shipments", store_id, current_user.id)


@router.get("/export/download")
async def download_shipments_export(
        store_id: int = Query(..., description="ID магазина"),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    store_result = await db.execute(
        select(Store.id).where(Store.user_id == cabinet_owner_id, Store.id == store_id)
    )
    if store_result.scalar_one_or_none() is None:
        raise HTTPException(404, "Магазин не найден")

    status = get_export_status("shipments", store_id, current_user.id)
    file_path = str(status.get("file_path") or "")
    file_name = str(status.get("file_name") or "shipments.xlsx")
    if status.get("status") != "success" or not file_path:
        raise HTTPException(404, "Готовый Excel пока не найден")
    if not os.path.exists(file_path):
        raise HTTPException(404, "Файл выгрузки устарел или недоступен. Сформируй Excel заново.")
    return FileResponse(path=file_path, filename=file_name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@router.delete("/export")
async def clear_shipments_export(
        store_id: int = Query(..., description="ID магазина"),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    store_result = await db.execute(
        select(Store.id).where(Store.user_id == cabinet_owner_id, Store.id == store_id)
    )
    if store_result.scalar_one_or_none() is None:
        raise HTTPException(404, "Магазин не найден")
    if has_export_lock("shipments", store_id):
        raise HTTPException(409, "Нельзя очистить отчет, пока он формируется")
    return clear_export_status("shipments", store_id, current_user.id)
