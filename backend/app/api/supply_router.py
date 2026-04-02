# backend/app/api/supply_router.py
from datetime import timedelta
from statistics import median

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.database import SessionLocal
from app.models.ozon_warehouse import OzonWarehouse
from app.models.supply import Supply, SupplyItem
from app.models.variant import Variant
from app.models.variant_attribute import VariantAttribute
from app.models.product import Product
from app.models.store import Store
from app.models.user import User
from app.core.dependencies import get_current_user
from app.services.cabinet_access import get_cabinet_owner_id
from app.services.product_grouping import extract_base_product_name
from app.services.supply_reservation_wait import get_supply_reservation_wait_map

router = APIRouter(prefix="/supplies", tags=["supplies"])


async def get_db():
    async with SessionLocal() as session:
        yield session


async def _build_variant_attrs_map(db: AsyncSession, variant_ids: list[int]) -> dict[int, dict[str, str]]:
    if not variant_ids:
        return {}

    stmt = select(VariantAttribute).where(VariantAttribute.variant_id.in_(variant_ids))
    result = await db.execute(stmt)

    attrs_map: dict[int, dict[str, str]] = {}
    for attr in result.scalars().all():
        attrs_map.setdefault(attr.variant_id, {})[attr.name] = attr.value

    return attrs_map


def _serialize_supply_item_row(row, attrs_map: dict[int, dict[str, str]]) -> dict:
    return {
        "supply_id": row.SupplyItem.supply_id,
        "variant_id": row.SupplyItem.variant_id,
        "sku": row.Variant.sku,
        "offer_id": row.Variant.offer_id,
        "product_name": row.Product.base_name or extract_base_product_name(row.Product.name),
        "pack_size": row.Variant.pack_size,
        "attributes": attrs_map.get(row.SupplyItem.variant_id, {}),
        "quantity": row.SupplyItem.quantity,
        "accepted_quantity": row.SupplyItem.accepted_quantity,
    }


async def _load_supply_items_map(db: AsyncSession, supply_ids: list[int]) -> dict[int, list[dict]]:
    if not supply_ids:
        return {}

    stmt = select(
        SupplyItem,
        Variant,
        Product,
    ).join(
        Variant, Variant.id == SupplyItem.variant_id
    ).join(
        Product, Product.id == Variant.product_id
    ).where(
        SupplyItem.supply_id.in_(supply_ids)
    )

    result = await db.execute(stmt)
    rows = result.all()
    attrs_map = await _build_variant_attrs_map(
        db,
        [row.SupplyItem.variant_id for row in rows]
    )

    items_map: dict[int, list[dict]] = {}
    for row in rows:
        items_map.setdefault(row.SupplyItem.supply_id, []).append(
            _serialize_supply_item_row(row, attrs_map)
        )

    return items_map


async def _get_owned_supply(db: AsyncSession, supply_id: int, user_id: int) -> Supply:
    supply = await db.get(Supply, supply_id)
    if not supply:
        raise HTTPException(status_code=404, detail="Supply not found")

    store = await db.get(Store, supply.store_id)
    if not store or store.user_id != user_id:
        raise HTTPException(403, "Not enough permissions")

    return supply


async def _avg_delivery_days(db: AsyncSession, store_id: int, storage_warehouse_id: int | None) -> int:
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

    deltas: list[int] = []
    for supply in supplies:
        try:
            arrival_at = supply.acceptance_at_storage_at or supply.completed_at
            if arrival_at is None:
                continue
            delta = (arrival_at.date() - supply.timeslot_from.date()).days
            deltas.append(max(0, delta))
        except Exception:
            continue

    if not deltas:
        return 2

    return max(1, round(median(deltas)))


def _resolve_eta_iso(supply: Supply, avg_days: int | None = None) -> str | None:
    if supply.eta_date:
        return supply.eta_date.isoformat()
    if supply.timeslot_from and avg_days:
        return (supply.timeslot_from.date() + timedelta(days=avg_days)).isoformat()
    return None


@router.get("")
@router.get("/", include_in_schema=False)
async def get_supplies(
        status: list[str] | None = Query(None),
        page: int = 1,
        page_size: int = 20,
        include_items: bool = False,
        store_id: int | None = None,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Получить список поставок пользователя"""
    cabinet_owner_id = get_cabinet_owner_id(current_user)

    if page < 1:
        page = 1
    if page_size < 1 or page_size > 100:
        page_size = 20

    # Получаем все магазины пользователя
    stores_stmt = select(Store.id).where(Store.user_id == cabinet_owner_id)
    if store_id:
        stores_stmt = stores_stmt.where(Store.id == store_id)
    stores_result = await db.execute(stores_stmt)
    store_ids = stores_result.scalars().all()

    if not store_ids:
        return {"items": [], "total": 0, "page": page, "page_size": page_size}

    # Получаем поставки для этих магазинов
    stmt = select(Supply).where(Supply.store_id.in_(store_ids))
    if status:
        stmt = stmt.where(Supply.status.in_(status))
    stmt = stmt.order_by(Supply.created_at.desc())

    total_stmt = select(func.count(Supply.id)).where(Supply.store_id.in_(store_ids))
    if status:
        total_stmt = total_stmt.where(Supply.status.in_(status))
    total = await db.scalar(total_stmt)
    stmt = stmt.limit(page_size).offset((page - 1) * page_size)

    result = await db.execute(stmt)
    supplies = result.scalars().all()

    store_map = {}
    if store_ids:
        stores_full_stmt = select(Store).where(Store.id.in_(store_ids))
        stores_full_result = await db.execute(stores_full_stmt)
        store_map = {store.id: store for store in stores_full_result.scalars().all()}

    warehouse_ids = {
        warehouse_id
        for supply in supplies
        for warehouse_id in (supply.dropoff_warehouse_id, supply.storage_warehouse_id)
        if warehouse_id
    }
    warehouse_map = {}
    if warehouse_ids:
        warehouses_stmt = select(OzonWarehouse).where(OzonWarehouse.id.in_(warehouse_ids))
        warehouses_result = await db.execute(warehouses_stmt)
        warehouse_map = {warehouse.id: warehouse for warehouse in warehouses_result.scalars().all()}

    supply_items_map = await _load_supply_items_map(db, [supply.id for supply in supplies]) if include_items else {}
    wait_map = await get_supply_reservation_wait_map([supply.id for supply in supplies])

    items = []
    avg_cache: dict[tuple[int, int | None], int] = {}
    for s in supplies:
        store = store_map.get(s.store_id)
        avg_key = (s.store_id, s.storage_warehouse_id)
        if avg_key not in avg_cache:
            avg_cache[avg_key] = await _avg_delivery_days(db, s.store_id, s.storage_warehouse_id)
        payload = {
            "id": s.id,
            "order_number": s.order_number,
            "status": s.status,
            "reservation_waiting_for_stock": s.id in wait_map,
            "reservation_wait_message": wait_map.get(s.id, {}).get("message"),
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "timeslot_from": s.timeslot_from.isoformat() if s.timeslot_from else None,
            "timeslot_to": s.timeslot_to.isoformat() if s.timeslot_to else None,
            "completed_at": s.completed_at.isoformat() if s.completed_at else None,
            "eta_date": _resolve_eta_iso(s, avg_cache.get(avg_key)),
            "store_id": s.store_id,
            "store_name": store.name if store else None,
            "dropoff_warehouse_name": warehouse_map.get(s.dropoff_warehouse_id).name if s.dropoff_warehouse_id in warehouse_map else None,
            "storage_warehouse_name": warehouse_map.get(s.storage_warehouse_id).name if s.storage_warehouse_id in warehouse_map else None,
        }

        if include_items:
            payload["items"] = supply_items_map.get(s.id, [])

        items.append(payload)

    return {
        "items": items,
        "total": total or 0,
        "page": page,
        "page_size": page_size
    }


@router.get("/{supply_id}")
async def get_supply(
        supply_id: int,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Получить детали поставки"""
    supply = await _get_owned_supply(db, supply_id, get_cabinet_owner_id(current_user))

    return {
        "id": supply.id,
        "order_number": supply.order_number,
        "status": supply.status,
        "reservation_waiting_for_stock": False,
        "reservation_wait_message": None,
        "timeslot_from": supply.timeslot_from.isoformat() if supply.timeslot_from else None,
        "timeslot_to": supply.timeslot_to.isoformat() if supply.timeslot_to else None,
        "created_at": supply.created_at.isoformat() if supply.created_at else None
    }


@router.get("/{supply_id}/items")
async def get_supply_items(
        supply_id: int,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Получить товары в поставке"""
    await _get_owned_supply(db, supply_id, get_cabinet_owner_id(current_user))
    supply_items_map = await _load_supply_items_map(db, [supply_id])
    return supply_items_map.get(supply_id, [])
