from collections import defaultdict
from datetime import date, datetime, timedelta
from statistics import median
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_current_user
from app.database import SessionLocal
from app.models.ozon_warehouse import OzonWarehouse
from app.models.product import Product
from app.models.store import Store
from app.models.supply import Supply, SupplyItem
from app.models.user import User
from app.models.variant import Variant
from app.models.variant_attribute import VariantAttribute
from app.services.cabinet_access import get_cabinet_owner_id
from app.services.supply_reservation_wait import get_supply_reservation_wait_map

router = APIRouter(prefix="/calendar", tags=["calendar"])


async def get_db():
    async with SessionLocal() as session:
        yield session


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


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
    for s in supplies:
        try:
            arrival_at = s.acceptance_at_storage_at or s.completed_at
            if arrival_at is None:
                continue
            delta = (arrival_at.date() - s.timeslot_from.date()).days
            if delta < 0:
                delta = 0
            deltas.append(delta)
        except Exception:
            continue

    if not deltas:
        return 2

    return max(1, round(median(deltas)))


@router.get("")
@router.get("/", include_in_schema=False)
async def get_calendar(
        store_id: Optional[int] = Query(None, description="ID магазина"),
        status: list[str] | None = Query(None, description="Повторяемый фильтр статусов"),
        date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
        date_to: Optional[str] = Query(None, description="YYYY-MM-DD"),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Календарь поставок с ETA и товарами внутри заявок"""
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    stores_stmt = select(Store.id).where(Store.user_id == cabinet_owner_id)
    if store_id:
        stores_stmt = stores_stmt.where(Store.id == store_id)
    stores_result = await db.execute(stores_stmt)
    store_ids = stores_result.scalars().all()

    if not store_ids:
        return {"items": []}

    from_date = _parse_date(date_from) or (datetime.now().date() - timedelta(days=30))
    to_date = _parse_date(date_to) or (datetime.now().date() + timedelta(days=60))

    stmt = select(Supply).where(
        Supply.store_id.in_(store_ids),
        Supply.timeslot_from.is_not(None),
        Supply.timeslot_from >= datetime.combine(from_date, datetime.min.time()),
        Supply.timeslot_from <= datetime.combine(to_date, datetime.max.time())
    ).order_by(Supply.timeslot_from.asc())
    if status:
        stmt = stmt.where(Supply.status.in_(status))

    result = await db.execute(stmt)
    supplies = result.scalars().all()

    store_map = {}
    warehouse_ids = {supply.storage_warehouse_id for supply in supplies if supply.storage_warehouse_id}
    supply_ids = [supply.id for supply in supplies]

    if store_ids:
        stores_stmt = select(Store).where(Store.id.in_(store_ids))
        stores_result = await db.execute(stores_stmt)
        store_map = {store.id: store for store in stores_result.scalars().all()}

    warehouse_map = {}
    if warehouse_ids:
        warehouses_stmt = select(OzonWarehouse).where(OzonWarehouse.id.in_(warehouse_ids))
        warehouses_result = await db.execute(warehouses_stmt)
        warehouse_map = {warehouse.id: warehouse for warehouse in warehouses_result.scalars().all()}

    supply_items_map: dict[int, list[dict]] = defaultdict(list)
    if supply_ids:
        items_stmt = (
            select(SupplyItem, Variant, Product)
            .join(Variant, Variant.id == SupplyItem.variant_id)
            .join(Product, Product.id == Variant.product_id)
            .where(SupplyItem.supply_id.in_(supply_ids))
            .order_by(Product.name.asc(), Variant.offer_id.asc(), SupplyItem.id.asc())
        )
        items_result = await db.execute(items_stmt)
        supply_item_rows = items_result.all()

        variant_ids = [row.SupplyItem.variant_id for row in supply_item_rows]
        attrs_map: dict[int, dict[str, str]] = defaultdict(dict)
        if variant_ids:
            attrs_result = await db.execute(
                select(VariantAttribute).where(VariantAttribute.variant_id.in_(variant_ids))
            )
            for attr in attrs_result.scalars().all():
                attrs_map[attr.variant_id][attr.name] = attr.value

        for row in supply_item_rows:
            supply_items_map[row.SupplyItem.supply_id].append({
                "variant_id": row.SupplyItem.variant_id,
                "sku": row.Variant.sku,
                "offer_id": row.Variant.offer_id,
                "product_name": row.Product.base_name or row.Product.name,
                "pack_size": row.Variant.pack_size,
                "attributes": attrs_map.get(row.SupplyItem.variant_id, {}),
                "quantity": row.SupplyItem.quantity,
                "accepted_quantity": row.SupplyItem.accepted_quantity,
            })

    items = []
    wait_map = await get_supply_reservation_wait_map([supply.id for supply in supplies])
    for supply in supplies:
        avg_days = await _avg_delivery_days(db, supply.store_id, supply.storage_warehouse_id)
        eta_date = (supply.timeslot_from.date() + timedelta(days=avg_days)) if supply.timeslot_from else None
        store = store_map.get(supply.store_id)
        storage_warehouse = warehouse_map.get(supply.storage_warehouse_id) if supply.storage_warehouse_id else None

        items.append({
            "id": supply.id,
            "order_number": supply.order_number,
            "status": supply.status,
            "reservation_waiting_for_stock": supply.id in wait_map,
            "reservation_wait_message": wait_map.get(supply.id, {}).get("message"),
            "created_at": supply.created_at.isoformat() if supply.created_at else None,
            "timeslot_from": supply.timeslot_from.isoformat() if supply.timeslot_from else None,
            "timeslot_to": supply.timeslot_to.isoformat() if supply.timeslot_to else None,
            "eta_date": eta_date.isoformat() if eta_date else None,
            "avg_delivery_days": avg_days,
            "store_id": supply.store_id,
            "store_name": store.name if store else None,
            "storage_warehouse_name": storage_warehouse.name if storage_warehouse else None,
            "items": supply_items_map.get(supply.id, []),
        })

    return {"items": items}
