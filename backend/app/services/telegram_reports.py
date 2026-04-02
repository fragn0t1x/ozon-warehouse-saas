from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.product import Product
from app.models.store import Store
from app.models.supply import Supply, SupplyItem
from app.models.variant import Variant


async def get_products_grouped_by_variants(db: AsyncSession, supply_id: int):
    items_stmt = select(SupplyItem).where(SupplyItem.supply_id == supply_id)
    items_result = await db.execute(items_stmt)
    items = items_result.scalars().all()

    products_dict = defaultdict(list)

    for item in items:
        variant = await db.get(Variant, item.variant_id)
        if not variant:
            continue
        product = await db.get(Product, variant.product_id)
        if not product:
            continue

        variant_info = {
            "variant_id": variant.id,
            "sku": variant.sku,
            "offer_id": variant.offer_id,
            "pack_size": variant.pack_size,
            "quantity": item.quantity,
            "accepted_quantity": item.accepted_quantity,
            "attributes": " / ".join(
                attr.value
                for attr in variant.attributes
                if attr.name.lower() in ["цвет", "color", "размер", "size"]
            ),
            "color": next((attr.value for attr in variant.attributes if attr.name.lower() in ["цвет", "color"]), ""),
            "size": next((attr.value for attr in variant.attributes if attr.name.lower() in ["размер", "size"]), ""),
        }

        products_dict[product.id].append(
            {
                "product_id": product.id,
                "product_name": product.name,
                "variant": variant_info,
            }
        )

    return products_dict


async def serialize_supplies_for_notification(
    db: AsyncSession,
    rows: list[tuple[Supply, Store]],
) -> list[dict]:
    supplies_list = []
    for supply, store in rows:
        products_dict = await get_products_grouped_by_variants(db, supply.id)
        supplies_list.append(
            {
                "id": supply.id,
                "order_number": supply.order_number,
                "status": supply.status,
                "status_ru": _supply_status_ru(supply.status),
                "store_name": store.name if store else "Неизвестный магазин",
                "timeslot_from": supply.timeslot_from,
                "timeslot_to": supply.timeslot_to,
                "products": products_dict,
                "total_items": sum(len(variants) for variants in products_dict.values()),
                "total_quantity": sum(
                    item["variant"]["quantity"]
                    for variants in products_dict.values()
                    for item in variants
                ),
            }
        )

    return supplies_list


async def build_user_today_supplies(
    db: AsyncSession,
    *,
    cabinet_owner_id: int,
    target_date: date,
) -> list[dict]:
    next_day = target_date + timedelta(days=1)
    stmt = (
        select(Supply, Store)
        .join(Store, Store.id == Supply.store_id)
        .where(
            Store.user_id == cabinet_owner_id,
            Supply.timeslot_from >= target_date,
            Supply.timeslot_from < next_day,
        )
        .order_by(Supply.timeslot_from)
    )
    result = await db.execute(stmt)
    return await serialize_supplies_for_notification(db, result.all())


async def build_user_next_supplies(
    db: AsyncSession,
    *,
    cabinet_owner_id: int,
    from_date: date,
) -> tuple[list[dict], date | None]:
    stmt = (
        select(Supply, Store)
        .join(Store, Store.id == Supply.store_id)
        .where(
            Store.user_id == cabinet_owner_id,
            Supply.timeslot_from >= from_date,
        )
        .order_by(Supply.timeslot_from)
    )
    result = await db.execute(stmt)
    rows = result.all()

    if not rows:
        return [], None

    first_supply, _ = rows[0]
    if not first_supply.timeslot_from:
        return [], None

    nearest_date = first_supply.timeslot_from.date()
    nearest_rows = [
        (supply, store)
        for supply, store in rows
        if supply.timeslot_from and supply.timeslot_from.date() == nearest_date
    ]
    return await serialize_supplies_for_notification(db, nearest_rows), nearest_date


def _supply_status_ru(status: str) -> str:
    return {
        "READY_TO_SUPPLY": "Готова к отгрузке",
        "ACCEPTED_AT_SUPPLY_WAREHOUSE": "Принята на точке отгрузки",
        "IN_TRANSIT": "В пути",
        "COMPLETED": "Завершена",
        "CANCELLED": "Отменена",
        "REJECTED_AT_SUPPLY_WAREHOUSE": "Отказано в приемке",
        "ACCEPTANCE_AT_STORAGE_WAREHOUSE": "На приемке на складе OZON",
        "REPORTS_CONFIRMATION_AWAITING": "Ожидает подтверждения актов",
        "REPORT_REJECTED": "Акт приемки отклонен",
        "OVERDUE": "Просрочена",
        "DATA_FILLING": "Подготовка к поставкам",
    }.get(status, status)
