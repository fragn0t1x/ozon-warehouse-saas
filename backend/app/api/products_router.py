# backend/app/api/products_router.py
from datetime import date, datetime, timezone
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete, select
from sqlalchemy.orm import selectinload
from app.database import SessionLocal
from app.models.product import Product
from app.models.variant import Variant
from app.models.variant_cost_history import VariantCostHistory
from app.models.store import Store
from app.models.user import User
from app.core.dependencies import get_current_user
from app.services.cabinet_access import get_cabinet_owner_id
from app.services.closed_months_recalc_queue import ClosedMonthsRecalcQueue
from app.services.export_status import mark_store_exports_stale
from app.services.product_grouping import extract_base_product_name, get_size_order, normalize_color
from app.services.economics_history_service import EconomicsHistoryService
from app.schemas.economics_history import VariantCostHistoryEntryResponse
from typing import List, Optional
from collections import defaultdict

router = APIRouter(prefix="/products", tags=["products"])


def _closed_months_start_month(effective_from: date | None) -> str:
    value = effective_from or datetime.now(timezone.utc).date()
    return f"{value.year:04d}-{value.month:02d}"


async def _earliest_store_month_needing_cost(db: AsyncSession, store_id: int) -> date | None:
    from app.models.store_month_finance import StoreMonthFinance

    result = await db.execute(
        select(StoreMonthFinance.month)
        .where(
            StoreMonthFinance.store_id == store_id,
            StoreMonthFinance.status == "needs_cost",
            StoreMonthFinance.realization_available.is_(True),
        )
        .order_by(StoreMonthFinance.month.asc())
        .limit(1)
    )
    month = result.scalar_one_or_none()
    if not month:
        return None
    year_str, month_str = str(month).split("-", 1)
    return date(int(year_str), int(month_str), 1)


class VariantCostUpdate(BaseModel):
    unit_cost: float | None = Field(default=None, ge=0)
    effective_from: date | None = None


class BulkVariantCostUpdate(BaseModel):
    product_ids: List[int] = Field(min_length=1)
    unit_cost: float | None = Field(default=None, ge=0)
    effective_from: date | None = None


class BatchVariantCostUpdateItem(BaseModel):
    variant_id: int
    unit_cost: float | None = Field(default=None, ge=0)
    effective_from: date | None = None


class BatchVariantCostUpdatePayload(BaseModel):
    items: List[BatchVariantCostUpdateItem] = Field(min_length=1, max_length=500)


async def get_db():
    async with SessionLocal() as session:
        yield session


def _closed_months_export_stale_message() -> str:
    return "Себестоимость изменилась. Excel по закрытым месяцам устарел, сформируй его заново."


async def _queue_closed_months_recalc_after_cost_change(store_recalc_from: dict[int, date]) -> dict[int, dict]:
    queue = ClosedMonthsRecalcQueue()
    results: dict[int, dict] = {}
    for store_id, recalc_from in store_recalc_from.items():
        mark_store_exports_stale(
            "closed_months",
            store_id,
            message=_closed_months_export_stale_message(),
        )
        results[store_id] = await queue.queue(
            store_id,
            start_month=_closed_months_start_month(recalc_from),
        )
    return results


async def _apply_variant_cost_batch_updates(
    *,
    items: List[BatchVariantCostUpdateItem],
    db: AsyncSession,
    current_user: User,
) -> tuple[dict[int, Variant], dict[int, date], set[int]]:
    item_by_variant_id = {int(item.variant_id): item for item in items}
    variant_ids = sorted(item_by_variant_id)

    stmt = (
        select(Variant, Product.warehouse_product_id)
        .join(Product, Product.id == Variant.product_id)
        .join(Store, Store.id == Product.store_id)
        .options(selectinload(Variant.attributes), selectinload(Variant.product))
        .where(
            Variant.id.in_(variant_ids),
            Store.user_id == get_cabinet_owner_id(current_user),
        )
    )
    result = await db.execute(stmt)
    rows = result.all()
    if not rows:
        raise HTTPException(status_code=404, detail="Вариации не найдены")

    found_variant_ids = {int(variant.id) for variant, _warehouse_product_id in rows}
    missing_variant_ids = [variant_id for variant_id in variant_ids if variant_id not in found_variant_ids]
    if missing_variant_ids:
        raise HTTPException(
            status_code=404,
            detail=f"Не найдены вариации: {', '.join(str(item) for item in missing_variant_ids)}",
        )

    economics_service = EconomicsHistoryService(db)
    history_variants_by_effective_from: dict[date, list[tuple[Variant, int | None, dict[str, str]]]] = defaultdict(list)
    touched_store_effective_from: dict[int, date] = {}
    touched_product_ids: set[int] = set()
    variants_by_id: dict[int, Variant] = {}

    for variant, warehouse_product_id in rows:
        item = item_by_variant_id[int(variant.id)]
        normalized_effective_from = item.effective_from or datetime.now(timezone.utc).date()
        variant.unit_cost = item.unit_cost
        attributes = {attr.name: attr.value for attr in variant.attributes}
        history_variants_by_effective_from[normalized_effective_from].append((variant, warehouse_product_id, attributes))
        touched_product_ids.add(int(variant.product_id))
        variants_by_id[int(variant.id)] = variant

        store_id = int(variant.product.store_id)
        current_earliest = touched_store_effective_from.get(store_id)
        if current_earliest is None or normalized_effective_from < current_earliest:
            touched_store_effective_from[store_id] = normalized_effective_from

    for effective_from, history_variants in history_variants_by_effective_from.items():
        await economics_service.ensure_variant_cost_history_entries(
            variants=history_variants,
            effective_from=effective_from,
            created_by_user_id=current_user.id,
        )

    recalc_from_by_store: dict[int, date] = {}
    for store_id, effective_from in touched_store_effective_from.items():
        earliest_needs_cost_month = await _earliest_store_month_needing_cost(db, store_id)
        recalc_from = min(effective_from, earliest_needs_cost_month) if earliest_needs_cost_month else effective_from
        recalc_from_by_store[store_id] = recalc_from
        await economics_service.unlock_store_months_from_date(
            store_id=store_id,
            effective_from=recalc_from,
        )

    await db.commit()
    await _queue_closed_months_recalc_after_cost_change(recalc_from_by_store)
    return variants_by_id, recalc_from_by_store, touched_product_ids


async def _get_grouped_products(
        store_id: Optional[int],
        db: AsyncSession,
        current_user: User
):
    """
    Получить товары сгруппированные по продуктам.
    Если store_id указан - товары конкретного магазина,
    иначе - товары всех магазинов пользователя.
    """

    cabinet_owner_id = get_cabinet_owner_id(current_user)
    if store_id is not None:
        # Товары конкретного магазина
        store = await db.get(Store, store_id)
        if not store or store.user_id != cabinet_owner_id:
            raise HTTPException(status_code=403, detail="Not enough permissions")

        stmt = select(Product).where(Product.store_id == store_id).options(
            selectinload(Product.variants).selectinload(Variant.attributes),
            selectinload(Product.warehouse_product),
        )
    else:
        # Товары всех магазинов пользователя
        stores_stmt = select(Store.id).where(Store.user_id == cabinet_owner_id)
        stores_result = await db.execute(stores_stmt)
        store_ids = stores_result.scalars().all()

        stmt = select(Product).where(Product.store_id.in_(store_ids)).options(
            selectinload(Product.variants).selectinload(Variant.attributes),
            selectinload(Product.warehouse_product),
        )

    result = await db.execute(stmt)
    products = result.scalars().all()

    # Группируем товары по складскому товару, если связь уже есть.
    # Иначе откатываемся к base_name, чтобы не смешивать непривязанные позиции.
    grouped_products = defaultdict(list)

    for product in products:
        group_key = (
            f"warehouse:{product.warehouse_product_id}"
            if product.warehouse_product_id and product.warehouse_product
            else f"product:{product.id}"
        )
        grouped_products[group_key].append(product)

    response = []

    for group_key, product_group in grouped_products.items():
        all_variants = []

        for product in product_group:
            for variant in product.variants:
                attributes = {}
                for attr in variant.attributes:
                    attributes[attr.name] = attr.value

                raw_color = attributes.get('Цвет', 'без цвета')
                normalized_color = normalize_color(raw_color)
                # Сохраняем информацию о размере
                size = attributes.get('Размер', '')
                size_order = get_size_order(size)

                all_variants.append({
                    "id": variant.id,
                    "sku": variant.sku,
                    "offer_id": variant.offer_id,
                    "pack_size": variant.pack_size,
                    "is_archived": variant.is_archived,
                    "unit_cost": variant.unit_cost,
                    "barcode": variant.barcode,
                    "product_id": product.id,
                    "product_name": product.name,
                    "attributes": attributes,
                    "color": normalized_color,
                    "raw_color": raw_color,
                    "size": size,
                    "size_order": size_order,  # Для сортировки
                })

        # Группируем по цвету
        by_color = defaultdict(list)
        for variant in all_variants:
            by_color[variant["color"]].append(variant)

        color_groups = []
        for color, variants in by_color.items():
            # Группируем по размеру внутри цвета
            by_size = defaultdict(list)
            for variant in variants:
                by_size[variant["size"]].append(variant)

            # Преобразуем в список и сортируем размеры
            size_groups = []
            for size, size_variants in by_size.items():
                # Сортируем вариации внутри размера по pack_size
                size_variants.sort(key=lambda v: v['pack_size'])

                size_groups.append({
                    "size": size,
                    "variants": size_variants
                })

            # Сортируем размеры по приоритету
            size_groups.sort(key=lambda x: get_size_order(x['size']))

            color_groups.append({
                "color": color,
                "sizes": size_groups
            })

        # Сортируем цвета по алфавиту
        color_groups.sort(key=lambda x: x['color'])

        # Используем первое изображение из группы
        image_url = None
        for product in product_group:
            if product.image_url:
                image_url = product.image_url
                break

        display_name = (
            product_group[0].warehouse_product.name
            if product_group[0].warehouse_product_id and product_group[0].warehouse_product
            else product_group[0].base_name or extract_base_product_name(product_group[0].name)
        )
        archived_variants_count = sum(1 for variant in all_variants if variant["is_archived"])
        active_variants_count = len(all_variants) - archived_variants_count

        response.append({
            "id": product_group[0].id,
            "name": display_name,
            "image_url": image_url,
            "total_variants": len(all_variants),
            "is_archived": archived_variants_count > 0 and active_variants_count == 0,
            "active_variants_count": active_variants_count,
            "archived_variants_count": archived_variants_count,
            "colors": color_groups,
            "original_products": [product.id for product in product_group],
        })

    # Сортируем товары по названию
    response.sort(key=lambda x: x['name'])

    return response


@router.get("/grouped")
async def get_grouped_products_all(
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Товары всех магазинов пользователя"""
    return await _get_grouped_products(None, db, current_user)


@router.get("/grouped/{store_id}")
async def get_grouped_products(
        store_id: int,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Товары конкретного магазина"""
    return await _get_grouped_products(store_id, db, current_user)


@router.get("/cost-history/{store_id}", response_model=List[VariantCostHistoryEntryResponse])
async def get_cost_history(
    store_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    store = await db.get(Store, store_id)
    if not store or store.user_id != get_cabinet_owner_id(current_user):
        raise HTTPException(status_code=403, detail="Not enough permissions")

    result = await db.execute(
        select(VariantCostHistory, Product.id, Product.name, Variant.is_archived)
        .join(Variant, Variant.id == VariantCostHistory.variant_id)
        .join(Product, Product.id == Variant.product_id)
        .join(Store, Store.id == Product.store_id)
        .where(Store.id == store_id)
        .order_by(
            VariantCostHistory.effective_from.desc(),
            VariantCostHistory.created_at.desc(),
            Product.name.asc(),
            VariantCostHistory.offer_id.asc(),
        )
    )
    rows = result.all()
    return [
        VariantCostHistoryEntryResponse(
            id=item.id,
            variant_id=item.variant_id,
            product_id=int(product_id),
            product_name=str(product_name or ""),
            offer_id=str(item.offer_id or ""),
            pack_size=int(item.pack_size or 1),
            color=item.color,
            size=item.size,
            unit_cost=float(item.unit_cost) if item.unit_cost is not None else None,
            effective_from=item.effective_from,
            created_at=item.created_at,
            is_archived=bool(is_archived),
        )
        for item, product_id, product_name, is_archived in rows
    ]


@router.delete("/cost-history/{history_id}")
async def delete_cost_history_entry(
    history_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    target_row = (
        await db.execute(
            select(VariantCostHistory, Variant, Product, Store)
            .join(Variant, Variant.id == VariantCostHistory.variant_id)
            .join(Product, Product.id == Variant.product_id)
            .join(Store, Store.id == Product.store_id)
            .options(selectinload(Variant.product), selectinload(Variant.attributes))
            .where(
                VariantCostHistory.id == history_id,
                Store.user_id == cabinet_owner_id,
            )
        )
    ).first()
    if not target_row:
        raise HTTPException(status_code=404, detail="Запись истории себестоимости не найдена")

    target_history, target_variant, target_product, _target_store = target_row
    touched_variant_ids = {int(target_variant.id)}
    touched_store_ids = {int(target_product.store_id)}

    delete_stmt = delete(VariantCostHistory).where(VariantCostHistory.id == history_id)
    await db.execute(delete_stmt)
    await db.flush()

    for variant_id in touched_variant_ids:
        latest = (
            await db.execute(
                select(VariantCostHistory)
                .where(VariantCostHistory.variant_id == variant_id)
                .order_by(VariantCostHistory.effective_from.desc(), VariantCostHistory.id.desc())
                .limit(1)
            )
        ).scalars().first()
        variant = await db.get(Variant, variant_id)
        if variant is not None:
          variant.unit_cost = float(latest.unit_cost) if latest and latest.unit_cost is not None else None

    for touched_store_id in touched_store_ids:
        await EconomicsHistoryService(db).unlock_store_months_from_date(
            store_id=touched_store_id,
            effective_from=target_history.effective_from,
        )

    await db.commit()

    await _queue_closed_months_recalc_after_cost_change(
        {touched_store_id: target_history.effective_from for touched_store_id in touched_store_ids}
    )

    return {"status": "success", "message": "Запись истории себестоимости удалена"}


@router.patch("/variants/{variant_id}/cost")
async def update_variant_cost(
    variant_id: int,
    payload: VariantCostUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    variants_by_id, _recalc_from_by_store, _touched_product_ids = await _apply_variant_cost_batch_updates(
        items=[
            BatchVariantCostUpdateItem(
                variant_id=variant_id,
                unit_cost=payload.unit_cost,
                effective_from=payload.effective_from,
            )
        ],
        db=db,
        current_user=current_user,
    )
    variant = variants_by_id.get(variant_id)
    if not variant:
        raise HTTPException(status_code=404, detail="Вариация не найдена")
    return {
        "id": variant.id,
        "unit_cost": variant.unit_cost,
    }


@router.patch("/grouped/cost")
async def update_grouped_product_cost(
    payload: BulkVariantCostUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    product_ids = sorted({int(product_id) for product_id in payload.product_ids})
    products_stmt = (
        select(Product.id, Product.warehouse_product_id)
        .join(Store, Store.id == Product.store_id)
        .where(
            Product.id.in_(product_ids),
            Store.user_id == get_cabinet_owner_id(current_user),
        )
    )
    products_result = await db.execute(products_stmt)
    product_rows = products_result.all()
    if not product_rows:
        raise HTTPException(status_code=404, detail="Товары для массового обновления не найдены")

    target_product_ids = {int(product_id) for product_id, _warehouse_product_id in product_rows}

    stmt = (
        select(Variant)
        .join(Product, Product.id == Variant.product_id)
        .join(Store, Store.id == Product.store_id)
        .options(selectinload(Variant.product), selectinload(Variant.attributes))
        .where(
            Store.user_id == get_cabinet_owner_id(current_user),
            Product.id.in_(target_product_ids),
        )
    )

    result = await db.execute(stmt)
    variants = result.scalars().all()
    if not variants:
        raise HTTPException(status_code=404, detail="Вариации для массового обновления не найдены")

    updated_product_ids = {int(variant.product_id) for variant in variants}
    _, _recalc_from_by_store, _ = await _apply_variant_cost_batch_updates(
        items=[
            BatchVariantCostUpdateItem(
                variant_id=int(variant.id),
                unit_cost=payload.unit_cost,
                effective_from=payload.effective_from,
            )
            for variant in variants
        ],
        db=db,
        current_user=current_user,
    )

    return {
        "updated_variants": len(variants),
        "updated_products": len(updated_product_ids),
        "unit_cost": payload.unit_cost,
    }


@router.post("/costs/batch")
async def update_variant_costs_batch(
    payload: BatchVariantCostUpdatePayload,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    variants_by_id, recalc_from_by_store, touched_product_ids = await _apply_variant_cost_batch_updates(
        items=payload.items,
        db=db,
        current_user=current_user,
    )
    return {
        "updated_variants": len(variants_by_id),
        "updated_products": len(touched_product_ids),
        "affected_stores": sorted(recalc_from_by_store),
        "queued_recalc": True,
    }
