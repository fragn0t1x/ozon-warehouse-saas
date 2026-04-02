from collections import defaultdict
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.dependencies import get_current_user
from app.database import SessionLocal
from app.models.product import Product
from app.models.store import Store
from app.models.user import User
from app.models.variant import Variant
from app.models.warehouse_product import WarehouseProduct
from app.services.cabinet_access import get_cabinet_owner_id
from app.services.product_grouping import get_size_order, normalize_color

router = APIRouter(prefix="/warehouse-products", tags=["warehouse-products"])


async def get_db():
    async with SessionLocal() as session:
        yield session


class VariantSelectionRequest(BaseModel):
    variant_ids: list[int] = Field(min_length=1)


class VariantAttachRequest(VariantSelectionRequest):
    warehouse_product_id: Optional[int] = None
    warehouse_product_name: Optional[str] = None

    @field_validator("warehouse_product_name", mode="before")
    @classmethod
    def normalize_name(cls, value: Optional[str]) -> Optional[str]:
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @model_validator(mode="after")
    def validate_target(self):
        if self.warehouse_product_id is None and not self.warehouse_product_name:
            raise ValueError("Нужно выбрать существующий товар или указать название нового")
        return self


# Backward-compatible alias for older imports/tests.
class ProductRelinkRequest(BaseModel):
    warehouse_product_id: Optional[int] = None
    warehouse_product_name: Optional[str] = None

    @field_validator("warehouse_product_name", mode="before")
    @classmethod
    def normalize_name(cls, value: Optional[str]) -> Optional[str]:
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @model_validator(mode="after")
    def validate_target(self):
        if self.warehouse_product_id is None and not self.warehouse_product_name:
            raise ValueError("Нужно выбрать существующий товар или указать название нового")
        return self


def _variant_payload(product: Product, variant: Variant) -> dict:
    attributes = {attr.name: attr.value for attr in variant.attributes}
    raw_color = attributes.get("Цвет", "без цвета")
    normalized_color = normalize_color(raw_color)
    size = attributes.get("Размер", "") or "Без размера"

    return {
        "id": variant.id,
        "offer_id": variant.offer_id,
        "pack_size": variant.pack_size or 1,
        "color": normalized_color,
        "size": size,
        "size_order": get_size_order(size),
        "attributes": attributes,
        "store_id": product.store_id,
        "store_name": product.store.name if product.store else "Без магазина",
        "source_product_id": product.id,
        "source_product_name": product.name,
        "source_base_name": product.base_name or product.name,
        "image_url": product.image_url,
        "is_archived": variant.is_archived,
    }


def _sort_variant_payloads(items: list[dict]) -> list[dict]:
    return sorted(
        items,
        key=lambda item: (
            item["store_name"].lower(),
            item["source_base_name"].lower(),
            item["color"].lower(),
            item["size_order"],
            item["pack_size"],
            item["offer_id"].lower(),
        ),
    )


async def _resolve_target_warehouse_product(
    db: AsyncSession,
    *,
    owner_id: int,
    warehouse_product_id: Optional[int],
    warehouse_product_name: Optional[str],
) -> WarehouseProduct:
    if warehouse_product_id is not None:
        result = await db.execute(
            select(WarehouseProduct).where(
                WarehouseProduct.id == warehouse_product_id,
                WarehouseProduct.user_id == owner_id,
            )
        )
        warehouse_product = result.scalar_one_or_none()
        if not warehouse_product:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Товар склада не найден")
        if warehouse_product.is_archived:
            warehouse_product.is_archived = False
            await db.flush()
        return warehouse_product

    normalized_name = (warehouse_product_name or "").strip()
    if not normalized_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Укажите название нового товара")

    result = await db.execute(
        select(WarehouseProduct).where(
            WarehouseProduct.user_id == owner_id,
            WarehouseProduct.name == normalized_name,
        )
    )
    warehouse_product = result.scalars().first()
    if warehouse_product:
        if warehouse_product.is_archived:
            warehouse_product.is_archived = False
            await db.flush()
        return warehouse_product

    warehouse_product = WarehouseProduct(user_id=owner_id, name=normalized_name, is_archived=False)
    db.add(warehouse_product)
    await db.flush()
    return warehouse_product


async def _get_or_create_product_bucket(
    db: AsyncSession,
    *,
    source_product: Product,
    warehouse_product_id: Optional[int],
    cache: dict[tuple[int, str, Optional[int]], Product],
) -> Product:
    base_name = source_product.base_name or source_product.name
    cache_key = (source_product.store_id, base_name, warehouse_product_id)
    if cache_key in cache:
        return cache[cache_key]

    if source_product.warehouse_product_id == warehouse_product_id:
        cache[cache_key] = source_product
        return source_product

    stmt = select(Product).where(
        Product.store_id == source_product.store_id,
        Product.base_name == base_name,
    )
    if warehouse_product_id is None:
        stmt = stmt.where(Product.warehouse_product_id.is_(None))
    else:
        stmt = stmt.where(Product.warehouse_product_id == warehouse_product_id)

    result = await db.execute(stmt.order_by(Product.id))
    existing = result.scalars().first()
    if existing:
        cache[cache_key] = existing
        return existing

    product = Product(
        store_id=source_product.store_id,
        warehouse_product_id=warehouse_product_id,
        name=source_product.name,
        base_name=base_name,
        image_url=source_product.image_url,
    )
    db.add(product)
    await db.flush()
    cache[cache_key] = product
    return product


async def _cleanup_empty_products(db: AsyncSession, product_ids: set[int]) -> set[int]:
    if not product_ids:
        return set()

    empty_result = await db.execute(
        select(Product.id)
        .outerjoin(Variant, Variant.product_id == Product.id)
        .where(Product.id.in_(product_ids))
        .group_by(Product.id)
        .having(func.count(Variant.id) == 0)
    )
    empty_ids = set(empty_result.scalars().all())
    if not empty_ids:
        return set()

    await db.execute(delete(Product).where(Product.id.in_(empty_ids)))
    return empty_ids


async def _cleanup_orphaned_warehouse_products(
    db: AsyncSession,
    *,
    owner_id: int,
    warehouse_product_ids: set[int],
) -> None:
    candidate_ids = {warehouse_product_id for warehouse_product_id in warehouse_product_ids if warehouse_product_id}
    if not candidate_ids:
        return

    linked_result = await db.execute(
        select(Product.warehouse_product_id).where(Product.warehouse_product_id.in_(candidate_ids))
    )
    still_linked = {warehouse_product_id for warehouse_product_id in linked_result.scalars().all() if warehouse_product_id}
    orphan_ids = candidate_ids - still_linked
    if not orphan_ids:
        return

    await db.execute(
        delete(WarehouseProduct).where(
            WarehouseProduct.user_id == owner_id,
            WarehouseProduct.id.in_(orphan_ids),
        )
    )


async def _load_owned_variants(
    db: AsyncSession,
    *,
    owner_id: int,
    variant_ids: list[int],
) -> list[Variant]:
    result = await db.execute(
        select(Variant)
        .join(Product, Product.id == Variant.product_id)
        .join(Store, Store.id == Product.store_id)
        .where(
            Variant.id.in_(variant_ids),
            Store.user_id == owner_id,
        )
        .options(
            selectinload(Variant.attributes),
            selectinload(Variant.product).selectinload(Product.store),
        )
    )
    variants = result.scalars().unique().all()
    if len({variant.id for variant in variants}) != len(set(variant_ids)):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Часть вариаций не найдена")
    return variants


async def _move_variants(
    db: AsyncSession,
    *,
    owner_id: int,
    variants: list[Variant],
    warehouse_product_id: Optional[int],
) -> int:
    source_product_ids = {variant.product_id for variant in variants}
    source_warehouse_product_ids = {
        variant.product.warehouse_product_id
        for variant in variants
        if variant.product and variant.product.warehouse_product_id is not None
    }
    bucket_cache: dict[tuple[int, str, Optional[int]], Product] = {}
    moved = 0

    for variant in variants:
        source_product = variant.product
        if not source_product:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="У вариации не найден исходный товар")
        target_product = await _get_or_create_product_bucket(
            db,
            source_product=source_product,
            warehouse_product_id=warehouse_product_id,
            cache=bucket_cache,
        )
        if variant.product_id != target_product.id:
            variant.product_id = target_product.id
            moved += 1

    await db.flush()
    await _cleanup_empty_products(db, source_product_ids)
    await _cleanup_orphaned_warehouse_products(
        db,
        owner_id=owner_id,
        warehouse_product_ids=source_warehouse_product_ids,
    )
    return moved


@router.get("/links")
async def get_warehouse_product_links(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    owner_id = get_cabinet_owner_id(current_user)

    warehouse_products_result = await db.execute(
        select(WarehouseProduct)
        .where(
            WarehouseProduct.user_id == owner_id,
            WarehouseProduct.is_archived.is_(False),
        )
        .options(
            selectinload(WarehouseProduct.products).selectinload(Product.store),
            selectinload(WarehouseProduct.products).selectinload(Product.variants).selectinload(Variant.attributes),
        )
        .order_by(WarehouseProduct.name)
    )
    warehouse_products = warehouse_products_result.scalars().unique().all()

    unlinked_products_result = await db.execute(
        select(Product)
        .join(Store, Store.id == Product.store_id)
        .where(
            Store.user_id == owner_id,
            Product.warehouse_product_id.is_(None),
        )
        .options(
            selectinload(Product.store),
            selectinload(Product.variants).selectinload(Variant.attributes),
        )
        .order_by(Product.base_name, Product.id)
    )
    unlinked_products = unlinked_products_result.scalars().unique().all()

    warehouse_payload = []
    for warehouse_product in warehouse_products:
        variants_payload = _sort_variant_payloads(
            [
                _variant_payload(product, variant)
                for product in warehouse_product.products
                for variant in product.variants
            ]
        )
        if not variants_payload:
            continue

        warehouse_payload.append(
            {
                "id": warehouse_product.id,
                "name": warehouse_product.name,
                "variants_count": len(variants_payload),
                "stores_count": len({item["store_id"] for item in variants_payload}),
                "variants": variants_payload,
            }
        )

    unlinked_variants = _sort_variant_payloads(
        [
            _variant_payload(product, variant)
            for product in unlinked_products
            for variant in product.variants
        ]
    )

    return {
        "warehouse_products": warehouse_payload,
        "unlinked_variants": unlinked_variants,
    }


@router.post("/variants/attach")
async def attach_variants_to_warehouse_product(
    payload: VariantAttachRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    owner_id = get_cabinet_owner_id(current_user)
    variants = await _load_owned_variants(db, owner_id=owner_id, variant_ids=payload.variant_ids)
    target_warehouse_product = await _resolve_target_warehouse_product(
        db,
        owner_id=owner_id,
        warehouse_product_id=payload.warehouse_product_id,
        warehouse_product_name=payload.warehouse_product_name,
    )

    moved = await _move_variants(
        db,
        owner_id=owner_id,
        variants=variants,
        warehouse_product_id=target_warehouse_product.id,
    )
    await db.commit()

    return {
        "status": "ok",
        "message": "Вариации привязаны к товару",
        "warehouse_product_id": target_warehouse_product.id,
        "warehouse_product_name": target_warehouse_product.name,
        "moved_variants": moved,
    }


async def relink_product_group(
    product_id: int,
    payload: ProductRelinkRequest,
    db: AsyncSession,
    current_user: User,
):
    owner_id = get_cabinet_owner_id(current_user)
    result = await db.execute(
        select(Product)
        .where(Product.id == product_id)
        .options(
            selectinload(Product.store),
            selectinload(Product.variants).selectinload(Variant.attributes),
        )
    )
    product = result.scalar_one_or_none()
    if not product:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Товар не найден")

    if not product.store or product.store.user_id != owner_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Нет доступа к товару")

    variant_ids = [variant.id for variant in product.variants]
    if not variant_ids:
        return {
            "status": "ok",
            "message": "У товара нет вариаций для переноса",
            "warehouse_product_id": None,
            "warehouse_product_name": None,
            "moved_variants": 0,
        }

    return await attach_variants_to_warehouse_product(
        VariantAttachRequest(
            variant_ids=variant_ids,
            warehouse_product_id=payload.warehouse_product_id,
            warehouse_product_name=payload.warehouse_product_name,
        ),
        db=db,
        current_user=current_user,
    )


@router.post("/variants/detach")
async def detach_variants_from_warehouse_product(
    payload: VariantSelectionRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    owner_id = get_cabinet_owner_id(current_user)
    variants = await _load_owned_variants(db, owner_id=owner_id, variant_ids=payload.variant_ids)

    moved = await _move_variants(
        db,
        owner_id=owner_id,
        variants=variants,
        warehouse_product_id=None,
    )
    await db.commit()

    return {
        "status": "ok",
        "message": "Вариации отвязаны от товара",
        "moved_variants": moved,
    }


@router.delete("/{warehouse_product_id}")
async def delete_warehouse_product(
    warehouse_product_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    owner_id = get_cabinet_owner_id(current_user)
    result = await db.execute(
        select(WarehouseProduct)
        .where(
            WarehouseProduct.id == warehouse_product_id,
            WarehouseProduct.user_id == owner_id,
        )
        .options(
            selectinload(WarehouseProduct.products).selectinload(Product.store),
            selectinload(WarehouseProduct.products).selectinload(Product.variants).selectinload(Variant.attributes),
        )
    )
    warehouse_product = result.scalar_one_or_none()
    if not warehouse_product:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Товар склада не найден")

    variants = [variant for product in warehouse_product.products for variant in product.variants]
    if variants:
        await _move_variants(
            db,
            owner_id=owner_id,
            variants=variants,
            warehouse_product_id=None,
        )

    await db.execute(
        delete(WarehouseProduct).where(
            WarehouseProduct.id == warehouse_product_id,
            WarehouseProduct.user_id == owner_id,
        )
    )
    await db.commit()

    return {
        "status": "ok",
        "message": "Товар склада удален, вариации отвязаны",
        "detached_variants": len(variants),
    }
