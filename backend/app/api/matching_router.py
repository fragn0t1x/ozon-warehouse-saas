# backend/app/api/matching_router.py
from __future__ import annotations

from difflib import SequenceMatcher
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.dependencies import get_current_user
from app.database import SessionLocal
from app.models.base_models import BaseProduct, BaseVariant, ProductMatch, VariantMatch
from app.models.product import Product
from app.models.store import Store
from app.models.user import User
from app.models.variant import Variant
from app.models.warehouse import WarehouseStock
from app.services.cabinet_access import get_cabinet_owner_id

router = APIRouter(prefix="/matching", tags=["matching"])


async def get_db():
    async with SessionLocal() as session:
        yield session


def calculate_similarity(name1: str, name2: str) -> float:
    """Рассчитывает похожесть двух названий"""
    return SequenceMatcher(None, name1.lower(), name2.lower()).ratio()


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _normalize_attribute_key(name: str) -> str:
    normalized = _normalize_text(name)
    if "цвет" in normalized:
        return "цвет"
    if "размер" in normalized:
        return "размер"
    return normalized


def _extract_variant_attributes(variant: Variant) -> dict[str, str]:
    return {
        attr.name: attr.value
        for attr in (variant.attributes or [])
        if getattr(attr, "name", None)
    }


def _canonical_attributes(attributes: dict[str, Any] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw_key, raw_value in (attributes or {}).items():
        key = _normalize_attribute_key(str(raw_key))
        value = _normalize_text(raw_value)
        if key and value:
            result[key] = value
    return result


def _variant_signature_from_attributes(pack_size: int | None, attributes: dict[str, Any] | None) -> tuple[int, str, str]:
    canonical = _canonical_attributes(attributes)
    return (
        int(pack_size or 1),
        canonical.get("цвет", ""),
        canonical.get("размер", ""),
    )


def _variant_signature(variant: Variant) -> tuple[int, str, str]:
    return _variant_signature_from_attributes(variant.pack_size, _extract_variant_attributes(variant))


def _base_variant_signature(base_variant: BaseVariant) -> tuple[int, str, str]:
    return _variant_signature_from_attributes(base_variant.pack_size, base_variant.attributes)


def _variant_signature_set_for_product(product: Product) -> set[tuple[int, str, str]]:
    return {_variant_signature(variant) for variant in (product.variants or [])}


def _variant_signature_set_for_base_product(base_product: BaseProduct) -> set[tuple[int, str, str]]:
    return {_base_variant_signature(variant) for variant in (base_product.base_variants or [])}


def _product_match_score(product: Product, base_product: BaseProduct) -> float:
    name_score = calculate_similarity(product.name, base_product.name)
    product_signatures = _variant_signature_set_for_product(product)
    base_signatures = _variant_signature_set_for_base_product(base_product)

    if not product_signatures or not base_signatures:
        return name_score

    union = product_signatures | base_signatures
    overlap = len(product_signatures & base_signatures) / len(union) if union else 0.0
    return name_score * 0.7 + overlap * 0.3


def _available_variant_stock(variant: Variant, *, user_id: int) -> int:
    pack_size = int(variant.pack_size or 1)
    total = 0
    for stock in (variant.warehouse_stocks or []):
        warehouse = getattr(stock, "warehouse", None)
        if warehouse is None or warehouse.user_id != user_id:
            continue
        total += int(stock.unpacked_quantity or 0)
        total += int(stock.packed_quantity or 0) * pack_size
        total -= int(stock.reserved_quantity or 0)
    return max(total, 0)


async def _load_product_for_matching(db: AsyncSession, product_id: int) -> Product | None:
    result = await db.execute(
        select(Product)
        .where(Product.id == product_id)
        .options(selectinload(Product.variants).selectinload(Variant.attributes))
    )
    return result.scalar_one_or_none()


async def _load_base_product_for_matching(db: AsyncSession, base_product_id: int) -> BaseProduct | None:
    result = await db.execute(
        select(BaseProduct)
        .where(BaseProduct.id == base_product_id)
        .options(
            selectinload(BaseProduct.base_variants),
            selectinload(BaseProduct.product_matches),
        )
    )
    return result.scalar_one_or_none()


async def _ensure_base_variant_for_variant(
    db: AsyncSession,
    *,
    base_product: BaseProduct,
    variant: Variant,
) -> BaseVariant:
    attributes = _extract_variant_attributes(variant)
    signature = _variant_signature_from_attributes(variant.pack_size, attributes)

    for base_variant in base_product.base_variants or []:
        if _base_variant_signature(base_variant) == signature:
            return base_variant

    normalized_sku = _normalize_text(variant.sku)
    if normalized_sku:
        for base_variant in base_product.base_variants or []:
            if _normalize_text(base_variant.sku) == normalized_sku:
                return base_variant

    base_variant = BaseVariant(
        base_product_id=base_product.id,
        sku=variant.sku,
        pack_size=variant.pack_size,
        attributes=attributes,
    )
    db.add(base_variant)
    await db.flush()
    base_product.base_variants.append(base_variant)
    return base_variant


@router.get("/suggestions/{store_id}")
async def get_matching_suggestions(
    store_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Получить предложения по сопоставлению товаров из нового магазина
    с существующими базовыми товарами
    """
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    store = await db.get(Store, store_id)
    if not store or store.user_id != cabinet_owner_id:
        raise HTTPException(404, "Store not found")

    products_result = await db.execute(
        select(Product)
        .where(Product.store_id == store_id)
        .options(selectinload(Product.variants).selectinload(Variant.attributes))
    )
    new_products = products_result.scalars().all()

    base_products_result = await db.execute(
        select(BaseProduct).options(selectinload(BaseProduct.base_variants))
    )
    base_products = base_products_result.scalars().all()

    existing_matches_result = await db.execute(
        select(ProductMatch.product_id).where(ProductMatch.store_id == store_id)
    )
    matched_product_ids = {row[0] for row in existing_matches_result.all()}

    suggestions = []

    for new_product in new_products:
        if new_product.id in matched_product_ids:
            continue

        best_match = None
        best_score = 0.0

        for base_product in base_products:
            score = _product_match_score(new_product, base_product)
            if score > best_score and score > 0.6:
                best_score = score
                best_match = base_product

        suggestions.append(
            {
                "new_product": {
                    "id": new_product.id,
                    "name": new_product.name,
                    "store_id": store_id,
                    "variants_count": len(new_product.variants or []),
                },
                "base_product": (
                    {
                        "id": best_match.id,
                        "name": best_match.name,
                        "variants_count": len(best_match.base_variants or []),
                    }
                    if best_match
                    else None
                ),
                "similarity": round(best_score, 4) if best_match else 0,
            }
        )

    return suggestions


@router.post("/match")
async def match_product(
    new_product_id: int,
    base_product_id: Optional[int] = None,
    create_new: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Сопоставить товар из магазина с базовым товаром
    или создать новый базовый товар
    """
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    product = await _load_product_for_matching(db, new_product_id)
    if not product:
        raise HTTPException(404, "Product not found")

    store = await db.get(Store, product.store_id)
    if not store or store.user_id != cabinet_owner_id:
        raise HTTPException(403, "Not enough permissions")

    if create_new or not base_product_id:
        base_product = BaseProduct(name=product.name, category="unknown")
        db.add(base_product)
        await db.flush()
    else:
        base_product = await _load_base_product_for_matching(db, base_product_id)
        if not base_product:
            raise HTTPException(404, "Base product not found")

    existing_product_match_result = await db.execute(
        select(ProductMatch).where(
            ProductMatch.store_id == store.id,
            ProductMatch.product_id == product.id,
        )
    )
    product_match = existing_product_match_result.scalar_one_or_none()
    if product_match:
        product_match.base_product_id = base_product.id
        product_match.match_type = "manual"
    else:
        product_match = ProductMatch(
            base_product_id=base_product.id,
            store_id=store.id,
            product_id=product.id,
            match_type="manual",
        )
        db.add(product_match)

    variant_ids = [variant.id for variant in product.variants or []]
    existing_variant_matches: dict[int, VariantMatch] = {}
    if variant_ids:
        existing_variant_matches_result = await db.execute(
            select(VariantMatch).where(
                VariantMatch.store_id == store.id,
                VariantMatch.variant_id.in_(variant_ids),
            )
        )
        existing_variant_matches = {
            variant_match.variant_id: variant_match
            for variant_match in existing_variant_matches_result.scalars().all()
        }

    for variant in product.variants or []:
        base_variant = await _ensure_base_variant_for_variant(
            db,
            base_product=base_product,
            variant=variant,
        )
        attributes = _extract_variant_attributes(variant)
        variant_match = existing_variant_matches.get(variant.id)
        if variant_match:
            variant_match.base_variant_id = base_variant.id
            variant_match.attributes = attributes
        else:
            db.add(
                VariantMatch(
                    base_variant_id=base_variant.id,
                    store_id=store.id,
                    variant_id=variant.id,
                    attributes=attributes,
                )
            )

    await db.commit()

    return {
        "status": "success",
        "base_product_id": base_product.id,
        "product_id": product.id,
        "variants_matched": len(product.variants or []),
    }


@router.get("/combined-stocks")
async def get_combined_stocks(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Получить объединенные остатки по базовым товарам
    """
    cabinet_owner_id = get_cabinet_owner_id(current_user)
    base_products_result = await db.execute(
        select(BaseProduct)
        .join(ProductMatch, ProductMatch.base_product_id == BaseProduct.id)
        .join(Product, Product.id == ProductMatch.product_id)
        .join(Store, Store.id == Product.store_id)
        .where(Store.user_id == cabinet_owner_id)
        .distinct()
        .options(
            selectinload(BaseProduct.product_matches).selectinload(ProductMatch.store),
            selectinload(BaseProduct.product_matches)
            .selectinload(ProductMatch.product)
            .selectinload(Product.variants)
            .selectinload(Variant.attributes),
            selectinload(BaseProduct.product_matches)
            .selectinload(ProductMatch.product)
            .selectinload(Product.variants)
            .selectinload(Variant.warehouse_stocks)
            .selectinload(WarehouseStock.warehouse),
            selectinload(BaseProduct.base_variants)
            .selectinload(BaseVariant.variant_matches)
            .selectinload(VariantMatch.variant)
            .selectinload(Variant.attributes),
            selectinload(BaseProduct.base_variants)
            .selectinload(BaseVariant.variant_matches)
            .selectinload(VariantMatch.variant)
            .selectinload(Variant.warehouse_stocks)
            .selectinload(WarehouseStock.warehouse),
        )
    )
    base_products = base_products_result.scalars().all()

    result = []

    for base_product in base_products:
        products_data = []
        total_stock = 0

        for product_match in base_product.product_matches or []:
            product = product_match.product
            store = product_match.store
            if not product or not store or store.user_id != cabinet_owner_id:
                continue

            product_stock = 0
            variants_data = []

            for variant in product.variants or []:
                variant_stock = _available_variant_stock(variant, user_id=cabinet_owner_id)
                product_stock += variant_stock
                total_stock += variant_stock

                variants_data.append(
                    {
                        "id": variant.id,
                        "sku": variant.sku,
                        "pack_size": variant.pack_size,
                        "attributes": _extract_variant_attributes(variant),
                        "stock": variant_stock,
                    }
                )

            products_data.append(
                {
                    "store_id": store.id,
                    "store_name": store.name,
                    "product_id": product.id,
                    "product_name": product.name,
                    "variants": variants_data,
                    "stock": product_stock,
                }
            )

        grouped_variants = {}
        for base_variant in base_product.base_variants or []:
            total_variant_stock = 0
            stores_variants = []

            for variant_match in base_variant.variant_matches or []:
                if variant_match.store and variant_match.store.user_id != cabinet_owner_id:
                    continue

                variant = variant_match.variant
                if not variant:
                    continue

                stock = _available_variant_stock(variant, user_id=cabinet_owner_id)
                total_variant_stock += stock
                stores_variants.append(
                    {
                        "store_id": variant_match.store_id,
                        "variant_id": variant_match.variant_id,
                        "stock": stock,
                    }
                )

            grouped_variants[base_variant.id] = {
                "sku": base_variant.sku,
                "pack_size": base_variant.pack_size,
                "attributes": base_variant.attributes or {},
                "total_stock": total_variant_stock,
                "stores": stores_variants,
            }

        result.append(
            {
                "base_product_id": base_product.id,
                "name": base_product.name,
                "total_stock": total_stock,
                "stores_count": len(products_data),
                "products": products_data,
                "grouped_variants": grouped_variants,
            }
        )

    return result
