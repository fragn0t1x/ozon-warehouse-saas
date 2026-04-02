from __future__ import annotations

import asyncio
import hashlib
from collections import defaultdict
from typing import Any, Optional

from httpx import HTTPStatusError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.product import Product
from app.models.variant import Variant
from app.models.warehouse_product import WarehouseProduct
from app.schemas.store import (
    StoreImportPreviewResponse,
    StorePreviewCandidate,
    StorePreviewColorGroup,
    StorePreviewProductGroup,
    StorePreviewSizeGroup,
    StorePreviewVariant,
    WarehouseProductOption,
)
from app.config import settings
from app.services.ozon.client import OzonClient
from app.services.product_grouping import (
    build_ozon_product_groups,
    get_size_order,
    normalize_color,
    serialize_group_key,
)
from app.utils.redis_cache import cache_get_json, cache_set_json


class StoreLinkingService:
    def __init__(self, db: AsyncSession):
        self.db = db

    @staticmethod
    def _build_snapshot_cache_key(user_id: int, client_id: str, api_key: str) -> str:
        api_key_hash = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
        return f"ozon:onboarding-preview:{user_id}:{client_id}:{api_key_hash}"

    async def get_cached_catalog_snapshot(
        self,
        user_id: int,
        client_id: str,
        api_key: str,
    ) -> list[dict[str, Any]] | None:
        cached = await cache_get_json(self._build_snapshot_cache_key(user_id, client_id, api_key))
        if not isinstance(cached, dict):
            return None
        attributes_payloads = cached.get("attributes_payloads")
        if not isinstance(attributes_payloads, list):
            return None
        return attributes_payloads

    async def _cache_catalog_snapshot(
        self,
        user_id: int,
        client_id: str,
        api_key: str,
        *,
        attributes_payloads: list[dict[str, Any]],
    ) -> None:
        ttl_seconds = max(int(settings.OZON_ONBOARDING_PREVIEW_TTL_SECONDS), 0)
        if ttl_seconds <= 0:
            return
        await cache_set_json(
            self._build_snapshot_cache_key(user_id, client_id, api_key),
            {"attributes_payloads": attributes_payloads},
            ttl_seconds,
        )

    async def build_preview(self, user_id: int, client_id: str, api_key: str) -> StoreImportPreviewResponse:
        client = OzonClient(client_id, api_key)
        try:
            product_payloads = await self._load_all_products(client)
            attributes_payloads = await self._load_attribute_snapshot(client, product_payloads)
            grouped_products = self._build_grouped_products(attributes_payloads)
            await self._cache_catalog_snapshot(
                user_id,
                client_id,
                api_key,
                attributes_payloads=attributes_payloads,
            )
        finally:
            await client.close()

        warehouse_products_result = await self.db.execute(
            select(WarehouseProduct)
            .where(
                WarehouseProduct.user_id == user_id,
                WarehouseProduct.is_archived.is_(False),
            )
            .options(
                selectinload(WarehouseProduct.products)
                .selectinload(Product.variants)
                .selectinload(Variant.attributes)
            )
            .order_by(WarehouseProduct.name)
        )
        warehouse_products = warehouse_products_result.scalars().all()

        grouped_products = self._apply_match_suggestions(grouped_products, warehouse_products)

        return StoreImportPreviewResponse(
            grouped_products=grouped_products,
            available_warehouse_products=[
                WarehouseProductOption(id=item.id, name=item.name)
                for item in warehouse_products
            ],
        )

    def _normalize_name(self, value: str) -> str:
        return " ".join((value or "").lower().split())

    def _variant_fingerprints_from_preview(self, group: StorePreviewProductGroup) -> set[str]:
        return {
            f"{self._normalize_name(color_group.color)}::{self._normalize_name(size_group.size)}"
            for color_group in group.colors
            for size_group in color_group.sizes
        }

    def _variant_fingerprints_from_warehouse_product(self, warehouse_product: WarehouseProduct) -> set[str]:
        fingerprints: set[str] = set()
        for product in warehouse_product.products:
            for variant in product.variants:
                attributes = {attr.name: attr.value for attr in variant.attributes}
                color = normalize_color(attributes.get("Цвет", "Без цвета"))
                size = attributes.get("Размер", "") or "Без размера"
                fingerprints.add(f"{self._normalize_name(color)}::{self._normalize_name(size)}")
        return fingerprints

    def _score_candidate(
        self,
        group: StorePreviewProductGroup,
        warehouse_product: WarehouseProduct,
    ) -> StorePreviewCandidate:
        normalized_base_name = self._normalize_name(group.base_name)
        normalized_candidate_name = self._normalize_name(warehouse_product.name)

        score = 0
        reasons: list[str] = []
        if normalized_candidate_name == normalized_base_name:
            score += 100
            reasons.append("точное совпадение названия товара")
        elif normalized_candidate_name in normalized_base_name or normalized_base_name in normalized_candidate_name:
            score += 60
            reasons.append("похожее базовое название товара")

        group_fingerprints = self._variant_fingerprints_from_preview(group)
        candidate_fingerprints = self._variant_fingerprints_from_warehouse_product(warehouse_product)
        overlap = len(group_fingerprints & candidate_fingerprints)
        overlap_total = len(group_fingerprints)

        if overlap_total:
            score += int((overlap / overlap_total) * 100)
            if overlap:
                reasons.append(f"совпадает {overlap} из {overlap_total} сочетаний цвет + размер")

        return StorePreviewCandidate(
            id=warehouse_product.id,
            name=warehouse_product.name,
            score=score,
            overlap_count=overlap,
            overlap_total=overlap_total,
            reasons=reasons,
        )

    def _apply_match_suggestions(
        self,
        grouped_products: list[StorePreviewProductGroup],
        warehouse_products: list[WarehouseProduct],
    ) -> list[StorePreviewProductGroup]:
        if not warehouse_products:
            return grouped_products

        enriched_groups: list[StorePreviewProductGroup] = []
        for group in grouped_products:
            candidates = sorted(
                (self._score_candidate(group, warehouse_product) for warehouse_product in warehouse_products),
                key=lambda item: (-item.score, item.name.lower()),
            )
            best_candidates = [candidate for candidate in candidates if candidate.score > 0][:3]

            if not best_candidates:
                group.match_status = "new"
                group.suggested_warehouse_product_id = None
                group.candidates = []
                enriched_groups.append(group)
                continue

            best_candidate = best_candidates[0]
            second_candidate = best_candidates[1] if len(best_candidates) > 1 else None

            if (
                best_candidate.score >= 170
                and (
                    second_candidate is None
                    or best_candidate.score - second_candidate.score >= 25
                )
            ):
                group.match_status = "auto"
                group.suggested_warehouse_product_id = best_candidate.id
                group.match_explanation = (
                    f"Система уверенно выбрала товар «{best_candidate.name}»: "
                    + "; ".join(best_candidate.reasons)
                )
            elif best_candidate.score >= 100 and (
                second_candidate is not None and best_candidate.score - second_candidate.score < 25
            ):
                group.match_status = "conflict"
                group.suggested_warehouse_product_id = None
                group.match_explanation = (
                    f"Найдено несколько похожих товаров. Лучший вариант «{best_candidate.name}» "
                    f"слишком близок к альтернативе «{second_candidate.name}»."
                )
            else:
                group.match_status = "new"
                group.suggested_warehouse_product_id = None
                if best_candidate.score > 0:
                    group.match_explanation = (
                        f"Похожий товар «{best_candidate.name}» найден, но уверенности недостаточно для автосвязи."
                    )
                else:
                    group.match_explanation = "Подходящей связи не найдено, поэтому лучше создать новый товар."

            group.candidates = best_candidates
            enriched_groups.append(group)

        return enriched_groups

    async def _load_all_products(self, client: OzonClient) -> list[dict[str, Any]]:
        all_products_payloads: list[dict[str, Any]] = []
        seen_product_ids: set[int] = set()

        for visibility in ("ALL", "ARCHIVED"):
            last_id: Optional[str] = None

            while True:
                try:
                    data = await client.get_products_list(last_id=last_id, visibility=visibility)
                except HTTPStatusError as exc:
                    if exc.response.status_code == 429:
                        await asyncio.sleep(5)
                        continue
                    raise

                result = data.get("result", {})
                items = result.get("items", [])
                if not items:
                    break

                for item in items:
                    product_id = item.get("product_id")
                    if not product_id or product_id in seen_product_ids:
                        continue
                    seen_product_ids.add(product_id)
                    enriched_item = dict(item)
                    enriched_item["_ozon_visibility"] = visibility
                    all_products_payloads.append(enriched_item)

                last_id = result.get("last_id")
                if not last_id:
                    break

        return all_products_payloads

    async def _load_attribute_snapshot(
        self,
        client: OzonClient,
        product_payloads: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        attributes_payloads: list[dict[str, Any]] = []
        batch_size = 100
        visibility_by_product_id = {
            int(item["product_id"]): str(item.get("_ozon_visibility") or "ALL")
            for item in product_payloads
            if item.get("product_id")
        }

        for index in range(0, len(product_payloads), batch_size):
            batch_payloads = product_payloads[index:index + batch_size]
            product_ids = [item["product_id"] for item in batch_payloads if item.get("product_id")]
            if not product_ids:
                continue

            batch_attributes = await client.get_product_attributes(product_ids)
            for payload in batch_attributes:
                product_id = payload.get("id")
                if product_id:
                    payload["_ozon_visibility"] = visibility_by_product_id.get(int(product_id), "ALL")
            attributes_payloads.extend(batch_attributes)

        return attributes_payloads

    def _build_grouped_products(
        self,
        attributes_payloads: list[dict[str, Any]],
    ) -> list[StorePreviewProductGroup]:
        if not attributes_payloads:
            return []

        response: list[StorePreviewProductGroup] = []
        for payload in build_ozon_product_groups(attributes_payloads):
            color_map: dict[str, list[StorePreviewVariant]] = defaultdict(list)
            for variant_payload in payload["variants"]:
                variant = StorePreviewVariant(
                    offer_id=variant_payload["offer_id"],
                    pack_size=variant_payload["pack_size"],
                    color=variant_payload["color"],
                    size=variant_payload["size"],
                )
                color_map[variant.color].append(variant)

            color_groups: list[StorePreviewColorGroup] = []
            for color, color_variants in sorted(
                color_map.items(),
                key=lambda item: item[0].lower(),
            ):
                size_map: dict[str, list[StorePreviewVariant]] = defaultdict(list)
                for variant in color_variants:
                    size_map[variant.size].append(variant)

                size_groups: list[StorePreviewSizeGroup] = []
                for size, size_variants in sorted(size_map.items(), key=lambda item: get_size_order(item[0])):
                    sorted_variants = sorted(size_variants, key=lambda item: (item.pack_size, item.offer_id))
                    size_groups.append(StorePreviewSizeGroup(size=size, variants=sorted_variants))

                color_groups.append(StorePreviewColorGroup(color=color, sizes=size_groups))

            response.append(
                StorePreviewProductGroup(
                    group_key=serialize_group_key(payload["key"]),
                    base_name=payload["base_name"],
                    product_name=payload["product_name"],
                    image_url=payload["image_url"],
                    total_variants=len(payload["variants"]),
                    colors=color_groups,
                )
            )

        return sorted(response, key=lambda item: item.base_name.lower())
