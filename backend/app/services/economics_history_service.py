from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterable

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.product import Product
from app.models.store import Store
from app.models.store_economics_history import StoreEconomicsHistory
from app.models.store_month_finance import StoreMonthFinance
from app.models.variant import Variant
from app.models.variant_cost_history import VariantCostHistory
from app.models.variant_attribute import VariantAttribute
from app.services.product_grouping import normalize_color


@dataclass
class StoreEconomicsSnapshot:
    vat_mode: str
    tax_mode: str
    tax_rate: float
    effective_from: date | None


@dataclass
class VariantCostSnapshot:
    unit_cost: float | None
    effective_from: date | None


class EconomicsHistoryService:
    def __init__(self, db: AsyncSession):
        self.db = db

    @staticmethod
    def _normalize_effective_from(value: date | datetime | str | None) -> date:
        if value is None:
            return datetime.now(timezone.utc).date()
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value).strip())

    @staticmethod
    def _variant_identity(
        *,
        warehouse_product_id: int | None,
        pack_size: int | None,
        attributes: dict[str, str],
    ) -> tuple[int | None, str, str, int]:
        color = normalize_color(attributes.get("Цвет", "без цвета")).strip().lower()
        size = str(attributes.get("Размер", "")).strip().lower()
        return (
            int(warehouse_product_id) if warehouse_product_id is not None else None,
            color,
            size,
            int(pack_size or 1),
        )

    async def ensure_store_history_entry(
        self,
        *,
        store: Store,
        effective_from: date | datetime | str | None = None,
        created_by_user_id: int | None = None,
    ) -> StoreEconomicsHistory:
        normalized_effective_from = self._normalize_effective_from(effective_from or store.created_at or None)
        latest_stmt = (
            select(StoreEconomicsHistory)
            .where(StoreEconomicsHistory.store_id == store.id)
            .order_by(StoreEconomicsHistory.effective_from.desc(), StoreEconomicsHistory.id.desc())
        )
        latest = (await self.db.execute(latest_stmt)).scalars().first()
        if latest and latest.effective_from == normalized_effective_from:
            latest.vat_mode = str(store.economics_vat_mode or "none")
            latest.tax_mode = str(store.economics_tax_mode or "usn_income_expenses")
            latest.tax_rate = float(store.economics_tax_rate or 15.0)
            if created_by_user_id is not None:
                latest.created_by_user_id = created_by_user_id
            return latest
        if latest and (
            latest.vat_mode == str(store.economics_vat_mode or "none")
            and latest.tax_mode == str(store.economics_tax_mode or "usn_income_expenses")
            and float(latest.tax_rate or 0) == float(store.economics_tax_rate or 15.0)
            and latest.effective_from == normalized_effective_from
        ):
            return latest

        item = StoreEconomicsHistory(
            store_id=store.id,
            effective_from=normalized_effective_from,
            vat_mode=str(store.economics_vat_mode or "none"),
            tax_mode=str(store.economics_tax_mode or "usn_income_expenses"),
            tax_rate=float(store.economics_tax_rate or 15.0),
            created_by_user_id=created_by_user_id,
        )
        self.db.add(item)
        await self.db.flush()
        return item

    async def get_store_economics_for_date(self, *, store: Store, as_of: date) -> StoreEconomicsSnapshot:
        stmt = (
            select(StoreEconomicsHistory)
            .where(
                StoreEconomicsHistory.store_id == store.id,
                StoreEconomicsHistory.effective_from <= as_of,
            )
            .order_by(StoreEconomicsHistory.effective_from.desc(), StoreEconomicsHistory.id.desc())
        )
        row = (await self.db.execute(stmt)).scalars().first()
        if row is None:
            fallback_stmt = (
                select(StoreEconomicsHistory)
                .where(StoreEconomicsHistory.store_id == store.id)
                .order_by(StoreEconomicsHistory.effective_from.asc(), StoreEconomicsHistory.id.asc())
            )
            row = (await self.db.execute(fallback_stmt)).scalars().first()
        if row is None:
            return StoreEconomicsSnapshot(
                vat_mode=str(store.economics_vat_mode or "none"),
                tax_mode=str(store.economics_tax_mode or "usn_income_expenses"),
                tax_rate=float(store.economics_tax_rate or 15.0),
                effective_from=None,
            )
        return StoreEconomicsSnapshot(
            vat_mode=str(row.vat_mode or "none"),
            tax_mode=str(row.tax_mode or "usn_income_expenses"),
            tax_rate=float(row.tax_rate or 15.0),
            effective_from=row.effective_from,
        )

    async def sync_store_current_economics_from_history(
        self,
        *,
        store: Store,
        as_of: date | None = None,
    ) -> StoreEconomicsSnapshot:
        snapshot = await self.get_store_economics_for_date(
            store=store,
            as_of=as_of or datetime.now(timezone.utc).date(),
        )
        store.economics_vat_mode = snapshot.vat_mode
        store.economics_tax_mode = snapshot.tax_mode
        store.economics_tax_rate = float(snapshot.tax_rate or 0)
        await self.db.flush()
        return snapshot

    async def ensure_variant_cost_history_entries(
        self,
        *,
        variants: Iterable[tuple[Variant, int | None, dict[str, str]]],
        effective_from: date | datetime | str | None = None,
        created_by_user_id: int | None = None,
    ) -> None:
        normalized_effective_from = self._normalize_effective_from(effective_from)
        for variant, warehouse_product_id, attributes in variants:
            latest_stmt = (
                select(VariantCostHistory)
                .where(VariantCostHistory.variant_id == variant.id)
                .order_by(VariantCostHistory.effective_from.desc(), VariantCostHistory.id.desc())
            )
            latest = (await self.db.execute(latest_stmt)).scalars().first()
            current_value = float(variant.unit_cost) if variant.unit_cost is not None else None
            if latest and latest.effective_from == normalized_effective_from:
                latest.unit_cost = current_value
                latest.offer_id = str(variant.offer_id or "")
                latest.store_id = int(variant.product.store_id)
                latest.warehouse_product_id = int(warehouse_product_id) if warehouse_product_id is not None else None
                latest.pack_size = int(variant.pack_size or 1)
                latest.color = normalize_color(attributes.get("Цвет", "без цвета")).strip().lower() or None
                latest.size = str(attributes.get("Размер", "")).strip().lower() or None
                if created_by_user_id is not None:
                    latest.created_by_user_id = created_by_user_id
                continue
            if latest and latest.unit_cost == current_value and latest.effective_from == normalized_effective_from:
                continue

            item = VariantCostHistory(
                variant_id=variant.id,
                store_id=int(variant.product.store_id),
                warehouse_product_id=int(warehouse_product_id) if warehouse_product_id is not None else None,
                offer_id=str(variant.offer_id or ""),
                pack_size=int(variant.pack_size or 1),
                color=normalize_color(attributes.get("Цвет", "без цвета")).strip().lower() or None,
                size=str(attributes.get("Размер", "")).strip().lower() or None,
                unit_cost=current_value,
                effective_from=normalized_effective_from,
                created_by_user_id=created_by_user_id,
            )
            self.db.add(item)
        await self.db.flush()

    async def load_variant_costs_for_date(
        self,
        *,
        user_id: int,
        as_of: date,
    ) -> dict[tuple[int, str], VariantCostSnapshot]:
        stmt = (
            select(Store.id, VariantCostHistory)
            .join(Product, Product.store_id == Store.id)
            .join(Variant, Variant.product_id == Product.id)
            .join(VariantCostHistory, VariantCostHistory.variant_id == Variant.id)
            .where(Store.user_id == user_id)
            .order_by(
                Store.id.asc(),
                VariantCostHistory.offer_id.asc(),
                VariantCostHistory.effective_from.asc(),
                VariantCostHistory.id.asc(),
            )
        )
        result = await self.db.execute(stmt)
        earliest_costs: dict[tuple[int, str], VariantCostSnapshot] = {}
        latest_costs: dict[tuple[int, str], VariantCostSnapshot] = {}
        for store_id, item in result.all():
            key = (int(store_id), str(item.offer_id or ""))
            snapshot = VariantCostSnapshot(
                unit_cost=float(item.unit_cost) if item.unit_cost is not None else None,
                effective_from=item.effective_from,
            )
            earliest_costs.setdefault(key, snapshot)
            if item.effective_from <= as_of:
                latest_costs[key] = snapshot

        costs: dict[tuple[int, str], VariantCostSnapshot] = {}
        for key, snapshot in earliest_costs.items():
            costs[key] = latest_costs.get(key, snapshot)
        return costs

    async def backfill_store_histories(self) -> None:
        stores = (await self.db.execute(select(Store))).scalars().all()
        for store in stores:
            await self.ensure_store_history_entry(
                store=store,
                effective_from=store.created_at.date() if store.created_at else None,
                created_by_user_id=store.user_id,
            )
        await self.db.flush()

    async def backfill_variant_cost_histories(self) -> None:
        stmt = (
            select(Variant, Product.warehouse_product_id)
            .join(Product, Product.id == Variant.product_id)
            .options(selectinload(Variant.product), selectinload(Variant.attributes))
        )
        rows = (await self.db.execute(stmt)).all()
        prepared: list[tuple[Variant, int | None, dict[str, str]]] = []
        for variant, warehouse_product_id in rows:
            attributes = {attr.name: attr.value for attr in variant.attributes}
            prepared.append((variant, warehouse_product_id, attributes))
        await self.ensure_variant_cost_history_entries(
            variants=prepared,
            effective_from=datetime.now(timezone.utc).date(),
            created_by_user_id=None,
        )

    async def unlock_store_months_from_date(self, *, store_id: int, effective_from: date) -> None:
        month_floor = f"{effective_from.year:04d}-{effective_from.month:02d}"
        await self.db.execute(
            update(StoreMonthFinance)
            .where(
                StoreMonthFinance.store_id == store_id,
                StoreMonthFinance.month >= month_floor,
            )
            .values(is_locked=False)
        )
        await self.db.flush()
