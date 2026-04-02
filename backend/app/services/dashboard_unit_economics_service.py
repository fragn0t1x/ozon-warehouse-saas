from __future__ import annotations

from datetime import date, datetime, timezone
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.product import Product
from app.models.store import Store
from app.models.store_month_finance import StoreMonthFinance, StoreMonthOfferFinance
from app.models.variant import Variant
from app.services.economics_history_service import EconomicsHistoryService
from app.services.ozon.client import OzonClient
from app.services.ozon.finance_snapshot_service import OzonFinanceSnapshotService
from app.services.ozon.report_snapshot_service import OzonReportSnapshotService
from app.services.unit_economics_math import calculate_tax_amount, revenue_net_of_vat
from app.utils.encryption import decrypt_api_key
from app.utils.redis_cache import cache_get_json

logger = logging.getLogger(__name__)


class DashboardUnitEconomicsService:
    @staticmethod
    def _parse_money_value(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(str(value).replace(" ", "").replace(",", "."))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _price_index_label(color_index: str | None) -> str | None:
        mapping = {
            "COLOR_INDEX_SUPER": "Супервыгодный",
            "COLOR_INDEX_GREEN": "Выгодный",
            "COLOR_INDEX_YELLOW": "Умеренный",
            "COLOR_INDEX_RED": "Невыгодный",
            "COLOR_INDEX_WITHOUT_INDEX": "Без индекса",
        }
        return mapping.get(str(color_index or "").strip())

    @classmethod
    def _closed_month_adjustment_total(
        cls,
        finance_snapshot: dict[str, Any],
        *,
        period: str | None,
    ) -> float:
        if not period:
            return 0.0
        adjustments = finance_snapshot.get("adjustments") or {}
        compensation = 0.0
        decompensation = 0.0
        for item in adjustments.get("compensation_reports") or []:
            if str(item.get("month") or "") == period:
                compensation += float(item.get("amount_total") or 0)
        for item in adjustments.get("decompensation_reports") or []:
            if str(item.get("month") or "") == period:
                decompensation += float(item.get("amount_total") or 0)
        return round(compensation - decompensation, 2)

    @staticmethod
    def _parse_period_value(value: str | None) -> tuple[int, int] | None:
        if not value:
            return None
        parts = str(value).strip().split("-")
        if len(parts) != 2:
            return None
        try:
            year = int(parts[0])
            month = int(parts[1])
        except ValueError:
            return None
        if month < 1 or month > 12:
            return None
        return (year, month)

    @classmethod
    def _choose_collection_basis(cls, basis_counts: dict[str, int]) -> str:
        realization_count = int(basis_counts.get("realization_closed_month") or 0)
        orders_count = int(basis_counts.get("orders_recent") or 0)
        if realization_count and orders_count:
            return "mixed"
        if realization_count:
            return "realization_closed_month"
        if orders_count:
            return "orders_recent"
        return "unavailable"

    @classmethod
    def _build_collection_metadata(
        cls,
        *,
        basis_counts: dict[str, int],
        realization_periods: set[str],
        sales_period_days: int,
    ) -> dict[str, Any]:
        basis = cls._choose_collection_basis(basis_counts)

        if basis == "realization_closed_month":
            period = next(iter(sorted(realization_periods)), None)
            return {
                "basis": basis,
                "basis_label": "Точная реализация закрытого месяца",
                "period_label": period or "Закрытый месяц Ozon",
                "revenue_label": "Чистая реализация",
                "units_label": "Нетто, шт.",
                "profit_label": "Чистая прибыль",
                "profit_hint": "Считаем по точной реализации Ozon: фактическая цена продажи за период, себестоимость, комиссия из отчета реализации и налоговая модель магазина.",
                "details_note": "Цена продажи по одному SKU может меняться внутри месяца. В этом режиме берется фактическая реализация Ozon, а не одна фиксированная цена.",
            }

        if basis == "mixed":
            period = next(iter(sorted(realization_periods)), None)
            return {
                "basis": basis,
                "basis_label": "Смешанная основа",
                "period_label": period or f"Заказы за {sales_period_days} дней + закрытый месяц",
                "revenue_label": "Основа расчета",
                "units_label": "Шт.",
                "profit_label": "Чистая прибыль",
                "profit_hint": "Часть магазинов считается по точной реализации закрытого месяца, часть — по свежим заказам и распределенным расходам с учетом налоговой модели магазина.",
                "details_note": "На части магазинов Ozon уже отдал закрытый месяц, на части пока используем свежие заказы.",
            }

        if basis == "orders_recent":
            return {
                "basis": basis,
                "basis_label": "Свежие заказы",
                "period_label": f"Последние {sales_period_days} дней",
                "revenue_label": "Заказано",
                "units_label": "Заказано, шт.",
                "profit_label": "Оценочная чистая прибыль",
                "profit_hint": "Заказы берутся из postings, а расходы Ozon распределяются по доле выручки. Цена по продажам может плавать, поэтому это оценка, а не закрывающий документ.",
                "details_note": "SKU считаются по свежим заказам и фактической выручке Ozon за период, пока закрытый месяц по реализации недоступен.",
            }

        return {
            "basis": basis,
            "basis_label": "Нет данных",
            "period_label": "Период неизвестен",
            "revenue_label": "Выручка",
            "units_label": "Шт.",
            "profit_label": "Чистая прибыль",
            "profit_hint": "Данных для построения экономики пока нет.",
            "details_note": "Ждем свежие snapshot-ы Ozon.",
        }

    async def _load_variant_costs(
        self,
        db: AsyncSession,
        user_id: int,
        *,
        warehouse_mode: str = "shared",
        as_of: date | None = None,
    ) -> dict[tuple[int, str], float]:
        history_costs = await EconomicsHistoryService(db).load_variant_costs_for_date(
            user_id=user_id,
            as_of=as_of or datetime.now(timezone.utc).date(),
        )
        if history_costs:
            return {
                key: float(item.unit_cost)
                for key, item in history_costs.items()
                if item.unit_cost is not None
            }

        stmt = (
            select(Store.id, Product.warehouse_product_id, Variant)
            .join(Product, Product.store_id == Store.id)
            .join(Variant, Variant.product_id == Product.id)
            .options(selectinload(Variant.attributes))
            .where(Store.user_id == user_id)
        )
        result = await db.execute(stmt)
        costs: dict[tuple[int, str], float] = {}
        for store_id, warehouse_product_id, variant in result.all():
            offer_id = variant.offer_id
            if not offer_id:
                continue

            store_key = (int(store_id), str(offer_id))
            if variant.unit_cost is not None:
                costs[store_key] = float(variant.unit_cost)

        return costs

    @staticmethod
    def _build_finance_pool(finance_snapshot: dict[str, Any]) -> dict[str, float]:
        transactions_recent = finance_snapshot.get("transactions_recent") or {}
        transaction_totals = transactions_recent.get("totals") or {}
        service_buckets = transactions_recent.get("service_buckets") or {}

        if transactions_recent.get("available") and transaction_totals:
            marketing_amount = float(service_buckets.get("marketing") or 0)
            return {
                "commission": float(transaction_totals.get("sale_commission") or 0),
                "services": float(transaction_totals.get("services_amount") or 0) - marketing_amount,
                "logistics": float(transaction_totals.get("processing_and_delivery") or 0)
                + float(transaction_totals.get("refunds_and_cancellations") or 0),
                "marketing": marketing_amount,
                "compensation": float(transaction_totals.get("compensation_amount") or 0),
                "other": float(transaction_totals.get("others_amount") or 0),
            }

        finance_summary = finance_snapshot.get("summary") or {}
        return {
            "commission": float(finance_summary.get("commission_amount") or 0),
            "services": float(finance_summary.get("services_amount") or 0),
            "logistics": float(finance_summary.get("logistics_amount") or 0),
            "marketing": 0.0,
            "compensation": float(finance_summary.get("compensation_amount") or 0)
            - float(finance_summary.get("decompensation_amount") or 0),
            "other": 0.0,
        }

    async def _load_current_product_info_map(
        self,
        store: dict[str, Any],
        *,
        offer_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
        normalized_offer_ids = [str(offer_id).strip() for offer_id in offer_ids if str(offer_id).strip()]
        client_id = str(store.get("client_id") or "").strip()
        encrypted_key = str(store.get("api_key_encrypted") or "").strip()
        if not normalized_offer_ids or not client_id or not encrypted_key:
            return {}

        client = OzonClient(
            client_id,
            decrypt_api_key(encrypted_key),
            store_name=str(store.get("name") or "Магазин"),
            emit_notifications=False,
        )
        items_by_offer_id: dict[str, dict[str, Any]] = {}
        try:
            chunk_size = 1000
            for chunk_start in range(0, len(normalized_offer_ids), chunk_size):
                chunk = normalized_offer_ids[chunk_start:chunk_start + chunk_size]
                items = await client.get_product_info_list(offer_ids=chunk)
                for item in items:
                    offer_id = str(item.get("offer_id") or "").strip()
                    if offer_id:
                        items_by_offer_id[offer_id] = item
        except Exception as exc:
            logger.warning(
                "Failed to load current Ozon product info for store %s (%s): %s",
                store.get("id"),
                store.get("name"),
                exc,
            )
        finally:
            await client.close()

        return items_by_offer_id

    async def _load_historical_offer_profiles(
        self,
        db: AsyncSession,
        *,
        rows: list[dict[str, Any]],
    ) -> dict[tuple[int, str], dict[str, Any]]:
        store_ids = sorted({int(row.get("store_id") or 0) for row in rows if int(row.get("store_id") or 0) > 0})
        offer_ids = sorted({str(row.get("offer_id") or "").strip() for row in rows if str(row.get("offer_id") or "").strip()})
        if not store_ids or not offer_ids:
            return {}

        stmt = (
            select(StoreMonthOfferFinance, StoreMonthFinance.status)
            .join(
                StoreMonthFinance,
                StoreMonthFinance.id == StoreMonthOfferFinance.store_month_finance_id,
            )
            .where(
                StoreMonthFinance.status == "ready",
                StoreMonthOfferFinance.store_id.in_(store_ids),
                StoreMonthOfferFinance.offer_id.in_(offer_ids),
            )
            .order_by(
                StoreMonthOfferFinance.store_id.asc(),
                StoreMonthOfferFinance.offer_id.asc(),
                StoreMonthOfferFinance.month.desc(),
            )
        )
        result = await db.execute(stmt)

        grouped_rows: dict[tuple[int, str], list[StoreMonthOfferFinance]] = {}
        for offer_row, _status in result.all():
            key = (int(offer_row.store_id), str(offer_row.offer_id))
            bucket = grouped_rows.setdefault(key, [])
            if len(bucket) >= 3:
                continue
            sold_units = int(offer_row.sold_units or 0)
            net_units = int(offer_row.net_units or 0)
            if sold_units <= 0 and net_units <= 0:
                continue
            bucket.append(offer_row)

        profiles: dict[tuple[int, str], dict[str, Any]] = {}
        for key, bucket in grouped_rows.items():
            sold_units_total = float(sum(max(int(item.sold_units or 0), 0) for item in bucket))
            returned_units_total = float(sum(max(int(item.returned_units or 0), 0) for item in bucket))
            per_sale_denominator = sold_units_total if sold_units_total > 0 else float(sum(max(int(item.net_units or 0), 0) for item in bucket))
            if per_sale_denominator <= 0:
                continue

            profiles[key] = {
                "months_count": len(bucket),
                "sold_units_total": int(sold_units_total),
                "returned_units_total": int(returned_units_total),
                "return_rate": round((returned_units_total / sold_units_total), 4) if sold_units_total > 0 else 0.0,
                "services_per_unit": round(
                    sum(abs(float(item.ozon_services or 0)) for item in bucket) / per_sale_denominator,
                    2,
                ),
                "acquiring_per_unit": round(
                    sum(abs(float(item.ozon_acquiring or 0)) for item in bucket) / per_sale_denominator,
                    2,
                ),
                "other_per_unit": round(
                    sum(abs(float(item.ozon_other_expenses or 0)) for item in bucket) / per_sale_denominator,
                    2,
                ),
            }

        return profiles

    @classmethod
    def _apply_current_profitability(
        cls,
        row: dict[str, Any],
        *,
        item: dict[str, Any],
    ) -> None:
        current_price_gross = cls._parse_money_value(item.get("price"))
        old_price_gross = cls._parse_money_value(item.get("old_price"))
        min_price_gross = cls._parse_money_value(item.get("min_price"))

        price_indexes = item.get("price_indexes") or {}
        commissions = item.get("commissions") or []
        selected_commission = next(
            (
                commission
                for commission in commissions
                if str(commission.get("sale_schema") or "").strip().upper() in {"FBO", "SDS", ""}
            ),
            commissions[0] if commissions else {},
        ) or {}

        row["current_price_gross"] = current_price_gross
        row["current_old_price_gross"] = old_price_gross
        row["current_min_price_gross"] = min_price_gross
        row["current_price_index_color"] = str(price_indexes.get("color_index") or "").strip() or None
        row["current_price_index_label"] = cls._price_index_label(row.get("current_price_index_color"))
        row["current_ozon_minimal_price_gross"] = cls._parse_money_value(
            ((price_indexes.get("ozon_index_data") or {}).get("minimal_price"))
        )
        row["current_external_minimal_price_gross"] = cls._parse_money_value(
            ((price_indexes.get("external_index_data") or {}).get("minimal_price"))
        )
        row["current_commission_percent"] = cls._parse_money_value(selected_commission.get("percent"))
        row["current_delivery_amount"] = cls._parse_money_value(selected_commission.get("delivery_amount"))
        row["current_return_amount"] = cls._parse_money_value(selected_commission.get("return_amount"))
        row["current_commission_value"] = cls._parse_money_value(selected_commission.get("value"))

        if current_price_gross is None:
            row["current_profitability_available"] = False
            return

        units = float(row.get("units") or 0)
        unit_divisor = units if units > 0 else 1.0
        revenue_net = round(revenue_net_of_vat(current_price_gross, row.get("vat_mode")), 2)
        unit_cost = float(row.get("unit_cost") or 0)
        gross_profit = round(revenue_net - unit_cost, 2)

        commission_value = row.get("current_commission_value")
        commission_percent = row.get("current_commission_percent")
        if commission_value is not None:
            allocated_commission = -abs(float(commission_value))
        elif commission_percent is not None:
            allocated_commission = round(-abs(current_price_gross * float(commission_percent) / 100.0), 2)
        else:
            allocated_commission = 0.0

        delivery_amount = row.get("current_delivery_amount")
        allocated_logistics = -abs(float(delivery_amount)) if delivery_amount is not None else 0.0
        allocated_marketing = -abs(float(row.get("historical_marketing_per_unit") or 0.0))
        allocated_services = -abs(float(row.get("historical_services_per_unit") or 0.0))
        allocated_acquiring = -abs(float(row.get("historical_acquiring_per_unit") or 0.0))
        allocated_other = -abs(float(row.get("historical_other_per_unit") or 0.0))
        return_rate = max(float(row.get("historical_return_rate") or 0.0), 0.0)
        return_amount = row.get("current_return_amount")
        return_reserve = round(abs(float(return_amount or 0.0)) * return_rate, 2) if return_amount is not None else 0.0

        profit_before_tax = round(
            gross_profit
            + allocated_commission
            + allocated_logistics
            + allocated_services
            + allocated_acquiring
            + allocated_other
            + allocated_marketing
            - return_reserve
            ,
            2,
        )
        tax_amount = round(
            calculate_tax_amount(
                revenue_net=revenue_net,
                profit_before_tax=profit_before_tax,
                tax_mode=row.get("tax_mode"),
                tax_rate=row.get("tax_rate"),
            ),
            2,
        )
        estimated_net_profit = round(profit_before_tax - tax_amount, 2)

        row["current_revenue_net_of_vat"] = revenue_net
        row["current_gross_profit_before_ozon"] = gross_profit
        row["current_allocated_commission"] = round(allocated_commission, 2)
        row["current_allocated_services"] = round(allocated_services, 2)
        row["current_allocated_logistics"] = round(allocated_logistics, 2)
        row["current_allocated_marketing"] = round(allocated_marketing, 2)
        row["current_allocated_compensation"] = round(allocated_acquiring, 2)
        row["current_allocated_other"] = round(allocated_other, 2)
        row["current_return_rate"] = round(return_rate, 4)
        row["current_return_reserve"] = round(return_reserve, 2)
        row["current_profit_before_tax"] = profit_before_tax
        row["current_tax_amount"] = tax_amount
        row["current_estimated_net_profit"] = estimated_net_profit
        row["current_margin_ratio"] = round((estimated_net_profit / revenue_net), 4) if revenue_net else 0.0
        row["current_profitability_available"] = True

    async def _enrich_rows_with_current_product_info(
        self,
        db: AsyncSession,
        rows: list[dict[str, Any]],
        *,
        stores: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        stores_by_id = {int(store["id"]): store for store in stores if store.get("id") is not None}
        historical_profiles = await self._load_historical_offer_profiles(db, rows=rows)
        offer_ids_by_store: dict[int, list[str]] = {}
        for row in rows:
            store_id = int(row.get("store_id") or 0)
            offer_id = str(row.get("offer_id") or "").strip()
            if not store_id or not offer_id:
                continue
            offer_ids_by_store.setdefault(store_id, []).append(offer_id)

        current_info_by_store: dict[int, dict[str, dict[str, Any]]] = {}
        for current_store_id, offer_ids in offer_ids_by_store.items():
            store = stores_by_id.get(current_store_id)
            if not store:
                continue
            unique_offer_ids = list(dict.fromkeys(offer_ids))
            current_info_by_store[current_store_id] = await self._load_current_product_info_map(
                store,
                offer_ids=unique_offer_ids,
            )

        for row in rows:
            store_id = int(row.get("store_id") or 0)
            offer_id = str(row.get("offer_id") or "").strip()
            units = float(row.get("units") or 0)
            unit_divisor = units if units > 0 else 1.0
            historical_profile = historical_profiles.get((store_id, offer_id)) or {}
            fallback_return_rate = 0.0
            sold_units = float(row.get("sold_units") or 0)
            returned_units = float(row.get("returned_units") or 0)
            if sold_units > 0:
                fallback_return_rate = round(returned_units / sold_units, 4)
            row["historical_services_per_unit"] = float(historical_profile.get("services_per_unit") or 0.0)
            row["historical_acquiring_per_unit"] = float(historical_profile.get("acquiring_per_unit") or 0.0)
            row["historical_other_per_unit"] = float(historical_profile.get("other_per_unit") or 0.0)
            row["historical_marketing_per_unit"] = round(abs(float(row.get("allocated_marketing") or 0.0)) / unit_divisor, 2)
            row["historical_return_rate"] = float(historical_profile.get("return_rate") or fallback_return_rate or 0.0)
            row["historical_profile_months"] = int(historical_profile.get("months_count") or 0)
            row["historical_profile_sales_units"] = int(historical_profile.get("sold_units_total") or 0)
            item = ((current_info_by_store.get(store_id) or {}).get(offer_id))
            if not item:
                row["current_profitability_available"] = False
                continue
            cls_item = dict(item)
            self._apply_current_profitability(row, item=cls_item)

        return rows

    async def _collect_rows(
        self,
        *,
        db: AsyncSession,
        user_id: int,
        stores: list[dict[str, Any]],
        store_id: int | None = None,
        warehouse_mode: str = "shared",
    ) -> dict[str, Any]:
        variant_costs = await self._load_variant_costs(db, user_id, warehouse_mode=warehouse_mode)

        totals = {
            "tracked_units": 0.0,
            "tracked_revenue": 0.0,
            "tracked_revenue_net_of_vat": 0.0,
            "tracked_cogs": 0.0,
            "gross_profit_before_ozon": 0.0,
            "allocated_commission": 0.0,
            "allocated_services": 0.0,
            "allocated_logistics": 0.0,
            "allocated_marketing": 0.0,
            "allocated_compensation": 0.0,
            "allocated_other": 0.0,
            "profit_before_tax": 0.0,
            "tax_amount": 0.0,
            "estimated_net_profit": 0.0,
        }
        offers_total = 0
        offers_with_cost = 0
        revenue_with_cost = 0.0
        rows: list[dict[str, Any]] = []
        basis_counts = {
            "realization_closed_month": 0,
            "orders_recent": 0,
        }
        realization_periods: set[str] = set()
        sales_period_days = 30

        for store in stores:
            current_store_id = int(store["id"])
            if store_id is not None and current_store_id != store_id:
                continue

            client_id = str(store.get("client_id") or "").strip()
            if not client_id:
                continue
            vat_mode = str(store.get("economics_vat_mode") or "none")
            tax_mode = str(store.get("economics_tax_mode") or "usn_income_expenses")
            tax_rate = float(store.get("economics_tax_rate") or 15.0)

            sales_snapshot = await cache_get_json(
                OzonReportSnapshotService.cache_key_for(client_id, "postings")
            )
            finance_snapshot = await cache_get_json(
                OzonFinanceSnapshotService.cache_key_for(client_id)
            )
            if not sales_snapshot or not finance_snapshot:
                continue

            sales_summary = ((sales_snapshot.get("preview") or {}).get("summary") or {})
            sales_period_days = int((((sales_snapshot.get("preview") or {}).get("period") or {}).get("days")) or sales_period_days)
            finance_pool = self._build_finance_pool(finance_snapshot)
            offer_stats = sales_summary.get("offer_stats") or sales_summary.get("top_offers") or []
            total_store_revenue = float(sales_summary.get("total_revenue") or 0)
            realization_snapshot = finance_snapshot.get("realization_closed_month") or {}
            realization_items = list(realization_snapshot.get("items") or [])
            store_cost_offer_ids = {
                offer_id
                for (cost_store_id, offer_id), _cost in variant_costs.items()
                if int(cost_store_id) == current_store_id and str(offer_id).strip()
            }
            realization_offer_ids = {
                str(item.get("offer_id") or "").strip()
                for item in realization_items
                if str(item.get("offer_id") or "").strip()
            }
            realization_matches_store_offers = bool(realization_offer_ids & store_cost_offer_ids)

            if realization_snapshot.get("available") and realization_items and realization_matches_store_offers:
                basis_counts["realization_closed_month"] += 1
                period = str(realization_snapshot.get("period") or "").strip()
                if period:
                    realization_periods.add(period)
                closed_month_adjustment_total = self._closed_month_adjustment_total(
                    finance_snapshot,
                    period=period or None,
                )
                realization_net_revenue = float(realization_snapshot.get("net_amount") or 0)
                if realization_net_revenue <= 0:
                    realization_net_revenue = round(
                        sum(float(item.get("net_amount") or 0) for item in realization_items),
                        2,
                    )
                closed_month_cashflow = finance_snapshot.get("closed_month_cashflow") or {}
                closed_month_services_total = float(closed_month_cashflow.get("services_amount") or 0)
                closed_month_logistics_total = float(closed_month_cashflow.get("logistics_amount") or 0)
                if not closed_month_cashflow and period:
                    period_prefix = f"{period}-"
                    for period_item in finance_snapshot.get("periods") or []:
                        period_begin = str(period_item.get("period_begin") or "")
                        if period_begin.startswith(period_prefix):
                            closed_month_services_total += float(period_item.get("services_amount") or 0)
                            closed_month_logistics_total += float(period_item.get("logistics_amount") or 0)
                closed_month_services_total = round(closed_month_services_total, 2)
                closed_month_logistics_total = round(closed_month_logistics_total, 2)

                for item in realization_items:
                    offer_id = str(item.get("offer_id") or "")
                    sold_units = float(item.get("sold_units") or 0)
                    returned_units = float(item.get("returned_units") or 0)
                    net_units = float(item.get("net_units") or 0)
                    net_amount = float(item.get("net_amount") or 0)
                    net_total = float(item.get("net_total") or 0)
                    title = str(item.get("title") or "")

                    if not offer_id:
                        continue
                    if sold_units <= 0 and returned_units <= 0 and net_amount == 0 and net_total == 0:
                        continue

                    offers_total += 1
                    cost = variant_costs.get((current_store_id, offer_id))
                    if cost is None:
                        continue

                    offers_with_cost += 1
                    revenue_with_cost += net_amount
                    cogs_units = max(net_units, 0)
                    cogs = round(cost * cogs_units, 2)
                    revenue_ex_vat = round(revenue_net_of_vat(net_amount, vat_mode), 2)
                    gross_profit = round(revenue_ex_vat - cogs, 2)
                    realization_commission_total = round(float(item.get("net_fee") or 0), 2)
                    realization_incentives_total = round(float(item.get("net_incentives") or 0), 2)
                    revenue_share = (net_amount / realization_net_revenue) if realization_net_revenue > 0 else 0.0
                    allocated_adjustment = round(closed_month_adjustment_total * revenue_share, 2)
                    allocated_commission = round(-realization_commission_total, 2)
                    allocated_services = round(closed_month_services_total * revenue_share, 2)
                    allocated_logistics = round(closed_month_logistics_total * revenue_share, 2)
                    profit_before_tax = round(
                        gross_profit
                        + realization_incentives_total
                        + allocated_commission
                        + allocated_services
                        + allocated_logistics
                        + allocated_adjustment,
                        2,
                    )
                    tax_amount = round(
                        calculate_tax_amount(
                            revenue_net=revenue_ex_vat,
                            profit_before_tax=profit_before_tax,
                            tax_mode=tax_mode,
                            tax_rate=tax_rate,
                        ),
                        2,
                    )
                    estimated_net_profit = round(profit_before_tax - tax_amount, 2)
                    average_sale_price_gross = round(
                        (float(item.get("sold_amount") or 0) / sold_units)
                        if sold_units > 0
                        else ((net_amount / net_units) if net_units else 0.0),
                        2,
                    )

                    row = {
                        "store_id": current_store_id,
                        "store_name": store.get("name") or "Магазин",
                        "offer_id": offer_id,
                        "title": title,
                        "sold_units": round(sold_units, 2),
                        "returned_units": round(returned_units, 2),
                        "units": round(net_units, 2),
                        "revenue": round(net_amount, 2),
                        "revenue_net_of_vat": revenue_ex_vat,
                        "average_sale_price_gross": average_sale_price_gross,
                        "unit_cost": round(cost, 2),
                        "cogs": cogs,
                        "gross_profit_before_ozon": gross_profit,
                        "profit_before_tax": profit_before_tax,
                        "tax_amount": tax_amount,
                        "estimated_net_profit": estimated_net_profit,
                        "allocated_commission": allocated_commission,
                        "allocated_services": allocated_services,
                        "allocated_logistics": allocated_logistics,
                        "allocated_marketing": 0.0,
                        "allocated_compensation": allocated_adjustment,
                        "allocated_other": 0.0,
                        "margin_ratio": round((estimated_net_profit / revenue_ex_vat), 4) if revenue_ex_vat else 0.0,
                        "basis": "realization_closed_month",
                        "vat_mode": vat_mode,
                        "tax_mode": tax_mode,
                        "tax_rate": tax_rate,
                    }
                    rows.append(row)

                    totals["tracked_units"] += net_units
                    totals["tracked_revenue"] += net_amount
                    totals["tracked_revenue_net_of_vat"] += revenue_ex_vat
                    totals["tracked_cogs"] += cogs
                    totals["gross_profit_before_ozon"] += gross_profit
                    totals["allocated_commission"] += allocated_commission
                    totals["allocated_services"] += allocated_services
                    totals["allocated_logistics"] += allocated_logistics
                    totals["allocated_compensation"] += allocated_adjustment
                    totals["profit_before_tax"] += profit_before_tax
                    totals["tax_amount"] += tax_amount
                    totals["estimated_net_profit"] += estimated_net_profit
                continue

            if total_store_revenue <= 0:
                continue

            basis_counts["orders_recent"] += 1

            for offer in offer_stats:
                offer_id = str(offer.get("offer_id") or "")
                revenue = float(offer.get("revenue") or 0)
                units = float(offer.get("units") or 0)
                if not offer_id or revenue <= 0 or units <= 0:
                    continue

                offers_total += 1
                cost = variant_costs.get((current_store_id, offer_id))
                if cost is None:
                    continue

                offers_with_cost += 1
                revenue_with_cost += revenue
                cogs = round(cost * units, 2)
                revenue_share = revenue / total_store_revenue if total_store_revenue else 0
                allocated_commission = round(float(finance_pool["commission"]) * revenue_share, 2)
                allocated_services = round(float(finance_pool["services"]) * revenue_share, 2)
                allocated_logistics = round(float(finance_pool["logistics"]) * revenue_share, 2)
                allocated_marketing = round(float(finance_pool.get("marketing") or 0) * revenue_share, 2)
                allocated_compensation = round(float(finance_pool["compensation"]) * revenue_share, 2)
                allocated_other = round(float(finance_pool["other"]) * revenue_share, 2)
                revenue_ex_vat = round(revenue_net_of_vat(revenue, vat_mode), 2)
                gross_profit = round(revenue_ex_vat - cogs, 2)
                profit_before_tax = round(
                    gross_profit
                    + allocated_commission
                    + allocated_services
                    + allocated_logistics
                    + allocated_marketing
                    + allocated_compensation
                    + allocated_other,
                    2,
                )
                tax_amount = round(
                    calculate_tax_amount(
                        revenue_net=revenue_ex_vat,
                        profit_before_tax=profit_before_tax,
                        tax_mode=tax_mode,
                        tax_rate=tax_rate,
                    ),
                    2,
                )
                estimated_net_profit = round(profit_before_tax - tax_amount, 2)

                row = {
                    "store_id": current_store_id,
                    "store_name": store.get("name") or "Магазин",
                    "offer_id": offer_id,
                    "title": offer.get("title") or "",
                    "sold_units": 0.0,
                    "returned_units": 0.0,
                    "units": units,
                    "revenue": round(revenue, 2),
                    "revenue_net_of_vat": revenue_ex_vat,
                    "average_sale_price_gross": round(revenue / units, 2) if units else 0.0,
                    "unit_cost": round(cost, 2),
                    "cogs": cogs,
                    "gross_profit_before_ozon": gross_profit,
                    "profit_before_tax": profit_before_tax,
                    "tax_amount": tax_amount,
                    "estimated_net_profit": estimated_net_profit,
                    "allocated_commission": allocated_commission,
                    "allocated_services": allocated_services,
                    "allocated_logistics": allocated_logistics,
                    "allocated_marketing": allocated_marketing,
                    "allocated_compensation": allocated_compensation,
                    "allocated_other": allocated_other,
                    "margin_ratio": round((estimated_net_profit / revenue_ex_vat), 4) if revenue_ex_vat else 0.0,
                    "basis": "orders_recent",
                    "vat_mode": vat_mode,
                    "tax_mode": tax_mode,
                    "tax_rate": tax_rate,
                }
                rows.append(row)

                totals["tracked_units"] += units
                totals["tracked_revenue"] += revenue
                totals["tracked_revenue_net_of_vat"] += revenue_ex_vat
                totals["tracked_cogs"] += cogs
                totals["gross_profit_before_ozon"] += gross_profit
                totals["allocated_commission"] += allocated_commission
                totals["allocated_services"] += allocated_services
                totals["allocated_logistics"] += allocated_logistics
                totals["allocated_marketing"] += allocated_marketing
                totals["allocated_compensation"] += allocated_compensation
                totals["allocated_other"] += allocated_other
                totals["profit_before_tax"] += profit_before_tax
                totals["tax_amount"] += tax_amount
                totals["estimated_net_profit"] += estimated_net_profit

        return {
            "rows": rows,
            "offers_total": offers_total,
            "offers_with_cost": offers_with_cost,
            "revenue_with_cost": revenue_with_cost,
            "totals": totals,
            "metadata": self._build_collection_metadata(
                basis_counts=basis_counts,
                realization_periods=realization_periods,
                sales_period_days=sales_period_days,
            ),
        }

    def _build_summary_from_collection(self, collection: dict[str, Any]) -> dict[str, Any]:
        rows = list(collection["rows"])
        totals = dict(collection["totals"])
        rows.sort(key=lambda item: item["estimated_net_profit"], reverse=True)
        worst_rows = sorted(rows, key=lambda item: item["estimated_net_profit"])[:8]

        return {
            "source": "estimated" if rows else "unavailable",
            **dict(collection.get("metadata") or {}),
            "offers_total": collection["offers_total"],
            "offers_with_cost": collection["offers_with_cost"],
            "cost_coverage_ratio": round((collection["offers_with_cost"] / collection["offers_total"]), 4)
            if collection["offers_total"]
            else 0,
            "revenue_coverage_ratio": round((collection["revenue_with_cost"] / totals["tracked_revenue"]), 4)
            if totals["tracked_revenue"]
            else 0,
            **{key: round(value, 2) for key, value in totals.items()},
            "top_profitable_offers": rows[:8],
            "top_loss_offers": worst_rows,
        }

    async def build_summary(
        self,
        *,
        db: AsyncSession,
        user_id: int,
        stores: list[dict[str, Any]],
        warehouse_mode: str = "shared",
    ) -> dict[str, Any]:
        collection = await self._collect_rows(
            db=db,
            user_id=user_id,
            stores=stores,
            warehouse_mode=warehouse_mode,
        )
        return self._build_summary_from_collection(collection)

    async def build_report(
        self,
        *,
        db: AsyncSession,
        user_id: int,
        stores: list[dict[str, Any]],
        store_id: int | None = None,
        query: str = "",
        profitability: str = "all",
        limit: int = 100,
        warehouse_mode: str = "shared",
    ) -> dict[str, Any]:
        collection = await self._collect_rows(
            db=db,
            user_id=user_id,
            stores=stores,
            store_id=store_id,
            warehouse_mode=warehouse_mode,
        )
        summary = self._build_summary_from_collection(collection)

        rows = list(collection["rows"])
        rows = await self._enrich_rows_with_current_product_info(db, rows, stores=stores)
        normalized_query = query.strip().lower()
        if normalized_query:
            rows = [
                row
                for row in rows
                if normalized_query in row["offer_id"].lower()
                or normalized_query in row["title"].lower()
                or normalized_query in row["store_name"].lower()
            ]

        if profitability == "loss":
            rows = [row for row in rows if row["estimated_net_profit"] < 0]
            rows.sort(key=lambda item: item["estimated_net_profit"])
        elif profitability == "profit":
            rows = [row for row in rows if row["estimated_net_profit"] >= 0]
            rows.sort(key=lambda item: item["estimated_net_profit"], reverse=True)
        else:
            rows.sort(key=lambda item: item["estimated_net_profit"])

        filtered_totals = {
            "rows_count": len(rows),
            "units": round(sum(float(row["units"]) for row in rows), 2),
            "revenue": round(sum(float(row["revenue"]) for row in rows), 2),
            "revenue_net_of_vat": round(sum(float(row.get("revenue_net_of_vat") or 0) for row in rows), 2),
            "cogs": round(sum(float(row["cogs"]) for row in rows), 2),
            "gross_profit_before_ozon": round(sum(float(row["gross_profit_before_ozon"]) for row in rows), 2),
            "profit_before_tax": round(sum(float(row.get("profit_before_tax") or 0) for row in rows), 2),
            "tax_amount": round(sum(float(row.get("tax_amount") or 0) for row in rows), 2),
            "estimated_net_profit": round(sum(float(row["estimated_net_profit"]) for row in rows), 2),
        }

        return {
            "summary": summary,
            "filters": {
                "store_id": store_id,
                "query": query,
                "profitability": profitability,
            },
            "filtered_totals": filtered_totals,
            "rows": rows[:limit],
            "rows_total": len(rows),
        }

    async def build_live_rows(
        self,
        *,
        db: AsyncSession,
        user_id: int,
        stores: list[dict[str, Any]],
        store_id: int | None = None,
        warehouse_mode: str = "shared",
    ) -> list[dict[str, Any]]:
        collection = await self._collect_rows(
            db=db,
            user_id=user_id,
            stores=stores,
            store_id=store_id,
            warehouse_mode=warehouse_mode,
        )
        rows = list(collection["rows"])
        rows = await self._enrich_rows_with_current_product_info(db, rows, stores=stores)
        rows.sort(key=lambda item: float(item.get("current_estimated_net_profit") or item.get("estimated_net_profit") or 0))
        return rows
