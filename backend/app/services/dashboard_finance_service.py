from __future__ import annotations

from datetime import date, datetime
from typing import Any

from app.services.ozon.finance_snapshot_service import OzonFinanceSnapshotService
from app.utils.redis_cache import cache_get_json


class DashboardFinanceService:
    @staticmethod
    def _parse_date(value: str | None) -> date | None:
        dt = DashboardFinanceService._parse_dt(value)
        return dt.date() if dt is not None else None

    @staticmethod
    def _sum_period_rows_for_month(
        periods: list[dict[str, Any]],
        *,
        year: int,
        month: int,
    ) -> dict[str, float]:
        totals = {
            "orders_amount": 0.0,
            "returns_amount": 0.0,
            "commission_amount": 0.0,
            "services_amount": 0.0,
            "logistics_amount": 0.0,
        }
        for item in periods:
            period_begin = DashboardFinanceService._parse_dt(item.get("period_begin"))
            if period_begin is None or period_begin.year != year or period_begin.month != month:
                continue
            for key in totals:
                totals[key] += float(item.get(key) or 0)
        return totals

    async def _get_snapshot(self, client_id: str) -> dict[str, Any] | None:
        return await cache_get_json(OzonFinanceSnapshotService.cache_key_for(client_id))

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None

    async def build_finance_summary(
        self,
        *,
        stores: list[dict[str, Any]],
        period_days: int = 62,
    ) -> dict[str, Any]:
        totals = {
            "orders_amount": 0.0,
            "returns_amount": 0.0,
            "commission_amount": 0.0,
            "services_amount": 0.0,
            "logistics_amount": 0.0,
            "net_payout": 0.0,
            "compensation_amount": 0.0,
            "decompensation_amount": 0.0,
            "net_payout_adjusted": 0.0,
        }
        covered_stores = 0
        latest_refresh: datetime | None = None
        store_breakdown: list[dict[str, Any]] = []
        realization_totals = {
            "sold_units": 0,
            "sold_amount": 0.0,
            "sold_fee": 0.0,
            "sold_bonus": 0.0,
            "sold_incentives": 0.0,
            "returned_units": 0,
            "returned_amount": 0.0,
            "returned_fee": 0.0,
            "returned_bonus": 0.0,
            "returned_incentives": 0.0,
            "net_units": 0,
            "net_amount": 0.0,
            "net_total": 0.0,
            "net_fee": 0.0,
            "net_bonus": 0.0,
            "net_incentives": 0.0,
        }
        realization_store_breakdown: list[dict[str, Any]] = []
        realization_period: str | None = None
        realization_covered = 0
        previous_realization_totals = {
            "sold_units": 0,
            "sold_amount": 0.0,
            "sold_fee": 0.0,
            "sold_bonus": 0.0,
            "sold_incentives": 0.0,
            "returned_units": 0,
            "returned_amount": 0.0,
            "returned_fee": 0.0,
            "returned_bonus": 0.0,
            "returned_incentives": 0.0,
            "net_units": 0,
            "net_amount": 0.0,
            "net_total": 0.0,
            "net_fee": 0.0,
            "net_bonus": 0.0,
            "net_incentives": 0.0,
        }
        previous_realization_period: str | None = None
        previous_realization_covered = 0
        closed_month_adjustments = {
            "compensation_amount": 0.0,
            "decompensation_amount": 0.0,
            "net_adjustment": 0.0,
        }
        current_month_to_date = {
            "orders_amount": 0.0,
            "returns_amount": 0.0,
            "previous_orders_amount": 0.0,
            "previous_returns_amount": 0.0,
        }
        current_month_to_date_covered = 0
        closed_month_cashflow = {
            "orders_amount": 0.0,
            "returns_amount": 0.0,
            "commission_amount": 0.0,
            "services_amount": 0.0,
            "logistics_amount": 0.0,
        }

        transaction_totals = {
            "accruals_for_sale": 0.0,
            "compensation_amount": 0.0,
            "money_transfer": 0.0,
            "others_amount": 0.0,
            "processing_and_delivery": 0.0,
            "refunds_and_cancellations": 0.0,
            "sale_commission": 0.0,
            "services_amount": 0.0,
        }
        transaction_service_buckets = {
            "marketing": 0.0,
            "storage": 0.0,
            "acquiring": 0.0,
            "returns": 0.0,
            "logistics": 0.0,
            "other": 0.0,
        }
        top_service_rows: dict[str, dict[str, Any]] = {}
        transaction_period_days = 30
        transaction_covered = 0
        marketing_total = 0.0
        marketing_services_count = 0
        marketing_store_breakdown: list[dict[str, Any]] = []
        placement_total = 0.0
        placement_rows_count = 0
        placement_offers_count = 0
        placement_period_days = 30
        placement_covered = 0
        placement_store_breakdown: list[dict[str, Any]] = []
        placement_top_rows: list[dict[str, Any]] = []
        placement_supplies_total = 0.0
        placement_supplies_stock_days_total = 0
        placement_supplies_rows_count = 0
        placement_supplies_count = 0
        placement_supplies_period_days = 30
        placement_supplies_covered = 0
        placement_supplies_metric_kind = "amount"
        placement_supplies_store_breakdown: list[dict[str, Any]] = []
        placement_supplies_top_rows: list[dict[str, Any]] = []
        removals_period_days = 30
        removals_covered = 0
        removals_rows_count = 0
        removals_returns_count = 0
        removals_offers_count = 0
        removals_quantity_total = 0
        removals_delivery_price_total = 0.0
        removals_auto_returns_count = 0
        removals_utilization_count = 0
        removals_store_rows: list[dict[str, Any]] = []
        removals_top_rows: list[dict[str, Any]] = []
        removal_source_breakdown: dict[str, dict[str, Any]] = {}
        removal_state_breakdown: dict[str, dict[str, Any]] = {}
        removal_offer_keys: set[str] = set()

        for store in stores:
            client_id = str(store.get("client_id") or "").strip()
            if not client_id:
                continue
            snapshot = await self._get_snapshot(client_id)
            if not snapshot:
                continue
            snapshot_periods = list(snapshot.get("periods") or [])

            covered_stores += 1
            summary = snapshot.get("summary") or {}
            breakdown_item = {
                "store_name": store.get("name") or "Магазин",
                "orders_amount": float(summary.get("orders_amount") or 0),
                "returns_amount": float(summary.get("returns_amount") or 0),
                "commission_amount": float(summary.get("commission_amount") or 0),
                "services_amount": float(summary.get("services_amount") or 0),
                "logistics_amount": float(summary.get("logistics_amount") or 0),
                "net_payout": float(summary.get("net_payout") or 0),
                "compensation_amount": float(summary.get("compensation_amount") or 0),
                "decompensation_amount": float(summary.get("decompensation_amount") or 0),
                "net_payout_adjusted": float(summary.get("net_payout_adjusted") or 0),
            }
            store_breakdown.append(breakdown_item)

            realization = snapshot.get("realization_closed_month") or {}
            if realization.get("available"):
                realization_covered += 1
                realization_period = realization_period or realization.get("period")
                realization_item = {
                    "store_name": store.get("name") or "Магазин",
                    "sold_units": int(realization.get("sold_units") or 0),
                    "sold_amount": float(realization.get("sold_amount") or 0),
                    "sold_fee": float(realization.get("sold_fee") or 0),
                    "sold_bonus": float(realization.get("sold_bonus") or 0),
                    "sold_incentives": float(realization.get("sold_incentives") or 0),
                    "returned_units": int(realization.get("returned_units") or 0),
                    "returned_amount": float(realization.get("returned_amount") or 0),
                    "returned_fee": float(realization.get("returned_fee") or 0),
                    "returned_bonus": float(realization.get("returned_bonus") or 0),
                    "returned_incentives": float(realization.get("returned_incentives") or 0),
                    "net_units": int(realization.get("net_units") or 0),
                    "net_amount": float(realization.get("net_amount") or 0),
                    "net_total": float(realization.get("net_total") or 0),
                    "net_fee": float(realization.get("net_fee") or 0),
                    "net_bonus": float(realization.get("net_bonus") or 0),
                    "net_incentives": float(realization.get("net_incentives") or 0),
                }
                realization_store_breakdown.append(realization_item)
                for key in realization_totals:
                    realization_totals[key] += realization_item[key]

                target_period = str(realization.get("period") or "")
                if target_period:
                    parsed_period = self._parse_dt(f"{target_period}-01T00:00:00+00:00")
                    if parsed_period is not None:
                        month_cashflow = self._sum_period_rows_for_month(
                            snapshot_periods,
                            year=parsed_period.year,
                            month=parsed_period.month,
                        )
                        for key in closed_month_cashflow:
                            closed_month_cashflow[key] += float(month_cashflow.get(key) or 0)
                adjustments = snapshot.get("adjustments") or {}
                for item in adjustments.get("compensation_reports") or []:
                    if str(item.get("month") or "") == target_period:
                        closed_month_adjustments["compensation_amount"] += float(item.get("amount_total") or 0)
                for item in adjustments.get("decompensation_reports") or []:
                    if str(item.get("month") or "") == target_period:
                        closed_month_adjustments["decompensation_amount"] += float(item.get("amount_total") or 0)

            previous_realization = snapshot.get("realization_previous_closed_month") or {}
            if previous_realization.get("available"):
                previous_realization_covered += 1
                previous_realization_period = previous_realization_period or previous_realization.get("period")
                for key in previous_realization_totals:
                    previous_realization_totals[key] += (
                        float(previous_realization.get(key) or 0)
                        if isinstance(previous_realization_totals[key], float)
                        else int(previous_realization.get(key) or 0)
                    )

            current_month_snapshot = snapshot.get("current_month_to_date") or {}
            previous_month_same_period_snapshot = snapshot.get("previous_month_same_period") or {}
            if not current_month_snapshot and snapshot_periods:
                latest_period_begin = max(
                    (self._parse_dt(item.get("period_begin")) for item in snapshot_periods if item.get("period_begin")),
                    default=None,
                )
                if latest_period_begin is not None:
                    current_month_snapshot = self._sum_period_rows_for_month(
                        snapshot_periods,
                        year=latest_period_begin.year,
                        month=latest_period_begin.month,
                    )
                    previous_year = latest_period_begin.year
                    previous_month = latest_period_begin.month - 1
                    if previous_month == 0:
                        previous_month = 12
                        previous_year -= 1
                    previous_month_same_period_snapshot = self._sum_period_rows_for_month(
                        snapshot_periods,
                        year=previous_year,
                        month=previous_month,
                    )
            today = datetime.now().date()
            current_month_start = today.replace(day=1)
            previous_month_start = current_month_start.replace(day=1)
            if current_month_start.month == 1:
                previous_month_start = previous_month_start.replace(year=previous_month_start.year - 1, month=12)
            else:
                previous_month_start = previous_month_start.replace(month=previous_month_start.month - 1)

            current_period_start = self._parse_date(current_month_snapshot.get("date_from"))
            previous_period_start = self._parse_date(previous_month_same_period_snapshot.get("date_from"))
            if current_period_start == current_month_start and previous_period_start == previous_month_start:
                current_month_to_date["orders_amount"] += float(current_month_snapshot.get("orders_amount") or 0)
                current_month_to_date["returns_amount"] += abs(float(current_month_snapshot.get("returns_amount") or 0))
                current_month_to_date["previous_orders_amount"] += float(previous_month_same_period_snapshot.get("orders_amount") or 0)
                current_month_to_date["previous_returns_amount"] += abs(float(previous_month_same_period_snapshot.get("returns_amount") or 0))
                current_month_to_date_covered += 1

            transactions_recent = snapshot.get("transactions_recent") or {}
            if transactions_recent.get("available"):
                transaction_covered += 1
                period = transactions_recent.get("period") or {}
                transaction_period_days = int(period.get("days") or transaction_period_days)

                tx_totals = transactions_recent.get("totals") or {}
                for key in transaction_totals:
                    transaction_totals[key] += float(tx_totals.get(key) or 0)

                for key in transaction_service_buckets:
                    transaction_service_buckets[key] += float((transactions_recent.get("service_buckets") or {}).get(key) or 0)

                store_marketing_amount_raw = float((transactions_recent.get("service_buckets") or {}).get("marketing") or 0)
                store_marketing_amount = abs(store_marketing_amount_raw)
                store_marketing_services_count = 0

                for row in transactions_recent.get("top_services") or []:
                    service_name = str(row.get("name") or "Прочая услуга")
                    bucket = row.get("bucket") or "other"
                    item = top_service_rows.setdefault(
                        service_name,
                        {
                            "name": service_name,
                            "bucket": bucket,
                            "amount": 0.0,
                            "count": 0,
                        },
                    )
                    item["amount"] += float(row.get("amount") or 0)
                    item["count"] += int(row.get("count") or 0)
                    if bucket == "marketing":
                        store_marketing_services_count += int(row.get("count") or 0)

                marketing_total += store_marketing_amount
                marketing_services_count += store_marketing_services_count
                marketing_store_breakdown.append(
                    {
                        "store_name": store.get("name") or "Магазин",
                        "marketing_amount": store_marketing_amount,
                        "services_count": store_marketing_services_count,
                        "share_of_orders": (
                            store_marketing_amount / breakdown_item["orders_amount"]
                            if breakdown_item["orders_amount"] > 0
                            else 0.0
                        ),
                    }
                )

            placement_recent = snapshot.get("placement_by_products_recent") or {}
            if placement_recent.get("available"):
                placement_covered += 1
                period = placement_recent.get("period") or {}
                placement_period_days = int(period.get("days") or placement_period_days)
                placement_amount = float(placement_recent.get("amount_total") or 0)
                placement_total += placement_amount
                placement_rows_count += int(placement_recent.get("rows_count") or 0)
                placement_offers_count += int(placement_recent.get("offers_count") or 0)
                placement_store_breakdown.append(
                    {
                        "store_name": store.get("name") or "Магазин",
                        "amount_total": placement_amount,
                        "rows_count": int(placement_recent.get("rows_count") or 0),
                        "offers_count": int(placement_recent.get("offers_count") or 0),
                    }
                )
                for row in placement_recent.get("top_items") or []:
                    placement_top_rows.append(
                        {
                            "store_name": store.get("name") or "Магазин",
                            "offer_id": str(row.get("offer_id") or ""),
                            "title": row.get("title") or "",
                            "amount": float(row.get("amount") or 0),
                            "quantity": int(row.get("quantity") or 0),
                            "days": int(row.get("days") or 0),
                        }
                    )

            placement_supplies_recent = snapshot.get("placement_by_supplies_recent") or {}
            if placement_supplies_recent.get("available"):
                placement_supplies_covered += 1
                period = placement_supplies_recent.get("period") or {}
                placement_supplies_period_days = int(period.get("days") or placement_supplies_period_days)
                placement_supplies_metric_kind = str(
                    placement_supplies_recent.get("metric_kind") or placement_supplies_metric_kind
                )
                placement_supplies_amount = float(placement_supplies_recent.get("amount_total") or 0)
                placement_supplies_stock_days = int(placement_supplies_recent.get("stock_days_total") or 0)
                placement_supplies_total += placement_supplies_amount
                placement_supplies_stock_days_total += placement_supplies_stock_days
                placement_supplies_rows_count += int(placement_supplies_recent.get("rows_count") or 0)
                placement_supplies_count += int(placement_supplies_recent.get("supplies_count") or 0)
                placement_supplies_store_breakdown.append(
                    {
                        "store_name": store.get("name") or "Магазин",
                        "amount_total": placement_supplies_amount,
                        "stock_days_total": placement_supplies_stock_days,
                        "rows_count": int(placement_supplies_recent.get("rows_count") or 0),
                        "supplies_count": int(placement_supplies_recent.get("supplies_count") or 0),
                    }
                )
                for row in placement_supplies_recent.get("top_items") or []:
                    placement_supplies_top_rows.append(
                        {
                            "store_name": store.get("name") or "Магазин",
                            "supply_ref": str(row.get("supply_ref") or ""),
                            "warehouse_name": row.get("warehouse_name") or "",
                            "amount": float(row.get("amount") or 0),
                            "items_count": int(row.get("items_count") or 0),
                            "days": int(row.get("days") or 0),
                            "stock_days_total": int(row.get("stock_days_total") or 0),
                        }
                    )

            removal_from_stock_recent = snapshot.get("removal_from_stock_recent") or {}
            removal_from_supply_recent = snapshot.get("removal_from_supply_recent") or {}
            store_removals_rows = 0
            store_removals_quantity = 0
            store_removals_delivery_price = 0.0
            store_removals_auto_returns = 0
            store_removals_utilization = 0
            store_has_removals = False

            for removal_snapshot in (removal_from_stock_recent, removal_from_supply_recent):
                if not removal_snapshot.get("available"):
                    continue

                store_has_removals = True
                period = removal_snapshot.get("period") or {}
                removals_period_days = int(period.get("days") or removals_period_days)
                kind = str(removal_snapshot.get("kind") or "from_stock")
                kind_label = str(removal_snapshot.get("kind_label") or ("Со стока" if kind == "from_stock" else "С поставки"))

                rows_count = int(removal_snapshot.get("rows_count") or 0)
                returns_count = int(removal_snapshot.get("returns_count") or 0)
                quantity_total = int(removal_snapshot.get("quantity_total") or 0)
                delivery_price_total = float(removal_snapshot.get("delivery_price_total") or 0)
                auto_returns_count = int(removal_snapshot.get("auto_returns_count") or 0)
                utilization_count = int(removal_snapshot.get("utilization_count") or 0)

                removals_rows_count += rows_count
                removals_returns_count += returns_count
                removals_quantity_total += quantity_total
                removals_delivery_price_total += delivery_price_total
                removals_auto_returns_count += auto_returns_count
                removals_utilization_count += utilization_count

                store_removals_rows += rows_count
                store_removals_quantity += quantity_total
                store_removals_delivery_price += delivery_price_total
                store_removals_auto_returns += auto_returns_count
                store_removals_utilization += utilization_count

                source_item = removal_source_breakdown.setdefault(
                    kind,
                    {
                        "kind": kind,
                        "kind_label": kind_label,
                        "rows_count": 0,
                        "returns_count": 0,
                        "offers_count": 0,
                        "quantity_total": 0,
                        "delivery_price_total": 0.0,
                        "auto_returns_count": 0,
                        "utilization_count": 0,
                    },
                )
                source_item["rows_count"] += rows_count
                source_item["returns_count"] += returns_count
                source_item["offers_count"] += int(removal_snapshot.get("offers_count") or 0)
                source_item["quantity_total"] += quantity_total
                source_item["delivery_price_total"] += delivery_price_total
                source_item["auto_returns_count"] += auto_returns_count
                source_item["utilization_count"] += utilization_count

                for row in removal_snapshot.get("items") or []:
                    offer_id = str(row.get("offer_id") or "").strip()
                    if offer_id:
                        removal_offer_keys.add(f"{store.get('id') or store.get('name') or ''}:{offer_id}")
                    removals_top_rows.append(
                        {
                            "store_name": store.get("name") or "Магазин",
                            "kind": kind,
                            "kind_label": kind_label,
                            "offer_id": offer_id,
                            "title": row.get("title") or "",
                            "quantity_total": int(row.get("quantity_total") or 0),
                            "delivery_price_total": float(row.get("delivery_price_total") or 0),
                            "auto_returns_count": int(row.get("auto_returns_count") or 0),
                            "utilization_count": int(row.get("utilization_count") or 0),
                            "last_return_state": row.get("last_return_state") or "",
                            "delivery_type": row.get("delivery_type") or "",
                            "stock_type": row.get("stock_type") or "",
                        }
                    )

                for row in removal_snapshot.get("states") or []:
                    state = str(row.get("state") or "Без статуса")
                    state_item = removal_state_breakdown.setdefault(
                        state,
                        {
                            "state": state,
                            "count": 0,
                            "quantity_total": 0,
                            "delivery_price_total": 0.0,
                        },
                    )
                    state_item["count"] += int(row.get("count") or 0)
                    state_item["quantity_total"] += int(row.get("quantity_total") or 0)
                    state_item["delivery_price_total"] += float(row.get("delivery_price_total") or 0)

            if store_has_removals:
                removals_covered += 1
                removals_store_rows.append(
                    {
                        "store_name": store.get("name") or "Магазин",
                        "rows_count": store_removals_rows,
                        "quantity_total": store_removals_quantity,
                        "delivery_price_total": store_removals_delivery_price,
                        "auto_returns_count": store_removals_auto_returns,
                        "utilization_count": store_removals_utilization,
                    }
                )

            for key in totals:
                totals[key] += breakdown_item[key]

            refreshed_at = self._parse_dt(snapshot.get("refreshed_at"))
            if refreshed_at and (latest_refresh is None or refreshed_at > latest_refresh):
                latest_refresh = refreshed_at

        store_breakdown.sort(key=lambda item: item["net_payout"], reverse=True)
        realization_store_breakdown.sort(key=lambda item: item["net_amount"], reverse=True)
        top_services = sorted(
            (
                {
                    "name": item["name"],
                    "bucket": item["bucket"],
                    "amount": round(float(item["amount"]), 2),
                    "count": int(item["count"]),
                }
                for item in top_service_rows.values()
            ),
            key=lambda item: abs(item["amount"]),
            reverse=True,
        )
        marketing_top_services = [
            {
                **item,
                "amount": round(abs(float(item["amount"])), 2),
            }
            for item in top_services
            if item["bucket"] == "marketing"
        ]
        marketing_store_breakdown.sort(key=lambda item: item["marketing_amount"], reverse=True)
        placement_store_breakdown.sort(key=lambda item: item["amount_total"], reverse=True)
        placement_top_items = sorted(
            (
                {
                    "store_name": item["store_name"],
                    "offer_id": item["offer_id"],
                    "title": item["title"],
                    "amount": round(float(item["amount"]), 2),
                    "quantity": int(item["quantity"]),
                    "days": int(item["days"]),
                }
                for item in placement_top_rows
            ),
            key=lambda item: item["amount"],
            reverse=True,
        )
        placement_supplies_store_breakdown.sort(key=lambda item: item["amount_total"], reverse=True)
        placement_supplies_top_items = sorted(
            (
                {
                    "store_name": item["store_name"],
                    "supply_ref": item["supply_ref"],
                        "warehouse_name": item["warehouse_name"],
                        "amount": round(float(item["amount"]), 2),
                        "items_count": int(item["items_count"]),
                        "days": int(item["days"]),
                        "stock_days_total": int(item["stock_days_total"]),
                    }
                for item in placement_supplies_top_rows
            ),
            key=lambda item: (item["amount"], item["stock_days_total"]),
            reverse=True,
        )
        removals_store_breakdown = sorted(
            (
                {
                    "store_name": item["store_name"],
                    "rows_count": int(item["rows_count"]),
                    "quantity_total": int(item["quantity_total"]),
                    "delivery_price_total": round(float(item["delivery_price_total"]), 2),
                    "auto_returns_count": int(item["auto_returns_count"]),
                    "utilization_count": int(item["utilization_count"]),
                }
                for item in removals_store_rows
            ),
            key=lambda item: (item["delivery_price_total"], item["quantity_total"], item["rows_count"]),
            reverse=True,
        )
        removals_source_items = sorted(
            (
                {
                    "kind": item["kind"],
                    "kind_label": item["kind_label"],
                    "rows_count": int(item["rows_count"]),
                    "returns_count": int(item["returns_count"]),
                    "offers_count": int(item["offers_count"]),
                    "quantity_total": int(item["quantity_total"]),
                    "delivery_price_total": round(float(item["delivery_price_total"]), 2),
                    "auto_returns_count": int(item["auto_returns_count"]),
                    "utilization_count": int(item["utilization_count"]),
                }
                for item in removal_source_breakdown.values()
            ),
            key=lambda item: (item["delivery_price_total"], item["quantity_total"], item["rows_count"]),
            reverse=True,
        )
        removals_top_items = sorted(
            (
                {
                    "store_name": item["store_name"],
                    "kind": item["kind"],
                    "kind_label": item["kind_label"],
                    "offer_id": item["offer_id"],
                    "title": item["title"],
                    "quantity_total": int(item["quantity_total"]),
                    "delivery_price_total": round(float(item["delivery_price_total"]), 2),
                    "auto_returns_count": int(item["auto_returns_count"]),
                    "utilization_count": int(item["utilization_count"]),
                    "last_return_state": item["last_return_state"],
                    "delivery_type": item["delivery_type"],
                    "stock_type": item["stock_type"],
                }
                for item in removals_top_rows
            ),
            key=lambda item: (item["delivery_price_total"], item["quantity_total"], item["auto_returns_count"]),
            reverse=True,
        )
        removals_top_states = sorted(
            (
                {
                    "state": item["state"],
                    "count": int(item["count"]),
                    "quantity_total": int(item["quantity_total"]),
                    "delivery_price_total": round(float(item["delivery_price_total"]), 2),
                }
                for item in removal_state_breakdown.values()
            ),
            key=lambda item: (item["delivery_price_total"], item["quantity_total"], item["count"]),
            reverse=True,
        )

        return {
            "source": "finance_snapshot" if covered_stores else "unavailable",
            "period_days": period_days,
            "stores_covered": covered_stores,
            "stores_missing": max(len(stores) - covered_stores, 0),
            "updated_at": latest_refresh.isoformat() if latest_refresh else None,
            "store_breakdown": store_breakdown[:8],
            "realization_closed_month": {
                "available": realization_covered > 0,
                "period": realization_period,
                "stores_covered": realization_covered,
                "stores_missing": max(len(stores) - realization_covered, 0),
                **{key: round(value, 2) if isinstance(value, float) else int(value) for key, value in realization_totals.items()},
                "compensation_amount": round(closed_month_adjustments["compensation_amount"], 2),
                "decompensation_amount": round(closed_month_adjustments["decompensation_amount"], 2),
                "net_adjustment": round(
                    closed_month_adjustments["compensation_amount"] - closed_month_adjustments["decompensation_amount"],
                    2,
                ),
                "store_breakdown": realization_store_breakdown[:8],
            },
            "realization_month_compare": {
                "available": realization_covered > 0 and previous_realization_covered > 0,
                "current_period": realization_period,
                "previous_period": previous_realization_period,
                "current": {
                    key: round(value, 2) if isinstance(value, float) else int(value)
                    for key, value in realization_totals.items()
                },
                "previous": {
                    key: round(value, 2) if isinstance(value, float) else int(value)
                    for key, value in previous_realization_totals.items()
                },
                "delta": {
                    key: round(realization_totals[key] - previous_realization_totals[key], 2)
                    if isinstance(realization_totals[key], float)
                    else int(realization_totals[key] - previous_realization_totals[key])
                    for key in realization_totals
                },
            },
            "closed_month_cashflow": {key: round(value, 2) for key, value in closed_month_cashflow.items()},
            "current_month_to_date": {
                "available": current_month_to_date_covered > 0,
                "stores_covered": current_month_to_date_covered,
                "stores_missing": max(len(stores) - current_month_to_date_covered, 0),
                "orders_amount": round(current_month_to_date["orders_amount"], 2) if current_month_to_date_covered > 0 else 0.0,
                "returns_amount": round(current_month_to_date["returns_amount"], 2) if current_month_to_date_covered > 0 else 0.0,
                "previous_orders_amount": round(current_month_to_date["previous_orders_amount"], 2) if current_month_to_date_covered > 0 else 0.0,
                "previous_returns_amount": round(current_month_to_date["previous_returns_amount"], 2) if current_month_to_date_covered > 0 else 0.0,
                "delta_orders_amount": round(current_month_to_date["orders_amount"] - current_month_to_date["previous_orders_amount"], 2) if current_month_to_date_covered > 0 else 0.0,
                "delta_returns_amount": round(current_month_to_date["returns_amount"] - current_month_to_date["previous_returns_amount"], 2) if current_month_to_date_covered > 0 else 0.0,
            },
            "transactions_recent": {
                "available": transaction_covered > 0,
                "period_days": transaction_period_days,
                "stores_covered": transaction_covered,
                "stores_missing": max(len(stores) - transaction_covered, 0),
                "service_buckets": {key: round(value, 2) for key, value in transaction_service_buckets.items()},
                "top_services": top_services[:10],
                **{key: round(value, 2) for key, value in transaction_totals.items()},
            },
            "marketing_recent": {
                "available": transaction_covered > 0,
                "period_days": transaction_period_days,
                "stores_covered": transaction_covered,
                "stores_missing": max(len(stores) - transaction_covered, 0),
                "amount_total": round(marketing_total, 2),
                "services_count": int(marketing_services_count),
                "share_of_orders": round((marketing_total / totals["orders_amount"]) if totals["orders_amount"] > 0 else 0.0, 4),
                "store_breakdown": [
                    {
                        "store_name": item["store_name"],
                        "marketing_amount": round(float(item["marketing_amount"]), 2),
                        "services_count": int(item["services_count"]),
                        "share_of_orders": round(float(item["share_of_orders"]), 4),
                    }
                    for item in marketing_store_breakdown[:8]
                ],
                "top_services": marketing_top_services[:10],
            },
            "placement_by_products_recent": {
                "available": placement_covered > 0,
                "period_days": placement_period_days,
                "stores_covered": placement_covered,
                "stores_missing": max(len(stores) - placement_covered, 0),
                "amount_total": round(placement_total, 2),
                "rows_count": placement_rows_count,
                "offers_count": placement_offers_count,
                "store_breakdown": placement_store_breakdown[:8],
                "top_items": placement_top_items[:10],
            },
            "placement_by_supplies_recent": {
                "available": placement_supplies_covered > 0,
                "period_days": placement_supplies_period_days,
                "stores_covered": placement_supplies_covered,
                "stores_missing": max(len(stores) - placement_supplies_covered, 0),
                "metric_kind": placement_supplies_metric_kind,
                "amount_total": round(placement_supplies_total, 2),
                "stock_days_total": placement_supplies_stock_days_total,
                "rows_count": placement_supplies_rows_count,
                "supplies_count": placement_supplies_count,
                "store_breakdown": placement_supplies_store_breakdown[:8],
                "top_items": placement_supplies_top_items[:10],
            },
            "removals_recent": {
                "available": removals_covered > 0,
                "period_days": removals_period_days,
                "stores_covered": removals_covered,
                "stores_missing": max(len(stores) - removals_covered, 0),
                "rows_count": removals_rows_count,
                "returns_count": removals_returns_count,
                "offers_count": len(removal_offer_keys),
                "quantity_total": removals_quantity_total,
                "delivery_price_total": round(removals_delivery_price_total, 2),
                "auto_returns_count": removals_auto_returns_count,
                "utilization_count": removals_utilization_count,
                "source_breakdown": removals_source_items[:4],
                "store_breakdown": removals_store_breakdown[:8],
                "top_items": removals_top_items[:10],
                "top_states": removals_top_states[:10],
            },
            **{key: round(value, 2) for key, value in totals.items()},
        }
