from __future__ import annotations

import asyncio
import csv
import io
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.product import Product
from app.models.store import Store
from app.models.supply import Supply
from app.models.variant import Variant
from app.models.warehouse import Warehouse, WarehouseStock
from app.services.ozon.report_schema_guard import (
    missing_required_column_groups,
    notify_ozon_report_columns_changed,
    normalize_report_headers,
)
from app.services.ozon.client import OzonClient
from app.services.ozon.finance_snapshot_service import OzonFinanceSnapshotService
from app.services.ozon.report_service import OzonReportService
from app.services.ozon.report_snapshot_service import OzonReportSnapshotService
from app.utils.encryption import decrypt_api_key


class DailyReportService:
    RETURNS_REQUIRED_COLUMNS: tuple[tuple[str, ...], ...] = (
        ("Количество возвращаемых товаров", "Returned quantity", "returned quantity", "Quantity of returned goods"),
    )

    @staticmethod
    def _build_offer_movers(
        yesterday_offers: list[dict[str, Any]],
        compare_offers: list[dict[str, Any]],
        *,
        limit: int = 5,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        yesterday_by_offer: dict[str, dict[str, Any]] = {}
        compare_by_offer: dict[str, dict[str, Any]] = {}

        for item in yesterday_offers:
            offer_id = item.get("offer_id") or ""
            if offer_id:
                yesterday_by_offer[offer_id] = item

        for item in compare_offers:
            offer_id = item.get("offer_id") or ""
            if offer_id:
                compare_by_offer[offer_id] = item

        gainers: list[dict[str, Any]] = []
        losers: list[dict[str, Any]] = []

        for offer_id in set(yesterday_by_offer) | set(compare_by_offer):
            yesterday_stats = yesterday_by_offer.get(offer_id) or {"title": "", "units": 0.0, "revenue": 0.0}
            compare_stats = compare_by_offer.get(offer_id) or {"title": "", "units": 0.0, "revenue": 0.0}
            delta_units = float(yesterday_stats.get("units") or 0) - float(compare_stats.get("units") or 0)
            row = {
                "offer_id": offer_id,
                "title": yesterday_stats.get("title") or compare_stats.get("title") or "",
                "units_yesterday": int(round(float(yesterday_stats.get("units") or 0))),
                "units_prev_day": int(round(float(compare_stats.get("units") or 0))),
                "revenue_yesterday": round(float(yesterday_stats.get("revenue") or 0), 2),
                "delta_units": int(round(delta_units)),
            }
            if delta_units > 0:
                gainers.append(row)
            elif delta_units < 0:
                losers.append(row)

        gainers.sort(key=lambda item: (item["delta_units"], item["units_yesterday"]), reverse=True)
        losers.sort(key=lambda item: (abs(item["delta_units"]), item["units_prev_day"]), reverse=True)
        return gainers[:limit], losers[:limit]

    async def _ensure_postings_snapshot_with_daily_data(
        self,
        store: Store,
        *,
        target_day: str,
        compare_day: str,
        current_snapshot: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        snapshot = current_snapshot
        analytics = ((snapshot or {}).get("analytics") or {}).get("shipment_orders") or {}
        daily_totals = analytics.get("daily_totals") or {}
        if daily_totals.get(target_day) or daily_totals.get(compare_day):
            return snapshot

        client = OzonClient(
            store.client_id,
            decrypt_api_key(store.api_key_encrypted),
            store_name=store.name,
            emit_notifications=False,
        )
        try:
            report_service = OzonReportService(client)
            snapshot_service = OzonReportSnapshotService(report_service)
            return await asyncio.wait_for(
                snapshot_service.refresh_fbo_postings_snapshot(client_id=store.client_id, days_back=30),
                timeout=20,
            )
        except Exception:
            return snapshot
        finally:
            await client.close()

    @staticmethod
    def _to_float(value: Any) -> float:
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _normalize_header(value: str) -> str:
        return " ".join(str(value or "").strip().lower().replace("_", " ").split())

    @classmethod
    def _pick_value(cls, row: dict[str, str], *aliases: str) -> str:
        normalized = {cls._normalize_header(key): value for key, value in row.items()}
        for alias in aliases:
            value = normalized.get(cls._normalize_header(alias))
            if value not in (None, ""):
                return str(value).strip()
        return ""

    @staticmethod
    def _decode_bytes(raw_bytes: bytes) -> str:
        for encoding in ("utf-8-sig", "utf-8", "cp1251"):
            try:
                return raw_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue
        return raw_bytes.decode("utf-8", errors="replace")

    @classmethod
    def _parse_returns_units(cls, raw_bytes: bytes) -> int:
        text = cls._decode_bytes(raw_bytes)
        sample = text[:4096]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
            dialect.delimiter = ";"

        total_units = 0.0
        reader = csv.DictReader(io.StringIO(text), dialect=dialect)
        for raw_row in reader:
            row = {str(key).strip(): str(value).strip() for key, value in raw_row.items() if key}
            if not any(row.values()):
                continue
            quantity = cls._pick_value(
                row,
                "quantity",
                "qty",
                "количество",
                "кол-во",
                "returned quantity",
            )
            total_units += cls._to_float(quantity) or 1.0
        return int(round(total_units))

    @classmethod
    async def _validate_returns_report_schema(
        cls,
        *,
        raw_bytes: bytes,
        client_id: str,
        store_name: str,
        report_date: date,
    ) -> None:
        headers: list[str] = []
        try:
            text = cls._decode_bytes(raw_bytes)
            sample = text[:4096]
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
            except csv.Error:
                dialect = csv.excel
                dialect.delimiter = ";"
            reader = csv.DictReader(io.StringIO(text), dialect=dialect)
            headers = [str(header).strip() for header in (reader.fieldnames or []) if header]
        except Exception:
            headers = []

        missing_groups = missing_required_column_groups(headers, cls.RETURNS_REQUIRED_COLUMNS)
        if not missing_groups:
            return

        await notify_ozon_report_columns_changed(
            endpoint="/v2/report/returns/create",
            client_id=client_id,
            report_name=f"returns_daily_report:{store_name}",
            required_groups=missing_groups,
            actual_headers=normalize_report_headers(headers),
            payload={"report_date": report_date.isoformat()},
        )
        raise RuntimeError("Returns report is missing required columns")

    async def _fetch_store_day_finance_summary(self, store: Store, *, report_date: date) -> dict[str, Any]:
        client = OzonClient(store.client_id, decrypt_api_key(store.api_key_encrypted), store_name=store.name, emit_notifications=False)
        try:
            service = OzonFinanceSnapshotService(client)
            return await service.fetch_day_summary(report_date=report_date)
        finally:
            await client.close()

    async def _fetch_store_day_returns_summary(self, store: Store, *, report_date: date) -> dict[str, Any]:
        period_start = datetime(report_date.year, report_date.month, report_date.day, 0, 0, 0, tzinfo=timezone.utc)
        period_finish = datetime(report_date.year, report_date.month, report_date.day, 23, 59, 59, tzinfo=timezone.utc)

        client = OzonClient(store.client_id, decrypt_api_key(store.api_key_encrypted), store_name=store.name, emit_notifications=False)
        try:
            service = OzonReportService(client)
            ready = await service.ensure_returns_report(
                date_from=period_start.isoformat().replace("+00:00", "Z"),
                date_to=period_finish.isoformat().replace("+00:00", "Z"),
            )
            raw_bytes = await service.download_ready_report(ready)
            await self._validate_returns_report_schema(
                raw_bytes=raw_bytes,
                client_id=store.client_id,
                store_name=store.name,
                report_date=report_date,
            )
            return {
                "date": report_date.isoformat(),
                "units": self._parse_returns_units(raw_bytes),
                "available": True,
            }
        except Exception:
            return {
                "date": report_date.isoformat(),
                "units": 0,
                "available": False,
            }
        finally:
            await client.close()

    async def build_owner_daily_report(
        self,
        db: AsyncSession,
        *,
        cabinet_owner_id: int,
        report_date: date | None = None,
        allow_external_fetch: bool = True,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        dispatch_date = report_date or datetime.now(timezone.utc).date()
        target_date = dispatch_date - timedelta(days=1)
        compare_date = target_date - timedelta(days=1)
        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)

        stores_result = await db.execute(
            select(Store).where(Store.user_id == cabinet_owner_id, Store.is_active == True)  # noqa: E712
        )
        stores = list(stores_result.scalars().all())

        store_dicts = [{"store_id": store.id, "store_name": store.name} for store in stores]
        store_ids = [store.id for store in stores]

        if store_ids:
            total_supplies = await db.scalar(select(func.count(Supply.id)).where(Supply.store_id.in_(store_ids)))
            active_supplies = await db.scalar(
                select(func.count(Supply.id)).where(
                    Supply.store_id.in_(store_ids),
                    Supply.status.in_(["READY_TO_SUPPLY", "ACCEPTED_AT_SUPPLY_WAREHOUSE", "IN_TRANSIT"]),
                )
            )
            completed_supplies = await db.scalar(
                select(func.count(Supply.id)).where(Supply.store_id.in_(store_ids), Supply.status == "COMPLETED")
            )
            cancelled_supplies = await db.scalar(
                select(func.count(Supply.id)).where(
                    Supply.store_id.in_(store_ids),
                    Supply.status.in_(["CANCELLED", "REJECTED_AT_SUPPLY_WAREHOUSE"]),
                )
            )
            today_supplies = await db.scalar(
                select(func.count(Supply.id)).where(
                    Supply.store_id.in_(store_ids),
                    Supply.timeslot_from >= today,
                    Supply.timeslot_from < tomorrow,
                )
            )
        else:
            total_supplies = active_supplies = completed_supplies = cancelled_supplies = today_supplies = 0

        stocks_result = await db.execute(
            select(WarehouseStock, Variant)
            .join(Warehouse, Warehouse.id == WarehouseStock.warehouse_id)
            .join(Variant, Variant.id == WarehouseStock.variant_id)
            .where(Warehouse.user_id == cabinet_owner_id)
        )
        stocks = stocks_result.all()
        total_variants = len(stocks)
        total_units = sum(stock.unpacked_quantity + stock.packed_quantity * (variant.pack_size or 1) for stock, variant in stocks)
        total_reserved = sum(stock.reserved_quantity for stock, _variant in stocks)
        total_available = total_units - total_reserved

        yesterday_key = target_date.isoformat()
        compare_key = compare_date.isoformat()
        latest_snapshot_refresh: str | None = None
        combined_yesterday_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"offer_id": "", "title": "", "units": 0.0, "revenue": 0.0}
        )
        combined_compare_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"offer_id": "", "title": "", "units": 0.0, "revenue": 0.0}
        )
        yesterday_order_units = 0.0
        yesterday_order_revenue = 0.0
        sales_stores_covered = 0
        store_stats: list[dict[str, Any]] = []
        finance_orders_amount = 0.0
        finance_orders_amount_prev = 0.0
        finance_returns_amount = 0.0
        finance_returns_amount_prev = 0.0
        finance_covered = 0
        returns_units = 0
        returns_units_prev = 0
        returns_units_covered = 0
        returns_units_prev_covered = 0

        for store in stores:
            snapshot = await OzonReportSnapshotService.get_cached_snapshot_for_client(
                client_id=store.client_id,
                kind="postings",
            )
            snapshot = await self._ensure_postings_snapshot_with_daily_data(
                store,
                target_day=yesterday_key,
                compare_day=compare_key,
                current_snapshot=snapshot,
            )

            store_yesterday_summary = OzonReportSnapshotService.get_postings_day_summary(snapshot, day=yesterday_key)
            store_compare_summary = OzonReportSnapshotService.get_postings_day_summary(snapshot, day=compare_key)
            store_yesterday_offers = OzonReportSnapshotService.get_postings_day_offer_stats(snapshot, day=yesterday_key)
            store_compare_offers = OzonReportSnapshotService.get_postings_day_offer_stats(snapshot, day=compare_key)

            if snapshot:
                sales_stores_covered += 1
                refreshed_at = snapshot.get("refreshed_at")
                if refreshed_at and (latest_snapshot_refresh is None or refreshed_at > latest_snapshot_refresh):
                    latest_snapshot_refresh = refreshed_at

            yesterday_order_units += store_yesterday_summary["units"]
            yesterday_order_revenue += store_yesterday_summary["revenue"]

            for item in store_yesterday_offers:
                offer_id = item.get("offer_id") or ""
                if not offer_id:
                    continue
                combined = combined_yesterday_stats[offer_id]
                combined["offer_id"] = offer_id
                combined["title"] = combined["title"] or item.get("title") or ""
                combined["units"] += float(item.get("units") or 0)
                combined["revenue"] += float(item.get("revenue") or 0)

            for item in store_compare_offers:
                offer_id = item.get("offer_id") or ""
                if not offer_id:
                    continue
                combined = combined_compare_stats[offer_id]
                combined["offer_id"] = offer_id
                combined["title"] = combined["title"] or item.get("title") or ""
                combined["units"] += float(item.get("units") or 0)
                combined["revenue"] += float(item.get("revenue") or 0)

            store_returns_units_yesterday: int | None = None
            store_returns_units_prev_day: int | None = None
            store_returns_units_available = False
            store_returns_delta_available = False
            store_returns_amount_yesterday = 0.0
            store_returns_amount_prev_day = 0.0

            if allow_external_fetch:
                finance_target_task = asyncio.create_task(
                    asyncio.wait_for(
                        self._fetch_store_day_finance_summary(store, report_date=target_date),
                        timeout=12,
                    )
                )
                finance_compare_task = asyncio.create_task(
                    asyncio.wait_for(
                        self._fetch_store_day_finance_summary(store, report_date=compare_date),
                        timeout=12,
                    )
                )
                returns_target_task = asyncio.create_task(
                    asyncio.wait_for(
                        self._fetch_store_day_returns_summary(store, report_date=target_date),
                        timeout=12,
                    )
                )
                returns_compare_task = asyncio.create_task(
                    asyncio.wait_for(
                        self._fetch_store_day_returns_summary(store, report_date=compare_date),
                        timeout=12,
                    )
                )

                finance_target_result, finance_compare_result, returns_target_result, returns_compare_result = await asyncio.gather(
                    finance_target_task,
                    finance_compare_task,
                    returns_target_task,
                    returns_compare_task,
                    return_exceptions=True,
                )

                if not isinstance(finance_target_result, Exception):
                    store_returns_amount_yesterday = abs(float(finance_target_result.get("returns_amount") or 0))
                    finance_orders_amount += float(finance_target_result.get("orders_amount") or 0)
                    finance_returns_amount += store_returns_amount_yesterday
                    finance_covered += 1

                if not isinstance(finance_compare_result, Exception):
                    store_returns_amount_prev_day = abs(float(finance_compare_result.get("returns_amount") or 0))
                    finance_orders_amount_prev += float(finance_compare_result.get("orders_amount") or 0)
                    finance_returns_amount_prev += store_returns_amount_prev_day

                if not isinstance(returns_target_result, Exception) and returns_target_result.get("available"):
                    store_returns_units_yesterday = int(returns_target_result.get("units") or 0)
                    store_returns_units_available = True
                    returns_units += store_returns_units_yesterday
                    returns_units_covered += 1

                if not isinstance(returns_compare_result, Exception) and returns_compare_result.get("available"):
                    store_returns_units_prev_day = int(returns_compare_result.get("units") or 0)
                    returns_units_prev += store_returns_units_prev_day
                    returns_units_prev_covered += 1

                store_returns_delta_available = store_returns_units_available and store_returns_units_prev_day is not None

            store_movers_up, store_movers_down = self._build_offer_movers(
                store_yesterday_offers,
                store_compare_offers,
                limit=3,
            )

            store_stats.append({
                "store_id": store.id,
                "store_name": store.name,
                "total_supplies": await db.scalar(select(func.count(Supply.id)).where(Supply.store_id == store.id)) or 0,
                "active_supplies": await db.scalar(
                    select(func.count(Supply.id)).where(
                        Supply.store_id == store.id,
                        Supply.status.in_(["READY_TO_SUPPLY", "ACCEPTED_AT_SUPPLY_WAREHOUSE", "IN_TRANSIT"]),
                    )
                ) or 0,
                "today_supplies": await db.scalar(
                    select(func.count(Supply.id)).where(
                        Supply.store_id == store.id,
                        Supply.timeslot_from >= today,
                        Supply.timeslot_from < tomorrow,
                    )
                ) or 0,
                "ordered_units_yesterday": int(round(store_yesterday_summary["units"])),
                "ordered_revenue_yesterday": round(store_yesterday_summary["revenue"], 2),
                "ordered_units_prev_day": int(round(store_compare_summary["units"])),
                "ordered_revenue_prev_day": round(store_compare_summary["revenue"], 2),
                "delta_units_yesterday": int(round(store_yesterday_summary["units"] - store_compare_summary["units"])),
                "returns_units_yesterday": store_returns_units_yesterday,
                "returns_units_prev_day": store_returns_units_prev_day,
                "returns_units_available": store_returns_units_available,
                "returns_units_delta_available": store_returns_delta_available,
                "returns_amount_yesterday": round(store_returns_amount_yesterday, 2),
                "returns_amount_prev_day": round(store_returns_amount_prev_day, 2),
                "top_gainers": store_movers_up,
                "top_losers": store_movers_down,
            })

        movers_up, movers_down = self._build_offer_movers(
            [
                {"offer_id": offer_id, **values}
                for offer_id, values in combined_yesterday_stats.items()
            ],
            [
                {"offer_id": offer_id, **values}
                for offer_id, values in combined_compare_stats.items()
            ],
            limit=5,
        )

        stores_with_orders_yesterday = sum(
            1 for store in store_stats if int(store.get("ordered_units_yesterday") or 0) > 0
        )
        top_store = max(
            store_stats,
            key=lambda store: (
                int(store.get("ordered_units_yesterday") or 0),
                float(store.get("ordered_revenue_yesterday") or 0),
            ),
            default=None,
        )

        stats = {
            "report_date": target_date.isoformat(),
            "compare_date": compare_date.isoformat(),
            "total_supplies": total_supplies or 0,
            "active_supplies": active_supplies or 0,
            "completed_supplies": completed_supplies or 0,
            "cancelled_supplies": cancelled_supplies or 0,
            "today_supplies": today_supplies or 0,
            "total_variants": total_variants,
            "total_units": total_units,
            "total_reserved": total_reserved,
            "total_available": total_available,
            "ordered_units_yesterday": int(round(yesterday_order_units)),
            "ordered_revenue_yesterday": round(yesterday_order_revenue, 2),
            "orders_amount_yesterday": round(finance_orders_amount or yesterday_order_revenue, 2),
            "orders_amount_prev_day": round(finance_orders_amount_prev, 2),
            "returns_amount_yesterday": round(finance_returns_amount, 2),
            "returns_amount_prev_day": round(finance_returns_amount_prev, 2),
            "returns_units_yesterday": returns_units if returns_units_covered > 0 else None,
            "returns_units_prev_day": returns_units_prev if returns_units_prev_covered > 0 else None,
            "returns_units_available": returns_units_covered > 0,
            "returns_units_delta_available": returns_units_covered > 0 and returns_units_prev_covered > 0,
            "sales_stores_covered": sales_stores_covered,
            "finance_stores_covered": finance_covered,
            "latest_sales_snapshot_refresh": latest_snapshot_refresh,
            "stores_total": len(store_stats),
            "stores_with_orders_yesterday": stores_with_orders_yesterday,
            "top_store_name": top_store.get("store_name") if top_store else None,
            "top_store_units_yesterday": int(top_store.get("ordered_units_yesterday") or 0) if top_store else 0,
            "top_store_revenue_yesterday": round(float(top_store.get("ordered_revenue_yesterday") or 0), 2) if top_store else 0.0,
            "top_gainers": movers_up[:5],
            "top_losers": movers_down[:5],
        }

        return stats, store_stats
