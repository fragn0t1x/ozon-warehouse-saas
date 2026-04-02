from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Any

from app.services.ozon.client import OzonClient
from app.services.ozon.report_service import OzonReportService
from app.services.ozon.report_snapshot_service import OzonReportSnapshotService
from app.utils.encryption import decrypt_api_key
from app.utils.redis_cache import cache_get_json


class DashboardSalesService:
    async def _get_snapshot(self, client_id: str) -> dict[str, Any] | None:
        return await cache_get_json(
            OzonReportSnapshotService.cache_key_for(client_id, "postings")
        )

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None

    @staticmethod
    def _shift_month(source: date, months: int) -> date:
        month_index = (source.month - 1) + months
        year = source.year + month_index // 12
        month = month_index % 12 + 1
        return date(year, month, 1)

    @classmethod
    def _sum_daily_window(cls, snapshot: dict[str, Any] | None, *, start: date, end: date) -> dict[str, float]:
        analytics = ((snapshot or {}).get("preview") or {}).get("analytics") or {}
        daily_totals = analytics.get("daily_totals")
        if not daily_totals:
            summary = ((snapshot or {}).get("preview") or {}).get("summary") or {}
            return {
                "units": round(float(summary.get("total_units") or 0), 2),
                "revenue": round(float(summary.get("total_revenue") or 0), 2),
            }

        total_units = 0.0
        total_revenue = 0.0
        current = start
        while current <= end:
            day_summary = OzonReportSnapshotService.get_postings_day_summary(snapshot, day=current.isoformat())
            total_units += float(day_summary.get("units") or 0)
            total_revenue += float(day_summary.get("revenue") or 0)
            current = current.fromordinal(current.toordinal() + 1)
        return {
            "units": round(total_units, 2),
            "revenue": round(total_revenue, 2),
        }

    @classmethod
    def _sum_offer_window(cls, snapshot: dict[str, Any] | None, *, start: date, end: date) -> list[dict[str, Any]]:
        analytics = ((snapshot or {}).get("preview") or {}).get("analytics") or {}
        if not analytics.get("daily_offer_stats"):
            summary = ((snapshot or {}).get("preview") or {}).get("summary") or {}
            return sorted(
                [
                    {
                        "offer_id": str(item.get("offer_id") or ""),
                        "title": str(item.get("title") or ""),
                        "units": round(float(item.get("units") or 0), 2),
                        "revenue": round(float(item.get("revenue") or 0), 2),
                    }
                    for item in (summary.get("top_offers") or [])
                    if str(item.get("offer_id") or "").strip()
                ],
                key=lambda item: (item["units"], item["revenue"]),
                reverse=True,
            )

        offer_totals: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"offer_id": "", "title": "", "units": 0.0, "revenue": 0.0}
        )
        current = start
        while current <= end:
            for item in OzonReportSnapshotService.get_postings_day_offer_stats(snapshot, day=current.isoformat()):
                offer_id = str(item.get("offer_id") or "").strip()
                if not offer_id:
                    continue
                row = offer_totals[offer_id]
                row["offer_id"] = offer_id
                row["title"] = row["title"] or str(item.get("title") or "").strip()
                row["units"] += float(item.get("units") or 0)
                row["revenue"] += float(item.get("revenue") or 0)
            current = current.fromordinal(current.toordinal() + 1)
        return sorted(
            (
                {
                    "offer_id": offer_id,
                    "title": values["title"],
                    "units": round(float(values["units"]), 2),
                    "revenue": round(float(values["revenue"]), 2),
                }
                for offer_id, values in offer_totals.items()
            ),
            key=lambda item: (item["units"], item["revenue"]),
            reverse=True,
        )

    @classmethod
    def _snapshot_covers_period(cls, snapshot: dict[str, Any] | None, *, start: date, end: date) -> bool:
        if not snapshot:
            return False

        filters = snapshot.get("filters") or {}
        snapshot_start = cls._parse_dt(filters.get("processed_at_from"))
        snapshot_finish = cls._parse_dt(filters.get("processed_at_to"))
        if snapshot_start is None or snapshot_finish is None:
            return False

        return snapshot_start.date() <= start and snapshot_finish.date() >= end

    async def _ensure_snapshot_range(
        self,
        store: dict[str, Any],
        *,
        start: date,
        end: date,
        current_snapshot: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if self._snapshot_covers_period(current_snapshot, start=start, end=end):
            return current_snapshot

        client_id = str(store.get("client_id") or "").strip()
        api_key_encrypted = store.get("api_key_encrypted")
        if not client_id or not api_key_encrypted:
            return current_snapshot

        client = OzonClient(
            client_id,
            decrypt_api_key(api_key_encrypted),
            store_name=store.get("name") or "Магазин",
            emit_notifications=False,
        )
        try:
            report_service = OzonReportService(client)
            snapshot_service = OzonReportSnapshotService(report_service)
            days_back = max((end - start).days + 1, 31)
            return await snapshot_service.refresh_fbo_postings_snapshot(
                client_id=client_id,
                days_back=days_back,
            )
        except Exception:
            return current_snapshot
        finally:
            await client.close()

    async def build_fbo_sales_summary(
        self,
        *,
        stores: list[dict[str, Any]],
        period_days: int = 30,
    ) -> dict[str, Any]:
        total_units = 0.0
        total_revenue = 0.0
        covered_stores = 0
        top_offers: list[dict[str, Any]] = []
        top_offers_by_store: list[dict[str, Any]] = []
        latest_refresh: datetime | None = None
        month_to_date = {
            "ordered_units": 0.0,
            "ordered_revenue": 0.0,
            "previous_ordered_units": 0.0,
            "previous_ordered_revenue": 0.0,
        }
        month_to_date_covered = 0

        today = datetime.now().date()
        current_month_start = today.replace(day=1)
        previous_month_start = self._shift_month(current_month_start, -1)
        days_elapsed = max((today - current_month_start).days, 0)
        previous_month_same_day = previous_month_start.fromordinal(
            min(
                previous_month_start.toordinal() + days_elapsed,
                self._shift_month(current_month_start, 0).toordinal() - 1,
            )
        )

        for store in stores:
            client_id = str(store.get("client_id") or "").strip()
            if not client_id:
                continue

            snapshot = await self._get_snapshot(client_id)
            snapshot = await self._ensure_snapshot_range(
                store,
                start=previous_month_start,
                end=today,
                current_snapshot=snapshot,
            )
            if not snapshot:
                continue

            covered_stores += 1
            top_period_start = max(today.fromordinal(today.toordinal() - max(period_days - 1, 0)), previous_month_start)
            top_window = self._sum_offer_window(snapshot, start=top_period_start, end=today)
            total_window = self._sum_daily_window(snapshot, start=top_period_start, end=today)
            total_units += float(total_window.get("units") or 0)
            total_revenue += float(total_window.get("revenue") or 0)

            current_window = self._sum_daily_window(snapshot, start=current_month_start, end=today)
            previous_window = self._sum_daily_window(snapshot, start=previous_month_start, end=previous_month_same_day)
            analytics = ((snapshot or {}).get("preview") or {}).get("analytics") or {}
            if analytics.get("daily_totals"):
                month_to_date["ordered_units"] += current_window["units"]
                month_to_date["ordered_revenue"] += current_window["revenue"]
                month_to_date["previous_ordered_units"] += previous_window["units"]
                month_to_date["previous_ordered_revenue"] += previous_window["revenue"]
                month_to_date_covered += 1

            refreshed_at = self._parse_dt(snapshot.get("refreshed_at"))
            if refreshed_at and (latest_refresh is None or refreshed_at > latest_refresh):
                latest_refresh = refreshed_at

            store_top_offers: list[dict[str, Any]] = []
            for offer in top_window:
                item = {
                    "store_id": int(store.get("id") or 0),
                    "store_name": store.get("name") or "Магазин",
                    "offer_id": offer.get("offer_id") or "",
                    "title": offer.get("title") or "",
                    "units": float(offer.get("units") or 0),
                    "revenue": float(offer.get("revenue") or 0),
                }
                top_offers.append(item)
                store_top_offers.append(item)

            store_top_offers.sort(key=lambda item: (item["units"], item["revenue"]), reverse=True)
            top_offers_by_store.append(
                {
                    "store_id": int(store.get("id") or 0),
                    "store_name": store.get("name") or "Магазин",
                    "updated_at": refreshed_at.isoformat() if refreshed_at else None,
                    "items": store_top_offers[:5],
                }
            )

        top_offers.sort(key=lambda item: (item["units"], item["revenue"]), reverse=True)
        top_offers_by_store.sort(
            key=lambda item: (
                1 if item["items"] else 0,
                item["updated_at"] or "",
                item["store_name"],
            ),
            reverse=True,
        )

        return {
            "source": "report_snapshot" if covered_stores else "unavailable",
            "period_days": period_days,
            "total_units": round(total_units, 2),
            "total_revenue": round(total_revenue, 2),
            "stores_covered": covered_stores,
            "stores_missing": max(len(stores) - covered_stores, 0),
            "updated_at": latest_refresh.isoformat() if latest_refresh else None,
            "top_offers": top_offers[:8],
            "top_offers_by_store": top_offers_by_store,
            "month_to_date": {
                "available": month_to_date_covered > 0,
                "stores_covered": month_to_date_covered,
                "stores_missing": max(len(stores) - month_to_date_covered, 0),
                "ordered_units": round(month_to_date["ordered_units"], 2) if month_to_date_covered > 0 else 0.0,
                "ordered_revenue": round(month_to_date["ordered_revenue"], 2) if month_to_date_covered > 0 else 0.0,
                "previous_ordered_units": round(month_to_date["previous_ordered_units"], 2) if month_to_date_covered > 0 else 0.0,
                "previous_ordered_revenue": round(month_to_date["previous_ordered_revenue"], 2) if month_to_date_covered > 0 else 0.0,
                "delta_ordered_units": round(month_to_date["ordered_units"] - month_to_date["previous_ordered_units"], 2) if month_to_date_covered > 0 else 0.0,
                "delta_ordered_revenue": round(month_to_date["ordered_revenue"] - month_to_date["previous_ordered_revenue"], 2) if month_to_date_covered > 0 else 0.0,
                "period_label": f"{current_month_start.isoformat()} - {today.isoformat()}",
                "compare_period_label": f"{previous_month_start.isoformat()} - {previous_month_same_day.isoformat()}",
            },
        }
