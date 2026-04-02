from __future__ import annotations

import csv
import io
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from app.config import settings
from app.services.ozon.report_schema_guard import missing_required_column_groups, notify_ozon_report_columns_changed
from app.services.ozon.report_service import OzonReportReady, OzonReportService
from app.utils.redis_cache import cache_get_json, cache_set_json


@dataclass
class ParsedReportPreview:
    headers: list[str]
    rows: list[dict[str, str]]
    total_rows: int
    summary: dict[str, Any]
    analytics: dict[str, Any]


class OzonReportSnapshotService:
    REQUIRED_COLUMNS: dict[str, tuple[tuple[str, ...], ...]] = {
        "products": (
            ("offer id", "offer_id", "артикул", "ваш sku", "seller sku"),
            ("name", "название товара", "товар"),
        ),
        "postings": (
            ("offer id", "offer_id", "артикул", "ваш sku", "seller sku"),
            ("name", "название товара", "товар"),
            ("quantity", "количество", "кол-во", "qty", "количество товара"),
            ("processed_at", "processed at", "принят в обработку", "дата принятия в обработку", "дата заказа"),
            ("Склад отгрузки", "Shipment warehouse"),
            ("SKU", "sku"),
        ),
    }

    def __init__(self, report_service: OzonReportService):
        self.report_service = report_service

    @staticmethod
    def _snapshot_ttl() -> int:
        return max(int(settings.OZON_REPORT_SNAPSHOT_TTL_SECONDS), 60)

    @staticmethod
    def cache_key_for(client_id: str, kind: str) -> str:
        return f"ozon-report-snapshot:{client_id}:{kind}"

    @staticmethod
    def _normalize_header(value: str) -> str:
        return " ".join(str(value or "").strip().lower().replace("_", " ").split())

    @staticmethod
    def _normalize_dimension(value: str | None) -> str:
        return " ".join(str(value or "").strip().lower().split())

    @classmethod
    def _pick_value(cls, row: dict[str, str], *aliases: str) -> str:
        normalized = {cls._normalize_header(key): value for key, value in row.items()}
        for alias in aliases:
            value = normalized.get(cls._normalize_header(alias))
            if value not in (None, ""):
                return str(value).strip()
        return ""

    @staticmethod
    def _to_number(value: str) -> float | None:
        if value is None:
            return None
        cleaned = str(value).strip().replace(" ", "").replace("\xa0", "").replace(",", ".")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if not value:
            return None

        raw = str(value).strip()
        if not raw:
            return None

        normalized = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass

        for fmt in (
            "%d.%m.%Y %H:%M:%S",
            "%d.%m.%Y %H:%M",
            "%d.%m.%Y",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
        ):
            try:
                parsed = datetime.strptime(raw, fmt)
                return parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

        return None

    @staticmethod
    def _decode_bytes(raw_bytes: bytes) -> str:
        for encoding in ("utf-8-sig", "utf-8", "cp1251"):
            try:
                return raw_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue
        return raw_bytes.decode("utf-8", errors="replace")

    @staticmethod
    def _build_reader(text: str) -> csv.DictReader:
        sample = text[:4096]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
            dialect.delimiter = ";"
        return csv.DictReader(io.StringIO(text), dialect=dialect)

    @classmethod
    def _parse_preview(cls, raw_bytes: bytes, *, kind: str, sample_rows: int = 12) -> ParsedReportPreview:
        text = cls._decode_bytes(raw_bytes)
        reader = cls._build_reader(text)
        headers = [str(header).strip() for header in (reader.fieldnames or []) if header]
        rows: list[dict[str, str]] = []
        total_rows = 0
        offer_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"units": 0.0, "revenue": 0.0, "title": ""})
        warehouse_sku_units: dict[str, dict[str, float]] = defaultdict(dict)
        warehouse_offer_units: dict[str, dict[str, float]] = defaultdict(dict)
        warehouse_totals: dict[str, float] = defaultdict(float)
        warehouse_labels: dict[str, str] = {}
        daily_totals: dict[str, dict[str, float]] = defaultdict(lambda: {"units": 0.0, "revenue": 0.0})
        daily_offer_stats: dict[str, dict[str, dict[str, Any]]] = defaultdict(
            lambda: defaultdict(lambda: {"units": 0.0, "revenue": 0.0, "title": ""})
        )
        daily_warehouse_offer_units: dict[str, dict[str, dict[str, float]]] = defaultdict(
            lambda: defaultdict(dict)
        )

        for raw_row in reader:
            row = {str(key).strip(): str(value).strip() for key, value in raw_row.items() if key}
            if not any(row.values()):
                continue
            total_rows += 1
            if len(rows) < sample_rows:
                rows.append(row)

            offer_id = cls._pick_value(row, "offer id", "offer_id", "артикул", "ваш sku", "seller sku")
            title = cls._pick_value(row, "name", "название товара", "товар")
            quantity = cls._to_number(
                cls._pick_value(
                    row,
                    "quantity",
                    "количество",
                    "кол-во",
                    "qty",
                    "количество товара",
                )
            )
            revenue = cls._to_number(
                cls._pick_value(
                    row,
                    "оплачено покупателем",
                    "сумма отправления",
                    "seller price per instance",
                    "цена продавца с учётом скидки",
                    "сумма продажи",
                    "price",
                    "стоимость",
                    "итого",
                    "сумма",
                    "amount",
                )
            )
            if offer_id:
                stat = offer_stats[offer_id]
                stat["title"] = stat["title"] or title
                if quantity is not None:
                    stat["units"] += quantity
                else:
                    stat["units"] += 1
                if revenue is not None:
                    stat["revenue"] += revenue

            if kind == "postings":
                processed_at = cls._parse_datetime(
                    cls._pick_value(
                        row,
                        "processed_at",
                        "processed at",
                        "принят в обработку",
                        "дата принятия в обработку",
                        "дата заказа",
                        "created_at",
                        "created at",
                        "создано",
                    )
                )
                if processed_at:
                    day_key = processed_at.date().isoformat()
                    daily_totals[day_key]["units"] += quantity if quantity is not None else 1.0
                    if revenue is not None:
                        daily_totals[day_key]["revenue"] += revenue
                    if offer_id:
                        daily_offer = daily_offer_stats[day_key][offer_id]
                        daily_offer["title"] = daily_offer["title"] or title
                        daily_offer["units"] += quantity if quantity is not None else 1.0
                        if revenue is not None:
                            daily_offer["revenue"] += revenue

                shipment_warehouse = cls._pick_value(
                    row,
                    "Склад отгрузки",
                    "Shipment warehouse",
                )
                normalized_warehouse = cls._normalize_dimension(shipment_warehouse)
                quantity_value = quantity if quantity is not None else 1.0
                sku_value = cls._pick_value(row, "SKU", "sku")

                if normalized_warehouse:
                    warehouse_labels.setdefault(normalized_warehouse, shipment_warehouse.strip() or normalized_warehouse)
                    warehouse_totals[normalized_warehouse] += quantity_value

                    if sku_value:
                        sku_map = warehouse_sku_units[normalized_warehouse]
                        sku_map[sku_value] = round(float(sku_map.get(sku_value, 0.0)) + quantity_value, 2)

                    if offer_id:
                        offer_map = warehouse_offer_units[normalized_warehouse]
                        offer_map[offer_id] = round(float(offer_map.get(offer_id, 0.0)) + quantity_value, 2)
                        if processed_at:
                            day_offer_map = daily_warehouse_offer_units[processed_at.date().isoformat()][normalized_warehouse]
                            day_offer_map[offer_id] = round(float(day_offer_map.get(offer_id, 0.0)) + quantity_value, 2)

        top_offers = sorted(
            (
                {
                    "offer_id": offer_id,
                    "title": data["title"],
                    "units": round(data["units"], 2),
                    "revenue": round(data["revenue"], 2),
                }
                for offer_id, data in offer_stats.items()
            ),
            key=lambda item: (item["units"], item["revenue"]),
            reverse=True,
        )

        summary = {
            "kind": kind,
            "total_rows": total_rows,
            "unique_offer_ids": len(offer_stats),
            "total_units": round(sum(item["units"] for item in offer_stats.values()), 2),
            "total_revenue": round(sum(item["revenue"] for item in offer_stats.values()), 2),
            "top_offers": top_offers[:10],
            "offer_stats": top_offers,
        }
        analytics = {
            "shipment_orders": {
                "warehouse_labels": warehouse_labels,
                "warehouse_totals": {key: round(value, 2) for key, value in warehouse_totals.items()},
                "warehouse_sku_units": warehouse_sku_units,
                "warehouse_offer_units": warehouse_offer_units,
                "daily_totals": {
                    day: {
                        "units": round(values["units"], 2),
                        "revenue": round(values["revenue"], 2),
                    }
                    for day, values in daily_totals.items()
                },
                "daily_offer_stats": {
                    day: sorted(
                        (
                            {
                                "offer_id": offer_id,
                                "title": data["title"],
                                "units": round(data["units"], 2),
                                "revenue": round(data["revenue"], 2),
                            }
                            for offer_id, data in offers.items()
                        ),
                        key=lambda item: (item["units"], item["revenue"]),
                        reverse=True,
                    )
                    for day, offers in daily_offer_stats.items()
                },
                "daily_warehouse_offer_units": {
                    day: {
                        warehouse: {offer_id: round(float(units), 2) for offer_id, units in offers.items()}
                        for warehouse, offers in warehouses.items()
                    }
                    for day, warehouses in daily_warehouse_offer_units.items()
                },
            }
            if kind == "postings"
            else {}
        }
        return ParsedReportPreview(
            headers=headers,
            rows=rows,
            total_rows=total_rows,
            summary=summary,
            analytics=analytics,
        )

    @classmethod
    async def _notify_missing_columns_if_needed(
        cls,
        *,
        client_id: str,
        kind: str,
        headers: list[str],
        payload: dict[str, Any] | None = None,
    ) -> None:
        required_groups = cls.REQUIRED_COLUMNS.get(kind) or ()
        if not required_groups:
            return
        missing_groups = missing_required_column_groups(headers, required_groups)
        if not missing_groups:
            return
        await notify_ozon_report_columns_changed(
            endpoint="/v1/report/info",
            client_id=client_id,
            report_name=f"ozon_report_snapshot:{kind}",
            required_groups=missing_groups,
            actual_headers=headers,
            payload=payload,
        )
        raise RuntimeError(
            f"Ozon report columns changed for {kind}: missing groups {', '.join(' / '.join(group) for group in missing_groups)}"
        )

    @classmethod
    def _build_snapshot_payload(
        cls,
        *,
        client_id: str,
        kind: str,
        ready_report: OzonReportReady,
        preview: ParsedReportPreview,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "client_id": client_id,
            "kind": kind,
            "report": {
                "code": ready_report.code,
                "report_type": ready_report.report_type,
                "status": ready_report.status,
                "file_url": ready_report.file_url,
                "created_at": ready_report.created_at,
                "expires_at": ready_report.expires_at,
            },
            "preview": {
                "headers": preview.headers,
                "rows": preview.rows,
                "summary": preview.summary,
            },
            "analytics": preview.analytics,
            "refreshed_at": datetime.now(timezone.utc).isoformat(),
        }
        if extra:
            payload.update(extra)
        return payload

    @classmethod
    async def get_cached_snapshot_for_client(cls, *, client_id: str, kind: str) -> dict[str, Any] | None:
        return await cache_get_json(cls.cache_key_for(client_id, kind))

    async def get_cached_snapshot(self, *, client_id: str, kind: str) -> dict[str, Any] | None:
        return await self.get_cached_snapshot_for_client(client_id=client_id, kind=kind)

    @classmethod
    def get_postings_order_units(
        cls,
        snapshot: dict[str, Any] | None,
        *,
        warehouse_name: str | None,
        sku: str | None = None,
        offer_id: str | None = None,
    ) -> int:
        if not snapshot:
            return 0

        analytics = ((snapshot.get("analytics") or {}).get("shipment_orders") or {})
        warehouse_key = cls._normalize_dimension(warehouse_name)
        if not warehouse_key:
            return 0

        if sku:
            sku_units = (analytics.get("warehouse_sku_units") or {}).get(warehouse_key) or {}
            value = sku_units.get(str(sku).strip())
            if value is not None:
                return int(round(float(value)))

        if offer_id:
            offer_units = (analytics.get("warehouse_offer_units") or {}).get(warehouse_key) or {}
            value = offer_units.get(str(offer_id).strip())
            if value is not None:
                return int(round(float(value)))

        return 0

    @classmethod
    def get_total_postings_order_units(cls, snapshot: dict[str, Any] | None) -> int:
        if not snapshot:
            return 0
        analytics = ((snapshot.get("analytics") or {}).get("shipment_orders") or {})
        warehouse_totals = analytics.get("warehouse_totals") or {}
        return int(round(sum(float(value or 0) for value in warehouse_totals.values())))

    @classmethod
    def get_postings_day_summary(cls, snapshot: dict[str, Any] | None, *, day: str) -> dict[str, float]:
        if not snapshot:
            return {"units": 0.0, "revenue": 0.0}
        analytics = ((snapshot.get("analytics") or {}).get("shipment_orders") or {})
        daily_totals = analytics.get("daily_totals") or {}
        day_summary = daily_totals.get(day) or {}
        return {
            "units": float(day_summary.get("units") or 0),
            "revenue": float(day_summary.get("revenue") or 0),
        }

    @classmethod
    def get_postings_day_offer_stats(cls, snapshot: dict[str, Any] | None, *, day: str) -> list[dict[str, Any]]:
        if not snapshot:
            return []
        analytics = ((snapshot.get("analytics") or {}).get("shipment_orders") or {})
        daily_offer_stats = analytics.get("daily_offer_stats") or {}
        offers_by_day = daily_offer_stats.get(day) or {}
        if isinstance(offers_by_day, list):
            return [item for item in offers_by_day if isinstance(item, dict)]
        if isinstance(offers_by_day, dict):
            return [item for item in offers_by_day.values() if isinstance(item, dict)]
        return []

    @staticmethod
    def _coerce_date(value: date | str | None) -> date | None:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            return None

    @staticmethod
    def _lookup_offer_stats_item(offers_by_day: Any, normalized_offer_id: str) -> dict[str, Any]:
        if isinstance(offers_by_day, dict):
            direct = offers_by_day.get(normalized_offer_id)
            if isinstance(direct, dict):
                return direct
            for item in offers_by_day.values():
                if isinstance(item, dict) and str(item.get("offer_id") or "").strip() == normalized_offer_id:
                    return item
            return {}

        if isinstance(offers_by_day, list):
            for item in offers_by_day:
                if isinstance(item, dict) and str(item.get("offer_id") or "").strip() == normalized_offer_id:
                    return item

        return {}

    @classmethod
    def get_storewide_postings_offer_window_summary(
        cls,
        snapshot: dict[str, Any] | None,
        *,
        offer_id: str | None,
        start: date | str | None = None,
        end: date | str | None = None,
    ) -> dict[str, Any]:
        if not snapshot or not offer_id:
            return {"units": 0.0, "revenue": 0.0, "title": ""}

        normalized_offer_id = str(offer_id).strip()
        if not normalized_offer_id:
            return {"units": 0.0, "revenue": 0.0, "title": ""}

        start_date = cls._coerce_date(start)
        end_date = cls._coerce_date(end)
        analytics = ((snapshot.get("analytics") or {}).get("shipment_orders") or {})
        daily_offer_stats = analytics.get("daily_offer_stats") or {}

        units = 0.0
        revenue = 0.0
        title = ""

        for day_key, offers_by_day in daily_offer_stats.items():
            current_day = cls._coerce_date(day_key)
            if current_day is None:
                continue
            if start_date and current_day < start_date:
                continue
            if end_date and current_day > end_date:
                continue

            item = cls._lookup_offer_stats_item(offers_by_day, normalized_offer_id)
            if not item:
                continue

            units += float(item.get("units") or 0)
            revenue += float(item.get("revenue") or 0)
            title = title or str(item.get("title") or "")

        return {
            "units": round(units, 2),
            "revenue": round(revenue, 2),
            "title": title,
        }

    @classmethod
    def get_warehouse_postings_offer_window_units(
        cls,
        snapshot: dict[str, Any] | None,
        *,
        warehouse_name: str | None,
        offer_id: str | None,
        start: date | str | None = None,
        end: date | str | None = None,
    ) -> int:
        if not snapshot or not warehouse_name or not offer_id:
            return 0

        warehouse_key = cls._normalize_dimension(warehouse_name)
        normalized_offer_id = str(offer_id).strip()
        if not warehouse_key or not normalized_offer_id:
            return 0

        start_date = cls._coerce_date(start)
        end_date = cls._coerce_date(end)
        analytics = ((snapshot.get("analytics") or {}).get("shipment_orders") or {})
        daily_warehouse_offer_units = analytics.get("daily_warehouse_offer_units") or {}

        total_units = 0.0
        for day_key, warehouses in daily_warehouse_offer_units.items():
            current_day = cls._coerce_date(day_key)
            if current_day is None:
                continue
            if start_date and current_day < start_date:
                continue
            if end_date and current_day > end_date:
                continue

            warehouse_offers = (warehouses or {}).get(warehouse_key) or {}
            total_units += float(warehouse_offers.get(normalized_offer_id) or 0)

        return int(round(total_units))

    @classmethod
    def get_storewide_postings_latest_offer_price(
        cls,
        snapshot: dict[str, Any] | None,
        *,
        offer_id: str | None,
        start: date | str | None = None,
        end: date | str | None = None,
    ) -> float | None:
        if not snapshot or not offer_id:
            return None

        normalized_offer_id = str(offer_id).strip()
        if not normalized_offer_id:
            return None

        start_date = cls._coerce_date(start)
        end_date = cls._coerce_date(end)
        analytics = ((snapshot.get("analytics") or {}).get("shipment_orders") or {})
        daily_offer_stats = analytics.get("daily_offer_stats") or {}

        dated_items: list[tuple[date, dict[str, Any]]] = []
        for day_key, offers_by_day in daily_offer_stats.items():
            current_day = cls._coerce_date(day_key)
            if current_day is None:
                continue
            if start_date and current_day < start_date:
                continue
            if end_date and current_day > end_date:
                continue
            item = cls._lookup_offer_stats_item(offers_by_day, normalized_offer_id)
            if item:
                dated_items.append((current_day, item))

        for _, item in sorted(dated_items, key=lambda pair: pair[0], reverse=True):
            units = float(item.get("units") or 0)
            revenue = float(item.get("revenue") or 0)
            if units > 0 and revenue > 0:
                return round(revenue / units, 2)

        return None

    @classmethod
    def get_storewide_postings_order_units(
        cls,
        snapshot: dict[str, Any] | None,
        *,
        sku: str | None = None,
        offer_id: str | None = None,
    ) -> int:
        if not snapshot:
            return 0

        analytics = ((snapshot.get("analytics") or {}).get("shipment_orders") or {})
        total = 0.0

        if sku:
            for sku_map in (analytics.get("warehouse_sku_units") or {}).values():
                total += float((sku_map or {}).get(str(sku).strip(), 0) or 0)
        if total > 0:
            return int(round(total))

        if offer_id:
            for offer_map in (analytics.get("warehouse_offer_units") or {}).values():
                total += float((offer_map or {}).get(str(offer_id).strip(), 0) or 0)

        return int(round(total))

    async def refresh_products_snapshot(self, *, client_id: str, visibility: str = "ALL") -> dict[str, Any]:
        ready = await self.report_service.ensure_products_report(visibility=visibility)
        raw_bytes = await self.report_service.download_ready_report(ready)
        preview = self._parse_preview(raw_bytes, kind="products")
        await self._notify_missing_columns_if_needed(
            client_id=client_id,
            kind="products",
            headers=preview.headers,
            payload={"visibility": visibility},
        )
        payload = self._build_snapshot_payload(
            client_id=client_id,
            kind="products",
            ready_report=ready,
            preview=preview,
            extra={"filters": {"visibility": visibility}},
        )
        await cache_set_json(self.cache_key_for(client_id, "products"), payload, self._snapshot_ttl())
        return payload

    async def refresh_fbo_postings_snapshot(
        self,
        *,
        client_id: str,
        days_back: int = 30,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        start = (now - timedelta(days=max(days_back, 1))).replace(microsecond=0)
        finish = now.replace(microsecond=0)
        ready = await self.report_service.ensure_fbo_postings_report(
            processed_at_from=start.isoformat().replace("+00:00", "Z"),
            processed_at_to=finish.isoformat().replace("+00:00", "Z"),
            analytics_data=True,
        )
        raw_bytes = await self.report_service.download_ready_report(ready)
        preview = self._parse_preview(raw_bytes, kind="postings")
        await self._notify_missing_columns_if_needed(
            client_id=client_id,
            kind="postings",
            headers=preview.headers,
            payload={
                "delivery_schema": "fbo",
                "analytics_data": True,
                "processed_at_from": start.isoformat().replace("+00:00", "Z"),
                "processed_at_to": finish.isoformat().replace("+00:00", "Z"),
            },
        )
        payload = self._build_snapshot_payload(
            client_id=client_id,
            kind="postings",
            ready_report=ready,
            preview=preview,
            extra={
                "filters": {
                    "delivery_schema": "fbo",
                    "analytics_data": True,
                    "processed_at_from": start.isoformat().replace("+00:00", "Z"),
                    "processed_at_to": finish.isoformat().replace("+00:00", "Z"),
                }
            },
        )
        await cache_set_json(self.cache_key_for(client_id, "postings"), payload, self._snapshot_ttl())
        return payload
