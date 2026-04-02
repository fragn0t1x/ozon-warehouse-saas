from __future__ import annotations

import asyncio
import csv
import io
import re
import zipfile
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from typing import Any
from xml.etree import ElementTree as ET

import httpx

from app.config import settings
from app.services.ozon.report_schema_guard import normalize_report_header, notify_ozon_report_columns_changed
from app.services.ozon.report_service import OzonReportService
from app.utils.redis_cache import cache_get_json, cache_set_json


class OzonFinanceSnapshotService:
    REQUIRED_REPORT_COLUMNS: dict[str, tuple[tuple[str, ...], ...]] = {
        "placement_by_products": (
            ("offer_id", "offer id", "артикул", "идентификатор товара в системе продавца", "идентификатор товара продавца"),
            ("name", "наименование товара", "название товара", "товар"),
        ),
        "placement_by_supplies": (
            ("номер поставки", "supply id", "supply_id", "идентификатор поставки", "order_number", "номер"),
            ("склад", "склад поставки", "warehouse", "warehouse_name", "название склада"),
        ),
        "adjustment_amount": (
            ("сумма", "amount", "итого", "total", "сумма компенсации", "сумма декомпенсации", "стоимость", "к начислению", "к списанию"),
        ),
    }

    def __init__(self, client):
        self.client = client
        self.report_service = OzonReportService(client)

    @staticmethod
    def cache_key_for(client_id: str) -> str:
        return f"ozon-finance-snapshot:{client_id}"

    @staticmethod
    def placement_cache_key_for(client_id: str, *, date_from: str, date_to: str) -> str:
        return f"ozon-finance-placement-products:v2:{client_id}:{date_from}:{date_to}"

    @staticmethod
    def placement_supplies_cache_key_for(client_id: str, *, date_from: str, date_to: str) -> str:
        return f"ozon-finance-placement-supplies:v2:{client_id}:{date_from}:{date_to}"

    @staticmethod
    def _snapshot_ttl() -> int:
        return max(int(settings.OZON_REPORT_SNAPSHOT_TTL_SECONDS), 60)

    @staticmethod
    def _placement_snapshot_ttl() -> int:
        return 24 * 60 * 60

    @staticmethod
    def _to_float(value: Any) -> float:
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _month_keys(start: datetime, finish: datetime) -> list[str]:
        current = datetime(start.year, start.month, 1, tzinfo=timezone.utc)
        last = datetime(finish.year, finish.month, 1, tzinfo=timezone.utc)
        result: list[str] = []
        while current <= last:
            result.append(current.strftime("%Y-%m"))
            if current.month == 12:
                current = datetime(current.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                current = datetime(current.year, current.month + 1, 1, tzinfo=timezone.utc)
        return result

    @staticmethod
    def _decode_bytes(raw_bytes: bytes) -> str:
        for encoding in ("utf-8-sig", "utf-8", "cp1251"):
            try:
                return raw_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue
        return raw_bytes.decode("utf-8", errors="replace")

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
    def _xlsx_namespace() -> dict[str, str]:
        return {
            "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
            "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
            "pkgrel": "http://schemas.openxmlformats.org/package/2006/relationships",
        }

    @staticmethod
    def _xlsx_col_index(cell_ref: str) -> int:
        match = re.match(r"([A-Z]+)", str(cell_ref or "").upper())
        if not match:
            return 0
        index = 0
        for char in match.group(1):
            index = index * 26 + (ord(char) - ord("A") + 1)
        return index

    @classmethod
    def _xlsx_shared_strings(cls, workbook: zipfile.ZipFile) -> list[str]:
        if "xl/sharedStrings.xml" not in workbook.namelist():
            return []

        root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
        ns = cls._xlsx_namespace()
        strings: list[str] = []
        for si in root.findall("main:si", ns):
            parts = [
                (node.text or "")
                for node in si.findall(".//main:t", ns)
            ]
            strings.append("".join(parts).strip())
        return strings

    @classmethod
    def _xlsx_first_sheet_path(cls, workbook: zipfile.ZipFile) -> str | None:
        ns = cls._xlsx_namespace()
        names = set(workbook.namelist())
        if "xl/workbook.xml" in names and "xl/_rels/workbook.xml.rels" in names:
            workbook_root = ET.fromstring(workbook.read("xl/workbook.xml"))
            rels_root = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
            rel_map = {
                rel.attrib.get("Id"): rel.attrib.get("Target")
                for rel in rels_root.findall("pkgrel:Relationship", ns)
            }
            first_sheet = workbook_root.find("main:sheets/main:sheet", ns)
            if first_sheet is not None:
                rel_id = first_sheet.attrib.get(f"{{{ns['rel']}}}id")
                target = rel_map.get(rel_id or "")
                if target:
                    if target.startswith("/"):
                        return target.lstrip("/")
                    if target.startswith("xl/"):
                        return target
                    return f"xl/{target}"

        sheet_candidates = sorted(
            name
            for name in names
            if name.startswith("xl/worksheets/sheet") and name.endswith(".xml")
        )
        return sheet_candidates[0] if sheet_candidates else None

    @classmethod
    def _xlsx_cell_value(
        cls,
        cell: ET.Element,
        *,
        shared_strings: list[str],
        ns: dict[str, str],
    ) -> str:
        cell_type = cell.attrib.get("t")
        if cell_type == "inlineStr":
            parts = [(node.text or "") for node in cell.findall(".//main:t", ns)]
            return "".join(parts).strip()

        raw_value = cell.findtext("main:v", default="", namespaces=ns)
        if cell_type == "s":
            index = cls._to_int(raw_value)
            if 0 <= index < len(shared_strings):
                return shared_strings[index]
            return ""
        if raw_value not in (None, ""):
            return str(raw_value).strip()

        formula_value = cell.findtext("main:f", default="", namespaces=ns)
        return str(formula_value or "").strip()

    @classmethod
    def _extract_xlsx_dict_rows(cls, raw_bytes: bytes) -> list[dict[str, str]] | None:
        buffer = io.BytesIO(raw_bytes)
        if not zipfile.is_zipfile(buffer):
            return None

        buffer.seek(0)
        with zipfile.ZipFile(buffer) as workbook:
            sheet_path = cls._xlsx_first_sheet_path(workbook)
            if not sheet_path or sheet_path not in workbook.namelist():
                return None

            ns = cls._xlsx_namespace()
            shared_strings = cls._xlsx_shared_strings(workbook)
            sheet_root = ET.fromstring(workbook.read(sheet_path))
            value_rows: list[list[str]] = []

            for row in sheet_root.findall(".//main:sheetData/main:row", ns):
                cells: dict[int, str] = {}
                max_col = 0
                next_col = 1

                for cell in row.findall("main:c", ns):
                    col_index = cls._xlsx_col_index(cell.attrib.get("r", ""))
                    if col_index <= 0:
                        col_index = next_col
                    max_col = max(max_col, col_index)
                    cells[col_index] = cls._xlsx_cell_value(
                        cell,
                        shared_strings=shared_strings,
                        ns=ns,
                    )
                    next_col = col_index + 1

                if max_col <= 0:
                    continue

                values = [cells.get(index, "").strip() for index in range(1, max_col + 1)]
                if not any(values):
                    continue
                value_rows.append(values)

            if not value_rows:
                return []

            header: list[str] = []
            dict_rows: list[dict[str, str]] = []
            for values in value_rows:
                if not header:
                    header = values
                    continue

                row_dict: dict[str, str] = {}
                for index, column_name in enumerate(header, start=1):
                    column_name = str(column_name or "").strip()
                    if not column_name:
                        continue
                    row_dict[column_name] = values[index - 1] if index - 1 < len(values) else ""
                if any(str(value or "").strip() for value in row_dict.values()):
                    dict_rows.append(row_dict)

            return dict_rows

    @classmethod
    def _extract_xlsx_value_rows(cls, raw_bytes: bytes) -> list[list[str]] | None:
        buffer = io.BytesIO(raw_bytes)
        if not zipfile.is_zipfile(buffer):
            return None

        buffer.seek(0)
        with zipfile.ZipFile(buffer) as workbook:
            sheet_path = cls._xlsx_first_sheet_path(workbook)
            if not sheet_path or sheet_path not in workbook.namelist():
                return None

            ns = cls._xlsx_namespace()
            shared_strings = cls._xlsx_shared_strings(workbook)
            sheet_root = ET.fromstring(workbook.read(sheet_path))
            rows: list[list[str]] = []

            for row in sheet_root.findall(".//main:sheetData/main:row", ns):
                cells: dict[int, str] = {}
                max_col = 0
                next_col = 1
                for cell in row.findall("main:c", ns):
                    col_index = cls._xlsx_col_index(cell.attrib.get("r", ""))
                    if col_index <= 0:
                        col_index = next_col
                    max_col = max(max_col, col_index)
                    cells[col_index] = cls._xlsx_cell_value(
                        cell,
                        shared_strings=shared_strings,
                        ns=ns,
                    )
                    next_col = col_index + 1

                if max_col <= 0:
                    rows.append([])
                    continue

                values = [cells.get(index, "").strip() for index in range(1, max_col + 1)]
                rows.append(values)

            return rows

    @classmethod
    def _extract_report_headers(cls, raw_bytes: bytes) -> list[str]:
        xlsx_value_rows = cls._extract_xlsx_value_rows(raw_bytes)
        if xlsx_value_rows is not None:
            for row in xlsx_value_rows:
                headers = [str(value or "").strip() for value in row if str(value or "").strip()]
                if headers:
                    return headers

        text = cls._decode_bytes(raw_bytes).replace("\r\n", "\n").replace("\r", "\n")
        sample = text[:4096]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
            dialect.delimiter = ";"
        return [str(header).strip() for header in (csv.DictReader(io.StringIO(text), dialect=dialect).fieldnames or []) if str(header).strip()]

    @classmethod
    def _extract_report_headers_for_groups(
        cls,
        raw_bytes: bytes,
        required_groups: tuple[tuple[str, ...], ...],
    ) -> list[str]:
        xlsx_value_rows = cls._extract_xlsx_value_rows(raw_bytes)
        if xlsx_value_rows is not None:
            normalized_groups = [
                tuple(cls._normalize_header(alias) for alias in group if cls._normalize_header(alias))
                for group in required_groups
            ]
            best_headers: list[str] = []
            best_score: tuple[int, int, int] | None = None

            for row_index, row_values in enumerate(xlsx_value_rows):
                normalized_cells = [cls._normalize_header(value) for value in row_values]
                filled_headers = [str(value or "").strip() for value in row_values if str(value or "").strip()]
                if not filled_headers:
                    continue

                matched_groups = 0
                for group in normalized_groups:
                    if any(alias and any(alias in cell for cell in normalized_cells) for alias in group):
                        matched_groups += 1

                score = (matched_groups, len(filled_headers), -row_index)
                if best_score is None or score > best_score:
                    best_score = score
                    best_headers = filled_headers

            if best_headers:
                return best_headers

        return cls._extract_report_headers(raw_bytes)

    async def _ensure_report_columns(
        self,
        *,
        raw_bytes: bytes,
        report_kind: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        required_groups = self.REQUIRED_REPORT_COLUMNS.get(report_kind) or ()
        if not required_groups:
            return
        headers = self._extract_report_headers_for_groups(raw_bytes, required_groups)
        normalized_headers = [normalize_report_header(header) for header in headers if normalize_report_header(header)]
        missing_groups: list[list[str]] = []
        for group in required_groups:
            normalized_group = [
                normalize_report_header(alias)
                for alias in group
                if normalize_report_header(alias)
            ]
            if not normalized_group:
                continue
            matched = False
            for alias in normalized_group:
                if any(header == alias or alias in header for header in normalized_headers):
                    matched = True
                    break
            if not matched:
                missing_groups.append(list(group))
        if not missing_groups:
            return
        await notify_ozon_report_columns_changed(
            endpoint="/v1/report/info",
            client_id=self.client.client_id,
            report_name=f"finance_snapshot:{report_kind}",
            required_groups=missing_groups,
            actual_headers=headers,
            payload=payload,
        )
        raise RuntimeError(f"Ozon report columns changed for {report_kind}")

    @staticmethod
    def _half_month_periods(start: datetime, finish: datetime) -> list[tuple[datetime, datetime]]:
        """
        Ozon cash-flow принимает только полу-месячные интервалы:
        01-15 и 16-конец месяца.
        Возвращаем только те интервалы, которые пересекаются с нужным диапазоном.
        """
        cursor = datetime(start.year, start.month, 1, tzinfo=timezone.utc)
        end_month = datetime(finish.year, finish.month, 1, tzinfo=timezone.utc)

        periods: list[tuple[datetime, datetime]] = []

        while cursor <= end_month:
            year = cursor.year
            month = cursor.month
            last_day = monthrange(year, month)[1]

            first_half_start = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
            first_half_end = datetime(year, month, 15, 23, 59, 59, tzinfo=timezone.utc)

            second_half_start = datetime(year, month, 16, 0, 0, 0, tzinfo=timezone.utc)
            second_half_end = datetime(year, month, last_day, 23, 59, 59, tzinfo=timezone.utc)

            for period_start, period_end in (
                (first_half_start, first_half_end),
                (second_half_start, second_half_end),
            ):
                if period_end < start or period_start > finish:
                    continue
                periods.append((period_start, period_end))

            if month == 12:
                cursor = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                cursor = datetime(year, month + 1, 1, tzinfo=timezone.utc)

        return periods

    @staticmethod
    def _to_int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _to_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        normalized = str(value or "").strip().lower()
        return normalized in {"1", "true", "yes", "да"}

    @staticmethod
    def _last_closed_month(now: datetime) -> tuple[int, int, str]:
        month = now.month - 1
        year = now.year
        if month == 0:
            month = 12
            year -= 1
        return year, month, f"{year:04d}-{month:02d}"

    @staticmethod
    def _shift_month(year: int, month: int, offset: int) -> tuple[int, int, str]:
        absolute = year * 12 + (month - 1) + offset
        shifted_year = absolute // 12
        shifted_month = absolute % 12 + 1
        return shifted_year, shifted_month, f"{shifted_year:04d}-{shifted_month:02d}"

    @classmethod
    def _build_realization_snapshot(cls, report: dict[str, Any], *, year: int, month: int) -> dict[str, Any]:
        rows = list(report.get("rows") or [])
        offer_stats: dict[str, dict[str, Any]] = {}

        sold_units = 0
        sold_amount = 0.0
        sold_total = 0.0
        sold_fee = 0.0
        sold_bonus = 0.0
        sold_incentives = 0.0
        returned_units = 0
        returned_amount = 0.0
        returned_total = 0.0
        returned_fee = 0.0
        returned_bonus = 0.0
        returned_incentives = 0.0

        for row in rows:
            item = row.get("item") or {}
            offer_id = str(item.get("offer_id") or "").strip()
            title = str(item.get("name") or "").strip()
            sku = item.get("sku")

            sale = row.get("delivery_commission") or {}
            returned = row.get("return_commission") or {}

            sale_units = cls._to_int(sale.get("quantity"))
            sale_amount = cls._to_float(sale.get("amount"))
            sale_total = cls._to_float(sale.get("total"))
            sale_fee = cls._to_float(sale.get("standard_fee"))
            return_units = cls._to_int(returned.get("quantity"))
            return_amount = cls._to_float(returned.get("amount"))
            return_total = cls._to_float(returned.get("total"))
            return_fee = cls._to_float(returned.get("standard_fee"))

            sale_bonus = cls._to_float(sale.get("bonus"))
            return_bonus = cls._to_float(returned.get("bonus"))
            sale_incentives = sale_bonus + cls._to_float(sale.get("bank_coinvestment")) + cls._to_float(sale.get("stars")) + cls._to_float(sale.get("pick_up_point_coinvestment"))
            return_incentives = return_bonus + cls._to_float(returned.get("bank_coinvestment")) + cls._to_float(returned.get("stars")) + cls._to_float(returned.get("pick_up_point_coinvestment"))

            sold_units += sale_units
            sold_amount += sale_amount
            sold_total += sale_total
            sold_fee += sale_fee
            sold_bonus += sale_bonus
            sold_incentives += sale_incentives
            returned_units += return_units
            returned_amount += return_amount
            returned_total += return_total
            returned_fee += return_fee
            returned_bonus += return_bonus
            returned_incentives += return_incentives

            if not offer_id:
                continue

            stat = offer_stats.setdefault(
                offer_id,
                {
                    "offer_id": offer_id,
                    "title": title,
                    "sku": sku,
                    "sold_units": 0,
                    "sold_amount": 0.0,
                    "sold_total": 0.0,
                    "sold_fee": 0.0,
                    "sold_bonus": 0.0,
                    "sold_incentives": 0.0,
                    "returned_units": 0,
                    "returned_amount": 0.0,
                    "returned_total": 0.0,
                    "returned_fee": 0.0,
                    "returned_bonus": 0.0,
                    "returned_incentives": 0.0,
                    "net_units": 0,
                    "net_amount": 0.0,
                    "net_total": 0.0,
                    "net_fee": 0.0,
                    "net_bonus": 0.0,
                    "net_incentives": 0.0,
                },
            )
            stat["title"] = stat["title"] or title
            stat["sku"] = stat["sku"] or sku
            stat["sold_units"] += sale_units
            stat["sold_amount"] += sale_amount
            stat["sold_total"] += sale_total
            stat["sold_fee"] += sale_fee
            stat["sold_bonus"] += sale_bonus
            stat["sold_incentives"] += sale_incentives
            stat["returned_units"] += return_units
            stat["returned_amount"] += return_amount
            stat["returned_total"] += return_total
            stat["returned_fee"] += return_fee
            stat["returned_bonus"] += return_bonus
            stat["returned_incentives"] += return_incentives
            stat["net_units"] = stat["sold_units"] - stat["returned_units"]
            stat["net_amount"] = round(stat["sold_amount"] - stat["returned_amount"], 2)
            stat["net_total"] = round(stat["sold_total"] - stat["returned_total"], 2)
            stat["net_fee"] = round(stat["sold_fee"] - stat["returned_fee"], 2)
            stat["net_bonus"] = round(stat["sold_bonus"] - stat["returned_bonus"], 2)
            stat["net_incentives"] = round(stat["sold_incentives"] - stat["returned_incentives"], 2)

        items = sorted(
            (
                {
                    **item,
                    "sold_amount": round(float(item["sold_amount"]), 2),
                    "sold_total": round(float(item["sold_total"]), 2),
                    "sold_fee": round(float(item["sold_fee"]), 2),
                    "sold_bonus": round(float(item["sold_bonus"]), 2),
                    "sold_incentives": round(float(item["sold_incentives"]), 2),
                    "returned_amount": round(float(item["returned_amount"]), 2),
                    "returned_total": round(float(item["returned_total"]), 2),
                    "returned_fee": round(float(item["returned_fee"]), 2),
                    "returned_bonus": round(float(item["returned_bonus"]), 2),
                    "returned_incentives": round(float(item["returned_incentives"]), 2),
                    "net_amount": round(float(item["net_amount"]), 2),
                    "net_total": round(float(item["net_total"]), 2),
                    "net_fee": round(float(item["net_fee"]), 2),
                    "net_bonus": round(float(item["net_bonus"]), 2),
                    "net_incentives": round(float(item["net_incentives"]), 2),
                }
                for item in offer_stats.values()
            ),
            key=lambda item: (item["net_amount"], item["sold_amount"]),
            reverse=True,
        )

        return {
            "period": f"{year:04d}-{month:02d}",
            "year": year,
            "month": month,
            "header": report.get("header") or {},
            "rows_count": len(rows),
            "sold_units": sold_units,
            "sold_amount": round(sold_amount, 2),
            "sold_total": round(sold_total, 2),
            "sold_fee": round(sold_fee, 2),
            "sold_bonus": round(sold_bonus, 2),
            "sold_incentives": round(sold_incentives, 2),
            "returned_units": returned_units,
            "returned_amount": round(returned_amount, 2),
            "returned_total": round(returned_total, 2),
            "returned_fee": round(returned_fee, 2),
            "returned_bonus": round(returned_bonus, 2),
            "returned_incentives": round(returned_incentives, 2),
            "net_units": sold_units - returned_units,
            "net_amount": round(sold_amount - returned_amount, 2),
            "net_total": round(sold_total - returned_total, 2),
            "net_fee": round(sold_fee - returned_fee, 2),
            "net_bonus": round(sold_bonus - returned_bonus, 2),
            "net_incentives": round(sold_incentives - returned_incentives, 2),
            "items": items,
            "top_items": items[:10],
            "top_returns": sorted(items, key=lambda item: item["returned_amount"], reverse=True)[:10],
        }

    @classmethod
    def _parse_placement_by_products_report(
        cls,
        raw_bytes: bytes,
        *,
        date_from: date,
        date_to: date,
    ) -> dict[str, Any]:
        text = cls._decode_bytes(raw_bytes).replace("\r\n", "\n").replace("\r", "\n")
        sample = text[:4096]

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
            dialect.delimiter = ";"

        offer_stats: dict[str, dict[str, Any]] = {}
        rows_count = 0
        amount_total = 0.0
        preview_rows: list[dict[str, str]] = []

        def consume_rows(dict_rows):
            nonlocal rows_count, amount_total, preview_rows
            for raw_row in dict_rows:
                row = {
                    str(key).strip(): str(value).strip()
                    for key, value in raw_row.items()
                    if key is not None
                }
                if not any(row.values()):
                    continue

                rows_count += 1
                if len(preview_rows) < 8:
                    preview_rows.append(row)

                offer_id = cls._pick_value(
                    row,
                    "offer_id",
                    "offer id",
                    "артикул",
                    "идентификатор товара в системе продавца",
                    "идентификатор товара продавца",
                )
                title = cls._pick_value(
                    row,
                    "name",
                    "наименование товара",
                    "название товара",
                    "товар",
                )
                amount = cls._to_float(
                    cls._pick_value(
                        row,
                        "стоимость размещения",
                        "стоимость размещения, руб.",
                        "начисленная стоимость размещения",
                        "сумма размещения",
                        "размещение, руб.",
                        "к оплате",
                        "итого",
                        "стоимость",
                        "amount",
                    ).replace(" ", "").replace("\xa0", "").replace(",", ".")
                )
                quantity = cls._to_int(
                    cls._pick_value(
                        row,
                        "количество",
                        "кол-во экземпляров",
                        "кол-во",
                        "qty",
                    )
                )
                days = cls._to_int(
                    cls._pick_value(
                        row,
                        "дней размещения",
                        "дни размещения",
                        "дней",
                    )
                )

                amount_total += amount

                if not offer_id:
                    continue

                item = offer_stats.setdefault(
                    offer_id,
                    {
                        "offer_id": offer_id,
                        "title": title or offer_id,
                        "amount": 0.0,
                        "quantity": 0,
                        "days": 0,
                    },
                )
                if title and not item["title"]:
                    item["title"] = title
                item["amount"] += amount
                item["quantity"] += quantity
                item["days"] = max(int(item["days"]), days)

        xlsx_rows = cls._extract_xlsx_dict_rows(raw_bytes)
        if xlsx_rows is not None:
            consume_rows(xlsx_rows)
        else:
            try:
                stream = io.StringIO(text, newline="")
                reader = csv.DictReader(stream, dialect=dialect)
                consume_rows(reader)
            except csv.Error:
                lines = [line for line in text.split("\n") if line.strip()]
                if lines:
                    reader = csv.reader(lines, delimiter=dialect.delimiter)
                    parsed_rows = list(reader)
                    if parsed_rows:
                        header = parsed_rows[0]
                        dict_rows = [dict(zip(header, values)) for values in parsed_rows[1:]]
                        consume_rows(dict_rows)

        items = sorted(
            (
                {
                    "offer_id": item["offer_id"],
                    "title": item["title"],
                    "amount": round(float(item["amount"]), 2),
                    "quantity": int(item["quantity"]),
                    "days": int(item["days"]),
                }
                for item in offer_stats.values()
            ),
            key=lambda item: item["amount"],
            reverse=True,
        )

        return {
            "period": {
                "from": date_from.isoformat(),
                "to": date_to.isoformat(),
                "days": max((date_to - date_from).days + 1, 1),
            },
            "rows_count": rows_count,
            "offers_count": len(items),
            "amount_total": round(amount_total, 2),
            "items": items,
            "top_items": items[:10],
            "preview_rows": preview_rows,
        }

    @classmethod
    def _build_removal_snapshot(
        cls,
        rows: list[dict[str, Any]],
        *,
        date_from: date,
        date_to: date,
        kind: str,
    ) -> dict[str, Any]:
        kind_label = "Со стока" if kind == "from_stock" else "С поставки"
        offer_stats: dict[str, dict[str, Any]] = {}
        state_stats: dict[str, dict[str, Any]] = {}
        rows_count = 0
        quantity_total = 0
        delivery_price_total = 0.0
        auto_returns_count = 0
        utilization_count = 0
        unique_return_ids: set[str] = set()
        preview_rows: list[dict[str, Any]] = []

        for raw_row in rows:
            row = dict(raw_row or {})
            rows_count += 1
            if len(preview_rows) < 8:
                preview_rows.append(row)

            offer_id = str(row.get("offer_id") or "").strip()
            title = str(row.get("name") or "").strip()
            quantity = cls._to_int(row.get("quantity_for_return"))
            delivery_price = cls._to_float(row.get("preliminary_delivery_price"))
            is_auto_return = cls._to_bool(row.get("is_auto_return"))
            return_state = str(row.get("return_state") or "").strip() or "Без статуса"
            stock_type = str(row.get("stock_type") or "").strip()
            delivery_type = str(row.get("delivery_type") or "").strip()
            box_state = str(row.get("box_state") or "").strip()
            utilization_date = str(row.get("utilization_date") or "").strip()
            is_utilized = bool(utilization_date) or "утилиз" in return_state.lower() or "утилиз" in box_state.lower()

            quantity_total += quantity
            delivery_price_total += delivery_price
            if is_auto_return:
                auto_returns_count += 1
            if is_utilized:
                utilization_count += 1

            return_id = str(row.get("return_id") or "").strip()
            if return_id:
                unique_return_ids.add(return_id)

            state_item = state_stats.setdefault(
                return_state,
                {
                    "state": return_state,
                    "count": 0,
                    "quantity_total": 0,
                    "delivery_price_total": 0.0,
                },
            )
            state_item["count"] += 1
            state_item["quantity_total"] += quantity
            state_item["delivery_price_total"] += delivery_price

            if not offer_id:
                continue

            offer_item = offer_stats.setdefault(
                offer_id,
                {
                    "offer_id": offer_id,
                    "title": title,
                    "quantity_total": 0,
                    "delivery_price_total": 0.0,
                    "rows_count": 0,
                    "auto_returns_count": 0,
                    "utilization_count": 0,
                    "last_return_state": return_state,
                    "delivery_type": delivery_type,
                    "stock_type": stock_type,
                },
            )
            if title and not offer_item["title"]:
                offer_item["title"] = title
            if delivery_type and not offer_item["delivery_type"]:
                offer_item["delivery_type"] = delivery_type
            if stock_type and not offer_item["stock_type"]:
                offer_item["stock_type"] = stock_type
            offer_item["quantity_total"] += quantity
            offer_item["delivery_price_total"] += delivery_price
            offer_item["rows_count"] += 1
            offer_item["auto_returns_count"] += 1 if is_auto_return else 0
            offer_item["utilization_count"] += 1 if is_utilized else 0
            offer_item["last_return_state"] = return_state

        items = sorted(
            (
                {
                    "offer_id": item["offer_id"],
                    "title": item["title"],
                    "quantity_total": int(item["quantity_total"]),
                    "delivery_price_total": round(float(item["delivery_price_total"]), 2),
                    "rows_count": int(item["rows_count"]),
                    "auto_returns_count": int(item["auto_returns_count"]),
                    "utilization_count": int(item["utilization_count"]),
                    "last_return_state": item["last_return_state"],
                    "delivery_type": item["delivery_type"],
                    "stock_type": item["stock_type"],
                }
                for item in offer_stats.values()
            ),
            key=lambda item: (item["delivery_price_total"], item["quantity_total"], item["rows_count"]),
            reverse=True,
        )
        states = sorted(
            (
                {
                    "state": item["state"],
                    "count": int(item["count"]),
                    "quantity_total": int(item["quantity_total"]),
                    "delivery_price_total": round(float(item["delivery_price_total"]), 2),
                }
                for item in state_stats.values()
            ),
            key=lambda item: (item["delivery_price_total"], item["quantity_total"], item["count"]),
            reverse=True,
        )

        return {
            "kind": kind,
            "kind_label": kind_label,
            "period": {
                "from": date_from.isoformat(),
                "to": date_to.isoformat(),
                "days": max((date_to - date_from).days + 1, 1),
            },
            "rows_count": rows_count,
            "returns_count": len(unique_return_ids),
            "offers_count": len(items),
            "quantity_total": quantity_total,
            "delivery_price_total": round(delivery_price_total, 2),
            "auto_returns_count": auto_returns_count,
            "utilization_count": utilization_count,
            "items": items,
            "top_items": items[:10],
            "states": states,
            "top_states": states[:10],
            "preview_rows": preview_rows,
        }

    @classmethod
    def _parse_placement_by_supplies_report(
        cls,
        raw_bytes: bytes,
        *,
        date_from: date,
        date_to: date,
    ) -> dict[str, Any]:
        text = cls._decode_bytes(raw_bytes).replace("\r\n", "\n").replace("\r", "\n")
        sample = text[:4096]

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
            dialect.delimiter = ";"

        supply_stats: dict[str, dict[str, Any]] = {}
        rows_count = 0
        amount_total = 0.0
        stock_days_total = 0
        metric_kind = "amount"
        preview_rows: list[dict[str, str]] = []

        def daily_stock_days_from_row(row: dict[str, str]) -> int:
            total = 0.0
            for key, value in row.items():
                normalized = cls._normalize_header(key)
                if re.match(r"^\d{2}\.", normalized):
                    total += cls._to_float(str(value).replace(" ", "").replace("\xa0", "").replace(",", "."))
            return int(round(total))

        def daily_days_from_row(row: dict[str, str]) -> int:
            count = 0
            for key, value in row.items():
                normalized = cls._normalize_header(key)
                if re.match(r"^\d{2}\.", normalized) and str(value or "").strip() not in {"", "0", "0.0", "0.00"}:
                    count += 1
            return count

        def consume_rows(dict_rows):
            nonlocal rows_count, amount_total, stock_days_total, metric_kind, preview_rows
            for raw_row in dict_rows:
                row = {
                    str(key).strip(): str(value).strip()
                    for key, value in raw_row.items()
                    if key is not None
                }
                if not any(row.values()):
                    continue

                rows_count += 1
                if len(preview_rows) < 8:
                    preview_rows.append(row)

                supply_ref = (
                    cls._pick_value(
                        row,
                        "номер поставки",
                        "supply id",
                        "supply_id",
                        "идентификатор поставки",
                        "order_number",
                        "номер",
                    )
                    or f"row-{rows_count}"
                )
                warehouse_name = cls._pick_value(
                    row,
                    "склад",
                    "склад поставки",
                    "warehouse",
                    "warehouse_name",
                    "название склада",
                )
                amount_value = cls._pick_value(
                    row,
                    "стоимость размещения",
                    "стоимость размещения, руб.",
                    "начисленная стоимость размещения",
                    "сумма размещения",
                    "размещение, руб.",
                    "к оплате",
                    "итого",
                    "стоимость",
                    "amount",
                )
                has_explicit_amount_column = any(
                    cls._normalize_header(key)
                    in {
                        "стоимость размещения",
                        "стоимость размещения, руб.",
                        "начисленная стоимость размещения",
                        "сумма размещения",
                        "размещение, руб.",
                        "к оплате",
                        "итого",
                        "стоимость",
                        "amount",
                    }
                    for key in row
                )
                amount = (
                    cls._to_float(amount_value.replace(" ", "").replace("\xa0", "").replace(",", "."))
                    if amount_value
                    else 0.0
                )
                stock_days = daily_stock_days_from_row(row)
                if not has_explicit_amount_column:
                    metric_kind = "stock_days"
                items_count = cls._to_int(
                    cls._pick_value(
                        row,
                        "количество товаров",
                        "количество",
                        "кол-во",
                        "items_count",
                    )
                )
                days = cls._to_int(
                    cls._pick_value(
                        row,
                        "дней размещения",
                        "дни размещения",
                        "дней",
                    )
                )
                if not days:
                    days = daily_days_from_row(row)

                if not supply_ref or supply_ref.startswith("row-"):
                    continue

                amount_total += amount
                stock_days_total += stock_days
                stat = supply_stats.setdefault(
                    supply_ref,
                    {
                        "supply_ref": supply_ref,
                        "warehouse_name": warehouse_name,
                        "amount": 0.0,
                        "items_count": 0,
                        "days": 0,
                        "stock_days_total": 0,
                    },
                )
                if warehouse_name and not stat["warehouse_name"]:
                    stat["warehouse_name"] = warehouse_name
                stat["amount"] += amount
                stat["items_count"] += items_count
                stat["days"] = max(int(stat["days"]), days)
                stat["stock_days_total"] += stock_days

        xlsx_value_rows = cls._extract_xlsx_value_rows(raw_bytes)
        if xlsx_value_rows is not None:
            header_index = next(
                (
                    index
                    for index, values in enumerate(xlsx_value_rows)
                    if any(cls._normalize_header(value) == "номер поставки" for value in values)
                ),
                None,
            )
            if header_index is not None:
                header = xlsx_value_rows[header_index]
                dict_rows = []
                for values in xlsx_value_rows[header_index + 1 :]:
                    if not any(str(value or "").strip() for value in values):
                        continue
                    dict_rows.append(
                        {
                            str(column or "").strip(): values[index] if index < len(values) else ""
                            for index, column in enumerate(header)
                            if str(column or "").strip()
                        }
                    )
                consume_rows(dict_rows)
        else:
            try:
                stream = io.StringIO(text, newline="")
                reader = csv.DictReader(stream, dialect=dialect)
                consume_rows(reader)
            except csv.Error:
                lines = [line for line in text.split("\n") if line.strip()]
                if lines:
                    reader = csv.reader(lines, delimiter=dialect.delimiter)
                    parsed_rows = list(reader)
                    if parsed_rows:
                        header = parsed_rows[0]
                        dict_rows = [dict(zip(header, values)) for values in parsed_rows[1:]]
                        consume_rows(dict_rows)

        items = sorted(
            (
                {
                    "supply_ref": item["supply_ref"],
                    "warehouse_name": item["warehouse_name"],
                    "amount": round(float(item["amount"]), 2),
                    "items_count": int(item["items_count"]),
                    "days": int(item["days"]),
                    "stock_days_total": int(item["stock_days_total"]),
                }
                for item in supply_stats.values()
            ),
            key=lambda item: (item["amount"], item["stock_days_total"]),
            reverse=True,
        )

        return {
            "period": {
                "from": date_from.isoformat(),
                "to": date_to.isoformat(),
                "days": max((date_to - date_from).days + 1, 1),
            },
            "rows_count": rows_count,
            "supplies_count": len(items),
            "amount_total": round(amount_total, 2),
            "metric_kind": metric_kind,
            "stock_days_total": stock_days_total,
            "items": items,
            "top_items": items[:10],
            "preview_rows": preview_rows,
        }

    @staticmethod
    def _service_bucket(name: str) -> str:
        normalized = str(name or "").lower()
        if any(
            token in normalized
            for token in (
                "promotion",
                "cashback",
                "brand",
                "трафар",
                "search",
                "продвиж",
                "premium",
                "reviews",
                "marketing",
                "stars",
                "клик",
                "реклам",
                "banner",
                "баннер",
                "полка",
            )
        ):
            return "marketing"
        if "storage" in normalized or "размещ" in normalized:
            return "storage"
        if "acquiring" in normalized or "эквайр" in normalized:
            return "acquiring"
        if any(token in normalized for token in ("return", "refund", "cancel", "невыкуп", "обратн", "возврат")):
            return "returns"
        if any(token in normalized for token in ("delivery", "logistic", "flow", "dropoff", "pickup", "fulfillment", "trans", "mile", "достав", "магистра", "сборк", "последн")):
            return "logistics"
        return "other"

    @staticmethod
    def _split_by_calendar_month(
        *,
        date_from: datetime,
        date_to: datetime,
    ) -> list[tuple[datetime, datetime]]:
        segments: list[tuple[datetime, datetime]] = []
        current_from = date_from

        while current_from <= date_to:
            last_day = monthrange(current_from.year, current_from.month)[1]
            month_end = current_from.replace(
                day=last_day,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
            )
            current_to = min(month_end, date_to)
            segments.append((current_from, current_to))
            current_from = current_to + timedelta(microseconds=1)

        return segments

    @staticmethod
    def _split_by_days(
        *,
        date_from: datetime,
        date_to: datetime,
        days: int,
    ) -> list[tuple[datetime, datetime]]:
        step_days = max(int(days), 1)
        segments: list[tuple[datetime, datetime]] = []
        current_from = date_from

        while current_from <= date_to:
            current_to = min(
                current_from + timedelta(days=step_days, microseconds=-1),
                date_to,
            )
            segments.append((current_from, current_to))
            current_from = current_to + timedelta(microseconds=1)

        return segments

    async def _load_recent_transaction_snapshot(
        self,
        *,
        date_from: datetime,
        date_to: datetime,
    ) -> dict[str, Any]:
        ranges = self._split_by_days(date_from=date_from, date_to=date_to, days=15)
        rows_count = 0
        operation_breakdown: dict[str, dict[str, Any]] = {}
        service_breakdown: dict[str, dict[str, Any]] = {}
        totals_accumulator = {
            "accruals_for_sale": 0.0,
            "compensation_amount": 0.0,
            "money_transfer": 0.0,
            "others_amount": 0.0,
            "processing_and_delivery": 0.0,
            "refunds_and_cancellations": 0.0,
            "sale_commission": 0.0,
            "services_amount": 0.0,
        }
        bucket_totals: dict[str, float] = {
            "marketing": 0.0,
            "storage": 0.0,
            "acquiring": 0.0,
            "returns": 0.0,
            "logistics": 0.0,
            "other": 0.0,
        }
        warnings: list[dict[str, Any]] = []
        transaction_page_size = max(int(settings.OZON_FINANCE_TRANSACTION_PAGE_SIZE), 1)
        transaction_page_delay_seconds = max(
            float(settings.OZON_FINANCE_TRANSACTION_PAGE_DELAY_MS) / 1000.0,
            0.0,
        )

        for range_from, range_to in ranges:
            totals = await self.client.get_transaction_totals(
                date_from=range_from.isoformat().replace("+00:00", "Z"),
                date_to=range_to.isoformat().replace("+00:00", "Z"),
                transaction_type="all",
            )
            for key in totals_accumulator:
                totals_accumulator[key] += self._to_float(totals.get(key))

            page = 1
            page_count = 1
            try:
                while page <= page_count:
                    payload = await self.client.get_transaction_list(
                        date_from=range_from.isoformat().replace("+00:00", "Z"),
                        date_to=range_to.isoformat().replace("+00:00", "Z"),
                        page=page,
                        page_size=transaction_page_size,
                        transaction_type="all",
                    )
                    operations = list(payload.get("operations") or [])
                    rows_count += len(operations)
                    page_count = int(payload.get("page_count") or 0) or 0
                    if page_count == 0:
                        break

                    for operation in operations:
                        operation_name = str(operation.get("operation_type_name") or operation.get("operation_type") or "Прочее")
                        op_amount = self._to_float(operation.get("amount"))
                        op_item = operation_breakdown.setdefault(
                            operation_name,
                            {"name": operation_name, "amount": 0.0, "count": 0},
                        )
                        op_item["amount"] += op_amount
                        op_item["count"] += 1

                        operation_services = list(operation.get("services") or [])
                        for service in operation_services:
                            service_name = str(service.get("name") or "Прочая услуга")
                            price = self._to_float(service.get("price"))
                            bucket = self._service_bucket(service_name)
                            service_item = service_breakdown.setdefault(
                                service_name,
                                {"name": service_name, "amount": 0.0, "count": 0, "bucket": bucket},
                            )
                            service_item["amount"] += price
                            service_item["count"] += 1
                            bucket_totals[bucket] += price

                        if not operation_services:
                            bucket = self._service_bucket(operation_name)
                            if bucket != "other":
                                service_item = service_breakdown.setdefault(
                                    operation_name,
                                    {"name": operation_name, "amount": 0.0, "count": 0, "bucket": bucket},
                                )
                                service_item["amount"] += op_amount
                                service_item["count"] += 1
                                bucket_totals[bucket] += op_amount

                    if page < page_count and transaction_page_delay_seconds > 0:
                        await asyncio.sleep(transaction_page_delay_seconds)
                    page += 1
            except Exception as e:
                warnings.append(
                    {
                        "range_from": range_from.isoformat().replace("+00:00", "Z"),
                        "range_to": range_to.isoformat().replace("+00:00", "Z"),
                        "error": f"{type(e).__name__}: {e}",
                    }
                )

        top_operations = sorted(
            (
                {"name": item["name"], "amount": round(float(item["amount"]), 2), "count": int(item["count"])}
                for item in operation_breakdown.values()
            ),
            key=lambda item: abs(item["amount"]),
            reverse=True,
        )
        top_services = sorted(
            (
                {
                    "name": item["name"],
                    "amount": round(float(item["amount"]), 2),
                    "count": int(item["count"]),
                    "bucket": item["bucket"],
                }
                for item in service_breakdown.values()
            ),
            key=lambda item: abs(item["amount"]),
            reverse=True,
        )

        return {
            "period": {
                "from": date_from.isoformat().replace("+00:00", "Z"),
                "to": date_to.isoformat().replace("+00:00", "Z"),
                "days": max((date_to.date() - date_from.date()).days + 1, 1),
            },
            "totals": {
                "accruals_for_sale": round(totals_accumulator["accruals_for_sale"], 2),
                "compensation_amount": round(totals_accumulator["compensation_amount"], 2),
                "money_transfer": round(totals_accumulator["money_transfer"], 2),
                "others_amount": round(totals_accumulator["others_amount"], 2),
                "processing_and_delivery": round(totals_accumulator["processing_and_delivery"], 2),
                "refunds_and_cancellations": round(totals_accumulator["refunds_and_cancellations"], 2),
                "sale_commission": round(totals_accumulator["sale_commission"], 2),
                "services_amount": round(totals_accumulator["services_amount"], 2),
            },
            "rows_count": rows_count,
            "service_buckets": {key: round(value, 2) for key, value in bucket_totals.items()},
            "top_operations": top_operations[:12],
            "top_services": top_services[:15],
            "warnings": warnings,
            "details_available": not bool(warnings),
        }

    async def _load_removal_snapshot(
        self,
        *,
        date_from: date,
        date_to: date,
        kind: str,
    ) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        last_id = ""

        while True:
            if kind == "from_stock":
                payload = await self.client.get_removal_from_stock_list(
                    date_from=date_from.isoformat(),
                    date_to=date_to.isoformat(),
                    last_id=last_id,
                    limit=500,
                )
            else:
                payload = await self.client.get_removal_from_supply_list(
                    date_from=date_from.isoformat(),
                    date_to=date_to.isoformat(),
                    last_id=last_id,
                    limit=500,
                )

            batch = list(payload.get("returns_summary_report_rows") or [])
            rows.extend(batch)

            next_last_id = str(payload.get("last_id") or "").strip()
            if not next_last_id or not batch or next_last_id == last_id:
                break
            last_id = next_last_id

        snapshot = self._build_removal_snapshot(
            rows,
            date_from=date_from,
            date_to=date_to,
            kind=kind,
        )
        snapshot["available"] = True
        return snapshot

    @classmethod
    def _parse_amount_from_report(cls, raw_bytes: bytes) -> dict[str, Any]:
        xlsx_value_rows = cls._extract_xlsx_value_rows(raw_bytes)
        if xlsx_value_rows is not None:
            aliases = [
                "сумма",
                "amount",
                "итого",
                "total",
                "сумма компенсации",
                "сумма декомпенсации",
                "стоимость",
                "к начислению",
                "к списанию",
            ]
            normalized_aliases = [cls._normalize_header(alias) for alias in aliases]
            best_header_index: int | None = None
            best_amount_col_index: int | None = None
            best_score: tuple[int, int, int] | None = None

            for row_index, row_values in enumerate(xlsx_value_rows):
                normalized_cells = [cls._normalize_header(value) for value in row_values]
                filled_cells = sum(1 for value in normalized_cells if value)
                if filled_cells <= 1:
                    continue

                candidate_amount_index: int | None = None
                for col_index, cell_value in enumerate(normalized_cells):
                    if any(alias in cell_value for alias in normalized_aliases):
                        candidate_amount_index = col_index
                        break
                if candidate_amount_index is None:
                    continue

                has_row_number = any("п/п" in value or value == "№" for value in normalized_cells)
                score = (int(has_row_number), filled_cells, -row_index)
                if best_score is None or score > best_score:
                    best_score = score
                    best_header_index = row_index
                    best_amount_col_index = candidate_amount_index

            if best_header_index is not None and best_amount_col_index is not None:
                header = [
                    str(value or "").strip() or f"col_{index + 1}"
                    for index, value in enumerate(xlsx_value_rows[best_header_index])
                ]
                preview_rows: list[dict[str, str]] = []
                detail_rows = 0
                amount_total = 0.0
                footer_total: float | None = None

                for row_values in xlsx_value_rows[best_header_index + 1 :]:
                    if not any(str(value or "").strip() for value in row_values):
                        continue

                    row_dict = {
                        header[index]: str(row_values[index]).strip() if index < len(row_values) else ""
                        for index in range(len(header))
                    }
                    if len(preview_rows) < 8:
                        preview_rows.append(row_dict)

                    normalized_cells = [cls._normalize_header(value) for value in row_values]
                    normalized_joined = " ".join(value for value in normalized_cells if value)
                    amount_raw = (
                        str(row_values[best_amount_col_index]).strip()
                        if best_amount_col_index < len(row_values)
                        else ""
                    )
                    amount_value = cls._to_float(
                        amount_raw.replace(" ", "").replace("\xa0", "").replace(",", ".")
                    )

                    if (
                        "всего к начислению" in normalized_joined
                        or "всего к списанию" in normalized_joined
                        or normalized_joined.startswith("итого")
                    ):
                        if amount_value:
                            footer_total = amount_value
                        continue

                    first_non_empty = next(
                        (str(value).strip() for value in row_values if str(value or "").strip()),
                        "",
                    )
                    if re.fullmatch(r"\d+", first_non_empty):
                        detail_rows += 1
                        amount_total += amount_value

                return {
                    "rows": detail_rows or len(preview_rows),
                    "amount_total": round(footer_total if footer_total is not None else amount_total, 2),
                    "preview_rows": preview_rows,
                }

        xlsx_rows = cls._extract_xlsx_dict_rows(raw_bytes)
        if xlsx_rows is not None:
            rows = 0
            amount_total = 0.0
            preview_rows: list[dict[str, str]] = []

            for raw_row in xlsx_rows:
                row = {
                    str(key).strip(): str(value).strip()
                    for key, value in raw_row.items()
                    if key is not None
                }
                if not any(row.values()):
                    continue
                rows += 1
                if len(preview_rows) < 8:
                    preview_rows.append(row)
                amount = cls._pick_value(
                    row,
                    "сумма",
                    "amount",
                    "итого",
                    "total",
                    "сумма компенсации",
                    "сумма декомпенсации",
                    "стоимость",
                    "к начислению",
                    "к списанию",
                )
                amount_total += cls._to_float(
                    str(amount).replace(" ", "").replace("\xa0", "").replace(",", ".")
                )

            return {
                "rows": rows,
                "amount_total": round(amount_total, 2),
                "preview_rows": preview_rows,
            }

        text = cls._decode_bytes(raw_bytes).replace("\r\n", "\n").replace("\r", "\n")
        sample = text[:4096]

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
            dialect.delimiter = ";"

        rows = 0
        amount_total = 0.0
        preview_rows: list[dict[str, str]] = []

        def consume_rows(dict_rows):
            nonlocal rows, amount_total, preview_rows
            for raw_row in dict_rows:
                row = {
                    str(key).strip(): str(value).strip()
                    for key, value in raw_row.items()
                    if key is not None
                }
                if not any(row.values()):
                    continue

                rows += 1
                if len(preview_rows) < 8:
                    preview_rows.append(row)

                amount = cls._pick_value(
                    row,
                    "сумма",
                    "amount",
                    "итого",
                    "total",
                    "сумма компенсации",
                    "сумма декомпенсации",
                    "стоимость",
                )
                amount_total += cls._to_float(
                    str(amount).replace(" ", "").replace("\xa0", "").replace(",", ".")
                )

        try:
            stream = io.StringIO(text, newline="")
            reader = csv.DictReader(stream, dialect=dialect)
            consume_rows(reader)
        except csv.Error:
            lines = [line for line in text.split("\n") if line.strip()]
            if not lines:
                return {
                    "rows": 0,
                    "amount_total": 0.0,
                    "preview_rows": [],
                }

            reader = csv.reader(lines, delimiter=dialect.delimiter)
            parsed_rows = list(reader)
            if not parsed_rows:
                return {
                    "rows": 0,
                    "amount_total": 0.0,
                    "preview_rows": [],
                }

            headers = [str(h).strip() for h in parsed_rows[0]]
            dict_rows = []
            expected_len = len(headers)

            for values in parsed_rows[1:]:
                if not values:
                    continue

                if len(values) < expected_len:
                    values = values + [""] * (expected_len - len(values))
                elif len(values) > expected_len:
                    values = values[: expected_len - 1] + [dialect.delimiter.join(values[expected_len - 1 :])]

                dict_rows.append(dict(zip(headers, values)))

            consume_rows(dict_rows)

        return {
            "rows": rows,
            "amount_total": round(amount_total, 2),
            "preview_rows": preview_rows,
        }

    async def fetch_day_summary(self, *, report_date: date) -> dict[str, Any]:
        return await self.fetch_range_summary(date_from=report_date, date_to=report_date)

    async def fetch_range_summary(self, *, date_from: date, date_to: date) -> dict[str, Any]:
        period_start = datetime(
            date_from.year,
            date_from.month,
            date_from.day,
            0,
            0,
            0,
            tzinfo=timezone.utc,
        )
        period_finish = datetime(
            date_to.year,
            date_to.month,
            date_to.day,
            23,
            59,
            59,
            tzinfo=timezone.utc,
        )

        periods: list[dict[str, Any]] = []
        page = 1
        page_count = 1

        while page <= page_count:
            payload = await self.client.get_cash_flow_statement(
                date_from=period_start.isoformat().replace("+00:00", "Z"),
                date_to=period_finish.isoformat().replace("+00:00", "Z"),
                page=page,
                page_size=100,
                with_details=True,
            )
            cash_flows = payload.get("cash_flows") or []
            page_count = int(payload.get("page_count") or 1)

            for item in cash_flows:
                periods.append(
                    {
                        "orders_amount": self._to_float(item.get("orders_amount")),
                        "returns_amount": self._to_float(item.get("returns_amount")),
                        "commission_amount": self._to_float(item.get("commission_amount")),
                        "services_amount": self._to_float(item.get("services_amount")),
                        "logistics_amount": self._to_float(item.get("item_delivery_and_return_amount")),
                    }
                )
            page += 1

        return {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "orders_amount": round(sum(item["orders_amount"] for item in periods), 2),
            "returns_amount": round(sum(item["returns_amount"] for item in periods), 2),
            "commission_amount": round(sum(item["commission_amount"] for item in periods), 2),
            "services_amount": round(sum(item["services_amount"] for item in periods), 2),
            "logistics_amount": round(sum(item["logistics_amount"] for item in periods), 2),
            "periods_count": len(periods),
        }

    @staticmethod
    def _is_missing_adjustment_document(error: Exception) -> bool:
        if isinstance(error, httpx.HTTPStatusError):
            if error.response is not None and error.response.status_code == 404:
                return True
            text = (error.response.text or "").lower() if error.response is not None else ""
            if "document not found" in text:
                return True

        message = str(error).lower()
        return "document not found" in message or "404 not found" in message

    @staticmethod
    def _is_retryable_adjustment_error(error: Exception) -> bool:
        if isinstance(error, httpx.RequestError):
            return True
        if isinstance(error, httpx.HTTPStatusError) and error.response is not None:
            return error.response.status_code in {429, 500, 502, 503, 504}
        return False

    @staticmethod
    def _adjustment_retry_wait_seconds(error: Exception, attempt: int) -> int:
        if isinstance(error, httpx.HTTPStatusError) and error.response is not None:
            if error.response.status_code == 429:
                return max(attempt + 1, 1)
            return max((attempt + 1) * 2, 2)
        return max(attempt + 1, 1)

    async def _load_single_adjustment_report(
        self,
        *,
        month_key: str,
        kind: str,
    ) -> dict[str, Any]:
        attempts = 4
        last_error: Exception | None = None

        for attempt in range(attempts):
            try:
                if kind == "compensation":
                    ready_report = await self.report_service.ensure_compensation_report(date=month_key)
                else:
                    ready_report = await self.report_service.ensure_decompensation_report(date=month_key)

                raw_bytes = await self.report_service.download_ready_report(ready_report)
                await self._ensure_report_columns(
                    raw_bytes=raw_bytes,
                    report_kind="adjustment_amount",
                    payload={"month": month_key, "kind": kind},
                )
                parsed = self._parse_amount_from_report(raw_bytes)
                return {
                    "month": month_key,
                    "code": ready_report.code,
                    "amount_total": parsed["amount_total"],
                    "rows": parsed["rows"],
                    "preview_rows": parsed["preview_rows"],
                }
            except Exception as exc:
                last_error = exc
                missing = self._is_missing_adjustment_document(exc)
                retryable = self._is_retryable_adjustment_error(exc)
                if not missing and retryable and attempt < attempts - 1:
                    await asyncio.sleep(self._adjustment_retry_wait_seconds(exc, attempt))
                    continue

                return {
                    "month": month_key,
                    "code": None,
                    "amount_total": 0.0,
                    "rows": 0,
                    "preview_rows": [],
                    "error": f"{type(exc).__name__}: {exc}",
                    "missing_document": missing,
                }

        exc = last_error or RuntimeError("Unknown adjustment report error")
        missing = self._is_missing_adjustment_document(exc)
        return {
            "month": month_key,
            "code": None,
            "amount_total": 0.0,
            "rows": 0,
            "preview_rows": [],
            "error": f"{type(exc).__name__}: {exc}",
            "missing_document": missing,
        }

    async def _load_adjustment_reports(self, month_keys: list[str]) -> dict[str, Any]:
        compensation_total = 0.0
        decompensation_total = 0.0
        compensation_reports: list[dict[str, Any]] = []
        decompensation_reports: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []

        for month_key in month_keys:
            compensation_report = await self._load_single_adjustment_report(
                month_key=month_key,
                kind="compensation",
            )
            compensation_total += float(compensation_report.get("amount_total") or 0)
            compensation_reports.append(compensation_report)
            if compensation_report.get("error"):
                missing = bool(compensation_report.get("missing_document"))
                warnings.append(
                    {
                        "month": month_key,
                        "kind": "compensation",
                        "error": str(compensation_report.get("error") or ""),
                        "missing_document": missing,
                    }
                )

            decompensation_report = await self._load_single_adjustment_report(
                month_key=month_key,
                kind="decompensation",
            )
            decompensation_total += float(decompensation_report.get("amount_total") or 0)
            decompensation_reports.append(decompensation_report)
            if decompensation_report.get("error"):
                missing = bool(decompensation_report.get("missing_document"))
                warnings.append(
                    {
                        "month": month_key,
                        "kind": "decompensation",
                        "error": str(decompensation_report.get("error") or ""),
                        "missing_document": missing,
                    }
                )

        return {
            "compensation_total": round(compensation_total, 2),
            "decompensation_total": round(decompensation_total, 2),
            "compensation_reports": compensation_reports,
            "decompensation_reports": decompensation_reports,
            "warnings": warnings,
            "adjustments_available": not bool(warnings),
        }

    async def _load_placement_by_products_snapshot(
        self,
        *,
        client_id: str,
        date_from: date,
        date_to: date,
    ) -> dict[str, Any]:
        cache_key = self.placement_cache_key_for(
            client_id,
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
        )
        cached = await cache_get_json(cache_key)
        if cached:
            return cached

        report_code = await self.client.create_placement_by_products_report(
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
        )
        if not report_code:
            raise RuntimeError("Ozon did not return placement-by-products report code")

        ready_report = await self.report_service.wait_until_ready(report_code)
        report_bytes = await self.report_service.download_ready_report(ready_report)
        await self._ensure_report_columns(
            raw_bytes=report_bytes,
            report_kind="placement_by_products",
            payload={"date_from": date_from.isoformat(), "date_to": date_to.isoformat()},
        )
        snapshot = self._parse_placement_by_products_report(
            report_bytes,
            date_from=date_from,
            date_to=date_to,
        )
        snapshot.update(
            {
                "available": True,
                "code": ready_report.code,
                "report_type": ready_report.report_type,
                "created_at": ready_report.created_at,
            }
        )
        await cache_set_json(cache_key, snapshot, self._placement_snapshot_ttl())
        return snapshot

    async def _load_placement_by_supplies_snapshot(
        self,
        *,
        client_id: str,
        date_from: date,
        date_to: date,
    ) -> dict[str, Any]:
        cache_key = self.placement_supplies_cache_key_for(
            client_id,
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
        )
        cached = await cache_get_json(cache_key)
        if cached:
            return cached

        report_code = await self.client.create_placement_by_supplies_report(
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
        )
        if not report_code:
            raise RuntimeError("Ozon did not return placement-by-supplies report code")

        ready_report = await self.report_service.wait_until_ready(report_code)
        report_bytes = await self.report_service.download_ready_report(ready_report)
        await self._ensure_report_columns(
            raw_bytes=report_bytes,
            report_kind="placement_by_supplies",
            payload={"date_from": date_from.isoformat(), "date_to": date_to.isoformat()},
        )
        snapshot = self._parse_placement_by_supplies_report(
            report_bytes,
            date_from=date_from,
            date_to=date_to,
        )
        snapshot.update(
            {
                "available": True,
                "code": ready_report.code,
                "report_type": ready_report.report_type,
                "created_at": ready_report.created_at,
            }
        )
        await cache_set_json(cache_key, snapshot, self._placement_snapshot_ttl())
        return snapshot

    async def get_cached_snapshot(self, *, client_id: str) -> dict[str, Any] | None:
        return await cache_get_json(self.cache_key_for(client_id))

    async def refresh_cash_flow_snapshot(
        self,
        *,
        client_id: str,
        days_back: int = 62,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        requested_start = (now - timedelta(days=max(days_back, 1))).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        requested_finish = now.replace(hour=23, minute=59, second=59, microsecond=0)

        allowed_periods = self._half_month_periods(requested_start, requested_finish)
        month_keys = self._month_keys(requested_start, requested_finish)

        periods: list[dict[str, Any]] = []

        for period_start, period_finish in allowed_periods:
            page = 1
            page_count = 1

            while page <= page_count:
                payload = await self.client.get_cash_flow_statement(
                    date_from=period_start.isoformat().replace("+00:00", "Z"),
                    date_to=period_finish.isoformat().replace("+00:00", "Z"),
                    page=page,
                    page_size=100,
                    with_details=True,
                )
                cash_flows = payload.get("cash_flows") or []
                page_count = int(payload.get("page_count") or 1)

                for item in cash_flows:
                    period = item.get("period") or {}
                    periods.append(
                        {
                            "period_begin": period.get("begin"),
                            "period_end": period.get("end"),
                            "period_id": period.get("id"),
                            "orders_amount": self._to_float(item.get("orders_amount")),
                            "returns_amount": self._to_float(item.get("returns_amount")),
                            "commission_amount": self._to_float(item.get("commission_amount")),
                            "services_amount": self._to_float(item.get("services_amount")),
                            "logistics_amount": self._to_float(item.get("item_delivery_and_return_amount")),
                            "currency_code": item.get("currency_code") or "RUB",
                        }
                    )
                page += 1

        summary = {
            "orders_amount": round(sum(item["orders_amount"] for item in periods), 2),
            "returns_amount": round(sum(item["returns_amount"] for item in periods), 2),
            "commission_amount": round(sum(item["commission_amount"] for item in periods), 2),
            "services_amount": round(sum(item["services_amount"] for item in periods), 2),
            "logistics_amount": round(sum(item["logistics_amount"] for item in periods), 2),
        }

        adjustments = await self._load_adjustment_reports(month_keys)

        summary["compensation_amount"] = adjustments["compensation_total"]
        summary["decompensation_amount"] = adjustments["decompensation_total"]
        summary["net_payout"] = round(
            summary["orders_amount"]
            + summary["returns_amount"]
            - summary["commission_amount"]
            - summary["services_amount"]
            - summary["logistics_amount"],
            2,
        )
        summary["net_payout_adjusted"] = round(
            summary["net_payout"]
            + summary["compensation_amount"]
            - summary["decompensation_amount"],
            2,
        )

        warnings: list[dict[str, Any]] = []
        realization_year, realization_month, realization_period = self._last_closed_month(now)
        try:
            realization_raw = await self.client.get_realization_report(
                year=realization_year,
                month=realization_month,
            )
            realization_snapshot = self._build_realization_snapshot(
                realization_raw,
                year=realization_year,
                month=realization_month,
            )
        except Exception as e:
            realization_snapshot = {
                "period": realization_period,
                "year": realization_year,
                "month": realization_month,
                "available": False,
                "error": f"{type(e).__name__}: {e}",
                "rows_count": 0,
                "sold_units": 0,
                "sold_amount": 0.0,
                "sold_total": 0.0,
                "returned_units": 0,
                "returned_amount": 0.0,
                "returned_total": 0.0,
                "net_units": 0,
                "net_amount": 0.0,
                "net_total": 0.0,
                "items": [],
                "top_items": [],
                "top_returns": [],
            }
            warnings.append({"kind": "realization_closed_month", "error": realization_snapshot["error"]})
        else:
            realization_snapshot["available"] = True

        previous_realization_year, previous_realization_month, previous_realization_period = self._shift_month(
            realization_year,
            realization_month,
            -1,
        )
        try:
            previous_realization_raw = await self.client.get_realization_report(
                year=previous_realization_year,
                month=previous_realization_month,
            )
            previous_realization_snapshot = self._build_realization_snapshot(
                previous_realization_raw,
                year=previous_realization_year,
                month=previous_realization_month,
            )
        except Exception as e:
            previous_realization_snapshot = {
                "period": previous_realization_period,
                "year": previous_realization_year,
                "month": previous_realization_month,
                "available": False,
                "error": f"{type(e).__name__}: {e}",
                "rows_count": 0,
                "sold_units": 0,
                "sold_amount": 0.0,
                "sold_total": 0.0,
                "returned_units": 0,
                "returned_amount": 0.0,
                "returned_total": 0.0,
                "net_units": 0,
                "net_amount": 0.0,
                "net_total": 0.0,
                "items": [],
                "top_items": [],
                "top_returns": [],
            }
            warnings.append({"kind": "realization_previous_closed_month", "error": previous_realization_snapshot["error"]})
        else:
            previous_realization_snapshot["available"] = True

        recent_transactions_from = (now - timedelta(days=29)).replace(hour=0, minute=0, second=0, microsecond=0)
        recent_transactions_to = now.replace(hour=23, minute=59, second=59, microsecond=0)

        current_month_start_date = now.date().replace(day=1)
        previous_month_start_year, previous_month_start_month, _ = self._shift_month(
            current_month_start_date.year,
            current_month_start_date.month,
            -1,
        )
        previous_month_start_date = date(previous_month_start_year, previous_month_start_month, 1)
        current_month_last_day = (now.date() - current_month_start_date).days
        compare_end_year, compare_end_month, _ = self._shift_month(
            current_month_start_date.year,
            current_month_start_date.month,
            0,
        )
        previous_month_end_date = date(compare_end_year, compare_end_month, 1) - timedelta(days=1)
        previous_month_same_period_end = min(
            previous_month_start_date + timedelta(days=current_month_last_day),
            previous_month_end_date,
        )

        current_month_live = await self.fetch_range_summary(
            date_from=current_month_start_date,
            date_to=now.date(),
        )
        previous_month_same_period_live = await self.fetch_range_summary(
            date_from=previous_month_start_date,
            date_to=previous_month_same_period_end,
        )
        try:
            transaction_snapshot = await self._load_recent_transaction_snapshot(
                date_from=recent_transactions_from,
                date_to=recent_transactions_to,
            )
        except Exception as e:
            transaction_snapshot = {
                "available": False,
                "error": f"{type(e).__name__}: {e}",
                "period": {
                    "from": recent_transactions_from.isoformat().replace("+00:00", "Z"),
                    "to": recent_transactions_to.isoformat().replace("+00:00", "Z"),
                    "days": 30,
                },
                "totals": {
                    "accruals_for_sale": 0.0,
                    "compensation_amount": 0.0,
                    "money_transfer": 0.0,
                    "others_amount": 0.0,
                    "processing_and_delivery": 0.0,
                    "refunds_and_cancellations": 0.0,
                    "sale_commission": 0.0,
                    "services_amount": 0.0,
                },
                "rows_count": 0,
                "service_buckets": {
                    "marketing": 0.0,
                    "storage": 0.0,
                    "acquiring": 0.0,
                    "returns": 0.0,
                    "logistics": 0.0,
                    "other": 0.0,
                },
                "top_operations": [],
                "top_services": [],
            }
            warnings.append({"kind": "transactions_recent", "error": transaction_snapshot["error"]})
        else:
            transaction_snapshot["available"] = True

        placement_date_from = recent_transactions_from.date()
        placement_date_to = recent_transactions_to.date()
        try:
            placement_snapshot = await self._load_placement_by_products_snapshot(
                client_id=client_id,
                date_from=placement_date_from,
                date_to=placement_date_to,
            )
        except Exception as e:
            placement_snapshot = {
                "available": False,
                "error": f"{type(e).__name__}: {e}",
                "period": {
                    "from": placement_date_from.isoformat(),
                    "to": placement_date_to.isoformat(),
                    "days": max((placement_date_to - placement_date_from).days + 1, 1),
                },
                "rows_count": 0,
                "offers_count": 0,
                "amount_total": 0.0,
                "items": [],
                "top_items": [],
                "preview_rows": [],
            }
            warnings.append({"kind": "placement_by_products_recent", "error": placement_snapshot["error"]})

        try:
            placement_supplies_snapshot = await self._load_placement_by_supplies_snapshot(
                client_id=client_id,
                date_from=placement_date_from,
                date_to=placement_date_to,
            )
        except Exception as e:
            placement_supplies_snapshot = {
                "available": False,
                "error": f"{type(e).__name__}: {e}",
                "period": {
                    "from": placement_date_from.isoformat(),
                    "to": placement_date_to.isoformat(),
                    "days": max((placement_date_to - placement_date_from).days + 1, 1),
                },
                "rows_count": 0,
                "supplies_count": 0,
                "amount_total": 0.0,
                "items": [],
                "top_items": [],
                "preview_rows": [],
            }
            warnings.append({"kind": "placement_by_supplies_recent", "error": placement_supplies_snapshot["error"]})

        try:
            removal_from_stock_snapshot = await self._load_removal_snapshot(
                date_from=placement_date_from,
                date_to=placement_date_to,
                kind="from_stock",
            )
        except Exception as e:
            removal_from_stock_snapshot = {
                "available": False,
                "kind": "from_stock",
                "kind_label": "Со стока",
                "error": f"{type(e).__name__}: {e}",
                "period": {
                    "from": placement_date_from.isoformat(),
                    "to": placement_date_to.isoformat(),
                    "days": max((placement_date_to - placement_date_from).days + 1, 1),
                },
                "rows_count": 0,
                "returns_count": 0,
                "offers_count": 0,
                "quantity_total": 0,
                "delivery_price_total": 0.0,
                "auto_returns_count": 0,
                "utilization_count": 0,
                "items": [],
                "top_items": [],
                "states": [],
                "top_states": [],
                "preview_rows": [],
            }
            warnings.append({"kind": "removal_from_stock_recent", "error": removal_from_stock_snapshot["error"]})

        try:
            removal_from_supply_snapshot = await self._load_removal_snapshot(
                date_from=placement_date_from,
                date_to=placement_date_to,
                kind="from_supply",
            )
        except Exception as e:
            removal_from_supply_snapshot = {
                "available": False,
                "kind": "from_supply",
                "kind_label": "С поставки",
                "error": f"{type(e).__name__}: {e}",
                "period": {
                    "from": placement_date_from.isoformat(),
                    "to": placement_date_to.isoformat(),
                    "days": max((placement_date_to - placement_date_from).days + 1, 1),
                },
                "rows_count": 0,
                "returns_count": 0,
                "offers_count": 0,
                "quantity_total": 0,
                "delivery_price_total": 0.0,
                "auto_returns_count": 0,
                "utilization_count": 0,
                "items": [],
                "top_items": [],
                "states": [],
                "top_states": [],
                "preview_rows": [],
            }
            warnings.append({"kind": "removal_from_supply_recent", "error": removal_from_supply_snapshot["error"]})

        snapshot = {
            "client_id": client_id,
            "kind": "cash_flow",
            "filters": {
                "date_from": requested_start.isoformat().replace("+00:00", "Z"),
                "date_to": requested_finish.isoformat().replace("+00:00", "Z"),
                "days_back": days_back,
                "allowed_periods": [
                    {
                        "from": period_start.isoformat().replace("+00:00", "Z"),
                        "to": period_finish.isoformat().replace("+00:00", "Z"),
                    }
                    for period_start, period_finish in allowed_periods
                ],
            },
            "periods": periods,
            "period_windows": {
                "cash_flow_days_back": days_back,
                "cash_flow_allowed_half_months": len(allowed_periods),
                "realization_closed_month": realization_period,
                "transactions_recent_days": transaction_snapshot.get("period", {}).get("days", 30),
                "placement_by_products_recent_days": placement_snapshot.get("period", {}).get("days", 30),
                "placement_by_supplies_recent_days": placement_supplies_snapshot.get("period", {}).get("days", 30),
                "removal_from_stock_recent_days": removal_from_stock_snapshot.get("period", {}).get("days", 30),
                "removal_from_supply_recent_days": removal_from_supply_snapshot.get("period", {}).get("days", 30),
            },
            "adjustments": {
                "months": month_keys,
                "compensation_reports": adjustments["compensation_reports"],
                "decompensation_reports": adjustments["decompensation_reports"],
                "warnings": adjustments["warnings"],
                "adjustments_available": adjustments["adjustments_available"],
            },
            "summary": summary,
            "current_month_to_date": current_month_live,
            "previous_month_same_period": previous_month_same_period_live,
            "realization_closed_month": realization_snapshot,
            "realization_previous_closed_month": previous_realization_snapshot,
            "transactions_recent": transaction_snapshot,
            "placement_by_products_recent": placement_snapshot,
            "placement_by_supplies_recent": placement_supplies_snapshot,
            "removal_from_stock_recent": removal_from_stock_snapshot,
            "removal_from_supply_recent": removal_from_supply_snapshot,
            "warnings": warnings,
            "refreshed_at": datetime.now(timezone.utc).isoformat(),
        }
        await cache_set_json(self.cache_key_for(client_id), snapshot, self._snapshot_ttl())
        return snapshot
