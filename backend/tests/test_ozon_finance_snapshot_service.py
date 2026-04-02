import io
import zipfile
from datetime import datetime, timezone

import app.services.ozon.finance_snapshot_service as finance_snapshot_module
from app.services.ozon.finance_snapshot_service import OzonFinanceSnapshotService


def build_minimal_xlsx(rows: list[list[str]]) -> bytes:
    workbook = io.BytesIO()
    with zipfile.ZipFile(workbook, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        shared_strings: list[str] = []
        string_index: dict[str, int] = {}

        def shared_index(value: str) -> int:
            if value not in string_index:
                string_index[value] = len(shared_strings)
                shared_strings.append(value)
            return string_index[value]

        sheet_rows_xml: list[str] = []
        for row_number, row in enumerate(rows, start=1):
            cells_xml: list[str] = []
            for col_number, value in enumerate(row, start=1):
                column = ""
                current = col_number
                while current > 0:
                    current, remainder = divmod(current - 1, 26)
                    column = chr(ord("A") + remainder) + column
                shared_id = shared_index(str(value))
                cells_xml.append(
                    f'<c r="{column}{row_number}" t="s"><v>{shared_id}</v></c>'
                )
            sheet_rows_xml.append(f'<row r="{row_number}">{"".join(cells_xml)}</row>')

        shared_xml = "".join(
            f"<si><t>{value}</t></si>" for value in shared_strings
        )

        zf.writestr(
            "[Content_Types].xml",
            """<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>
</Types>""",
        )
        zf.writestr(
            "_rels/.rels",
            """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>""",
        )
        zf.writestr(
            "xl/workbook.xml",
            """<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Sheet1" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>""",
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>
</Relationships>""",
        )
        zf.writestr(
            "xl/sharedStrings.xml",
            f"""<?xml version="1.0" encoding="UTF-8"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="{len(shared_strings)}" uniqueCount="{len(shared_strings)}">
  {shared_xml}
</sst>""",
        )
        zf.writestr(
            "xl/worksheets/sheet1.xml",
            f"""<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    {"".join(sheet_rows_xml)}
  </sheetData>
</worksheet>""",
        )
    return workbook.getvalue()


def test_parse_amount_from_xlsx_with_title_rows_and_footer_total():
    raw = build_minimal_xlsx(
        [
            [],
            ["", "Отчет о компенсациях № 2459253 от 31.01.2026"],
            ["", "по Договору оферты"],
            [],
            ["", "Плательщик:", "", "", "", "", "Получатель:"],
            ["", "Интернет Решения, ООО", "", "", "", "", "ИП Тарасов"],
            [],
            [
                "",
                "№ п/п",
                "Тип компенсации",
                "Название товара",
                "Артикул",
                "SKU",
                "Кол-во",
                "Итого к начислению \n RUB",
            ],
            ["", "1", "Потеря", "Носки", "SKU-1", "1001", "1", "695.33"],
            ["", "2", "Потеря", "Носки", "SKU-2", "1002", "1", "312.01"],
            ["", "3", "Потеря", "Носки", "SKU-3", "1003", "1", "370.36"],
            ["", "4", "Потеря", "Носки", "SKU-4", "1004", "1", "528.67"],
            ["", "Всего к начислению:", "", "", "", "", "", "1906.37"],
        ]
    )

    parsed = OzonFinanceSnapshotService._parse_amount_from_report(raw)

    assert parsed["rows"] == 4
    assert parsed["amount_total"] == 1906.37
    assert parsed["preview_rows"][0]["№ п/п"] == "1"
    assert parsed["preview_rows"][0]["Итого к начислению \n RUB"] == "695.33"


class FakeFinanceClient:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []
        self.compensation_codes = []
        self.decompensation_codes = []
        self.placement_codes = []
        self.placement_supply_codes = []
        self.realization_calls = []
        self.transaction_list_calls = []
        self.transaction_totals_calls = []
        self.removal_from_stock_calls = []
        self.removal_from_supply_calls = []

    async def get_cash_flow_statement(self, **kwargs):
        self.calls.append(kwargs)
        page = kwargs["page"]
        return self.pages[page - 1]

    async def list_reports(self, **kwargs):
        return {"result": {"reports": [], "total": 0}}

    async def get_report_info(self, code: str):
        if code.startswith("placement-"):
            report_type = "PLACEMENT_BY_PRODUCTS"
        elif code.startswith("placement-supply-"):
            report_type = "PLACEMENT_BY_SUPPLIES"
        else:
            report_type = "COMPENSATION_REPORT" if code.startswith("comp") else "DECOMPENSATION_REPORT"
        return {
            "code": code,
            "report_type": report_type,
            "status": "success",
            "file": f"https://example.com/{code}.csv",
            "created_at": "2026-03-18T12:00:00Z",
            "expires_at": None,
        }

    async def create_compensation_report(self, *, date: str, language: str = "RU"):
        self.compensation_codes.append(date)
        return f"comp-{date}"

    async def create_decompensation_report(self, *, date: str, language: str = "RU"):
        self.decompensation_codes.append(date)
        return f"decomp-{date}"

    async def create_placement_by_products_report(self, *, date_from: str, date_to: str):
        self.placement_codes.append((date_from, date_to))
        return f"placement-{date_from}-{date_to}"

    async def create_placement_by_supplies_report(self, *, date_from: str, date_to: str):
        self.placement_supply_codes.append((date_from, date_to))
        return f"placement-supply-{date_from}-{date_to}"

    async def download_report_file(self, file_url: str) -> bytes:
        if "placement-supply-" in file_url:
            return (
                "Номер поставки;Склад;Стоимость размещения;Количество товаров;Дней размещения\n"
                "SUP-1001;ХОРУГВИНО;120.0;24;18\n"
                "SUP-1002;ЖУКОВСКИЙ;35.5;8;6\n"
            ).encode("utf-8")
        if "placement-" in file_url:
            return (
                "Артикул;Наименование товара;Стоимость размещения;Количество;Дней размещения\n"
                "offer-a;Товар A;55.5;3;14\n"
                "offer-b;Товар B;24.5;2;10\n"
            ).encode("utf-8")
        if "decomp-" in file_url:
            return "Сумма\n25\n".encode("utf-8")
        if "comp-" in file_url:
            return "Сумма\n150.5\n49.5\n".encode("utf-8")
        return "Сумма\n0\n".encode("utf-8")

    async def get_realization_report(self, *, month: int, year: int):
        self.realization_calls.append((year, month))
        return {
            "header": {"start_date": f"{year}-{month:02d}-01", "stop_date": f"{year}-{month:02d}-31"},
            "rows": [
                {
                    "item": {"offer_id": "offer-a", "name": "Товар A", "sku": 101},
                    "delivery_commission": {"quantity": 3, "amount": 1200, "total": 900},
                    "return_commission": {"quantity": 1, "amount": 400, "total": 300},
                },
                {
                    "item": {"offer_id": "offer-b", "name": "Товар B", "sku": 102},
                    "delivery_commission": {"quantity": 2, "amount": 800, "total": 620},
                    "return_commission": {"quantity": 0, "amount": 0, "total": 0},
                },
            ],
        }

    async def get_transaction_totals(self, **kwargs):
        self.transaction_totals_calls.append(kwargs)
        return {
            "accruals_for_sale": 10000,
            "compensation_amount": 200,
            "money_transfer": 0,
            "others_amount": 150,
            "processing_and_delivery": 1100,
            "refunds_and_cancellations": 350,
            "sale_commission": 900,
            "services_amount": 700,
        }

    async def get_transaction_list(self, **kwargs):
        self.transaction_list_calls.append(kwargs)
        page = kwargs["page"]
        pages = [
            {
                "operations": [
                    {
                        "amount": 500,
                        "operation_type_name": "Продвижение товаров",
                        "services": [
                            {"name": "MarketplaceMarketingActionCostItem", "price": 300},
                            {"name": "MarketplaceServiceItemDirectFlowLogistic", "price": 120},
                        ],
                    }
                ],
                "page_count": 2,
            },
            {
                "operations": [
                    {
                        "amount": 250,
                        "operation_type_name": "Размещение товаров",
                        "services": [
                            {"name": "OperationMarketplaceServiceStorage", "price": 80},
                            {"name": "MarketplaceRedistributionOfAcquiringOperation", "price": 40},
                        ],
                    }
                ],
                "page_count": 2,
            },
        ]
        return pages[page - 1]

    async def get_removal_from_stock_list(self, **kwargs):
        self.removal_from_stock_calls.append(kwargs)
        return {
            "returns_summary_report_rows": [
                {
                    "name": "Товар A",
                    "offer_id": "offer-a",
                    "quantity_for_return": 2,
                    "return_id": 1001,
                    "return_state": "утилизирована",
                    "is_auto_return": True,
                    "preliminary_delivery_price": 30,
                    "stock_type": "брак",
                    "delivery_type": "самовывоз",
                    "utilization_date": "2026-03-09T11:00:00Z",
                },
                {
                    "name": "Товар C",
                    "offer_id": "offer-c",
                    "quantity_for_return": 1,
                    "return_id": 1002,
                    "return_state": "можно забрать все",
                    "is_auto_return": False,
                    "preliminary_delivery_price": 15,
                    "stock_type": "доступно к продаже",
                    "delivery_type": "ПВЗ",
                    "utilization_date": "",
                },
            ],
            "last_id": "",
        }

    async def get_removal_from_supply_list(self, **kwargs):
        self.removal_from_supply_calls.append(kwargs)
        return {
            "returns_summary_report_rows": [
                {
                    "name": "Товар B",
                    "offer_id": "offer-b",
                    "quantity_for_return": 4,
                    "return_id": 2001,
                    "return_state": "готово к вывозу",
                    "is_auto_return": False,
                    "preliminary_delivery_price": 25,
                    "stock_type": "",
                    "delivery_type": "самовывоз",
                    "utilization_date": "",
                },
            ],
            "last_id": "",
        }


class InMemoryFinanceSnapshotService(OzonFinanceSnapshotService):
    def __init__(self, client):
        super().__init__(client)
        self.saved = {}

    async def get_cached_snapshot(self, *, client_id: str):
        return self.saved.get(client_id)

    async def refresh_cash_flow_snapshot(self, *, client_id: str, days_back: int = 62) -> dict:
        snapshot = await super().refresh_cash_flow_snapshot(client_id=client_id, days_back=days_back)
        self.saved[client_id] = snapshot
        return snapshot


class FailingTransactionListClient(FakeFinanceClient):
    async def get_transaction_list(self, **kwargs):
        raise RuntimeError("temporary ozon failure")


async def test_refresh_cash_flow_snapshot_aggregates_pages(monkeypatch):
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            base = cls(2026, 3, 10, 12, 0, 0, tzinfo=timezone.utc)
            return base if tz is None else base.astimezone(tz)

    monkeypatch.setattr(finance_snapshot_module, "datetime", FrozenDateTime)

    client = FakeFinanceClient(
        [
            {
                "cash_flows": [
                    {
                        "period": {"begin": "2026-03-01T00:00:00Z", "end": "2026-03-15T23:59:59Z", "id": 1},
                        "orders_amount": 10000,
                        "returns_amount": -2000,
                        "commission_amount": 1000,
                        "services_amount": 700,
                        "item_delivery_and_return_amount": 500,
                    }
                ],
                "page_count": 2,
            },
            {
                "cash_flows": [
                    {
                        "period": {"begin": "2026-03-16T00:00:00Z", "end": "2026-03-31T23:59:59Z", "id": 2},
                        "orders_amount": 8000,
                        "returns_amount": -500,
                        "commission_amount": 900,
                        "services_amount": 600,
                        "item_delivery_and_return_amount": 400,
                    }
                ],
                "page_count": 2,
            },
        ]
    )
    service = InMemoryFinanceSnapshotService(client)

    snapshot = await service.refresh_cash_flow_snapshot(client_id="1034362", days_back=5)

    assert len(client.calls) == 6
    assert snapshot["summary"]["orders_amount"] == 18000
    assert snapshot["summary"]["returns_amount"] == -2500
    assert snapshot["summary"]["commission_amount"] == 1900
    assert snapshot["summary"]["services_amount"] == 1300
    assert snapshot["summary"]["logistics_amount"] == 900
    assert len(client.compensation_codes) == 1
    assert len(client.decompensation_codes) == 1
    assert client.realization_calls == [(2026, 2), (2026, 1)]
    assert len(client.transaction_totals_calls) == 2
    assert len(client.transaction_list_calls) == 4
    assert snapshot["summary"]["compensation_amount"] == 200
    assert snapshot["summary"]["decompensation_amount"] == 25
    assert snapshot["summary"]["net_payout"] == 11400
    assert snapshot["summary"]["net_payout_adjusted"] == 11575
    assert snapshot["realization_closed_month"]["available"] is True
    assert snapshot["realization_closed_month"]["period"] == "2026-02"
    assert snapshot["realization_closed_month"]["sold_units"] == 5
    assert snapshot["realization_closed_month"]["returned_units"] == 1
    assert snapshot["realization_closed_month"]["net_amount"] == 1600
    assert snapshot["transactions_recent"]["available"] is True
    assert snapshot["transactions_recent"]["totals"]["sale_commission"] == 1800
    assert snapshot["transactions_recent"]["service_buckets"]["marketing"] == 600
    assert snapshot["transactions_recent"]["service_buckets"]["logistics"] == 240
    assert snapshot["transactions_recent"]["service_buckets"]["storage"] == 160
    assert snapshot["transactions_recent"]["service_buckets"]["acquiring"] == 80
    assert snapshot["current_month_to_date"]["orders_amount"] == 18000
    assert snapshot["current_month_to_date"]["returns_amount"] == -2500
    assert snapshot["previous_month_same_period"]["orders_amount"] == 18000
    assert snapshot["previous_month_same_period"]["returns_amount"] == -2500
    assert snapshot["placement_by_products_recent"]["available"] is True
    assert snapshot["placement_by_products_recent"]["amount_total"] == 80.0
    assert snapshot["placement_by_products_recent"]["offers_count"] == 2
    assert snapshot["placement_by_supplies_recent"]["available"] is True
    assert snapshot["placement_by_supplies_recent"]["amount_total"] == 155.5
    assert snapshot["placement_by_supplies_recent"]["supplies_count"] == 2
    assert snapshot["removal_from_stock_recent"]["available"] is True
    assert snapshot["removal_from_stock_recent"]["delivery_price_total"] == 45.0
    assert snapshot["removal_from_stock_recent"]["quantity_total"] == 3
    assert snapshot["removal_from_stock_recent"]["utilization_count"] == 1
    assert snapshot["removal_from_supply_recent"]["available"] is True
    assert snapshot["removal_from_supply_recent"]["delivery_price_total"] == 25.0
    assert snapshot["removal_from_supply_recent"]["quantity_total"] == 4
    assert len(client.removal_from_stock_calls) == 1
    assert len(client.removal_from_supply_calls) == 1


async def test_fetch_day_summary_aggregates_requested_day():
    client = FakeFinanceClient(
        [
            {
                "cash_flows": [
                    {
                        "orders_amount": 3200,
                        "returns_amount": -450,
                        "commission_amount": 200,
                        "services_amount": 100,
                        "item_delivery_and_return_amount": 90,
                    }
                ],
                "page_count": 2,
            },
            {
                "cash_flows": [
                    {
                        "orders_amount": 1800,
                        "returns_amount": -50,
                        "commission_amount": 120,
                        "services_amount": 60,
                        "item_delivery_and_return_amount": 40,
                    }
                ],
                "page_count": 2,
            },
        ]
    )
    service = OzonFinanceSnapshotService(client)

    summary = await service.fetch_day_summary(report_date=datetime(2026, 3, 24, tzinfo=timezone.utc).date())

    assert len(client.calls) == 2
    assert summary["orders_amount"] == 5000
    assert summary["returns_amount"] == -500
    assert summary["commission_amount"] == 320
    assert summary["services_amount"] == 160
    assert summary["logistics_amount"] == 130


async def test_load_recent_transaction_snapshot_falls_back_to_totals_when_list_fails():
    client = FailingTransactionListClient([])
    service = OzonFinanceSnapshotService(client)

    snapshot = await service._load_recent_transaction_snapshot(
        date_from=datetime(2026, 2, 24, tzinfo=timezone.utc),
        date_to=datetime(2026, 3, 25, tzinfo=timezone.utc),
    )

    assert snapshot["totals"]["accruals_for_sale"] == 20000
    assert snapshot["totals"]["sale_commission"] == 1800
    assert snapshot["rows_count"] == 0
    assert snapshot["warnings"]
    assert snapshot["details_available"] is False


async def test_load_recent_transaction_snapshot_counts_marketing_operations_without_services():
    class OperationOnlyMarketingClient(FakeFinanceClient):
        async def get_transaction_list(self, **kwargs):
            self.transaction_list_calls.append(kwargs)
            return {
                "operations": [
                    {
                        "amount": -500,
                        "operation_type_name": "Оплата за клик",
                        "services": [],
                    }
                ],
                "page_count": 1,
            }

    client = OperationOnlyMarketingClient([])
    service = OzonFinanceSnapshotService(client)

    snapshot = await service._load_recent_transaction_snapshot(
        date_from=datetime(2026, 2, 24, tzinfo=timezone.utc),
        date_to=datetime(2026, 3, 25, tzinfo=timezone.utc),
    )

    assert snapshot["service_buckets"]["marketing"] == -1000
    assert snapshot["top_services"][0]["name"] == "Оплата за клик"
    assert snapshot["top_services"][0]["bucket"] == "marketing"


def test_parse_placement_by_products_report_from_xlsx():
    raw_bytes = build_minimal_xlsx(
        [
            ["Артикул", "Наименование товара", "Стоимость размещения", "Количество", "Дней размещения"],
            ["offer-a", "Товар A", "55.5", "3", "14"],
            ["offer-b", "Товар B", "24.5", "2", "10"],
        ]
    )

    snapshot = OzonFinanceSnapshotService._parse_placement_by_products_report(
        raw_bytes,
        date_from=datetime(2026, 2, 24, tzinfo=timezone.utc).date(),
        date_to=datetime(2026, 3, 25, tzinfo=timezone.utc).date(),
    )

    assert snapshot["rows_count"] == 2
    assert snapshot["offers_count"] == 2
    assert snapshot["amount_total"] == 80.0
    assert snapshot["top_items"][0]["offer_id"] == "offer-a"
    assert snapshot["top_items"][0]["amount"] == 55.5


def test_parse_placement_by_supplies_report_from_xlsx():
    raw_bytes = build_minimal_xlsx(
        [
            ["Период: 24.02.2026 - 25.03.2026"],
            ["Дата формирования: 25.03.2026"],
            [],
            ["SKU", "Номер поставки", "Склад поставки", "24.февр", "25.февр", "26.февр"],
            ["3517702051", "SUP-1001", "ХОРУГВИНО", "20", "20", "20"],
            ["3517702052", "SUP-1002", "ЖУКОВСКИЙ", "5", "15", "15"],
        ]
    )

    snapshot = OzonFinanceSnapshotService._parse_placement_by_supplies_report(
        raw_bytes,
        date_from=datetime(2026, 2, 24, tzinfo=timezone.utc).date(),
        date_to=datetime(2026, 3, 25, tzinfo=timezone.utc).date(),
    )

    assert snapshot["rows_count"] == 2
    assert snapshot["supplies_count"] == 2
    assert snapshot["metric_kind"] == "stock_days"
    assert snapshot["amount_total"] == 0.0
    assert snapshot["stock_days_total"] == 95
    assert snapshot["top_items"][0]["supply_ref"] == "SUP-1001"
    assert snapshot["top_items"][0]["amount"] == 0.0
    assert snapshot["top_items"][0]["stock_days_total"] == 60
