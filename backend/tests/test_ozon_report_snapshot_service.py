from app.services.ozon.report_service import OzonReportReady
from app.services.ozon.report_snapshot_service import OzonReportSnapshotService


class FakeReportService:
    def __init__(self, *, products_bytes: bytes, postings_bytes: bytes):
        self.products_bytes = products_bytes
        self.postings_bytes = postings_bytes
        self.products_calls = 0
        self.postings_calls = 0

    async def ensure_products_report(self, **kwargs):
        self.products_calls += 1
        return OzonReportReady(
            code="products-code",
            report_type="SELLER_PRODUCTS",
            status="success",
            file_url="https://example.com/products.csv",
            created_at="2026-03-18T09:00:00Z",
            expires_at=None,
            raw={},
        )

    async def ensure_fbo_postings_report(self, **kwargs):
        self.postings_calls += 1
        return OzonReportReady(
            code="postings-code",
            report_type="SELLER_POSTINGS",
            status="success",
            file_url="https://example.com/postings.csv",
            created_at="2026-03-18T09:00:00Z",
            expires_at=None,
            raw={},
        )

    async def download_ready_report(self, ready_report: OzonReportReady) -> bytes:
        if ready_report.report_type == "SELLER_PRODUCTS":
            return self.products_bytes
        return self.postings_bytes


class InMemorySnapshotService(OzonReportSnapshotService):
    def __init__(self, report_service):
        super().__init__(report_service)
        self.saved = {}

    async def get_cached_snapshot(self, *, client_id: str, kind: str):
        return self.saved.get((client_id, kind))

    async def _save(self, client_id: str, kind: str, payload: dict):
        self.saved[(client_id, kind)] = payload
        return payload

    async def refresh_products_snapshot(self, *, client_id: str, visibility: str = "ALL") -> dict:
        ready = await self.report_service.ensure_products_report(visibility=visibility)
        preview = self._parse_preview(await self.report_service.download_ready_report(ready), kind="products")
        payload = self._build_snapshot_payload(
            client_id=client_id,
            kind="products",
            ready_report=ready,
            preview=preview,
            extra={"filters": {"visibility": visibility}},
        )
        return await self._save(client_id, "products", payload)

    async def refresh_fbo_postings_snapshot(self, *, client_id: str, days_back: int = 30) -> dict:
        ready = await self.report_service.ensure_fbo_postings_report(
            processed_at_from="2026-02-17T00:00:00Z",
            processed_at_to="2026-03-18T00:00:00Z",
        )
        preview = self._parse_preview(await self.report_service.download_ready_report(ready), kind="postings")
        payload = self._build_snapshot_payload(
            client_id=client_id,
            kind="postings",
            ready_report=ready,
            preview=preview,
            extra={"filters": {"delivery_schema": "fbo", "days_back": days_back}},
        )
        return await self._save(client_id, "postings", payload)


async def test_parse_products_snapshot_preview_extracts_offer_ids():
    service = InMemorySnapshotService(
        FakeReportService(
            products_bytes=(
                "Артикул;Название товара;Barcode\n"
                "FUT1белыйM;Футболка ПРЕМИУМ Белая M;2900\n"
                "FUT1чернM;Футболка ПРЕМИУМ Черная M;2901\n"
            ).encode("utf-8"),
            postings_bytes=b"offer_id;quantity;price\n",
        )
    )

    snapshot = await service.refresh_products_snapshot(client_id="1034362")

    assert snapshot["preview"]["summary"]["total_rows"] == 2
    assert snapshot["preview"]["summary"]["unique_offer_ids"] == 2
    assert snapshot["preview"]["summary"]["top_offers"][0]["offer_id"] == "FUT1белыйM"


async def test_parse_postings_snapshot_aggregates_units_and_revenue():
    service = InMemorySnapshotService(
        FakeReportService(
            products_bytes=b"offer_id;name\n",
            postings_bytes=(
                "offer_id;quantity;price;name\n"
                "FUT1белыйM;2;999.5;Футболка ПРЕМИУМ Белая M\n"
                "FUT1белыйM;1;999.5;Футболка ПРЕМИУМ Белая M\n"
                "FUT1чернM;5;899;Футболка ПРЕМИУМ Черная M\n"
            ).encode("utf-8"),
        )
    )

    snapshot = await service.refresh_fbo_postings_snapshot(client_id="1034362", days_back=30)
    top_offers = snapshot["preview"]["summary"]["top_offers"]

    assert snapshot["preview"]["summary"]["total_rows"] == 3
    assert top_offers[0]["offer_id"] == "FUT1чернM"
    assert top_offers[0]["units"] == 5
    assert top_offers[1]["offer_id"] == "FUT1белыйM"
    assert top_offers[1]["units"] == 3
    assert top_offers[1]["revenue"] == 1999.0


async def test_parse_postings_snapshot_builds_warehouse_order_aggregates():
    service = InMemorySnapshotService(
        FakeReportService(
            products_bytes=b"offer_id;name\n",
            postings_bytes=(
                "Артикул;SKU;Количество;Склад отгрузки\n"
                "FUT1белыйM;1592743582;2;ЕКАТЕРИНБУРГ_РФЦ_НОВЫЙ\n"
                "FUT1белыйM;1592743582;1;ЕКАТЕРИНБУРГ_РФЦ_НОВЫЙ\n"
                "FUT1чернM;1592743294;5;МОСКВА_РФЦ\n"
            ).encode("utf-8"),
        )
    )

    snapshot = await service.refresh_fbo_postings_snapshot(client_id="1034362", days_back=30)
    analytics = (snapshot.get("analytics") or {}).get("shipment_orders") or {}

    assert analytics["warehouse_totals"]["екатеринбург_рфц_новый"] == 3
    assert analytics["warehouse_sku_units"]["екатеринбург_рфц_новый"]["1592743582"] == 3
    assert analytics["warehouse_offer_units"]["екатеринбург_рфц_новый"]["FUT1белыйM"] == 3
    assert analytics["warehouse_labels"]["екатеринбург_рфц_новый"] == "ЕКАТЕРИНБУРГ_РФЦ_НОВЫЙ"


async def test_parse_postings_snapshot_builds_daily_offer_stats():
    service = InMemorySnapshotService(
        FakeReportService(
            products_bytes=b"offer_id;name\n",
            postings_bytes=(
                "Артикул;Количество;Сумма;Принят в обработку;Название товара\n"
                "FUT1белыйM;2;1999;24.03.2026 10:15;Футболка белая\n"
                "FUT1белыйM;1;999;24.03.2026 12:30;Футболка белая\n"
                "FUT1чернM;5;4495;23.03.2026 09:00;Футболка черная\n"
            ).encode("utf-8"),
        )
    )

    snapshot = await service.refresh_fbo_postings_snapshot(client_id="1034362", days_back=30)

    day_summary = OzonReportSnapshotService.get_postings_day_summary(snapshot, day="2026-03-24")
    day_offer_stats = OzonReportSnapshotService.get_postings_day_offer_stats(snapshot, day="2026-03-24")

    assert day_summary["units"] == 3
    assert day_summary["revenue"] == 2998
    assert day_offer_stats[0]["offer_id"] == "FUT1белыйM"
    assert day_offer_stats[0]["units"] == 3
    assert day_offer_stats[0]["revenue"] == 2998
