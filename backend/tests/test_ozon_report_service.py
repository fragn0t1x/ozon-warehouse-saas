from datetime import datetime, timedelta, timezone

from app.services.ozon.report_service import OzonReportReady, OzonReportService


class _FakeReportClient:
    def __init__(self, *, reports=None, report_info=None):
        self._reports = reports or []
        self._report_info = report_info or {}
        self.created = []

    async def list_reports(self, *, page: int = 1, page_size: int = 100, report_type: str = "ALL"):
        return {
            "result": {
                "reports": self._reports,
                "total": len(self._reports),
            }
        }

    async def get_report_info(self, code: str):
        return self._report_info[code]

    async def create_products_report(self, **kwargs):
        self.created.append(("products", kwargs))
        return "new-products-code"

    async def create_postings_report(self, **kwargs):
        self.created.append(("postings", kwargs))
        return "new-postings-code"

    async def download_report_file(self, file_url: str) -> bytes:
        return file_url.encode("utf-8")


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


async def test_find_recent_success_report_matches_params():
    now = datetime.now(timezone.utc)
    client = _FakeReportClient(
        reports=[
            {
                "code": "old",
                "status": "success",
                "report_type": "SELLER_PRODUCTS",
                "params": {"visibility": "ARCHIVED"},
                "created_at": _iso(now),
            },
            {
                "code": "ok",
                "status": "success",
                "report_type": "SELLER_PRODUCTS",
                "params": {"visibility": "ALL"},
                "created_at": _iso(now),
            },
        ]
    )
    service = OzonReportService(client)

    report = await service.find_recent_success_report(
        report_type="SELLER_PRODUCTS",
        expected_params={"visibility": "ALL"},
    )

    assert report is not None
    assert report["code"] == "ok"


async def test_find_recent_success_report_skips_stale_reports(monkeypatch):
    stale = datetime.now(timezone.utc) - timedelta(days=1)
    client = _FakeReportClient(
        reports=[
            {
                "code": "stale",
                "status": "success",
                "report_type": "SELLER_PRODUCTS",
                "params": {"visibility": "ALL"},
                "created_at": _iso(stale),
            }
        ]
    )
    service = OzonReportService(client)

    report = await service.find_recent_success_report(
        report_type="SELLER_PRODUCTS",
        expected_params={"visibility": "ALL"},
    )

    assert report is None


async def test_ensure_products_report_reuses_existing_success_report():
    now = datetime.now(timezone.utc)
    client = _FakeReportClient(
        reports=[
            {
                "code": "ready-code",
                "status": "success",
                "report_type": "SELLER_PRODUCTS",
                "params": {"visibility": "ALL"},
                "created_at": _iso(now),
            }
        ],
        report_info={
            "ready-code": {
                "code": "ready-code",
                "status": "success",
                "report_type": "SELLER_PRODUCTS",
                "file": "https://example.com/report.csv",
                "created_at": _iso(now),
                "expires_at": _iso(now + timedelta(hours=1)),
            }
        },
    )
    service = OzonReportService(client)

    ready = await service.ensure_products_report()

    assert isinstance(ready, OzonReportReady)
    assert ready.code == "ready-code"
    assert client.created == []


async def test_download_ready_report_uses_client_file_loader():
    client = _FakeReportClient()
    service = OzonReportService(client)
    ready = OzonReportReady(
        code="code",
        report_type="SELLER_PRODUCTS",
        status="success",
        file_url="https://example.com/report.csv",
        created_at=None,
        expires_at=None,
        raw={},
    )

    content = await service.download_ready_report(ready)

    assert content == b"https://example.com/report.csv"
