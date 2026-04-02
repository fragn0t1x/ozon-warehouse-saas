from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from app.config import settings
from app.services.ozon.client import OzonClient


ReportCreator = Callable[[], asyncio.Future | Any]


@dataclass
class OzonReportReady:
    code: str
    report_type: str
    status: str
    file_url: str
    created_at: str | None
    expires_at: str | None
    raw: dict[str, Any]


class OzonReportService:
    def __init__(self, client: OzonClient):
        self.client = client

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        normalized = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    @staticmethod
    def _params_match(report_params: dict[str, Any] | None, expected_params: dict[str, Any] | None) -> bool:
        if not expected_params:
            return True
        if not isinstance(report_params, dict):
            return False

        for key, expected_value in expected_params.items():
            actual_value = report_params.get(key)
            if isinstance(expected_value, list):
                if sorted(actual_value or []) != sorted(expected_value):
                    return False
            else:
                if str(actual_value) != str(expected_value):
                    return False
        return True

    async def find_recent_success_report(
        self,
        *,
        report_type: str,
        expected_params: dict[str, Any] | None = None,
        max_pages: int = 2,
    ) -> dict[str, Any] | None:
        reuse_window = timedelta(seconds=max(int(settings.OZON_REPORT_REUSE_WINDOW_SECONDS), 0))
        now = datetime.now(timezone.utc)

        for page in range(1, max_pages + 1):
            payload = await self.client.list_reports(page=page, page_size=100, report_type=report_type)
            result = payload.get("result") or {}
            reports = result.get("reports") or []
            for report in reports:
                if report.get("status") != "success":
                    continue
                if str(report.get("report_type", "")).upper() != report_type.upper():
                    continue
                if not self._params_match(report.get("params") or {}, expected_params):
                    continue
                created_at = self._parse_dt(report.get("created_at"))
                if created_at and reuse_window and created_at < now - reuse_window:
                    continue
                return report
        return None

    async def wait_until_ready(self, code: str) -> OzonReportReady:
        timeout_seconds = max(int(settings.OZON_REPORT_POLL_TIMEOUT_SECONDS), 1)
        poll_seconds = max(int(settings.OZON_REPORT_POLL_INTERVAL_SECONDS), 1)
        deadline = asyncio.get_running_loop().time() + timeout_seconds

        while True:
            report = await self.client.get_report_info(code)
            status = (report.get("status") or "").lower()
            if status == "success" and report.get("file"):
                return OzonReportReady(
                    code=report.get("code") or code,
                    report_type=report.get("report_type") or "",
                    status=status,
                    file_url=report.get("file") or "",
                    created_at=report.get("created_at"),
                    expires_at=report.get("expires_at"),
                    raw=report,
                )
            if status == "failed":
                raise RuntimeError(f"Ozon report {code} failed: {report.get('error') or 'unknown error'}")
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(f"Ozon report {code} is still not ready after {timeout_seconds} seconds")
            await asyncio.sleep(poll_seconds)

    async def ensure_report(
        self,
        *,
        report_type: str,
        expected_params: dict[str, Any] | None,
        create_report: Callable[[], Any],
    ) -> OzonReportReady:
        recent = await self.find_recent_success_report(
            report_type=report_type,
            expected_params=expected_params,
        )
        if recent and recent.get("code"):
            return await self.wait_until_ready(recent["code"])

        code = await create_report()
        if not code:
            raise RuntimeError(f"Ozon did not return a report code for {report_type}")
        return await self.wait_until_ready(code)

    async def ensure_products_report(
        self,
        *,
        visibility: str = "ALL",
        language: str = "DEFAULT",
    ) -> OzonReportReady:
        return await self.ensure_report(
            report_type="SELLER_PRODUCTS",
            expected_params={"visibility": visibility},
            create_report=lambda: self.client.create_products_report(
                visibility=visibility,
                language=language,
            ),
        )

    async def ensure_fbo_postings_report(
        self,
        *,
        processed_at_from: str,
        processed_at_to: str,
        language: str = "DEFAULT",
        analytics_data: bool = False,
    ) -> OzonReportReady:
        return await self.ensure_report(
            report_type="SELLER_POSTINGS",
            expected_params={
                "processed_at_from": processed_at_from,
                "processed_at_to": processed_at_to,
                "delivery_schema": ["fbo"],
                "analytics_data": analytics_data,
            },
            create_report=lambda: self.client.create_postings_report(
                processed_at_from=processed_at_from,
                processed_at_to=processed_at_to,
                language=language,
                analytics_data=analytics_data,
            ),
        )

    async def ensure_returns_report(
        self,
        *,
        date_from: str,
        date_to: str,
        status: str = "",
        language: str = "DEFAULT",
    ) -> OzonReportReady:
        expected_params = {
            "date_from": date_from,
            "date_to": date_to,
        }
        if status:
            expected_params["status"] = status
        return await self.ensure_report(
            report_type="SELLER_RETURNS",
            expected_params=expected_params,
            create_report=lambda: self.client.create_returns_report(
                date_from=date_from,
                date_to=date_to,
                status=status,
                language=language,
            ),
        )

    async def download_ready_report(self, ready_report: OzonReportReady) -> bytes:
        if not ready_report.file_url:
            raise RuntimeError(f"Ozon report {ready_report.code} does not contain a file URL")
        return await self.client.download_report_file(ready_report.file_url)

    async def ensure_compensation_report(
        self,
        *,
        date: str,
        language: str = "RU",
    ) -> OzonReportReady:
        return await self.ensure_report(
            report_type="COMPENSATION_REPORT",
            expected_params={"date": date},
            create_report=lambda: self.client.create_compensation_report(
                date=date,
                language=language,
            ),
        )

    async def ensure_decompensation_report(
        self,
        *,
        date: str,
        language: str = "RU",
    ) -> OzonReportReady:
        return await self.ensure_report(
            report_type="DECOMPENSATION_REPORT",
            expected_params={"date": date},
            create_report=lambda: self.client.create_decompensation_report(
                date=date,
                language=language,
            ),
        )
