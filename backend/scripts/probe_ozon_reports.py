from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from app.services.ozon.client import OzonClient
from app.services.ozon.report_service import OzonReportService
from app.services.ozon.report_snapshot_service import OzonReportSnapshotService


async def main_async(args) -> int:
    client = OzonClient(args.client_id, args.api_key, emit_notifications=False)
    service = OzonReportService(client)
    snapshot_service = OzonReportSnapshotService(service)

    try:
        result: dict[str, object] = {
            "client_id": args.client_id,
            "report": None,
            "download_preview": None,
        }

        if args.report_type == "products":
            ready = await service.ensure_products_report(visibility=args.visibility)
            if args.download:
                snapshot = await snapshot_service.refresh_products_snapshot(client_id=args.client_id, visibility=args.visibility)
                result["snapshot_preview"] = snapshot.get("preview")
        else:
            if not args.date_from or not args.date_to:
                raise ValueError("--date-from and --date-to are required for postings report")
            ready = await service.ensure_fbo_postings_report(
                processed_at_from=args.date_from,
                processed_at_to=args.date_to,
            )
            if args.download:
                content = await service.download_ready_report(ready)
                preview = snapshot_service._parse_preview(content, kind="postings")
                result["snapshot_preview"] = {
                    "headers": preview.headers,
                    "rows": preview.rows,
                    "summary": preview.summary,
                }

        result["report"] = {
            "code": ready.code,
            "report_type": ready.report_type,
            "status": ready.status,
            "file_url": ready.file_url,
            "created_at": ready.created_at,
            "expires_at": ready.expires_at,
        }

        if args.download:
            content = await service.download_ready_report(ready)
            preview = content[:400].decode("utf-8", errors="replace")
            result["download_preview"] = preview

        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(output_path)
        return 0
    finally:
        await client.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe Ozon async reports")
    parser.add_argument("--client-id", default=os.getenv("OZON_CLIENT_ID"))
    parser.add_argument("--api-key", default=os.getenv("OZON_API_KEY"))
    parser.add_argument("--report-type", choices=["products", "postings"], default="products")
    parser.add_argument("--visibility", default="ALL")
    parser.add_argument("--date-from")
    parser.add_argument("--date-to")
    parser.add_argument("--download", action="store_true")
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[1] / "tmp" / "ozon_report_probe.json"),
    )
    args = parser.parse_args()

    if not args.client_id or not args.api_key:
        raise SystemExit("Pass --client-id and --api-key or set OZON_CLIENT_ID / OZON_API_KEY")

    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
