from app.services.dashboard_sales_service import DashboardSalesService


class FakeDashboardSalesService(DashboardSalesService):
    def __init__(self, snapshots):
        self.snapshots = snapshots

    async def _get_snapshot(self, client_id: str):
        return self.snapshots.get(client_id)


async def test_build_fbo_sales_summary_aggregates_multiple_store_snapshots():
    service = FakeDashboardSalesService(
        {
            "1034362": {
                "refreshed_at": "2026-03-18T10:45:00+00:00",
                "preview": {
                    "summary": {
                        "total_units": 8,
                        "total_revenue": 7200,
                        "top_offers": [
                            {"offer_id": "FUT1белыйM", "title": "Футболка ПРЕМИУМ", "units": 3, "revenue": 2997},
                        ],
                    }
                },
            },
            "3148949": {
                "refreshed_at": "2026-03-18T11:00:00+00:00",
                "preview": {
                    "summary": {
                        "total_units": 5,
                        "total_revenue": 4100,
                        "top_offers": [
                            {"offer_id": "NOSKI1", "title": "Носки", "units": 5, "revenue": 4100},
                        ],
                    }
                },
            },
        }
    )

    result = await service.build_fbo_sales_summary(
        stores=[
            {"name": "Веня", "client_id": "1034362"},
            {"name": "Паша", "client_id": "3148949"},
            {"name": "Чуви", "client_id": "440365"},
        ]
    )

    assert result["source"] == "report_snapshot"
    assert result["total_units"] == 13
    assert result["total_revenue"] == 11300
    assert result["stores_covered"] == 2
    assert result["stores_missing"] == 1
    assert result["top_offers"][0]["offer_id"] == "NOSKI1"
