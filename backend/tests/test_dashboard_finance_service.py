from app.services.dashboard_finance_service import DashboardFinanceService


class FakeDashboardFinanceService(DashboardFinanceService):
    def __init__(self, snapshots):
        self.snapshots = snapshots

    async def _get_snapshot(self, client_id: str):
        return self.snapshots.get(client_id)


async def test_build_finance_summary_aggregates_store_snapshots():
    service = FakeDashboardFinanceService(
        {
            "1034362": {
                "refreshed_at": "2026-03-18T11:05:00+00:00",
                "summary": {
                    "orders_amount": 10000,
                    "returns_amount": -1500,
                    "commission_amount": 1100,
                    "services_amount": 400,
                    "logistics_amount": 300,
                    "net_payout": 6700,
                    "compensation_amount": 300,
                    "decompensation_amount": 50,
                    "net_payout_adjusted": 6950,
                },
                "transactions_recent": {
                    "available": True,
                    "period": {"from": "2026-02-17T00:00:00Z", "to": "2026-03-18T23:59:59Z", "days": 30},
                    "totals": {
                        "accruals_for_sale": 10000,
                        "compensation_amount": 200,
                        "money_transfer": 0,
                        "others_amount": 150,
                        "processing_and_delivery": 1100,
                        "refunds_and_cancellations": 350,
                        "sale_commission": 900,
                        "services_amount": 700,
                    },
                    "service_buckets": {
                        "marketing": 600,
                        "storage": 160,
                        "acquiring": 80,
                        "returns": 0,
                        "logistics": 240,
                        "other": 0,
                    },
                    "top_services": [
                        {"name": "Трафареты", "bucket": "marketing", "amount": 420, "count": 3},
                        {"name": "Продвижение в поиске", "bucket": "marketing", "amount": 180, "count": 2},
                    ],
                },
                "placement_by_products_recent": {
                    "available": True,
                    "period": {"from": "2026-02-17", "to": "2026-03-18", "days": 30},
                    "amount_total": 180.0,
                    "rows_count": 3,
                    "offers_count": 2,
                    "top_items": [
                        {"offer_id": "offer-a", "title": "Товар A", "amount": 120.0, "quantity": 3, "days": 14},
                    ],
                },
                "placement_by_supplies_recent": {
                    "available": True,
                    "period": {"from": "2026-02-17", "to": "2026-03-18", "days": 30},
                    "metric_kind": "amount",
                    "amount_total": 260.0,
                    "stock_days_total": 0,
                    "rows_count": 2,
                    "supplies_count": 2,
                    "top_items": [
                        {"supply_ref": "SUP-1001", "warehouse_name": "ХОРУГВИНО", "amount": 180.0, "stock_days_total": 0, "items_count": 24, "days": 18},
                    ],
                },
                "removal_from_stock_recent": {
                    "available": True,
                    "kind": "from_stock",
                    "kind_label": "Со стока",
                    "period": {"from": "2026-02-17", "to": "2026-03-18", "days": 30},
                    "rows_count": 2,
                    "returns_count": 2,
                    "offers_count": 2,
                    "quantity_total": 3,
                    "delivery_price_total": 45.0,
                    "auto_returns_count": 1,
                    "utilization_count": 1,
                    "items": [
                        {"offer_id": "offer-a", "title": "Товар A", "quantity_total": 2, "delivery_price_total": 30.0, "auto_returns_count": 1, "utilization_count": 1, "last_return_state": "утилизирована", "delivery_type": "самовывоз", "stock_type": "брак"},
                    ],
                    "states": [
                        {"state": "утилизирована", "count": 1, "quantity_total": 2, "delivery_price_total": 30.0},
                    ],
                },
                "removal_from_supply_recent": {
                    "available": True,
                    "kind": "from_supply",
                    "kind_label": "С поставки",
                    "period": {"from": "2026-02-17", "to": "2026-03-18", "days": 30},
                    "rows_count": 1,
                    "returns_count": 1,
                    "offers_count": 1,
                    "quantity_total": 4,
                    "delivery_price_total": 25.0,
                    "auto_returns_count": 0,
                    "utilization_count": 0,
                    "items": [
                        {"offer_id": "offer-b", "title": "Товар B", "quantity_total": 4, "delivery_price_total": 25.0, "auto_returns_count": 0, "utilization_count": 0, "last_return_state": "готово к вывозу", "delivery_type": "самовывоз", "stock_type": ""},
                    ],
                    "states": [
                        {"state": "готово к вывозу", "count": 1, "quantity_total": 4, "delivery_price_total": 25.0},
                    ],
                },
            },
            "3148949": {
                "refreshed_at": "2026-03-18T11:15:00+00:00",
                "summary": {
                    "orders_amount": 5000,
                    "returns_amount": -500,
                    "commission_amount": 600,
                    "services_amount": 250,
                    "logistics_amount": 150,
                    "net_payout": 3500,
                    "compensation_amount": 100,
                    "decompensation_amount": 20,
                    "net_payout_adjusted": 3580,
                },
                "transactions_recent": {
                    "available": True,
                    "period": {"from": "2026-02-17T00:00:00Z", "to": "2026-03-18T23:59:59Z", "days": 30},
                    "totals": {
                        "accruals_for_sale": 5000,
                        "compensation_amount": 0,
                        "money_transfer": 0,
                        "others_amount": 50,
                        "processing_and_delivery": 500,
                        "refunds_and_cancellations": 120,
                        "sale_commission": 400,
                        "services_amount": 260,
                    },
                    "service_buckets": {
                        "marketing": 110,
                        "storage": 50,
                        "acquiring": 20,
                        "returns": 30,
                        "logistics": 50,
                        "other": 0,
                    },
                    "top_services": [
                        {"name": "Трафареты", "bucket": "marketing", "amount": 60, "count": 1},
                        {"name": "Брендовая полка", "bucket": "marketing", "amount": 50, "count": 1},
                    ],
                },
                "placement_by_products_recent": {
                    "available": True,
                    "period": {"from": "2026-02-17", "to": "2026-03-18", "days": 30},
                    "amount_total": 70.0,
                    "rows_count": 2,
                    "offers_count": 1,
                    "top_items": [
                        {"offer_id": "offer-b", "title": "Товар B", "amount": 70.0, "quantity": 2, "days": 10},
                    ],
                },
                "placement_by_supplies_recent": {
                    "available": True,
                    "period": {"from": "2026-02-17", "to": "2026-03-18", "days": 30},
                    "metric_kind": "amount",
                    "amount_total": 90.0,
                    "stock_days_total": 0,
                    "rows_count": 1,
                    "supplies_count": 1,
                    "top_items": [
                        {"supply_ref": "SUP-2001", "warehouse_name": "ЖУКОВСКИЙ", "amount": 90.0, "stock_days_total": 0, "items_count": 12, "days": 7},
                    ],
                },
                "removal_from_stock_recent": {
                    "available": True,
                    "kind": "from_stock",
                    "kind_label": "Со стока",
                    "period": {"from": "2026-02-17", "to": "2026-03-18", "days": 30},
                    "rows_count": 1,
                    "returns_count": 1,
                    "offers_count": 1,
                    "quantity_total": 1,
                    "delivery_price_total": 12.0,
                    "auto_returns_count": 0,
                    "utilization_count": 0,
                    "items": [
                        {"offer_id": "offer-z", "title": "Товар Z", "quantity_total": 1, "delivery_price_total": 12.0, "auto_returns_count": 0, "utilization_count": 0, "last_return_state": "можно забрать все", "delivery_type": "ПВЗ", "stock_type": "доступно к продаже"},
                    ],
                    "states": [
                        {"state": "можно забрать все", "count": 1, "quantity_total": 1, "delivery_price_total": 12.0},
                    ],
                },
                "removal_from_supply_recent": {
                    "available": False,
                },
            },
        },
    )

    result = await service.build_finance_summary(
        stores=[
            {"name": "Веня", "client_id": "1034362"},
            {"name": "Паша", "client_id": "3148949"},
            {"name": "Чуви", "client_id": "440365"},
        ]
    )

    assert result["source"] == "finance_snapshot"
    assert result["orders_amount"] == 15000
    assert result["returns_amount"] == -2000
    assert result["commission_amount"] == 1700
    assert result["services_amount"] == 650
    assert result["logistics_amount"] == 450
    assert result["net_payout"] == 10200
    assert result["compensation_amount"] == 400
    assert result["decompensation_amount"] == 70
    assert result["net_payout_adjusted"] == 10530
    assert result["stores_covered"] == 2
    assert result["stores_missing"] == 1
    assert result["realization_closed_month"]["net_fee"] == 0
    assert result["closed_month_cashflow"]["orders_amount"] == 0
    assert result["store_breakdown"][0]["store_name"] == "Веня"
    assert result["placement_by_products_recent"]["available"] is True
    assert result["placement_by_products_recent"]["amount_total"] == 250.0
    assert result["placement_by_products_recent"]["offers_count"] == 3
    assert result["placement_by_products_recent"]["store_breakdown"][0]["store_name"] == "Веня"
    assert result["placement_by_supplies_recent"]["available"] is True
    assert result["placement_by_supplies_recent"]["metric_kind"] == "amount"
    assert result["placement_by_supplies_recent"]["amount_total"] == 350.0
    assert result["placement_by_supplies_recent"]["stock_days_total"] == 0
    assert result["placement_by_supplies_recent"]["supplies_count"] == 3
    assert result["placement_by_supplies_recent"]["store_breakdown"][0]["store_name"] == "Веня"
    assert result["removals_recent"]["available"] is True
    assert result["removals_recent"]["delivery_price_total"] == 82.0
    assert result["removals_recent"]["quantity_total"] == 8
    assert result["removals_recent"]["auto_returns_count"] == 1
    assert result["removals_recent"]["utilization_count"] == 1
    assert result["removals_recent"]["source_breakdown"][0]["kind"] == "from_stock"
    assert result["removals_recent"]["store_breakdown"][0]["store_name"] == "Веня"
    assert result["marketing_recent"]["available"] is True
    assert result["marketing_recent"]["amount_total"] == 710.0
    assert result["marketing_recent"]["services_count"] == 7
    assert result["marketing_recent"]["store_breakdown"][0]["store_name"] == "Веня"
    assert result["marketing_recent"]["top_services"][0]["name"] == "Трафареты"
