from app.services.dashboard_unit_economics_service import DashboardUnitEconomicsService


class FakeDashboardUnitEconomicsService(DashboardUnitEconomicsService):
    def __init__(self, costs, sales_snapshots, finance_snapshots, current_product_info=None, historical_profiles=None):
        self.costs = costs
        self.sales_snapshots = sales_snapshots
        self.finance_snapshots = finance_snapshots
        self.current_product_info = current_product_info or {}
        self.historical_profiles = historical_profiles or {}

    async def _load_variant_costs(self, db, user_id: int, *, warehouse_mode: str = "shared"):
        return self.costs

    async def _load_current_product_info_map(self, store, *, offer_ids):
        store_id = int(store.get("id") or 0)
        store_map = self.current_product_info.get(store_id) or {}
        return {offer_id: store_map[offer_id] for offer_id in offer_ids if offer_id in store_map}

    async def _load_historical_offer_profiles(self, db, *, rows):
        return self.historical_profiles


async def test_build_summary_allocates_financial_load_by_revenue_share():
    service = FakeDashboardUnitEconomicsService(
        costs={
            (1, "FUT1белыйM"): 300.0,
            (1, "FUT1чернM"): 250.0,
        },
        sales_snapshots={},
        finance_snapshots={},
    )

    async def fake_cache_get_json(key: str):
        if "ozon-report-snapshot:1034362:postings" in key:
            return {
                "preview": {
                    "summary": {
                        "total_revenue": 3000,
                        "offer_stats": [
                            {"offer_id": "FUT1белыйM", "title": "Белая", "units": 2, "revenue": 2000},
                            {"offer_id": "FUT1чернM", "title": "Черная", "units": 1, "revenue": 1000},
                        ],
                    }
                }
            }
        if "ozon-finance-snapshot:1034362" in key:
            return {
                "transactions_recent": {
                    "available": True,
                    "totals": {
                        "sale_commission": -300,
                        "services_amount": -150,
                        "processing_and_delivery": -90,
                        "refunds_and_cancellations": 0,
                        "compensation_amount": 60,
                        "others_amount": 0,
                    },
                }
            }
        return None

    import app.services.dashboard_unit_economics_service as module

    original = module.cache_get_json
    module.cache_get_json = fake_cache_get_json
    try:
        summary = await service.build_summary(
            db=None,
            user_id=1,
            stores=[{"id": 1, "name": "Веня", "client_id": "1034362"}],
        )
    finally:
        module.cache_get_json = original

    assert summary["source"] == "estimated"
    assert summary["offers_total"] == 2
    assert summary["offers_with_cost"] == 2
    assert summary["tracked_cogs"] == 850
    assert summary["estimated_net_profit"] == 1419.5
    assert summary["top_profitable_offers"][0]["offer_id"] == "FUT1белыйM"


async def test_build_report_filters_loss_rows_for_selected_store():
    service = FakeDashboardUnitEconomicsService(
        costs={
            (1, "LOSS1"): 900.0,
            (1, "PROFIT1"): 100.0,
            (2, "OTHER1"): 50.0,
        },
        sales_snapshots={},
        finance_snapshots={},
    )

    async def fake_cache_get_json(key: str):
        if "ozon-report-snapshot:1034362:postings" in key:
            return {
                "preview": {
                    "summary": {
                        "total_revenue": 2000,
                        "offer_stats": [
                            {"offer_id": "LOSS1", "title": "Убыточный", "units": 1, "revenue": 500},
                            {"offer_id": "PROFIT1", "title": "Прибыльный", "units": 3, "revenue": 1500},
                        ],
                    }
                }
            }
        if "ozon-finance-snapshot:1034362" in key:
            return {
                "transactions_recent": {
                    "available": True,
                    "totals": {
                        "sale_commission": -100,
                        "services_amount": -50,
                        "processing_and_delivery": -50,
                        "refunds_and_cancellations": 0,
                        "compensation_amount": 0,
                        "others_amount": 0,
                    },
                }
            }
        if "ozon-report-snapshot:440365:postings" in key:
            return {
                "preview": {
                    "summary": {
                        "total_revenue": 500,
                        "offer_stats": [
                            {"offer_id": "OTHER1", "title": "Другой магазин", "units": 2, "revenue": 500},
                        ],
                    }
                }
            }
        if "ozon-finance-snapshot:440365" in key:
            return {
                "transactions_recent": {
                    "available": True,
                    "totals": {
                        "sale_commission": -50,
                        "services_amount": -20,
                        "processing_and_delivery": -10,
                        "refunds_and_cancellations": 0,
                        "compensation_amount": 0,
                        "others_amount": 0,
                    },
                }
            }
        return None

    import app.services.dashboard_unit_economics_service as module

    original = module.cache_get_json
    module.cache_get_json = fake_cache_get_json
    try:
        report = await service.build_report(
            db=None,
            user_id=1,
            stores=[
                {"id": 1, "name": "Веня", "client_id": "1034362"},
                {"id": 2, "name": "Чуви", "client_id": "440365"},
            ],
            store_id=1,
            query="loss",
            profitability="loss",
            limit=50,
        )
    finally:
        module.cache_get_json = original

    assert report["summary"]["source"] == "estimated"
    assert report["rows_total"] == 1
    assert report["rows"][0]["offer_id"] == "LOSS1"
    assert report["filtered_totals"]["estimated_net_profit"] < 0


async def test_build_summary_prefers_closed_month_realization_when_available():
    service = FakeDashboardUnitEconomicsService(
        costs={
            (1, "SKU1"): 300.0,
            (1, "SKU2"): 200.0,
        },
        sales_snapshots={},
        finance_snapshots={},
    )

    async def fake_cache_get_json(key: str):
        if "ozon-report-snapshot:1034362:postings" in key:
            return {
                "preview": {
                    "period": {"days": 30},
                    "summary": {
                        "total_revenue": 999999,
                        "offer_stats": [
                            {"offer_id": "SKU1", "title": "Заказы не должны использоваться", "units": 99, "revenue": 999999},
                        ],
                    },
                }
            }
        if "ozon-finance-snapshot:1034362" in key:
            return {
                "periods": [
                    {
                        "period_begin": "2026-02-01T00:00:00+00:00",
                        "orders_amount": 2200,
                        "returns_amount": 0,
                        "commission_amount": -440,
                        "services_amount": -220,
                        "logistics_amount": -110,
                    }
                ],
                "realization_closed_month": {
                    "available": True,
                    "period": "2026-02",
                    "items": [
                        {
                            "offer_id": "SKU1",
                            "title": "Белая",
                            "sold_units": 3,
                            "returned_units": 1,
                            "net_units": 2,
                            "net_amount": 1500,
                            "net_total": 1200,
                            "net_fee": 300,
                            "net_incentives": 120,
                        },
                        {
                            "offer_id": "SKU2",
                            "title": "Черная",
                            "sold_units": 1,
                            "returned_units": 0,
                            "net_units": 1,
                            "net_amount": 700,
                            "net_total": 560,
                            "net_fee": 140,
                            "net_incentives": 80,
                        },
                    ],
                }
            }
        return None

    import app.services.dashboard_unit_economics_service as module

    original = module.cache_get_json
    module.cache_get_json = fake_cache_get_json
    try:
        summary = await service.build_summary(
            db=None,
            user_id=1,
            stores=[{"id": 1, "name": "Веня", "client_id": "1034362"}],
        )
    finally:
        module.cache_get_json = original

    assert summary["basis"] == "realization_closed_month"
    assert summary["period_label"] == "2026-02"
    assert summary["revenue_label"] == "Чистая реализация"
    assert summary["profit_label"] == "Чистая прибыль"
    assert summary["tracked_revenue"] == 2200
    assert summary["tracked_cogs"] == 800
    assert summary["estimated_net_profit"] == 705.5
    assert summary["top_profitable_offers"][0]["offer_id"] == "SKU1"


async def test_build_report_enriches_rows_with_live_product_price():
    service = FakeDashboardUnitEconomicsService(
        costs={
            (1, "LIVE1"): 300.0,
        },
        sales_snapshots={},
        finance_snapshots={},
        current_product_info={
            1: {
                "LIVE1": {
                    "offer_id": "LIVE1",
                    "price": "700",
                    "old_price": "1000",
                    "min_price": "650",
                    "commissions": [
                        {
                            "sale_schema": "FBO",
                            "percent": 25,
                            "value": 175,
                            "delivery_amount": 45,
                            "return_amount": 60,
                        }
                    ],
                    "price_indexes": {
                        "color_index": "COLOR_INDEX_RED",
                        "ozon_index_data": {"minimal_price": "680"},
                    },
                }
            }
        },
    )

    async def fake_cache_get_json(key: str):
        if "ozon-report-snapshot:1034362:postings" in key:
            return {
                "preview": {
                    "summary": {
                        "total_revenue": 1000,
                        "offer_stats": [
                            {"offer_id": "LIVE1", "title": "Тестовый товар", "units": 1, "revenue": 1000},
                        ],
                    }
                }
            }
        if "ozon-finance-snapshot:1034362" in key:
            return {
                "transactions_recent": {
                    "available": True,
                    "totals": {
                        "sale_commission": -100,
                        "services_amount": -50,
                        "processing_and_delivery": -50,
                        "refunds_and_cancellations": 0,
                        "compensation_amount": 0,
                        "others_amount": 0,
                    },
                }
            }
        return None

    import app.services.dashboard_unit_economics_service as module

    original = module.cache_get_json
    module.cache_get_json = fake_cache_get_json
    try:
        report = await service.build_report(
            db=None,
            user_id=1,
            stores=[{"id": 1, "name": "Веня", "client_id": "1034362"}],
            store_id=1,
            limit=20,
        )
    finally:
        module.cache_get_json = original

    assert report["rows_total"] == 1
    row = report["rows"][0]
    assert row["current_profitability_available"] is True
    assert row["current_price_gross"] == 700.0
    assert row["current_min_price_gross"] == 650.0
    assert row["current_price_index_label"] == "Невыгодный"
    assert row["current_commission_value"] == 175.0
    assert row["current_delivery_amount"] == 45.0
    assert row["current_return_amount"] == 60.0
    assert row["current_allocated_commission"] == -175.0
    assert row["current_allocated_logistics"] == -45.0
    assert row["current_allocated_marketing"] == 0.0
    assert row["current_profit_before_tax"] == 180.0
    assert row["current_estimated_net_profit"] == 153.0


async def test_build_report_applies_return_reserve_and_historical_overheads():
    service = FakeDashboardUnitEconomicsService(
        costs={
            (1, "LIVE2"): 300.0,
        },
        sales_snapshots={},
        finance_snapshots={},
        current_product_info={
            1: {
                "LIVE2": {
                    "offer_id": "LIVE2",
                    "price": "700",
                    "commissions": [
                        {
                            "sale_schema": "FBO",
                            "percent": 25,
                            "value": 175,
                            "delivery_amount": 45,
                            "return_amount": 60,
                        }
                    ],
                }
            }
        },
        historical_profiles={
            (1, "LIVE2"): {
                "months_count": 2,
                "sold_units_total": 30,
                "returned_units_total": 3,
                "return_rate": 0.1,
                "services_per_unit": 12.0,
                "acquiring_per_unit": 8.0,
                "other_per_unit": 5.0,
            }
        },
    )

    async def fake_cache_get_json(key: str):
        if "ozon-report-snapshot:1034362:postings" in key:
            return {
                "preview": {
                    "summary": {
                        "total_revenue": 1000,
                        "offer_stats": [
                            {"offer_id": "LIVE2", "title": "Тестовый товар 2", "units": 1, "revenue": 1000},
                        ],
                    }
                }
            }
        if "ozon-finance-snapshot:1034362" in key:
            return {
                "transactions_recent": {
                    "available": True,
                    "totals": {
                        "sale_commission": -100,
                        "services_amount": -50,
                        "processing_and_delivery": -50,
                        "refunds_and_cancellations": 0,
                        "compensation_amount": 0,
                        "others_amount": 0,
                    },
                }
            }
        return None

    import app.services.dashboard_unit_economics_service as module

    original = module.cache_get_json
    module.cache_get_json = fake_cache_get_json
    try:
        report = await service.build_report(
            db=None,
            user_id=1,
            stores=[{"id": 1, "name": "Веня", "client_id": "1034362"}],
            store_id=1,
            limit=20,
        )
    finally:
        module.cache_get_json = original

    row = report["rows"][0]
    assert row["current_return_rate"] == 0.1
    assert row["current_return_reserve"] == 6.0
    assert row["current_allocated_services"] == -12.0
    assert row["current_allocated_compensation"] == -8.0
    assert row["current_allocated_other"] == -5.0
    assert row["current_profit_before_tax"] == 149.0
    assert row["current_estimated_net_profit"] == 126.65


async def test_build_report_falls_back_to_orders_when_realization_offer_ids_do_not_match_store():
    service = FakeDashboardUnitEconomicsService(
        costs={
            (3, "FUT1чернM"): 300.0,
        },
        sales_snapshots={},
        finance_snapshots={},
    )

    async def fake_cache_get_json(key: str):
        if "ozon-report-snapshot:1034362:postings" in key:
            return {
                "preview": {
                    "summary": {
                        "total_revenue": 1000,
                        "offer_stats": [
                            {"offer_id": "FUT1чернM", "title": "Футболка", "units": 2, "revenue": 1000},
                        ],
                    }
                }
            }
        if "ozon-finance-snapshot:1034362" in key:
            return {
                "realization_closed_month": {
                    "available": True,
                    "period": "2026-02",
                    "items": [
                        {
                            "offer_id": "offer-a",
                            "title": "Чужой товар",
                            "sold_units": 2,
                            "returned_units": 0,
                            "net_units": 2,
                            "net_amount": 800,
                            "net_total": 620,
                            "net_fee": 0,
                            "net_incentives": 0,
                        }
                    ],
                },
                "transactions_recent": {
                    "available": True,
                    "totals": {
                        "sale_commission": -100,
                        "services_amount": -50,
                        "processing_and_delivery": -50,
                        "refunds_and_cancellations": 0,
                        "compensation_amount": 0,
                        "others_amount": 0,
                    },
                },
            }
        return None

    import app.services.dashboard_unit_economics_service as module

    original = module.cache_get_json
    module.cache_get_json = fake_cache_get_json
    try:
        report = await service.build_report(
            db=None,
            user_id=3,
            stores=[{"id": 3, "name": "Чуви", "client_id": "1034362"}],
            store_id=3,
            limit=20,
        )
    finally:
        module.cache_get_json = original

    assert report["rows_total"] == 1
    assert report["rows"][0]["offer_id"] == "FUT1чернM"
    assert report["rows"][0]["basis"] == "orders_recent"
