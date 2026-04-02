from app.services.telegram_service import TelegramService


def test_build_daily_report_text_includes_growth_and_returns():
    service = TelegramService()

    text = service.build_daily_report_text(
        {
            "report_date": "2026-03-24",
            "ordered_units_yesterday": 184,
            "orders_amount_yesterday": 126540,
            "returns_units_yesterday": 11,
            "returns_units_available": True,
            "returns_amount_yesterday": 7920,
            "active_supplies": 12,
            "today_supplies": 3,
            "total_available": 845,
            "total_reserved": 112,
            "top_gainers": [
                {"offer_id": "FUT1белыйM", "units_yesterday": 12, "delta_units": 7},
            ],
            "top_losers": [
                {"offer_id": "FUT1черныйL", "units_yesterday": 3, "delta_units": -6},
            ],
        },
        title="Ежедневный отчет",
        store_stats=[
            {
                "store_name": "Костя",
                "ordered_units_yesterday": 44,
                "active_supplies": 5,
                "today_supplies": 2,
            }
        ],
    )

    assert "Заказано: 184 шт. на 126 540" in text
    assert "Возвращено: 11 шт. на 7 920" in text
    assert "FUT1белыйM" in text
    assert "(+7)" in text
    assert "FUT1черныйL" in text
    assert "(-6)" in text
    assert "Костя" in text
