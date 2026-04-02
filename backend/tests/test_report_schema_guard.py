from app.services.ozon.report_schema_guard import missing_required_column_groups


def test_missing_required_column_groups_accepts_reordered_headers():
    headers = [
        "Название товара",
        "Склад отгрузки",
        "SKU",
        "Артикул",
        "Дата заказа",
        "Количество",
    ]
    required_groups = (
        ("offer id", "offer_id", "артикул"),
        ("name", "название товара"),
        ("processed_at", "дата заказа"),
        ("sku",),
        ("склад отгрузки", "shipment warehouse"),
    )

    assert missing_required_column_groups(headers, required_groups) == []


def test_missing_required_column_groups_reports_missing_alias_group():
    headers = [
        "Название товара",
        "Количество",
    ]
    required_groups = (
        ("offer id", "offer_id", "артикул"),
        ("name", "название товара"),
    )

    assert missing_required_column_groups(headers, required_groups) == [["offer id", "offer_id", "артикул"]]
