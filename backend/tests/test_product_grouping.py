from app.services.product_grouping import build_ozon_product_groups, extract_base_product_name


def test_extract_base_product_name_strips_color_and_size_suffixes():
    assert extract_base_product_name("Футболка ПРЕМИУМ Бирюзовая M") == "Футболка ПРЕМИУМ"
    assert extract_base_product_name("Футболка ПРЕМИУМ Светло-Серая XL") == "Футболка ПРЕМИУМ"


def test_extract_base_product_name_strips_packaging_suffixes():
    assert extract_base_product_name("Носки Белые набор 3 пар") == "Носки"
    assert extract_base_product_name("Носки в рубчик короткие Белые 41-47 Набор 10 пар") == "Носки в рубчик короткие"


def test_build_ozon_product_groups_keeps_premium_family_together_inside_one_model():
    payloads = [
        {
            "id": 1,
            "sku": 101,
            "offer_id": "FUT1белыйM",
            "name": "Футболка ТОНКАЯ Летняя Белая M",
            "barcode": "1",
            "description_category_id": 200000933,
            "type_id": 93244,
            "model_info": {"model_id": 56189659},
            "images": ["https://example.com/1.jpg"],
            "attributes": [
                {"id": 10097, "values": [{"value": "Белый"}]},
                {"id": 9533, "values": [{"value": "M"}]},
            ],
        },
        {
            "id": 2,
            "sku": 102,
            "offer_id": "FUT1бирюзM",
            "name": "Футболка ПРЕМИУМ Бирюзовая M",
            "barcode": "2",
            "description_category_id": 200000933,
            "type_id": 93244,
            "model_info": {"model_id": 56189659},
            "images": ["https://example.com/2.jpg"],
            "attributes": [
                {"id": 10097, "values": [{"value": "Бирюзовая"}]},
                {"id": 9533, "values": [{"value": "M"}]},
            ],
        },
    ]

    groups = build_ozon_product_groups(payloads)

    assert len(groups) == 1
    assert groups[0]["base_name"] == "Футболка ПРЕМИУМ"
    assert groups[0]["variants_count"] == 2


def test_build_ozon_product_groups_still_splits_strong_sock_families_inside_one_model():
    payloads = [
        {
            "id": 1,
            "sku": 101,
            "offer_id": "НосВыс/Бел36-41/5пар",
            "name": "Носки высокие Белые 36-41 набор 5 пар",
            "barcode": "1",
            "description_category_id": 200001517,
            "type_id": 93157,
            "model_info": {"model_id": 116557443},
            "images": [],
            "attributes": [
                {"id": 10097, "values": [{"value": "Белый"}]},
                {"id": 9533, "values": [{"value": "36-41"}]},
                {"id": 9662, "values": [{"value": "5 пар"}]},
            ],
        },
        {
            "id": 2,
            "sku": 102,
            "offer_id": "НосКорот/Бел36-41/5пар",
            "name": "Носки короткие Белые 36-41 набор 5 пар",
            "barcode": "2",
            "description_category_id": 200001517,
            "type_id": 93157,
            "model_info": {"model_id": 116557443},
            "images": [],
            "attributes": [
                {"id": 10097, "values": [{"value": "Белый"}]},
                {"id": 9533, "values": [{"value": "36-41"}]},
                {"id": 9662, "values": [{"value": "5 пар"}]},
            ],
        },
    ]

    groups = build_ozon_product_groups(payloads)

    assert len(groups) == 2
    assert {group["base_name"] for group in groups} == {"Носки высокие", "Носки короткие"}
