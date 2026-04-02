from types import SimpleNamespace

from app.api.matching_router import (
    _available_variant_stock,
    _product_match_score,
    _variant_signature_from_attributes,
)


def _variant(*, pack_size=1, attributes=None, stocks=None):
    return SimpleNamespace(
        pack_size=pack_size,
        attributes=[
            SimpleNamespace(name=name, value=value)
            for name, value in (attributes or {}).items()
        ],
        warehouse_stocks=stocks or [],
    )


def _stock(*, user_id, unpacked=0, packed=0, reserved=0):
    return SimpleNamespace(
        unpacked_quantity=unpacked,
        packed_quantity=packed,
        reserved_quantity=reserved,
        warehouse=SimpleNamespace(user_id=user_id),
    )


def test_variant_signature_normalizes_color_size_and_pack_size():
    signature = _variant_signature_from_attributes(
        6,
        {" Цвет ": "Черный ", "Размер": " 41-47 ", "Материал": "Хлопок"},
    )

    assert signature == (6, "черный", "41-47")


def test_available_variant_stock_uses_only_current_user_warehouses():
    variant = _variant(
        pack_size=3,
        stocks=[
            _stock(user_id=7, unpacked=4, packed=2, reserved=1),
            _stock(user_id=8, unpacked=100, packed=100, reserved=0),
        ],
    )

    assert _available_variant_stock(variant, user_id=7) == 9


def test_product_match_score_prefers_variant_overlap():
    product = SimpleNamespace(
        name="Носки черные 41-47",
        variants=[
            _variant(
                pack_size=6,
                attributes={"Цвет": "Черный", "Размер": "41-47"},
            )
        ],
    )
    matching_base = SimpleNamespace(
        name="Носки черные базовые",
        base_variants=[
            SimpleNamespace(
                pack_size=6,
                attributes={"Цвет": "черный", "Размер": "41-47"},
            )
        ],
    )
    different_base = SimpleNamespace(
        name="Футболка белая",
        base_variants=[
            SimpleNamespace(
                pack_size=1,
                attributes={"Цвет": "белый", "Размер": "L"},
            )
        ],
    )

    assert _product_match_score(product, matching_base) > 0.6
    assert _product_match_score(product, matching_base) > _product_match_score(product, different_base)
