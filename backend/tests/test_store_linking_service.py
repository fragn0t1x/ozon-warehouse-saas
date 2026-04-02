from app.services.store_linking_service import StoreLinkingService


def _attrs_payload(*, product_id: int, model_id: int, color: str, offer_id: str) -> dict:
    return {
        "id": product_id,
        "name": f"Футболка ПРЕМИУМ {color}",
        "offer_id": offer_id,
        "sku": str(product_id),
        "barcode": f"barcode-{product_id}",
        "description_category_id": 17031663,
        "type_id": 93072,
        "model_info": {"model_id": model_id},
        "images": [],
        "attributes": [
            {"id": 10097, "values": [{"value": color}]},
            {"id": 9533, "values": [{"value": "M"}]},
        ],
    }


def test_build_grouped_products_exposes_stable_group_keys_for_duplicate_base_names():
    service = StoreLinkingService(None)  # type: ignore[arg-type]

    grouped = service._build_grouped_products(
        [
            _attrs_payload(product_id=1, model_id=101, color="черный", offer_id="shirt-black-m"),
            _attrs_payload(product_id=2, model_id=202, color="белый", offer_id="shirt-white-m"),
        ]
    )

    assert len(grouped) == 2
    assert {group.base_name for group in grouped} == {"Футболка ПРЕМИУМ"}
    assert len({group.group_key for group in grouped}) == 2


class _FakeOzonClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get_products_list(self, *, last_id=None, visibility="ALL"):
        self.calls.append(visibility)
        if visibility == "ALL":
            return {"result": {"items": [{"product_id": 1}, {"product_id": 2}], "last_id": ""}}
        if visibility == "ARCHIVED":
            return {"result": {"items": [{"product_id": 2}, {"product_id": 3}], "last_id": ""}}
        return {"result": {"items": [], "last_id": ""}}


async def test_load_all_products_includes_archived_without_duplicates():
    service = StoreLinkingService(None)  # type: ignore[arg-type]
    client = _FakeOzonClient()

    products = await service._load_all_products(client)

    assert client.calls == ["ALL", "ARCHIVED"]
    assert [item["product_id"] for item in products] == [1, 2, 3]
