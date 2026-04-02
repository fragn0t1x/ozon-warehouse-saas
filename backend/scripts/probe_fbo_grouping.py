from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


BASE_URL = "https://api-seller.ozon.ru"

ATTR_ID_COLOR = 10097
ATTR_ID_COLOR_ALT = 10096
ATTR_ID_SIZE = 9533
ATTR_ID_PACK_SIZE = 9662

LETTER_SIZE_ORDER = {
    "xs": 1,
    "s": 2,
    "m": 3,
    "l": 4,
    "xl": 5,
    "xxl": 6,
    "xxxl": 7,
    "2xl": 6,
    "3xl": 7,
    "4xl": 8,
    "5xl": 9,
}

CHARACTERISTIC_PATTERNS = (
    r"ч[её]рн",
    r"бел",
    r"син",
    r"голуб",
    r"красн",
    r"зелен",
    r"ж[её]лт",
    r"сер",
    r"бордов",
    r"бирюз",
    r"хаки",
    r"разноцвет",
    r"розов",
    r"фиолет",
    r"оранж",
    r"коричн",
    r"беж",
    r"молоч",
    r"мужск",
    r"женск",
    r"унисекс",
    r"темно",
    r"т[её]мно",
    r"светло",
    r"упак",
    r"комплект",
    r"набор",
    r"пар",
    r"шт",
    r"штук",
    r"размер",
)


@dataclass
class StoreCredentials:
    client_id: str
    api_key: str


class OzonProbeClient:
    def __init__(self, client_id: str, api_key: str, *, verify_ssl: bool = False):
        self.client_id = str(client_id)
        self.api_key = api_key
        self.context = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        request = urllib.request.Request(
            BASE_URL + path,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Client-Id": self.client_id,
                "Api-Key": self.api_key,
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=60, context=self.context) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{path} -> HTTP {exc.code}: {body}") from exc

    def get_products_page(self, *, last_id: str | None = None, limit: int = 1000) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "filter": {"visibility": "ALL"},
            "limit": limit,
            "sort_dir": "ASC",
        }
        if last_id:
            payload["last_id"] = last_id
        return self._post("/v3/product/list", payload)

    def get_product_attributes(self, product_ids: list[int]) -> list[dict[str, Any]]:
        if not product_ids:
            return []
        payload = {
            "filter": {"product_id": product_ids},
            "limit": len(product_ids),
        }
        response = self._post("/v4/product/info/attributes", payload)
        return response.get("result", [])


def normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def tokenize_title(value: str) -> list[str]:
    return [token for token in re.split(r"[\s,()/+\\-]+", normalize_spaces(value)) if token]


def is_characteristic_token(token: str) -> bool:
    lowered = token.lower()
    if not lowered:
        return False
    if re.fullmatch(r"\d+", lowered):
        return True
    if re.fullmatch(r"\d+[xх]\d+", lowered):
        return True
    if re.fullmatch(r"\d+xl", lowered):
        return True
    if lowered in LETTER_SIZE_ORDER:
        return True
    if re.fullmatch(r"\d+\s*-\s*\d+", lowered):
        return True
    return any(re.search(pattern, lowered) for pattern in CHARACTERISTIC_PATTERNS)


def extract_base_product_name(name: str) -> str:
    tokens = tokenize_title(name)
    while tokens and is_characteristic_token(tokens[-1]):
        tokens.pop()
    while tokens and is_characteristic_token(tokens[0]):
        tokens.pop(0)
    return " ".join(tokens) if tokens else normalize_spaces(name) or "Без названия"


def remove_gender_tokens(value: str) -> str:
    text = re.sub(r"\b(мужск(?:ие|ая|ой)?|женск(?:ие|ая|ой)?|унисекс)\b", " ", value, flags=re.IGNORECASE)
    return normalize_spaces(text)


def normalize_color(value: str) -> str:
    lowered = (value or "").lower()
    if ("темно" in lowered or "тёмно" in lowered) and "син" in lowered:
        return "темно-синий"
    if "черн" in lowered or "чёрн" in lowered:
        return "черный"
    if "бел" in lowered:
        return "белый"
    if "син" in lowered:
        return "синий"
    if "красн" in lowered:
        return "красный"
    if "зелен" in lowered:
        return "зеленый"
    if "желт" in lowered:
        return "желтый"
    if "сер" in lowered:
        return "серый"
    if "хаки" in lowered:
        return "хаки"
    return value or "Без цвета"


def extract_family_signature(name: str, offer_id: str, base_name: str) -> str:
    combined = " ".join([name or "", offer_id or "", base_name or ""]).lower()
    parts: list[str] = []

    if "футбол" in combined:
        parts.append("футболка")
    elif "носк" in combined:
        parts.append("носки")

    for label, pattern in (
        ("премиум", r"премиум"),
        ("тонкая", r"тонк"),
        ("летняя", r"летн"),
        ("короткие", r"корот"),
        ("высокие", r"выс"),
        ("рубчик", r"рубч"),
        ("шерстяные", r"шерст|кашем"),
    ):
        if re.search(pattern, combined):
            parts.append(label)

    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if part not in seen:
            deduped.append(part)
            seen.add(part)

    if deduped:
        return " ".join(deduped)

    return remove_gender_tokens(base_name or name or "").lower() or "без названия"


def extract_partition_signature(signature: str) -> str:
    lowered = normalize_spaces((signature or "").lower())
    parts = lowered.split()
    if not parts:
        return "без названия"

    head = parts[0]
    tail = parts[1:]
    strong_splitters = {
        "высокие",
        "короткие",
        "рубчик",
        "шерстяные",
    }
    kept = [token for token in tail if token in strong_splitters]
    return " ".join([head, *kept]).strip() or lowered


def signature_priority(signature: str) -> tuple[int, int, str]:
    lowered = normalize_spaces((signature or "").lower())
    score = 0
    if "премиум" in lowered:
        score += 30
    if "тонкая" in lowered or "летняя" in lowered:
        score += 10
    if "рубчик" in lowered:
        score += 15
    if "шерстяные" in lowered:
        score += 15
    if "высокие" in lowered or "короткие" in lowered:
        score += 20
    return (score, len(lowered), lowered)


def display_group_name(signature: str, names: list[str]) -> str:
    mapping = {
        "носки": "Носки",
        "носки высокие": "Носки высокие",
        "носки короткие": "Носки короткие",
        "носки короткие рубчик": "Носки короткие рубчик",
        "носки рубчик короткие": "Носки короткие рубчик",
        "носки шерстяные": "Носки шерстяные",
        "футболка премиум": "Футболка ПРЕМИУМ",
        "футболка тонкая летняя": "Футболка ТОНКАЯ ЛЕТНЯЯ",
        "футболка летняя тонкая": "Футболка ТОНКАЯ ЛЕТНЯЯ",
        "футболка": "Футболка",
    }
    normalized = normalize_spaces(signature.lower())
    if normalized in mapping:
        return mapping[normalized]
    return longest_common_base_name(names)


def extract_pack_size_from_text(text: str) -> int:
    lowered = (text or "").lower()
    for pattern in (
        r"(\d+)\s*(?:пар|шт|штук|упак|набор)",
        r"(\d+)(?:пар|шт|штук|упак)",
        r"[xх](\d+)",
        r"(\d+)\s*(?:пары|пару)",
    ):
        match = re.search(pattern, lowered)
        if match:
            value = int(match.group(1))
            if 1 <= value <= 100:
                return value
    return 1


def extract_pack_size_from_attribute(attr_value: str) -> int:
    match = re.search(r"(\d+)", attr_value or "")
    if not match:
        return 1
    return int(match.group(1))


def get_size_order(size: str) -> tuple[int, int, str]:
    if not size:
        return (999, 0, "")
    lowered = size.lower()
    for key, value in LETTER_SIZE_ORDER.items():
        if key in lowered:
            return (1, value, size)
    if "-" in size:
        try:
            return (2, int(size.split("-")[0]), size)
        except ValueError:
            pass
    match = re.search(r"\d+", size)
    if match:
        return (2, int(match.group(0)), size)
    return (3, 0, size)


def longest_common_base_name(names: list[str]) -> str:
    tokenized = [tokenize_title(name) for name in names if name]
    if not tokenized:
        return "Без названия"

    prefix: list[str] = []
    for candidate in zip(*tokenized):
        values = {item.lower() for item in candidate}
        if len(values) != 1:
            break
        prefix.append(candidate[0])

    prefix_name = " ".join(prefix).strip()
    if prefix_name and len(prefix_name) >= 4:
        return prefix_name

    counter = Counter(extract_base_product_name(name) for name in names if name)
    return counter.most_common(1)[0][0]


def variant_fingerprint(variant: dict[str, Any]) -> str:
    return " | ".join(
        [
            (variant.get("color") or "Без цвета").lower(),
            (variant.get("size") or "Без размера").lower(),
            str(variant.get("pack_size") or 1),
        ]
    )


def extract_variant(attrs_payload: dict[str, Any]) -> dict[str, Any]:
    color = "Без цвета"
    size = "Без размера"
    pack_size = 1

    for attr in attrs_payload.get("attributes", []):
        attr_id = attr.get("id")
        values = attr.get("values") or []
        if not values:
            continue
        raw_value = str(values[0].get("value", "")).strip()
        if not raw_value:
            continue
        if attr_id in {ATTR_ID_COLOR, ATTR_ID_COLOR_ALT}:
            color = normalize_color(raw_value)
        elif attr_id == ATTR_ID_SIZE:
            size = raw_value
        elif attr_id == ATTR_ID_PACK_SIZE:
            pack_size = extract_pack_size_from_attribute(raw_value)

    name = attrs_payload.get("name", "")
    offer_id = attrs_payload.get("offer_id", "")
    if pack_size == 1 and offer_id:
        pack_size = extract_pack_size_from_text(offer_id)
    if pack_size == 1 and name:
        pack_size = extract_pack_size_from_text(name)
    base_name = extract_base_product_name(name)
    family_signature = extract_family_signature(name, offer_id, base_name)

    return {
        "sku": str(attrs_payload.get("sku") or ""),
        "offer_id": offer_id,
        "barcode": attrs_payload.get("barcode"),
        "name": name,
        "base_name": base_name,
        "family_signature": family_signature,
        "color": color,
        "size": size or "Без размера",
        "pack_size": pack_size or 1,
        "product_id": attrs_payload.get("id"),
        "model_id": ((attrs_payload.get("model_info") or {}).get("model_id")),
        "description_category_id": attrs_payload.get("description_category_id"),
        "type_id": attrs_payload.get("type_id"),
    }


def build_group_key(variant: dict[str, Any]) -> tuple[Any, ...]:
    model_id = variant.get("model_id")
    category_id = variant.get("description_category_id")
    type_id = variant.get("type_id")
    family_signature = variant.get("family_signature")
    if model_id:
        return ("model_family", category_id, type_id, model_id, extract_partition_signature(family_signature))
    return ("base_family", category_id, type_id, family_signature)


def build_groups(attributes_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for attrs_payload in attributes_payloads:
        variant = extract_variant(attrs_payload)
        grouped[build_group_key(variant)].append(variant)

    result: list[dict[str, Any]] = []
    for key, variants in grouped.items():
        variants_sorted = sorted(
            variants,
            key=lambda item: (
                normalize_color(item["color"]).lower(),
                get_size_order(item["size"]),
                item["pack_size"],
                item["offer_id"].lower(),
            ),
        )
        names = [item["name"] for item in variants_sorted if item.get("name")]
        signatures = Counter(item.get("family_signature") or "" for item in variants_sorted)
        dominant_signature = max(
            signatures.items(),
            key=lambda item: (item[1], signature_priority(item[0])),
        )[0]
        base_name = display_group_name(dominant_signature, names)
        unique_models = sorted({item.get("model_id") for item in variants_sorted if item.get("model_id")})
        unique_fingerprints = {variant_fingerprint(item) for item in variants_sorted}
        result.append(
            {
                "key": list(key),
                "group_source": key[0],
                "family_signature": dominant_signature,
                "warehouse_product_name": base_name,
                "description_category_id": variants_sorted[0].get("description_category_id"),
                "type_id": variants_sorted[0].get("type_id"),
                "model_ids": unique_models,
                "variants_count": len(variants_sorted),
                "fingerprints_count": len(unique_fingerprints),
                "variants": variants_sorted,
            }
        )

    return sorted(result, key=lambda item: item["warehouse_product_name"].lower())


def load_all_product_ids(client: OzonProbeClient) -> list[int]:
    last_id: str | None = None
    product_ids: list[int] = []
    while True:
        page = client.get_products_page(last_id=last_id)
        result = page.get("result", {})
        items = result.get("items", [])
        if not items:
            break
        product_ids.extend(item["product_id"] for item in items if item.get("product_id"))
        last_id = result.get("last_id")
        if not last_id:
            break
    return product_ids


def load_all_attributes(client: OzonProbeClient, product_ids: list[int], *, batch_size: int = 100) -> list[dict[str, Any]]:
    attributes_payloads: list[dict[str, Any]] = []
    for offset in range(0, len(product_ids), batch_size):
        batch = product_ids[offset:offset + batch_size]
        attributes_payloads.extend(client.get_product_attributes(batch))
        time.sleep(0.25)
    return attributes_payloads


def probe_store(store: StoreCredentials) -> dict[str, Any]:
    client = OzonProbeClient(store.client_id, store.api_key)
    product_ids = load_all_product_ids(client)
    attributes_payloads = load_all_attributes(client, product_ids)
    groups = build_groups(attributes_payloads)

    model_groups = sum(1 for group in groups if group["group_source"] == "model_family")
    fallback_groups = sum(1 for group in groups if group["group_source"] == "base_family")
    multi_variant_groups = sum(1 for group in groups if group["variants_count"] > 1)

    return {
        "client_id": store.client_id,
        "products_count": len(product_ids),
        "attributes_count": len(attributes_payloads),
        "groups_count": len(groups),
        "model_groups_count": model_groups,
        "fallback_groups_count": fallback_groups,
        "multi_variant_groups_count": multi_variant_groups,
        "groups": groups,
    }


def render_markdown_summary(report: dict[str, Any]) -> str:
    lines: list[str] = ["# FBO Grouping Probe", ""]
    for store in report["stores"]:
        lines.extend(
            [
                f"## Магазин {store['client_id']}",
                "",
                f"- Товаров: {store['products_count']}",
                f"- Атрибутных записей: {store['attributes_count']}",
                f"- Предлагаемых групп: {store['groups_count']}",
                f"- Групп по `model_id`: {store['model_groups_count']}",
                f"- Fallback-групп по названию: {store['fallback_groups_count']}",
                f"- Групп с несколькими вариациями: {store['multi_variant_groups_count']}",
                "",
            ]
        )
        for group in store["groups"][:40]:
            lines.append(
                f"- `{group['warehouse_product_name']}`: {group['variants_count']} вариаций, "
                f"source={group['group_source']}, model_ids={group['model_ids'] or '-'}"
            )
            for variant in group["variants"][:8]:
                lines.append(
                    f"  - `{variant['offer_id']}` | цвет={variant['color']} | размер={variant['size']} | упаковка={variant['pack_size']}"
                )
            if len(group["variants"]) > 8:
                lines.append(f"  - ... ещё {len(group['variants']) - 8}")
        lines.append("")
    return "\n".join(lines)


def parse_store_credentials(raw: list[dict[str, Any]]) -> list[StoreCredentials]:
    stores: list[StoreCredentials] = []
    for item in raw:
        client_id = item.get("Client-id") or item.get("client_id") or item.get("Client-Id")
        api_key = item.get("Api-key") or item.get("api_key") or item.get("Api-Key")
        if not client_id or not api_key:
            raise ValueError("Каждый магазин должен содержать Client-id и Api-key")
        stores.append(StoreCredentials(client_id=str(client_id), api_key=str(api_key)))
    return stores


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe Ozon FBO grouping across stores")
    parser.add_argument("--stores-json", help="JSON array with Client-id and Api-key")
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parents[1] / "tmp"),
        help="Directory for probe outputs",
    )
    args = parser.parse_args()

    stores_json = args.stores_json or os.getenv("OZON_STORES_JSON")
    if not stores_json:
        print("Передайте --stores-json или переменную окружения OZON_STORES_JSON", file=sys.stderr)
        return 1

    raw = json.loads(stores_json)
    stores = parse_store_credentials(raw)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "stores": [],
    }

    for store in stores:
        print(f"→ probing store {store.client_id}", file=sys.stderr)
        report["stores"].append(probe_store(store))

    json_path = output_dir / "fbo_grouping_probe.json"
    md_path = output_dir / "fbo_grouping_probe.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown_summary(report), encoding="utf-8")

    print(str(json_path))
    print(str(md_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
