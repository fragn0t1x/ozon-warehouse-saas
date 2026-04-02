from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from typing import Any


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
    """Возвращает базовое название товара без цвета, пола, упаковки и размера."""
    if not name:
        return "Без названия"

    cleaned = normalize_spaces(name)
    words = tokenize_title(cleaned)

    while words and is_characteristic_token(words[-1]):
        words.pop()

    while words and is_characteristic_token(words[0]):
        words.pop(0)

    if words:
        return " ".join(words)

    return cleaned


def remove_gender_tokens(value: str) -> str:
    text = re.sub(r"\b(мужск(?:ие|ая|ой)?|женск(?:ие|ая|ой)?|унисекс)\b", " ", value, flags=re.IGNORECASE)
    return normalize_spaces(text)


def normalize_color(color: str) -> str:
    color_lower = (color or "").lower()

    if ("темно" in color_lower or "тёмно" in color_lower) and "син" in color_lower:
        return "темно-синий"
    if ("светло" in color_lower or "светло-" in color_lower) and "син" in color_lower:
        return "светло-синий"
    if "черн" in color_lower or "чёрн" in color_lower:
        return "черный"
    if "бел" in color_lower:
        return "белый"
    if "син" in color_lower:
        return "синий"
    if "красн" in color_lower:
        return "красный"
    if "зелен" in color_lower:
        return "зеленый"
    if "желт" in color_lower:
        return "желтый"
    if "сер" in color_lower:
        return "серый"
    if "хаки" in color_lower:
        return "хаки"

    return color or "Без цвета"


def get_size_order(size: str) -> tuple[int, int, str]:
    if not size:
        return (999, 0, "")

    size_lower = size.lower()

    for key, value in LETTER_SIZE_ORDER.items():
        if key in size_lower:
            return (1, value, size)

    if "-" in size:
        try:
            first_num = int(size.split("-")[0])
            return (2, first_num, size)
        except Exception:
            pass

    match = re.search(r"\d+", size)
    if match:
        try:
            return (2, int(match.group()), size)
        except Exception:
            pass

    return (3, 0, size)


def extract_pack_size_from_text(text: str) -> int:
    if not text:
        return 1

    text_lower = text.lower()
    patterns = [
        r"(\d+)\s*(?:пар|шт|штук|упак|набор)",
        r"(\d+)(?:пар|шт|штук|упак)",
        r"[xх](\d+)",
        r"(\d+)\s*(?:пары|пару)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text_lower)
        if match:
            try:
                value = int(match.group(1))
                if 1 <= value <= 100:
                    return value
            except Exception:
                continue

    return 1


def extract_pack_size_from_attribute(attr_value: str) -> int:
    if not attr_value:
        return 1

    match = re.search(r"(\d+)", attr_value)
    if match:
        try:
            return int(match.group(1))
        except Exception:
            pass

    return 1


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


def variant_fingerprint(variant: dict[str, Any]) -> str:
    return " | ".join(
        [
            (variant.get("color") or "Без цвета").lower(),
            (variant.get("size") or "Без размера").lower(),
            str(variant.get("pack_size") or 1),
        ]
    )


def extract_grouping_variant(attrs_payload: dict[str, Any]) -> dict[str, Any]:
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
    images = attrs_payload.get("images", [])

    return {
        "product_id": attrs_payload.get("id"),
        "sku": str(attrs_payload.get("sku") or ""),
        "offer_id": offer_id,
        "barcode": attrs_payload.get("barcode"),
        "name": name,
        "base_name": base_name,
        "family_signature": family_signature,
        "partition_signature": extract_partition_signature(family_signature),
        "color": color,
        "size": size or "Без размера",
        "pack_size": pack_size or 1,
        "model_id": (attrs_payload.get("model_info") or {}).get("model_id"),
        "description_category_id": attrs_payload.get("description_category_id"),
        "type_id": attrs_payload.get("type_id"),
        "image_url": images[0] if images else None,
    }


def build_group_key(variant: dict[str, Any]) -> tuple[Any, ...]:
    model_id = variant.get("model_id")
    category_id = variant.get("description_category_id")
    type_id = variant.get("type_id")
    partition_signature = variant.get("partition_signature")
    family_signature = variant.get("family_signature")
    if model_id:
        return ("model_family", category_id, type_id, model_id, partition_signature)
    return ("base_family", category_id, type_id, family_signature)


def serialize_group_key(key: Any) -> str:
    return json.dumps(key, ensure_ascii=False, separators=(",", ":"))


def build_ozon_product_groups(attributes_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for attrs_payload in attributes_payloads:
        variant = extract_grouping_variant(attrs_payload)
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
        canonical_name = display_group_name(dominant_signature, names)
        unique_models = sorted({item.get("model_id") for item in variants_sorted if item.get("model_id")})
        unique_fingerprints = {variant_fingerprint(item) for item in variants_sorted}
        representative_name = display_group_name(dominant_signature, names)
        representative_image = next((item["image_url"] for item in variants_sorted if item.get("image_url")), None)

        result.append(
            {
                "key": list(key),
                "group_source": key[0],
                "base_name": canonical_name,
                "product_name": representative_name,
                "family_signature": dominant_signature,
                "description_category_id": variants_sorted[0].get("description_category_id"),
                "type_id": variants_sorted[0].get("type_id"),
                "model_ids": unique_models,
                "fingerprints_count": len(unique_fingerprints),
                "variants_count": len(variants_sorted),
                "image_url": representative_image,
                "variants": variants_sorted,
            }
        )

    return sorted(result, key=lambda item: item["base_name"].lower())
