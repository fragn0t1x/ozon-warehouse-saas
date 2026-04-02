from __future__ import annotations

from typing import Iterable, Sequence

from app.services.admin_notifications import notify_ozon_schema_change


def normalize_report_header(value: str | None) -> str:
    return " ".join(str(value or "").strip().lower().replace("_", " ").split())


def normalize_report_headers(headers: Iterable[str | None]) -> list[str]:
    seen: list[str] = []
    for header in headers:
        normalized = normalize_report_header(header)
        if normalized:
            seen.append(normalized)
    return seen


def missing_required_column_groups(
    headers: Iterable[str | None],
    required_groups: Sequence[Sequence[str]],
) -> list[list[str]]:
    normalized_headers = set(normalize_report_headers(headers))
    missing_groups: list[list[str]] = []
    for group in required_groups:
        normalized_group = [normalize_report_header(alias) for alias in group if normalize_report_header(alias)]
        if not normalized_group:
            continue
        if normalized_headers.intersection(normalized_group):
            continue
        missing_groups.append(list(group))
    return missing_groups


def render_required_groups(required_groups: Sequence[Sequence[str]]) -> list[str]:
    rendered: list[str] = []
    for group in required_groups:
        cleaned = [str(alias).strip() for alias in group if str(alias).strip()]
        if cleaned:
            rendered.append(" | ".join(cleaned))
    return rendered


async def notify_ozon_report_columns_changed(
    *,
    endpoint: str,
    client_id: str,
    report_name: str,
    required_groups: Sequence[Sequence[str]],
    actual_headers: Iterable[str | None],
    payload: dict | None = None,
) -> bool:
    return await notify_ozon_schema_change(
        endpoint,
        client_id,
        expected_key=f"{report_name}: {', '.join(render_required_groups(required_groups))}",
        actual_keys=sorted(normalize_report_headers(actual_headers)),
        payload=payload,
    )
