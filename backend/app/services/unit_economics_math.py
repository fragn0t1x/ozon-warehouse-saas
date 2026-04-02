from __future__ import annotations

from typing import Literal


VatMode = Literal["none", "usn_5", "usn_7", "osno_10", "osno_22"]
TaxMode = Literal["before_tax", "usn_income", "usn_income_expenses", "custom_profit"]

VAT_RATES: dict[VatMode, float] = {
    "none": 0.0,
    "usn_5": 0.05,
    "usn_7": 0.07,
    "osno_10": 0.10,
    "osno_22": 0.22,
}


def get_vat_rate(mode: str | None) -> float:
    normalized = (mode or "none").strip()
    return VAT_RATES.get(normalized, 0.0)


def revenue_net_of_vat(revenue_gross: float, vat_mode: str | None) -> float:
    vat_rate = get_vat_rate(vat_mode)
    if vat_rate <= 0:
        return float(revenue_gross or 0.0)
    return float(revenue_gross or 0.0) / (1 + vat_rate)


def calculate_tax_amount(
    *,
    revenue_net: float,
    profit_before_tax: float,
    tax_mode: str | None,
    tax_rate: float | None,
) -> float:
    mode = (tax_mode or "before_tax").strip()
    rate = max(float(tax_rate or 0.0), 0.0) / 100
    revenue_base = max(float(revenue_net or 0.0), 0.0)
    profit_base = max(float(profit_before_tax or 0.0), 0.0)

    if mode == "usn_income":
        return revenue_base * rate

    if mode == "usn_income_expenses":
        return max(profit_base * rate, revenue_base * 0.01)

    if mode == "custom_profit":
        return profit_base * rate

    return 0.0
