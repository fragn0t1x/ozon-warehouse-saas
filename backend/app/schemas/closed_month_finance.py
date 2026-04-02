from datetime import date, datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict


class ClosedMonthOfferFinanceResponse(BaseModel):
    id: int
    store_month_finance_id: int
    store_id: int
    month: str
    offer_id: str
    title: Optional[str] = None
    basis: str
    sold_units: int
    sold_amount: float
    returned_units: int
    returned_amount: float
    net_units: int
    revenue_amount: float
    revenue_net_of_vat: float
    ozon_commission: float
    ozon_logistics: float
    ozon_services: float
    ozon_acquiring: float
    ozon_other_expenses: float
    ozon_incentives: float
    ozon_adjustments_net: float
    unit_cost: Optional[float] = None
    cogs: Optional[float] = None
    gross_profit: Optional[float] = None
    profit_before_tax: Optional[float] = None
    tax_amount: Optional[float] = None
    net_profit: Optional[float] = None
    margin_ratio: Optional[float] = None
    vat_mode_used: Optional[str] = None
    tax_mode_used: Optional[str] = None
    tax_rate_used: Optional[float] = None
    tax_effective_from_used: Optional[date] = None
    cost_effective_from_used: Optional[date] = None
    has_cost: bool

    model_config = ConfigDict(from_attributes=True)


class ClosedMonthFinanceResponse(BaseModel):
    id: int
    store_id: int
    month: str
    status: str
    is_final: bool
    is_locked: bool
    realization_available: bool
    coverage_ratio: float
    sold_units: int
    sold_amount: float
    returned_units: int
    returned_amount: float
    revenue_amount: float
    revenue_net_of_vat: float
    cogs: float
    gross_profit: float
    ozon_commission: float
    ozon_logistics: float
    ozon_services: float
    ozon_acquiring: float
    ozon_other_expenses: float
    ozon_incentives: float
    ozon_compensation: float
    ozon_decompensation: float
    ozon_adjustments_net: float
    profit_before_tax: float
    tax_amount: float
    net_profit: float
    vat_mode_used: Optional[str] = None
    tax_mode_used: Optional[str] = None
    tax_rate_used: Optional[float] = None
    tax_effective_from_used: Optional[date] = None
    cost_basis: Optional[str] = None
    cost_snapshot_date: Optional[date] = None
    generated_at: Optional[datetime] = None
    checked_at: Optional[datetime] = None
    source_payload: Optional[dict[str, Any]] = None

    model_config = ConfigDict(from_attributes=True)


class ClosedMonthFinanceDetailResponse(BaseModel):
    month: ClosedMonthFinanceResponse
    offers: list[ClosedMonthOfferFinanceResponse]


class ClosedMonthSyncResponse(BaseModel):
    status: str
    store_id: int
    months_requested: int
    start_month: Optional[str] = None
    end_month: Optional[str] = None
    task_queued: bool = True
