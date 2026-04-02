from datetime import date, datetime

from pydantic import BaseModel


class StoreEconomicsHistoryEntryResponse(BaseModel):
    id: int
    store_id: int
    effective_from: date
    vat_mode: str
    tax_mode: str
    tax_rate: float
    created_at: datetime


class VariantCostHistoryEntryResponse(BaseModel):
    id: int
    variant_id: int
    product_id: int
    product_name: str
    offer_id: str
    pack_size: int
    color: str | None = None
    size: str | None = None
    unit_cost: float | None = None
    effective_from: date
    created_at: datetime
    is_archived: bool
