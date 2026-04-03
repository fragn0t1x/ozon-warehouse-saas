from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing import Literal, Optional
from datetime import date, datetime

StoreEconomicsVatMode = Literal["none", "usn_5", "usn_7", "osno_10", "osno_22"]
StoreEconomicsTaxMode = Literal["before_tax", "usn_income", "usn_income_expenses", "custom_profit"]

class StoreBase(BaseModel):
    name: str
    client_id: str

class StoreCreate(StoreBase):
    api_key: str = Field(..., min_length=1, description="API ключ OZON")
    economics_vat_mode: StoreEconomicsVatMode = "none"
    economics_tax_mode: StoreEconomicsTaxMode = "usn_income_expenses"
    economics_tax_rate: float = Field(default=15.0, ge=0, le=100)
    product_links: list["StoreProductLinkDecision"] = Field(default_factory=list)

class StoreValidate(StoreCreate):
    pass


class StoreUpdate(StoreBase):
    api_key: Optional[str] = Field(None, min_length=1, description="Новый API ключ OZON")
    economics_vat_mode: StoreEconomicsVatMode = "none"
    economics_tax_mode: StoreEconomicsTaxMode = "usn_income_expenses"
    economics_tax_rate: float = Field(default=15.0, ge=0, le=100)
    economics_default_sale_price_gross: Optional[float] = Field(default=None, ge=0)
    economics_effective_from: Optional[date] = None

    @field_validator("api_key", mode="before")
    @classmethod
    def empty_api_key_to_none(cls, value: Optional[str]) -> Optional[str]:
        if isinstance(value, str) and not value.strip():
            return None
        return value


class StorePatch(BaseModel):
    name: Optional[str] = None
    client_id: Optional[str] = None
    api_key: Optional[str] = Field(None, min_length=1, description="Новый API ключ OZON")
    is_active: Optional[bool] = None
    economics_vat_mode: Optional[StoreEconomicsVatMode] = None
    economics_tax_mode: Optional[StoreEconomicsTaxMode] = None
    economics_tax_rate: Optional[float] = Field(default=None, ge=0, le=100)
    economics_default_sale_price_gross: Optional[float] = Field(default=None, ge=0)
    economics_effective_from: Optional[date] = None

    @field_validator("api_key", mode="before")
    @classmethod
    def empty_api_key_to_none(cls, value: Optional[str]) -> Optional[str]:
        if isinstance(value, str) and not value.strip():
            return None
        return value

class StoreResponse(StoreBase):
    id: int
    user_id: int
    is_active: bool
    created_at: datetime
    warehouse_id: Optional[int] = None
    bootstrap_state: Optional[str] = None
    economics_vat_mode: StoreEconomicsVatMode = "none"
    economics_tax_mode: StoreEconomicsTaxMode = "usn_income_expenses"
    economics_tax_rate: float = 15.0
    economics_default_sale_price_gross: Optional[float] = None
    economics_effective_from: Optional[date] = None

    model_config = ConfigDict(from_attributes=True)


class StoreProductLinkDecision(BaseModel):
    base_name: str
    group_key: Optional[str] = None
    offer_ids: list[str] = Field(default_factory=list)
    warehouse_product_id: Optional[int] = None
    warehouse_product_name: Optional[str] = None

    @field_validator("warehouse_product_name", mode="before")
    @classmethod
    def normalize_name(cls, value: Optional[str]) -> Optional[str]:
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @model_validator(mode="after")
    def validate_link_target(self):
        if self.warehouse_product_id is None and not self.warehouse_product_name:
            raise ValueError("Для каждой группы нужно выбрать существующий товар или указать новое название")
        return self


class StorePreviewVariant(BaseModel):
    offer_id: str
    pack_size: int
    color: str
    size: str


class StorePreviewSizeGroup(BaseModel):
    size: str
    variants: list[StorePreviewVariant]


class StorePreviewColorGroup(BaseModel):
    color: str
    sizes: list[StorePreviewSizeGroup]


class StorePreviewCandidate(BaseModel):
    id: int
    name: str
    score: int
    overlap_count: int
    overlap_total: int
    reasons: list[str] = Field(default_factory=list)


class StorePreviewProductGroup(BaseModel):
    group_key: str
    base_name: str
    product_name: str
    image_url: Optional[str] = None
    total_variants: int
    colors: list[StorePreviewColorGroup]
    match_status: str = "new"
    suggested_warehouse_product_id: Optional[int] = None
    candidates: list[StorePreviewCandidate] = Field(default_factory=list)
    match_explanation: Optional[str] = None


class WarehouseProductOption(BaseModel):
    id: int
    name: str


class StorePreviewMatchStatus(str):
    AUTO = "auto"
    CONFLICT = "conflict"
    NEW = "new"


class StoreImportPreviewResponse(BaseModel):
    grouped_products: list[StorePreviewProductGroup]
    available_warehouse_products: list[WarehouseProductOption]
