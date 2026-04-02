from pydantic import BaseModel, Field
from typing import Optional

class IncomeRequest(BaseModel):
    store_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    variant_id: int
    quantity: int = Field(gt=0)

class IncomeBatchItem(BaseModel):
    variant_id: int
    quantity: int = Field(gt=0)

class IncomeBatchRequest(BaseModel):
    store_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    items: list[IncomeBatchItem]

class PackRequest(BaseModel):
    store_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    variant_id: int
    boxes: int = Field(gt=0)

class PackBatchItem(BaseModel):
    variant_id: int
    boxes: int = Field(gt=0)

class PackBatchRequest(BaseModel):
    store_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    items: list[PackBatchItem]

class ReserveRequest(BaseModel):
    store_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    variant_id: int
    quantity: int = Field(gt=0)
    supply_id: int
