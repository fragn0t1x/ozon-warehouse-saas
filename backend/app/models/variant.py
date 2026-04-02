from sqlalchemy import Boolean, Column, Integer, String, ForeignKey, DateTime, Float
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base

class Variant(Base):
    __tablename__ = "variants"

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    sku = Column(String, unique=True, nullable=False)
    offer_id = Column(String, nullable=False)
    barcode = Column(String, nullable=True)
    pack_size = Column(Integer, default=1)
    is_archived = Column(Boolean, nullable=False, default=False, server_default="false")
    unit_cost = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    product = relationship("Product", back_populates="variants")
    attributes = relationship(
        "VariantAttribute",
        back_populates="variant",
        cascade="all, delete-orphan",
        lazy="selectin"
    )
    warehouse_stocks = relationship("WarehouseStock", back_populates="variant", cascade="all, delete-orphan")
    supply_items = relationship("SupplyItem", back_populates="variant", cascade="all, delete-orphan")
    inventory_transactions = relationship("InventoryTransaction", back_populates="variant", cascade="all, delete-orphan")
    ozon_stocks = relationship("OzonStock", back_populates="variant", cascade="all, delete-orphan")
    cost_history = relationship("VariantCostHistory", back_populates="variant", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Variant {self.sku}>"
