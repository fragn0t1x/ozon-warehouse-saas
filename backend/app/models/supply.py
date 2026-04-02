from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Date, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base

class Supply(Base):
    __tablename__ = "supplies"
    __table_args__ = (
        UniqueConstraint("store_id", "ozon_order_id", name="uq_supplies_store_ozon_order_id"),
    )

    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    ozon_order_id = Column(String, nullable=False)
    order_number = Column(String, nullable=False)
    status = Column(String, nullable=False)
    dropoff_warehouse_id = Column(Integer, ForeignKey("ozon_warehouses.id"), nullable=True)
    storage_warehouse_id = Column(Integer, ForeignKey("ozon_warehouses.id"), nullable=True)
    timeslot_from = Column(DateTime, nullable=True)
    timeslot_to = Column(DateTime, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime, nullable=True)
    acceptance_at_storage_at = Column(DateTime, nullable=True)
    eta_date = Column(Date, nullable=True)
    reserved_at = Column(DateTime(timezone=True), nullable=True)

    store = relationship("Store", back_populates="supplies")
    dropoff = relationship("OzonWarehouse", foreign_keys=[dropoff_warehouse_id])
    storage = relationship("OzonWarehouse", foreign_keys=[storage_warehouse_id])
    items = relationship("SupplyItem", back_populates="supply", cascade="all, delete-orphan")

class SupplyItem(Base):
    __tablename__ = "supply_items"

    id = Column(Integer, primary_key=True)
    supply_id = Column(Integer, ForeignKey("supplies.id"), nullable=False)
    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=False)
    quantity = Column(Integer, nullable=False)
    accepted_quantity = Column(Integer, nullable=True)

    supply = relationship("Supply", back_populates="items")
    variant = relationship("Variant", back_populates="supply_items")
