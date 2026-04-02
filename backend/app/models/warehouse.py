# backend/app/models/warehouse.py
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base


class Warehouse(Base):
    __tablename__ = "warehouses"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=True)  # NULL для shared mode
    name = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Связи
    user = relationship("User", back_populates="warehouses")
    store = relationship("Store", back_populates="warehouse")
    stocks = relationship("WarehouseStock", back_populates="warehouse", cascade="all, delete-orphan")
    transactions = relationship("InventoryTransaction", back_populates="warehouse", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('user_id', 'store_id', name='uix_user_store_warehouse'),
    )


class WarehouseStock(Base):
    __tablename__ = "warehouse_stock"

    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"), nullable=False)
    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=False)
    unpacked_quantity = Column(Integer, default=0)
    packed_quantity = Column(Integer, default=0)
    reserved_quantity = Column(Integer, default=0)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    warehouse = relationship("Warehouse", back_populates="stocks")
    variant = relationship("Variant", back_populates="warehouse_stocks")

    __table_args__ = (
        UniqueConstraint('warehouse_id', 'variant_id', name='uix_warehouse_variant'),
    )