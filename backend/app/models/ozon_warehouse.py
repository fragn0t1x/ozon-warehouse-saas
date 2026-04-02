from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base

class Cluster(Base):
    __tablename__ = "clusters"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    warehouses = relationship("OzonWarehouse", back_populates="cluster")

class OzonWarehouse(Base):
    __tablename__ = "ozon_warehouses"

    id = Column(Integer, primary_key=True)
    ozon_id = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    cluster_id = Column(Integer, ForeignKey("clusters.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    cluster = relationship("Cluster", back_populates="warehouses")
    stocks = relationship("OzonStock", back_populates="warehouse", cascade="all, delete-orphan")

class OzonStock(Base):
    __tablename__ = "ozon_stocks"

    __table_args__ = (
        UniqueConstraint('variant_id', 'warehouse_id', name='uix_variant_warehouse'),
    )

    id = Column(Integer, primary_key=True)
    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=False)
    warehouse_id = Column(Integer, ForeignKey("ozon_warehouses.id"), nullable=False)

    available_to_sell = Column(Integer, default=0)
    in_supply = Column(Integer, default=0)
    requested_to_supply = Column(Integer, default=0)
    in_transit = Column(Integer, default=0)
    returning = Column(Integer, default=0)

    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    variant = relationship("Variant", back_populates="ozon_stocks")
    warehouse = relationship("OzonWarehouse", back_populates="stocks")
