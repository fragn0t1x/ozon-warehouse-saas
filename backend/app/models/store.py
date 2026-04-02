from sqlalchemy import Column, Integer, String, ForeignKey, Boolean, DateTime, Float
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base

class Store(Base):
    __tablename__ = "stores"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    name = Column(String, nullable=False)
    client_id = Column(String, nullable=False)
    api_key_encrypted = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    economics_vat_mode = Column(String, nullable=False, default="none", server_default="none")
    economics_tax_mode = Column(String, nullable=False, default="usn_income_expenses", server_default="usn_income_expenses")
    economics_tax_rate = Column(Float, nullable=False, default=15.0, server_default="15")
    economics_default_sale_price_gross = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    user = relationship("User", back_populates="stores")
    products = relationship("Product", back_populates="store", cascade="all, delete-orphan")
    supplies = relationship("Supply", back_populates="store", cascade="all, delete-orphan")
    warehouse = relationship("Warehouse", back_populates="store", uselist=False, cascade="all, delete-orphan")
    product_matches = relationship("ProductMatch", back_populates="store", cascade="all, delete-orphan")
    variant_matches = relationship("VariantMatch", back_populates="store", cascade="all, delete-orphan")
    month_finance = relationship("StoreMonthFinance", back_populates="store", cascade="all, delete-orphan")
    economics_history = relationship("StoreEconomicsHistory", back_populates="store", cascade="all, delete-orphan")

    @property
    def warehouse_id(self):
        return self.warehouse.id if self.warehouse else None
