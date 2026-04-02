from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class StoreMonthFinance(Base):
    __tablename__ = "store_month_finance"
    __table_args__ = (
        UniqueConstraint("store_id", "month", name="uq_store_month_finance_store_month"),
    )

    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False, index=True)
    month = Column(String(7), nullable=False, index=True)
    status = Column(String, nullable=False, default="pending", server_default="pending")
    is_final = Column(Boolean, nullable=False, default=False, server_default="false")
    is_locked = Column(Boolean, nullable=False, default=False, server_default="false")
    realization_available = Column(Boolean, nullable=False, default=False, server_default="false")
    coverage_ratio = Column(Float, nullable=False, default=0.0, server_default="0")
    sold_units = Column(Integer, nullable=False, default=0, server_default="0")
    sold_amount = Column(Float, nullable=False, default=0.0, server_default="0")
    returned_units = Column(Integer, nullable=False, default=0, server_default="0")
    returned_amount = Column(Float, nullable=False, default=0.0, server_default="0")
    revenue_amount = Column(Float, nullable=False, default=0.0, server_default="0")
    revenue_net_of_vat = Column(Float, nullable=False, default=0.0, server_default="0")
    cogs = Column(Float, nullable=False, default=0.0, server_default="0")
    gross_profit = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_commission = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_logistics = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_services = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_acquiring = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_other_expenses = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_incentives = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_compensation = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_decompensation = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_adjustments_net = Column(Float, nullable=False, default=0.0, server_default="0")
    profit_before_tax = Column(Float, nullable=False, default=0.0, server_default="0")
    tax_amount = Column(Float, nullable=False, default=0.0, server_default="0")
    net_profit = Column(Float, nullable=False, default=0.0, server_default="0")
    vat_mode_used = Column(String, nullable=True)
    tax_mode_used = Column(String, nullable=True)
    tax_rate_used = Column(Float, nullable=True)
    tax_effective_from_used = Column(Date, nullable=True)
    cost_basis = Column(String, nullable=True)
    cost_snapshot_date = Column(Date, nullable=True)
    generated_at = Column(DateTime(timezone=True), nullable=True)
    checked_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    source_payload = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=True, onupdate=func.now())

    store = relationship("Store", back_populates="month_finance")
    offer_rows = relationship(
        "StoreMonthOfferFinance",
        back_populates="month_finance",
        cascade="all, delete-orphan",
    )


class StoreMonthOfferFinance(Base):
    __tablename__ = "store_month_offer_finance"
    __table_args__ = (
        UniqueConstraint(
            "store_month_finance_id",
            "offer_id",
            name="uq_store_month_offer_finance_month_offer",
        ),
    )

    id = Column(Integer, primary_key=True)
    store_month_finance_id = Column(
        Integer,
        ForeignKey("store_month_finance.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False, index=True)
    month = Column(String(7), nullable=False, index=True)
    offer_id = Column(String, nullable=False, index=True)
    title = Column(String, nullable=True)
    basis = Column(String, nullable=False, default="realization_closed_month", server_default="realization_closed_month")
    sold_units = Column(Integer, nullable=False, default=0, server_default="0")
    sold_amount = Column(Float, nullable=False, default=0.0, server_default="0")
    returned_units = Column(Integer, nullable=False, default=0, server_default="0")
    returned_amount = Column(Float, nullable=False, default=0.0, server_default="0")
    net_units = Column(Integer, nullable=False, default=0, server_default="0")
    revenue_amount = Column(Float, nullable=False, default=0.0, server_default="0")
    revenue_net_of_vat = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_commission = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_logistics = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_services = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_acquiring = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_other_expenses = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_incentives = Column(Float, nullable=False, default=0.0, server_default="0")
    ozon_adjustments_net = Column(Float, nullable=False, default=0.0, server_default="0")
    unit_cost = Column(Float, nullable=True)
    cogs = Column(Float, nullable=True)
    gross_profit = Column(Float, nullable=True)
    profit_before_tax = Column(Float, nullable=True)
    tax_amount = Column(Float, nullable=True)
    net_profit = Column(Float, nullable=True)
    margin_ratio = Column(Float, nullable=True)
    vat_mode_used = Column(String, nullable=True)
    tax_mode_used = Column(String, nullable=True)
    tax_rate_used = Column(Float, nullable=True)
    tax_effective_from_used = Column(Date, nullable=True)
    cost_effective_from_used = Column(Date, nullable=True)
    has_cost = Column(Boolean, nullable=False, default=False, server_default="false")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=True, onupdate=func.now())

    month_finance = relationship("StoreMonthFinance", back_populates="offer_rows")
    store = relationship("Store")
