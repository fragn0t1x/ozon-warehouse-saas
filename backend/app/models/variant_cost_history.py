from sqlalchemy import Column, Date, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class VariantCostHistory(Base):
    __tablename__ = "variant_cost_history"

    id = Column(Integer, primary_key=True)
    variant_id = Column(Integer, ForeignKey("variants.id", ondelete="CASCADE"), nullable=False, index=True)
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False, index=True)
    warehouse_product_id = Column(Integer, ForeignKey("warehouse_products.id", ondelete="SET NULL"), nullable=True, index=True)
    offer_id = Column(String, nullable=False, index=True)
    pack_size = Column(Integer, nullable=False, default=1, server_default="1")
    color = Column(String, nullable=True)
    size = Column(String, nullable=True)
    unit_cost = Column(Float, nullable=True)
    effective_from = Column(Date, nullable=False, index=True)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    variant = relationship("Variant", back_populates="cost_history")
    store = relationship("Store")

