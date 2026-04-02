from sqlalchemy import Column, Date, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class StoreEconomicsHistory(Base):
    __tablename__ = "store_economics_history"

    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False, index=True)
    effective_from = Column(Date, nullable=False, index=True)
    vat_mode = Column(String, nullable=False)
    tax_mode = Column(String, nullable=False)
    tax_rate = Column(Float, nullable=False)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    store = relationship("Store", back_populates="economics_history")

