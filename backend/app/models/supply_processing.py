from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base

class SupplyProcessing(Base):
    __tablename__ = "supply_processing"

    __table_args__ = (
        UniqueConstraint('supply_id', 'processed_type', name='uix_supply_processed_type'),
    )

    id = Column(Integer, primary_key=True)
    supply_id = Column(Integer, ForeignKey("supplies.id", ondelete="CASCADE"), nullable=False)
    processed_type = Column(String, nullable=False)
    processed_at = Column(DateTime(timezone=True), server_default=func.now())

    supply = relationship("Supply", backref="processing_records")

    def __repr__(self):
        return f"<SupplyProcessing supply_id={self.supply_id} type={self.processed_type}>"