from sqlalchemy import Column, Integer, String, DateTime, UniqueConstraint
from sqlalchemy.sql import func
from app.database import Base

class CategoryPackAttribute(Base):
    __tablename__ = "category_pack_attributes"

    __table_args__ = (
        UniqueConstraint('category_id', 'store_id', name='uix_category_store'),
    )

    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, nullable=False)
    attribute_id = Column(Integer, nullable=False)
    attribute_name = Column(String, nullable=True)
    store_id = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())