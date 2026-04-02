from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    warehouse_product_id = Column(Integer, ForeignKey("warehouse_products.id"), nullable=True, index=True)
    name = Column(String, nullable=False)
    base_name = Column(String, nullable=True)
    image_url = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    store = relationship("Store", back_populates="products")
    warehouse_product = relationship("WarehouseProduct", back_populates="products")
    variants = relationship("Variant", back_populates="product", cascade="all, delete-orphan")
