from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, JSON, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base


class BaseProduct(Base):
    __tablename__ = "base_products"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    category = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    base_variants = relationship("BaseVariant", back_populates="base_product", cascade="all, delete-orphan")
    product_matches = relationship("ProductMatch", back_populates="base_product", cascade="all, delete-orphan")


class BaseVariant(Base):
    __tablename__ = "base_variants"

    id = Column(Integer, primary_key=True)
    base_product_id = Column(Integer, ForeignKey("base_products.id"), nullable=False)
    sku = Column(String, nullable=True)
    pack_size = Column(Integer, default=1)
    attributes = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    base_product = relationship("BaseProduct", back_populates="base_variants")
    variant_matches = relationship("VariantMatch", back_populates="base_variant", cascade="all, delete-orphan")


class ProductMatch(Base):
    __tablename__ = "product_matches"

    id = Column(Integer, primary_key=True)
    base_product_id = Column(Integer, ForeignKey("base_products.id"), nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    match_type = Column(String, default="manual")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    base_product = relationship("BaseProduct", back_populates="product_matches")
    store = relationship("Store", back_populates="product_matches")
    product = relationship("Product")

    __table_args__ = (
        UniqueConstraint("store_id", "product_id", name="uix_store_product_match"),
    )


class VariantMatch(Base):
    __tablename__ = "variant_matches"

    id = Column(Integer, primary_key=True)
    base_variant_id = Column(Integer, ForeignKey("base_variants.id"), nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=False)
    attributes = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    base_variant = relationship("BaseVariant", back_populates="variant_matches")
    store = relationship("Store", back_populates="variant_matches")
    variant = relationship("Variant")

    __table_args__ = (
        UniqueConstraint("store_id", "variant_id", name="uix_store_variant_match"),
    )
