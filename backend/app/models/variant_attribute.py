from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from app.database import Base

class VariantAttribute(Base):
    __tablename__ = "variant_attributes"

    __table_args__ = (
        UniqueConstraint('variant_id', 'name', name='uix_variant_attribute'),
    )

    id = Column(Integer, primary_key=True)
    variant_id = Column(Integer, ForeignKey("variants.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    value = Column(String, nullable=False)

    variant = relationship("Variant", back_populates="attributes")

    def __repr__(self):
        return f"<VariantAttribute {self.name}={self.value}>"