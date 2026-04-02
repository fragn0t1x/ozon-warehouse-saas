# backend/app/models/user.py (обновленная версия)
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True, nullable=False, index=True)
    password_hash = Column(String, nullable=False)
    is_admin = Column(Boolean, default=False)
    role = Column(String, nullable=False, default="owner", server_default="owner")
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Связи
    owner = relationship("User", remote_side=[id], back_populates="members")
    members = relationship("User", back_populates="owner")
    stores = relationship("Store", back_populates="user", cascade="all, delete-orphan")
    warehouses = relationship("Warehouse", back_populates="user", cascade="all, delete-orphan")
    warehouse_products = relationship("WarehouseProduct", back_populates="user", cascade="all, delete-orphan")
    settings = relationship("UserSettings", back_populates="user", uselist=False, cascade="all, delete-orphan")
    notifications = relationship("UserNotification", back_populates="user", cascade="all, delete-orphan")
    web_push_subscriptions = relationship("WebPushSubscription", back_populates="user", cascade="all, delete-orphan")

    @property
    def cabinet_owner_id(self) -> int:
        return self.owner_user_id or self.id

    @property
    def can_manage_business_settings(self) -> bool:
        return bool(self.is_admin) or (self.role or "owner") == "owner"

    def __repr__(self):
        return f"<User {self.email}>"
