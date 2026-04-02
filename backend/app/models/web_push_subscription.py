from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class WebPushSubscription(Base):
    __tablename__ = "web_push_subscriptions"
    __table_args__ = (
        UniqueConstraint("endpoint", name="uq_web_push_subscription_endpoint"),
    )

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    endpoint = Column(String, nullable=False)
    p256dh_key = Column(String, nullable=False)
    auth_key = Column(String, nullable=False)
    user_agent = Column(String, nullable=True)
    last_seen_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_sent_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=True, onupdate=func.now())

    user = relationship("User", back_populates="web_push_subscriptions")
