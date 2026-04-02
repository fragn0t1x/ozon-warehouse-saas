from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class SupplyNotificationEvent(Base):
    __tablename__ = "supply_notification_events"

    __table_args__ = (
        UniqueConstraint("dedupe_key", name="uq_supply_notification_events_dedupe_key"),
    )

    id = Column(Integer, primary_key=True)
    supply_id = Column(Integer, ForeignKey("supplies.id", ondelete="CASCADE"), nullable=False, index=True)
    event_type = Column(String, nullable=False, index=True)
    dedupe_key = Column(String, nullable=False)
    order_number = Column(String, nullable=False)
    store_id = Column(Integer, nullable=False, index=True)
    store_name = Column(String, nullable=False)
    user_email = Column(String, nullable=True)
    status_before = Column(String, nullable=True)
    status_after = Column(String, nullable=True)
    timeslot_from = Column(DateTime, nullable=True)
    timeslot_to = Column(DateTime, nullable=True)
    old_timeslot_from = Column(DateTime, nullable=True)
    old_timeslot_to = Column(DateTime, nullable=True)
    telegram_sent_at = Column(DateTime(timezone=True), nullable=True, index=True)
    last_error = Column(String, nullable=True)
    attempts = Column(Integer, nullable=False, server_default="0")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), index=True)

    supply = relationship("Supply", backref="notification_events")

    def __repr__(self) -> str:
        return (
            f"<SupplyNotificationEvent supply_id={self.supply_id} "
            f"type={self.event_type} sent_at={self.telegram_sent_at}>"
        )
