# backend/app/models/user_settings.py
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base


class UserSettings(Base):
    __tablename__ = "user_settings"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, unique=True)

    # Режим склада
    warehouse_mode = Column(String, default="shared")  # 'shared' или 'per_store'

    # ID общего склада (для shared mode)
    shared_warehouse_id = Column(Integer, ForeignKey("warehouses.id"), nullable=True)

    # Режим упаковки
    packing_mode = Column(String, default="simple")  # 'simple' или 'advanced'

    # Дата начала учета отправок
    shipments_start_date = Column(DateTime(timezone=True), nullable=True)
    shipments_accounting_enabled = Column(Boolean, default=False, nullable=False)
    shipments_accounting_enabled_at = Column(DateTime(timezone=True), nullable=True)

    # Периодичность фоновых синхронизаций OZON
    sync_products_interval_minutes = Column(Integer, default=360, nullable=False)
    sync_supplies_interval_minutes = Column(Integer, default=5, nullable=False)
    sync_stocks_interval_minutes = Column(Integer, default=20, nullable=False)
    sync_reports_interval_minutes = Column(Integer, default=180, nullable=False)
    sync_finance_interval_minutes = Column(Integer, default=360, nullable=False)

    # Telegram chat ID для уведомлений
    telegram_chat_id = Column(String, nullable=True)
    notification_timezone = Column(String, default="Europe/Moscow")
    today_supplies_time_local = Column(String, default="08:00")
    daily_report_time_local = Column(String, default="09:00")

    # Настройки уведомлений
    notify_today_supplies = Column(Boolean, default=True)
    notify_losses = Column(Boolean, default=True)
    notify_daily_report = Column(Boolean, default=True)
    notify_rejection = Column(Boolean, default=True)
    notify_acceptance_status = Column(Boolean, default=True)
    email_notifications_enabled = Column(Boolean, default=False, nullable=False)
    email_today_supplies = Column(Boolean, default=True, nullable=False)
    email_losses = Column(Boolean, default=True, nullable=False)
    email_daily_report = Column(Boolean, default=True, nullable=False)
    email_rejection = Column(Boolean, default=True, nullable=False)
    email_acceptance_status = Column(Boolean, default=True, nullable=False)
    web_push_notifications_enabled = Column(Boolean, default=False, nullable=False)

    # Режим учета расхождений
    discrepancy_mode = Column(String, default="loss")  # 'loss' или 'correction'

    # Флаг первого входа
    is_first_login = Column(Boolean, default=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Связи
    user = relationship("User", back_populates="settings")
    shared_warehouse = relationship("Warehouse", foreign_keys=[shared_warehouse_id])
