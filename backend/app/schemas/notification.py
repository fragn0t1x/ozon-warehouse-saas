from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class UserNotificationResponse(BaseModel):
    id: int
    kind: str
    title: str
    body: str
    action_url: Optional[str] = None
    severity: str
    is_important: bool
    read_at: Optional[datetime] = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class NotificationListResponse(BaseModel):
    items: list[UserNotificationResponse]
    unread_count: int


class NotificationUnreadCountResponse(BaseModel):
    unread_count: int


class WebPushSubscriptionRequest(BaseModel):
    endpoint: str
    p256dh_key: str
    auth_key: str
    user_agent: str | None = None


class WebPushUnsubscribeRequest(BaseModel):
    endpoint: str | None = None


class WebPushStatusResponse(BaseModel):
    configured: bool
    library_available: bool
    enabled: bool
    subscription_count: int
    public_key: str | None = None
    message: str | None = None


class WebPushTestResponse(BaseModel):
    sent_count: int
    message: str
