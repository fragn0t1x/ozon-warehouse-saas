# backend/app/schemas/auth.py
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"

class TokenPayload(BaseModel):
    sub: Optional[str] = None
    exp: Optional[int] = None
    type: Optional[str] = None

class LoginRequest(BaseModel):
    email: str
    password: str = Field(..., min_length=6)

class RefreshRequest(BaseModel):
    refresh_token: Optional[str] = None

class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str = Field(..., min_length=6)

class UserCreate(BaseModel):
    email: str
    password: Optional[str] = Field(None, min_length=6)
    is_admin: bool = False


class TeamMemberCreate(BaseModel):
    email: str
    password: Optional[str] = Field(None, min_length=6)


class UserResponse(BaseModel):
    id: int
    email: str
    is_admin: bool
    role: str = "owner"
    owner_user_id: Optional[int] = None
    cabinet_owner_id: int
    can_manage_business_settings: bool = False
    is_active: bool
    created_at: Optional[datetime] = None  # Изменено с Optional[str] на Optional[datetime]

    class Config:
        from_attributes = True
        # Добавляем поддержку datetime
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class AdminUserCreateResponse(BaseModel):
    user: UserResponse
    generated_password: Optional[str] = None
