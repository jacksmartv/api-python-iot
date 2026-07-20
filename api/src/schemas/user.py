"""
Pydantic schemas for users and authentication.
"""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, EmailStr

from ..models.user import UserRole


class UserBase(BaseModel):
    email: EmailStr
    name: str
    role: UserRole = UserRole.VIEWER


class UserCreate(UserBase):
    password: str


class UserUpdate(BaseModel):
    email: EmailStr | None = None
    name: str | None = None
    role: UserRole | None = None
    is_active: bool | None = None


class UserResponse(UserBase):
    id: UUID
    is_active: bool
    created_at: datetime
    last_login: datetime | None = None

    class Config:
        from_attributes = True


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class TokenPayload(BaseModel):
    sub: str  # user_id
    email: str
    role: str
    exp: datetime
