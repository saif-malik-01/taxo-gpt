from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime

class ProfileUpdate(BaseModel):
    dynamic_summary: Optional[str] = None
    preferences: Optional[dict] = None

class UserResponseAdmin(BaseModel):
    id: int
    full_name: Optional[str] = None
    email: str
    mobile_number: Optional[str] = None
    country: Optional[str] = None
    role: str
    max_sessions: int
    is_verified: bool
    created_at: datetime
    google_id: Optional[str] = None
    facebook_id: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)
