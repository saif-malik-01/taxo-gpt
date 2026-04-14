from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime

class ProfileUpdate(BaseModel):
    full_name: Optional[str] = None
    mobile_number: Optional[str] = None
    state: Optional[str] = None
    gst_number: Optional[str] = None
    dynamic_summary: Optional[str] = None
    preferences: Optional[dict] = None

class UserResponseAdmin(BaseModel):
    id: int
    full_name: Optional[str] = None
    email: str
    mobile_number: Optional[str] = None
    state: Optional[str] = None
    gst_number: Optional[str] = None
    country: Optional[str] = None
    role: str
    max_sessions: int
    is_verified: bool
    created_at: datetime
    google_id: Optional[str] = None
    facebook_id: Optional[str] = None
    referral_code: Optional[str] = None
    onboarding_step: Optional[int] = 1

    model_config = ConfigDict(from_attributes=True)

class UserCreateAdmin(BaseModel):
    email: str
    password: str
    full_name: Optional[str] = None
    mobile_number: Optional[str] = None
    state: Optional[str] = None
    gst_number: Optional[str] = None
    country: Optional[str] = None
    role: str = "user"
    is_verified: bool = True
    referral_code: Optional[str] = None
    package_id: Optional[int] = None
    base_amount: Optional[int] = None # In Paise, exclusive of GST
