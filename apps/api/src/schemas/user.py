from pydantic import BaseModel, ConfigDict, model_validator
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
    referral_code: Optional[str] = None
    package_id: Optional[int] = None
    base_amount: Optional[int] = None # In Paise, exclusive of GST
    max_sessions: Optional[int] = None
    expiration_days: Optional[int] = None

    @model_validator(mode="after")
    def check_package_logic(self) -> "UserCreateAdmin":
        has_pkg = self.package_id is not None
        has_amt = self.base_amount is not None
        has_exp = self.expiration_days is not None

        # 1. amount/expiration without a package doesn't make sense
        if (has_amt or has_exp) and not has_pkg:
            raise ValueError("package_id is required when providing base_amount or expiration_days.")
            
        # 2. Prevent negative values
        if has_amt and self.base_amount < 0:
            raise ValueError("base_amount cannot be negative.")
            
        if has_exp and self.expiration_days < 0:
            raise ValueError("expiration_days cannot be negative.")
            
        return self
