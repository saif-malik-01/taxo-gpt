from pydantic import BaseModel
from typing import Optional

class LoginRequest(BaseModel):
    email: str
    password: str
    identifier: Optional[str] = None # User device or Employee Name Name


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

class GoogleLoginRequest(BaseModel):
    credential: str

class FacebookLoginRequest(BaseModel):
    access_token: str

class RegisterRequest(BaseModel):
    email: str
    password: str
    full_name: Optional[str] = None
    mobile_number: Optional[str] = None
    country: Optional[str] = None
    role: str = "user"

class RegisterResponse(BaseModel):
    message: str
    is_success: bool

class VerifyEmailRequest(BaseModel):
    token: str

class ResendVerificationRequest(BaseModel):
    email: str

class ForgotPasswordRequest(BaseModel):
    email: str

class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str
