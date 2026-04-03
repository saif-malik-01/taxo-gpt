from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class OrderRequest(BaseModel):
    package_name: str
    coupon_code: Optional[str] = None

class VerifyRequest(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str

class PackageCreate(BaseModel):
    name: str
    title: str
    description: str
    amount: int
    credits_added: int
    is_active: Optional[bool] = True

class PackageUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    amount: Optional[int] = None
    credits_added: Optional[int] = None
    is_active: Optional[bool] = None

class CouponCreate(BaseModel):
    code: str
    discount_type: str
    discount_value: int
    max_uses: Optional[int] = None
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None
    is_active: Optional[bool] = True

class CouponUpdate(BaseModel):
    is_active: Optional[bool] = None
    max_uses: Optional[int] = None
    valid_until: Optional[datetime] = None

class CouponValidateRequest(BaseModel):
    coupon_code: str
    package_name: str

class PackageResponse(BaseModel):
    id: int
    name: str
    title: str
    description: Optional[str] = None
    amount: int
    currency: str
    credits_added: int
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}

class CouponResponse(BaseModel):
    id: int
    code: str
    discount_type: str
    discount_value: int
    max_uses: Optional[int] = None
    current_uses: int
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}

class TransactionPackageInfo(BaseModel):
    id: int
    name: Optional[str] = None
    title: Optional[str] = None

    model_config = {"from_attributes": True}

class TransactionUserInfo(BaseModel):
    id: int
    email: str
    full_name: Optional[str] = None

    model_config = {"from_attributes": True}

class TransactionResponse(BaseModel):
    id: int
    order_id: str
    payment_id: Optional[str] = None
    amount: Optional[int] = 0
    currency: Optional[str] = "INR"
    credits_added: Optional[int] = 0
    discount_amount: Optional[int] = 0
    status: Optional[str] = "pending"
    created_at: Optional[datetime] = None
    package: Optional[TransactionPackageInfo] = None

    model_config = {"from_attributes": True}

class AdminTransactionResponse(TransactionResponse):
    user: Optional[TransactionUserInfo] = None
