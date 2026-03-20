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
