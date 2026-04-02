from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from typing import Optional, List
from datetime import datetime, timedelta, timezone

from apps.api.src.db.session import get_db
from apps.api.src.services.auth.deps import auth_guard, admin_guard
from apps.api.src.db.models.base import CreditPackage, Coupon, PaymentTransaction, User, CreditLog
from apps.api.src.schemas.payments import (
    OrderRequest, VerifyRequest, PackageCreate, 
    PackageUpdate, CouponCreate, CouponUpdate, CouponValidateRequest
)

from apps.api.src.services.payments import create_razorpay_order, verify_payment, validate_coupon_logic, send_invoice_background

router = APIRouter(prefix="/payments", tags=["Payments"])

@router.get("/packages")
async def list_packages(db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(CreditPackage).where(CreditPackage.is_active == True))
    return res.scalars().all()

@router.post("/create-order")
async def create_order(payload: OrderRequest, background_tasks: BackgroundTasks, user=Depends(auth_guard), db: AsyncSession = Depends(get_db)):
    user_id = user.get("id")
    try:
        order = await create_razorpay_order(user_id, payload.package_name, payload.coupon_code, db)
        
        # If it's a free activation, the order is already completed
        if isinstance(order, dict) and order.get("is_free"):
            background_tasks.add_task(send_invoice_background, order.get("order_id"))
            
        return order
    except ValueError as e: raise HTTPException(status_code=400, detail=str(e))

@router.post("/verify")
async def verify(payload: VerifyRequest, background_tasks: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    success = await verify_payment(payload.razorpay_order_id, payload.razorpay_payment_id, payload.razorpay_signature, db)
    if success: 
        background_tasks.add_task(send_invoice_background, payload.razorpay_order_id)
        return {"status": "success"}
    raise HTTPException(status_code=400, detail="Payment verification failed")

@router.get("/history")
async def get_credit_history(user=Depends(auth_guard), db: AsyncSession = Depends(get_db)):
    user_id = user.get("id")
    res = await db.execute(select(CreditLog).where(CreditLog.user_id == user_id).order_by(CreditLog.created_at.desc()).limit(100))
    return res.scalars().all()

@router.post("/validate-coupon")
async def validate_coupon(payload: CouponValidateRequest, db: AsyncSession = Depends(get_db)):
    try:
        return await validate_coupon_logic(payload.coupon_code, payload.package_name, db)
    except ValueError as e: raise HTTPException(status_code=400, detail=str(e))

# All administrative package/coupon logic moved to api/v1/admin.py
