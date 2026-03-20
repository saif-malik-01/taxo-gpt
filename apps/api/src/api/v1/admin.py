from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func
from pydantic import BaseModel

from apps.api.src.services.auth.deps import admin_guard
from apps.api.src.db.session import get_db
from apps.api.src.db.models.base import User, CreditPackage, Coupon, PaymentTransaction
from apps.api.src.schemas.payments import (
    PackageCreate, PackageUpdate, CouponCreate, CouponUpdate
)

router = APIRouter(prefix="/admin", tags=["Admin"])

# --- User Management ---
class UserUpdateAdmin(BaseModel):
    role: Optional[str] = None
    max_sessions: Optional[int] = None

@router.get("/users")
async def list_users(admin_user=Depends(admin_guard), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User))
    return result.scalars().all()

@router.get("/users/{user_id}")
async def get_user(user_id: int, admin_user=Depends(admin_guard), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalars().first()
    if not user: raise HTTPException(status_code=404, detail="User not found")
    return user

@router.patch("/users/{user_id}")
async def update_user(user_id: int, payload: UserUpdateAdmin, admin_user=Depends(admin_guard), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalars().first()
    if not user: raise HTTPException(status_code=404, detail="User not found")
    
    if payload.role is not None: user.role = payload.role
    if payload.max_sessions is not None: user.max_sessions = payload.max_sessions
    
    await db.commit(); await db.refresh(user)
    return user

@router.delete("/users/{user_id}")
async def delete_user(user_id: int, admin_user=Depends(admin_guard), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalars().first()
    if not user: raise HTTPException(status_code=404, detail="User not found")
    await db.delete(user); await db.commit()
    return {"status": "success"}

# --- Package & Coupon Management ---
@router.post("/payments/packages")
async def create_package(payload: PackageCreate, admin_user=Depends(admin_guard), db: AsyncSession = Depends(get_db)):
    new_pkg = CreditPackage(**payload.dict())
    db.add(new_pkg); await db.commit(); await db.refresh(new_pkg)
    return new_pkg

@router.get("/payments/packages")
async def list_all_packages(admin_user=Depends(admin_guard), db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(CreditPackage))
    return res.scalars().all()

@router.post("/payments/coupons")
async def create_coupon(payload: CouponCreate, admin_user=Depends(admin_guard), db: AsyncSession = Depends(get_db)):
    new_coupon = Coupon(**payload.dict())
    db.add(new_coupon); await db.commit(); await db.refresh(new_coupon)
    return new_coupon

@router.get("/analytics")
async def get_analytics(admin_user=Depends(admin_guard), db: AsyncSession = Depends(get_db)):
    # Basic analytics: total users, total transactions
    user_count = await db.scalar(select(func.count()).select_from(User))
    trans_count = await db.scalar(select(func.count()).select_from(PaymentTransaction))
    return {
        "total_users": user_count,
        "total_transactions": trans_count
    }
