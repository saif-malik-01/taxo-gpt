from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from services.database import get_db
from services.auth.deps import auth_guard, admin_guard
from services.payments import create_razorpay_order, verify_payment
from services.models import CreditPackage, Coupon, PaymentTransaction, User, CreditLog
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

router = APIRouter(prefix="/payments", tags=["Payments"])

# --- SCHEMAS ---

class OrderRequest(BaseModel):
    package_name: str
    coupon_code: Optional[str] = None

class VerifyRequest(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str

class PackageCreate(BaseModel):
    name: str # e.g. "draft-5"
    title: str
    description: str
    amount: int # In paise
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
    discount_type: str # 'percentage' or 'fixed'
    discount_value: int # In paise or 0-100 percentage
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

# --- PUBLIC ROUTES ---

@router.get("/packages")
async def list_packages(db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(CreditPackage).where(CreditPackage.is_active == True))
    packages = res.scalars().all()
    return packages

@router.post("/create-order")
async def create_order(
    payload: OrderRequest,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    user_id = user.get("id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User ID missing in token")
    
    try:
        order = await create_razorpay_order(user_id, payload.package_name, payload.coupon_code, db)
        return order
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Could not create order")

@router.post("/verify")
async def verify(
    payload: VerifyRequest,
    db: AsyncSession = Depends(get_db)
):
    success = await verify_payment(
        payload.razorpay_order_id,
        payload.razorpay_payment_id,
        payload.razorpay_signature,
        db
    )
    
    if success:
        return {"status": "success", "message": "Credits added successfully"}
    else:
        raise HTTPException(status_code=400, detail="Payment verification failed")

@router.get("/history")
async def get_credit_history(
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    user_id = user.get("id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User ID missing")
        
    res = await db.execute(
        select(CreditLog)
        .where(CreditLog.user_id == user_id)
        .order_by(CreditLog.created_at.desc())
        .limit(100)
    )
    logs = res.scalars().all()
    return logs

# --- ADMIN ROUTES ---

@router.post("/admin/package")
async def create_package(
    payload: PackageCreate,
    user=Depends(admin_guard),
    db: AsyncSession = Depends(get_db)
):
    
    new_package = CreditPackage(
        name=payload.name,
        title=payload.title,
        description=payload.description,
        amount=payload.amount,
        credits_added=payload.credits_added,
        is_active=payload.is_active
    )
    db.add(new_package)
    await db.commit()
    await db.refresh(new_package)
    return new_package

@router.put("/admin/package/{package_id}")
async def update_package(
    package_id: int,
    payload: PackageUpdate,
    user=Depends(admin_guard),
    db: AsyncSession = Depends(get_db)
):
    res = await db.execute(select(CreditPackage).where(CreditPackage.id == package_id))
    package = res.scalars().first()
    
    if not package:
        raise HTTPException(status_code=404, detail="Package not found")
    
    if payload.title is not None: package.title = payload.title
    if payload.description is not None: package.description = payload.description
    if payload.amount is not None: package.amount = payload.amount
    if payload.credits_added is not None: package.credits_added = payload.credits_added
    if payload.is_active is not None: package.is_active = payload.is_active
    
    await db.commit()
    return {"status": "updated"}

@router.delete("/admin/package/{package_id}")
async def delete_package(
    package_id: int,
    user=Depends(admin_guard),
    db: AsyncSession = Depends(get_db)
):
    res = await db.execute(select(CreditPackage).where(CreditPackage.id == package_id))
    package = res.scalars().first()
    
    if not package:
        raise HTTPException(status_code=404, detail="Package not found")
    
    # Nullify references in transactions before deleting
    from sqlalchemy import update
    await db.execute(
        update(PaymentTransaction)
        .where(PaymentTransaction.package_id == package_id)
        .values(package_id=None)
    )
    
    await db.delete(package)
    await db.commit()
    return {"status": "deleted", "id": package_id}

@router.post("/admin/coupon")
async def create_coupon(
    payload: CouponCreate,
    user=Depends(admin_guard),
    db: AsyncSession = Depends(get_db)
):
    new_coupon = Coupon(
        code=payload.code,
        discount_type=payload.discount_type,
        discount_value=payload.discount_value,
        max_uses=payload.max_uses,
        valid_from=payload.valid_from,
        valid_until=payload.valid_until,
        is_active=payload.is_active
    )
    db.add(new_coupon)
    try:
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail="Coupon code may already exist or invalid data")
    await db.refresh(new_coupon)
    return new_coupon

@router.get("/admin/coupons")
async def list_coupons(
    user=Depends(admin_guard),
    db: AsyncSession = Depends(get_db)
):
    res = await db.execute(select(Coupon))
    coupons = res.scalars().all()
    return coupons

@router.put("/admin/coupon/{coupon_id}")
async def update_coupon(
    coupon_id: int,
    payload: CouponUpdate,
    user=Depends(admin_guard),
    db: AsyncSession = Depends(get_db)
):
    res = await db.execute(select(Coupon).where(Coupon.id == coupon_id))
    coupon = res.scalars().first()
    if not coupon:
        raise HTTPException(status_code=404, detail="Coupon not found")
        
    if payload.is_active is not None: coupon.is_active = payload.is_active
    if payload.max_uses is not None: coupon.max_uses = payload.max_uses
    if payload.valid_until is not None: coupon.valid_until = payload.valid_until
    
    await db.commit()
    return {"status": "updated"}

@router.delete("/admin/coupon/{coupon_id}")
async def delete_coupon(
    coupon_id: int,
    user=Depends(admin_guard),
    db: AsyncSession = Depends(get_db)
):
    res = await db.execute(select(Coupon).where(Coupon.id == coupon_id))
    coupon = res.scalars().first()
    if not coupon:
        raise HTTPException(status_code=404, detail="Coupon not found")
        
    # Nullify references in transactions before deleting
    from sqlalchemy import update
    await db.execute(
        update(PaymentTransaction)
        .where(PaymentTransaction.coupon_id == coupon_id)
        .values(coupon_id=None)
    )
    
    await db.delete(coupon)
    await db.commit()
    return {"status": "deleted", "id": coupon_id}

@router.post("/validate-coupon")
async def validate_coupon(
    payload: CouponValidateRequest,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    res = await db.execute(select(CreditPackage).where(CreditPackage.name == payload.package_name, CreditPackage.is_active == True))
    package = res.scalars().first()
    if not package:
        raise HTTPException(status_code=404, detail="Package not found")
        
    res_c = await db.execute(select(Coupon).where(Coupon.code == payload.coupon_code, Coupon.is_active == True))
    coupon = res_c.scalars().first()
    
    if not coupon:
        raise HTTPException(status_code=400, detail="Invalid coupon code")
    
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    # Check naive vs aware depending on DB, but assume UTC
    if coupon.valid_from and coupon.valid_from > now:
        raise HTTPException(status_code=400, detail="Coupon is not valid yet")
    if coupon.valid_until and coupon.valid_until < now:
        raise HTTPException(status_code=400, detail="Coupon has expired")
    if coupon.max_uses and coupon.current_uses >= coupon.max_uses:
        raise HTTPException(status_code=400, detail="Coupon maximum usage total limit reached")
        
    discount = 0
    if coupon.discount_type == 'fixed':
        discount = coupon.discount_value
    elif coupon.discount_type == 'percentage':
        discount = int(package.amount * (coupon.discount_value / 100))
        
    final_amount = max(0, package.amount - discount)
    
    return {
        "is_valid": True,
        "original_amount": package.amount,
        "discount_amount": discount,
        "final_amount": final_amount,
        "currency": package.currency
    }

@router.get("/admin/analytics")
async def get_analytics(
    user=Depends(admin_guard),
    db: AsyncSession = Depends(get_db)
):
    # Total Users
    res_users = await db.execute(select(func.count(User.id)))
    total_users = res_users.scalar() or 0
    
    # Revenue (Only completed transactions)
    res_rev = await db.execute(select(func.sum(PaymentTransaction.amount)).where(PaymentTransaction.status == "completed"))
    total_revenue_paise = res_rev.scalar() or 0
    
    # Packages Sold
    res_sales = await db.execute(select(func.count(PaymentTransaction.id)).where(PaymentTransaction.status == "completed"))
    total_sales = res_sales.scalar() or 0
    
    return {
        "total_users": total_users,
        "total_revenue_paise": total_revenue_paise,
        "total_sales": total_sales
    }
