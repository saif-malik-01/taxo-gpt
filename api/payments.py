from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from services.database import get_db
from services.auth.deps import auth_guard
from services.payments import create_razorpay_order, verify_payment
from services.models import CreditPackage
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter(prefix="/payments", tags=["Payments"])

# --- SCHEMAS ---

class OrderRequest(BaseModel):
    package_name: str

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
        order = await create_razorpay_order(user_id, payload.package_name, db)
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

# --- ADMIN ROUTES (Can be restricted further by role in future) ---

@router.post("/admin/package")
async def create_package(
    payload: PackageCreate,
    user=Depends(auth_guard), # Add role check here if needed
    db: AsyncSession = Depends(get_db)
):
    # Optional: check if user.role == 'admin'
    
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
    user=Depends(auth_guard),
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
