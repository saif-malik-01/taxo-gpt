from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession

from services.auth.jwt import create_access_token
from services.auth.utils import verify_password, get_password_hash
from services.auth.deps import auth_guard
from services.database import get_db
from services.models import User, UserProfile

router = APIRouter(prefix="/auth", tags=["Auth"])

class LoginRequest(BaseModel):
    email: str
    password: str

class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

class RegisterRequest(BaseModel):
    email: str
    password: str
    role: str = "user" # Optional, default to user

@router.post("/register", response_model=LoginResponse)
async def register(payload: RegisterRequest, db: AsyncSession = Depends(get_db)):
    # Check if user exists
    result = await db.execute(select(User).where(User.email == payload.email))
    if result.scalars().first():
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Create user
    new_user = User(
        email=payload.email, 
        password_hash=get_password_hash(payload.password),
        role=payload.role
    )
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)

    # Create empty profile
    new_profile = UserProfile(user_id=new_user.id, preferences={})
    db.add(new_profile)
    await db.commit()

    # Generate token
    token = create_access_token({
        "sub": new_user.email,
        "role": new_user.role
    })

    return {"access_token": token}


@router.post("/login", response_model=LoginResponse)
async def login(payload: LoginRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.email == payload.email))
    user = result.scalars().first()

    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token({
        "sub": user.email,
        "role": user.role
    })

    return {"access_token": token}

class ProfileUpdate(BaseModel):
    dynamic_summary: Optional[str] = None
    preferences: Optional[dict] = None

@router.put("/profile")
async def update_profile(
    payload: ProfileUpdate, 
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    email = user.get("sub")
    
    # Get user
    res = await db.execute(select(User).where(User.email == email))
    db_user = res.scalars().first()
    
    # Get profile
    res = await db.execute(select(UserProfile).where(UserProfile.user_id == db_user.id))
    profile = res.scalars().first()
    
    if not profile:
        profile = UserProfile(user_id=db_user.id)
        db.add(profile)
    
    if payload.dynamic_summary is not None:
        profile.dynamic_summary = payload.dynamic_summary
    if payload.preferences is not None:
        profile.preferences = payload.preferences
        
    await db.commit()
    return {"status": "profile updated"}

