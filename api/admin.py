from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession

from services.auth.deps import auth_guard
from services.database import get_db
from services.models import User

router = APIRouter(prefix="/admin/users", tags=["Admin Users"])

def admin_guard(user=Depends(auth_guard)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not authorized. Admin access required.")
    return user

class UserResponse(BaseModel):
    id: int
    email: str
    full_name: Optional[str] = None
    role: str
    max_sessions: int

    class Config:
        from_attributes = True

class UserUpdateRequest(BaseModel):
    role: Optional[str] = None
    max_sessions: Optional[int] = None

@router.get("/", response_model=List[UserResponse])
async def list_users(admin_user=Depends(admin_guard), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User))
    users = result.scalars().all()
    return users

@router.get("/{user_id}", response_model=UserResponse)
async def get_user(user_id: int, admin_user=Depends(admin_guard), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@router.put("/{user_id}", response_model=UserResponse)
async def update_user(user_id: int, payload: UserUpdateRequest, admin_user=Depends(admin_guard), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if payload.role is not None:
        user.role = payload.role
    if payload.max_sessions is not None:
        user.max_sessions = payload.max_sessions
        
    await db.commit()
    await db.refresh(user)
    return user

@router.delete("/{user_id}")
async def delete_user(user_id: int, admin_user=Depends(admin_guard), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    await db.delete(user)
    await db.commit()
    return {"status": "success", "message": f"User {user_id} deleted"}
