from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
import httpx

from services.auth.jwt import create_access_token
from services.auth.utils import verify_password, get_password_hash
from services.auth.deps import auth_guard
from services.database import get_db
from services.models import User, UserProfile, UserUsage
from services.redis import add_session, remove_session
from api.config import settings
import uuid

router = APIRouter(prefix="/auth", tags=["Auth"])

class LoginRequest(BaseModel):
    email: str
    password: str

class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

class GoogleLoginRequest(BaseModel):
    credential: str

class FacebookLoginRequest(BaseModel):
    access_token: str

@router.post("/google", response_model=LoginResponse)
async def google_login(payload: GoogleLoginRequest, db: AsyncSession = Depends(get_db)):
    try:
        # Check if credential is an access token (starts with ya29.)
        if payload.credential.startswith("ya29."):
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    "https://www.googleapis.com/oauth2/v3/userinfo",
                    headers={"Authorization": f"Bearer {payload.credential}"}
                )
                if resp.status_code != 200:
                    raise ValueError(f"Invalid access token: {resp.text}")
                idinfo = resp.json()
        else:
            # It's an ID token (JWT)
            idinfo = id_token.verify_oauth2_token(
                payload.credential, 
                google_requests.Request(), 
                settings.GOOGLE_CLIENT_ID
            )

        google_id = idinfo['sub']
        email = idinfo['email']
        name = idinfo.get('name')

        # Check if user exists by google_id
        result = await db.execute(select(User).where(User.google_id == google_id))
        user = result.scalars().first()

        if not user:
            # Check if user exists by email
            result = await db.execute(select(User).where(User.email == email))
            user = result.scalars().first()

            if user:
                # Link existing user to Google account
                user.google_id = google_id
                if not user.full_name and name:
                    user.full_name = name
                await db.commit()
            else:
                # Create new user
                user = User(
                    email=email,
                    google_id=google_id,
                    full_name=name,
                    password_hash=None, # No password for Google users
                    role="user"
                )
                db.add(user)
                await db.commit()
                await db.refresh(user)

                # Create empty profile and usage
                new_profile = UserProfile(user_id=user.id, preferences={})
                db.add(new_profile)
                
                new_usage = UserUsage(user_id=user.id)
                db.add(new_usage)
                
                await db.commit()

        session_id = str(uuid.uuid4())
        await add_session(user.id, session_id, user.max_sessions)

        token = create_access_token({
            "sub": user.email,
            "id": user.id,
            "role": user.role,
            "session_id": session_id
        })

        return {"access_token": token}

    except ValueError as e:
        raise HTTPException(status_code=401, detail=f"Invalid Google token: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/facebook", response_model=LoginResponse)
async def facebook_login(payload: FacebookLoginRequest, db: AsyncSession = Depends(get_db)):
    try:
        # Verify Facebook token with Graph API
        async with httpx.AsyncClient() as client:
            fb_res = await client.get(
                "https://graph.facebook.com/me",
                params={
                    "fields": "id,name,email",
                    "access_token": payload.access_token
                }
            )
            
            if fb_res.status_code != 200:
                raise HTTPException(status_code=401, detail="Invalid Facebook token")
            
            fb_data = fb_res.json()
            
        fb_id = fb_data.get('id')
        email = fb_data.get('email')
        name = fb_data.get('name')

        if not fb_id:
            raise HTTPException(status_code=401, detail="Could not retrieve Facebook ID")

        # Check if user exists by facebook_id
        result = await db.execute(select(User).where(User.facebook_id == fb_id))
        user = result.scalars().first()

        if not user:
            # If no email from FB (rare but possible), we might need another way or just fail
            if not email:
                # We can try to find by ID only or ask for email. 
                # For simplicity, if email is missing, we'll use a placeholder or fail.
                # Usually FB provides email if requested in scopes.
                pass

            if email:
                # Check if user exists by email
                result = await db.execute(select(User).where(User.email == email))
                user = result.scalars().first()

            if user:
                # Link existing user to Facebook account
                user.facebook_id = fb_id
                if not user.full_name and name:
                    user.full_name = name
                await db.commit()
            else:
                # Create new user
                # If email is missing, we generate a fake one or handle it
                effective_email = email or f"{fb_id}@facebook.user"
                user = User(
                    email=effective_email,
                    facebook_id=fb_id,
                    full_name=name,
                    password_hash=None,
                    role="user"
                )
                db.add(user)
                await db.commit()
                await db.refresh(user)

                # Create profile and usage
                new_profile = UserProfile(user_id=user.id, preferences={})
                db.add(new_profile)
                new_usage = UserUsage(user_id=user.id)
                db.add(new_usage)
                await db.commit()

        session_id = str(uuid.uuid4())
        await add_session(user.id, session_id, user.max_sessions)

        token = create_access_token({
            "sub": user.email,
            "id": user.id,
            "role": user.role,
            "session_id": session_id
        })

        return {"access_token": token}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class RegisterRequest(BaseModel):
    email: str
    password: str
    full_name: Optional[str] = None
    mobile_number: Optional[str] = None
    country: Optional[str] = None
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
        full_name=payload.full_name,
        mobile_number=payload.mobile_number,
        country=payload.country,
        role=payload.role
    )
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)

    # Create empty profile and usage
    new_profile = UserProfile(user_id=new_user.id, preferences={})
    db.add(new_profile)
    
    new_usage = UserUsage(user_id=new_user.id)
    db.add(new_usage)
    
    await db.commit()

    # Create session
    session_id = str(uuid.uuid4())
    await add_session(new_user.id, session_id, new_user.max_sessions)

    # Generate token
    token = create_access_token({
        "sub": new_user.email,
        "id": new_user.id,
        "role": new_user.role,
        "session_id": session_id
    })

    return {"access_token": token}


@router.post("/login", response_model=LoginResponse)
async def login(payload: LoginRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.email == payload.email))
    user = result.scalars().first()

    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    session_id = str(uuid.uuid4())
    await add_session(user.id, session_id, user.max_sessions)

    token = create_access_token({
        "sub": user.email,
        "id": user.id,
        "role": user.role,
        "session_id": session_id
    })

    return {"access_token": token}

@router.post("/logout")
async def logout(user=Depends(auth_guard)):
    user_id = user.get("id")
    session_id = user.get("session_id")
    if user_id and session_id:
        await remove_session(user_id, session_id)
    return {"status": "logged out"}

@router.get("/me")
async def get_me(user=Depends(auth_guard), db: AsyncSession = Depends(get_db)):
    email = user.get("sub")
    if not email:
        return {"user": user}
        
    # Get user with profile
    res = await db.execute(
        select(User)
        .options(selectinload(User.profile))
        .where(User.email == email)
    )
    db_user = res.scalars().first()
    
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
        
    return {
        "user": {
            "id": db_user.id,
            "email": db_user.email,
            "full_name": db_user.full_name,
            "mobile_number": db_user.mobile_number,
            "country": db_user.country,
            "role": db_user.role,
            "created_at": db_user.created_at,
            "profile": {
                "dynamic_summary": db_user.profile.dynamic_summary if db_user.profile else None,
                "preferences": db_user.profile.preferences if db_user.profile else {}
            }
        }
    }

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

