import logging
import uuid
import json
from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, Header, Request
from sqlalchemy import func
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
import httpx

from apps.api.src.services.auth.jwt import create_access_token
from apps.api.src.services.auth.utils import verify_password, get_password_hash
from apps.api.src.services.auth.deps import auth_guard
from apps.api.src.db.session import get_db, add_session, remove_session, list_sessions, heartbeat_session
from apps.api.src.db.models.base import User, UserProfile, UserUsage
from apps.api.src.core.config import settings

from apps.api.src.schemas.auth import (
    LoginRequest, LoginResponse, GoogleLoginRequest, 
    FacebookLoginRequest, RegisterRequest, RegisterResponse,
    VerifyEmailRequest, ResendVerificationRequest, ForgotPasswordRequest, ResetPasswordRequest
)
from apps.api.src.schemas.user import ProfileUpdate
from apps.api.src.services.email import EmailService

router = APIRouter(prefix="/auth", tags=["Auth"])
logger = logging.getLogger(__name__)

@router.post("/login", response_model=LoginResponse)
async def login(payload: LoginRequest, request: Request, db: AsyncSession = Depends(get_db)):
    email = payload.email.lower()
    result = await db.execute(select(User).where(func.lower(User.email) == email))
    user = result.scalars().first()

    if not user or not verify_password(payload.password, user.password_hash):
        logger.warning(f"Failed login attempt for email: {email}")
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if not user.is_verified:
        raise HTTPException(status_code=403, detail="Email not verified. Please check your inbox.")

    session_id = str(uuid.uuid4())
    metadata = {
        "ip": request.client.host if request.client else "unknown",
        "user_agent": request.headers.get("user-agent", "unknown"),
        "identifier": payload.identifier
    }
    
    try:
        user.last_login_at = datetime.now(timezone.utc)
        await add_session(user.id, session_id, user.max_sessions, metadata)
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e)) # Rejects login due to Limit constraint!

    token = create_access_token({
        "sub": user.email,
        "id": user.id,
        "role": user.role,
        "session_id": session_id
    })
    return {"access_token": token}

@router.post("/google", response_model=LoginResponse)
async def google_login(payload: GoogleLoginRequest, request: Request, db: AsyncSession = Depends(get_db)):
    try:
        if payload.credential.startswith("ya29."):
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    "https://www.googleapis.com/oauth2/v3/userinfo",
                    headers={"Authorization": f"Bearer {payload.credential}"}
                )
                if resp.status_code != 200:
                    raise ValueError(f"Invalid access token")
                idinfo = resp.json()
        else:
            idinfo = id_token.verify_oauth2_token(
                payload.credential, 
                google_requests.Request(), 
                settings.GOOGLE_CLIENT_ID
            )

        google_id = idinfo['sub']
        email = idinfo['email'].lower()
        name = idinfo.get('name')

        result = await db.execute(select(User).where(User.google_id == google_id))
        user = result.scalars().first()

        if not user:
            result = await db.execute(select(User).where(func.lower(User.email) == email))
            user = result.scalars().first()

            if user:
                user.google_id = google_id
                user.is_verified = True # Linked social means verified
                if not user.full_name and name:
                    user.full_name = name
                await db.commit()
            else:
                user = User(
                    email=email,
                    google_id=google_id,
                    full_name=name,
                    password_hash=None,
                    role="user",
                    is_verified=True # Social logins are auto-verified
                )
                db.add(user)
                await db.commit()
                await db.refresh(user)

                db.add(UserProfile(user_id=user.id))
                db.add(UserUsage(user_id=user.id))
                await db.commit()

        # Always verify if social login was successful
        if not user.is_verified:
            user.is_verified = True
            await db.commit()

        session_id = str(uuid.uuid4())
        metadata = {
            "ip": request.client.host if request.client else "unknown",
            "user_agent": request.headers.get("user-agent", "unknown"),
            "identifier": "Google Social Login"
        }
        try:
            user.last_login_at = datetime.now(timezone.utc)
            await add_session(user.id, session_id, user.max_sessions, metadata)
        except ValueError as e:
            raise HTTPException(status_code=403, detail=str(e))

        token = create_access_token({
            "sub": user.email, "id": user.id, "role": user.role, "session_id": session_id
        })
        return {"access_token": token}

    except Exception as e:
        logger.error(f"Google login error: {e}")
        raise HTTPException(status_code=401, detail=str(e))

@router.post("/facebook", response_model=LoginResponse)
async def facebook_login(payload: FacebookLoginRequest, request: Request, db: AsyncSession = Depends(get_db)):
    try:
        async with httpx.AsyncClient() as client:
            fb_res = await client.get(
                "https://graph.facebook.com/me",
                params={"fields": "id,name,email", "access_token": payload.access_token}
            )
            if fb_res.status_code != 200:
                raise HTTPException(status_code=401, detail="Invalid Facebook token")
            fb_data = fb_res.json()
            
        fb_id = fb_data.get('id')
        email = fb_data.get('email', f"{fb_id}@facebook.user").lower()
        name = fb_data.get('name')

        result = await db.execute(select(User).where(User.facebook_id == fb_id))
        user = result.scalars().first()

        if not user:
            result = await db.execute(select(User).where(func.lower(User.email) == email))
            user = result.scalars().first()

            if user:
                user.facebook_id = fb_id
                user.is_verified = True # Linked social means verified
                await db.commit()
            else:
                user = User(
                    email=email, facebook_id=fb_id, full_name=name,
                    password_hash=None, role="user", is_verified=True
                )
                db.add(user)
                await db.commit()
                await db.refresh(user)
                db.add(UserProfile(user_id=user.id))
                db.add(UserUsage(user_id=user.id))
                await db.commit()

        # Always verify if social login was successful
        if not user.is_verified:
            user.is_verified = True
            await db.commit()

        session_id = str(uuid.uuid4())
        metadata = {
            "ip": request.client.host if request.client else "unknown",
            "user_agent": request.headers.get("user-agent", "unknown"),
            "identifier": "Facebook Social Login"
        }
        try:
            user.last_login_at = datetime.now(timezone.utc)
            await add_session(user.id, session_id, user.max_sessions, metadata)
        except ValueError as e:
            raise HTTPException(status_code=403, detail=str(e))

        token = create_access_token({
            "sub": user.email, "id": user.id, "role": user.role, "session_id": session_id
        })
        return {"access_token": token}
    except Exception as e:
        logger.error(f"Facebook login error: {e}")
        raise HTTPException(status_code=401, detail=str(e))

@router.post("/register", response_model=RegisterResponse)
async def register(payload: RegisterRequest, db: AsyncSession = Depends(get_db)):
    email = payload.email.lower()
    result = await db.execute(select(User).where(func.lower(User.email) == email))
    if result.scalars().first():
        raise HTTPException(status_code=400, detail="Email already registered")
    
    verification_token = str(uuid.uuid4())
    new_user = User(
        email=email, 
        password_hash=get_password_hash(payload.password),
        full_name=payload.full_name,
        mobile_number=payload.mobile_number,
        state=payload.state,
        gst_number=payload.gst_number,
        country=payload.country,
        role=payload.role,
        is_verified=False,
        verification_token=verification_token
    )
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)

    db.add(UserProfile(user_id=new_user.id))
    db.add(UserUsage(
        user_id=new_user.id,
        credits_expire_at=datetime.now(timezone.utc) + timedelta(days=365)
    ))
    await db.commit()

    EmailService.send_verification_email(email=email, token=verification_token, full_name=payload.full_name)

    return {"message": "Registration successful. Please verify email.", "is_success": True}

@router.post("/verify-email")
async def verify_email(payload: VerifyEmailRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.verification_token == payload.token))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=400, detail="Invalid token")
        
    user.is_verified = True
    user.verification_token = None
    await db.commit()
    return {"message": "Email verified successfully", "is_success": True}

@router.post("/resend-verification")
async def resend_verification(payload: ResendVerificationRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(func.lower(User.email) == payload.email.lower()))
    user = result.scalars().first()
    if not user: return {"message": "Verification email sent if account exists."}
    if user.is_verified: return {"message": "Email already verified."}

    token = str(uuid.uuid4())
    user.verification_token = token
    await db.commit()
    EmailService.send_verification_email(email=user.email, token=token, full_name=user.full_name)
    return {"message": "Verification email resent."}
    
@router.post("/forgot-password")
async def forgot_password(payload: ForgotPasswordRequest, db: AsyncSession = Depends(get_db)):
    from datetime import datetime, timedelta, timezone
    email = payload.email.lower()
    result = await db.execute(select(User).where(func.lower(User.email) == email))
    user = result.scalars().first()
    
    # We return success even if user not found for security (prevent email enumeration)
    if user:
        token = str(uuid.uuid4())
        user.reset_password_token = token
        user.reset_password_expires = datetime.now(timezone.utc) + timedelta(hours=1)
        await db.commit()
        EmailService.send_password_reset_email(email=user.email, token=token, full_name=user.full_name)
    
    return {"message": "If that email is registered, you will receive a reset link shortly."}

@router.post("/reset-password")
async def reset_password(payload: ResetPasswordRequest, db: AsyncSession = Depends(get_db)):
    from datetime import datetime, timezone
    result = await db.execute(select(User).where(User.reset_password_token == payload.token))
    user = result.scalars().first()
    
    if not user:
        raise HTTPException(status_code=400, detail="Invalid token")
    
    if user.reset_password_expires < datetime.now(timezone.utc):
        raise HTTPException(status_code=400, detail="Token expired")
    
    user.password_hash = get_password_hash(payload.new_password)
    user.reset_password_token = None
    user.reset_password_expires = None
    await db.commit()
    
    return {"message": "Password reset successful.", "is_success": True}

@router.post("/logout")
async def logout(user=Depends(auth_guard)):
    user_id = user.get("id")
    session_id = user.get("session_id")
    if user_id and session_id:
        await remove_session(user_id, session_id)
    return {"status": "logged out"}

@router.post("/heartbeat")
async def maintain_heartbeat(user=Depends(auth_guard)):
    """
    Called by Frontend via setInterval every 1 or 2 minutes to block session from 
    garbage collection and dying from inactivity TTL. Needs Bearer token.
    
    Returns a refreshed token if the current one is nearing its 30-minute expiry.
    """
    import time
    user_id = user.get("id")
    session_id = user.get("session_id")
    exp = user.get("exp")
    
    alive = await heartbeat_session(user_id, session_id)
    if not alive:
        # If frontend missed strict TTL (5 mins), we forcibly bounce them out
        raise HTTPException(status_code=401, detail="Session expired due to inactivity or closure.")
    
    # Update last active time
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(User).where(User.id == user_id))
        db_user = result.scalars().first()
        if db_user:
            db_user.last_login_at = datetime.now(timezone.utc)
            await db.commit()
    
    response = {"status": "alive"}
    
    # Token Rotation: If token expires within next 10 minutes, issue a fresh one
    # This ensures the session stays active indefinitely as long as tab is open.
    if exp and (exp - time.time() < 600):
        new_token = create_access_token({
            "sub": user.get("sub"),
            "id": user_id,
            "role": user.get("role"),
            "session_id": session_id
        })
        response["new_access_token"] = new_token
        
    return response

@router.get("/sessions")
async def list_active_sessions(user=Depends(auth_guard)):
    """
    Company Admin View - See exactly who is logged in simultaneously right now
    """
    return await list_sessions(user.get("id"))

@router.delete("/sessions/{revoke_session_id}")
async def revoke_active_session(revoke_session_id: str, user=Depends(auth_guard)):
    """
    Immediately destroy a ghost session so another user can freely log in
    """
    await remove_session(user.get("id"), revoke_session_id)
    return {"status": "revoked"}

@router.get("/me")
async def get_me(user=Depends(auth_guard)):
    async with AsyncSessionLocal() as db:
        email = user.get("sub")
        res = await db.execute(
            select(User).options(
                selectinload(User.profile),
                selectinload(User.usage)
            ).where(func.lower(User.email) == email.lower())
        )
        db_user = res.scalars().first()
        if not db_user: raise HTTPException(status_code=404, detail="User not found")
            
        return {
            "user": {
                "id": db_user.id, "email": db_user.email, "full_name": db_user.full_name,
                "mobile_number": db_user.mobile_number, "state": db_user.state, "gst_number": db_user.gst_number,
                "country": db_user.country, "role": db_user.role, 
                "profile": {
                    "dynamic_summary": db_user.profile.dynamic_summary if db_user.profile else None,
                    "preferences": db_user.profile.preferences if db_user.profile else {}
                },
                "credits": {
                    "simple_query_balance": db_user.usage.simple_query_balance if db_user.usage else 0,
                    "draft_reply_balance": db_user.usage.draft_reply_balance if db_user.usage else 0,
                    "total_tokens_used": db_user.usage.total_tokens_used if db_user.usage else 0
                }
            }
        }

@router.get("/credits")
async def get_credits(user=Depends(auth_guard)):
    async with AsyncSessionLocal() as db:
        user_id = user.get("id")
        res = await db.execute(
            select(UserUsage).where(UserUsage.user_id == user_id)
        )
        usage = res.scalars().first()
        if not usage:
            usage = UserUsage(
                user_id=user_id,
                credits_expire_at=datetime.now(timezone.utc) + timedelta(days=365)
            )
            db.add(usage)
            await db.commit()
        
        return {
            "simple_query_balance": usage.simple_query_balance,
            "draft_reply_balance": usage.draft_reply_balance,
            "total_tokens_used": usage.total_tokens_used
        }

@router.patch("/me")
async def update_profile(payload: ProfileUpdate, user=Depends(auth_guard)):
    async with AsyncSessionLocal() as db:
        email = user.get("sub")
        res = await db.execute(select(User).where(func.lower(User.email) == email.lower()))
        db_user = res.scalars().first()
        
        res = await db.execute(select(UserProfile).where(UserProfile.user_id == db_user.id))
        profile = res.scalars().first()
        
        if not profile:
            profile = UserProfile(user_id=db_user.id); db.add(profile)
        
        if payload.full_name is not None: db_user.full_name = payload.full_name
        if payload.mobile_number is not None: db_user.mobile_number = payload.mobile_number
        if payload.state is not None: db_user.state = payload.state
        if payload.gst_number is not None: db_user.gst_number = payload.gst_number

        if payload.dynamic_summary is not None: profile.dynamic_summary = payload.dynamic_summary
        if payload.preferences is not None: profile.preferences = payload.preferences
            
        await db.commit()
        await db.refresh(db_user)
        await db.refresh(profile)

        return {
            "status": "profile updated", 
            "user": {
                "full_name": db_user.full_name,
                "mobile_number": db_user.mobile_number,
                "state": db_user.state,
                "gst_number": db_user.gst_number
            },
            "profile": {
                "dynamic_summary": profile.dynamic_summary,
                "preferences": profile.preferences
            }
        }
