import logging
import uuid
import json
import httpx
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, HTTPException, Depends, Header, Request, Response, BackgroundTasks
from sqlalchemy import func
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests

from apps.api.src.services.auth.jwt import create_access_token, create_refresh_token, verify_token
from apps.api.src.services.auth.utils import verify_password, get_password_hash
from apps.api.src.services.auth.deps import auth_guard
from apps.api.src.db.session import get_db, add_session, remove_session, is_session_active, list_sessions, heartbeat_session, AsyncSessionLocal, redis_client
from apps.api.src.db.models.base import User, UserProfile, UserUsage
from apps.api.src.core.config import settings
from apps.api.src.services.auth.ws_manager import manager

from apps.api.src.schemas.auth import (
    LoginRequest, LoginResponse, GoogleLoginRequest, 
    FacebookLoginRequest, RegisterRequest, RegisterResponse,
    VerifyEmailRequest, ResendVerificationRequest, ForgotPasswordRequest, ResetPasswordRequest
)
from apps.api.src.schemas.user import ProfileUpdate
from apps.api.src.services.email import EmailService
from apps.api.src.services.payments import initialize_user_credits

router = APIRouter(prefix="/auth", tags=["Auth"])
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Standard response helpers
# ---------------------------------------------------------------------------

def ok(data: dict) -> dict:
    """Wrap a successful response payload in the standard envelope."""
    return {"success": True, "data": data}

def message_ok(msg: str) -> dict:
    """Shorthand for simple message-only success responses."""
    return {"success": True, "data": {"message": msg}}

# ---------------------------------------------------------------------------
# Cookie helper
# ---------------------------------------------------------------------------

def set_refresh_cookie(response: Response, refresh_token: str):
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        max_age=settings.REFRESH_TOKEN_EXPIRE_DAYS * 86400,
        expires=settings.REFRESH_TOKEN_EXPIRE_DAYS * 86400,
        samesite="lax",
        secure=settings.ENVIRONMENT.lower() in ["prod", "production"],  # False in dev (HTTP), True in prod (HTTPS)
        domain=settings.COOKIE_DOMAIN if settings.COOKIE_DOMAIN else None
    )

# ---------------------------------------------------------------------------
# Shared login finalization
# ---------------------------------------------------------------------------

async def finalize_login_response(user: User, request: Request, response: Response, identifier: str = "unknown"):
    """Helper to unify session creation and token issuance for all login types."""
    session_id = str(uuid.uuid4())
    csrf_sid = str(uuid.uuid4())
    metadata = {
        "ip": request.client.host if request.client else "unknown",
        "user_agent": request.headers.get("user-agent", "unknown"),
        "identifier": identifier
    }
    
    await add_session(user.id, session_id, user.max_sessions, metadata)
    
    # Reset security counters on any successful login
    user.failed_login_attempts = 0
    user.is_locked = False
    user.locked_until = None
    user.last_login_at = datetime.now(timezone.utc)

    access_token = create_access_token({
        "sub": user.email, "id": user.id, "role": user.role, 
        "session_id": session_id, "csrf_sid": csrf_sid
    })
    refresh_token = create_refresh_token({
        "sub": user.email, "id": user.id, "session_id": session_id
    })
    
    set_refresh_cookie(response, refresh_token)
    return ok({
        "access_token": access_token,
        "csrf_token": csrf_sid,
        "session_id": session_id
    })

# ---------------------------------------------------------------------------
# Core Auth Routes
# ---------------------------------------------------------------------------

@router.post("/login")
async def login(payload: LoginRequest, request: Request, response: Response, db: AsyncSession = Depends(get_db)):
    email = payload.email.lower()
    result = await db.execute(select(User).where(func.lower(User.email) == email))
    user = result.scalars().first()

    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if user.is_locked and user.locked_until:
        if datetime.now(timezone.utc) < user.locked_until.replace(tzinfo=timezone.utc):
            diff = user.locked_until.replace(tzinfo=timezone.utc) - datetime.now(timezone.utc)
            minutes = int(diff.total_seconds() // 60)
            raise HTTPException(status_code=403, detail=f"Account locked. Try again in {minutes + 1} minutes.")
        else:
            user.is_locked = False
            await db.commit()

    if not verify_password(payload.password, user.password_hash):
        user.failed_login_attempts += 1
        if user.failed_login_attempts >= 5:
            user.is_locked = True
            wait_map = {5: 1, 6: 5, 7: 30}
            wait_mins = wait_map.get(user.failed_login_attempts, 1440)
            user.locked_until = datetime.now(timezone.utc) + timedelta(minutes=wait_mins)
        await db.commit()
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if not user.is_verified:
        raise HTTPException(status_code=403, detail="Email not verified. Please check your inbox.")

    data = await finalize_login_response(user, request, response, payload.identifier)
    await db.commit()
    return data

@router.post("/refresh")
async def refresh_tokens(request: Request, response: Response, db: AsyncSession = Depends(get_db)):
    rt_cookie = request.cookies.get("refresh_token")
    if not rt_cookie:
        raise HTTPException(status_code=401, detail="Missing refresh token")
        
    payload = verify_token(rt_cookie, expected_type="refresh")
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")
        
    user_id = payload.get("id")
    session_id = payload.get("session_id")
    
    if not await is_session_active(user_id, session_id):
        raise HTTPException(status_code=401, detail="Session revoked")
        
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalars().first()
    if not user:
         raise HTTPException(status_code=404, detail="User no longer exists")

    new_csrf = str(uuid.uuid4())
    new_at = create_access_token({
        "sub": user.email, "id": user.id, "role": user.role, 
        "session_id": session_id, "csrf_sid": new_csrf
    })
    new_rt = create_refresh_token({
        "sub": user.email, "id": user.id, "session_id": session_id
    })
    
    set_refresh_cookie(response, new_rt)
    return ok({
        "access_token": new_at,
        "csrf_token": new_csrf,
        "session_id": session_id
    })

@router.post("/logout")
async def logout(response: Response, user=Depends(auth_guard)):
    user_id = user.get("id")
    session_id = user.get("session_id")
    if user_id and session_id:
        await remove_session(user_id, session_id)
    response.delete_cookie("refresh_token")
    return message_ok("Logged out successfully")

# ---------------------------------------------------------------------------
# Verification & Bridging
# ---------------------------------------------------------------------------

@router.post("/register")
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
    await initialize_user_credits(new_user.id, db, use_welcome_package=False)
    await db.commit()
    if payload.temp_uid:
        await redis_client.setex(f"pending_verify:{email}", 86400, payload.temp_uid)
    EmailService.send_verification_email(email=email, token=verification_token, full_name=payload.full_name)
    return message_ok("Registration successful. Please verify your email.")

@router.post("/verify-email")
async def verify_email(payload: VerifyEmailRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.verification_token == payload.token))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=400, detail="Invalid or expired verification token")
    user.is_verified = True
    user.verification_token = None
    sync_code = str(uuid.uuid4())
    await db.commit()
    
    # Try to find temp_uid (either from payload or from registration lookup)
    target_uid = payload.temp_uid
    if not target_uid:
        target_uid_bytes = await redis_client.get(f"pending_verify:{user.email}")
        if target_uid_bytes:
            target_uid = target_uid_bytes.decode("utf-8") if isinstance(target_uid_bytes, bytes) else target_uid_bytes
    
    if target_uid:
        await manager.broadcast_verification(target_uid, {
            "type": "USER_VERIFIED",
            "sync_code": sync_code
        })
        await redis_client.setex(f"sync_code:{sync_code}", 300, json.dumps({"user_id": user.id}))
        await redis_client.delete(f"pending_verify:{user.email}")

    return message_ok("Email verified successfully.")

@router.post("/finalize-sync")
async def finalize_sync(sync_code: str, request: Request, response: Response, db: AsyncSession = Depends(get_db)):
    data_json = await redis_client.get(f"sync_code:{sync_code}")
    if not data_json:
        raise HTTPException(status_code=400, detail="Invalid or expired sync code")
    data = json.loads(data_json)
    user_id = data.get("user_id")
    await redis_client.delete(f"sync_code:{sync_code}")
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalars().first()
    resp = await finalize_login_response(user, request, response, "Auto-Login via Verification")
    await db.commit()
    return resp

# ---------------------------------------------------------------------------
# Social Auth
# ---------------------------------------------------------------------------

@router.post("/google")
async def google_login(payload: GoogleLoginRequest, request: Request, response: Response, db: AsyncSession = Depends(get_db)):
    try:
        if payload.credential.startswith("ya29."):
            async with httpx.AsyncClient() as client:
                resp = await client.get("https://www.googleapis.com/oauth2/v3/userinfo", headers={"Authorization": f"Bearer {payload.credential}"})
                if resp.status_code != 200: raise ValueError("Invalid access token")
                idinfo = resp.json()
        else:
            idinfo = id_token.verify_oauth2_token(payload.credential, google_requests.Request(), settings.GOOGLE_CLIENT_ID)
        google_id = idinfo['sub']
        email = idinfo['email'].lower()
        result = await db.execute(select(User).where(User.google_id == google_id))
        user = result.scalars().first()
        if not user:
            result = await db.execute(select(User).where(func.lower(User.email) == email)); user = result.scalars().first()
            if user:
                user.google_id = google_id; user.is_verified = True
            else:
                user = User(email=email, google_id=google_id, full_name=idinfo.get('name'), role="user", is_verified=True)
                db.add(user); await db.commit(); await db.refresh(user)
                db.add(UserProfile(user_id=user.id)); await initialize_user_credits(user.id, db, use_welcome_package=False)
        user.is_verified = True
        resp = await finalize_login_response(user, request, response, "Google Social Login")
        await db.commit()
        return resp
    except Exception as e:
        logger.error(f"Google login error: {e}"); raise HTTPException(status_code=401, detail=str(e))

@router.post("/facebook")
async def facebook_login(payload: FacebookLoginRequest, request: Request, response: Response, db: AsyncSession = Depends(get_db)):
    try:
        async with httpx.AsyncClient() as client:
            fb_res = await client.get("https://graph.facebook.com/me", params={"fields": "id,name,email", "access_token": payload.access_token})
            if fb_res.status_code != 200: raise HTTPException(status_code=401, detail="Invalid Facebook token")
            fb_data = fb_res.json()
        fb_id = fb_data.get('id')
        email = fb_data.get('email', f"{fb_id}@facebook.user").lower()
        result = await db.execute(select(User).where(User.facebook_id == fb_id))
        user = result.scalars().first()
        if not user:
            result = await db.execute(select(User).where(func.lower(User.email) == email)); user = result.scalars().first()
            if user:
                user.facebook_id = fb_id; user.is_verified = True
            else:
                user = User(email=email, facebook_id=fb_id, full_name=fb_data.get('name'), role="user", is_verified=True)
                db.add(user); await db.commit(); await db.refresh(user)
                db.add(UserProfile(user_id=user.id)); await initialize_user_credits(user.id, db, use_welcome_package=False)
        user.is_verified = True
        resp = await finalize_login_response(user, request, response, "Facebook Social Login")
        await db.commit()
        return resp
    except Exception as e:
        logger.error(f"Facebook login error: {e}"); raise HTTPException(status_code=401, detail=str(e))

# ---------------------------------------------------------------------------
# Password Management & Utilities
# ---------------------------------------------------------------------------

@router.post("/resend-verification")
async def resend_verification(payload: ResendVerificationRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(func.lower(User.email) == payload.email.lower()))
    user = result.scalars().first()
    if not user or user.is_verified:
        return message_ok("If your email exists and is unverified, a new link has been sent.")
    token = str(uuid.uuid4()); user.verification_token = token
    await db.commit()
    EmailService.send_verification_email(email=user.email, token=token, full_name=user.full_name)
    return message_ok("Verification email resent.")

@router.post("/forgot-password")
async def forgot_password(payload: ForgotPasswordRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(func.lower(User.email) == payload.email.lower()))
    user = result.scalars().first()
    if user:
        token = str(uuid.uuid4()); user.reset_password_token = token
        user.reset_password_expires = datetime.now(timezone.utc) + timedelta(hours=1)
        await db.commit()
        EmailService.send_password_reset_email(email=user.email, token=token, full_name=user.full_name)
    return message_ok("If your email exists, a reset link has been sent.")

@router.post("/reset-password")
async def reset_password(payload: ResetPasswordRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.reset_password_token == payload.token))
    user = result.scalars().first()
    if not user or user.reset_password_expires < datetime.now(timezone.utc):
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")
    user.password_hash = get_password_hash(payload.new_password)
    user.reset_password_token = None; user.reset_password_expires = None
    user.failed_login_attempts = 0; user.is_locked = False
    await db.commit()
    return message_ok("Password reset successful.")

@router.get("/me")
async def get_me(user=Depends(auth_guard)):
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(User).options(selectinload(User.profile), selectinload(User.usage)).where(User.id == user.get("id")))
        u = res.scalars().first()
        return ok({
            "user": {
                "id": u.id, 
                "email": u.email, 
                "full_name": u.full_name, 
                "role": u.role, 
                "mobile_number": u.mobile_number,
                "state": u.state,
                "gst_number": u.gst_number,
                "country": u.country,
                "onboarding_step": u.onboarding_step, 
                "profile": {
                    "dynamic_summary": u.profile.dynamic_summary if u.profile else None,
                    "preferences": u.profile.preferences if u.profile else {}
                },
                "credits": {
                    "simple_query_balance": u.usage.simple_query_balance if u.usage else 0, 
                    "draft_reply_balance": u.usage.draft_reply_balance if u.usage else 0,
                    "total_tokens_used": u.usage.total_tokens_used if u.usage else 0
                },
                "session_id": user.get("session_id")
            }
        })

@router.patch("/me")
async def update_profile(payload: ProfileUpdate, user=Depends(auth_guard)):
    async with AsyncSessionLocal() as db:
        u = await db.get(User, user.get("id"))
        p = await db.execute(select(UserProfile).where(UserProfile.user_id == u.id))
        profile = p.scalars().first()
        if not profile:
            profile = UserProfile(user_id=u.id)
            db.add(profile)
            
        if payload.full_name is not None: u.full_name = payload.full_name
        if payload.mobile_number is not None: u.mobile_number = payload.mobile_number
        if payload.state is not None: u.state = payload.state
        if payload.gst_number is not None: u.gst_number = payload.gst_number
        
        if payload.dynamic_summary is not None: profile.dynamic_summary = payload.dynamic_summary
        if payload.preferences is not None: profile.preferences = payload.preferences
        
        await db.commit()
        return message_ok("Profile updated successfully.")

@router.get("/sessions")
async def list_active_sessions(user=Depends(auth_guard)):
    sessions = await list_sessions(user.get("id"))
    return ok({"sessions": sessions})

@router.delete("/sessions/{revoke_session_id}")
async def revoke_active_session(revoke_session_id: str, user=Depends(auth_guard)):
    await remove_session(user.get("id"), revoke_session_id)
    return message_ok("Session revoked.")

@router.get("/credits")
async def get_credits(user=Depends(auth_guard)):
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(UserUsage).where(UserUsage.user_id == user.get("id")))
        usage = res.scalars().first()
        return ok({
            "simple_query_balance": usage.simple_query_balance if usage else 0, 
            "draft_reply_balance": usage.draft_reply_balance if usage else 0,
            "total_tokens_used": usage.total_tokens_used if usage else 0
        })
