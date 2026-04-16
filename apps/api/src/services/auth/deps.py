from fastapi import Header, HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from apps.api.src.services.auth.jwt import verify_token
from apps.api.src.db.session import is_session_active
from apps.api.src.core.config import settings

security = HTTPBearer(auto_error=False)

async def auth_guard(
    request: Request,
    authorization: HTTPAuthorizationCredentials = Depends(security),
    x_csrf_token: str = Header(None, alias="X-CSRF-Token")
):
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing authorization header")

    token = authorization.credentials
    payload = verify_token(token, expected_type="access")
    
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired access token")

    user_id = payload.get("id")
    session_id = payload.get("session_id")
    csrf_sid = payload.get("csrf_sid") # The CSRF secret linked to this token

    # 1. Session Liveness Check (Redis)
    if not await is_session_active(user_id, session_id):
        raise HTTPException(status_code=401, detail="Session expired or revoked")

    # 2. CSRF Protection for state-changing requests
    if request.method not in ("GET", "HEAD", "OPTIONS", "TRACE"):
        if not x_csrf_token or x_csrf_token != csrf_sid:
            raise HTTPException(status_code=403, detail="CSRF token mismatch or missing")

    return payload

async def admin_guard(user_payload = Depends(auth_guard)):
    if user_payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user_payload
