from fastapi import Header, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from services.auth.jwt import verify_token

from services.redis import is_session_active

API_KEY = "gst-secret-123"
security = HTTPBearer(auto_error=False)


async def auth_guard(
    authorization: HTTPAuthorizationCredentials = Depends(security),
    x_api_key: str = Header(default=None)
):
    # ✅ API KEY (fallback)
    if x_api_key == API_KEY:
        return {"auth": "api_key"}

    # ✅ JWT
    if authorization:
        token = authorization.credentials
        payload = verify_token(token)
        if payload:
            # Verify session in Redis
            user_id = payload.get("id")
            session_id = payload.get("session_id")
            
            # If token was generated before session tracking was added, it won't have session_id
            # In a real migration, you might want to allow them or force relogin.
            # Here we enforce it if session_id is missing or inactive.
            if user_id and session_id:
                if await is_session_active(user_id, session_id):
                    return payload
            elif payload.get("sub") and not session_id:
                # Fallback for old tokens if needed, but safer to deny
                # For this task, we assume we want strict session control
                pass

    raise HTTPException(status_code=401, detail="Unauthorized")
