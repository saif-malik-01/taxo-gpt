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
    if x_api_key == API_KEY:
        return {"auth": "api_key"}

    if authorization:
        token = authorization.credentials
        payload = verify_token(token)
        if payload:
            user_id = payload.get("id")
            session_id = payload.get("session_id")
            
            if user_id and session_id:
                if await is_session_active(user_id, session_id):
                    return payload
            elif payload.get("sub") and not session_id:
                pass

    raise HTTPException(status_code=401, detail="Unauthorized")

async def admin_guard(user_payload = Depends(auth_guard)):
    if user_payload.get("auth") == "api_key":
        return user_payload
    if user_payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user_payload
