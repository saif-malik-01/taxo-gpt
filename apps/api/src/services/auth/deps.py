from fastapi import Header, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from apps.api.src.services.auth.jwt import verify_token
from apps.api.src.db.session import is_session_active
from apps.api.src.core.config import settings

security = HTTPBearer(auto_error=False)

async def auth_guard(
    authorization: HTTPAuthorizationCredentials = Depends(security)
):
    if authorization:
        token = authorization.credentials
        payload = verify_token(token)
        if payload:
            email = payload.get("sub")
            user_id = payload.get("id")
            session_id = payload.get("session_id")
            
            if user_id and session_id:
                if await is_session_active(user_id, session_id):
                    return payload
            elif email:
                return payload

    raise HTTPException(status_code=401, detail="Unauthorized")

async def admin_guard(user_payload = Depends(auth_guard)):
    if user_payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user_payload
