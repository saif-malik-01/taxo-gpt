from fastapi import Header, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from services.auth.jwt import verify_token

API_KEY = "gst-secret-123"
security = HTTPBearer(auto_error=False)


def auth_guard(
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
            return payload

    raise HTTPException(status_code=401, detail="Unauthorized")
