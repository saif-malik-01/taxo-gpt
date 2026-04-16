from datetime import datetime, timedelta, timezone
from jose import jwt, JWTError, ExpiredSignatureError
import asyncio
from apps.api.src.core.config import settings

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire, "type": "access"})
    return jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)

def create_refresh_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    return jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)

def verify_token(token: str, expected_type: str = "access"):
    try:
        payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        
        # Verify token type to prevent using Refresh Token as Access Token
        if payload.get("type") != expected_type:
            return None
            
        return payload
    except ExpiredSignatureError:
        # For Access tokens, we might want to attempt cleanup if the session is dead
        if expected_type == "access":
            try:
                dead_payload = jwt.decode(
                    token,
                    settings.JWT_SECRET_KEY,
                    algorithms=[settings.JWT_ALGORITHM],
                    options={"verify_exp": False}
                )
                user_id = dead_payload.get("id")
                session_id = dead_payload.get("session_id")

                if user_id and session_id:
                    from apps.api.src.db.session import remove_session
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            asyncio.ensure_future(remove_session(user_id, session_id))
                    except Exception:
                        pass
            except Exception:
                pass
        return None
    except JWTError:
        return None
