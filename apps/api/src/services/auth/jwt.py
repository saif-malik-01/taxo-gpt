from datetime import datetime, timedelta
from jose import jwt, JWTError, ExpiredSignatureError
import asyncio
from apps.api.src.core.config import settings

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)

def verify_token(token: str):
    try:
        payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        return payload
    except ExpiredSignatureError:
        # JWT is dead — but its payload still contains user_id + session_id.
        # Decode it without verifying expiry so we can free the Redis slot.
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
                # Schedule the async cleanup without blocking this sync function
                from apps.api.src.db.session import remove_session
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.ensure_future(remove_session(user_id, session_id))
                except Exception:
                    pass  # Best-effort cleanup — never crash on this
        except Exception:
            pass
        return None  # Token is still invalid — caller gets 401
    except JWTError:
        return None
