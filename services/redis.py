import redis.asyncio as redis
from api.config import settings
import json

# Initialize Redis pool
redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)

async def add_session(user_id: int, session_id: str, max_sessions: int):
    """
    Adds a new session for the user and enforces the max_sessions limit.
    If the limit is reached, the new login attempt is rejected,
    preserving existing active sessions.
    """
    key = f"user_sessions:{user_id}"
    
    # Check current number of active sessions
    current_count = await redis_client.llen(key)
    
    if current_count >= max_sessions:
        # Block the new login attempt
        raise ValueError(f"Academic/Work limit reached: You already have {max_sessions} active session(s). Please log out from another device/browser to log in here.")

    # Add new session to the end of the list
    await redis_client.rpush(key, session_id)
    
    # Safety trim
    await redis_client.ltrim(key, -max_sessions, -1)
    
    # Set a TTL of 2 hours for the session list (renewed on every action)
    # This prevents permanent lockouts if user closes browser without logout
    await redis_client.expire(key, 7200)

async def is_session_active(user_id: int, session_id: str) -> bool:
    """
    Checks if a session_id exists in the user's active session list.
    """
    key = f"user_sessions:{user_id}"
    sessions = await redis_client.lrange(key, 0, -1)
    
    # Renew TTL on every active check (action)
    if sessions:
        await redis_client.expire(key, 7200)
        
    return session_id in sessions

async def remove_session(user_id: int, session_id: str):
    """
    Removes a specific session (logout).
    """
    key = f"user_sessions:{user_id}"
    await redis_client.lrem(key, 0, session_id)
