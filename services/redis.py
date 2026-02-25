import redis.asyncio as redis
from api.config import settings
import json

# Initialize Redis pool
redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)

async def add_session(user_id: int, session_id: str, max_sessions: int):
    """
    Adds a new session for the user and enforces the max_sessions limit.
    Uses a Redis List to track sessions for each user.
    """
    key = f"user_sessions:{user_id}"
    
    # Add new session to the end of the list
    await redis_client.rpush(key, session_id)
    
    # Enforce limit: keep only the last `max_sessions`
    # LTRIM keeps elements from index -max_sessions to -1 (the end)
    # If max_sessions is 1, it keeps only the last added session.
    await redis_client.ltrim(key, -max_sessions, -1)

async def is_session_active(user_id: int, session_id: str) -> bool:
    """
    Checks if a session_id exists in the user's active session list.
    """
    key = f"user_sessions:{user_id}"
    sessions = await redis_client.lrange(key, 0, -1)
    return session_id in sessions

async def remove_session(user_id: int, session_id: str):
    """
    Removes a specific session (logout).
    """
    key = f"user_sessions:{user_id}"
    await redis_client.lrem(key, 0, session_id)
