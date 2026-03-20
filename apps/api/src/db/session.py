from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
import redis.asyncio as redis
import json
from apps.api.src.core.config import settings

# --- Postgres ---
engine = create_async_engine(settings.DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

# --- Redis ---
redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)

async def get_redis():
    return redis_client

async def add_session(user_id: int, session_id: str, max_sessions: int):
    """
    Adds a new session for the user and enforces the max_sessions limit.
    """
    key = f"user_sessions:{user_id}"
    current_count = await redis_client.llen(key)
    
    if current_count >= max_sessions:
        raise ValueError(f"Limit reached: You already have {max_sessions} active session(s).")

    await redis_client.rpush(key, session_id)
    await redis_client.ltrim(key, -max_sessions, -1)
    await redis_client.expire(key, 7200)

async def is_session_active(user_id: int, session_id: str) -> bool:
    key = f"user_sessions:{user_id}"
    sessions = await redis_client.lrange(key, 0, -1)
    if sessions:
        await redis_client.expire(key, 7200)
    return session_id in sessions

async def remove_session(user_id: int, session_id: str):
    key = f"user_sessions:{user_id}"
    await redis_client.lrem(key, 0, session_id)
