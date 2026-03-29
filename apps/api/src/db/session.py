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

import time
import json
from datetime import datetime

SESSION_TIMEOUT_SECONDS = 300 # 5 minutes TTL for ghost sessions

async def add_session(user_id: int, session_id: str, max_sessions: int, metadata: dict = None):
    now = time.time()
    zset_key = f"user_sessions_zset:{user_id}"
    meta_key = f"session_meta:{session_id}"

    # 1. Self-healing DB: Clean up stale ghost sessions first
    valid_threshold = now - SESSION_TIMEOUT_SECONDS
    await redis_client.zremrangebyscore(zset_key, "-inf", valid_threshold)

    # 2. Strict Limit Check Policy
    current_count = await redis_client.zcard(zset_key)
    if current_count >= max_sessions:
        raise ValueError(f"Account Limit Reached: Ensure other devices are logged out. Maximum {max_sessions} active employee(s) allowed.")

    # 3. Add new session and register heartbeat timestamp
    async with redis_client.pipeline(transaction=True) as pipe:
        # Add to Active pulse stack
        pipe.zadd(zset_key, {session_id: now})
        # Keep ZSET expiring completely if user abandons it
        pipe.expire(zset_key, SESSION_TIMEOUT_SECONDS * 2)
        
        # Save IP, Device Name info
        meta = metadata or {}
        meta["session_id"] = session_id
        meta["created_at"] = datetime.utcnow().isoformat()
        pipe.setex(meta_key, SESSION_TIMEOUT_SECONDS, json.dumps(meta))
        
        await pipe.execute()

async def is_session_active(user_id: int, session_id: str) -> bool:
    zset_key = f"user_sessions_zset:{user_id}"
    
    score = await redis_client.zscore(zset_key, session_id)
    if not score:
        return False
    
    # If the last heartbeat was longer ago than the timeout, it's a dead session
    if score < (time.time() - SESSION_TIMEOUT_SECONDS):
        return False
        
    return True

async def heartbeat_session(user_id: int, session_id: str):
    """
    Called by the frontend every ~2 minutes.
    Updates the session's 'last active' timestamp to keep it fiercely alive.
    """
    now = time.time()
    zset_key = f"user_sessions_zset:{user_id}"
    meta_key = f"session_meta:{session_id}"
    
    if await is_session_active(user_id, session_id):
        async with redis_client.pipeline(transaction=True) as pipe:
            pipe.zadd(zset_key, {session_id: now})
            pipe.expire(zset_key, SESSION_TIMEOUT_SECONDS * 2)
            pipe.expire(meta_key, SESSION_TIMEOUT_SECONDS)
            await pipe.execute()
        return True
    return False

async def remove_session(user_id: int, session_id: str):
    zset_key = f"user_sessions_zset:{user_id}"
    meta_key = f"session_meta:{session_id}"
    async with redis_client.pipeline(transaction=True) as pipe:
        pipe.zrem(zset_key, session_id)
        pipe.delete(meta_key)
        await pipe.execute()

async def list_sessions(user_id: int) -> list:
    """
    Gets the currently pulsing humans.
    """
    now = time.time()
    zset_key = f"user_sessions_zset:{user_id}"
    valid_threshold = now - SESSION_TIMEOUT_SECONDS
    
    # Clean up so UI doesn't show old dead sessions
    await redis_client.zremrangebyscore(zset_key, "-inf", valid_threshold)
    
    active_ids = await redis_client.zrange(zset_key, 0, -1)
    sessions = []
    
    for sid in active_ids:
        meta_json = await redis_client.get(f"session_meta:{sid}")
        if meta_json:
            sessions.append(json.loads(meta_json))
            
    return sessions
