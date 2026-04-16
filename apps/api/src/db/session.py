from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
import redis.asyncio as redis
import json
import time
from datetime import datetime
from apps.api.src.core.config import settings

# --- Postgres ---
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_size=40,
    max_overflow=20,
    pool_timeout=40,
    pool_recycle=1800,
    pool_pre_ping=True, 
)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

# --- Redis / Valkey ---
redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)

async def get_redis():
    return redis_client

# Real-time session decay is handled by replacement or explicit logout.
# We keep a safety TTL of 7 days to match the Refresh Token life.
SESSION_SAFETY_TTL = settings.REFRESH_TOKEN_EXPIRE_DAYS * 86400 

async def add_session(user_id: int, session_id: str, max_sessions: int, metadata: dict = None):
    now = time.time()
    zset_key = f"user_sessions_zset:{user_id}"
    meta_key = f"session_meta:{session_id}"

    # 1. Netflix-Style Proactive Eviction
    current_count = await redis_client.zcard(zset_key)
    if current_count >= max_sessions:
        # Find the oldest session (lowest score)
        oldest = await redis_client.zrange(zset_key, 0, 0)
        if oldest:
            old_sid = oldest[0]
            # Notify the kicked session via WebSocket before deleting
            from apps.api.src.services.auth.ws_manager import manager
            await manager.notify_session(old_sid, {
                "type": "SESSION_EVICTED", 
                "reason": "Logged in from another device"
            })
            # Remove from Redis
            await remove_session(user_id, old_sid)

    # 2. Add new session
    async with redis_client.pipeline(transaction=True) as pipe:
        pipe.zadd(zset_key, {session_id: now})
        pipe.expire(zset_key, SESSION_SAFETY_TTL)
        
        meta = metadata or {}
        meta["session_id"] = session_id
        meta["created_at"] = datetime.utcnow().isoformat()
        pipe.setex(meta_key, SESSION_SAFETY_TTL, json.dumps(meta))
        
        await pipe.execute()

async def is_session_active(user_id: int, session_id: str) -> bool:
    zset_key = f"user_sessions_zset:{user_id}"
    score = await redis_client.zscore(zset_key, session_id)
    return score is not None

async def heartbeat_session(user_id: int, session_id: str):
    """Updates the session's 'last active' timestamp."""
    now = time.time()
    zset_key = f"user_sessions_zset:{user_id}"
    meta_key = f"session_meta:{session_id}"
    
    if await is_session_active(user_id, session_id):
        async with redis_client.pipeline(transaction=True) as pipe:
            pipe.zadd(zset_key, {session_id: now})
            pipe.expire(zset_key, SESSION_SAFETY_TTL)
            pipe.expire(meta_key, SESSION_SAFETY_TTL)
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
    zset_key = f"user_sessions_zset:{user_id}"
    active_ids = await redis_client.zrange(zset_key, 0, -1)
    sessions = []
    for sid in active_ids:
        meta_json = await redis_client.get(f"session_meta:{sid}")
        if meta_json:
            sessions.append(json.loads(meta_json))
    return sessions
