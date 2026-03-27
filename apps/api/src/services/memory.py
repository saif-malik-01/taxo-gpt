import json
import logging
import secrets
import string
from sqlalchemy import select, func as sa_func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from starlette.concurrency import run_in_threadpool

from apps.api.src.db.session import get_redis, AsyncSessionLocal
from apps.api.src.db.models.base import ChatSession, ChatMessage, UserProfile, User, SharedSession, UserUsage, CreditLog
from apps.api.src.core.config import settings

try:
    from apps.api.src.services.email import EmailService
except ImportError:
    class EmailService:
        @staticmethod
        def send_low_credit_notification(*args, **kwargs): pass

logger = logging.getLogger(__name__)
SESSION_KEY = "session:{}:history"

async def get_session_history(session_id: str, limit: int = 50):
    """
    Returns history. If source_ids are present, hydrates them from Qdrant.
    """
    from apps.api.src.services.rag.retrieval.hydrator import hydrate_sources
    from apps.api.src.services.chat.engine import get_pipeline
    
    redis = await get_redis()
    key = SESSION_KEY.format(session_id)
    try:
        cached_history = await redis.lrange(key, 0, -1)
        if cached_history: 
            return [json.loads(msg) for msg in cached_history]
    except Exception as e: 
        logger.warning(f"Redis error: {e}")

    async with AsyncSessionLocal() as db:
        res = await db.execute(
            select(ChatMessage)
            .where(ChatMessage.session_id == session_id)
            .order_by(ChatMessage.timestamp.desc())
            .limit(limit)
        )
        messages = sorted(res.scalars().all(), key=lambda x: x.timestamp)
        
        history = []
        for m in messages:
            msg_dict = {
                "id": m.id, 
                "role": m.role, 
                "content": m.content
            }
            if m.source_ids:
                # Hydrate sources for this message
                pipeline = await run_in_threadpool(get_pipeline)
                sources = await hydrate_sources(m.source_ids, pipeline._qdrant)
                msg_dict["sources"] = sources
            
            history.append(msg_dict)
        logger.warning(f"Redis error {history}")
    

        if history:
            try:
                await redis.rpush(key, *[json.dumps(m) for m in history])
                await redis.expire(key, 3600)
            except Exception as e: 
                logger.warning(f"Redis cache push error: {e}")
        return history

async def add_message(
    session_id: str, 
    role: str, 
    content: str, 
    user_id: int = None, 
    chat_mode: str = None, 
    prompt_tokens: int = 0, 
    response_tokens: int = 0,
    source_ids: list = None # New field
):
    async with AsyncSessionLocal() as db:
        if user_id:
            exists = await db.execute(select(ChatSession.id).where(ChatSession.id == session_id))
            if not exists.scalar():
                stype = chat_mode or ("draft" if "[Documents:" in content else "simple")
                db.add(ChatSession(id=session_id, user_id=user_id, title=content[:30], session_type=stype))
                await db.commit()
        
        new_msg = ChatMessage(
            session_id=session_id, 
            role=role, 
            content=content, 
            prompt_tokens=prompt_tokens, 
            response_tokens=response_tokens,
            source_ids=source_ids # Save IDs
        )
        db.add(new_msg)
        await db.commit()
        await db.refresh(new_msg)
        
        # We don't cache individual messages in the list key here to avoid complexity
        # The list key in Redis is purged when get_session_history is called or when it expires.
        # However, for performance we often purge the Redis key on new messages.
        try:
            redis = await get_redis()
            await redis.delete(SESSION_KEY.format(session_id))
        except Exception: pass
        
        return new_msg

async def get_user_profile(user_id: int):
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(UserProfile).where(UserProfile.user_id == user_id))
        return res.scalar_one_or_none()

async def track_usage(user_id: int, session_id: str, db: AsyncSession, usage: dict = None, force_deduct: bool = False):
    res = await db.execute(select(UserUsage).where(UserUsage.user_id == user_id))
    user_usage = res.scalars().first()
    if not user_usage:
        user_usage = UserUsage(user_id=user_id)
        db.add(user_usage)
        await db.flush()

    if usage:
        user_usage.total_tokens_used = (user_usage.total_tokens_used or 0) + usage.get("totalTokens", 0)

    if force_deduct:
        # Credit deduction logic simplified for now
        user_usage.simple_query_used = (user_usage.simple_query_used or 0) + 1
    
    await db.commit()

async def check_credits(user_id: int, session_id: str, has_files: bool, db: AsyncSession, chat_mode: str = None, extra_tokens: int = 0):
    res = await db.execute(select(UserUsage).where(UserUsage.user_id == user_id))
    usage = res.scalars().first()
    if not usage:
        usage = UserUsage(user_id=user_id)
        db.add(usage)
        await db.commit()
    
    # Static check for simple queries for now
    if usage.simple_query_balance <= 0:
        return False, "Insufficient balance."

    return True, None
