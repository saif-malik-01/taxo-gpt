import json
import logging

logger = logging.getLogger(__name__)

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from services.database import get_redis, AsyncSessionLocal
import secrets
import string
from sqlalchemy.orm import selectinload
from services.models import ChatSession, ChatMessage, UserProfile, User, SharedMessage
from api.config import settings

# Key prefixes
SESSION_KEY = "session:{}:history"

async def get_session_history(session_id: str, limit: int = 50):
    redis = await get_redis()
    key = SESSION_KEY.format(session_id)
    try:
        # Try Redis first
        cached_history = await redis.lrange(key, 0, -1)
        if cached_history:
            return [json.loads(msg) for msg in cached_history]
    except Exception as e:
        logger.warning(f"Redis error in get_session_history: {e}")
    
    # Fallback to DB (and populate Redis)
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(ChatMessage)
            .where(ChatMessage.session_id == session_id)
            .order_by(ChatMessage.timestamp.desc())
            .limit(limit)
        )
        messages = result.scalars().all()
        # Sort back to chronological for chat context
        messages = sorted(messages, key=lambda x: x.timestamp)
        
        history = [{"id": m.id, "role": m.role, "content": m.content} for m in messages]
        
        # Populate Redis (Push all)
        if history:
            try:
                await redis.rpush(key, *[json.dumps(m) for m in history])
                await redis.expire(key, 3600) # 1 hour TTL
            except Exception as e:
                logger.warning(f"Failed to populate Redis: {e}")
            
        return history

async def add_message(session_id: str, role: str, content: str, user_id: int = None):
    # 1. Update DB First to get the ID
    async with AsyncSessionLocal() as db:
        # Check if session exists, if not create it (only if user_id is provided)
        if user_id:
             # Fast check if session exists
            session_exists = await db.execute(select(ChatSession.id).where(ChatSession.id == session_id))
            if not session_exists.scalar():
                new_session = ChatSession(id=session_id, user_id=user_id, title=content[:30])
                db.add(new_session)
                await db.commit()

        new_msg = ChatMessage(session_id=session_id, role=role, content=content)
        db.add(new_msg)
        await db.commit()
        await db.refresh(new_msg)
        
        msg_id = new_msg.id

    # 2. Update Redis
    try:
        redis = await get_redis()
        key = SESSION_KEY.format(session_id)
        msg_obj = {"id": msg_id, "role": role, "content": content}
        await redis.rpush(key, json.dumps(msg_obj))
        await redis.expire(key, 3600)
    except Exception as e:
        logger.warning(f"Redis error in add_message: {e}")
    
    return new_msg

async def delete_session(session_id: str):
    # 1. Delete from Redis
    try:
        redis = await get_redis()
        key = SESSION_KEY.format(session_id)
        await redis.delete(key)
    except Exception as e:
        logger.warning(f"Redis error in delete_session: {e}")
    
    # 2. Delete from Postgres
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ChatSession).where(ChatSession.id == session_id))
        session = result.scalar_one_or_none()
        if session:
            await db.delete(session)
            await db.commit()
            return True
    return False

async def get_user_profile(user_id: int):
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(UserProfile).where(UserProfile.user_id == user_id))
        return result.scalar_one_or_none()

def generate_share_id(length=12):
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for i in range(length))

async def share_message(message_id: int, db: AsyncSession):
    """Creates a shared link record for a message."""
    # Check if already shared
    existing = await db.execute(select(SharedMessage).where(SharedMessage.message_id == message_id))
    shared = existing.scalar_one_or_none()
    
    if shared:
        return shared.id
        
    # Create new shared record
    shared_id = generate_share_id()
    new_shared = SharedMessage(id=shared_id, message_id=message_id)
    db.add(new_shared)
    await db.commit()
    return shared_id

async def get_shared_message(shared_id: str, db: AsyncSession):
    """Retrieves a message and its session context for a public shared link."""
    result = await db.execute(
        select(SharedMessage)
        .options(selectinload(SharedMessage.message).selectinload(ChatMessage.session))
        .where(SharedMessage.id == shared_id)
    )
    shared = result.scalar_one_or_none()
    if not shared:
        return None
    return shared.message
