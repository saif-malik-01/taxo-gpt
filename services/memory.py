import json
import logging

logger = logging.getLogger(__name__)

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from services.database import get_redis, AsyncSessionLocal
import secrets
import string
from sqlalchemy.orm import selectinload
from services.models import ChatSession, ChatMessage, UserProfile, User, SharedSession, UserUsage, CreditLog
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

async def add_message(session_id: str, role: str, content: str, user_id: int = None, chat_mode: str = None):
    # 1. Update DB First to get the ID
    async with AsyncSessionLocal() as db:
        # Check if session exists, if not create it (only if user_id is provided)
        if user_id:
             # Fast check if session exists
            session_exists = await db.execute(select(ChatSession.id).where(ChatSession.id == session_id))
            if not session_exists.scalar():
                session_type = "simple"
                if chat_mode:
                    session_type = chat_mode
                elif role == "user" and "[Documents:" in content:
                    session_type = "draft"
                    
                new_session = ChatSession(id=session_id, user_id=user_id, title=content[:30], session_type=session_type)
                db.add(new_session)
                await db.commit()
            elif chat_mode == "draft" or (role == "user" and "[Documents:" in content):
                # Upgrade existing session to draft if explicitly requested or a document is uploaded
                current_type_res = await db.execute(select(ChatSession.session_type).where(ChatSession.id == session_id))
                current_type = current_type_res.scalar()
                
                if current_type != "draft":
                    await db.execute(
                        ChatSession.__table__.update()
                        .where(ChatSession.id == session_id)
                        .values(session_type="draft")
                    )
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

async def share_session(session_id: str, db: AsyncSession):
    """Creates a shared link record for a session."""
    # Check if already shared
    existing = await db.execute(select(SharedSession).where(SharedSession.session_id == session_id))
    shared = existing.scalar_one_or_none()
    
    if shared:
        return shared.id
        
    # Create new shared record
    shared_id = generate_share_id()
    new_shared = SharedSession(id=shared_id, session_id=session_id)
    db.add(new_shared)
    await db.commit()
    return shared_id

async def get_shared_session(shared_id: str, db: AsyncSession):
    """Retrieves a session and its message history for a public shared link."""
    result = await db.execute(
        select(SharedSession)
        .options(
            selectinload(SharedSession.session).selectinload(ChatSession.messages)
        )
        .where(SharedSession.id == shared_id)
    )
    shared = result.scalar_one_or_none()
    if not shared:
        return None
    return shared.session

async def track_usage(user_id: int, session_id: str, db: AsyncSession):
    """
    Deducts balance and increments used counters based on session type.
    """
    # 1. Get session type
    res = await db.execute(select(ChatSession.session_type).where(ChatSession.id == session_id))
    session_type = res.scalar() or "simple"

    # 2. Update usage table (Atomic decrement/increment)
    res = await db.execute(select(UserUsage).where(UserUsage.user_id == user_id))
    usage = res.scalars().first()

    if not usage:
        usage = UserUsage(user_id=user_id)
        db.add(usage)
        await db.flush()

    if session_type == "draft":
        # Deduct balance (ensure it doesn't go below 0, though check_credits should handle this)
        if usage.draft_reply_balance > 0:
            usage.draft_reply_balance -= 1
            log = CreditLog(
                user_id=user_id,
                amount=-1,
                credit_type="draft",
                transaction_type="usage",
                reference_id=session_id
            )
            db.add(log)
        usage.draft_reply_used += 1
    else:
        # Simple queries are "unlimited" for now, but we still decrement balance to track
        if usage.simple_query_balance > 0:
            usage.simple_query_balance -= 1
            log = CreditLog(
                user_id=user_id,
                amount=-1,
                credit_type="simple",
                transaction_type="usage",
                reference_id=session_id
            )
            db.add(log)
        usage.simple_query_used += 1
    
    await db.commit()

async def check_credits(user_id: int, session_id: str, has_files: bool, db: AsyncSession, chat_mode: str = None):
    """
    Gatekeeper check before the LLM runs.
    """
    # 1. Determine effective session type for this request
    effective_type = "simple"
    if chat_mode:
        effective_type = chat_mode
    elif has_files:
        effective_type = "draft"
    else:
        # Check existing session type
        if session_id:
            res = await db.execute(select(ChatSession.session_type).where(ChatSession.id == session_id))
            effective_type = res.scalar() or "simple"

    # 2. Check balance
    res = await db.execute(select(UserUsage).where(UserUsage.user_id == user_id))
    usage = res.scalars().first()

    if not usage:
        usage = UserUsage(user_id=user_id)
        db.add(usage)
        await db.commit()
        await db.refresh(usage)

    if effective_type == "draft" and usage.draft_reply_balance <= 0:
        return False, "Insufficient Draft Reply credits. Please purchase a package."
    
    if effective_type == "simple" and usage.simple_query_balance <= 0:
        return False, "Daily limit for simple queries reached."

    return True, None
