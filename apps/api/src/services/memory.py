import json
import logging
import secrets
import string
from datetime import datetime, timezone, timedelta
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
                pipeline = await get_pipeline()
                sources = await hydrate_sources(m.source_ids, pipeline._qdrant)
                msg_dict["sources"] = sources
            
            if m.attachments:
                msg_dict["attachments"] = m.attachments
            
            history.append(msg_dict)

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
    source_ids: list = None,
    attachments: list = None # New field: [{"filename": "...", "s3_key": "..."}]
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
            source_ids=source_ids, # Save IDs
            attachments=attachments # Save attachments
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

async def track_usage(user_id: int, session_id: str, db: AsyncSession = None, usage: dict = None, force_deduct: bool = False, chat_mode: str = "simple"):
    if db is None:
        async with AsyncSessionLocal() as session:
            return await track_usage(user_id, session_id, session, usage, force_deduct, chat_mode)

    res = await db.execute(
        select(UserUsage)
        .options(selectinload(UserUsage.user))
        .where(UserUsage.user_id == user_id)
    )
    user_usage = res.scalars().first()
    if not user_usage:
        from apps.api.src.services.payments import initialize_user_credits
        user_usage = await initialize_user_credits(user_id, db)

    # --- Token tracking (lifetime analytics + monthly window) ---
    if usage:
        input_t  = usage.get("inputTokens",  0)
        output_t = usage.get("outputTokens", 0)
        total_t  = usage.get("totalTokens",  input_t + output_t)
        # Lifetime accumulators
        user_usage.input_tokens_used  = (user_usage.input_tokens_used  or 0) + input_t
        user_usage.output_tokens_used = (user_usage.output_tokens_used or 0) + output_t
        user_usage.total_tokens_used  = (user_usage.total_tokens_used  or 0) + total_t
        # Monthly rolling window (for abuse guard)
        user_usage.monthly_tokens_used = (user_usage.monthly_tokens_used or 0) + total_t

    # --- Per-query balance deduction ---
    if force_deduct:
        if chat_mode == "draft":
            user_usage.draft_reply_used = (user_usage.draft_reply_used or 0) + 1
            user_usage.draft_reply_balance = max(0, (user_usage.draft_reply_balance or 0) - 1)
            logger.info(f"Deducted 1 DRAFT credit for user {user_id}. Remaining: {user_usage.draft_reply_balance}")
        else:
            user_usage.simple_query_used = (user_usage.simple_query_used or 0) + 1
            user_usage.simple_query_balance = max(0, (user_usage.simple_query_balance or 0) - 1)
            logger.info(f"Deducted 1 SIMPLE credit for user {user_id}. Remaining: {user_usage.simple_query_balance}")
        
        # --- Low credit notification (Target: specifically 1 credit left) ---
        if user_usage.simple_query_balance == 1 and user_usage.user:
            try:
                # We run this in a threadpool to avoid blocking the async flow if SMTP is slow
                await run_in_threadpool(
                    EmailService.send_low_credit_notification,
                    email=user_usage.user.email,
                    balance=1,
                    credit_type="tax intelligence",
                    full_name=user_usage.user.full_name
                )
                logger.info(f"Low credit notification sent to user {user_id}")
            except Exception as e:
                logger.error(f"Failed to send low credit notification: {e}")

    await db.commit()

async def check_credits(user_id: int, session_id: str, db: AsyncSession = None, chat_mode: str = "simple", extra_tokens: int = 0):
    if db is None:
        async with AsyncSessionLocal() as session:
            return await check_credits(user_id, session_id, session, chat_mode, extra_tokens)

    res = await db.execute(select(UserUsage).where(UserUsage.user_id == user_id))
    usage = res.scalars().first()
    if not usage:
        from apps.api.src.services.payments import initialize_user_credits
        usage = await initialize_user_credits(user_id, db)
        await db.commit()

    # --- Guard 0: Expiry check ---
    now = datetime.now(timezone.utc)
    # Ensure current timestamp is tz-aware if the DB one is
    if usage.credits_expire_at:
        expire_at = usage.credits_expire_at
        if expire_at.tzinfo is None:
            expire_at = expire_at.replace(tzinfo=timezone.utc)
            
        if now > expire_at:
            # Credits have expired, wipe balances
            usage.simple_query_balance = 0
            usage.draft_reply_balance = 0
            await db.commit()
            return False, "Your credits have expired on {}. Please purchase a new package to continue.".format(expire_at.strftime("%Y-%m-%d"))

    # --- Guard 1: Balance check ---
    if chat_mode == "draft":
        if (usage.draft_reply_balance or 0) <= 0:
            return False, "Insufficient Draft Reply balance. Please upgrade your plan."
    else:
        if (usage.simple_query_balance or 0) <= 0:
            return False, "Insufficient Simple Query balance. Please upgrade your plan."

    # --- Guard 2: Monthly token abuse guard (lazy 30-day rolling window reset) ---
    now = datetime.now(timezone.utc)
    reset_date = usage.monthly_reset_date
    # Normalise to UTC if DB returns a naive datetime
    if reset_date is not None and reset_date.tzinfo is None:
        reset_date = reset_date.replace(tzinfo=timezone.utc)

    if reset_date is None or (now - reset_date) >= timedelta(days=30):
        # New month window — reset the counter
        usage.monthly_tokens_used = 0
        usage.monthly_reset_date  = now
        await db.commit()
        logger.info(f"Monthly token window reset for user {user_id}")
    elif (usage.monthly_tokens_used or 0) >= settings.GLOBAL_MONTHLY_TOKEN_LIMIT:
        limit_m = settings.GLOBAL_MONTHLY_TOKEN_LIMIT // 1_000_000
        next_reset = reset_date + timedelta(days=30)
        days_left = max(0, (next_reset - now).days)
        return False, (
            f"You have reached the monthly usage limit of {limit_m}M tokens. "
            f"Your limit resets in {days_left} day(s)."
        )

    # --- Guard 3: Per-session token cap (FUP) ---
    # Token limit: 100K for simple chat, 60K for draft sessions.
    token_limit = (
        settings.SESSION_TOKEN_LIMIT_DRAFT
        if chat_mode == "draft"
        else settings.SESSION_TOKEN_LIMIT_SIMPLE
    )
    session_token_res = await db.execute(
        select(
            sa_func.coalesce(
                sa_func.sum(ChatMessage.prompt_tokens + ChatMessage.response_tokens), 0
            )
        ).where(ChatMessage.session_id == session_id)
    )
    session_tokens_used = session_token_res.scalar() or 0

    if session_tokens_used >= token_limit:
        limit_k = token_limit // 1000
        return False, (
            f"This session has reached its {limit_k}K token limit. "
            "Please start a new session to continue."
        )

    return True, None

async def edit_message_and_truncate(session_id: str, message_id: int, new_content: str):
    """
    Updates a message's content and deletes all subsequent messages in the session.
    """
    from sqlalchemy import delete
    async with AsyncSessionLocal() as db:
        # 1. Update the message
        res = await db.execute(select(ChatMessage).where(ChatMessage.id == message_id))
        msg = res.scalars().first()
        if msg:
            msg.content = new_content
            # 2. Delete all messages created AFTER this one in the same session
            # We assume message IDs are sequential for a session.
            await db.execute(
                delete(ChatMessage).where(
                    ChatMessage.session_id == session_id,
                    ChatMessage.id > message_id
                )
            )
            await db.commit()
            
            # 3. Invalidate Redis cache
            try:
                redis = await get_redis()
                await redis.delete(SESSION_KEY.format(session_id))
            except Exception as e:
                logger.warning(f"Redis cache delete error in edit: {e}")
        return msg
