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
from services.email import EmailService

# Key prefixes
SESSION_KEY = "session:{}:history"

# Maximum messages kept in Redis per session.
# Older messages beyond this are trimmed — they remain in Postgres and
# are fetched from DB on cache miss, so nothing is lost permanently.
_REDIS_HISTORY_LIMIT = 100
_REDIS_TTL = 3600  # 1 hour


async def get_session_history(session_id: str, limit: int = 50):
    redis = await get_redis()
    key = SESSION_KEY.format(session_id)
    try:
        # Fetch only the last `limit` messages — avoids deserialising the full list
        cached_history = await redis.lrange(key, -limit, -1)
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
        messages = sorted(messages, key=lambda x: x.timestamp)

        history = [{"id": m.id, "role": m.role, "content": m.content} for m in messages]

        if history:
            try:
                await redis.rpush(key, *[json.dumps(m) for m in history])
                await redis.ltrim(key, -_REDIS_HISTORY_LIMIT, -1)
                await redis.expire(key, _REDIS_TTL)
            except Exception as e:
                logger.warning(f"Failed to populate Redis: {e}")

        return history


async def add_message(
    session_id: str,
    role: str,
    content: str,
    user_id: int = None,
    chat_mode: str = None,
    prompt_tokens: int = 0,
    response_tokens: int = 0,
):
    # 1. Write to DB first to get the auto-increment id
    async with AsyncSessionLocal() as db:
        if user_id:
            session_exists = await db.execute(
                select(ChatSession.id).where(ChatSession.id == session_id)
            )
            if not session_exists.scalar():
                session_type = "simple"
                if chat_mode:
                    session_type = chat_mode
                elif role == "user" and "[Documents:" in content:
                    session_type = "draft"

                new_session = ChatSession(
                    id=session_id,
                    user_id=user_id,
                    title=content[:30],
                    session_type=session_type,
                )
                db.add(new_session)
                await db.commit()
            elif chat_mode == "draft" or (role == "user" and "[Documents:" in content):
                current_type_res = await db.execute(
                    select(ChatSession.session_type).where(ChatSession.id == session_id)
                )
                current_type = current_type_res.scalar()
                if current_type != "draft":
                    await db.execute(
                        ChatSession.__table__.update()
                        .where(ChatSession.id == session_id)
                        .values(session_type="draft")
                    )
                    await db.commit()

        new_msg = ChatMessage(
            session_id=session_id,
            role=role,
            content=content,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
        )
        db.add(new_msg)
        await db.commit()
        await db.refresh(new_msg)
        msg_id = new_msg.id

    # 2. Append to Redis list and keep it trimmed
    try:
        redis = await get_redis()
        key = SESSION_KEY.format(session_id)
        msg_obj = {"id": msg_id, "role": role, "content": content}
        await redis.rpush(key, json.dumps(msg_obj))
        # Keep only the last _REDIS_HISTORY_LIMIT messages —
        # prevents the list growing without bound for long-running sessions.
        await redis.ltrim(key, -_REDIS_HISTORY_LIMIT, -1)
        await redis.expire(key, _REDIS_TTL)
    except Exception as e:
        logger.warning(f"Redis error in add_message: {e}")

    return new_msg


async def update_message_tokens(message_id: int, prompt_tokens: int, response_tokens: int):
    """Updates an existing message with token counts."""
    async with AsyncSessionLocal() as db:
        await db.execute(
            ChatMessage.__table__.update()
            .where(ChatMessage.id == message_id)
            .values(prompt_tokens=prompt_tokens, response_tokens=response_tokens)
        )
        await db.commit()


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
        result = await db.execute(
            select(ChatSession).where(ChatSession.id == session_id)
        )
        session = result.scalar_one_or_none()
        if session:
            await db.delete(session)
            await db.commit()
            return True
    return False


async def get_user_profile(user_id: int):
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(UserProfile).where(UserProfile.user_id == user_id)
        )
        return result.scalar_one_or_none()


def generate_share_id(length=12):
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


async def share_session(session_id: str, db: AsyncSession):
    existing = await db.execute(
        select(SharedSession).where(SharedSession.session_id == session_id)
    )
    shared = existing.scalar_one_or_none()
    if shared:
        return shared.id

    shared_id = generate_share_id()
    new_shared = SharedSession(id=shared_id, session_id=session_id)
    db.add(new_shared)
    await db.commit()
    return shared_id


async def get_shared_session(shared_id: str, db: AsyncSession):
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


async def track_usage(
    user_id: int,
    session_id: str,
    db: AsyncSession,
    usage: dict = None,
    force_deduct: bool = False,
):
    """
    Increments token usage and optionally deducts balance.
    Deduction only happens if force_deduct=True.
    """
    res = await db.execute(select(UserUsage).where(UserUsage.user_id == user_id))
    user_usage = res.scalars().first()

    if not user_usage:
        user_usage = UserUsage(user_id=user_id)
        db.add(user_usage)
        await db.flush()

    if usage:
        user_usage.input_tokens_used  = (user_usage.input_tokens_used  or 0) + usage.get("inputTokens",  0)
        user_usage.output_tokens_used = (user_usage.output_tokens_used or 0) + usage.get("outputTokens", 0)
        user_usage.total_tokens_used  = (user_usage.total_tokens_used  or 0) + usage.get("totalTokens",  0)

    if not force_deduct:
        await db.commit()
        return

    res = await db.execute(
        select(ChatSession.session_type).where(ChatSession.id == session_id)
    )
    session_type = res.scalar() or "simple"

    async def _handle_low_credit_alert(u_id: int, c_type: str, bal: int):
        if bal == 1:
            try:
                result = await db.execute(select(User).where(User.id == u_id))
                user_obj = result.scalars().first()
                if user_obj:
                    EmailService.send_low_credit_notification(
                        email=user_obj.email,
                        credit_type=c_type,
                        remaining_balance=bal,
                        full_name=user_obj.full_name,
                    )
                    logger.info(f"Low credit alert sent to {user_obj.email} for {c_type}")
            except Exception as e:
                logger.error(f"Failed to send low credit alert: {e}")

    if session_type == "draft":
        if user_usage.draft_reply_balance > 0:
            user_usage.draft_reply_balance -= 1
            await _handle_low_credit_alert(user_id, "draft", user_usage.draft_reply_balance)
            db.add(CreditLog(
                user_id=user_id,
                amount=-1,
                credit_type="draft",
                transaction_type="usage",
                reference_id=session_id,
            ))
        user_usage.draft_reply_used += 1
    else:
        if user_usage.simple_query_balance > 0:
            user_usage.simple_query_balance -= 1
            await _handle_low_credit_alert(user_id, "simple", user_usage.simple_query_balance)
            db.add(CreditLog(
                user_id=user_id,
                amount=-1,
                credit_type="simple",
                transaction_type="usage",
                reference_id=session_id,
            ))
        user_usage.simple_query_used += 1

    await db.commit()


async def check_credits(
    user_id: int,
    session_id: str,
    has_files: bool,
    db: AsyncSession,
    chat_mode: str = None,
    extra_tokens: int = 0,
):
    """Gatekeeper check before the LLM runs."""
    effective_type = "simple"
    if chat_mode:
        effective_type = chat_mode
    elif has_files:
        effective_type = "draft"
    else:
        if session_id:
            res = await db.execute(
                select(ChatSession.session_type).where(ChatSession.id == session_id)
            )
            effective_type = res.scalar() or "simple"

    res = await db.execute(select(UserUsage).where(UserUsage.user_id == user_id))
    usage = res.scalars().first()

    if not usage:
        usage = UserUsage(user_id=user_id)
        db.add(usage)
        await db.commit()
        await db.refresh(usage)
    else:
        await db.refresh(usage)

    if effective_type == "draft" and usage.draft_reply_balance <= 0:
        return False, "Insufficient Draft credits."

    if effective_type == "simple" and usage.simple_query_balance <= 0:
        return False, "Daily limit for simple queries reached."

    total_used = (usage.total_tokens_used or 0) + extra_tokens
    logger.info(
        f"FUP Check: user={user_id}, used={total_used}, "
        f"limit={settings.GLOBAL_MONTHLY_TOKEN_LIMIT}, extra={extra_tokens}"
    )

    if total_used > settings.GLOBAL_MONTHLY_TOKEN_LIMIT:
        return False, "Monthly limit exceeded."

    if session_id:
        from sqlalchemy import func as sa_func
        res = await db.execute(
            select(sa_func.sum(ChatMessage.prompt_tokens + ChatMessage.response_tokens))
            .where(ChatMessage.session_id == session_id)
        )
        session_tokens = (res.scalar() or 0) + extra_tokens
        limit = (
            settings.SESSION_TOKEN_LIMIT_DRAFT
            if effective_type == "draft"
            else settings.SESSION_TOKEN_LIMIT_SIMPLE
        )
        if session_tokens > limit:
            return False, "Session token limit hit."

    return True, None