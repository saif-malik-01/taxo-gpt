import uuid
import json
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Form, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from apps.api.src.db.session import get_db
from apps.api.src.db.models.base import ChatSession, ChatMessage, Feedback, SharedSession
from sqlalchemy import select, delete, desc
from apps.api.src.services.memory import (
    check_credits, track_usage, get_session_history, 
    add_message, get_user_profile, edit_message_and_truncate
)
from apps.api.src.services.auth.deps import auth_guard
from apps.api.src.services.chat.engine import chat_stream
from apps.api.src.services.chat.memory_updater import auto_update_profile
from apps.api.src.schemas.chat import ChatRequest

router = APIRouter(tags=["Chat"])
logger = logging.getLogger(__name__)

@router.get("/sessions")
async def list_sessions(user=Depends(auth_guard), db: AsyncSession = Depends(get_db)):
    user_id = user.get("id")
    res = await db.execute(
        select(ChatSession).where(ChatSession.user_id == user_id).order_by(desc(ChatSession.created_at))
    )
    sessions = res.scalars().all()
    return [{
        "id": s.id, 
        "title": s.title, 
        "session_type": s.session_type, 
        "created_at": s.created_at
    } for s in sessions]

@router.get("/sessions/{session_id}/history")
async def get_history(session_id: str, user=Depends(auth_guard)):
    # Basic auth check - ensure session belongs to user (optional but recommended)
    history = await get_session_history(session_id)
    return history

@router.delete("/sessions/{session_id}")
async def delete_session(session_id: str, user=Depends(auth_guard), db: AsyncSession = Depends(get_db)):
    user_id = user.get("id")
    
    # Manually delete related records first to avoid foreign key violations
    # if database-level cascade is missing.
    await db.execute(
        delete(SharedSession).where(SharedSession.session_id == session_id)
    )
    
    # Feedback is linked to messages, so we delete messages (which should cascade through ORM
    # but for direct DELETE we need to be careful. However, ChatMessage has cascade="all, delete-orphan"
    # only for objects. For direct SQL DELETE, we must check if Feedback needs manual deletion)
    # Let's just do it sequentially for safety.
    
    # Subquery to find all message IDs for this session
    message_ids_query = select(ChatMessage.id).where(ChatMessage.session_id == session_id)
    message_ids_res = await db.execute(message_ids_query)
    msg_ids = message_ids_res.scalars().all()
    
    if msg_ids:
        await db.execute(
            delete(Feedback).where(Feedback.message_id.in_(msg_ids))
        )
        await db.execute(
            delete(ChatMessage).where(ChatMessage.id.in_(msg_ids))
        )

    # Finally delete the session
    await db.execute(
        delete(ChatSession).where(ChatSession.id == session_id, ChatSession.user_id == user_id)
    )
    await db.commit()
    return {"status": "deleted"}

@router.post("/chat/ask/stream/simple")
async def ask_gst_stream_simple(
    payload: ChatRequest,
    background_tasks: BackgroundTasks,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    question = payload.question
    session_id = payload.session_id or str(uuid.uuid4())
    user_id = user.get("id")

    allowed, error_msg = await check_credits(user_id, session_id, False, db, chat_mode="simple")
    if not allowed:
        raise HTTPException(status_code=402, detail=error_msg)

    async def stream_generator():
        try:
            # ── Handle Message (New vs Edit) ──────────────────────────────
            if payload.message_id:
                await edit_message_and_truncate(session_id, payload.message_id, question)
            else:
                await add_message(session_id, "user", question, user_id)

            history = await get_session_history(session_id)

            profile = await get_user_profile(user_id)
            profile_summary = profile.dynamic_summary if profile else None

            # ── Stream Response ────────────────────────────────────────────────
            full_response = ""
            source_ids = []
            llm_usage = {}  # real token counts from Bedrock (captured below)
            async for event in chat_stream(
                query=question, 
                history=history, 
                profile_summary=profile_summary
            ):
                if event.get("type") == "content":
                    full_response += event.get("delta", "")
                elif event.get("type") == "retrieval":
                    # Capture source IDs for hydration
                    for s in event.get("sources", []):
                        if s.get("chunk_id"):
                            source_ids.append(s["chunk_id"])
                    # Capture real token usage from Bedrock metadata
                    llm_usage = event.get("usage", {})
                
                yield json.dumps(event) + "\n"

            # ── Save bot message with real token counts ─────────────────────
            if full_response:
                bot_msg = await add_message(
                    session_id, "bot", full_response, user_id,
                    prompt_tokens=llm_usage.get("inputTokens", 0),
                    response_tokens=llm_usage.get("outputTokens", 0),
                    source_ids=source_ids,
                )
                yield json.dumps({
                    "type": "completion", 
                    "session_id": session_id,
                    "message_id": bot_msg.id
                }) + "\n"

            # ── Track usage: deduct 1 credit, log tokens ────────────────────
            # force_deduct=True: every question deducts 1 from simple_query_balance
            await track_usage(user_id, session_id, db, usage=llm_usage, force_deduct=True)

            # ── Auto-update Profile (Background) ───────────────────────────────
            is_new_session = len(history) <= 1
            if is_new_session:
                background_tasks.add_task(auto_update_profile, user_id, question, db)

        except Exception as e:
            logger.error(f"Stream error: {e}", exc_info=True)
            yield json.dumps({"type": "error", "message": str(e)}) + "\n"

    return StreamingResponse(stream_generator(), media_type="application/x-ndjson")

@router.post("/chat/ask/stream/draft")
async def ask_gst_stream_draft(
    payload: ChatRequest,
    background_tasks: BackgroundTasks,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    question = payload.question
    session_id = payload.session_id or str(uuid.uuid4())
    user_id = user.get("id")

    allowed, error_msg = await check_credits(user_id, session_id, True, db, chat_mode="draft")
    if not allowed:
        raise HTTPException(status_code=402, detail=error_msg)

    async def stream_generator():
        try:
            if payload.message_id:
                await edit_message_and_truncate(session_id, payload.message_id, question)
            else:
                await add_message(session_id, "user", question, user_id, chat_mode="draft")
            
            history = await get_session_history(session_id)
            is_new_session = len(history) <= 1
            await track_usage(user_id, session_id, db, force_deduct=True) # Draft always deducts

            profile = await get_user_profile(user_id)
            profile_summary = profile.dynamic_summary if profile else None

            full_response = ""
            source_ids = []
            async for event in chat_stream(
                query=question, 
                history=history, 
                profile_summary=profile_summary
            ):
                if event.get("type") == "content":
                    full_response += event.get("delta", "")
                elif event.get("type") == "retrieval":
                    for s in event.get("sources", []):
                        if s.get("chunk_id"):
                            source_ids.append(s["chunk_id"])
                yield json.dumps(event) + "\n"

            if full_response:
                bot_msg = await add_message(session_id, "bot", full_response, user_id, chat_mode="draft", source_ids=source_ids)
                yield json.dumps({"type": "completion", "session_id": session_id, "message_id": bot_msg.id}) + "\n"

        except Exception as e:
            logger.error(f"Draft stream error: {e}", exc_info=True)
            yield json.dumps({"type": "error", "message": str(e)}) + "\n"

    return StreamingResponse(stream_generator(), media_type="application/x-ndjson")

class FeedbackRequest(BaseModel):
    message_id: int
    rating: int
    comment: Optional[str] = None

@router.post("/sessions/feedback")
async def give_feedback(payload: FeedbackRequest, user=Depends(auth_guard), db: AsyncSession = Depends(get_db)):
    # Verify message exists
    res = await db.execute(select(ChatMessage).where(ChatMessage.id == payload.message_id))
    msg = res.scalars().first()
    if not msg: raise HTTPException(status_code=404, detail="Message not found")
    
    # Update or create feedback
    res = await db.execute(select(Feedback).where(Feedback.message_id == payload.message_id))
    existing = res.scalars().first()
    if existing:
        existing.rating = payload.rating
        existing.comment = payload.comment
    else:
        db.add(Feedback(message_id=payload.message_id, rating=payload.rating, comment=payload.comment))
    
    await db.commit()
    return {"status": "success"}

@router.post("/chat/share/session/{session_id}")
async def share_session(session_id: str, user=Depends(auth_guard), db: AsyncSession = Depends(get_db)):
    # Verify session belongs to user
    user_id = user.get("id")
    res = await db.execute(select(ChatSession).where(ChatSession.id == session_id, ChatSession.user_id == user_id))
    session = res.scalars().first()
    if not session: raise HTTPException(status_code=404, detail="Session not found")
    
    # Create shared link entry
    shared_id = str(uuid.uuid4())[:8] # Short ID
    db.add(SharedSession(id=shared_id, session_id=session_id))
    await db.commit()
    return {"shared_id": shared_id, "url": f"/chat/share/{shared_id}"}

@router.get("/chat/share/{shared_id}")
async def get_shared_session(shared_id: str, db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(SharedSession).where(SharedSession.id == shared_id))
    shared = res.scalars().first()
    if not shared: raise HTTPException(status_code=404, detail="Shared session not found")
    
    history = await get_session_history(shared.session_id)
    return {"history": history}
