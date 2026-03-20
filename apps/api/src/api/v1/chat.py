import uuid
import json
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Form, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from apps.api.src.db.session import get_db
from apps.api.src.services.memory import check_credits, track_usage, get_session_history, add_message, get_user_profile
from apps.api.src.services.auth.deps import auth_guard
from apps.api.src.services.chat.engine import chat_stream
from apps.api.src.services.chat.memory_updater import auto_update_profile

router = APIRouter(prefix="/chat", tags=["Chat"])
logger = logging.getLogger(__name__)

@router.post("/ask/stream/simple")
async def ask_gst_stream_simple(
    background_tasks: BackgroundTasks,
    question: str = Form(...),
    session_id: Optional[str] = Form(None),
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    session_id = session_id or str(uuid.uuid4())
    user_id = user.get("id")

    allowed, error_msg = await check_credits(user_id, session_id, False, db, chat_mode="simple")
    if not allowed:
        raise HTTPException(status_code=402, detail=error_msg)

    async def stream_generator():
        try:
            # ── Save user message ──────────────────────────────────────────────
            await add_message(session_id, "user", question, user_id)

            history = await get_session_history(session_id)
            is_new_session = len(history) <= 1
            await track_usage(user_id, session_id, db, force_deduct=is_new_session)

            profile = await get_user_profile(user_id)
            profile_summary = profile.dynamic_summary if profile else None

            # ── Stream Response ────────────────────────────────────────────────
            full_response = ""
            async for event in chat_stream(
                query=question, 
                history=history, 
                profile_summary=profile_summary
            ):
                if event.get("type") == "content":
                    full_response += event.get("delta", "")
                
                yield json.dumps(event) + "\n"

            # ── Auto-update Profile (Background) ───────────────────────────────
            if is_new_session:
                background_tasks.add_task(auto_update_profile, user_id, question, db)
                
            # Make sure we save the bot message so history stays correct
            if full_response:
                await add_message(session_id, "bot", full_response, user_id)

        except Exception as e:
            logger.error(f"Stream error: {e}", exc_info=True)
            yield json.dumps({"type": "error", "message": str(e)}) + "\n"

    return StreamingResponse(stream_generator(), media_type="application/x-ndjson")
