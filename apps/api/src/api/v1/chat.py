import asyncio
import uuid
import json
import logging
import time
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Form, BackgroundTasks, Request
from fastapi.responses import StreamingResponse
from starlette.concurrency import run_in_threadpool
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
from apps.api.src.services.document.pipeline import (
    _extract_all_documents,
    _run_step2,
    _apply_routing,
    _extract_all_issues,
    _handle_show_summary,
    _handle_draft_issues,
    _handle_update_issues,
    _handle_query_fallback,
    _content,
    _event_msg,
    _retrieval_event
)
from apps.api.src.services.document.doc_classifier import determine_route
from apps.api.src.services.document.s3_storage import upload_document_to_s3, generate_presigned_view_url
from apps.api.src.services.document.redis_queue import dispatch_extraction_task
from apps.api.src.services.document.doc_context import (
    create_empty_context,
    get_doc_context,
    get_active_case,
    set_doc_context,
    get_draftable_issues,
    snapshot_for_display,
    append_user_context,
    bump_version
)
from apps.api.src.services.document.intent_classifier import (
    classify_intent_no_docs,
    rewrite_query_if_needed
)
import os, shutil, tempfile, uuid
from fastapi import File, Form, UploadFile
from typing import List, Optional

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
    request: Request,
    payload: ChatRequest,
    background_tasks: BackgroundTasks,
    user=Depends(auth_guard)
):
    question = payload.question
    session_id = payload.session_id or str(uuid.uuid4())
    user_id = user.get("id")

    allowed, error_msg = await check_credits(user_id, session_id, chat_mode="simple")
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
            await track_usage(user_id, session_id, usage=llm_usage, force_deduct=True)

            # ── Auto-update Profile (Background) ───────────────────────────────
            is_new_session = len(history) <= 1
            if is_new_session:
                background_tasks.add_task(auto_update_profile, user_id, question, full_response)

        except Exception as e:
            logger.error(f"Stream error: {e}", exc_info=True)
            yield json.dumps({"type": "error", "message": str(e)}) + "\n"

    return StreamingResponse(stream_generator(), media_type="application/x-ndjson")


@router.post("/chat/ask/stream/draft")
async def ask_gst_stream_draft(
    background_tasks: BackgroundTasks,
    question:    str              = Form(""),
    session_id:  Optional[str]     = Form(None),
    files:       List[UploadFile] = File([]),
    user                          = Depends(auth_guard)
):
    """
    Unified Draft Reply endpoint. 
    Uses the advanced Legal Draft Architecture (processor, classifier, replier).
    """
    user_id = user.get("id")
    session_id = session_id or str(uuid.uuid4())
    has_files = bool(files and any(f.filename for f in files))

    # Credits & Usage check
    allowed, err = await check_credits(user_id, session_id, chat_mode="draft")
    if not allowed:
        raise HTTPException(status_code=402, detail=err)

    # ── Step 1: Upload to S3 & Dispatch Task (Phase 2 Decoupled) ──────────────
    attachments = []
    if has_files:
        for f in [x for x in files if x.filename]:
            ext = os.path.splitext(f.filename)[1].lower()
            f.file.seek(0)
            s3_key = await upload_document_to_s3(f.file, user_id, session_id, f.filename)
            attachments.append({"filename": f.filename, "s3_key": s3_key, "ext": ext})
        
        # Offload to ECS Worker Pool via SQS
        snapshot = await get_doc_context(session_id) or create_empty_context()
        snapshot["worker_status"] = "pending"
        await set_doc_context(session_id, snapshot)
        
        await dispatch_extraction_task(user_id, session_id, attachments)

    async def _gen():
        snap_ref = [None]
        try:
            history  = await get_session_history(session_id)
            profile  = await get_user_profile(user_id)
            profile_summary = profile.dynamic_summary if profile else None
            context_exists = await get_doc_context(session_id)
            if not context_exists:
                snapshot = create_empty_context()
                await set_doc_context(session_id, snapshot)
            else:
                snapshot = context_exists
            snap_ref[0] = snapshot
            
            await add_message(session_id, "user", question, user_id, chat_mode="draft", attachments=attachments)
            await track_usage(user_id, session_id, force_deduct=len(history) == 0, chat_mode="draft")

            active_case = get_active_case(snapshot)

            # --- Step 0: Intent & Query Refinement ---
            res_q = question
            if question.strip() and history and not has_files:
                res_q = await run_in_threadpool(rewrite_query_if_needed, question, history, snapshot)

            # --- Step 1: Handling Worker Completion (Phase 2 Wait Loop) ---
            if has_files:
                yield _event_msg("Analyzing your documents")
                
                # 10 minute safety cap for large/scanned documents
                max_wait_s = 600 
                start_t = time.time()
                try:
                    while True:
                        await asyncio.sleep(1.5) # Poll Redis Context every 1.5s
                        snapshot = await get_doc_context(session_id) or snapshot
                        snap_ref[0] = snapshot
                        active_case = get_active_case(snapshot)
                        
                        # If any new document is still "pending", wait
                        if snapshot.get("worker_status") == "completed":
                            break
                            
                        if time.time() - start_t > max_wait_s:
                            yield _event_msg("Analyzing documents is taking longer than expected. Please check back in a few moments or try again if the status doesn't change.")
                            break 
                except Exception as e:
                    logger.error(f"Polling error for session {session_id}: {e}")
                    yield _event_msg("An error occurred while tracking document analysis. Please try again.")
                    return
                
                # Update local snap ref for Step 2
                active_case = get_active_case(snapshot)
                logger.info(f"Worker complete for session {session_id}. Active case ID: {snapshot.get('active_case_id')}, Docs: {len(active_case.get('docs', []) if active_case else [])}, Issues: {len(active_case.get('issues', []) if active_case else [])}")
                yield _event_msg("Drafting")

            # --- Step 2: Determine & Execute Intent ---
            if not active_case:
                async for chunk in _handle_query_fallback(res_q, session_id, user_id, history, background_tasks, active_case, snapshot):
                    yield chunk
                return

            intent_res = await run_in_threadpool(classify_intent_no_docs, res_q, snapshot)
            intent = intent_res.get("intent", "summarize")
            mode = intent_res.get("mode")
            logger.info(f"Intent classified: {intent} for session {session_id}")

            if intent == "summarize" or has_files:
                async for chunk in _handle_show_summary(active_case, session_id, user_id):
                    yield chunk
            elif intent in ("draft_all", "draft_direct", "draft_specific", "update_reply"):
                if mode: active_case["mode"] = mode
                
                issue_ids = intent_res.get("issue_numbers") if intent in ("draft_specific", "update_reply") else None
                issues_to_draft = get_draftable_issues(active_case, issue_ids=issue_ids)
                
                if not issues_to_draft and intent in ("draft_specific", "update_reply"):
                    async for chunk in _handle_query_fallback(res_q, session_id, user_id, history, background_tasks, active_case, snapshot):
                        yield chunk
                    return

                if intent == "update_reply":
                    append_user_context(active_case, res_q)
                    bump_version(snapshot)
                    
                async for chunk in _handle_draft_issues(active_case, issues_to_draft, session_id, user_id, res_q, background_tasks, snapshot):
                    yield chunk
            elif intent == "update_issues":
                async for chunk in _handle_update_issues(active_case, res_q, session_id, user_id):
                    yield chunk
            else:
                async for chunk in _handle_query_fallback(res_q, session_id, user_id, history, background_tasks, active_case, snapshot):
                    yield chunk

        except Exception as e:
            logger.error(f"Logic failure in chat/ask/stream/draft: {e}", exc_info=True)
            yield _content(f"Error: {str(e)}")
        finally:
            if snap_ref[0]:
                await set_doc_context(session_id, snap_ref[0])

    return StreamingResponse(_gen(), media_type="application/x-ndjson")

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

@router.post("/chat/share/message/{message_id}")
async def share_message(message_id: int, user=Depends(auth_guard), db: AsyncSession = Depends(get_db)):
    # Verify message exists and belongs to user
    user_id = user.get("id")
    res = await db.execute(
        select(ChatMessage)
        .join(ChatSession)
        .where(ChatMessage.id == message_id, ChatSession.user_id == user_id)
    )
    msg = res.scalars().first()
    if not msg: raise HTTPException(status_code=404, detail="Message not found or unauthorized")
    
    # Create shared link entry
    shared_id = str(uuid.uuid4())[:8] # Short ID
    db.add(SharedSession(id=shared_id, message_id=message_id, session_id=msg.session_id))
    await db.commit()
    return {"shared_id": shared_id, "url": f"/chat/share/{shared_id}"}

@router.get("/chat/share/{shared_id}")
async def get_shared_content(shared_id: str, db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(SharedSession).where(SharedSession.id == shared_id))
    shared = res.scalars().first()
    if not shared: raise HTTPException(status_code=404, detail="Shared content not found")
    
    if shared.message_id:
        # Fetch the specific message and its immediate predecessor (the prompt)
        res = await db.execute(
            select(ChatMessage)
            .where(
                ChatMessage.session_id == shared.session_id,
                ChatMessage.id <= shared.message_id
            )
            .order_by(ChatMessage.id.desc())
            .limit(2)
        )
        messages = sorted(res.scalars().all(), key=lambda x: x.id)
        
        from apps.api.src.services.rag.retrieval.hydrator import hydrate_sources
        from apps.api.src.services.chat.engine import get_pipeline

        history = []
        for m in messages:
            msg_dict = {"id": m.id, "role": m.role, "content": m.content}
            if m.source_ids:
                pipeline = await get_pipeline()
                msg_dict["sources"] = await hydrate_sources(m.source_ids, pipeline._qdrant)
            history.append(msg_dict)
            
        return {"history": history, "type": "message"}
    
    # Default: Full session history
    history = await get_session_history(shared.session_id)
    return {"history": history, "type": "session"}

@router.get("/chat/document/view")
async def get_document_view_url(
    s3_key: str,
    user=Depends(auth_guard)
):
    """
    Generates a secure, temporary S3 viewing link for a document.
    Ensures the user belongs to the session path for security.
    """
    user_id = user.get("id")
    
    if f"docs/{user_id}/" not in s3_key:
        raise HTTPException(status_code=403, detail="Unauthorized access to document")
    
    url = generate_presigned_view_url(s3_key)
    if not url:
        raise HTTPException(status_code=404, detail="Document not found or inaccessible")
    
    return {"url": url, "expires_in": 3600}
