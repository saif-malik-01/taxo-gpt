"""
main.py  —  Tax Chatbot API v4.0
Two independent features:
    1. Query Chatbot  — Qdrant + BM25 hybrid retrieval, Bedrock Qwen LLM
    2. Document Tool  — Upload notices/orders → summary → draft replies

Endpoints
─────────
Query Chatbot:
    POST   /chat/ask                   non-streaming answer
    POST   /chat/stream/simple         streaming NDJSON
    POST   /chat/ask/stream/simple     streaming NDJSON (alias)
    POST   /chat/ask/stream            streaming NDJSON (new path)
    GET    /chat/history               session messages
    GET    /chat/sessions              list user sessions
    DELETE /chat/session               delete session
    POST   /chat/feedback              rate a message
    POST   /chat/share/session/{id}    share a session
    GET    /chat/share/{shared_id}     retrieve shared session

Document Tool:
    POST   /document/stream            all document cases (multipart/form-data)

Auth / Payments / Admin (unchanged):
    /auth/*, /payments/*, /admin/*

Utilities:
    GET    /auth/credits
    GET    /health
    GET    /admin/jobs
    POST   /admin/jobs/feedback/trigger

Stream format (unified — matches document feature):
    Every line is a complete JSON object followed by newline.
    Content token:  {"type": "content", "delta": "<text>"}\n
    Final event:    {"type": "retrieval", "sources": [...],
                     "session_id": "...", "intent": "...",
                     "confidence": 90}\n

Run:
    uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, List, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from starlette.concurrency import run_in_threadpool

from api.auth import router as auth_router
from api.config import settings
from api.payments import router as payment_router
from api.admin import router as admin_router
from services.auth.deps import auth_guard
from services.chat.memory_updater import auto_update_profile
from services.database import AsyncSession, get_db
from services.jobs import list_jobs, start_scheduler, stop_scheduler
from services.logging_config import setup_logging
from services.memory import (
    add_message, check_credits, get_session_history,
    get_shared_session, share_session, track_usage,
)
from services.models import ChatSession, Feedback, User, UserUsage

from retrieval import FinalResponse, RetrievalPipeline, SessionMessage

from api.document import router as document_router
from services.document.issue_replier import set_pipeline as set_doc_pipeline

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING + APP
# ─────────────────────────────────────────────────────────────────────────────

setup_logging(log_level=settings.LOG_LEVEL, log_file=settings.LOG_FILE)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Tax Chatbot API",
    version="4.0.0",
    description=(
        "Indian Tax Law — Qdrant + BM25 hybrid retrieval, Bedrock Qwen LLM. "
        "Two features: Query Chatbot and Document Draft Reply Tool."
    ),
)

_pipeline: Optional[RetrievalPipeline] = None


# ─────────────────────────────────────────────────────────────────────────────
# LIFECYCLE
# ─────────────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    global _pipeline
    logger.info("=== Tax Chatbot API starting ===")

    try:
        start_scheduler()
        logger.info("Background scheduler started")
    except Exception as e:
        logger.error(f"Scheduler start failed: {e}", exc_info=True)

    try:
        _pipeline = RetrievalPipeline()
        _pipeline.setup()
        set_doc_pipeline(_pipeline)
        logger.info("Retrieval pipeline ready")
    except Exception as e:
        logger.error(f"Pipeline init failed: {e}", exc_info=True)
        raise


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("=== Tax Chatbot API shutting down ===")
    try:
        stop_scheduler()
    except Exception as e:
        logger.error(f"Scheduler stop error: {e}", exc_info=True)


# ─────────────────────────────────────────────────────────────────────────────
# MIDDLEWARE
# ─────────────────────────────────────────────────────────────────────────────

@app.middleware("http")
async def log_requests(request: Request, call_next):
    import time
    t = time.time()
    logger.info(f"REQ  {request.method} {request.url.path}")
    try:
        resp = await call_next(request)
        logger.info(
            f"RES  {resp.status_code}  {request.method} "
            f"{request.url.path}  ({time.time()-t:.3f}s)"
        )
        return resp
    except Exception as e:
        logger.error(f"ERR  {request.method} {request.url.path}: {e}", exc_info=True)
        raise


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────────────────────
# ROUTERS
# ─────────────────────────────────────────────────────────────────────────────

app.include_router(auth_router)
app.include_router(payment_router)
app.include_router(admin_router)
app.include_router(document_router)


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    question:   str           = Field(..., min_length=2, max_length=2000)
    session_id: Optional[str] = None


class SourceChunk(BaseModel):
    label:       str
    score:       float
    chunk_type:  str
    parent_doc:  str
    chunk_index: Optional[int]
    source:      str
    identifier:  str
    summary:     str


class ChatResponse(BaseModel):
    answer:      str
    session_id:  str
    intent:      str
    confidence:  int
    sources:     List[SourceChunk]
    timestamp:   str


class FeedbackRequest(BaseModel):
    message_id: int
    rating:     int
    comment:    Optional[str] = None


class SharedMessageSchema(BaseModel):
    id:        int
    role:      str
    content:   str
    timestamp: datetime


class SharedSessionResponse(BaseModel):
    session_id: str
    title:      Optional[str]
    messages:   List[SharedMessageSchema]


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

async def _get_db_user(email: str, db: AsyncSession) -> User:
    result  = await db.execute(
        select(User).where(func.lower(User.email) == email.lower())
    )
    db_user = result.scalars().first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user


def _to_pipeline_history(history: list, limit: int = 3) -> List[SessionMessage]:
    turns: List[SessionMessage] = []
    pending_q: Optional[str]    = None

    for msg in history:
        role    = msg.get("role")    if isinstance(msg, dict) else getattr(msg, "role",    "")
        content = msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", "")
        if role == "user":
            pending_q = content
        elif role == "assistant" and pending_q is not None:
            turns.append(SessionMessage(user_query=pending_q, llm_response=content))
            pending_q = None

    return turns[-limit:]


# ─────────────────────────────────────────────────────────────────────────────
# NDJSON helpers  (shared by _stream_core and document.py)
# ─────────────────────────────────────────────────────────────────────────────

def _ndjson(data: dict) -> str:
    """Serialize dict to a single NDJSON line."""
    return json.dumps(data, ensure_ascii=False) + "\n"


def _content_line(text: str) -> str:
    """Single streaming content token line."""
    return _ndjson({"type": "content", "delta": text})


def _retrieval_line(
    session_id: str,
    sources: list,
    intent: str = "",
    confidence: int = 0,
    message_id=None,
) -> str:
    """Final retrieval metadata line — matches document feature format."""
    return _ndjson({
        "type":       "retrieval",
        "sources":    sources,
        "session_id": session_id,
        "intent":     intent,
        "confidence": confidence,
        "id":         message_id,
        "message_id": message_id,
    })


# ─────────────────────────────────────────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    ready = _pipeline is not None
    return {
        "status":           "ok" if ready else "starting",
        "pipeline_ready":   ready,
        "bm25_vocab_size":  len(_pipeline._bm25._vocab)   if ready else 0,
        "bm25_corpus_docs": _pipeline._bm25._corpus_docs  if ready else 0,
        "timestamp":        datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# NON-STREAMING CHAT
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/chat/ask", response_model=ChatResponse)
async def chat_ask(
    background_tasks: BackgroundTasks,
    question:         str           = Form(...),
    session_id:       Optional[str] = Form(None),
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db),
):
    if _pipeline is None:
        raise HTTPException(status_code=503, detail="Pipeline not ready")

    db_user    = await _get_db_user(user.get("sub"), db)
    session_id = session_id or str(uuid.uuid4())

    allowed, error_msg = await check_credits(db_user.id, session_id, False, db)
    if not allowed:
        raise HTTPException(status_code=402, detail=error_msg)

    history          = await get_session_history(session_id)
    pipeline_history = _to_pipeline_history(history)

    try:
        response: FinalResponse = await run_in_threadpool(
            _pipeline.query,
            question,
            pipeline_history,
        )
    except Exception as e:
        logger.exception(f"Pipeline error user={db_user.id} session={session_id[:8]}: {e}")
        raise HTTPException(status_code=500, detail="Pipeline error — see server logs")

    ts = datetime.now(timezone.utc).isoformat()
    await add_message(session_id, "user",      question, db_user.id)
    await add_message(session_id, "assistant", response.answer,  db_user.id)
    await track_usage(db_user.id, session_id, db, force_deduct=(len(history) == 0))

    background_tasks.add_task(
        auto_update_profile, db_user.id, question, response.answer
    )
    logger.info(
        f"chat_ask done: user={db_user.id} session={session_id[:8]} "
        f"intent={response.intent} conf={response.confidence}"
    )
    return ChatResponse(
        answer=response.answer,
        session_id=session_id,
        intent=response.intent,
        confidence=response.confidence,
        sources=[SourceChunk(**d) for d in response.retrieved_documents],
        timestamp=ts,
    )


# ─────────────────────────────────────────────────────────────────────────────
# STREAMING CHAT — CORE GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

async def _stream_core(
    question:         str,
    session_id:       str,
    db_user_id:       int,
    db:               AsyncSession,
    background_tasks: BackgroundTasks,
) -> AsyncGenerator[str, None]:
    """
    NDJSON generator for query chatbot.

    Every line is a self-contained JSON object:
        {"type": "content",   "delta": "<token>"}
        {"type": "retrieval", "sources": [...], "session_id": "...",
         "intent": "...", "confidence": 90}

    This matches the format used by /document/stream so the frontend
    only needs one parser for both features.
    """
    history          = await get_session_history(session_id)
    pipeline_history = _to_pipeline_history(history)

    # Stages 1–5: retrieval in thread pool
    try:
        staged = await run_in_threadpool(
            _pipeline.query_stages_1_to_5,
            question,
            pipeline_history,
        )
    except Exception as e:
        logger.exception(f"Pipeline stages 1-5 error: {e}")
        yield _content_line("An error occurred during retrieval. Please try again.")
        return

    # Stage 6: stream tokens from Bedrock
    answer_parts: List[str] = []

    try:
        for chunk in _pipeline.query_stage_6_stream(*staged):
            if chunk.startswith("\n\n__META__"):
                # Pipeline emits this sentinel with metadata after all tokens.
                # Parse it and emit the final retrieval event in NDJSON format.
                raw_meta = chunk[len("\n\n__META__"):]
                try:
                    meta = json.loads(raw_meta)
                except Exception:
                    meta = {}

                full_answer = "".join(answer_parts)
                ts          = datetime.now(timezone.utc).isoformat()

                asst = await add_message(session_id, "user",      question,    db_user_id)
                asst = await add_message(session_id, "assistant", full_answer, db_user_id)
                await track_usage(
                    db_user_id, session_id, db,
                    force_deduct=(len(history) == 0)
                )

                background_tasks.add_task(
                    auto_update_profile, db_user_id, question, full_answer
                )
                logger.info(
                    f"stream done: user={db_user_id} session={session_id[:8]} "
                    f"intent={meta.get('intent')} conf={meta.get('confidence')}"
                )

                # Final NDJSON line — retrieval metadata
                yield _retrieval_line(
                    session_id  = session_id,
                    sources     = meta.get("retrieved_documents", []),
                    intent      = meta.get("intent", ""),
                    confidence  = meta.get("confidence", 0),
                    message_id  = getattr(asst, "id", None),
                )

            else:
                # Regular token — emit as content line
                answer_parts.append(chunk)
                yield _content_line(chunk)

    except Exception as e:
        logger.exception(f"Stage 6 stream error: {e}")
        yield _content_line("An error occurred while generating the response.")


# ─────────────────────────────────────────────────────────────────────────────
# STREAMING ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/chat/stream/simple")
@app.post("/chat/ask/stream/simple")
@app.post("/chat/ask/stream")
async def chat_stream_endpoint(
    background_tasks: BackgroundTasks,
    question:         str           = Form(...),
    session_id:       Optional[str] = Form(None),
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db),
):
    """
    Streaming query chatbot — application/x-ndjson.
    Three path aliases for frontend backwards compatibility.

    Every line is a complete JSON object:
        {"type": "content",   "delta": "<token>"}
        {"type": "retrieval", "sources": [...], "session_id": "...",
         "intent": "GENERAL", "confidence": 90}
    """
    if _pipeline is None:
        raise HTTPException(status_code=503, detail="Pipeline not ready")

    db_user    = await _get_db_user(user.get("sub"), db)
    session_id = session_id or str(uuid.uuid4())

    allowed, error_msg = await check_credits(db_user.id, session_id, False, db)
    if not allowed:
        raise HTTPException(status_code=402, detail=error_msg)

    return StreamingResponse(
        _stream_core(question, session_id, db_user.id, db, background_tasks),
        media_type="application/x-ndjson",
        headers={"X-Session-Id": session_id},
    )


# ─────────────────────────────────────────────────────────────────────────────
# REMAINING ROUTES — unchanged
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/chat/feedback")
async def save_feedback(
    message_id: int           = Form(...),
    rating:     int           = Form(...),
    comment:    Optional[str] = Form(None),
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db),
):
    db.add(Feedback(
        message_id=message_id,
        rating=rating,
        comment=comment,
    ))
    await db.commit()
    return {"status": "recorded"}


@app.get("/chat/history")
async def get_history(
    session_id: str,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db),
):
    db_user = await _get_db_user(user.get("sub"), db)
    res = await db.execute(
        select(ChatSession).where(
            ChatSession.id == session_id,
            ChatSession.user_id == db_user.id,
        )
    )
    if not res.scalars().first():
        raise HTTPException(status_code=404, detail="Session not found or unauthorized")
    return await get_session_history(session_id, limit=50)


@app.get("/chat/sessions")
async def list_sessions_endpoint(
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db),
):
    db_user = await _get_db_user(user.get("sub"), db)
    res = await db.execute(
        select(ChatSession)
        .where(ChatSession.user_id == db_user.id)
        .order_by(ChatSession.created_at.desc())
    )
    return [
        {
            "id":         s.id,
            "title":      s.title,
            "created_at": s.created_at.isoformat() if s.created_at else None,
        }
        for s in res.scalars().all()
    ]


@app.delete("/chat/session")
async def delete_chat(
    session_id: str,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db),
):
    db_user = await _get_db_user(user.get("sub"), db)
    res = await db.execute(
        select(ChatSession).where(
            ChatSession.id == session_id,
            ChatSession.user_id == db_user.id,
        )
    )
    if not res.scalars().first():
        raise HTTPException(status_code=404, detail="Session not found or unauthorized")
    from services.memory import delete_session
    await delete_session(session_id)
    try:
        from services.document.doc_context import clear_doc_context
        from services.document.session_doc_store import delete_session_documents
        await clear_doc_context(session_id)
        await delete_session_documents(session_id)
    except Exception:
        pass
    return {"status": "deleted"}


@app.post("/chat/share/session/{session_id}")
async def enable_session_sharing(
    session_id: str,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db),
):
    email = user.get("sub")
    res   = await db.execute(
        select(ChatSession)
        .join(User)
        .where(
            ChatSession.id == session_id,
            func.lower(User.email) == email.lower(),
        )
    )
    if not res.scalars().first():
        raise HTTPException(status_code=403, detail="Unauthorized or session not found")
    shared_id = await share_session(session_id, db)
    return {
        "shared_id":  shared_id,
        "session_id": session_id,
        "share_url":  f"/share/{shared_id}",
    }


@app.get("/chat/share/{shared_id}", response_model=SharedSessionResponse)
async def retrieve_shared_session(
    shared_id: str,
    db: AsyncSession = Depends(get_db),
):
    session = await get_shared_session(shared_id, db)
    if not session:
        raise HTTPException(status_code=404, detail="Shared session not found")
    messages = [
        SharedMessageSchema(
            id=m.id, role=m.role, content=m.content, timestamp=m.timestamp
        )
        for m in sorted(session.messages, key=lambda x: x.timestamp)
    ]
    return {"session_id": session.id, "title": session.title, "messages": messages}


@app.get("/auth/credits")
async def get_user_credits(
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db),
):
    db_user = await _get_db_user(user.get("sub"), db)
    res     = await db.execute(
        select(UserUsage).where(UserUsage.user_id == db_user.id)
    )
    usage = res.scalars().first()
    if not usage:
        usage = UserUsage(user_id=db_user.id)
        db.add(usage)
        await db.commit()
        await db.refresh(usage)
    return {
        "balance": {
            "simple": usage.simple_query_balance,
            "draft":  usage.draft_reply_balance,
        },
        "used": {
            "simple": usage.simple_query_used,
            "draft":  usage.draft_reply_used,
        },
    }


@app.get("/admin/jobs")
async def get_scheduled_jobs(user=Depends(auth_guard)):
    return {"jobs": list_jobs(), "count": len(list_jobs())}


@app.post("/admin/jobs/feedback/trigger")
async def trigger_feedback_report(user=Depends(auth_guard)):
    from services.jobs.feedback_emailer import send_daily_feedback_report
    try:
        await send_daily_feedback_report()
        return {"status": "success", "message": "Feedback report sent"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send report: {str(e)}")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception(f"Unhandled on {request.url}: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)},
    )