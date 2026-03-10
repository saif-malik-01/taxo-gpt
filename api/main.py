import os
from datetime import datetime
import logging

os.environ['HF_HUB_DISABLE_SYMLINKS'] = '1'
os.environ['HF_HOME'] = os.path.join(os.path.dirname(__file__), '..', '.hf_cache')

import json
import uuid
import tempfile
import shutil

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from services.memory import update_message_tokens
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional
from sqlalchemy import select, func
from starlette.concurrency import run_in_threadpool

from services.chat.engine import chat, chat_stream
from services.vector.store import VectorStore
from services.auth.deps import auth_guard
from api.auth import router as auth_router
from api.payments import router as payment_router
from api.admin import router as admin_router
from services.database import get_db, AsyncSession
from services.memory import get_session_history, add_message, get_user_profile, share_session, get_shared_session, track_usage, check_credits
from services.models import Feedback, User, ChatSession, UserProfile, ChatMessage, UserUsage
from services.chat.memory_updater import auto_update_profile
from services.document.processor import DocumentProcessor, DocumentAnalyzer
from services.document.issue_replier import process_issues_streaming, _process_single_issue, MODE_DEFENSIVE, MODE_IN_FAVOUR, detect_mode
from services.jobs import start_scheduler, stop_scheduler, list_jobs

# ── New document context imports ───────────────────────────────────────────────
from services.document.doc_context import (
    get_doc_context, set_doc_context, clear_doc_context,
    create_empty_context, create_new_case, get_active_case,
    get_next_case_id, add_case_to_context, archive_active_case, switch_active_case,
    get_pending_issues, merge_new_issues_with_existing, apply_issue_update,
)
from services.document.doc_classifier import classify_document, determine_routing
from services.document.intent_classifier import classify_intent, parse_issue_update
from services.document.session_doc_store import (
    save_document_text, get_primary_texts, get_reference_texts, delete_session_documents,
)

# ---------------- INIT ---------------- #

app = FastAPI(title="GST Expert API", version="3.0.0")

# ---------------- LOGGING SETUP ---------------- #

from services.logging_config import setup_logging
from api.config import settings

setup_logging(log_level=settings.LOG_LEVEL, log_file=settings.LOG_FILE)
logger = logging.getLogger(__name__)

# ---------------- LIFECYCLE EVENTS ---------------- #

@app.on_event("startup")
async def startup_event():
    logger.info("🚀 Starting GST Expert API...")
    try:
        start_scheduler()
        logger.info("✅ Background jobs initialized")
    except Exception as e:
        logger.error(f"❌ Failed to start scheduler: {e}", exc_info=True)


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("👋 Shutting down GST Expert API...")
    try:
        stop_scheduler()
    except Exception as e:
        logger.error(f"❌ Failed to stop scheduler: {e}", exc_info=True)

# ---------------- MIDDLEWARE ---------------- #

@app.middleware("http")
async def log_requests(request, call_next):
    import time
    start_time = time.time()
    logger.info(f"REQ - {request.method} {request.url.path}")
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        logger.info(f"RES - {response.status_code} - {request.method} {request.url.path} ({process_time:.4f}s)")
        return response
    except Exception as e:
        process_time = time.time() - start_time
        logger.error(f"ERR - UNHANDLED EXCEPTION in {request.method} {request.url.path} ({process_time:.4f}s): {str(e)}", exc_info=True)
        raise e

# ---------------- CORS ---------------- #

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- ROUTERS ---------------- #

app.include_router(auth_router)
app.include_router(payment_router)
app.include_router(admin_router)

# ---------------- DATA ---------------- #

INDEX_PATH  = "data/vector_store/faiss.index"
CHUNKS_PATH = "data/processed/all_chunks.json"

vector_store = VectorStore(INDEX_PATH, CHUNKS_PATH)

with open(CHUNKS_PATH, "r", encoding="utf-8") as f:
    ALL_CHUNKS = json.load(f)

from services.retrieval.citation_matcher import get_index
get_index(ALL_CHUNKS)

# ---------------- DOCUMENT SERVICE INSTANCES ---------------- #

doc_processor = DocumentProcessor()
doc_analyzer  = DocumentAnalyzer()

# ---------------- SCHEMAS ---------------- #

class ChatRequest(BaseModel):
    question: str
    session_id: Optional[str] = None
    document_context: Optional[str] = None

class SourceChunk(BaseModel):
    id: str
    chunk_type: str
    text: str
    metadata: dict

class FullJudgment(BaseModel):
    citation: str
    title: str
    case_number: str
    court: str
    state: str
    year: str
    judge: str
    petitioner: str
    respondent: str
    decision: str
    current_status: str
    law: str
    act_name: str
    section_number: str
    rule_name: str
    rule_number: str
    notification_number: str
    case_note: str
    full_text: str
    external_id: str

class CitationInfo(BaseModel):
    citation: str
    case_number: str
    petitioner: str
    respondent: str
    external_id: str
    court: str
    year: str
    decision: str

class ChatResponse(BaseModel):
    answer: str
    session_id: str
    sources: List[SourceChunk]
    full_judgments: Optional[Dict[str, FullJudgment]] = None
    party_citations: Optional[Dict[str, List[CitationInfo]]] = None

class FeedbackRequest(BaseModel):
    message_id: int
    rating: int
    comment: Optional[str] = None

class AnalysisResponse(BaseModel):
    success: bool
    extracted_text: str
    structured_analysis: dict
    formatted_response: str
    metadata: dict

class SharedMessageSchema(BaseModel):
    id: int
    role: str
    content: str
    timestamp: datetime

class SharedSessionResponse(BaseModel):
    session_id: str
    title: Optional[str]
    messages: List[SharedMessageSchema]

# ---------------- HELPERS ---------------- #

def _check_needs_knowledge(analysis: dict) -> bool:
    user_response = analysis.get("user_question_response", "") or ""
    return (
        "It would we better to answer your query using my knowledge?" in user_response
        or "Should I resolve your query using my knowledge?" in user_response
    )


def _merge_full_judgments(target: dict, source: dict) -> None:
    for ext_id, judgment in source.items():
        if ext_id not in target:
            target[ext_id] = judgment


def _merge_sources(existing: list, new_sources: list, seen_ids: set) -> None:
    for s in new_sources:
        sid = s.get("id", "")
        if sid not in seen_ids:
            existing.append(s)
            seen_ids.add(sid)

# ---------------- CHAT ROUTES ---------------- #

@app.get("/health")
def health():
    return {"status": "ok"}

# ---------------- JOB MANAGEMENT ROUTES ---------------- #

@app.get("/admin/jobs")
async def get_scheduled_jobs(user=Depends(auth_guard)):
    jobs = list_jobs()
    return {"jobs": jobs, "count": len(jobs)}


@app.post("/admin/jobs/feedback/trigger")
async def trigger_feedback_report(user=Depends(auth_guard)):
    from services.jobs.feedback_emailer import send_daily_feedback_report
    try:
        await send_daily_feedback_report()
        return {"status": "success", "message": "Feedback report sent successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send feedback report: {str(e)}")


# ---------------- NON-STREAMING CHAT ---------------- #

@app.post("/chat/ask", response_model=ChatResponse)
async def ask_gst(
    payload: ChatRequest,
    background_tasks: BackgroundTasks,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    logger.info(f"Chat request received. User: {user.get('sub')}, Session: {payload.session_id}")
    session_id = payload.session_id or str(uuid.uuid4())

    email = user.get("sub")
    if not email:
        raise HTTPException(status_code=401, detail="Invalid user")

    result = await db.execute(select(User).where(func.lower(User.email) == email.lower()))
    db_user = result.scalars().first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")

    user_id = db_user.id

    allowed, error_msg = await check_credits(user_id, session_id, False, db)
    if not allowed:
        logger.warning(f"Credit check failed for user {user_id}: {error_msg}")
        raise HTTPException(status_code=402, detail=error_msg)

    profile         = await get_user_profile(user_id)
    profile_summary = profile.dynamic_summary if profile else None
    history         = await get_session_history(session_id)

    answer, sources, full_judgments, party_citations_dict, usage = await chat(
        query=payload.question,
        store=vector_store,
        all_chunks=ALL_CHUNKS,
        history=history,
        profile_summary=profile_summary
    )

    await add_message(session_id, "user", payload.question, user_id)
    await add_message(
        session_id, "assistant", answer, user_id,
        prompt_tokens=usage.get("inputTokens", 0),
        response_tokens=usage.get("outputTokens", 0)
    )

    is_new_session = len(history) == 0
    await track_usage(user_id, session_id, db, usage=usage, force_deduct=is_new_session)

    background_tasks.add_task(auto_update_profile, user_id, payload.question, answer)

    party_citations_formatted = {}
    for (p1, p2), citations in party_citations_dict.items():
        party_citations_formatted[f"{p1} vs {p2}"] = citations

    return {
        "answer":          answer,
        "session_id":      session_id,
        "sources":         sources,
        "full_judgments":  full_judgments if full_judgments else None,
        "party_citations": party_citations_formatted if party_citations_formatted else None
    }


# ---------------- STREAMING ENTRY POINTS ---------------- #

@app.post("/chat/stream/simple")
@app.post("/chat/ask/stream/simple")
async def ask_gst_stream_simple(
    background_tasks: BackgroundTasks,
    question: str = Form(...),
    session_id: Optional[str] = Form(None),
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    return await _ask_gst_stream_core("simple", background_tasks, question, session_id, [], user, db)


@app.post("/chat/stream/draft")
@app.post("/chat/ask/stream/draft")
async def ask_gst_stream_draft(
    background_tasks: BackgroundTasks,
    question: str = Form(default=""),
    session_id: Optional[str] = Form(None),
    files: List[UploadFile] = File(default=[]),
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    return await _ask_gst_stream_core("draft", background_tasks, question, session_id, files, user, db)


# ---------------- CORE STREAMING HANDLER ---------------- #

async def _ask_gst_stream_core(
    chat_mode: str,
    background_tasks: BackgroundTasks,
    question: str,
    session_id: Optional[str],
    files: List[UploadFile],
    user,
    db: AsyncSession
):
    session_id = session_id or str(uuid.uuid4())

    email  = user.get("sub")
    result = await db.execute(select(User).where(func.lower(User.email) == email.lower()))
    db_user = result.scalars().first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    user_id = db_user.id

    has_files_pre = any(f.filename for f in files) if files else False
    allowed, error_msg = await check_credits(user_id, session_id, has_files_pre, db, chat_mode=chat_mode)
    if not allowed:
        raise HTTPException(status_code=402, detail=error_msg)

    supported       = {'.pdf', '.docx', '.pptx', '.xlsx', '.html', '.png', '.jpg', '.jpeg', '.tiff', '.bmp'}
    temp_file_paths = []
    has_files       = bool(files and len(files) > 0)

    if has_files:
        try:
            for file in files:
                ext = os.path.splitext(file.filename)[1].lower()
                if ext not in supported:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Unsupported file format: {file.filename}. Supported: {', '.join(sorted(supported))}"
                    )
                with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                    shutil.copyfileobj(file.file, tmp)
                    size = os.path.getsize(tmp.name)
                    temp_file_paths.append((tmp.name, ext, file.filename, file.content_type, size))
        except HTTPException:
            for tp, *_ in temp_file_paths:
                if os.path.exists(tp): os.unlink(tp)
            raise

    async def stream_generator():
        try:
            history = await get_session_history(session_id)

            # ── Save user message ──────────────────────────────────────────────
            user_message = question
            if has_files and temp_file_paths:
                filenames     = [fp[2] for fp in temp_file_paths]
                user_message += f"\n\n[Documents: {', '.join(filenames)}]"
            await add_message(session_id, "user", user_message, user_id, chat_mode=chat_mode)

            is_new_session = len(history) == 0
            force_deduct   = is_new_session or has_files
            await track_usage(user_id, session_id, db, force_deduct=force_deduct)

            profile         = await get_user_profile(user_id)
            profile_summary = profile.dynamic_summary if profile else None
            history         = await get_session_history(session_id)

            # ── Load doc context from Redis ────────────────────────────────────
            doc_context = await get_doc_context(session_id) or create_empty_context()

            # ── Process uploaded files ─────────────────────────────────────────
            if has_files and temp_file_paths:
                async for event in _process_uploaded_files(temp_file_paths, question, session_id, doc_context):
                    if event.get("type") == "error":
                        yield json.dumps(event) + "\n"
                        return
                    elif event.get("type") == "clarification_needed":
                        msg      = event.get("message", "Could you clarify which case this document belongs to?")
                        yield json.dumps({"type": "content", "delta": msg}) + "\n"
                        asst_msg = await add_message(session_id, "assistant", msg, user_id)
                        await set_doc_context(session_id, doc_context)
                        yield json.dumps({
                            "type": "retrieval", "sources": [], "full_judgments": {},
                            "message_id": getattr(asst_msg, "id", None),
                            "session_id": session_id, "id": getattr(asst_msg, "id", None),
                        }) + "\n"
                        return

            # ── Get active case ────────────────────────────────────────────────
            active_case = get_active_case(doc_context)

            # ── Classify intent ────────────────────────────────────────────────
            intent_result  = await run_in_threadpool(classify_intent, question, active_case, bool(temp_file_paths))
            intent         = intent_result.get("intent", "query_general")
            intent_mode    = intent_result.get("mode")
            issue_nums     = [int(x) for x in (intent_result.get("issue_numbers") or [])]
            target_case_id = intent_result.get("case_id")

            # ── 3-case document upload routing ─────────────────────────────────
            # Applied only when files were just uploaded in this request
            if temp_file_paths:
                if intent in ("query_document", "query_mixed", "query_general"):
                    # Case C — user asked a question about the document content
                    intent = "query_with_doc"
                elif intent in ("draft_all", "draft_specific", "confirm_mode"):
                    # Case B — user wants draft replies immediately
                    # Show summary first, then draft if mode known
                    intent = "summarize_then_draft"
                else:
                    # Case A — no specific action / "analyse" / "summarise" / empty
                    intent = "summarize"

            logger.info(f"✅ Intent: {intent} | mode={intent_mode} | issues={issue_nums}")

            # ── Route to handler ───────────────────────────────────────────────

            # ── CASE B: Upload + draft request ─────────────────────────────────
            if intent == "summarize_then_draft":
                if not active_case:
                    msg = "No document found. Please upload a document first."
                    yield json.dumps({"type": "content", "delta": msg}) + "\n"
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    # Always show summary + issues first
                    async for chunk in _handle_summarize(active_case, session_id, user_id):
                        yield chunk
                    # If mode was detected in the question, immediately start drafting
                    if intent_mode:
                        active_case["mode"] = intent_mode
                        pending = get_pending_issues(active_case)
                        if pending:
                            ref_text = await get_reference_texts(session_id, active_case["case_id"])
                            async for chunk in _handle_draft_issues(
                                active_case, pending, session_id, user_id,
                                question, profile_summary, background_tasks, ref_text,
                                db, chat_mode
                            ):
                                yield chunk
                    # If no mode detected — summarize already asked "Defence or In Favour?"

            # ── CASE C: Upload + document question ─────────────────────────────
            elif intent == "query_with_doc":
                if not active_case:
                    async for chunk in _handle_regular_chat(
                        question, session_id, user_id, history, profile_summary,
                        None, background_tasks, db
                    ):
                        yield chunk
                else:
                    async for chunk in _handle_query_with_full_doc(
                        active_case, question, session_id, user_id,
                        history, profile_summary, background_tasks, db
                    ):
                        yield chunk

            # ── CASE A: Upload + no action / summarise ─────────────────────────
            elif intent == "summarize":
                if not active_case:
                    msg = "No document uploaded yet. Please upload a document to get started."
                    yield json.dumps({"type": "content", "delta": msg}) + "\n"
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    async for chunk in _handle_summarize(active_case, session_id, user_id):
                        yield chunk

            elif intent in ("confirm_mode", "draft_all"):
                if not active_case:
                    msg = "No active case found. Please upload a document first."
                    yield json.dumps({"type": "content", "delta": msg}) + "\n"
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    if intent_mode and not active_case.get("mode"):
                        active_case["mode"] = intent_mode
                    if not active_case.get("mode"):
                        msg = "\n\nShould I prepare the reply in **Defence** (protecting the recipient) or **In Favour** of the notice?"
                        yield json.dumps({"type": "content", "delta": msg}) + "\n"
                        active_case["state"] = "awaiting_mode"
                        await add_message(session_id, "assistant", msg, user_id)
                    else:
                        pending = get_pending_issues(active_case)
                        if not pending:
                            msg = "\n\nAll issues already have replies. Ask me to update any specific one."
                            yield json.dumps({"type": "content", "delta": msg}) + "\n"
                            await add_message(session_id, "assistant", msg, user_id)
                        else:
                            ref_text = await get_reference_texts(session_id, active_case["case_id"])
                            async for chunk in _handle_draft_issues(
                                active_case, pending, session_id, user_id,
                                question, profile_summary, background_tasks, ref_text,
                                db, chat_mode
                            ):
                                yield chunk

            elif intent == "draft_specific":
                if not active_case:
                    msg = "No active case found. Please upload a document first."
                    yield json.dumps({"type": "content", "delta": msg}) + "\n"
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    if intent_mode and not active_case.get("mode"):
                        active_case["mode"] = intent_mode
                    if not active_case.get("mode"):
                        msg = "\n\nShould I prepare the reply in **Defence** or **In Favour** of the notice?"
                        yield json.dumps({"type": "content", "delta": msg}) + "\n"
                        active_case["state"] = "awaiting_mode"
                        active_case["_pending_issue_nums"] = issue_nums
                        await add_message(session_id, "assistant", msg, user_id)
                    else:
                        all_issues    = active_case.get("issues", [])
                        target_issues = [i for i in all_issues if i["id"] in issue_nums] if issue_nums else get_pending_issues(active_case)
                        if not target_issues:
                            msg = "\n\nNo matching issues found. Please check the issue numbers."
                            yield json.dumps({"type": "content", "delta": msg}) + "\n"
                            await add_message(session_id, "assistant", msg, user_id)
                        else:
                            ref_text = await get_reference_texts(session_id, active_case["case_id"])
                            async for chunk in _handle_draft_issues(
                                active_case, target_issues, session_id, user_id,
                                question, profile_summary, background_tasks, ref_text,
                                db, chat_mode
                            ):
                                yield chunk

            elif intent == "update_issues":
                if not active_case:
                    msg = "No active case found."
                    yield json.dumps({"type": "content", "delta": msg}) + "\n"
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    async for chunk in _handle_update_issues(active_case, question, session_id, user_id):
                        yield chunk

            elif intent == "update_reply":
                if not active_case:
                    msg = "No active case found."
                    yield json.dumps({"type": "content", "delta": msg}) + "\n"
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    target_id = issue_nums[0] if issue_nums else None
                    if not target_id:
                        msg = "Please specify which issue number you'd like me to update the reply for."
                        yield json.dumps({"type": "content", "delta": msg}) + "\n"
                        await add_message(session_id, "assistant", msg, user_id)
                    else:
                        ref_text = await get_reference_texts(session_id, active_case["case_id"])
                        async for chunk in _handle_update_reply(
                            active_case, target_id, session_id, user_id,
                            profile_summary, background_tasks, ref_text
                        ):
                            yield chunk

            elif intent in ("query_document", "query_mixed"):
                if not active_case:
                    async for chunk in _handle_regular_chat(
                        question, session_id, user_id, history, profile_summary,
                        None, background_tasks, db
                    ):
                        yield chunk
                else:
                    async for chunk in _handle_query_with_document(
                        active_case, question, session_id, user_id,
                        history, profile_summary, background_tasks, db
                    ):
                        yield chunk

            elif intent == "switch_case":
                if target_case_id:
                    switch_active_case(doc_context, target_case_id)
                    switched = get_active_case(doc_context)
                    if switched:
                        p   = switched.get("parties", {})
                        msg = (
                            f"\n\nSwitched to **Case {target_case_id}** — "
                            f"{p.get('sender', 'Unknown')} / {p.get('recipient', 'Unknown')}.\n\n"
                            f"{(switched.get('summary') or '')[:200]}"
                        )
                    else:
                        msg = f"\n\nCase {target_case_id} not found."
                else:
                    cases = doc_context.get("cases", [])
                    if len(cases) > 1:
                        lines = [
                            "- Case " + str(c["case_id"]) + " (" + c["status"] + "): "
                            + c.get("parties", {}).get("sender", "?")
                            + " / "
                            + c.get("parties", {}).get("recipient", "?")
                            for c in cases
                        ]
                        msg = "Available cases:\n" + "\n".join(lines) + "\n\nWhich case would you like to switch to?"
                    else:
                        msg = "Only one case exists in this session."
                yield json.dumps({"type": "content", "delta": msg}) + "\n"
                await add_message(session_id, "assistant", msg, user_id)

            elif intent == "new_case":
                archive_active_case(doc_context)
                doc_context["active_case_id"] = None
                msg = "\n\nStarting fresh for a new case. Please upload the documents."
                yield json.dumps({"type": "content", "delta": msg}) + "\n"
                await add_message(session_id, "assistant", msg, user_id)

            else:
                # query_general — pure GST chat, no document
                async for chunk in _handle_regular_chat(
                    question, session_id, user_id, history, profile_summary,
                    active_case, background_tasks, db
                ):
                    yield chunk

            # ── Save updated doc context back to Redis ─────────────────────────
            await set_doc_context(session_id, doc_context)

        except Exception as e:
            logger.error(f"❌ stream_generator error: {str(e)}", exc_info=True)
            yield json.dumps({
                "type":    "error",
                "message": "An error occurred while generating the response. Please try again."
            }) + "\n"

        finally:
            for tp, *_ in temp_file_paths:
                if os.path.exists(tp):
                    try: os.unlink(tp)
                    except OSError: pass
            logger.info(f"✅ Stream closed cleanly for session {session_id}")

    return StreamingResponse(stream_generator(), media_type="application/x-ndjson")


# =============================================================================
# FILE PROCESSING
# =============================================================================

async def _process_uploaded_files(temp_file_paths, question, session_id, doc_context):
    """
    Extract → classify → route → save text to DB → update Redis memory.
    Full text is NEVER stored in Redis. DB is write-once per document.
    """
    try:
        file_data = []
        for tmp_path, ext, filename, content_type, size in temp_file_paths:
            try:
                text = await run_in_threadpool(doc_processor.extract_text, tmp_path)
                file_data.append((filename, text or ""))
            except Exception as e:
                yield {"type": "error", "message": f"Could not extract {filename}: {str(e)}"}
                return

        active_case = get_active_case(doc_context)

        classified                = []
        needs_clarification_files = []

        for filename, text in file_data:
            classification = await run_in_threadpool(
                classify_document, text, filename, question, active_case
            )
            routing = determine_routing(classification, has_existing_case=bool(active_case))

            if routing == "ask_user":
                needs_clarification_files.append((filename, text, classification))
            else:
                classified.append((filename, text, classification, routing))

        # All files ambiguous — ask user
        if needs_clarification_files and not classified:
            names = [f[0] for f in needs_clarification_files]
            doc_context["_pending_clarification"] = [
                {"filename": f[0], "routing_hint": "ambiguous"} for f in needs_clarification_files
            ]
            yield {
                "type": "clarification_needed",
                "message": (
                    "I see you've uploaded **" + ", ".join(names) + "**. "
                    "Is this related to the existing case or a new case?"
                )
            }
            return

        for filename, text, classification in needs_clarification_files:
            classified.append((filename, text, classification, "add_to_existing"))

        new_case_primary = [(f, t, c) for f, t, c, r in classified if r == "new_case"        and c.get("is_primary", True)]
        new_case_ref     = [(f, t, c) for f, t, c, r in classified if r == "new_case"        and not c.get("is_primary", True)]
        exist_primary    = [(f, t, c) for f, t, c, r in classified if r == "add_to_existing" and c.get("is_primary", True)]
        exist_ref        = [(f, t, c) for f, t, c, r in classified if r == "add_to_existing" and not c.get("is_primary", True)]

        if new_case_primary:
            if active_case:
                active_case["status"] = "archived"
            new_case_id = get_next_case_id(doc_context)
            parties     = new_case_primary[0][2].get("parties") or {"sender": None, "recipient": None}
            new_case    = create_new_case(new_case_id, parties)
            add_case_to_context(doc_context, new_case)
            active_case = new_case

            for filename, text, _ in new_case_primary:
                await save_document_text(session_id, active_case["case_id"], filename, "primary", text)
            for filename, text, _ in new_case_ref:
                await save_document_text(session_id, active_case["case_id"], filename, "reference", text)

        if exist_primary:
            if not active_case:
                new_case_id = get_next_case_id(doc_context)
                parties     = exist_primary[0][2].get("parties") or {"sender": None, "recipient": None}
                active_case = create_new_case(new_case_id, parties)
                add_case_to_context(doc_context, active_case)

            for filename, text, _ in exist_primary:
                await save_document_text(session_id, active_case["case_id"], filename, "primary", text)

        if exist_ref and active_case:
            for filename, text, _ in exist_ref:
                await save_document_text(session_id, active_case["case_id"], filename, "reference", text)

        if active_case:
            consolidated_text = await get_primary_texts(session_id, active_case["case_id"])
            if consolidated_text.strip():
                def _analyse():
                    return doc_analyzer.analyze(consolidated_text, question)

                analysis = await run_in_threadpool(_analyse)

                active_case["summary"] = analysis.get("summary", "")
                for field in ("sender", "recipient"):
                    if not active_case["parties"].get(field) and analysis.get(field):
                        active_case["parties"][field] = analysis[field]

                new_issues_raw        = analysis.get("issues") or []
                active_case["issues"] = merge_new_issues_with_existing(
                    active_case.get("issues", []), new_issues_raw
                )

    except Exception as e:
        logger.error(f"_process_uploaded_files error: {e}", exc_info=True)
        yield {"type": "error", "message": f"Document processing error: {str(e)}"}


# =============================================================================
# INTENT HANDLERS
# =============================================================================

async def _handle_summarize(active_case, session_id, user_id):
    """
    Case A — show summary, parties, and all extracted issues.
    If issues found → ask Defence or In Favour.
    """
    summary = active_case.get("summary", "")
    issues  = active_case.get("issues", [])
    parties = active_case.get("parties", {})

    lines = []
    if parties.get("sender"):
        lines.append("**From:** " + parties["sender"])
    if parties.get("recipient"):
        lines.append("**To:** " + parties["recipient"])
    if lines:
        lines.append("")

    lines.append(summary or "Summary not available.")

    if issues:
        lines.append("\n\n**Issues / Allegations:**\n")
        for i in issues:
            status_tag = " ✅" if i.get("reply") else ""
            lines.append(str(i["id"]) + ". " + i["text"] + status_tag)
        pending_count = sum(1 for i in issues if not i.get("reply"))
        if pending_count:
            lines.append(
                "\n\nShould I prepare draft replies for these "
                + str(pending_count)
                + " issue(s)? "
                "If yes — **Defence** (protecting the recipient) or **In Favour** of the notice?"
            )
        else:
            lines.append("\n\nAll issues already have replies. You can ask me to update any specific one.")
    else:
        lines.append("\n\nNo specific issues or allegations were found in the document.")

    full_text  = "\n".join(lines)
    chunk_size = 200
    for i in range(0, len(full_text), chunk_size):
        yield json.dumps({"type": "content", "delta": full_text[i:i+chunk_size]}) + "\n"

    active_case["state"] = "awaiting_decision"
    asst_msg = await add_message(session_id, "assistant", full_text, user_id)
    yield json.dumps({
        "type":     "retrieval",
        "sources":  [],
        "full_judgments": {},
        "message_id": getattr(asst_msg, "id", None),
        "session_id": session_id,
        "id":         getattr(asst_msg, "id", None),
        "document_analysis": {
            "summary": active_case.get("summary"),
            "issues":  active_case.get("issues"),
            "parties": active_case.get("parties"),
        },
    }) + "\n"


async def _handle_draft_issues(
    active_case, issues_to_draft, session_id, user_id,
    question, profile_summary, background_tasks, ref_text,
    db, chat_mode
):
    """Generate replies for the given list of issues, streaming each one."""
    mode         = active_case.get("mode", MODE_DEFENSIVE)
    recipient    = active_case.get("parties", {}).get("recipient")
    sender       = active_case.get("parties", {}).get("sender")
    doc_summary  = active_case.get("summary", "")
    total_global = len(active_case["issues"])

    all_sources         = []
    all_full_judgments  = {}
    seen_source_ids     = set()
    full_reply_text     = ""
    total_input_tokens  = 0
    total_output_tokens = 0

    active_case["state"] = "reply_in_progress"

    async for issue_number, reply, sources, full_judgments, usage in process_issues_streaming(
        issues=[i["text"] for i in issues_to_draft],
        mode=mode, store=vector_store, all_chunks=ALL_CHUNKS,
        recipient=recipient, sender=sender, doc_summary=doc_summary,
        profile_summary=profile_summary, max_parallel=3,
        reference_docs_text=ref_text,
    ):
        await track_usage(user_id, session_id, db, usage=usage)
        total_input_tokens  += usage.get("inputTokens", 0)
        total_output_tokens += usage.get("outputTokens", 0)

        issue_obj  = issues_to_draft[issue_number - 1]
        global_id  = issue_obj["id"]
        issue_text = issue_obj["text"]

        header = "\n\n---\n\n### Issue " + str(global_id) + " of " + str(total_global) + "\n\n> " + issue_text + "\n\n"
        yield json.dumps({"type": "content", "delta": header}) + "\n"
        yield json.dumps({
            "type":         "issue_start",
            "issue_number": global_id,
            "issue_text":   issue_text,
            "total_issues": total_global
        }) + "\n"

        chunk_size = 50
        for i in range(0, len(reply), chunk_size):
            yield json.dumps({"type": "content", "delta": reply[i:i+chunk_size]}) + "\n"

        yield json.dumps({"type": "issue_end", "issue_number": global_id}) + "\n"

        full_reply_text += "\n\n### Issue " + str(global_id) + ": " + issue_text + "\n\n" + reply

        for iss in active_case["issues"]:
            if iss["id"] == global_id:
                iss["reply"]  = reply
                iss["status"] = "replied"
                break

        _merge_sources(all_sources, sources, seen_source_ids)
        _merge_full_judgments(all_full_judgments, full_judgments)

        # Mid-stream FUP check
        allowed, error_msg = await check_credits(
            user_id, session_id, False, db,
            chat_mode=chat_mode,
            extra_tokens=total_input_tokens + total_output_tokens
        )
        if not allowed:
            warning = "\n\n---\n\n*ℹ️ Note: " + error_msg + " Generation stopped at this stage.*"
            yield json.dumps({"type": "content", "delta": warning}) + "\n"
            full_reply_text += warning
            logger.warning(f"Mid-stream FUP hit for user {user_id} at issue {global_id}: {error_msg}")
            break

    closing = (
        "\n\n---\n\n**Respectfully submitted.**\n\n"
        "*For " + (recipient or "the Taxpayer") + "*\n\n"
        "Authorised Signatory / Chartered Accountant / Legal Representative\n\nDate: [Insert Date]"
    )
    for i in range(0, len(closing), 50):
        yield json.dumps({"type": "content", "delta": closing[i:i+50]}) + "\n"
    full_reply_text += closing

    active_case["state"] = "complete"

    asst_msg = await add_message(
        session_id, "assistant", full_reply_text, user_id,
        prompt_tokens=total_input_tokens,
        response_tokens=total_output_tokens
    )
    message_id = getattr(asst_msg, "id", None)

    yield json.dumps({
        "type":           "retrieval",
        "sources":        all_sources,
        "full_judgments": all_full_judgments,
        "message_id":     message_id,
        "session_id":     session_id,
        "id":             message_id,
        "document_analysis": {
            "summary": active_case.get("summary"),
            "issues":  active_case.get("issues"),
            "parties": active_case.get("parties"),
        },
    }) + "\n"
    background_tasks.add_task(auto_update_profile, user_id, question, full_reply_text)


async def _handle_update_issues(active_case, question, session_id, user_id):
    current_issues = active_case.get("issues", [])
    update = await run_in_threadpool(parse_issue_update, question, current_issues)
    action = update.get("action")

    if action == "reextract":
        # Fetch full text from DB — never stored in Redis
        full_text = await get_primary_texts(session_id, active_case["case_id"])

        if not full_text.strip():
            msg = "I couldn't find the original document text to re-analyse. Please share which issue is missing."
            yield json.dumps({"type": "content", "delta": msg}) + "\n"
            await add_message(session_id, "assistant", msg, user_id)
            return

        new_issue_texts = await run_in_threadpool(
            doc_analyzer.reextract_missed_issues, full_text, current_issues
        )

        if new_issue_texts:
            next_id = max((i["id"] for i in current_issues), default=0) + 1
            for text in new_issue_texts:
                current_issues.append({"id": next_id, "text": text, "reply": None, "status": "pending"})
                next_id += 1

            lines = ["I found the following additional issues in the document:\n"]
            for t in new_issue_texts:
                lines.append("- " + t)
            lines.append("\n\nUpdated issues list:\n")
            for i in current_issues:
                status_tag = " ✅" if i.get("reply") else ""
                lines.append(str(i["id"]) + ". " + i["text"] + status_tag)
            lines.append("\n\nShould I generate replies for the new issues?")
            response_text = "\n".join(lines)
        else:
            response_text = (
                "I re-read the document carefully but could not find any additional issues "
                "beyond what was already extracted.\n\n"
                "Could you point me to the missing issue — you can quote the text or mention the paragraph?"
            )

        chunk_size = 200
        for i in range(0, len(response_text), chunk_size):
            yield json.dumps({"type": "content", "delta": response_text[i:i+chunk_size]}) + "\n"
        asst_msg = await add_message(session_id, "assistant", response_text, user_id)
        yield json.dumps({
            "type": "retrieval", "sources": [], "full_judgments": {},
            "message_id": getattr(asst_msg, "id", None),
            "session_id": session_id, "id": getattr(asst_msg, "id", None),
        }) + "\n"

    else:
        apply_issue_update(active_case, update)
        issues = active_case.get("issues", [])
        lines  = ["Issues list updated. Current issues:\n"]
        for i in issues:
            status_tag = " ✅" if i.get("reply") else ""
            lines.append(str(i["id"]) + ". " + i["text"] + status_tag)
        if get_pending_issues(active_case):
            lines.append("\n\nShould I generate replies for the updated issue(s)?")

        response_text = "\n".join(lines)
        chunk_size = 200
        for i in range(0, len(response_text), chunk_size):
            yield json.dumps({"type": "content", "delta": response_text[i:i+chunk_size]}) + "\n"
        asst_msg = await add_message(session_id, "assistant", response_text, user_id)
        yield json.dumps({
            "type": "retrieval", "sources": [], "full_judgments": {},
            "message_id": getattr(asst_msg, "id", None),
            "session_id": session_id, "id": getattr(asst_msg, "id", None),
        }) + "\n"


async def _handle_update_reply(
    active_case, issue_id, session_id, user_id,
    profile_summary, background_tasks, ref_text
):
    all_issues = active_case.get("issues", [])
    target     = next((i for i in all_issues if i["id"] == issue_id), None)

    if not target:
        msg = "Issue " + str(issue_id) + " not found."
        yield json.dumps({"type": "content", "delta": msg}) + "\n"
        await add_message(session_id, "assistant", msg, user_id)
        return

    mode            = active_case.get("mode", MODE_DEFENSIVE)
    recipient       = active_case.get("parties", {}).get("recipient")
    sender          = active_case.get("parties", {}).get("sender")
    doc_summary     = active_case.get("summary", "")
    all_issue_texts = [i["text"] for i in all_issues]
    issue_number    = (all_issue_texts.index(target["text"]) + 1
                       if target["text"] in all_issue_texts else 1)

    header = "\n\n---\n\n### Updated Reply — Issue " + str(issue_id) + "\n\n> " + target["text"] + "\n\n"
    yield json.dumps({"type": "content", "delta": header}) + "\n"

    _, reply, sources, full_judgments, usage = await run_in_threadpool(
        _process_single_issue,
        target["text"], issue_number, len(all_issues), all_issue_texts,
        mode, vector_store, ALL_CHUNKS,
        recipient, sender, doc_summary, profile_summary, ref_text,
    )

    chunk_size = 50
    for i in range(0, len(reply), chunk_size):
        yield json.dumps({"type": "content", "delta": reply[i:i+chunk_size]}) + "\n"

    for iss in all_issues:
        if iss["id"] == issue_id:
            iss["reply"]  = reply
            iss["status"] = "user_edited"
            break

    asst_msg = await add_message(
        session_id, "assistant", reply, user_id,
        prompt_tokens=usage.get("inputTokens", 0),
        response_tokens=usage.get("outputTokens", 0)
    )
    yield json.dumps({
        "type": "retrieval", "sources": sources, "full_judgments": full_judgments,
        "message_id": getattr(asst_msg, "id", None),
        "session_id": session_id, "id": getattr(asst_msg, "id", None),
    }) + "\n"
    background_tasks.add_task(auto_update_profile, user_id, target["text"], reply)


async def _handle_query_with_document(
    active_case, question, session_id, user_id,
    history, profile_summary, background_tasks, db
):
    """Follow-up document query (no new file) — uses case summary as context."""
    summary = active_case.get("summary", "")
    doc_ctx = ("[Active case summary: " + summary + "]") if summary else None

    full_answer     = ""
    message_saved   = False
    last_message_id = None

    async for chunk in chat_stream(
        query=question, store=vector_store, all_chunks=ALL_CHUNKS,
        history=history, profile_summary=profile_summary, document_context=doc_ctx,
    ):
        ctype = chunk.get("type")
        if ctype == "content":
            delta = chunk.get("delta", "")
            full_answer += delta
            yield json.dumps({"type": "content", "delta": delta}) + "\n"
        elif ctype == "retrieval":
            if not message_saved:
                asst_msg        = await add_message(session_id, "assistant", full_answer, user_id)
                last_message_id = getattr(asst_msg, "id", None)
                message_id      = last_message_id
                message_saved   = True
            else:
                message_id = None
            yield json.dumps({
                "type":           "retrieval",
                "sources":        chunk.get("sources", []) or [],
                "full_judgments": chunk.get("full_judgments", {}) or {},
                "message_id":     message_id,
                "session_id":     session_id,
                "id":             message_id,
            }) + "\n"
        elif ctype == "citations":
            yield json.dumps({"type": "citations", "party_citations": chunk.get("party_citations", {})}) + "\n"
        elif ctype == "usage":
            usage_dict = chunk.get("usage", {})
            await track_usage(user_id, session_id, db, usage=usage_dict)
            if last_message_id:
                await update_message_tokens(
                    last_message_id,
                    usage_dict.get("inputTokens", 0),
                    usage_dict.get("outputTokens", 0)
                )

    if not message_saved:
        await add_message(session_id, "assistant", full_answer, user_id)
    background_tasks.add_task(auto_update_profile, user_id, question, full_answer)


async def _handle_query_with_full_doc(
    active_case, question, session_id, user_id,
    history, profile_summary, background_tasks, db
):
    """
    Case C — user uploaded a doc AND asked a question about it.
    Fetches full primary text from DB and passes it as document_context.
    chat_stream also hits the knowledge base for anything not in the doc.
    Token tracking identical to regular chat.
    """
    full_doc_text = await get_primary_texts(session_id, active_case["case_id"])
    # Truncate to safe context window size — first 12000 chars
    doc_ctx = full_doc_text[:12000] if full_doc_text.strip() else None

    full_answer     = ""
    message_saved   = False
    last_message_id = None

    async for chunk in chat_stream(
        query=question, store=vector_store, all_chunks=ALL_CHUNKS,
        history=history, profile_summary=profile_summary, document_context=doc_ctx,
    ):
        ctype = chunk.get("type")

        if ctype == "content":
            delta = chunk.get("delta", "")
            full_answer += delta
            yield json.dumps({"type": "content", "delta": delta}) + "\n"

        elif ctype == "retrieval":
            if not message_saved:
                asst_msg        = await add_message(session_id, "assistant", full_answer, user_id)
                last_message_id = getattr(asst_msg, "id", None)
                message_id      = last_message_id
                message_saved   = True
            else:
                message_id = None
            yield json.dumps({
                "type":           "retrieval",
                "sources":        chunk.get("sources", []) or [],
                "full_judgments": chunk.get("full_judgments", {}) or {},
                "message_id":     message_id,
                "session_id":     session_id,
                "id":             message_id,
            }) + "\n"

        elif ctype == "citations":
            yield json.dumps({
                "type":            "citations",
                "party_citations": chunk.get("party_citations", {})
            }) + "\n"

        elif ctype == "usage":
            usage_dict = chunk.get("usage", {})
            await track_usage(user_id, session_id, db, usage=usage_dict)
            if last_message_id:
                await update_message_tokens(
                    last_message_id,
                    usage_dict.get("inputTokens", 0),
                    usage_dict.get("outputTokens", 0)
                )

    if not message_saved:
        await add_message(session_id, "assistant", full_answer, user_id)
    background_tasks.add_task(auto_update_profile, user_id, question, full_answer)


async def _handle_regular_chat(
    question, session_id, user_id, history, profile_summary,
    active_case, background_tasks, db
):
    """Pure GST chat — no document. Scenario 3 preserved exactly."""
    doc_ctx = None
    if active_case:
        summary = active_case.get("summary", "")
        if summary:
            doc_ctx = "[Active case summary: " + summary[:500] + "]"

    full_answer     = ""
    message_saved   = False
    last_message_id = None

    async for chunk in chat_stream(
        query=question, store=vector_store, all_chunks=ALL_CHUNKS,
        history=history, profile_summary=profile_summary, document_context=doc_ctx,
    ):
        ctype = chunk.get("type")

        if ctype == "content":
            delta = chunk.get("delta", "")
            full_answer += delta
            yield json.dumps({"type": "content", "delta": delta}) + "\n"

        elif ctype == "retrieval":
            if not message_saved:
                asst_msg        = await add_message(session_id, "assistant", full_answer, user_id)
                last_message_id = getattr(asst_msg, "id", None)
                message_id      = last_message_id
                message_saved   = True
            else:
                message_id = None
            yield json.dumps({
                "type":           "retrieval",
                "sources":        chunk.get("sources", []) or [],
                "full_judgments": chunk.get("full_judgments", {}) or {},
                "message_id":     message_id,
                "session_id":     session_id,
                "id":             message_id
            }) + "\n"

        elif ctype == "citations":
            yield json.dumps({
                "type":            "citations",
                "party_citations": chunk.get("party_citations", {})
            }) + "\n"

        elif ctype == "usage":
            llm_usage = chunk.get("usage", {})
            await track_usage(user_id, session_id, db, usage=llm_usage)
            if last_message_id:
                await update_message_tokens(
                    last_message_id,
                    llm_usage.get("inputTokens", 0),
                    llm_usage.get("outputTokens", 0)
                )

    if not message_saved:
        await add_message(session_id, "assistant", full_answer, user_id)
    background_tasks.add_task(auto_update_profile, user_id, question, full_answer)


# =============================================================================
# REMAINING ROUTES — unchanged from original
# =============================================================================

@app.post("/chat/feedback")
async def save_feedback(
    payload: FeedbackRequest,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    new_feedback = Feedback(
        message_id=payload.message_id,
        rating=payload.rating,
        comment=payload.comment
    )
    db.add(new_feedback)
    await db.commit()
    return {"status": "recorded"}


@app.get("/chat/history")
async def get_history(
    session_id: str,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    email = user.get("sub")
    res   = await db.execute(select(User).where(func.lower(User.email) == email.lower()))
    db_user = res.scalars().first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")

    res = await db.execute(
        select(ChatSession).where(
            ChatSession.id == session_id,
            ChatSession.user_id == db_user.id
        )
    )
    if not res.scalars().first():
        raise HTTPException(status_code=404, detail="Session not found or unauthorized")

    history = await get_session_history(session_id, limit=50)
    return history


@app.post("/chat/share/session/{session_id}")
async def enable_session_sharing(
    session_id: str,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    try:
        email = user.get("sub")
        res   = await db.execute(
            select(ChatSession)
            .join(User)
            .where(ChatSession.id == session_id, func.lower(User.email) == email.lower())
        )
        session = res.scalars().first()
        if not session:
            exists_res = await db.execute(select(ChatSession).where(ChatSession.id == session_id))
            if not exists_res.scalars().first():
                raise HTTPException(status_code=404, detail="Session not found")
            raise HTTPException(status_code=403, detail="Unauthorized: You do not own this session")

        shared_id = await share_session(session_id, db)
        return {"shared_id": shared_id, "session_id": session_id, "share_url": f"/share/{shared_id}"}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.get("/chat/share/{shared_id}", response_model=SharedSessionResponse)
async def retrieve_shared_session(
    shared_id: str,
    db: AsyncSession = Depends(get_db)
):
    session = await get_shared_session(shared_id, db)
    if not session:
        raise HTTPException(status_code=404, detail="Shared session not found")

    messages = [
        SharedMessageSchema(id=m.id, role=m.role, content=m.content, timestamp=m.timestamp)
        for m in sorted(session.messages, key=lambda x: x.timestamp)
    ]
    return {"session_id": session.id, "title": session.title, "messages": messages}


@app.get("/auth/credits")
async def get_user_credits(
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    email = user.get("sub")
    res = await db.execute(select(User).where(func.lower(User.email) == email.lower()))
    db_user = res.scalars().first()

    res = await db.execute(select(UserUsage).where(UserUsage.user_id == db_user.id))
    usage = res.scalars().first()

    if not usage:
        usage = UserUsage(user_id=db_user.id)
        db.add(usage)
        await db.commit()
        await db.refresh(usage)

    return {
        "balance": {
            "simple": usage.simple_query_balance,
            "draft":  usage.draft_reply_balance
        },
        "used": {
            "simple": usage.simple_query_used,
            "draft":  usage.draft_reply_used
        }
    }


@app.get("/chat/sessions")
async def list_sessions(
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    email = user.get("sub")
    if not email:
        raise HTTPException(status_code=401, detail="Invalid user")

    res = await db.execute(select(User).where(func.lower(User.email) == email.lower()))
    db_user = res.scalars().first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")

    res = await db.execute(
        select(ChatSession)
        .where(ChatSession.user_id == db_user.id)
        .order_by(ChatSession.created_at.desc())
    )
    sessions = res.scalars().all()

    return [
        {
            "id":           s.id,
            "title":        s.title,
            "created_at":   s.created_at.isoformat() if s.created_at else None,
            "session_type": getattr(s, "session_type", "simple")
        }
        for s in sessions
    ]


@app.delete("/chat/session")
async def delete_chat(
    session_id: str,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    email = user.get("sub")
    res   = await db.execute(select(User).where(func.lower(User.email) == email.lower()))
    db_user = res.scalars().first()

    res = await db.execute(
        select(ChatSession).where(
            ChatSession.id == session_id,
            ChatSession.user_id == db_user.id
        )
    )
    if not res.scalars().first():
        raise HTTPException(status_code=404, detail="Session not found or unauthorized")

    from services.memory import delete_session
    await delete_session(session_id)
    await clear_doc_context(session_id)
    await delete_session_documents(session_id)
    return {"status": "deleted"}