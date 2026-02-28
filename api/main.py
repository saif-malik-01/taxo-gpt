import os
from datetime import datetime

os.environ['HF_HUB_DISABLE_SYMLINKS'] = '1'
os.environ['HF_HOME'] = os.path.join(os.path.dirname(__file__), '..', '.hf_cache')

import json
import uuid
import tempfile
import shutil

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional
from sqlalchemy import select
from starlette.concurrency import run_in_threadpool

from services.chat.engine import chat, chat_stream
from services.vector.store import VectorStore
from services.auth.deps import auth_guard
from api.auth import router as auth_router
from api.payments import router as payment_router
from services.database import get_db, AsyncSession
from services.memory import get_session_history, add_message, get_user_profile, share_session, get_shared_session, track_usage, check_credits
from services.models import Feedback, User, ChatSession, UserProfile, ChatMessage, UserUsage
from services.chat.memory_updater import auto_update_profile
from services.document.processor import DocumentProcessor, DocumentAnalyzer
from services.document.issue_replier import process_issues_streaming, MODE_DEFENSIVE, MODE_IN_FAVOUR, detect_mode
from services.jobs import start_scheduler, stop_scheduler, list_jobs

# ---------------- INIT ---------------- #

app = FastAPI(title="GST Expert API", version="2.1.0")

# ---------------- LIFECYCLE EVENTS ---------------- #

@app.on_event("startup")
async def startup_event():
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting GST Expert API...")
    try:
        start_scheduler()
        logger.info("✅ Background jobs initialized")
    except Exception as e:
        logger.error(f"❌ Failed to start scheduler: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    import logging
    logger = logging.getLogger(__name__)
    logger.info("👋 Shutting down GST Expert API...")
    try:
        stop_scheduler()
    except Exception as e:
        logger.error(f"❌ Failed to stop scheduler: {e}")

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
    """Merge source full_judgments into target — no duplicates (keyed by external_id)."""
    for ext_id, judgment in source.items():
        if ext_id not in target:
            target[ext_id] = judgment


def _merge_sources(existing: list, new_sources: list, seen_ids: set) -> None:
    """Append new sources to existing list, deduplicated by id."""
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


@app.post("/chat/ask", response_model=ChatResponse)
async def ask_gst(
    payload: ChatRequest,
    background_tasks: BackgroundTasks,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    session_id = payload.session_id or str(uuid.uuid4())

    email = user.get("sub")
    if not email:
        raise HTTPException(status_code=401, detail="Invalid user")

    result = await db.execute(select(User).where(User.email == email))
    db_user = result.scalars().first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")

    user_id = db_user.id
    
    # --- CREDIT CHECK ---
    allowed, error_msg = await check_credits(user_id, session_id, False, db)
    if not allowed:
        raise HTTPException(status_code=402, detail=error_msg)

    profile         = await get_user_profile(user_id)
    profile_summary = profile.dynamic_summary if profile else None
    history         = await get_session_history(session_id)

    answer, sources, full_judgments, party_citations_dict = await chat(
        query=payload.question,
        store=vector_store,
        all_chunks=ALL_CHUNKS,
        history=history,
        profile_summary=profile_summary
    )

    await add_message(session_id, "user", payload.question, user_id)
    await add_message(session_id, "assistant", answer, user_id)
    
    await track_usage(user_id, session_id, db)

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
    question: str = Form(...),
    session_id: Optional[str] = Form(None),
    files: List[UploadFile] = File(default=[]),
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    return await _ask_gst_stream_core("draft", background_tasks, question, session_id, files, user, db)

@app.post("/chat/stream")
@app.post("/chat/ask/stream")
async def ask_gst_stream_legacy(
    background_tasks: BackgroundTasks,
    question: str = Form(...),
    session_id: Optional[str] = Form(None),
    files: List[UploadFile] = File(default=[]),
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    chat_mode = "draft" if files and any(f.filename for f in files) else "simple"
    return await _ask_gst_stream_core(chat_mode, background_tasks, question, session_id, files, user, db)

async def _ask_gst_stream_core(
    chat_mode: str,
    background_tasks: BackgroundTasks,
    question: str,
    session_id: Optional[str],
    files: List[UploadFile],
    user,
    db: AsyncSession
):
    import logging
    logger = logging.getLogger(__name__)

    session_id = session_id or str(uuid.uuid4())

    email  = user.get("sub")
    result = await db.execute(select(User).where(User.email == email))
    db_user = result.scalars().first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    user_id = db_user.id
 
    # --- CREDIT CHECK ---
    # Robust file detection: some clients send parts with empty filenames
    has_files_pre = any(f.filename for f in files) if files else False
    
    allowed, error_msg = await check_credits(user_id, session_id, has_files_pre, db, chat_mode=chat_mode)
    if not allowed:
        raise HTTPException(status_code=402, detail=error_msg)

    # ---- Save uploaded files to temp paths before stream starts ----
    # File objects are only valid during the request scope; save them now.
    supported  = {'.pdf', '.docx', '.pptx', '.xlsx', '.html', '.png', '.jpg', '.jpeg', '.tiff', '.bmp'}
    temp_file_paths = []  # (tmp_path, ext, filename, content_type, size)
    has_files = files and len(files) > 0

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
            # ----------------------------------------------------------------
            # Save user message immediately
            # ----------------------------------------------------------------
            user_message = question
            if has_files and temp_file_paths:
                filenames = [fp[2] for fp in temp_file_paths]
                user_message += f"\n\n[Documents: {', '.join(filenames)}]"
            
            await add_message(session_id, "user", user_message, user_id, chat_mode=chat_mode)
            
            # TRACK USAGE IMMEDIATELY after message is saved and session is upgraded
            # This ensures we charge for the "Draft" feature as soon as the work begins
            await track_usage(user_id, session_id, db)

            profile         = await get_user_profile(user_id)
            profile_summary = profile.dynamic_summary if profile else None
            history         = await get_session_history(session_id)

            # ----------------------------------------------------------------
            # DOCUMENT PATH — extraction + analysis run in thread pool
            # User sees a status event immediately; no waiting before stream opens
            # ----------------------------------------------------------------
            document_analysis  = None
            extracted_text     = None
            formatted_response = None
            document_metadata  = None
            structured         = {}
            has_issues         = False
            needs_knowledge    = False
            issues_list        = []

            if has_files and temp_file_paths:
                try:
                    file_paths    = [fp[0] for fp in temp_file_paths]
                    filenames     = [fp[2] for fp in temp_file_paths]
                    total_size    = sum(fp[4] for fp in temp_file_paths)
                    content_types = [fp[3] for fp in temp_file_paths]

                    def _extract_and_analyse():
                        """
                        Extraction immediately followed by analysis in one synchronous
                        function — no event-loop round-trip between the two steps.
                        Runs entirely in a single threadpool call.
                        """
                        if len(file_paths) == 1:
                            text = doc_processor.extract_text(file_paths[0])
                        else:
                            text = doc_processor.extract_text_from_multiple_files(file_paths, filenames)

                        if not text or not text.strip():
                            return None, None, None

                        structured_result = doc_analyzer.analyze(text, question)
                        formatted_result  = doc_analyzer.format_response_for_frontend(structured_result)
                        return text, structured_result, formatted_result

                    # Single await — no round-trip between extraction finishing and analysis starting
                    extracted_text, analysis, formatted_response = await run_in_threadpool(_extract_and_analyse)

                    if not extracted_text:
                        yield json.dumps({"type": "error", "message": "No text could be extracted from the document(s)."}) + "\n"
                        return

                    document_analysis = {"formatted": formatted_response, "structured": analysis}

                    document_metadata = {
                        "num_files":        len(temp_file_paths),
                        "filenames":        filenames,
                        "content_types":    content_types,
                        "total_size_bytes": total_size,
                        "has_analysis":     True
                    }

                    structured      = document_analysis["structured"]
                    issues_list     = structured.get("issues") or []
                    has_issues      = len(issues_list) > 0
                    needs_knowledge = _check_needs_knowledge(structured)

                    logger.info(f"✓ Document analysed. Issues: {len(issues_list)}, needs_knowledge: {needs_knowledge}")

                except Exception as e:
                    logger.error(f"Document processing failed: {e}", exc_info=True)
                    yield json.dumps({"type": "error", "message": f"Document processing error: {str(e)}"}) + "\n"
                    return
                finally:
                    for tp, *_ in temp_file_paths:
                        if os.path.exists(tp):
                            try: os.unlink(tp)
                            except OSError: pass

            # ----------------------------------------------------------------
            # SCENARIO 1: Document — no issues, no knowledge needed
            # Stream formatted analysis directly
            # ----------------------------------------------------------------
            if document_analysis and not has_issues and not needs_knowledge:
                logger.info("✅ Scenario 1: Document analysis only (no RAG)")

                chunk_size = 200
                for i in range(0, len(formatted_response), chunk_size):
                    yield json.dumps({"type": "content", "delta": formatted_response[i:i+chunk_size]}) + "\n"

                await add_message(session_id, "assistant", formatted_response, user_id)

                yield json.dumps({
                    "type":              "retrieval",
                    "sources":           [],
                    "full_judgments":    {},
                    "message_id":        None,
                    "session_id":        session_id,
                    "id":                None,
                    "document_metadata": document_metadata,
                    "document_analysis": structured
                }) + "\n"

                background_tasks.add_task(auto_update_profile, user_id, question, formatted_response)
                return

            # ----------------------------------------------------------------
            # SCENARIO 2a: Document HAS ISSUES
            # Parallel processing, ordered streaming, formatted per issue
            # Sources + full_judgments aggregated and returned in retrieval event
            # ----------------------------------------------------------------
            if has_issues:
                logger.info(f"✅ Scenario 2a: {len(issues_list)} issues — parallel processing, ordered streaming")

                # Stream document analysis header
                if formatted_response:
                    chunk_size = 200
                    header_text = f"**Document Analysis:**\n\n{formatted_response}\n\n---\n\n"
                    for i in range(0, len(header_text), chunk_size):
                        yield json.dumps({"type": "content", "delta": header_text[i:i+chunk_size]}) + "\n"

                mode      = detect_mode(question)
                recipient = structured.get("recipient")
                sender    = structured.get("sender")
                total     = len(issues_list)

                full_issues_text  = ""
                all_sources       = []
                all_full_judgments = {}
                seen_source_ids   = set()

                async for issue_number, reply, sources, full_judgments in process_issues_streaming(
                    issues=issues_list,
                    mode=mode,
                    store=vector_store,
                    all_chunks=ALL_CHUNKS,
                    recipient=recipient,
                    sender=sender,
                    profile_summary=profile_summary,
                    max_parallel=3
                ):
                    issue_text = issues_list[issue_number - 1]

                    # ---- Issue header (formatted) ----
                    header = f"\n\n---\n\n### Issue {issue_number} of {total}\n\n> {issue_text}\n\n"
                    yield json.dumps({"type": "content", "delta": header}) + "\n"

                    # Signal start to frontend (for any UI tracking)
                    yield json.dumps({
                        "type":         "issue_start",
                        "issue_number": issue_number,
                        "issue_text":   issue_text,
                        "total_issues": total
                    }) + "\n"

                    # Stream reply in chunks
                    chunk_size = 50
                    for i in range(0, len(reply), chunk_size):
                        yield json.dumps({"type": "content", "delta": reply[i:i+chunk_size]}) + "\n"

                    # Signal end to frontend
                    yield json.dumps({
                        "type":         "issue_end",
                        "issue_number": issue_number
                    }) + "\n"

                    full_issues_text += f"\n\n---\n\n### Issue {issue_number}: {issue_text}\n\n{reply}"

                    # Aggregate sources (deduplicated)
                    _merge_sources(all_sources, sources, seen_source_ids)

                    # Aggregate full judgments (deduplicated by external_id)
                    _merge_full_judgments(all_full_judgments, full_judgments)

                # ---- Single closing block after all issues ----
                closing_block = (
                    "\n\n---\n\n"
                    "**Respectfully submitted.**\n\n"
                    f"*For {recipient or 'the Taxpayer'}*\n\n"
                    "Authorised Signatory / Chartered Accountant / Legal Representative\n\n"
                    "Date: [Insert Date]"
                )
                chunk_size = 50
                for i in range(0, len(closing_block), chunk_size):
                    yield json.dumps({"type": "content", "delta": closing_block[i:i+chunk_size]}) + "\n"
                full_issues_text += closing_block

                # ---- Knowledge base supplement if needed ----
                if needs_knowledge:
                    yield json.dumps({
                        "type":  "content",
                        "delta": "\n\n---\n\n**Answer to your query:**\n\n"
                    }) + "\n"

                    knowledge_answer = ""
                    async for chunk in chat_stream(
                        query=question,
                        store=vector_store,
                        all_chunks=ALL_CHUNKS,
                        history=history,
                        profile_summary=profile_summary,
                        document_context=extracted_text
                    ):
                        ctype = chunk.get("type")
                        if ctype == "content":
                            delta = chunk.get("delta", "")
                            knowledge_answer += delta
                            yield json.dumps({"type": "content", "delta": delta}) + "\n"
                        elif ctype == "retrieval":
                            _merge_sources(all_sources, chunk.get("sources", []) or [], seen_source_ids)
                            _merge_full_judgments(all_full_judgments, chunk.get("full_judgments", {}) or {})

                    full_issues_text += f"\n\n---\n\n**Answer to your query:**\n\n{knowledge_answer}"

                # Save combined answer
                combined_answer = (formatted_response or "") + full_issues_text
                assistant_msg   = await add_message(session_id, "assistant", combined_answer, user_id)
                message_id      = getattr(assistant_msg, "id", None)

                # Retrieval event — sources + full_judgments in same shape as non-document
                yield json.dumps({
                    "type":              "retrieval",
                    "sources":           all_sources,
                    "full_judgments":    all_full_judgments,
                    "message_id":        message_id,
                    "session_id":        session_id,
                    "id":                message_id,
                    "document_metadata": document_metadata,
                    "document_analysis": structured
                }) + "\n"

                background_tasks.add_task(auto_update_profile, user_id, question, combined_answer)
                return

            # ----------------------------------------------------------------
            # SCENARIO 2b: Document — no issues but knowledge needed
            # ----------------------------------------------------------------
            if document_analysis and needs_knowledge:
                logger.info("✅ Scenario 2b: Document + knowledge base")

                if formatted_response:
                    yield json.dumps({
                        "type":  "content",
                        "delta": f"**Document Analysis:**\n\n{formatted_response}\n\n---\n\n**Answer to your query:**\n\n"
                    }) + "\n"

                full_rag_answer = ""

                async for chunk in chat_stream(
                    query=question,
                    store=vector_store,
                    all_chunks=ALL_CHUNKS,
                    history=history,
                    profile_summary=profile_summary,
                    document_context=extracted_text
                ):
                    ctype = chunk.get("type")
                    if ctype == "content":
                        delta = chunk.get("delta", "")
                        full_rag_answer += delta
                        yield json.dumps({"type": "content", "delta": delta}) + "\n"
                    elif ctype == "retrieval":
                        # Yield retrieval immediately — no buffering
                        combined_answer = (formatted_response or "") + "\n\n---\n\n**Answer to your query:**\n\n" + full_rag_answer
                        assistant_msg   = await add_message(session_id, "assistant", combined_answer, user_id)
                        message_id      = getattr(assistant_msg, "id", None)

                        retrieval_obj = {
                            "type":              "retrieval",
                            "sources":           chunk.get("sources", []) or [],
                            "full_judgments":    chunk.get("full_judgments", {}) or {},
                            "message_id":        message_id,
                            "session_id":        session_id,
                            "id":                message_id,
                            "document_metadata": document_metadata,
                            "document_analysis": structured
                        }
                        yield json.dumps(retrieval_obj) + "\n"

                    elif ctype == "citations":
                        yield json.dumps({
                            "type":           "citations",
                            "party_citations": chunk.get("party_citations", {})
                        }) + "\n"

                background_tasks.add_task(
                    auto_update_profile, user_id, question,
                    (formatted_response or "") + "\n\n" + full_rag_answer
                )
                return

            # ----------------------------------------------------------------
            # SCENARIO 3: Regular chat — no document
            # Yield retrieval/metadata/citations inline as they arrive (no buffering)
            # This eliminates the pause between content end and retrieval event
            # ----------------------------------------------------------------
            logger.info("✅ Scenario 3: Regular chat")

            full_answer = ""
            message_saved = False

            async for chunk in chat_stream(
                query=question,
                store=vector_store,
                all_chunks=ALL_CHUNKS,
                history=history,
                profile_summary=profile_summary,
                document_context=None
            ):
                ctype = chunk.get("type")

                if ctype == "content":
                    delta = chunk.get("delta", "")
                    full_answer += delta
                    yield json.dumps({"type": "content", "delta": delta}) + "\n"

                elif ctype == "retrieval":
                    # Save message once — get message_id for the retrieval event
                    if not message_saved:
                        assistant_msg = await add_message(session_id, "assistant", full_answer, user_id)
                        message_id    = getattr(assistant_msg, "id", None)
                        message_saved = True
                    else:
                        message_id = None

                    # Yield retrieval immediately — no pause
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
                        "type":           "citations",
                        "party_citations": chunk.get("party_citations", {})
                    }) + "\n"

            # Ensure message is saved even if retrieval event never came
            if not message_saved:
                assistant_msg = await add_message(session_id, "assistant", full_answer, user_id)

            # track_usage moved to beginning of stream_generator

            background_tasks.add_task(auto_update_profile, user_id, question, full_answer)

        except Exception as e:
            logger.error(f"❌ stream_generator error: {str(e)}", exc_info=True)
            yield json.dumps({
                "type":    "error",
                "message": "An error occurred while generating the response. Please try again."
            }) + "\n"

        finally:
            # Clean up any leftover temp files (safety net)
            for tp, *_ in temp_file_paths:
                if os.path.exists(tp):
                    try: os.unlink(tp)
                    except OSError: pass
            logger.info(f"✅ Stream closed cleanly for session {session_id}")

    return StreamingResponse(stream_generator(), media_type="application/x-ndjson")


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
    res   = await db.execute(select(User).where(User.email == email))
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
            .where(ChatSession.id == session_id, User.email == email)
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
    res = await db.execute(select(User).where(User.email == email))
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
            "draft": usage.draft_reply_balance
        },
        "used": {
            "simple": usage.simple_query_used,
            "draft": usage.draft_reply_used
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

    res = await db.execute(select(User).where(User.email == email))
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
            "id": s.id,
            "title": s.title,
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "session_type": getattr(s, "session_type", "simple")  # Fallback just in case
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
    res   = await db.execute(select(User).where(User.email == email))
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
    return {"status": "deleted"}