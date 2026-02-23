import os
from datetime import datetime

# Configure Hugging Face cache for cross-platform compatibility
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

from services.chat.engine import chat, chat_stream
from services.vector.store import VectorStore
from services.auth.deps import auth_guard
from api.auth import router as auth_router
from services.database import get_db, AsyncSession
from services.memory import get_session_history, add_message, get_user_profile, share_session, get_shared_session
from services.models import Feedback, User, ChatSession, UserProfile, ChatMessage
from services.chat.memory_updater import auto_update_profile
from services.document.processor import DocumentProcessor, DocumentAnalyzer
from services.document.issue_replier import process_issues_parallel, MODE_DEFENSIVE, MODE_IN_FAVOUR
from services.jobs import start_scheduler, stop_scheduler, list_jobs

# ---------------- INIT ---------------- #

app = FastAPI(
    title="GST Expert API",
    version="2.1.0"
)

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
        logger.info("✅ Background jobs stopped")
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
    """Check if user question needs knowledge base (not issues-related)."""
    user_response = analysis.get("user_question_response", "") or ""
    return (
        "It would we better to answer your query using my knowledge?" in user_response
        or "Should I resolve your query using my knowledge?" in user_response
    )

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


@app.post("/chat/ask/stream")
async def ask_gst_stream(
    background_tasks: BackgroundTasks,
    question: str = Form(...),
    session_id: Optional[str] = Form(None),
    files: List[UploadFile] = File(default=[]),
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    import logging
    logger = logging.getLogger(__name__)

    # 1. Manage Session
    session_id = session_id or str(uuid.uuid4())

    # 2. Get User ID
    email  = user.get("sub")
    result = await db.execute(select(User).where(User.email == email))
    db_user = result.scalars().first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    user_id = db_user.id

    # 3. Process documents if provided
    document_context   = None
    document_metadata  = None
    document_analysis  = None
    extracted_text     = None
    formatted_response = None
    temp_files         = []

    if files and len(files) > 0:
        try:
            supported  = {'.pdf', '.docx', '.pptx', '.xlsx', '.html', '.png', '.jpg', '.jpeg', '.tiff', '.bmp'}
            file_paths = []
            filenames  = []
            total_size = 0

            for file in files:
                ext = os.path.splitext(file.filename)[1].lower()
                if ext not in supported:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Unsupported file format: {file.filename}. Supported: {', '.join(sorted(supported))}"
                    )
                with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                    temp_files.append(tmp.name)
                    shutil.copyfileobj(file.file, tmp)
                    file_paths.append(tmp.name)
                    filenames.append(file.filename)
                    total_size += os.path.getsize(tmp.name)

            if len(file_paths) == 1:
                extracted_text = doc_processor.extract_text(file_paths[0])
            else:
                extracted_text = doc_processor.extract_text_from_multiple_files(file_paths, filenames)

            if not extracted_text or not extracted_text.strip():
                raise HTTPException(status_code=422, detail="No text could be extracted from the document(s)")

            try:
                analysis           = doc_analyzer.analyze(extracted_text, user_question=question)
                formatted_response = doc_analyzer.format_response_for_frontend(analysis)
                document_analysis  = {"formatted": formatted_response, "structured": analysis}
                logger.info(f"✓ Document analysis completed. Formatted length: {len(formatted_response)}")
            except Exception as e:
                logger.error(f"Document analysis failed: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Document analysis error: {str(e)}")

            document_metadata = {
                "num_files":        len(files),
                "filenames":        filenames,
                "content_types":    [f.content_type for f in files],
                "total_size_bytes": total_size,
                "has_analysis":     True
            }

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Document processing error: {str(e)}")
        finally:
            for fp in temp_files:
                if fp and os.path.exists(fp):
                    try:
                        os.unlink(fp)
                    except OSError:
                        pass

    # 4. Fetch Context
    profile         = await get_user_profile(user_id)
    profile_summary = profile.dynamic_summary if profile else None
    history         = await get_session_history(session_id)

    # 5. Determine routing
    has_issues      = False
    needs_knowledge = False
    issues_list     = []
    structured      = {}

    if document_analysis:
        structured      = document_analysis["structured"]
        issues_list     = structured.get("issues") or []
        has_issues      = len(issues_list) > 0
        needs_knowledge = _check_needs_knowledge(structured)

    # -------------------------------------------------------------------------
    # Stream generator
    # -------------------------------------------------------------------------
    async def stream_generator():
        try:
            # Save user message
            user_message = question
            if document_metadata:
                user_message += f"\n\n[Documents: {', '.join(document_metadata['filenames'])}]"
            if extracted_text:
                user_message += f"\n\n[Extracted Text]:\n{extracted_text[:1000]}..."
            await add_message(session_id, "user", user_message, user_id)

            # ----------------------------------------------------------------
            # SCENARIO 1: Document with NO issues and NO knowledge needed
            # Stream the formatted analysis directly — no RAG required
            # ----------------------------------------------------------------
            if document_analysis and not has_issues and not needs_knowledge:
                logger.info("✅ Scenario 1: Streaming document analysis (no RAG needed)")

                chunk_size = 50
                for i in range(0, len(formatted_response), chunk_size):
                    yield json.dumps({"type": "content", "delta": formatted_response[i:i+chunk_size]}) + "\n"

                await add_message(session_id, "assistant", formatted_response, user_id)

                yield json.dumps({
                    "type":              "retrieval",
                    "sources":           [],
                    "full_judgments":    {},
                    "session_id":        session_id,
                    "document_metadata": document_metadata,
                    "document_analysis": structured
                }) + "\n"

                background_tasks.add_task(auto_update_profile, user_id, question, formatted_response)
                return

            # ----------------------------------------------------------------
            # SCENARIO 2a: Document HAS ISSUES
            # Process each issue independently in parallel (max 5 concurrent).
            # Stream results sequentially — one issue fully streamed before next.
            # Default mode: DEFENSIVE (protects the notice recipient).
            # ----------------------------------------------------------------
            if has_issues:
                logger.info(f"✅ Scenario 2a: {len(issues_list)} issues — parallel processing, sequential streaming")

                # Stream document analysis header first
                if formatted_response:
                    yield json.dumps({
                        "type":  "content",
                        "delta": f"**Document Analysis:**\n\n{formatted_response}\n\n---\n\n"
                    }) + "\n"

                # Determine mode — default is defensive
                mode      = MODE_DEFENSIVE
                recipient = structured.get("recipient")
                sender    = structured.get("sender")

                # Process ALL issues in parallel — all running simultaneously
                logger.info(f"🚀 Launching parallel processing for {len(issues_list)} issues...")
                issue_replies = await process_issues_parallel(
                    issues=issues_list,
                    mode=mode,
                    store=vector_store,
                    all_chunks=ALL_CHUNKS,
                    recipient=recipient,
                    sender=sender,
                    profile_summary=profile_summary,
                    max_parallel=3
                )

                # Stream sequentially — one issue at a time, no mixing
                full_issues_text = ""
                for issue_num in range(1, len(issues_list) + 1):
                    reply = issue_replies.get(issue_num, "[Reply not available]")

                    # Signal start of this issue to frontend
                    yield json.dumps({
                        "type":         "issue_start",
                        "issue_number": issue_num,
                        "issue_text":   issues_list[issue_num - 1],
                        "total_issues": len(issues_list)
                    }) + "\n"

                    # Stream the reply for this issue in chunks
                    chunk_size = 50
                    for i in range(0, len(reply), chunk_size):
                        yield json.dumps({"type": "content", "delta": reply[i:i+chunk_size]}) + "\n"

                    # Signal end of this issue
                    yield json.dumps({
                        "type":         "issue_end",
                        "issue_number": issue_num
                    }) + "\n"

                    full_issues_text += f"\n\n**Issue {issue_num}:** {issues_list[issue_num - 1]}\n\n{reply}"

                # Single closing block after ALL issues — not repeated per issue
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

                # If user also asked a question needing knowledge, answer it too
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

                    full_issues_text += f"\n\n---\n\n**Answer to your query:**\n\n{knowledge_answer}"
                print("i am printing the complete reply",full_issues_text)
                # Save combined answer to memory
                combined_answer = formatted_response + full_issues_text
                assistant_msg   = await add_message(session_id, "assistant", combined_answer, user_id)
                message_id      = getattr(assistant_msg, "id", None)

                yield json.dumps({
                    "type":              "retrieval",
                    "sources":           [],
                    "full_judgments":    {},
                    "message_id":        message_id,
                    "session_id":        session_id,
                    "id":                message_id,
                    "document_metadata": document_metadata,
                    "document_analysis": structured
                }) + "\n"

                background_tasks.add_task(auto_update_profile, user_id, question, combined_answer)
                return

            # ----------------------------------------------------------------
            # SCENARIO 2b: Document with NO issues but user question
            # needs the knowledge base — use existing chat_stream
            # ----------------------------------------------------------------
            if document_analysis and needs_knowledge:
                logger.info("✅ Scenario 2b: Document + knowledge base for user question")

                if formatted_response:
                    yield json.dumps({
                        "type":  "content",
                        "delta": f"**Document Analysis:**\n\n{formatted_response}\n\n---\n\n**Answer to your query:**\n\n"
                    }) + "\n"

                full_rag_answer = ""
                sources         = []
                full_judgments  = {}
                party_citations = None

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
                        sources = chunk.get("sources", []) or []
                    elif ctype == "metadata":
                        full_judgments = chunk.get("full_judgments", {}) or {}
                    elif ctype == "citations":
                        party_citations = chunk.get("party_citations", {})

                combined_answer = formatted_response + "\n\n---\n\n**Answer to your query:**\n\n" + full_rag_answer
                assistant_msg   = await add_message(session_id, "assistant", combined_answer, user_id)
                message_id      = getattr(assistant_msg, "id", None)

                retrieval_obj = {
                    "type":              "retrieval",
                    "sources":           sources,
                    "full_judgments":    full_judgments,
                    "message_id":        message_id,
                    "session_id":        session_id,
                    "id":                message_id,
                    "document_metadata": document_metadata,
                    "document_analysis": structured
                }
                if party_citations:
                    retrieval_obj["party_citations"] = party_citations

                yield json.dumps(retrieval_obj) + "\n"

                background_tasks.add_task(auto_update_profile, user_id, question, combined_answer)
                return

            # ----------------------------------------------------------------
            # SCENARIO 3: Regular chat — no document, use existing chat_stream
            # This path is completely unchanged from the original flow
            # ----------------------------------------------------------------
            logger.info("✅ Scenario 3: Regular chat")

            full_answer     = ""
            sources         = []
            full_judgments  = {}
            party_citations = None

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
                    sources = chunk.get("sources", []) or []
                elif ctype == "metadata":
                    full_judgments = chunk.get("full_judgments", {}) or {}
                elif ctype == "citations":
                    party_citations = chunk.get("party_citations", {})

            assistant_msg = await add_message(session_id, "assistant", full_answer, user_id)
            message_id    = getattr(assistant_msg, "id", None)

            retrieval_obj = {
                "type":           "retrieval",
                "sources":        sources,
                "full_judgments": full_judgments,
                "message_id":     message_id,
                "session_id":     session_id,
                "id":             message_id
            }
            if party_citations:
                retrieval_obj["party_citations"] = party_citations

            yield json.dumps(retrieval_obj) + "\n"

            background_tasks.add_task(auto_update_profile, user_id, question, full_answer)

        except Exception as e:
            logger.error(f"❌ stream_generator error: {str(e)}", exc_info=True)
            yield json.dumps({
                "type":    "error",
                "message": "An error occurred while generating the response. Please try again."
            }) + "\n"

        finally:
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
        {"id": s.id, "title": s.title, "created_at": s.created_at.isoformat() if s.created_at else None}
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