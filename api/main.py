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

INDEX_PATH = "data/vector_store/faiss.index"
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

    profile = await get_user_profile(user_id)
    profile_summary = profile.dynamic_summary if profile else None
    history = await get_session_history(session_id)

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
        key = f"{p1} vs {p2}"
        party_citations_formatted[key] = citations

    return {
        "answer": answer,
        "session_id": session_id,
        "sources": sources,
        "full_judgments": full_judgments if full_judgments else None,
        "party_citations": party_citations_formatted if party_citations_formatted else None
    }


def _build_defensive_issues_query(issues: list, recipient: str = None, sender: str = None, user_question: str = None) -> str:
    """
    Build a RAG query that instructs the LLM to defend the notice recipient.
    """
    issues_numbered = "\n".join(
        f"{i + 1}. {issue}" for i, issue in enumerate(issues)
    )

    recipient_line = f"Notice Recipient (the person to be defended): {recipient}" if recipient else ""
    sender_line    = f"Issuing Authority: {sender}" if sender else ""
    context_block  = "\n".join(filter(None, [recipient_line, sender_line]))

    user_q_block = ""
    if user_question:
        user_q_block = f"\nAdditional query from the recipient: {user_question}\n"

    query = f"""I have received a legal notice / show-cause notice with the following allegations / issues against me. \
Please prepare a strong DEFENSIVE REPLY on my behalf that:

1. Analyses each allegation individually and provides counter-arguments.
2. Cites relevant GST Act sections, Rules, Notifications, Circulars, and GST Council decisions that SUPPORT the recipient's position.
3. References applicable case laws, judgments, or rulings (High Court / Supreme Court / AAR / AAAR / GST Tribunal) where the taxpayer / assessee has been protected or given relief in similar circumstances.
4. Argues that the allegations are legally unsustainable, time-barred, procedurally defective, or factually incorrect — whichever grounds apply.
5. Suggests any procedural remedies or protective steps available (filing a reply, seeking adjournment, writ jurisdiction, etc.).
6. Keeps the tone professional and legally precise, suitable for submission to the authority.

{context_block}

Allegations / Issues raised in the notice:
{issues_numbered}
{user_q_block}
Provide the defensive reply and the legal basis for each counter-argument. \
Prioritise judgments and provisions that have helped taxpayers in similar situations."""

    return query.strip()


def needs_rag_processing(analysis: dict, user_question: str = None) -> tuple[bool, str]:
    """
    Determine if document analysis needs RAG processing.
    For issues: always builds a defensive query protecting the notice recipient.
    """
    has_issues = bool(analysis.get("issues"))

    user_response   = analysis.get("user_question_response", "") or ""
    needs_knowledge = (
        "It would we better to answer your query using my knowledge?" in user_response
        or "Should I resolve your query using my knowledge?" in user_response
    )

    recipient = analysis.get("recipient")
    sender    = analysis.get("sender")

    if has_issues and needs_knowledge and user_question:
        query = _build_defensive_issues_query(
            issues=analysis["issues"],
            recipient=recipient,
            sender=sender,
            user_question=user_question
        )
        return True, query

    elif has_issues:
        query = _build_defensive_issues_query(
            issues=analysis["issues"],
            recipient=recipient,
            sender=sender,
            user_question=None
        )
        return True, query

    elif needs_knowledge and user_question:
        return True, user_question

    else:
        return False, ""


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
    email = user.get("sub")
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
            supported = {'.pdf', '.docx', '.pptx', '.xlsx', '.html', '.png', '.jpg', '.jpeg', '.tiff', '.bmp'}
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
                document_analysis  = {
                    "formatted":  formatted_response,
                    "structured": analysis
                }
                logger.info(f"✓ Document analysis completed. Formatted length: {len(formatted_response)}")
            except Exception as e:
                logger.error(f"Document analysis failed: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Document analysis error: {str(e)}")

            document_metadata = {
                "num_files":        len(files),
                "filenames":        filenames,
                "content_types":    [f.content_type for f in files],
                "total_size_bytes": total_size,
                "has_analysis":     document_analysis is not None
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

    # 5. Decision: does this need RAG?
    needs_rag = False
    rag_query = question

    if document_analysis:
        needs_rag, rag_query = needs_rag_processing(
            document_analysis["structured"],
            user_question=question
        )
        logger.info(f"📋 Document Analysis Decision: needs_rag={needs_rag}")
        if needs_rag:
            logger.info(f"📋 RAG Query (first 300 chars): {rag_query[:300]}...")

    # -------------------------------------------------------------------------
    # Stream generator
    # -------------------------------------------------------------------------
    async def stream_generator():
        """
        Unified stream generator for all three scenarios.

        CHANGES vs previous version:
        1. Entire body wrapped in try/except/finally — any unhandled exception
           is caught, an error event is sent to the client, and the generator
           always exits cleanly instead of silently dying and hanging the
           connection.
        2. {"type": "done"} sentinel is ALWAYS the last event yielded, on
           every code path (normal completion AND error path). This gives
           Postman and the frontend an explicit end-of-stream signal so the
           spinner/loading state stops immediately.
        """
        try:
            # Prepare user message for memory
            user_message = question
            if document_metadata:
                user_message += f"\n\n[Documents: {', '.join(document_metadata['filenames'])}]"
            if extracted_text:
                user_message += f"\n\n[Extracted Text]:\n{extracted_text[:1000]}..."

            await add_message(session_id, "user", user_message, user_id)

            # ----------------------------------------------------------------
            # SCENARIO 1: Document response WITHOUT RAG
            # ----------------------------------------------------------------
            if not needs_rag and formatted_response:
                logger.info("✅ Streaming document analysis response (no RAG needed)")

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
                    "document_analysis": document_analysis["structured"] if document_analysis else None
                }) + "\n"

                background_tasks.add_task(auto_update_profile, user_id, question, formatted_response)
                return

            # ----------------------------------------------------------------
            # SCENARIO 2: Document response WITH RAG (defensive reply)
            # ----------------------------------------------------------------
            if needs_rag:
                logger.info("✅ Streaming combined response: Document Analysis + Defensive RAG")

                if formatted_response:
                    yield json.dumps({
                        "type":  "content",
                        "delta": f"**Document Analysis:**\n\n{formatted_response}\n\n---\n\n**Legal Guidance & Defensive Reply:**\n\n"
                    }) + "\n"

                full_rag_answer = ""
                sources         = []
                full_judgments  = {}
                party_citations = None

                async for chunk in chat_stream(
                    query=rag_query,
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
                        continue

                    if ctype == "retrieval":
                        sources = chunk.get("sources", []) or []
                        continue

                    if ctype == "metadata":
                        full_judgments = chunk.get("full_judgments", {}) or {}
                        continue

                    if ctype == "citations":
                        party_citations = chunk.get("party_citations", {})
                        continue

                combined_answer = (
                    formatted_response
                    + "\n\n---\n\n**Legal Guidance & Defensive Reply:**\n\n"
                    + full_rag_answer
                )

                assistant_msg = await add_message(session_id, "assistant", combined_answer, user_id)
                message_id    = getattr(assistant_msg, "id", None)

                retrieval_obj = {
                    "type":              "retrieval",
                    "sources":           sources,
                    "full_judgments":    full_judgments,
                    "message_id":        message_id,
                    "session_id":        session_id,
                    "id":                message_id,
                    "document_metadata": document_metadata,
                    "document_analysis": document_analysis["structured"] if document_analysis else None
                }

                if party_citations:
                    retrieval_obj["party_citations"] = party_citations

                yield json.dumps(retrieval_obj) + "\n"

                background_tasks.add_task(auto_update_profile, user_id, question, combined_answer)
                return

            # ----------------------------------------------------------------
            # SCENARIO 3: Regular chat (no documents)
            # ----------------------------------------------------------------
            logger.info("✅ Streaming regular chat response")

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
                    continue

                if ctype == "retrieval":
                    sources = chunk.get("sources", []) or []
                    continue

                if ctype == "metadata":
                    full_judgments = chunk.get("full_judgments", {}) or {}
                    continue

                if ctype == "citations":
                    party_citations = chunk.get("party_citations", {})
                    continue

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
            # Any unhandled exception — log it and send an error event to
            # the client so it can stop the loading state gracefully.
            logger.error(f"❌ stream_generator error: {str(e)}", exc_info=True)
            yield json.dumps({
                "type":    "error",
                "message": "An error occurred while generating the response. Please try again."
            }) + "\n"

        finally:
            # Generator always exits cleanly here — the HTTP chunked transfer
            # encoding closes automatically when the generator returns,
            # stopping Postman / frontend loading state without sending any
            # extra event to the client.
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
    # 1. Get user record
    email = user.get("sub")
    res = await db.execute(select(User).where(User.email == email))
    db_user = res.scalars().first()
    
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")

    # 2. Verify session ownership
    res = await db.execute(
        select(ChatSession).where(
            ChatSession.id == session_id,
            ChatSession.user_id == db_user.id
        )
    )
    session = res.scalars().first()
    
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or unauthorized")

    # 3. Fetch history
    history = await get_session_history(session_id, limit=50)
    return history


@app.post("/chat/share/session/{session_id}")
async def enable_session_sharing(
    session_id: str,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    """Mark a full chat session as public and return a shared ID."""
    try:
        email = user.get("sub")
        
        # Verify session ownership
        res = await db.execute(
            select(ChatSession)
            .join(User)
            .where(ChatSession.id == session_id, User.email == email)
        )
        session = res.scalars().first()
        
        if not session:
            # Check if it exists at all
            exists_res = await db.execute(select(ChatSession).where(ChatSession.id == session_id))
            if not exists_res.scalars().first():
                raise HTTPException(status_code=404, detail="Session not found")
            raise HTTPException(status_code=403, detail="Unauthorized: You do not own this session")
        
        shared_id = await share_session(session_id, db)
        return {
            "shared_id": shared_id,
            "session_id": session_id,
            "share_url": f"/share/{shared_id}"
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"ERROR in enable_session_sharing: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.get("/chat/share/{shared_id}", response_model=SharedSessionResponse)
async def retrieve_shared_session(
    shared_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Fetch shared session content including history (Public access)."""
    session = await get_shared_session(shared_id, db)
    
    if not session:
        raise HTTPException(status_code=404, detail="Shared session not found")

    # Format messages
    messages = [
        SharedMessageSchema(
            id=m.id,
            role=m.role,
            content=m.content,
            timestamp=m.timestamp
        ) for m in sorted(session.messages, key=lambda x: x.timestamp)
    ]

    return {
        "session_id": session.id,
        "title": session.title,
        "messages": messages
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

    out = []
    for s in sessions:
        out.append({
            "id":         s.id,
            "title":      s.title,
            "created_at": s.created_at.isoformat() if s.created_at else None
        })

    return out


@app.delete("/chat/session")
async def delete_chat(
    session_id: str,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    email = user.get("sub")
    res = await db.execute(select(User).where(User.email == email))
    db_user = res.scalars().first()

    res = await db.execute(
        select(ChatSession).where(
            ChatSession.id == session_id,
            ChatSession.user_id == db_user.id
        )
    )
    session = res.scalars().first()

    if not session:
        raise HTTPException(status_code=404, detail="Session not found or unauthorized")

    from services.memory import delete_session
    await delete_session(session_id)

    return {"status": "deleted"}