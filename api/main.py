import os

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
from services.memory import get_session_history, add_message, get_user_profile, share_message, get_shared_message
from services.models import Feedback, User, ChatSession, UserProfile, ChatMessage
from services.chat.memory_updater import auto_update_profile
from services.document.processor import DocumentProcessor, DocumentAnalyzer
from services.jobs import start_scheduler, stop_scheduler, list_jobs

# ---------------- INIT ---------------- #

app = FastAPI(
    title="GST Expert API",
    version="2.1.0"  # Updated version
)

# ---------------- LIFECYCLE EVENTS ---------------- #

@app.on_event("startup")
async def startup_event():
    """Initialize services on application startup."""
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Starting GST Expert API...")
    
    # Start background job scheduler
    try:
        start_scheduler()
        logger.info("✅ Background jobs initialized")
    except Exception as e:
        logger.error(f"❌ Failed to start scheduler: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on application shutdown."""
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info("👋 Shutting down GST Expert API...")
    
    # Stop background job scheduler
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

# Build Metadata Index at startup (prevents slow first query)
from services.retrieval.citation_matcher import get_index
get_index(ALL_CHUNKS)

# ---------------- DOCUMENT SERVICE INSTANCES ---------------- #

doc_processor = DocumentProcessor()
doc_analyzer = DocumentAnalyzer()

# ---------------- SCHEMAS ---------------- #

class ChatRequest(BaseModel):
    question: str
    session_id: Optional[str] = None
    # Document context: can include extracted text from uploaded documents
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
    """Citation info for a party pair"""
    citation: str
    case_number: str
    petitioner: str
    respondent: str
    external_id: str
    court: str
    year: str
    decision: str

class ChatResponse(BaseModel):
    answer: str  # Enhanced answer with citation attribution appended
    session_id: str
    sources: List[SourceChunk]
    full_judgments: Optional[Dict[str, FullJudgment]] = None
    # NEW: Party citations extracted from response
    party_citations: Optional[Dict[str, List[CitationInfo]]] = None

class FeedbackRequest(BaseModel):
    message_id: int
    rating: int
    comment: Optional[str] = None

class AnalysisResponse(BaseModel):
    """Document analysis response model"""
    success: bool
    extracted_text: str
    structured_analysis: dict
    formatted_response: str
    metadata: dict

class SharedMessageResponse(BaseModel):
    id: str
    role: str
    content: str
    sources: List[SourceChunk] = []
    full_judgments: Dict[str, FullJudgment] = {}
    message_id: int

# ---------------- CHAT ROUTES ---------------- #

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/auth/me")
def me(user=Depends(auth_guard)):
    return {"user": user}


# ---------------- JOB MANAGEMENT ROUTES ---------------- #

@app.get("/admin/jobs")
async def get_scheduled_jobs(user=Depends(auth_guard)):
    """
    List all scheduled background jobs.
    Returns job details including next run time.
    """
    jobs = list_jobs()
    return {
        "jobs": jobs,
        "count": len(jobs)
    }


@app.post("/admin/jobs/feedback/trigger")
async def trigger_feedback_report(user=Depends(auth_guard)):
    """
    Manually trigger the daily feedback report.
    Useful for testing without waiting for the scheduled time.
    """
    from services.jobs.feedback_emailer import send_daily_feedback_report
    
    try:
        await send_daily_feedback_report()
        return {
            "status": "success",
            "message": "Feedback report sent successfully"
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send feedback report: {str(e)}"
        )



@app.post("/chat/ask", response_model=ChatResponse)
async def ask_gst(
    payload: ChatRequest,
    background_tasks: BackgroundTasks,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    # 1. Manage Session
    session_id = payload.session_id or str(uuid.uuid4())
    
    # 2. Get User ID
    email = user.get("sub")
    if not email:
        raise HTTPException(status_code=401, detail="Invalid user")
    
    result = await db.execute(select(User).where(User.email == email))
    db_user = result.scalars().first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    
    user_id = db_user.id

    # 3. Fetch Context (Profile & History)
    profile = await get_user_profile(user_id)
    profile_summary = profile.dynamic_summary if profile else None
    history = await get_session_history(session_id)
    
    # 4. Generate Answer (with citation attribution)
    answer, sources, full_judgments, party_citations_dict = await chat(
        query=payload.question,
        store=vector_store,
        all_chunks=ALL_CHUNKS,
        history=history,
        profile_summary=profile_summary
    )
    
    # Note: 'answer' now includes citation attribution appended at the end
    
    # 5. Save to Memory
    await add_message(session_id, "user", payload.question, user_id)
    await add_message(session_id, "assistant", answer, user_id)

    # 6. Background: Update Profile
    background_tasks.add_task(auto_update_profile, user_id, payload.question, answer)
    
    # 7. Format party citations for response (convert tuple keys to string)
    party_citations_formatted = {}
    for (p1, p2), citations in party_citations_dict.items():
        key = f"{p1} vs {p2}"
        party_citations_formatted[key] = citations

    return {
        "answer": answer,  # Includes citation attribution
        "session_id": session_id,
        "sources": sources,
        "full_judgments": full_judgments if full_judgments else None,
        "party_citations": party_citations_formatted if party_citations_formatted else None
    }


def needs_rag_processing(analysis: dict, user_question: str = None) -> tuple[bool, str]:
    """
    Determine if document analysis needs RAG processing
    
    Returns:
        (needs_rag, query_to_process)
        - needs_rag: True if RAG should be triggered
        - query_to_process: The query string to send to RAG
    """
    # Check if issues exist
    has_issues = analysis.get("issues") and len(analysis.get("issues", [])) > 0
    
    # Check if user question needs knowledge base
    user_response = analysis.get("user_question_response", "")
    needs_knowledge = "It would we better to answer your query using my knowledge?" in user_response or \
                     "Should I resolve your query using my knowledge?" in user_response
    
    if has_issues and needs_knowledge and user_question:
        # Both issues and user question need RAG
        # Combine: user question + issues context
        issues_text = "\n".join([f"{i+1}. {issue}" for i, issue in enumerate(analysis.get("issues", []))])
        query = f"{user_question}\n\nDocument Issues Found:\n{issues_text}"
        return True, query
    
    elif has_issues:
        # Only issues need RAG
        issues_text = "\n".join([f"{i+1}. {issue}" for i, issue in enumerate(analysis.get("issues", []))])
        query = f"Provide guidance for the following legal issues:\n{issues_text}"
        return True, query
    
    elif needs_knowledge and user_question:
        # Only user question needs RAG
        return True, user_question
    
    else:
        # No RAG needed - just return formatted response
        return False, ""


@app.post("/chat/ask/stream")
async def ask_gst_stream(
    background_tasks: BackgroundTasks,   # ✅ FIRST (no default)
    question: str = Form(...),
    session_id: Optional[str] = Form(None),
    files: List[UploadFile] = File(default=[]),
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):




    """
    ✅ INTELLIGENT DOCUMENT + RAG UNIFIED ENDPOINT
    
    Logic:
    1. If documents provided → analyze them
    2. Check if analysis needs RAG:
       - Has issues? → Send to RAG
       - User question needs knowledge? → Send to RAG
    3. If RAG needed:
       - Save extracted text + formatted analysis to memory
       - Run RAG with appropriate query
       - Stream RAG response
       - Save final combined response (formatted + RAG)
    4. If RAG NOT needed:
       - Save extracted text + formatted response to memory
       - Stream formatted response directly
    """
    
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
    document_context = None
    document_metadata = None
    document_analysis = None
    extracted_text = None
    formatted_response = None
    temp_files = []

    if files and len(files) > 0:
        try:
            supported = {'.pdf', '.docx', '.pptx', '.xlsx', '.html', '.png', '.jpg', '.jpeg', '.tiff', '.bmp'}
            file_paths = []
            filenames = []
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

            # Extract text from document(s)
            if len(file_paths) == 1:
                extracted_text = doc_processor.extract_text(file_paths[0])
            else:
                extracted_text = doc_processor.extract_text_from_multiple_files(file_paths, filenames)

            if not extracted_text or not extracted_text.strip():
                raise HTTPException(status_code=422, detail="No text could be extracted from the document(s)")

            # Run document analysis to extract structured insights
            try:
                analysis = doc_analyzer.analyze(extracted_text, user_question=question)
                formatted_response = doc_analyzer.format_response_for_frontend(analysis)
                document_analysis = {
                    "formatted": formatted_response,
                    "structured": analysis
                }
                logger.info(f"✓ Document analysis completed. Formatted length: {len(formatted_response)}")
            except Exception as e:
                logger.error(f"Document analysis failed: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Document analysis error: {str(e)}")

            document_metadata = {
                "num_files": len(files),
                "filenames": filenames,
                "content_types": [f.content_type for f in files],
                "total_size_bytes": total_size,
                "has_analysis": document_analysis is not None
            }

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Document processing error: {str(e)}")
        finally:
            # Cleanup temp files
            for fp in temp_files:
                if fp and os.path.exists(fp):
                    try:
                        os.unlink(fp)
                    except OSError:
                        pass

    # 4. Fetch Context
    profile = await get_user_profile(user_id)
    profile_summary = profile.dynamic_summary if profile else None
    history = await get_session_history(session_id)

    # ============================================================================
    # 5. INTELLIGENT DECISION: Does this need RAG processing?
    # ============================================================================
    
    needs_rag = False
    rag_query = question
    
    if document_analysis:
        needs_rag, rag_query = needs_rag_processing(
            document_analysis["structured"], 
            user_question=question
        )
        logger.info(f"📋 Document Analysis Decision: needs_rag={needs_rag}")
        if needs_rag:
            logger.info(f"📋 RAG Query: {rag_query[:200]}...")

    async def stream_generator():
        """
        Stream generator that handles both scenarios:
        1. Document-only response (no RAG)
        2. Document + RAG combined response
        """
        
        # Prepare user message for memory
        user_message = question
        if document_metadata:
            user_message += f"\n\n[Documents: {', '.join(document_metadata['filenames'])}]"
        
        # Add extracted text to user message if available
        if extracted_text:
            user_message += f"\n\n[Extracted Text]:\n{extracted_text[:1000]}..."  # Truncate for memory
        
        # Save user message to memory
        await add_message(session_id, "user", user_message, user_id)
        
        # ========================================================================
        # SCENARIO 1: Document response WITHOUT RAG
        # ========================================================================
        if not needs_rag and formatted_response:
            logger.info("✅ Streaming document analysis response (no RAG needed)")
            
            # Stream the formatted response in chunks
            chunk_size = 50
            for i in range(0, len(formatted_response), chunk_size):
                chunk_text = formatted_response[i:i+chunk_size]
                yield json.dumps({"type": "content", "delta": chunk_text}) + "\n"
            
            # Save formatted response to memory
            await add_message(session_id, "assistant", formatted_response, user_id)
            
            # Send metadata
            retrieval_obj = {
                "type": "retrieval",
                "sources": [],
                "full_judgments": {},
                "session_id": session_id,
                "document_metadata": document_metadata,
                "document_analysis": document_analysis["structured"] if document_analysis else None
            }
            
            yield json.dumps(retrieval_obj) + "\n"
            
            # Background: update profile
            background_tasks.add_task(auto_update_profile, user_id, question, formatted_response)
            return
        
        # ========================================================================
        # SCENARIO 2: Document response WITH RAG
        # ========================================================================
        if needs_rag:
            logger.info("✅ Streaming combined response: Document Analysis + RAG")
            
            # First, stream the formatted document analysis
            if formatted_response:
                logger.info("📄 Streaming document analysis first...")
                yield json.dumps({
                    "type": "content", 
                    "delta": f"**Document Analysis:**\n\n{formatted_response}\n\n---\n\n**Legal Guidance:**\n\n"
                }) + "\n"
            
            # Now run RAG and stream its response
            logger.info(f"🔍 Running RAG with query: {rag_query[:100]}...")
            
            full_rag_answer = ""
            sources = []
            full_judgments = {}
            party_citations = None
            
            async for chunk in chat_stream(
                query=rag_query,
                store=vector_store,
                all_chunks=ALL_CHUNKS,
                history=history,
                profile_summary=profile_summary,
                document_context=extracted_text  # Pass extracted text as context
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
            
            # Combine formatted response + RAG answer for memory
            combined_answer = formatted_response + "\n\n---\n\n**Legal Guidance:**\n\n" + full_rag_answer
            
            # Save combined response to memory
            assistant_msg = await add_message(session_id, "assistant", combined_answer, user_id)
            message_id = getattr(assistant_msg, "id", None)
            
            # Send retrieval metadata
            retrieval_obj = {
                "type": "retrieval",
                "sources": sources,
                "full_judgments": full_judgments,
                "message_id": message_id,
                "session_id": session_id,
                "id": message_id,
                "document_metadata": document_metadata,
                "document_analysis": document_analysis["structured"] if document_analysis else None
            }
            
            if party_citations:
                retrieval_obj["party_citations"] = party_citations
            
            yield json.dumps(retrieval_obj) + "\n"
            
            # Background: update profile
            background_tasks.add_task(auto_update_profile, user_id, question, combined_answer)
            return
        
        # ========================================================================
        # SCENARIO 3: Regular chat (no documents)
        # ========================================================================
        logger.info("✅ Streaming regular chat response")
        
        full_answer = ""
        sources = []
        full_judgments = {}
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
        
        # Save assistant message
        assistant_msg = await add_message(session_id, "assistant", full_answer, user_id)
        message_id = getattr(assistant_msg, "id", None)
        
        # Send retrieval metadata
        retrieval_obj = {
            "type": "retrieval",
            "sources": sources,
            "full_judgments": full_judgments,
            "message_id": message_id,
            "session_id": session_id,
            "id": message_id
        }
        
        if party_citations:
            retrieval_obj["party_citations"] = party_citations
        
        yield json.dumps(retrieval_obj) + "\n"
        
        # Background: update profile
        background_tasks.add_task(auto_update_profile, user_id, question, full_answer)

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
    user=Depends(auth_guard)
):
    history = await get_session_history(session_id, limit=50)
    return history


@app.post("/chat/share/{message_id}")
async def enable_sharing(
    message_id: int,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    """Mark a message as public and return a shared ID."""
    try:
        email = user.get("sub")
        
        res = await db.execute(
            select(ChatMessage)
            .join(ChatMessage.session)
            .join(ChatSession.user)
            .where(ChatMessage.id == message_id, User.email == email)
        )
        message = res.scalars().first()
        
        if not message:
            exists_res = await db.execute(select(ChatMessage).where(ChatMessage.id == message_id))
            exists = exists_res.scalars().first()
            if not exists:
                raise HTTPException(status_code=404, detail=f"Message {message_id} not found in database")
            raise HTTPException(status_code=403, detail="Unauthorized: You do not own this message")
        
        if message.role != "assistant":
            raise HTTPException(status_code=400, detail="Only assistant messages can be shared")
        
        shared_id = await share_message(message_id, db)
        
        return {
            "shared_id": shared_id,
            "message_id": message_id,
            "share_url": f"/share/{shared_id}"
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"ERROR in enable_sharing: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.get("/chat/share/{shared_id}", response_model=SharedMessageResponse)
async def retrieve_shared_message(
    shared_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Fetch shared message content (Public access)."""
    message = await get_shared_message(shared_id, db)
    
    if not message:
        raise HTTPException(status_code=404, detail="Shared message not found")

    return {
        "id": f"msg_{message.id}",
        "role": message.role,
        "content": message.content,
        "sources": [], # Currently not stored in DB, but required by frontend schema
        "full_judgments": {}, # Currently not stored in DB
        "message_id": message.id
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
        select(ChatSession).where(ChatSession.user_id == db_user.id).order_by(ChatSession.created_at.desc())
    )
    sessions = res.scalars().all()

    out = []
    for s in sessions:
        out.append({
            "id": s.id,
            "title": s.title,
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

