import os
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
from services.memory import get_session_history, add_message, get_user_profile
from services.models import Feedback, User, ChatSession, UserProfile
from services.chat.memory_updater import auto_update_profile
from services.document.processor import DocumentProcessor, DocumentAnalyzer

# ---------------- INIT ---------------- #

app = FastAPI(
    title="GST Expert API",
    version="2.1.0"  # Updated version
)

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

# ---------------- CHAT ROUTES ---------------- #

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/auth/me")
def me(user=Depends(auth_guard)):
    return {"user": user}


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


@app.post("/chat/ask/stream")
async def ask_gst_stream(
    payload: ChatRequest,
    background_tasks: BackgroundTasks,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    # 1. Manage Session
    session_id = payload.session_id or str(uuid.uuid4())
    
    # 2. Get User ID
    email = user.get("sub")
    result = await db.execute(select(User).where(User.email == email))
    db_user = result.scalars().first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    
    user_id = db_user.id

    # 3. Fetch Context
    profile = await get_user_profile(user_id)
    profile_summary = profile.dynamic_summary if profile else None
    history = await get_session_history(session_id)

    async def stream_generator():
        full_answer = ""
        # 4. Save User Query
        await add_message(session_id, "user", payload.question, user_id)

        # containers to capture metadata
        sources = []
        full_judgments = {}
        party_citations = None

        # 5. Stream response: send content chunks immediately
        async for chunk in chat_stream(
            query=payload.question,
            store=vector_store,
            all_chunks=ALL_CHUNKS,
            history=history,
            profile_summary=profile_summary
        ):
            ctype = chunk.get("type")

            if ctype == "content":
                delta = chunk.get("delta", "")
                full_answer += delta
                yield json.dumps({"type": "content", "delta": delta}) + "\n"
                continue

            # capture retrieval sources (do not forward as separate line)
            if ctype == "retrieval":
                sources = chunk.get("sources", []) or []
                # keep session_id for clients if needed
                continue

            if ctype == "metadata":
                full_judgments = chunk.get("full_judgments", {}) or {}
                continue

            if ctype == "citations":
                party_citations = chunk.get("party_citations", {})
                continue

            # forward anything else unchanged
            yield json.dumps(chunk) + "\n"

        # 6. After streaming, save assistant message to DB to obtain message_id
        assistant_msg = await add_message(session_id, "assistant", full_answer, user_id)
        message_id = getattr(assistant_msg, "id", None)

        # 7. Emit retrieval/metadata line as final NDJSON object
        retrieval_obj = {
            "type": "retrieval",
            "sources": sources,
            "full_judgments": full_judgments,
            "message_id": message_id,
            "session_id": session_id,
            "id": message_id
        }

        # include citations if available
        if party_citations:
            retrieval_obj["party_citations"] = party_citations

        yield json.dumps(retrieval_obj) + "\n"

        # 8. Background task: update profile
        background_tasks.add_task(auto_update_profile, user_id, payload.question, full_answer)

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


@app.get("/chat/sessions")
async def list_sessions(
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    # Return list of sessions for the authenticated user
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


# ---------------- DOCUMENT ROUTES ---------------- #

@app.post("/document/analyze", response_model=AnalysisResponse)
async def analyze_document(
    files: List[UploadFile] = File(..., description="Legal document(s) - PDF, DOCX, PPTX, XLSX, HTML, Images"),
    user_question: Optional[str] = Form(None, description="Optional: Question about the document(s)")
):
    """Analyze single or multiple legal documents with extraction, summary, and issue detection."""

    temp_files = []

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
                    detail=f"Unsupported file format for '{file.filename}'. Supported: {', '.join(sorted(supported))}"
                )

            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                temp_files.append(tmp.name)
                shutil.copyfileobj(file.file, tmp)

                file_paths.append(tmp.name)
                filenames.append(file.filename)
                total_size += os.path.getsize(tmp.name)

        # Step 1: Extract text
        if len(file_paths) == 1:
            extracted_text = doc_processor.extract_text(file_paths[0])
        else:
            extracted_text = doc_processor.extract_text_from_multiple_files(file_paths, filenames)

        if not extracted_text.strip():
            raise HTTPException(status_code=422, detail="No text could be extracted from the document(s)")

        # Step 2: Analyze
        structured_analysis = doc_analyzer.analyze(extracted_text, user_question)

        # Step 3: Format for frontend
        formatted_response = doc_analyzer.format_response_for_frontend(structured_analysis)

        return AnalysisResponse(
            success=True,
            extracted_text=extracted_text,
            structured_analysis=structured_analysis,
            formatted_response=formatted_response,
            metadata={
                "num_files": len(files),
                "filenames": [f.filename for f in files],
                "content_types": [f.content_type for f in files],
                "total_size_bytes": total_size,
                "llm_model": "qwen.qwen3-next-80b-a3b",
                "max_tokens": 32000
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")
    finally:
        for fp in temp_files:
            if fp and os.path.exists(fp):
                try:
                    os.unlink(fp)
                except OSError:
                    pass


@app.get("/document/supported-formats")
async def supported_formats():
    """List supported document formats"""
    return {
        "supported_formats": [
            "PDF (.pdf)",
            "Word (.docx)",
            "PowerPoint (.pptx)",
            "Excel (.xlsx)",
            "HTML (.html)",
            "Images (.png, .jpg, .jpeg, .tiff, .bmp)"
        ],
        "multi_file_support": True,
        "note": "You can upload multiple files at once. They will be combined and analyzed together."
    }


@app.get("/document/issue-detection-criteria")
async def issue_detection_criteria():
    """Explain issue detection criteria"""
    return {
        "issues_included": [
            "Formal allegations or charges against recipient",
            "Legal violations or statutory non-compliance",
            "Show cause notices",
            "Penalties, fines, or recovery actions",
            "Legal disputes or cases filed",
            "Regulatory breaches"
        ],
        "issues_excluded": [
            "Administrative discrepancies",
            "Routine compliance notifications",
            "Informational notices",
            "Requests for clarification/explanation",
            "System-generated difference intimations (without formal allegations)",
            "Procedural notifications"
        ],
        "example": {
            "included_as_issue": "Notice u/s 74 for tax evasion with penalty of Rs 1,00,000",
            "not_included_as_issue": "Intimation of difference in GSTR-1 vs GSTR-3B (requesting clarification)"
        }
    }


@app.get("/document/response-structure")
async def response_structure():
    """Explain the response structure"""
    return {
        "response_fields": {
            "extracted_text": "Full combined text from all documents with [TABLE_JSON] sections (cleaned and normalized)",
            "structured_analysis": "JSON object with individual fields (clean, no empty fields)",
            "formatted_response": "Single clean text merging all non-empty fields (ready for frontend display)",
            "metadata": "File and processing information for all documents"
        },
        "structured_analysis_fields": {
            "sender": "Present only if explicitly mentioned in document(s)",
            "recipient": "Present only if explicitly mentioned in document(s)",
            "summary": "Always present - comprehensive 4-7 sentence summary of entire document(s)",
            "user_question_response": "Present only if user asked a question",
            "issues": "Present only if genuine allegations/violations found",
            "issues_prompt": "Present only when issues are present"
        },
        "formatted_response_structure": {
            "description": "Simple clean text merging all non-empty fields",
            "format": [
                "From: ... (if sender present)",
                "To: ... (if recipient present)",
                "",
                "Summary text covering all documents...",
                "",
                "Answer: ... (if user asked question)",
                "",
                "Issues: (if issues present)",
                "1. Issue one",
                "2. Issue two",
                "",
                "Should I prepare the reply or guide for these issues?"
            ]
        },
        "multi_file_processing": {
            "description": "When multiple files are uploaded",
            "process": [
                "1. Each document is extracted separately",
                "2. Documents are combined with clear separators",
                "3. Combined text shows: DOCUMENT 1: filename.pdf, DOCUMENT 2: filename2.pdf, etc.",
                "4. Analyzer processes all documents together",
                "5. Summary and analysis cover all documents comprehensively"
            ]
        },
        "usage": {
            "for_json_processing": "Use structured_analysis field",
            "for_frontend_display": "Use formatted_response field (clean merged text, ready to display)"
        }
    }