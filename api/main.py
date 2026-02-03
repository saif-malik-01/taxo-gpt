from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional
import json
import uuid

from services.chat.engine import chat, chat_stream
from services.vector.store import VectorStore
from services.auth.deps import auth_guard
from api.auth import router as auth_router
from services.database import get_db, AsyncSession
from services.memory import get_session_history, add_message, get_user_profile
from services.models import Feedback, User, ChatSession, UserProfile
from services.chat.memory_updater import auto_update_profile
from sqlalchemy import select

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

# ---------------- ROUTES ---------------- #

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