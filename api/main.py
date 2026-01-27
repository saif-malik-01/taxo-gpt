# from fastapi import FastAPI, Depends
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel
# from typing import List
# import json

# from services.chat.engine import chat
# from services.vector.store import VectorStore
# from services.auth.deps import auth_guard
# from api.auth import router as auth_router

# # ---------------- INIT ---------------- #

# app = FastAPI(
#     title="GST Expert API",
#     version="1.1.0"
# )

# # ---------------- CORS ---------------- #

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],  # 🔒 change to specific domains in production
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # ---------------- ROUTERS ---------------- #

# app.include_router(auth_router)

# # ---------------- DATA ---------------- #

# INDEX_PATH = "data/vector_store/faiss.index"
# CHUNKS_PATH = "data/processed/all_chunks.json"

# vector_store = VectorStore(INDEX_PATH, CHUNKS_PATH)

# with open(CHUNKS_PATH, "r", encoding="utf-8") as f:
#     ALL_CHUNKS = json.load(f)

# # ---------------- SCHEMAS ---------------- #

# class ChatRequest(BaseModel):
#     question: str


# class SourceChunk(BaseModel):
#     id: str
#     chunk_type: str
#     text: str
#     metadata: dict


# class ChatResponse(BaseModel):
#     answer: str
#     sources: List[SourceChunk]

# # ---------------- ROUTES ---------------- #

# @app.get("/health")
# def health():
#     return {"status": "ok"}


# @app.get("/auth/me")
# def me(user=Depends(auth_guard)):
#     return {"user": user}


# @app.post("/chat/ask", response_model=ChatResponse)
# def ask_gst(
#     payload: ChatRequest,
#     user=Depends(auth_guard)
# ):
#     answer, sources = chat(
#         query=payload.question,
#         store=vector_store,
#         all_chunks=ALL_CHUNKS
#     )

#     return {
#         "answer": answer,
#         "sources": sources
#     }


from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional
import json
import uuid

from services.chat.engine import chat
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
    version="2.0.0"
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

class ChatResponse(BaseModel):
    answer: str
    session_id: str
    sources: List[SourceChunk]
    full_judgments: Optional[Dict[str, FullJudgment]] = None

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
    
    # 2. Get User ID (using email from token)
    email = user.get("sub")
    if not email:
        raise HTTPException(status_code=401, detail="Invalid user")
    
    result = await db.execute(select(User).where(User.email == email))
    db_user = result.scalars().first()
    if not db_user:
         # Fallback or error? For now error. In real app might auto-register if token is valid from external auth.
         raise HTTPException(status_code=404, detail="User not found")
    
    user_id = db_user.id

    # 3. Fetch Context (Profile & History)
    profile = await get_user_profile(user_id)
    profile_summary = profile.dynamic_summary if profile else None
    
    history = await get_session_history(session_id)
    
    # 4. Generate Answer
    answer, sources, full_judgments = await chat(
        query=payload.question,
        store=vector_store,
        all_chunks=ALL_CHUNKS,
        history=history,
        profile_summary=profile_summary
    )
    
    # 5. Save to Memory (Redis + DB)
    # Save User Query
    await add_message(session_id, "user", payload.question, user_id)
    
    # Save AI Response (wait for it to be saved to return ID? No, we just save it)
    ai_msg = await add_message(session_id, "assistant", answer, user_id)

    # 6. Automatic Long-term Memory Update (Background Task)
    background_tasks.add_task(auto_update_profile, user_id, payload.question, answer)

    
    # Note: We might want to return the message ID of the AI response for feedback purposes.
    # The current ChatResponse structure doesn't include message_id, but the user might need it.
    # I'll rely on the client fetching history or I can add it to response if needed. 
    # For now, keeping it simple as per schema.

    return {
        "answer": answer,
        "session_id": session_id,
        "sources": sources,
        "full_judgments": full_judgments if full_judgments else None
    } 

@app.post("/chat/feedback")
async def save_feedback(
    payload: FeedbackRequest,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    # Verify message ownership?
    # Simple insert
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

@app.delete("/chat/session")
async def delete_chat(
    session_id: str,
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db)
):
    # Verify ownership
    email = user.get("sub")
    res = await db.execute(
        select(User).where(User.email == email)
    )
    db_user = res.scalars().first()
    
    res = await db.execute(
        select(ChatSession).where(ChatSession.id == session_id, ChatSession.user_id == db_user.id)
    )
    session = res.scalars().first()
    
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or unauthorized")
    
    from services.memory import delete_session
    await delete_session(session_id)
    
    return {"status": "deleted"}

