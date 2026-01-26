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


from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional
import json

from services.chat.engine import chat
from services.vector.store import VectorStore
from services.auth.deps import auth_guard
from api.auth import router as auth_router

# ---------------- INIT ---------------- #

app = FastAPI(
    title="GST Expert API",
    version="1.1.0"
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
    sources: List[SourceChunk]
    full_judgments: Optional[Dict[str, FullJudgment]] = None

# ---------------- ROUTES ---------------- #

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/auth/me")
def me(user=Depends(auth_guard)):
    return {"user": user}


@app.post("/chat/ask", response_model=ChatResponse)
def ask_gst(
    payload: ChatRequest,
    user=Depends(auth_guard)
):
    answer, sources, full_judgments = chat(
        query=payload.question,
        store=vector_store,
        all_chunks=ALL_CHUNKS
    )

    return {
        "answer": answer,
        "sources": sources,
        "full_judgments": full_judgments if full_judgments else None
    }