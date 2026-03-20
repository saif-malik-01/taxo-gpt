import asyncio
import logging
import json
from typing import AsyncGenerator, List, Dict, Optional, Tuple
from starlette.concurrency import run_in_threadpool

from apps.api.src.services.rag.retrieval.pipeline import RetrievalPipeline
from apps.api.src.services.rag.models import SessionMessage

logger = logging.getLogger(__name__)


# Singleton pipeline instance
_pipeline = None

def get_pipeline():
    global _pipeline
    if _pipeline is None:
        _pipeline = RetrievalPipeline()
        _pipeline.setup()
    return _pipeline

def _map_history(history: List[Dict]) -> List[SessionMessage]:
    """Map DB history to Pipeline SessionMessage model."""
    pipeline_history = []
    # History is [user_msg, bot_msg, user_msg, bot_msg...]
    # We need pairs of (user, bot)
    for i in range(0, len(history) - 1, 2):
        try:
            user_q = history[i].get("content", "")
            bot_r = history[i+1].get("content", "")
            pipeline_history.append(SessionMessage(user_query=user_q, llm_response=bot_r))
        except (IndexError, AttributeError):
            continue
    return pipeline_history

async def chat(
    query: str, 
    store: any = None, 
    all_chunks: list = None, 
    history: list = [], 
    profile_summary: Optional[str] = None, 
    document_context: Optional[str] = None
) -> Tuple[str, list, dict, dict, dict]:
    """
    Call the new RetrievalPipeline in non-streaming mode.
    """
    pipeline = await run_in_threadpool(get_pipeline)
    
    # Enrich query with profile/doc context if present
    context_prefix = ""
    if profile_summary:
        context_prefix += f"[User Profile: {profile_summary}]\n"
    if document_context:
        context_prefix += f"[Project/Document Context: {document_context}]\n"
    
    final_query = f"{context_prefix}{query}" if context_prefix else query
    session_history = _map_history(history)

    result = await run_in_threadpool(pipeline.query, final_query, session_history)
    
    usage = {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0} 
    return result.answer, result.retrieved_documents, {}, {}, usage

async def chat_stream(
    query: str, 
    store: any = None, 
    all_chunks: list = None, 
    history: list = [], 
    profile_summary: Optional[str] = None, 
    document_context: Optional[str] = None
) -> AsyncGenerator[dict, None]:
    """
    Call the new RetrievalPipeline in streaming mode and map events back to our JSON protocol.
    """
    pipeline = await run_in_threadpool(get_pipeline)
    
    context_prefix = ""
    if profile_summary:
        context_prefix += f"[User Profile: {profile_summary}]\n"
    if document_context:
        context_prefix += f"[Project/Document Context: {document_context}]\n"
    
    final_query = f"{context_prefix}{query}" if context_prefix else query
    session_history = _map_history(history)

    # 1. Run Stages 1-5 (Initial retrieval/filtering)
    staged = await run_in_threadpool(pipeline.query_stages_1_to_5, final_query, session_history)

    # Unpack: (final_query, session_history, chunks, citation_result, intent)
    _, _, top_chunks, citation_result, intent_obj = staged

    # 2. Run Stage 6 (Streaming response)
    stream_gen = pipeline.query_stage_6_stream(*staged)

    for chunk in stream_gen:
        if chunk.startswith("\n\n__META__"):
            try:
                meta_json = chunk.replace("\n\n__META__", "")
                meta = json.loads(meta_json)
                
                yield {
                    "type": "retrieval",
                    "sources": meta.get("retrieved_documents", []),
                    "full_judgments": {} 
                }
                yield {
                    "type": "citations",
                    "party_citations": {meta.get("intent", "GENERAL"): []}
                }
                yield {
                    "type": "usage",
                    "usage": {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}
                }
            except Exception as e:
                logger.error(f"Error parsing pipeline metadata: {e}")
        else:
            yield {"type": "content", "delta": chunk}
