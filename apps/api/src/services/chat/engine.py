import asyncio
import logging
import json
from typing import AsyncGenerator, List, Dict, Optional, Tuple
from starlette.concurrency import run_in_threadpool

from apps.api.src.services.rag.retrieval.pipeline import RetrievalPipeline
from apps.api.src.services.rag.models import SessionMessage

logger = logging.getLogger(__name__)

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

async def chat_stream(
    query: str, 
    history: list = [], 
    profile_summary: Optional[str] = None, 
) -> AsyncGenerator[dict, None]:
    """
    Call the new RetrievalPipeline in streaming mode and map events back to our JSON protocol.
    """
    pipeline = await run_in_threadpool(get_pipeline)
    
    context_prefix = ""
    if profile_summary:
        context_prefix += f"[User Profile: {profile_summary}]\n"
    
    final_query = f"{context_prefix}{query}" if context_prefix else query
    session_history = _map_history(history)

    # 1. Run Stages 1-5 (Initial retrieval/filtering)
    staged = await run_in_threadpool(pipeline.query_stages_1_to_5, final_query, session_history)
    
    # 2. Run Stage 6 (Streaming response)
    stream_gen = pipeline.query_stage_6_stream(*staged)

    while True:
        try:
            # Use 'STOP' sentinel to safely detect end of synchronous generator
            chunk = await run_in_threadpool(next, stream_gen, "STOP")
            if chunk == "STOP":
                break

            if isinstance(chunk, str) and chunk.startswith("\n\n__META__"):
                try:
                    meta_json = chunk.replace("\n\n__META__", "")
                    meta = json.loads(meta_json)
                    yield {
                        "type": "retrieval",
                        "intent": meta.get("intent"),
                        "confidence": meta.get("confidence"),
                        "sources": meta.get("retrieved_documents", []),
                        "usage": meta.get("usage", {}),
                    }
                except Exception as e:
                    logger.error(f"Error parsing pipeline metadata: {e}")
            else:
                yield {"type": "content", "delta": chunk}
        except Exception as e:
            logger.error(f"Stream generation error in engine: {e}")
            yield {"type": "error", "message": "Stream interrupted"}
            break
