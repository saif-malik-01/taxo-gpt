import asyncio
import logging
import json
from typing import AsyncGenerator, List, Dict, Optional

from apps.api.src.services.rag.retrieval.pipeline import RetrievalPipeline
from apps.api.src.services.rag.models import SessionMessage

logger = logging.getLogger(__name__)

_UNSET = object()
_pipeline = _UNSET
_pipeline_lock: Optional[asyncio.Lock] = None

async def get_pipeline() -> RetrievalPipeline:
    global _pipeline, _pipeline_lock
    if _pipeline is not _UNSET:
        return _pipeline

    if _pipeline_lock is None:
        _pipeline_lock = asyncio.Lock()

    async with _pipeline_lock:
        if _pipeline is _UNSET:
            p = RetrievalPipeline()
            await p.setup()
            _pipeline = p
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

# _stage6_to_queue and other thread-based helpers removed in favor of pure async


async def chat_stream(
    query: str,
    history: list = [],
    profile_summary: Optional[str] = None,
) -> AsyncGenerator[dict, None]:

    pipeline = await get_pipeline()

    final_query = query
    session_history = _map_history(history)

    staged = await pipeline.query_stages_1_to_5(final_query, session_history)

    async for chunk in pipeline.query_stage_6_stream(*staged, profile_summary=profile_summary):
        if isinstance(chunk, str) and chunk.startswith("\n\n__META__"):
            try:
                meta = json.loads(chunk.replace("\n\n__META__", ""))
                yield {
                    "type":       "retrieval",
                    "intent":     meta.get("intent"),
                    "confidence": meta.get("confidence"),
                    "sources":    meta.get("retrieved_documents", []),
                    "usage":      meta.get("usage", {}),
                }
            except Exception as e:
                logger.error(f"Meta parse error: {e}")
        else:
            yield {"type": "content", "delta": chunk}
