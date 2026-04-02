import asyncio
import logging
import json
import threading   
from typing import AsyncGenerator, List, Dict, Optional
from starlette.concurrency import run_in_threadpool

from apps.api.src.services.rag.retrieval.pipeline import RetrievalPipeline
from apps.api.src.services.rag.models import SessionMessage

logger = logging.getLogger(__name__)

_pipeline: Optional[RetrievalPipeline] = None
_pipeline_lock = threading.Lock()

def get_pipeline() -> RetrievalPipeline:
    global _pipeline
    if _pipeline is None:
        with _pipeline_lock:
            if _pipeline is None:
                p = RetrievalPipeline()
                p.setup()
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

def _stage6_to_queue(
    pipeline: RetrievalPipeline,
    staged: tuple,
    queue: "asyncio.Queue[object]",
    loop: asyncio.AbstractEventLoop,
) -> None:
    try:
        for chunk in pipeline.query_stage_6_stream(*staged):
            asyncio.run_coroutine_threadsafe(queue.put(chunk), loop).result()
    except Exception as e:
        logger.error(f"Stage 6 thread error: {e}", exc_info=True)
        asyncio.run_coroutine_threadsafe(
            queue.put({"__error__": str(e)}), loop
        ).result()
    finally:
        asyncio.run_coroutine_threadsafe(queue.put(None), loop).result()


async def chat_stream(
    query: str,
    history: list = [],
    profile_summary: Optional[str] = None,
) -> AsyncGenerator[dict, None]:

    pipeline = await run_in_threadpool(get_pipeline)

    context_prefix = ""
    if profile_summary:
        context_prefix += f"[User Profile: {profile_summary}]\n"
    final_query = f"{context_prefix}{query}" if context_prefix else query
    session_history = _map_history(history)

    staged = await run_in_threadpool(
        pipeline.query_stages_1_to_5, final_query, session_history
    )

    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    queue: asyncio.Queue = asyncio.Queue(maxsize=64)

    loop.run_in_executor(
        None,
        _stage6_to_queue,
        pipeline,
        staged,
        queue,
        loop,
    )

    while True:
        chunk = await queue.get()

        if chunk is None:
            break

        if isinstance(chunk, dict) and "__error__" in chunk:
            yield {"type": "error", "message": chunk["__error__"]}
            break

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