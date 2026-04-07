"""
services/document/redis_queue.py

High-speed task dispatcher for decentralized document extraction.
Uses the existing Redis ElastiCache for task queueing (LPUSH pattern).
"""

import json
import logging
from apps.api.src.db.session import get_redis
from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)

# --- Redis Queue Configuration ---
# doc:queue:extraction -> List of JSON tasks
_QUEUE_KEY = "doc:queue:extraction"


async def dispatch_extraction_task(
    user_id:    int,
    session_id: str,
    documents:  list # List of {"filename": "..." , "s3_key": "...", "ext": "..."}
) -> bool:
    """
    Pushes a 'PROCESS_DOCUMENT' task to the Redis Queue.
    The distributed worker pool will BRPOP this task to perform Poppler-OCR.
    """
    redis = await get_redis()
    
    payload = {
        "action":     "PROCESS_DOCUMENT",
        "user_id":    user_id,
        "session_id": session_id,
        "documents":  documents,
        "config": {
            "dpi": settings.NOVA_LITE_DPI,
            "semaphore_max": settings.MAX_CONCURRENT_PAGES
        }
    }
    
    try:
        # LPUSH onto the queue
        await redis.lpush(_QUEUE_KEY, json.dumps(payload))
        logger.info(f"Dispatched document extraction task to REDIS for session {session_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to dispatch REDIS task for session {session_id}: {e}")
        return False
