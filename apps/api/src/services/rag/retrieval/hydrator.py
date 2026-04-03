"""
apps/api/src/services/rag/retrieval/hydrator.py
Logic for converting a list of Qdrant chunk IDs back into full source objects (with full judgments).
"""

import asyncio
import json
import logging
from typing import List, Dict, Any, Optional
from qdrant_client import QdrantClient

from apps.api.src.core.config import settings
from apps.api.src.db.session import get_redis
from apps.api.src.services.rag.retrieval.responder import _fetch_full_judgment, _doc_summary
from apps.api.src.services.rag.executor import rag_executor
from starlette.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)

async def hydrate_sources(source_ids: List[str], qdrant: QdrantClient) -> List[Dict[str, Any]]:
    """
    Fetches sources from Qdrant by ID and reconstructs full judgments if needed.
    """
    if not source_ids:
        return []

    redis = await get_redis()
    hydrated_sources = []
    ids_to_fetch = []

    # 1. Check Redis Cache first
    for sid in source_ids:
        # Robust ID handling: Qdrant point IDs are either int or UUID string.
        # If it's a numeric string, convert it back to int for Qdrant.
        resolved_id = sid
        is_valid = False
        
        if isinstance(sid, int):
            is_valid = True
        elif isinstance(sid, str):
            if sid.isdigit():
                resolved_id = int(sid)
                is_valid = True
            else:
                # Check if it's a valid UUID string
                import re
                uuid_regex = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
                if uuid_regex.match(sid):
                    is_valid = True
        
        if not is_valid:
            logger.warning(f"Skipping invalid Qdrant ID: {sid}")
            continue
        
        cache_key = f"hydrated_source:{resolved_id}"
        try:
            cached = await redis.get(cache_key)
            if cached:
                hydrated_sources.append(json.loads(cached))
                continue
        except Exception: pass
        ids_to_fetch.append(resolved_id)

    if not ids_to_fetch:
        return hydrated_sources

    logger.info(f"Hydrating {len(ids_to_fetch)} sources from Qdrant: {ids_to_fetch}")

    # 2. Fetch from Qdrant
    try:
        # Qdrant client here is synchronous (blocking). Offload to a worker
        # so concurrent requests don't stall the event loop.
        results = await run_in_threadpool(
            qdrant.retrieve,
            collection_name=settings.QDRANT_COLLECTION,
            ids=ids_to_fetch,
            with_payload=True,
            with_vectors=False,
        )
        
        seen_identifiers = set()

        # We may need to reconstruct full judgments (extra blocking Qdrant scrolls).
        # Run those on the same bounded pool used across RAG.
        judgment_futures: List[tuple[int, Any]] = []

        for r in results:
            if not r.payload: continue
            payload = r.payload
            
            # Use existing summary builder
            # score is 0.0 for history as it's not a search result anymore
            summary = _doc_summary(payload, "history", 0.0)
            
            # Deduplicate ONLY judgments — other chunks preserve their individual texts
            identifier = summary.get("identifier")
            is_judgment = payload.get("chunk_type") == "judgment"
            
            if identifier and is_judgment:
                if identifier in seen_identifiers:
                    continue
                seen_identifiers.add(identifier)
            
            # Ensure the Point ID is set correctly in the summary for the frontend
            summary["chunk_id"] = str(r.id)
            
            # Handle full judgments (just like active stream).
            # We defer the actual blocking Qdrant scroll into a bounded worker.
            if payload.get("chunk_type") == "judgment":
                idx = len(hydrated_sources)
                full_future = rag_executor.submit(_fetch_full_judgment, qdrant, payload)
                judgment_futures.append((idx, full_future))
            
            # Cache after enrichment: judgments need full_judgment first (see gather below).
            if payload.get("chunk_type") != "judgment":
                try:
                    await redis.setex(
                        f"hydrated_source:{r.id}", 86400, json.dumps(summary)
                    )
                except Exception:
                    pass

            hydrated_sources.append(summary)
            
        # Resolve all pending full-judgment futures.
        # This prevents one judgment scroll from blocking other requests.
        if judgment_futures:
            idxs = [i for i, _ in judgment_futures]
            futures = [f for _, f in judgment_futures]
            full_judgments = await asyncio.gather(
                *(asyncio.wrap_future(f) for f in futures),
                return_exceptions=True,
            )
            for idx, full_data in zip(idxs, full_judgments):
                if isinstance(full_data, Exception):
                    full_data = None
                if full_data:
                    hydrated_sources[idx]["full_judgment"] = full_data
                try:
                    pt_id = hydrated_sources[idx]["chunk_id"]
                    await redis.setex(
                        f"hydrated_source:{pt_id}",
                        86400,
                        json.dumps(hydrated_sources[idx]),
                    )
                except Exception:
                    pass

    except Exception as e:
        logger.error(f"Hydration error for IDs {ids_to_fetch}: {e}")

    return hydrated_sources
