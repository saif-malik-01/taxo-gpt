"""
apps/api/src/services/rag/retrieval/hydrator.py
Logic for converting a list of Qdrant chunk IDs back into full source objects (with full judgments).
"""

import asyncio
import json
import logging
from typing import List, Dict, Any, Optional
from qdrant_client import AsyncQdrantClient

from apps.api.src.core.config import settings
from apps.api.src.db.session import get_redis
from apps.api.src.services.rag.retrieval.responder import _fetch_full_judgment, _doc_summary

logger = logging.getLogger(__name__)

async def hydrate_sources(source_ids: List[str], qdrant: AsyncQdrantClient) -> List[Dict[str, Any]]:
    """
    Fetches sources from Qdrant by ID.
    """
    if not source_ids:
        return []

    redis = await get_redis()
    hydrated_sources = []
    ids_to_fetch = []

    # 1. Check Redis Cache first
    for sid in source_ids:
        resolved_id = sid
        is_valid = False
        
        if isinstance(sid, int):
            is_valid = True
        elif isinstance(sid, str):
            if sid.isdigit():
                resolved_id = int(sid)
                is_valid = True
            else:
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
        results = await qdrant.retrieve(
            collection_name=settings.QDRANT_COLLECTION,
            ids=ids_to_fetch,
            with_payload=True,
            with_vectors=False,
        )
        
        seen_identifiers = set()

        for r in results:
            if not r.payload: continue
            payload = r.payload
            
            summary = _doc_summary(payload, "history", 0.0)
            identifier = summary.get("identifier")
            is_judgment = payload.get("chunk_type") == "judgment"
            
            if identifier and is_judgment:
                if identifier in seen_identifiers:
                    continue
                seen_identifiers.add(identifier)
            
            summary["chunk_id"] = str(r.id)
            
            try:
                await redis.setex(
                    f"hydrated_source:{r.id}", 86400, json.dumps(summary)
                )
            except Exception:
                pass

            hydrated_sources.append(summary)

    except Exception as e:
        logger.error(f"Hydration error for IDs {ids_to_fetch}: {e}")

    return hydrated_sources
