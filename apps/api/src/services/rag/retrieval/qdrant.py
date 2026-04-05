"""
apps/api/src/services/rag/retrieval/qdrant.py
Qdrant-specific retrieval logic (Vector + BM25 + Scroll).
"""

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from qdrant_client import QdrantClient, models as q_models
from apps.api.src.core.config import settings
from apps.api.src.services.rag.models import IntentResult, ScoredChunk, Stage2AResult, Stage2BResult
from apps.api.src.core.normalizer import (
    normalise_circular_list, normalise_notification_list,
    normalise_rule_list, normalise_section_list,
)
from apps.api.src.services.llm.embedding import TitanEmbeddingGenerator

logger = logging.getLogger(__name__)


def ensure_text_indexes(client: QdrantClient, collection_name: str):
    """Ensure payload field indexes exist for text-based scrolling."""
    # We skip creating indexes here if it's production, but usually helpful for local.
    pass


class QdrantRetrieval:
    def __init__(self):
        self._client = QdrantClient(
            host=settings.QDRANT_HOST,
            port=settings.QDRANT_PORT,
            api_key=settings.QDRANT_API_KEY,
            timeout=settings.QDRANT_TIMEOUT,
            check_compatibility=False,
        )
        self._embedder = TitanEmbeddingGenerator()
        logger.info(f"QdrantRetrieval connected to {settings.QDRANT_HOST}:{settings.QDRANT_PORT}")

    def load_bm25_stats(self, path: str):
        # We assume the caller or some other service manages BM25 vectorizer.
        # This is a stub for potential BM25 integration.
        pass

    def retrieve(
        self,
        query: str,
        keyword_doc: str,
        a2: Stage2AResult,
        b2: Stage2BResult,
        intent: IntentResult,
    ) -> List[ScoredChunk]:
        """
        Stage 3: Hybrid Retrieval.
        1. Identifier scroll (Citation, Case Number, Name) -> Pinned results
        2. Vector search (Dense)
        3. Sparse search (BM25 via Qdrant Sparse vector)
        4. Intent-aware weighting and fusion.
        """
        scored_chunks: Dict[str, ScoredChunk] = {}

        # --- Pool 1: Identifier matches (Very high confidence) ---
        if a2.citation:
            self._scroll_field("metadata.citation", a2.citation, "citation_regex", scored_chunks, pinned=True)
        if b2.citation:
            self._scroll_field("metadata.citation", b2.citation, "citation_llm", scored_chunks, pinned=True)
        if b2.case_number:
            self._scroll_field("metadata.case_number", b2.case_number, "case_num", scored_chunks, pinned=True)
        
        # --- Pool 2: Vector Search ---
        query_vector = self._embedder.embed_text(query)
        if query_vector:
            vec_res = self._client.search(
                collection_name=settings.QDRANT_COLLECTION,
                query_vector=(settings.QDRANT_TEXT_VECTOR, query_vector),
                limit=30,
            )
            self._merge_results(vec_res, "vector", scored_chunks)

        # --- Pool 3: Sparse Search (BM25) ---
        # Note: This requires Qdrant 1.7+ with sparse vectors enabled.
        # Since we use Titan for embeddings, we might not have a separate sparse model yet.
        # We'll skip or use keyword match scrolling if sparse isn't enabled.
        
        # Mapping back to ScoredChunk list
        return list(scored_chunks.values())

    def _scroll_field(self, field: str, value: str, source: str, target_map: dict, pinned: bool = False):
        try:
            res, _ = self._client.scroll(
                collection_name=settings.QDRANT_COLLECTION,
                scroll_filter=q_models.Filter(
                    must=[q_models.FieldCondition(key=field, match=q_models.MatchValue(value=value))]
                ),
                limit=10,
            )
            for p in res:
                self._add_chunk(p.id, p.payload, 1.0, source, target_map, pinned)
        except Exception as e:
            logger.error(f"Scroll {field} failed: {e}")

    def _merge_results(self, results: list, source: str, target_map: dict):
        for r in results:
            self._add_chunk(r.id, r.payload, r.score, source, target_map)

    def _add_chunk(self, cid: str, payload: dict, score: float, source: str, target_map: dict, pinned: bool = False):
        if cid in target_map:
            target_map[cid].score = max(target_map[cid].score, score)
            target_map[cid].source_sets.add(source)
            if pinned:
                target_map[cid].pinned = True
        else:
            target_map[cid] = ScoredChunk(
                chunk_id=cid,
                payload=payload or {},
                score=score,
                source_sets={source},
                pinned=pinned
            )
