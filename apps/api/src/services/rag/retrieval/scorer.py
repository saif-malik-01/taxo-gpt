"""
apps/api/src/services/rag/retrieval/scorer.py
Stage 5 — Decision filter and minimum score threshold only.

All scoring (RRF, source bonus, intent weights, match depth boost)
is done inside qdrant_retrieval.py before top 25 selection.
This module handles the final filters only.
"""

import logging
from typing import List

from apps.api.src.services.rag.models import IntentResult, ScoredChunk, Stage2BResult

logger = logging.getLogger(__name__)

_MIN_SCORE = 0.005


class MetadataScorer:

    def filter(
        self,
        chunks: List[ScoredChunk],
        stage2b: Stage2BResult,
        intent: IntentResult,
    ) -> List[ScoredChunk]:
        """
        Final filters after top 25 selection from qdrant_retrieval.

        1. Decision filter (confidence >= 95 only)
        2. Minimum score threshold check
        """
        if not chunks:
            return []

        # Decision filter — only at confidence >= 95
        if intent.confidence >= 95 and stage2b.decision_type:
            before = len(chunks)
            chunks = [
                c for c in chunks
                if not (
                    c.payload.get("chunk_type") == "judgment"
                    and (c.payload.get("ext") or {}).get("decision")
                    and (c.payload.get("ext") or {}).get("decision") != stage2b.decision_type
                )
            ]
            removed = before - len(chunks)
            if removed:
                logger.info(
                    f"Decision filter: removed {removed} chunks "
                    f"(wanted={stage2b.decision_type})"
                )

        # Minimum score check
        if chunks and max(c.score for c in chunks) < _MIN_SCORE:
            logger.warning(
                f"All chunks below min score threshold ({_MIN_SCORE}) — "
                "context may be insufficient"
            )
            # Return them anyway — responder handles insufficient context messaging

        logger.info(
            f"Scorer: {len(chunks)} chunks pass to LLM — "
            f"types={[c.payload.get('chunk_type') for c in chunks[:5]]}"
        )
        return chunks
