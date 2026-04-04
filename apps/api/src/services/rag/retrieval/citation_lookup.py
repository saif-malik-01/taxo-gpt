"""
apps/api/src/services/rag/retrieval/citation_lookup.py
Stage 3 — Citation lookup only.
Citation is always separate from the RRF pool — pinned at rank 0.
"""

import logging
from typing import Optional

from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

from apps.api.src.core.config import settings
from apps.api.src.services.rag.models import CitationResult, Stage2BResult

logger = logging.getLogger(__name__)


class CitationLookup:

    def __init__(self, qdrant: QdrantClient):
        self._qdrant = qdrant
        self._col    = settings.QDRANT_COLLECTION

    def run(
        self,
        stage2a_citation: Optional[str],
        stage2b_citation: Optional[str],
        stage2b: Stage2BResult,
        intent: str,
        confidence: int,
    ) -> CitationResult:
        """
        Use citation from 2A or 2B (2A takes priority as it's regex-based).
        Returns CitationResult with found chunks.
        """
        citation = stage2a_citation or stage2b_citation
        if not citation:
            return CitationResult(found=False)

        logger.info(f"Citation lookup: {citation}")

        try:
            scroll_result, _ = self._qdrant.scroll(
                collection_name=self._col,
                scroll_filter=qmodels.Filter(must=[
                    qmodels.FieldCondition(
                        key="ext.citation",
                        match=qmodels.MatchValue(value=citation),
                    ),
                    qmodels.FieldCondition(
                        key="chunk_type",
                        match=qmodels.MatchValue(value="judgment"),
                    ),
                ]),
                limit=1,
                with_payload=True,
                with_vectors=False,
            )

            if not scroll_result:
                logger.debug(f"Citation not found: {citation}")
                return CitationResult(found=False)

            sample    = scroll_result[0].payload or {}
            ext       = sample.get("ext") or {}
            case_note = str(ext.get("case_note") or "").strip()

            if len(case_note.split()) >= 20:
                logger.info(f"Citation: using case_note ({len(case_note.split())} words)")
                payload = scroll_result[0].payload or {}
                # Inject point ID for hydration standard
                payload["_point_id"] = str(scroll_result[0].id)
                chunks = [payload]
            else:
                all_results, _ = self._qdrant.scroll(
                    collection_name=self._col,
                    scroll_filter=qmodels.Filter(must=[
                        qmodels.FieldCondition(
                            key="ext.citation",
                            match=qmodels.MatchValue(value=citation),
                        ),
                        qmodels.FieldCondition(
                            key="chunk_type",
                            match=qmodels.MatchValue(value="judgment"),
                        ),
                    ]),
                    limit=8,
                    with_payload=True,
                    with_vectors=False,
                )
                chunks = []
                for r in all_results:
                    if r.payload:
                        p = r.payload
                        p["_point_id"] = str(r.id)
                        chunks.append(p)
                chunks.sort(key=lambda c: c.get("chunk_index", 0))
                logger.info(f"Citation: fetched {len(chunks)} chunks for {citation}")

            only_this = (
                intent == "JUDGMENT"
                and confidence >= 95
                and not _has_other_question(stage2b)
            )

            return CitationResult(
                found=True,
                chunks=chunks,
                citation=citation,
                only_this_asked=only_this,
            )

        except Exception as e:
            logger.error(f"Citation lookup failed: {e}")
            return CitationResult(found=False)


def _has_other_question(stage2b: Stage2BResult) -> bool:
    return bool(
        stage2b.sections or stage2b.rules or stage2b.notifications
        or stage2b.keywords or stage2b.topics
    )
