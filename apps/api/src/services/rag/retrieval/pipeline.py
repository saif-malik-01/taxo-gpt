"""
apps/api/src/services/rag/retrieval/pipeline.py
Main retrieval pipeline orchestrator  -  complete new approach.

Flow:
  Stage 1  -  Query clarification
  Stage 2  -  Parallel: 2A regex, 2B LLM extraction, 2C intent
  Stage 3  -  Citation lookup (separate, pinned)
  Stage 4  -  All parallel Qdrant calls  ->  RRF  ->  scoring  ->  top 25
  Stage 5  -  Decision filter + threshold check
  Stage 6  -  Cross-ref enrichment + LLM response
"""

import os
import logging
from typing import List, Optional

from qdrant_client import AsyncQdrantClient

from apps.api.src.core.config import settings
from apps.api.src.services.rag.pipeline.bm25_vectorizer import BM25Vectorizer
from apps.api.src.services.llm.bedrock import AsyncBedrockLLMClient, get_async_bedrock_client
from apps.api.src.services.rag.retrieval.citation_lookup import CitationLookup
from apps.api.src.services.rag.retrieval.extractor import (
    CombinedExtractor, Stage1Clarifier, build_bm25_keyword_document,
)
from apps.api.src.services.rag.models import SessionMessage
from apps.api.src.services.rag.retrieval.qdrant_retrieval import QdrantRetrieval, ensure_text_indexes
from apps.api.src.services.rag.retrieval.responder import CrossRefEnricher, LLMResponder
from apps.api.src.services.rag.retrieval.scorer import MetadataScorer

logger = logging.getLogger(__name__)


class RetrievalPipeline:
    def __init__(self):
        self._qdrant = AsyncQdrantClient(
            host    = settings.QDRANT_HOST,
            port    = settings.QDRANT_PORT,
            api_key = settings.QDRANT_API_KEY,
            https   = settings.QDRANT_HTTPS,
            timeout = settings.QDRANT_TIMEOUT,
            check_compatibility = False
        )
        logger.info(
            f"Qdrant: {settings.QDRANT_HOST}:{settings.QDRANT_PORT} "
            f"/ {settings.QDRANT_COLLECTION}"
        )

        self._bm25      = BM25Vectorizer()
        self._llm: AsyncBedrockLLMClient = None  # set in setup()

        self._clarifier  = None  # set in setup() after async client is ready
        self._extractor  = None
        self._citation   = CitationLookup(self._qdrant)
        self._retrieval  = QdrantRetrieval(self._qdrant, self._bm25)
        self._scorer     = MetadataScorer()
        self._enricher   = CrossRefEnricher(self._qdrant)
        self._responder  = None  # set in setup()

    async def setup(self):
        logger.info("Pipeline setup starting...")

        # Initialise the async Bedrock client (singleton — shared across requests)
        self._llm = await get_async_bedrock_client()
        self._clarifier = Stage1Clarifier(self._llm)
        self._extractor = CombinedExtractor(self._llm)
        self._responder = LLMResponder(self._llm, self._qdrant)

        try:
            collections_list = await self._qdrant.get_collections()
            all_cols = [c.name for c in collections_list.collections]
            logger.info(f"Qdrant collections: {all_cols}")
            if settings.QDRANT_COLLECTION not in all_cols:
                logger.error(
                    f"Collection '{settings.QDRANT_COLLECTION}' NOT FOUND. "
                    f"Available: {all_cols}"
                )
            else:
                info  = await self._qdrant.get_collection(settings.QDRANT_COLLECTION)
                count = getattr(info, "points_count", None) or getattr(info, "vectors_count", None)
                logger.info(f"Collection ready  -  points: {count}")
        except Exception as e:
            logger.error(f"Qdrant check failed: {e}")

        self._bm25.load_corpus_stats(settings.CORPUS_STATS_FILE)
        logger.info(
            f"BM25 loaded  -  vocab={len(self._bm25._vocab)} "
            f"docs={self._bm25._corpus_docs}"
        )

        try:
            await ensure_text_indexes(self._qdrant)
        except Exception as e:
            logger.warning(f"Text index setup failed (non-fatal): {e}")

        logger.info("Pipeline ready.")

    async def query_stages_1_to_5(
        self,
        user_query: str,
        session_history: Optional[List[SessionMessage]] = None,
    ):
        session_history = session_history or []
        return await self._run_stages_1_to_5(user_query, session_history)

    async def _run_stages_1_to_5(
        self,
        user_query: str,
        session_history: List[SessionMessage],
    ):
        # -- Stage 1 ---------------------------------------------------
        final_query = await self._clarifier.clarify(user_query, session_history)

        # -- Stage 2 ---------------------------------------------------
        stage2a, stage2b, intent = await self._extractor.extract(final_query)
        keyword_doc = build_bm25_keyword_document(stage2a, stage2b)

        # -- Stage 3: Citation (Sequential) ----------------------------
        citation_result = await self._citation.run(
            stage2a.citation, stage2b.citation, stage2b,
            intent.intent, intent.confidence,
        )

        # -- Stage 4: Retrieval (Parallel searches within) -------------
        chunks = await self._retrieval.retrieve(
            final_query, keyword_doc, stage2b, intent,
            None,
            stage2b.citation or stage2a.citation,
        )

        # -- Stage 5  -  Filter ------------------------------------------
        chunks = self._scorer.filter(chunks, stage2b, intent)

        return (final_query, session_history, chunks, citation_result, intent)

    async def query_stage_6_stream(
        self,
        final_query: str,
        session_history: List[SessionMessage],
        chunks,
        citation_result,
        intent,
    ):
        cross_refs = await self._enricher.enrich(chunks) if chunks else []
        async for chunk in self._responder.generate_stream(
            final_query=final_query,
            session_history=session_history,
            top_chunks=chunks,
            cross_ref_chunks=cross_refs,
            citation_result=citation_result if citation_result and citation_result.found else None,
            intent=intent,
        ):
            yield chunk
    