"""
apps/api/src/services/rag/retrieval/pipeline.py
Main retrieval pipeline orchestrator — complete new approach.
"""

import os
import logging
from typing import List, Optional

from qdrant_client import QdrantClient

from apps.api.src.services.rag.config import CONFIG
from apps.api.src.services.rag.pipeline.bm25_vectorizer import BM25Vectorizer
from apps.api.src.services.rag.retrieval.bedrock_llm import BedrockLLMClient
from apps.api.src.services.rag.retrieval.citation_lookup import CitationLookup
from apps.api.src.services.rag.retrieval.extractor import (
    CombinedExtractor, Stage1Clarifier, build_bm25_keyword_document,
)
from apps.api.src.services.rag.models import FinalResponse, SessionMessage
from apps.api.src.services.rag.retrieval.qdrant_retrieval import QdrantRetrieval, ensure_text_indexes
from apps.api.src.services.rag.retrieval.responder import CrossRefEnricher, LLMResponder
from apps.api.src.services.rag.retrieval.scorer import MetadataScorer

logger = logging.getLogger(__name__)


class RetrievalPipeline:

    def __init__(self):
        self._qdrant = QdrantClient(
            host    = CONFIG.qdrant.host,
            port    = CONFIG.qdrant.port,
            api_key = CONFIG.qdrant.api_key,
            timeout = CONFIG.qdrant.timeout,
        )
        logger.info(
            f"Qdrant: {CONFIG.qdrant.host}:{CONFIG.qdrant.port} "
            f"/ {CONFIG.qdrant.collection_name}"
        )

        self._bm25      = BM25Vectorizer()
        self._llm       = BedrockLLMClient()

        self._clarifier  = Stage1Clarifier(self._llm)
        self._extractor  = CombinedExtractor(self._llm)
        self._citation   = CitationLookup(self._qdrant)
        self._retrieval  = QdrantRetrieval(self._qdrant, self._bm25)
        self._scorer     = MetadataScorer()
        self._enricher   = CrossRefEnricher(self._qdrant)
        self._responder  = LLMResponder(self._llm)

    def setup(self):
        logger.info("Pipeline setup starting...")

        try:
            all_cols = [c.name for c in self._qdrant.get_collections().collections]
            logger.info(f"Qdrant collections: {all_cols}")
            if CONFIG.qdrant.collection_name not in all_cols:
                logger.error(
                    f"Collection '{CONFIG.qdrant.collection_name}' NOT FOUND. "
                    f"Available: {all_cols}"
                )
            else:
                info  = self._qdrant.get_collection(CONFIG.qdrant.collection_name)
                count = getattr(info, "points_count", None) or getattr(info, "vectors_count", None)
                logger.info(f"Collection ready — points: {count}")
        except Exception as e:
            logger.error(f"Qdrant check failed: {e}")

        self._bm25.load_corpus_stats(CONFIG.paths.corpus_stats_file)
        logger.info(
            f"BM25 loaded — vocab={len(self._bm25._vocab)} "
            f"docs={self._bm25._corpus_docs}"
        )

        try:
            ensure_text_indexes(self._qdrant)
        except Exception as e:
            logger.warning(f"Text index setup failed (non-fatal): {e}")

        logger.info("Pipeline ready.")

    def query(
        self,
        user_query: str,
        session_history: Optional[List[SessionMessage]] = None,
    ) -> FinalResponse:
        """Non-streaming query — returns complete FinalResponse."""
        session_history = session_history or []
        try:
            staged = self.query_stages_1_to_5(user_query, session_history)
            return self._respond_non_stream(*staged)
        except Exception as e:
            logger.exception(f"Pipeline failed: {e}")
            return FinalResponse(
                answer="An error occurred. Please try again.",
                retrieved_documents=[],
                intent="GENERAL",
                confidence=0,
            )

    def query_stages_1_to_5(
        self,
        user_query: str,
        session_history: Optional[List[SessionMessage]] = None,
    ):
        session_history = session_history or []
        return self._run_stages_1_to_5(user_query, session_history)

    def query_stage_6_stream(
        self,
        final_query: str,
        session_history: List[SessionMessage],
        chunks,
        citation_result,
        intent,
    ):
        cross_refs = self._enricher.enrich(chunks) if chunks else []
        yield from self._responder.generate_stream(
            final_query=final_query,
            session_history=session_history,
            top_chunks=chunks,
            cross_ref_chunks=cross_refs,
            citation_result=citation_result if citation_result and citation_result.found else None,
            intent=intent,
        )

    def _respond_non_stream(
        self,
        final_query: str,
        session_history: List[SessionMessage],
        chunks,
        citation_result,
        intent,
    ) -> FinalResponse:
        cross_refs = self._enricher.enrich(chunks) if chunks else []
        return self._responder.generate(
            final_query=final_query,
            session_history=session_history,
            top_chunks=chunks,
            cross_ref_chunks=cross_refs,
            citation_result=citation_result if citation_result and citation_result.found else None,
            intent=intent,
        )

    def _run_stages_1_to_5(
        self,
        user_query: str,
        session_history: List[SessionMessage],
    ):
        # ── Stage 1 ───────────────────────────────────────────────────
        final_query = self._clarifier.clarify(user_query, session_history)

        # ── Stage 2 ───────────────────────────────────────────────────
        stage2a, stage2b, intent = self._extractor.extract(final_query)
        keyword_doc = build_bm25_keyword_document(stage2a, stage2b)

        # ── Stage 3 + Stage 4 ─────────────────────────────────────────
        from concurrent.futures import ThreadPoolExecutor
        citation_result = None
        chunks = []

        with ThreadPoolExecutor(max_workers=2) as ex:
            fut_citation = ex.submit(
                self._citation.run,
                stage2a.citation, stage2b.citation, stage2b,
                intent.intent, intent.confidence,
            )
            fut_retrieval = ex.submit(
                self._retrieval.retrieve,
                final_query, keyword_doc, stage2b, intent,
                None,
                stage2b.citation or stage2a.citation,
            )
            citation_result = fut_citation.result()
            chunks          = fut_retrieval.result()

        # ── Stage 5 — Filter ──────────────────────────────────────────
        chunks = self._scorer.filter(chunks, stage2b, intent)

        return (final_query, session_history, chunks, citation_result, intent)
