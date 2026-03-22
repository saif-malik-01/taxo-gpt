"""
retrieval/pipeline.py
Main retrieval pipeline orchestrator — complete new approach.

Flow:
  Stage 1 — Query clarification
  Stage 2 — Parallel: 2A regex, 2B LLM extraction, 2C intent
  Stage 3 — Citation lookup (separate, pinned)
  Stage 4 — All parallel Qdrant calls → RRF → scoring → top 25
  Stage 5 — Decision filter + threshold check
  Stage 5b— Cross-ref enrichment (moved here from Stage 6 so it runs inside
             the threadpool call in main.py/_stream_core, not on the event loop)
  Stage 6 — LLM response (streaming or non-streaming)

query_stages_1_to_5 now returns a 6-tuple:
    (final_query, session_history, chunks, citation_result, intent, cross_refs)

query_stage_6_stream and _respond_non_stream accept the pre-computed cross_refs
so they never call enrich() themselves — the streaming path starts producing
tokens immediately without any blocking Qdrant calls.
"""

from typing import List, Optional

from qdrant_client import QdrantClient

from config import CONFIG
from pipeline.bm25_vectorizer import BM25Vectorizer
from retrieval.bedrock_llm import BedrockLLMClient
from retrieval.citation_lookup import CitationLookup
from retrieval.extractor import (
    CombinedExtractor, Stage1Clarifier, build_bm25_keyword_document,
)
from retrieval.models import FinalResponse, SessionMessage
from retrieval.qdrant_retrieval import QdrantRetrieval, ensure_text_indexes
from retrieval.responder import CrossRefEnricher, LLMResponder
from retrieval.scorer import MetadataScorer
from utils.logger import get_logger

logger = get_logger("retrieval_pipeline")


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

        self._bm25     = BM25Vectorizer()
        self._llm      = BedrockLLMClient()

        self._clarifier = Stage1Clarifier(self._llm)
        self._extractor = CombinedExtractor(self._llm)
        self._citation  = CitationLookup(self._qdrant)
        self._retrieval = QdrantRetrieval(self._qdrant, self._bm25)
        self._scorer    = MetadataScorer()
        self._enricher  = CrossRefEnricher(self._qdrant)
        self._responder = LLMResponder(self._llm)

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

    # ── Public: non-streaming ─────────────────────────────────────────────────

    def query(
        self,
        user_query: str,
        session_history: Optional[List[SessionMessage]] = None,
    ) -> FinalResponse:
        """Non-streaming query — returns complete FinalResponse."""
        session_history = session_history or []
        logger.info("=" * 60)
        logger.info(f"QUERY: {user_query[:120]}")
        logger.info(f"History: {len(session_history)} turns")
        logger.info("=" * 60)
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

    # ── Public: stages 1-5 (+ enrich) ────────────────────────────────────────

    def query_stages_1_to_5(
        self,
        user_query: str,
        session_history: Optional[List[SessionMessage]] = None,
    ):
        """
        Runs Stages 1-5 plus cross-ref enrichment.
        Designed to be called inside run_in_threadpool() by callers in
        main.py and document.py so ALL blocking work (Qdrant + Bedrock)
        is off the event loop.

        Returns 6-tuple:
            (final_query, session_history, chunks, citation_result, intent, cross_refs)

        cross_refs is pre-computed here so query_stage_6_stream can start
        yielding tokens immediately without any blocking Qdrant calls.
        """
        session_history = session_history or []
        logger.info("=" * 60)
        logger.info(f"QUERY: {user_query[:120]}")
        logger.info(f"History: {len(session_history)} turns")
        logger.info("=" * 60)
        return self._run_stages_1_to_5(user_query, session_history)

    # ── Public: stage 6 streaming ─────────────────────────────────────────────

    def query_stage_6_stream(
        self,
        final_query: str,
        session_history: List[SessionMessage],
        chunks,
        citation_result,
        intent,
        cross_refs=None,            # pre-computed by query_stages_1_to_5
    ):
        """
        Runs Stage 6 in streaming mode.
        cross_refs should always be the pre-computed value from the staged tuple.
        Falls back to calling enrich() only if somehow not provided (safety net).
        Yields text chunks, then a final __META__ JSON chunk.
        """
        if cross_refs is None:
            logger.warning(
                "cross_refs not provided to query_stage_6_stream — "
                "calling enrich() inline (adds latency before first token)"
            )
            cross_refs = self._enricher.enrich(chunks) if chunks else []

        logger.info(f"Cross-refs: {len(cross_refs)}")
        yield from self._responder.generate_stream(
            final_query=final_query,
            session_history=session_history,
            top_chunks=chunks,
            cross_ref_chunks=cross_refs,
            citation_result=citation_result if citation_result and citation_result.found else None,
            intent=intent,
        )

    # ── Internal: non-streaming response ─────────────────────────────────────

    def _respond_non_stream(
        self,
        final_query: str,
        session_history: List[SessionMessage],
        chunks,
        citation_result,
        intent,
        cross_refs=None,
    ) -> FinalResponse:
        if cross_refs is None:
            cross_refs = self._enricher.enrich(chunks) if chunks else []
        logger.info(f"Cross-refs: {len(cross_refs)}")
        return self._responder.generate(
            final_query=final_query,
            session_history=session_history,
            top_chunks=chunks,
            cross_ref_chunks=cross_refs,
            citation_result=citation_result if citation_result and citation_result.found else None,
            intent=intent,
        )

    # ── Internal: stages 1-5 + enrich ────────────────────────────────────────

    def _run_stages_1_to_5(
        self,
        user_query: str,
        session_history: List[SessionMessage],
    ):
        """
        Returns 6-tuple:
            (final_query, session_history, chunks, citation_result, intent, cross_refs)
        """

        # ── Stage 1 ───────────────────────────────────────────────────
        # Skip the LLM call entirely when there is no history.
        # Clarification is only needed to resolve pronouns/references from
        # prior turns ("that section", "same case", "the above ruling").
        # A first message is always self-contained — the round-trip saves 1-2s.
        logger.info("--- Stage 1: Clarification ---")
        if not session_history:
            final_query = user_query
            logger.info("Stage 1: skipped (no history)")
        else:
            final_query = self._clarifier.clarify(user_query, session_history)
            if final_query != user_query:
                logger.info(f"Rewritten: {final_query[:100]}")

        # ── Stage 2 ───────────────────────────────────────────────────
        logger.info("--- Stage 2: Extraction (2A+2B+2C parallel) ---")
        stage2a, stage2b, intent = self._extractor.extract(final_query)
        logger.info(
            f"2A: {len(stage2a.normalised_tokens)} norm + "
            f"{len(stage2a.raw_tokens)} raw tokens, "
            f"citation={stage2a.citation} | "
            f"2B: citation={stage2b.citation} parties={stage2b.parties} | "
            f"2C: {intent.intent} conf={intent.confidence}"
        )

        keyword_doc = build_bm25_keyword_document(stage2a, stage2b)

        # ── Stage 3 + 4 in parallel ───────────────────────────────────
        logger.info("--- Stage 3 (Citation) + Stage 4 (Retrieval) in parallel ---")
        from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
        citation_result = None
        chunks          = []

        with ThreadPoolExecutor(max_workers=2) as ex:
            fut_citation  = ex.submit(
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

        if citation_result.found:
            logger.info(
                f"Citation: {citation_result.citation} "
                f"({len(citation_result.chunks)} chunks)"
            )
        else:
            logger.info("Citation: not found")

        logger.info(f"Stage 4: {len(chunks)} chunks retrieved")

        # ── Stage 5: Filter ───────────────────────────────────────────
        logger.info("--- Stage 5: Filter ---")
        chunks = self._scorer.filter(chunks, stage2b, intent)

        # ── Stage 5b: Cross-ref enrichment ────────────────────────────
        # Done here (inside threadpool) so query_stage_6_stream can start
        # yielding tokens immediately without any Qdrant calls.
        logger.info("--- Stage 5b: Cross-ref enrichment ---")
        cross_refs = self._enricher.enrich(chunks) if chunks else []
        logger.info(f"Cross-refs: {len(cross_refs)}")

        logger.info("--- Stages 1-5 complete ---")
        return (final_query, session_history, chunks, citation_result, intent, cross_refs)