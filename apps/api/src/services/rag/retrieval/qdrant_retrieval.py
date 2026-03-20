"""
retrieval/qdrant_retrieval.py
Stage 4 - Retrieval, scoring, and final selection.

Three retrieval pools (all parallel):
    Pool A: vector  - dense similarity search, top 30 with cosine scores
    Pool B: bm25    - BM25 sparse search, top 30 with BM25 scores
    Pool C: payload - scroll1 + scroll2 + scroll3 (payload match, no score)

Pinned lookups (parallel, separate from scoring):
    citation, name/party search, case number search

Scoring model:
    Step 1  Pool C chunks already in Pool A -> add scroll bonus to cosine score
    Step 2  Pool C chunks NOT in Pool A -> fetch stored vectors -> cosine sim
            + scroll bonus -> insert into Pool A
    Step 3  Pool A is now unified: original vector results + keyword/scroll
            results, all scored on cosine-similarity basis (one ranked list)
    Step 4  RRF between Pool A (ranked) and Pool B/BM25 (ranked) -> base score
    Step 5  Intent weights additive
    Step 6  Sort -> top 30
    Step 7  Match depth boost (cross-ref +0.08, keyword +0.08, cap +0.24)
    Step 8  Pinned chunks inserted at top
    Step 9  Dedup by chunk id -> top 25 for LLM

Scroll bonuses (additive to cosine score):
    scroll1 (direct ext field match)  -> +0.18  highest confidence
    scroll3 (keyword array match)     -> +0.12
    scroll2 (cross-reference match)   -> +0.10
"""

import math
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

from apps.api.src.core.config import (
    settings,
    SECTION_CHUNK_TYPES,
    RULE_CHUNK_TYPES,
    FORM_CHUNK_TYPES,
)
from apps.api.src.services.llm.embedding import TitanEmbeddingGenerator
from apps.api.src.services.rag.pipeline.bm25_vectorizer import BM25Vectorizer
from apps.api.src.services.rag.models import IntentResult, ScoredChunk, Stage2BResult
import logging

logger = logging.getLogger(__name__)

_TOP_N   = 30   # each individual search call returns this many
_FINAL_N = 25   # chunks sent to LLM
_RRF_K   = 60   # RRF constant

# Scroll bonuses - additive to cosine similarity score in Pool A
# Applied when a scroll confirms a chunk (scroll1 = most precise)
_SCROLL1_BONUS = 0.18   # direct ext field match (exact section/rule/form match)
_SCROLL3_BONUS = 0.12   # keyword array match
_SCROLL2_BONUS = 0.10   # cross-reference match


class QdrantRetrieval:

    def __init__(self, qdrant: QdrantClient, bm25: BM25Vectorizer):
        self._qdrant   = qdrant
        self._bm25     = bm25
        self._col      = settings.QDRANT_COLLECTION
        self._embedder = TitanEmbeddingGenerator()

    def retrieve(
        self,
        query: str,
        keyword_document: str,
        stage2b: Stage2BResult,
        intent: IntentResult,
        pinned_citation_chunks: Optional[List[Dict]] = None,
        pinned_citation: Optional[str] = None,
    ) -> List[ScoredChunk]:
        """
        Runs all retrieval calls in parallel.
        Returns top 25 scored and ranked chunks.

        pinned_citation_chunks: already-fetched citation chunks (passed from pipeline)
        pinned_citation:        the citation string to exclude from retrieval pool
        """

        # -- Embed query -----------------------------------------------
        logger.info("Stage 4: embedding query...")
        query_vector = self._embedder.embed_text(query)
        if query_vector is None:
            logger.error("Stage 4: query embedding failed - BM25 + payload only")
        else:
            logger.info(f"Stage 4: query vector ready ({len(query_vector)} dims)")

        # -- BM25 sparse vector ----------------------------------------
        sparse_indices, sparse_values = self._bm25.compute_sparse_vector(keyword_document)
        logger.info(
            f"Stage 4: BM25 sparse vector - "
            f"{len(sparse_indices)} non-zero dims from "
            f"{len(keyword_document.split())} tokens"
        )

        # -- Determine which calls to run ------------------------------
        run_scroll1  = _has_identifiers(stage2b) or bool(stage2b.hsn_code or stage2b.sac_code)
        run_scroll2  = _has_identifiers(stage2b)
        run_scroll3  = bool(
            stage2b.keywords or stage2b.topics
            or stage2b.sections or stage2b.rules
            or stage2b.form_name or stage2b.form_number
        )
        run_name     = bool(stage2b.parties or stage2b.person_names)
        run_case_num = bool(stage2b.case_number)

        logger.info(
            f"Stage 4 calls: "
            f"vector={'YES' if query_vector else 'NO'} "
            f"bm25=YES "
            f"scroll1={'YES' if run_scroll1 else 'NO'} "
            f"scroll2={'YES' if run_scroll2 else 'NO'} "
            f"scroll3={'YES' if run_scroll3 else 'NO'} "
            f"name={'YES' if run_name else 'NO'} "
            f"case_num={'YES' if run_case_num else 'NO'}"
        )

        # -- Launch all parallel calls ---------------------------------
        futures_map: Dict[Any, str] = {}
        with ThreadPoolExecutor(max_workers=10) as ex:
            if query_vector:
                futures_map[ex.submit(self._vector_search, query_vector)] = "vector"
            futures_map[ex.submit(self._bm25_search, sparse_indices, sparse_values)] = "bm25"
            if run_scroll1:
                futures_map[ex.submit(self._scroll1, stage2b)] = "scroll1"
            if run_scroll2:
                futures_map[ex.submit(self._scroll2, stage2b)] = "scroll2"
            if run_scroll3:
                futures_map[ex.submit(self._scroll3, stage2b)] = "scroll3"
            if run_name:
                futures_map[ex.submit(
                    self._name_search, stage2b.parties + stage2b.person_names
                )] = "name_search"
            if run_case_num:
                futures_map[ex.submit(
                    self._case_number_search, stage2b.case_number
                )] = "case_search"

            raw_results: Dict[str, Any] = {}
            for fut in as_completed(futures_map):
                source = futures_map[fut]
                try:
                    raw_results[source] = fut.result()
                except Exception as e:
                    logger.error(f"  [{source}] failed: {e}")
                    raw_results[source] = []

        # -- Unpack results --------------------------------------------
        # Vector and BM25 return (chunk_id, payload, score) triples
        # Scrolls return (chunk_id, payload) pairs (no score - payload match)
        # Name/case searches return (chunk_id, payload) pairs - PINNED

        vector_scored: List[Tuple[str, Dict, float]] = raw_results.get("vector", [])
        bm25_scored:   List[Tuple[str, Dict, float]] = raw_results.get("bm25", [])

        # Combine scroll1 + scroll2 + scroll3 into payload pool
        # Dedup ACROSS all three - a chunk in scroll1 is not re-added by scroll2/3.
        # scroll_type_map tracks which scroll first claimed each chunk id.
        scroll1_ids: Set[str] = set()
        scroll2_ids: Set[str] = set()
        scroll3_ids: Set[str] = set()
        payload_pool: Dict[str, Dict] = {}

        # Process in priority order: scroll1 (highest precision) first
        for source, scroll_set in [
            ("scroll1", scroll1_ids),
            ("scroll2", scroll2_ids),
            ("scroll3", scroll3_ids),
        ]:
            raw = raw_results.get(source, [])
            added = 0
            for chunk_id, payload in raw:
                # Skip if already claimed by a higher-priority scroll
                if chunk_id not in payload_pool:
                    scroll_set.add(chunk_id)
                    payload_pool[chunk_id] = payload
                    added += 1
            logger.info(
                f"  [{source}]: {added} unique chunks added to payload pool "
                f"({len(raw)} raw results, "
                f"{len(raw) - added} duplicates dropped) "
                f"types={list({p.get('chunk_type','?') for _,p in raw[:5]})}"
            )

        payload_ids = scroll1_ids | scroll2_ids | scroll3_ids

        logger.info(
            f"  [vector]: {len(vector_scored)} scored results "
            f"types={list({p.get('chunk_type','?') for _,p,_ in vector_scored[:5]})}"
        )
        logger.info(
            f"  [bm25]: {len(bm25_scored)} scored results "
            f"types={list({p.get('chunk_type','?') for _,p,_ in bm25_scored[:5]})}"
        )
        logger.info(
            f"Payload pool: {len(payload_ids)} unique chunks "
            f"(s1={len(scroll1_ids)} s2={len(scroll2_ids)} s3={len(scroll3_ids)})"
        )

        # Pinned lookups (name + case search)
        pinned_pairs: List[Tuple[str, Dict, str]] = []   # (id, payload, reason)
        for source, reason_prefix in [("name_search", "party/name match"), ("case_search", "case number match")]:
            for chunk_id, payload in raw_results.get(source, []):
                pinned_pairs.append((chunk_id, payload, reason_prefix))
        if pinned_pairs:
            logger.info(
                f"Pinned: {len(pinned_pairs)} chunks "
                f"from name/case search - guaranteed top slots"
            )

        # -- Build id sets and payload maps ----------------------------
        vector_ids = {cid for cid, _, _ in vector_scored}
        bm25_ids   = {cid for cid, _, _ in bm25_scored}

        # Pool A: vector scores - base pool, everything gets merged into this
        pool_a_scores:   Dict[str, float] = {cid: s for cid, _, s in vector_scored}
        pool_a_payloads: Dict[str, Dict]  = {cid: p for cid, p, _ in vector_scored}
        # BM25 payloads for chunks not already in vector
        bm25_payloads:   Dict[str, Dict]  = {cid: p for cid, p, _ in bm25_scored}

        logger.info(
            f"  [vector]: {len(vector_scored)} chunks "
            f"types={list({p.get('chunk_type','?') for _,p,_ in vector_scored[:5]})}"
        )
        logger.info(
            f"  [bm25]: {len(bm25_scored)} chunks "
            f"types={list({p.get('chunk_type','?') for _,p,_ in bm25_scored[:5]})}"
        )
        logger.info(
            f"Payload pool: {len(payload_ids)} unique chunks "
            f"(s1={len(scroll1_ids)} s2={len(scroll2_ids)} s3={len(scroll3_ids)})"
        )

        # Pinned lookups (name + case search)
        pinned_pairs: List[Tuple[str, Dict, str]] = []
        for source, reason_prefix in [("name_search", "party/name match"), ("case_search", "case number match")]:
            for chunk_id, payload in raw_results.get(source, []):
                pinned_pairs.append((chunk_id, payload, reason_prefix))
        if pinned_pairs:
            logger.info(
                f"Pinned: {len(pinned_pairs)} chunks from name/case search"
            )

        # ═══════════════════════════════════════════════════════════════
        # SCORING MODEL
        #
        # Goal: merge Pool C (scrolls, no score) into Pool A (vector, scored)
        # so that Pool A becomes one unified cosine-similarity-based ranked list.
        # Then RRF between Pool A (enriched) and Pool B (BM25).
        #
        # Step 1  Pool C chunks already in Pool A → add scroll bonus to vector score
        # Step 2  Pool C chunks NOT in Pool A → fetch vectors → cosine sim + scroll bonus → insert into Pool A
        # Step 3  Pool A is now the unified vector+keyword pool (one ranked list)
        # Step 4  RRF between Pool A and Pool B (BM25) → final base score
        # Step 5  Intent weights
        # Step 6  Sort → top 30
        # Step 7  Match depth boost
        # Step 8  Pinned chunks inserted
        # Step 9  Dedup → top 25
        # ═══════════════════════════════════════════════════════════════

        # ── Step 1: Pool C chunks already in Pool A ───────────────────
        # Add scroll-type bonus to their existing vector score.
        # This raises their rank within Pool A — confirming they are
        # relevant from a different retrieval signal.
        in_both_count = 0
        for cid in payload_ids:
            if cid in vector_ids:
                scroll_bonus = _scroll_bonus(cid, scroll1_ids, scroll2_ids, scroll3_ids)
                pool_a_scores[cid] += scroll_bonus
                in_both_count += 1

        logger.info(
            f"Step 1: {in_both_count} scroll chunks already in vector -> "
            f"scroll bonus added to cosine score"
        )

        # -- Step 2: Pool C chunks NOT in Pool A ----------------------
        # These were found by keyword/payload matching but not in vector top-30.
        # Fetch their stored vectors from Qdrant, compute cosine similarity,
        # add scroll bonus, insert into Pool A.
        payload_not_in_vector = payload_ids - vector_ids
        if payload_not_in_vector and query_vector:
            logger.info(
                f"Step 2: {len(payload_not_in_vector)} scroll-only chunks - "
                f"fetching stored vectors for cosine similarity..."
            )
            try:
                fetched = self._qdrant.retrieve(
                    collection_name=self._col,
                    ids=list(payload_not_in_vector),
                    with_payload=True,
                    with_vectors=[settings.QDRANT_TEXT_VECTOR],
                )
                inserted = 0
                for point in fetched:
                    cid  = str(point.id)
                    vecs = point.vector or {}
                    vec  = vecs.get(settings.QDRANT_TEXT_VECTOR)
                    if vec is None:
                        continue
                    sim          = _cosine_similarity(query_vector, vec)
                    scroll_bonus = _scroll_bonus(cid, scroll1_ids, scroll2_ids, scroll3_ids)
                    pool_a_scores[cid]   = sim + scroll_bonus
                    pool_a_payloads[cid] = point.payload or payload_pool.get(cid, {})
                    inserted += 1

                logger.info(
                    f"Step 2: {inserted} scroll-only chunks inserted into "
                    f"Pool A (cosine sim + scroll bonus)"
                )
            except Exception as e:
                logger.error(f"Step 2: vector fetch failed: {e}")
                # Fallback - scroll1 gets high base (exact field match is high confidence)
                for cid in payload_not_in_vector:
                    scroll_bonus = _scroll_bonus(cid, scroll1_ids, scroll2_ids, scroll3_ids)
                    base = 0.15 if cid in scroll1_ids else 0.05
                    pool_a_scores[cid]   = base + scroll_bonus
                    pool_a_payloads[cid] = payload_pool.get(cid, {})
                s1_count = sum(1 for cid in payload_not_in_vector if cid in scroll1_ids)
                logger.info(
                    f"Step 2 fallback: {s1_count} scroll1 chunks -> base=0.15+bonus, "
                    f"rest -> base=0.05+bonus"
                )
        elif payload_not_in_vector and not query_vector:
            # No query vector available - use scroll bonuses only as score
            for cid in payload_not_in_vector:
                scroll_bonus = _scroll_bonus(cid, scroll1_ids, scroll2_ids, scroll3_ids)
                pool_a_scores[cid]   = scroll_bonus
                pool_a_payloads[cid] = payload_pool.get(cid, {})

        # Pool A is now complete: vector + keyword/scroll combined
        pool_a_ids = set(pool_a_scores.keys())
        logger.info(
            f"Step 3: Pool A unified = {len(pool_a_ids)} chunks "
            f"(original vector {len(vector_ids)} + "
            f"{len(pool_a_ids) - len(vector_ids)} added from scrolls)"
        )

        # -- Step 4: RRF between Pool A and Pool B (BM25) --------------
        # Pool A is now a proper ranked list (by cosine sim + scroll bonus).
        # Pool B (BM25) is a proper ranked list (by BM25 score).
        # RRF combines their rank positions - sidesteps scale incompatibility.
        # k=60 standard.

        # Rank Pool A by score descending
        pool_a_ranked = sorted(pool_a_scores.items(), key=lambda x: x[1], reverse=True)
        # BM25 already ordered by Qdrant (highest score first)

        rrf_scores: Dict[str, float] = {}
        all_payloads: Dict[str, Dict] = dict(pool_a_payloads)

        for rank, (cid, _) in enumerate(pool_a_ranked, start=1):
            rrf_scores[cid] = rrf_scores.get(cid, 0.0) + 1.0 / (_RRF_K + rank)

        for rank, (cid, payload, _) in enumerate(bm25_scored, start=1):
            rrf_scores[cid] = rrf_scores.get(cid, 0.0) + 1.0 / (_RRF_K + rank)
            all_payloads.setdefault(cid, payload)   # BM25-only chunks need payload

        rrf_max = max(rrf_scores.values(), default=1.0)
        in_both = sum(1 for cid in pool_a_ids if cid in bm25_ids)
        logger.info(
            f"Step 4 RRF: {len(rrf_scores)} unique chunks "
            f"| {in_both} in both Pool A and BM25 "
            f"| rrf_max={rrf_max:.4f}"
        )

        # -- Exclude pinned citation chunks from pool -------------------
        if pinned_citation:
            before = len(rrf_scores)
            rrf_scores = {
                cid: s for cid, s in rrf_scores.items()
                if (all_payloads.get(cid, {}).get("ext") or {}).get("citation") != pinned_citation
            }
            removed = before - len(rrf_scores)
            if removed:
                logger.info(f"Excluded {removed} chunks matching pinned citation")

        # -- Step 5: Intent weights -------------------------------------
        weight_count = 0
        for cid in list(rrf_scores.keys()):
            ct = all_payloads.get(cid, {}).get("chunk_type", "")
            w  = intent.score_weights.get(ct, 0.0)
            if w > 0:
                rrf_scores[cid] += w
                weight_count += 1
        logger.info(
            f"Step 5 intent weights: applied to {weight_count} chunks "
            f"weights={intent.score_weights}"
        )

        # -- Step 6: Sort -> top 30 --------------------------------------
        sorted_pool = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)[:_TOP_N]
        top30 = [
            ScoredChunk(
                chunk_id=cid,
                payload=all_payloads.get(cid, {}),
                score=score,
                source_sets=_source_sets(
                    cid, vector_ids, bm25_ids,
                    scroll1_ids, scroll2_ids, scroll3_ids
                ),
            )
            for cid, score in sorted_pool
        ]

        score_hi = top30[0].score if top30 else 0
        score_lo = top30[-1].score if top30 else 0
        logger.info(
            f"Step 6 top 30: score range {score_hi:.4f}->{score_lo:.4f} | "
            f"types={[c.payload.get('chunk_type','?') for c in top30[:8]]}"
        )

        # -- Step 7: Match depth boost ---------------------------------
        query_tokens = _get_query_legal_tokens(stage2b)
        if query_tokens:
            boosted = 0
            for chunk in top30:
                b = _match_depth_boost(chunk.payload, query_tokens)
                if b > 0:
                    chunk.score += b
                    boosted += 1
            logger.info(
                f"Match depth boost: {boosted} chunks boosted "
                f"(query tokens={query_tokens})"
            )
            top30.sort(key=lambda c: c.score, reverse=True)

        # -- Step 8: Insert pinned chunks (citation + name + case) -----
        # These come in ORDER: citation first, then case/name
        # Append retrieval reason to payload text field
        all_pinned: List[ScoredChunk] = []

        if pinned_citation_chunks:
            for chunk in pinned_citation_chunks:
                cid = str(chunk.get("id", chunk.get("_chunk_id", "")))
                _append_retrieval_reason(chunk, f"citation '{pinned_citation}'")
                all_pinned.append(ScoredChunk(
                    chunk_id=cid,
                    payload=chunk,
                    score=1.0,
                    source_sets={"citation"},
                    pinned=True,
                ))

        for cid, payload, reason in pinned_pairs:
            _append_retrieval_reason(payload, reason)
            all_pinned.append(ScoredChunk(
                chunk_id=cid,
                payload=payload,
                score=0.99,
                source_sets={"name_case_search"},
                pinned=True,
            ))

        if all_pinned:
            logger.info(
                f"Pinned: {len(all_pinned)} chunks inserted at top "
                f"(citation={len(pinned_citation_chunks or [])} "
                f"name/case={len(pinned_pairs)})"
            )

        # -- Step 9: Dedup by chunk id -> final top 25 -----------------
        seen: Set[str] = set()
        final: List[ScoredChunk] = []

        # Pinned first (citation -> case/name order)
        for chunk in all_pinned:
            if chunk.chunk_id not in seen:
                seen.add(chunk.chunk_id)
                final.append(chunk)

        # Then highest-scoring regular chunks
        for chunk in top30:
            if chunk.chunk_id not in seen and len(final) < _FINAL_N:
                seen.add(chunk.chunk_id)
                final.append(chunk)

        if final:
            logger.info(
                f"Stage 4 complete: {len(final)} chunks for LLM | "
                f"pinned={sum(1 for c in final if c.pinned)} | "
                f"score range: {final[0].score:.4f} -> {final[-1].score:.4f} | "
                f"types: {[c.payload.get('chunk_type','?') for c in final]}"
            )
        else:
            logger.info("Stage 4 complete: 0 chunks found.")
            
        return final

    # -- Vector search -------------------------------------------------

    def _vector_search(
        self, query_vector: List[float]
    ) -> List[Tuple[str, Dict, float]]:
        try:
            resp = self._qdrant.query_points(
                collection_name=self._col,
                query=query_vector,
                using=settings.QDRANT_SPARSE_VECTOR,
                limit=_TOP_N,
                with_payload=True,
            )
            return [(str(r.id), r.payload or {}, r.score) for r in resp.points]
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            return []

    # -- BM25 search --------------------------------------------------------

    def _bm25_search(
        self, indices: List[int], values: List[float]
    ) -> List[Tuple[str, Dict, float]]:
        if not indices:
            logger.warning("BM25: empty sparse vector  -  skipping")
            return []
        try:
            resp = self._qdrant.query_points(
                collection_name=self._col,
                query=qmodels.SparseVector(indices=indices, values=values),
                using=settings.QDRANT_SPARSE_VECTOR,
                limit=_TOP_N,
                with_payload=True,
            )
            return [(str(r.id), r.payload or {}, r.score) for r in resp.points]
        except Exception as e:
            logger.error(f"BM25 search failed: {e}")
            return []

    # -- Scroll 1 - direct ext field match ----------------------------

    def _scroll1(self, stage2b: Stage2BResult) -> List[Tuple[str, Dict]]:
        results: List[Tuple[str, Dict]] = []

        for sec in stage2b.sections:
            num = _bare_number(sec)
            if not num:
                continue
            num_only = re.sub(r"[A-Za-z]+$", "", num)
            search_nums = list(dict.fromkeys(filter(None, [
                num, num_only if num_only != num else None
            ])))
            for sn in search_nums:
                found = self._scroll(
                    [qmodels.FieldCondition(key="ext.section_number",
                                            match=qmodels.MatchValue(value=sn)),
                     qmodels.FieldCondition(key="chunk_type",
                                            match=qmodels.MatchAny(any=SECTION_CHUNK_TYPES))],
                    limit=20, label=f"s1_sec_{sn}"
                )
                results += found
                if found:
                    logger.info(f"  scroll1 section {sn}: {len(found)} chunks")

        for rule in stage2b.rules:
            num = _bare_number(rule)
            if num:
                full = f"Rule {num}"
                found = self._scroll(
                    [qmodels.FieldCondition(key="ext.rule_number_full",
                                            match=qmodels.MatchValue(value=full)),
                     qmodels.FieldCondition(key="chunk_type",
                                            match=qmodels.MatchAny(any=RULE_CHUNK_TYPES))],
                    limit=20, label=f"s1_rule_{num}"
                )
                results += found
                if found:
                    logger.info(f"  scroll1 rule {full}: {len(found)} chunks")

        for notif in stage2b.notifications:
            num = _normalise_notif_num(notif)
            if num:
                results += self._scroll(
                    [qmodels.FieldCondition(key="ext.notification_number",
                                            match=qmodels.MatchValue(value=num)),
                     qmodels.FieldCondition(key="chunk_type",
                                            match=qmodels.MatchValue(value="notification"))],
                    limit=20, label=f"s1_notif_{num}"
                )

        for circ in stage2b.circulars:
            num = _normalise_circ_num(circ)
            if num:
                results += self._scroll(
                    [qmodels.FieldCondition(key="ext.circular_number",
                                            match=qmodels.MatchValue(value=num)),
                     qmodels.FieldCondition(key="chunk_type",
                                            match=qmodels.MatchValue(value="circular"))],
                    limit=20, label=f"s1_circ_{num}"
                )

        if stage2b.form_name or stage2b.form_number:
            fn = stage2b.form_name
            if fn:
                results += self._scroll(
                    [qmodels.FieldCondition(key="ext.form_name",
                                            match=qmodels.MatchValue(value=fn.upper())),
                     qmodels.FieldCondition(key="chunk_type",
                                            match=qmodels.MatchAny(any=FORM_CHUNK_TYPES))],
                    limit=20, label="s1_form_name"
                )
            fnum = _normalise_form_num(stage2b.form_number) if stage2b.form_number else None
            if fnum and not results:
                results += self._scroll(
                    [qmodels.FieldCondition(key="ext.form_number",
                                            match=qmodels.MatchValue(value=fnum)),
                     qmodels.FieldCondition(key="chunk_type",
                                            match=qmodels.MatchAny(any=FORM_CHUNK_TYPES))],
                    limit=20, label="s1_form_num"
                )

        if stage2b.hsn_code:
            code = stage2b.hsn_code
            results += self._scroll(
                [qmodels.FieldCondition(key="ext.hsn_code",
                                        match=qmodels.MatchValue(value=code)),
                 qmodels.FieldCondition(key="chunk_type",
                                        match=qmodels.MatchValue(value="hsn_code"))],
                limit=10, label=f"s1_hsn_{code}"
            )
            if len(code) >= 2:
                results += self._scroll(
                    [qmodels.FieldCondition(key="ext.chapter_code",
                                            match=qmodels.MatchValue(value=code[:2])),
                     qmodels.FieldCondition(key="chunk_type",
                                            match=qmodels.MatchValue(value="hsn_code"))],
                    limit=5, label=f"s1_hsn_ch_{code[:2]}"
                )
            results += self._scroll(
                [qmodels.FieldCondition(key="chunk_type",
                                        match=qmodels.MatchValue(value="notification")),
                 qmodels.FieldCondition(key="cross_references.hsn_codes",
                                        match=qmodels.MatchValue(value=code))],
                limit=10, label=f"s1_hsn_notif_{code}"
            )
            results += self._scroll(
                [qmodels.FieldCondition(key="chunk_type",
                                        match=qmodels.MatchValue(value="circular")),
                 qmodels.FieldCondition(key="cross_references.hsn_codes",
                                        match=qmodels.MatchValue(value=code))],
                limit=5, label=f"s1_hsn_circ_{code}"
            )

        if stage2b.sac_code:
            code = stage2b.sac_code
            results += self._scroll(
                [qmodels.FieldCondition(key="ext.sac_code",
                                        match=qmodels.MatchValue(value=code)),
                 qmodels.FieldCondition(key="chunk_type",
                                        match=qmodels.MatchValue(value="sac_code"))],
                limit=10, label=f"s1_sac_{code}"
            )
            results += self._scroll(
                [qmodels.FieldCondition(key="chunk_type",
                                        match=qmodels.MatchValue(value="notification")),
                 qmodels.FieldCondition(key="cross_references.sac_codes",
                                        match=qmodels.MatchValue(value=code))],
                limit=10, label=f"s1_sac_notif_{code}"
            )
            results += self._scroll(
                [qmodels.FieldCondition(key="chunk_type",
                                        match=qmodels.MatchValue(value="circular")),
                 qmodels.FieldCondition(key="cross_references.sac_codes",
                                        match=qmodels.MatchValue(value=code))],
                limit=5, label=f"s1_sac_circ_{code}"
            )

        return results

    # -- Scroll 2 - cross-reference match -----------------------------

    def _scroll2(self, stage2b: Stage2BResult) -> List[Tuple[str, Dict]]:
        results: List[Tuple[str, Dict]] = []

        for sec in stage2b.sections:
            num = _bare_number(sec)
            if not num:
                continue
            num_only = re.sub(r"[A-Za-z]+$", "", num)
            variants = list(dict.fromkeys(filter(None, [
                num, f"Section {num}", f"section {num}",
                num_only if num_only != num else None,
                f"Section {num_only}" if num_only != num else None,
            ])))
            for v in variants:
                results += self._scroll(
                    [qmodels.FieldCondition(key="cross_references.sections",
                                            match=qmodels.MatchValue(value=v))],
                    limit=15, label=f"s2_sec_{num}"
                )
                results += self._scroll(
                    [qmodels.FieldCondition(key="ext.sections_referred",
                                            match=qmodels.MatchValue(value=v))],
                    limit=10, label=f"s2_sec_ext_{num}"
                )
            # Judgment ext.sections_in_dispute text search
            for n in list(dict.fromkeys(filter(None, [num, num_only if num_only != num else None]))):
                try:
                    results += self._scroll(
                        [qmodels.FieldCondition(key="chunk_type",
                                                match=qmodels.MatchValue(value="judgment")),
                         qmodels.FieldCondition(key="ext.sections_in_dispute",
                                                match=qmodels.MatchText(text=n))],
                        limit=15, label=f"s2_jud_sec_{n}"
                    )
                except Exception:
                    pass

        for rule in stage2b.rules:
            num = _bare_number(rule)
            if not num:
                continue
            variants = [num, f"Rule {num}", f"rule {num}", f"{num}(1)", f"Rule {num}(1)"]
            for v in variants:
                results += self._scroll(
                    [qmodels.FieldCondition(key="cross_references.rules",
                                            match=qmodels.MatchValue(value=v))],
                    limit=15, label=f"s2_rule_{num}"
                )
                results += self._scroll(
                    [qmodels.FieldCondition(key="ext.rules_referred",
                                            match=qmodels.MatchValue(value=v))],
                    limit=10, label=f"s2_rule_ext_{num}"
                )

        for notif in stage2b.notifications:
            num = _normalise_notif_num(notif)
            if num:
                results += self._scroll(
                    [qmodels.FieldCondition(key="cross_references.notifications",
                                            match=qmodels.MatchValue(value=num))],
                    limit=15, label=f"s2_notif_{num}"
                )

        for circ in stage2b.circulars:
            num = _normalise_circ_num(circ)
            if num:
                results += self._scroll(
                    [qmodels.FieldCondition(key="cross_references.circulars",
                                            match=qmodels.MatchValue(value=num))],
                    limit=15, label=f"s2_circ_{num}"
                )

        for form in ([stage2b.form_name] if stage2b.form_name else []) + \
                    ([stage2b.form_number] if stage2b.form_number else []):
            results += self._scroll(
                [qmodels.FieldCondition(key="cross_references.forms",
                                        match=qmodels.MatchValue(value=form))],
                limit=10, label=f"s2_form_{form}"
            )
            results += self._scroll(
                [qmodels.FieldCondition(key="ext.forms_prescribed",
                                        match=qmodels.MatchValue(value=form))],
                limit=10, label=f"s2_form_ext_{form}"
            )

        return results

    # -- Scroll 3 - keyword match --------------------------------------

    def _scroll3(self, stage2b: Stage2BResult) -> List[Tuple[str, Dict]]:
        results: List[Tuple[str, Dict]] = []

        section_variants: List[str] = []
        for sec in stage2b.sections:
            num = _bare_number(sec)
            if not num:
                continue
            num_only = re.sub(r"[A-Za-z]+$", "", num)
            for v in dict.fromkeys(filter(None, [
                f"section {num}", f"Section {num}",
                f"section {num_only}" if num_only != num else None,
                f"Section {num_only}" if num_only != num else None,
            ])):
                section_variants.append(v)

        rule_variants: List[str] = []
        for rule in stage2b.rules:
            num = _bare_number(rule)
            if num:
                rule_variants += [f"Rule {num}", f"rule {num}"]

        form_variants: List[str] = []
        for form in ([stage2b.form_name] if stage2b.form_name else []) + \
                    ([stage2b.form_number] if stage2b.form_number else []):
            if form:
                form_variants.append(form)

        all_exact = list(dict.fromkeys(section_variants + rule_variants + form_variants))
        if all_exact:
            try:
                found = self._scroll(
                    [qmodels.FieldCondition(key="keywords",
                                            match=qmodels.MatchAny(any=all_exact))],
                    limit=20, label="s3_exact"
                )
                results += found
                if found:
                    logger.info(
                        f"  scroll3 MatchAny: {len(found)} chunks "
                        f"for {all_exact[:4]}"
                    )
            except Exception as e:
                logger.debug(f"scroll3 MatchAny failed: {e}")

        for term in stage2b.keywords[:8] + stage2b.topics[:4]:
            if not term:
                continue
            try:
                results += self._scroll(
                    [qmodels.FieldCondition(key="keywords",
                                            match=qmodels.MatchText(text=term))],
                    limit=8, label=f"s3_text_{term[:20]}"
                )
            except Exception:
                pass

        return results

    # -- Name search (pinned) ------------------------------------------

    def _name_search(self, names: List[str]) -> List[Tuple[str, Dict]]:
        words = _name_words(names)
        if not words:
            return []
        results: List[Tuple[str, Dict]] = []
        for word in words[:8]:
            for key in ("ext.case_name", "ext.petitioner", "ext.respondent"):
                try:
                    results += self._scroll(
                        [qmodels.FieldCondition(key="chunk_type",
                                                match=qmodels.MatchValue(value="judgment")),
                         qmodels.FieldCondition(key=key,
                                                match=qmodels.MatchText(text=word))],
                        limit=10, label=f"name_{word[:15]}"
                    )
                except Exception as e:
                    logger.debug(f"Name search failed for {word}: {e}")
        return _dedup_by_id_pairs(results)

    # -- Case number search (pinned) -----------------------------------

    def _case_number_search(self, case_number: str) -> List[Tuple[str, Dict]]:
        primary = _primary_case_number(case_number)
        if not primary:
            return []
        try:
            results = self._scroll(
                [qmodels.FieldCondition(key="chunk_type",
                                        match=qmodels.MatchValue(value="judgment")),
                 qmodels.FieldCondition(key="ext.case_number",
                                        match=qmodels.MatchText(text=primary))],
                limit=20, label=f"case_{primary}"
            )
            logger.info(f"  case_search: {len(results)} results for primary='{primary}'")
            return results
        except Exception as e:
            logger.warning(f"Case number search failed: {e}")
            return []

    # -- Scroll helper -------------------------------------------------

    def _scroll(
        self,
        conditions: List[Any],
        limit: int = 20,
        label: str = "",
    ) -> List[Tuple[str, Dict]]:
        try:
            results, _ = self._qdrant.scroll(
                collection_name=self._col,
                scroll_filter=qmodels.Filter(must=conditions),
                limit=limit,
                with_payload=True,
                with_vectors=False,
            )
            return [(str(r.id), r.payload or {}) for r in results]
        except Exception as e:
            logger.debug(f"Scroll [{label}] failed: {e}")
            return []


# -- Text index setup ---------------------------------------------------------

def ensure_text_indexes(qdrant: QdrantClient):
    fields = [
        "ext.case_name", "ext.petitioner", "ext.respondent",
        "ext.case_number", "ext.sections_in_dispute", "keywords",
    ]
    for field in fields:
        try:
            qdrant.create_payload_index(
                collection_name=settings.QDRANT_COLLECTION,
                field_name=field,
                field_schema=qmodels.TextIndexParams(
                    type="text",
                    tokenizer=qmodels.TokenizerType.WORD,
                    min_token_len=2,
                    max_token_len=50,
                    lowercase=True,
                ),
            )
            logger.info(f"Text index created: {field}")
        except Exception as e:
            logger.debug(f"Text index {field} skipped: {e}")


# -- Match depth boost --------------------------------------------------------

def _match_depth_boost(payload: Dict, query_tokens: List[str]) -> float:
    """
    +0.08 per identifier found in cross_references OR keywords (or both).
    Same identifier in both locations = still +0.08 - not cumulative.
    It is the same evidence stored in two places.
    Cap: +0.24 total (max 3 distinct identifiers matched).
    """
    chunk_type = payload.get("chunk_type", "")
    xrefs      = payload.get("cross_references") or {}
    boost      = 0.0

    if chunk_type == "judgment":
        xref_tokens = set(_parse_judgment_xrefs(xrefs))
    else:
        xref_tokens = set(_parse_clean_xrefs(xrefs))

    kw_text = " ".join(str(k).lower() for k in (payload.get("keywords") or []))

    for token in query_tokens:
        in_xref = token in xref_tokens
        in_kw   = token.replace("_", " ") in kw_text or token in kw_text
        if in_xref or in_kw:   # either location = same signal, +0.08 once
            boost += 0.08

    return min(boost, 0.24)


# -- Cross-reference parsers ---------------------------------------------------

def _parse_judgment_xrefs(xrefs: Dict) -> List[str]:
    tokens = []
    for raw in _xref_list(xrefs, "sections"):
        for part in re.split(r"[&,]|\band\b", raw, flags=re.IGNORECASE):
            part = re.sub(r"^section\s*", "", part.strip(), flags=re.IGNORECASE).strip()
            m = re.match(r"(\d+[A-Za-z]{0,2})", part)
            if m:
                tokens.append(f"section_{m.group(1).lower()}")
    for raw in _xref_list(xrefs, "rules"):
        for part in re.split(r"[&,]|\band\b", raw, flags=re.IGNORECASE):
            part = re.sub(r"^rule\s*", "", part.strip(), flags=re.IGNORECASE).strip()
            m = re.match(r"(\d+[A-Za-z]{0,2})", part)
            if m:
                tokens.append(f"rule_{m.group(1).lower()}")
    for raw in _xref_list(xrefs, "notifications"):
        parts = re.split(r"notification\s+no\.?\s*", raw, flags=re.IGNORECASE)
        for part in parts:
            m = re.match(r"(\d+\s*/\s*\d+)", part.strip(" ,-"))
            if m:
                num = re.sub(r"\s*", "", m.group(1)).replace("/", "_")
                tokens.append(f"notification_{num}")
    for raw in _xref_list(xrefs, "circulars"):
        parts = re.split(r"circular\s+no\.?\s*", raw, flags=re.IGNORECASE)
        for part in parts:
            m = re.match(r"(\d+[/\-]\d+(?:[/\-]\d+)?)", part.strip(" ,-"))
            if m:
                num = re.sub(r"[/\-]", "_", m.group(1))
                tokens.append(f"circular_{num}")
    return tokens


def _parse_clean_xrefs(xrefs: Dict) -> List[str]:
    tokens = []
    for sec in _xref_list(xrefs, "sections"):
        m = re.search(r"(\d+[A-Za-z]{0,2})", sec)
        if m:
            tokens.append(f"section_{m.group(1).lower()}")
    for rule in _xref_list(xrefs, "rules"):
        m = re.search(r"(\d+[A-Za-z]{0,2})", rule)
        if m:
            tokens.append(f"rule_{m.group(1).lower()}")
    for notif in _xref_list(xrefs, "notifications"):
        m = re.search(r"(\d+\s*/\s*\d+)", notif)
        if m:
            num = re.sub(r"\s*", "", m.group(1)).replace("/", "_")
            tokens.append(f"notification_{num}")
    for circ in _xref_list(xrefs, "circulars"):
        m = re.search(r"(\d+[/\-]\d+)", circ)
        if m:
            num = re.sub(r"[/\-]", "_", m.group(1))
            tokens.append(f"circular_{num}")
    return tokens


def _get_query_legal_tokens(stage2b: Stage2BResult) -> List[str]:
    tokens = []
    for s in stage2b.sections:
        m = re.search(r"(\d+[A-Za-z]{0,2})", s)
        if m:
            tokens.append(f"section_{m.group(1).lower()}")
    for r in stage2b.rules:
        m = re.search(r"(\d+[A-Za-z]{0,2})", r)
        if m:
            tokens.append(f"rule_{m.group(1).lower()}")
    for n in stage2b.notifications:
        m = re.search(r"(\d+\s*/\s*\d+)", n)
        if m:
            num = re.sub(r"\s*", "", m.group(1)).replace("/", "_")
            tokens.append(f"notification_{num}")
    for c in stage2b.circulars:
        m = re.search(r"(\d+[/\-]\d+)", c)
        if m:
            num = re.sub(r"[/\-]", "_", m.group(1))
            tokens.append(f"circular_{num}")
    return tokens


# -- Retrieval reason annotation -----------------------------------------------

def _append_retrieval_reason(payload: Dict, reason: str):
    """
    Appends the retrieval reason to the chunk's text field so the LLM
    understands why this chunk is pinned.
    """
    current = str(payload.get("text") or "").strip()
    tag = f"\n\n[PINNED - retrieved because: {reason}]"
    payload["text"] = current + tag


# -- Helpers -------------------------------------------------------------------

def _scroll_bonus(
    cid: str,
    scroll1_ids: Set[str],
    scroll2_ids: Set[str],
    scroll3_ids: Set[str],
) -> float:
    """Returns the scroll-type bonus for a chunk. Takes highest-priority match."""
    if cid in scroll1_ids:
        return _SCROLL1_BONUS
    if cid in scroll3_ids:
        return _SCROLL3_BONUS
    if cid in scroll2_ids:
        return _SCROLL2_BONUS
    return 0.0


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot   = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(x * x for x in b))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


def _source_sets(
    cid: str,
    vector_ids: Set[str],
    bm25_ids: Set[str],
    scroll1_ids: Set[str],
    scroll2_ids: Set[str],
    scroll3_ids: Set[str],
) -> Set[str]:
    sources = set()
    if cid in vector_ids:
        sources.add("vector")
    if cid in bm25_ids:
        sources.add("bm25")
    if cid in scroll1_ids:
        sources.add("scroll1")
    if cid in scroll2_ids:
        sources.add("scroll2")
    if cid in scroll3_ids:
        sources.add("scroll3")
    return sources


def _has_identifiers(stage2b: Stage2BResult) -> bool:
    return bool(
        stage2b.sections or stage2b.rules or stage2b.notifications
        or stage2b.circulars or stage2b.form_name or stage2b.form_number
    )


def _dedup_by_id_pairs(items: List[Tuple[str, Dict]]) -> List[Tuple[str, Dict]]:
    seen: Set[str] = set()
    out = []
    for chunk_id, payload in items:
        if chunk_id not in seen:
            seen.add(chunk_id)
            out.append((chunk_id, payload))
    return out


def _bare_number(ref: str) -> Optional[str]:
    m = re.search(r"(\d+[A-Za-z]{0,2})", str(ref))
    return m.group(1) if m else None


def _normalise_notif_num(ref: str) -> Optional[str]:
    m = re.search(r"(\d+\s*/\s*\d+)", ref)
    return re.sub(r"\s*", "", m.group(1)) if m else None


def _normalise_circ_num(ref: str) -> Optional[str]:
    ref = re.sub(r"circular\s*no\.?\s*[-]?\s*", "", ref, flags=re.IGNORECASE).strip()
    m = re.search(r"(\d+[/\-]\d+(?:[/\-]\d+)?)", ref)
    return m.group(1) if m else None


def _normalise_form_num(ref: str) -> Optional[str]:
    if not ref:
        return None
    ref = ref.strip().upper()
    ref = re.sub(r"FORM\s+GST\s+", "", ref)
    ref = re.sub(r"FORM\s+|GST\s+", "", ref)
    return re.sub(r"\s+", "-", ref.strip())


def _name_words(names: List[str]) -> List[str]:
    STOP = {"vs", "v", "the", "of", "and", "in", "ltd", "pvt",
            "mr", "mrs", "ms", "dr", "no", "for", "to", "a", "an"}
    words = []
    for name in names:
        for w in re.split(r"[\s,.\-&/]+", name.lower()):
            w = w.strip(".()")
            if len(w) >= 3 and w not in STOP:
                words.append(w)
    return list(dict.fromkeys(words))


def _primary_case_number(case_number: str) -> Optional[str]:
    nums = re.findall(r"\d+", case_number)
    for num in nums:
        if len(num) >= 4 and not (1900 <= int(num) <= 2100):
            return num
    return max(nums, key=len) if nums else None


def _xref_list(xrefs: Dict, key: str) -> List[str]:
    val = xrefs.get(key)
    if not val:
        return []
    if isinstance(val, list):
        return [str(v).strip() for v in val if v and str(v).strip()]
    return []
