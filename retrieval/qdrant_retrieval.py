"""
retrieval/qdrant_retrieval.py
Stage 4 — Retrieval, scoring, and final selection.

Three retrieval pools (all parallel):
    Pool A: vector  — dense similarity search, top 30 with cosine scores
    Pool B: bm25    — BM25 sparse search, top 30 with BM25 scores
    Pool C: payload — scroll1 + scroll2 + scroll3 (payload match, no score)

Pinned lookups (parallel, separate from scoring):
    citation, name/party search, case number search

Scoring model:
    Step 1  Pool C chunks already in Pool A → add scroll bonus to cosine score
    Step 2  Pool C chunks NOT in Pool A → fetch stored vectors → cosine sim
            + scroll bonus → insert into Pool A
    Step 3  Pool A is now unified: original vector results + keyword/scroll
            results, all scored on cosine-similarity basis (one ranked list)
    Step 4  RRF between Pool A (ranked) and Pool B/BM25 (ranked) → base score
    Step 5  Intent weights additive
    Step 6  Sort → top 30
    Step 7  Match depth boost (cross-ref +0.08, keyword +0.08, cap +0.24)
    Step 8  Pinned chunks inserted at top
    Step 9  Dedup by chunk id → top 25 for LLM

Scroll bonuses (additive to cosine score):
    scroll1 (direct ext field match)  → +0.18  highest confidence
    scroll3 (keyword array match)     → +0.12
    scroll2 (cross-reference match)   → +0.10

Fix applied:
    final[0] / final[-1] IndexError when Qdrant is unreachable and retrieve()
    returns an empty list.  Wrapped the final logger.info in `if final:` so
    0-chunk results log a warning instead of crashing with IndexError.
"""

import math
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

from config import (
    CONFIG,
    SECTION_CHUNK_TYPES,
    RULE_CHUNK_TYPES,
    FORM_CHUNK_TYPES,
)
from models.embedding_generator import TitanEmbeddingGenerator
from pipeline.bm25_vectorizer import BM25Vectorizer
from retrieval.models import IntentResult, ScoredChunk, Stage2BResult
from utils.logger import get_logger

logger = get_logger("qdrant_retrieval")

_TOP_N   = 30   # each individual search call returns this many
_FINAL_N = 25   # chunks sent to LLM
_RRF_K   = 60   # RRF constant

# Scroll bonuses — additive to cosine similarity score in Pool A
_SCROLL1_BONUS = 0.18   # direct ext field match (exact section/rule/form match)
_SCROLL3_BONUS = 0.12   # keyword array match
_SCROLL2_BONUS = 0.10   # cross-reference match


class QdrantRetrieval:

    def __init__(self, qdrant: QdrantClient, bm25: BM25Vectorizer):
        self._qdrant   = qdrant
        self._bm25     = bm25
        self._col      = CONFIG.qdrant.collection_name
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
        Returns [] safely when Qdrant is unreachable — individual search
        helpers already catch connection errors; this method never raises.
        """

        # ── Embed query ───────────────────────────────────────────────
        logger.info("Stage 4: embedding query...")
        query_vector = self._embedder.embed_text(query)
        if query_vector is None:
            logger.error("Stage 4: query embedding failed — BM25 + payload only")
        else:
            logger.info(f"Stage 4: query vector ready ({len(query_vector)} dims)")

        # ── BM25 sparse vector ────────────────────────────────────────
        sparse_indices, sparse_values = self._bm25.compute_sparse_vector(keyword_document)
        logger.info(
            f"Stage 4: BM25 sparse vector — "
            f"{len(sparse_indices)} non-zero dims from "
            f"{len(keyword_document.split())} tokens"
        )

        # ── Determine which calls to run ──────────────────────────────
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

        # ── Launch all parallel calls ─────────────────────────────────
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

        # ── Unpack results ────────────────────────────────────────────
        vector_scored: List[Tuple[str, Dict, float]] = raw_results.get("vector", [])
        bm25_scored:   List[Tuple[str, Dict, float]] = raw_results.get("bm25", [])

        scroll1_ids: Set[str] = set()
        scroll2_ids: Set[str] = set()
        scroll3_ids: Set[str] = set()
        payload_pool: Dict[str, Dict] = {}

        for source, scroll_set in [
            ("scroll1", scroll1_ids),
            ("scroll2", scroll2_ids),
            ("scroll3", scroll3_ids),
        ]:
            raw = raw_results.get(source, [])
            added = 0
            for chunk_id, payload in raw:
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

        # ── DIAGNOSTIC: top 10 vector search results ────────────────
        # Comment out this block once you have inspected the chunks.
        logger.info("  [DIAG] ── TOP 10 VECTOR SEARCH RESULTS ──────────────────")
        for _rank, (_cid, _p, _score) in enumerate(vector_scored[:10], 1):
            _ext      = _p.get("ext") or {}
            _ctype    = _p.get("chunk_type", "?")
            _parent   = _p.get("parent_doc", "?")
            _text_pre = str(_p.get("text") or "").strip()[:120].replace("", " ")
            _id_hint  = (
                _ext.get("_chunk_id") or
                _ext.get("citation") or
                _ext.get("section_number") or
                _ext.get("rule_number_full") or
                _ext.get("notification_number") or
                _ext.get("circular_number") or
                _p.get("_chunk_id") or
                _cid[:12]
            )
            logger.info(
                f"  [DIAG] #{_rank:02d}  score={_score:.4f}  "
                f"type={_ctype:<20}  id={_id_hint}  "
                f"parent={_parent}"
            )
            logger.info(f"         text_preview: {_text_pre}")
        logger.info("  [DIAG] ─────────────────────────────────────────────────")
        logger.info(
            f"Payload pool: {len(payload_ids)} unique chunks "
            f"(s1={len(scroll1_ids)} s2={len(scroll2_ids)} s3={len(scroll3_ids)})"
        )

        pinned_pairs: List[Tuple[str, Dict, str]] = []
        for source, reason_prefix in [("name_search", "party/name match"), ("case_search", "case number match")]:
            for chunk_id, payload in raw_results.get(source, []):
                pinned_pairs.append((chunk_id, payload, reason_prefix))
        if pinned_pairs:
            logger.info(
                f"Pinned: {len(pinned_pairs)} chunks "
                f"from name/case search — guaranteed top slots"
            )

        # ── Build id sets and payload maps ────────────────────────────
        vector_ids = {cid for cid, _, _ in vector_scored}
        bm25_ids   = {cid for cid, _, _ in bm25_scored}

        pool_a_scores:   Dict[str, float] = {cid: s for cid, _, s in vector_scored}
        pool_a_payloads: Dict[str, Dict]  = {cid: p for cid, p, _ in vector_scored}
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

        pinned_pairs: List[Tuple[str, Dict, str]] = []
        for source, reason_prefix in [("name_search", "party/name match"), ("case_search", "case number match")]:
            for chunk_id, payload in raw_results.get(source, []):
                pinned_pairs.append((chunk_id, payload, reason_prefix))
        if pinned_pairs:
            logger.info(
                f"Pinned: {len(pinned_pairs)} chunks from name/case search"
            )

        # ── Step 1: Pool C chunks already in Pool A ───────────────────
        in_both_count = 0
        for cid in payload_ids:
            if cid in vector_ids:
                scroll_bonus = _scroll_bonus(cid, scroll1_ids, scroll2_ids, scroll3_ids)
                pool_a_scores[cid] += scroll_bonus
                in_both_count += 1

        logger.info(
            f"Step 1: {in_both_count} scroll chunks already in vector → "
            f"scroll bonus added to cosine score"
        )

        # ── Step 2: Pool C chunks NOT in Pool A ──────────────────────
        payload_not_in_vector = payload_ids - vector_ids
        if payload_not_in_vector and query_vector:
            logger.info(
                f"Step 2: {len(payload_not_in_vector)} scroll-only chunks — "
                f"fetching stored vectors for cosine similarity..."
            )
            try:
                fetched = self._qdrant.retrieve(
                    collection_name=self._col,
                    ids=list(payload_not_in_vector),
                    with_payload=True,
                    with_vectors=[CONFIG.qdrant.text_vector_name],
                )
                inserted = 0
                for point in fetched:
                    cid  = str(point.id)
                    vecs = point.vector or {}
                    vec  = vecs.get(CONFIG.qdrant.text_vector_name)
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
                for cid in payload_not_in_vector:
                    scroll_bonus = _scroll_bonus(cid, scroll1_ids, scroll2_ids, scroll3_ids)
                    base = 0.15 if cid in scroll1_ids else 0.05
                    pool_a_scores[cid]   = base + scroll_bonus
                    pool_a_payloads[cid] = payload_pool.get(cid, {})
                s1_count = sum(1 for cid in payload_not_in_vector if cid in scroll1_ids)
                logger.info(
                    f"Step 2 fallback: {s1_count} scroll1 chunks → base=0.15+bonus, "
                    f"rest → base=0.05+bonus"
                )
        elif payload_not_in_vector and not query_vector:
            for cid in payload_not_in_vector:
                scroll_bonus = _scroll_bonus(cid, scroll1_ids, scroll2_ids, scroll3_ids)
                pool_a_scores[cid]   = scroll_bonus
                pool_a_payloads[cid] = payload_pool.get(cid, {})

        pool_a_ids = set(pool_a_scores.keys())
        logger.info(
            f"Step 3: Pool A unified = {len(pool_a_ids)} chunks "
            f"(original vector {len(vector_ids)} + "
            f"{len(pool_a_ids) - len(vector_ids)} added from scrolls)"
        )

        # ── Step 4: RRF between Pool A and Pool B (BM25) ──────────────
        pool_a_ranked = sorted(pool_a_scores.items(), key=lambda x: x[1], reverse=True)

        rrf_scores: Dict[str, float] = {}
        all_payloads: Dict[str, Dict] = dict(pool_a_payloads)

        for rank, (cid, _) in enumerate(pool_a_ranked, start=1):
            rrf_scores[cid] = rrf_scores.get(cid, 0.0) + 1.0 / (_RRF_K + rank)

        for rank, (cid, payload, _) in enumerate(bm25_scored, start=1):
            rrf_scores[cid] = rrf_scores.get(cid, 0.0) + 1.0 / (_RRF_K + rank)
            all_payloads.setdefault(cid, payload)

        rrf_max = max(rrf_scores.values(), default=1.0)
        in_both = sum(1 for cid in pool_a_ids if cid in bm25_ids)
        logger.info(
            f"Step 4 RRF: {len(rrf_scores)} unique chunks "
            f"| {in_both} in both Pool A and BM25 "
            f"| rrf_max={rrf_max:.4f}"
        )

        # ── Exclude pinned citation chunks from pool ───────────────────
        if pinned_citation:
            before = len(rrf_scores)
            rrf_scores = {
                cid: s for cid, s in rrf_scores.items()
                if (all_payloads.get(cid, {}).get("ext") or {}).get("citation") != pinned_citation
            }
            removed = before - len(rrf_scores)
            if removed:
                logger.info(f"Excluded {removed} chunks matching pinned citation")

        # ── Step 5: Intent weights ─────────────────────────────────────
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

        # ── Step 6: Sort → top 30 ──────────────────────────────────────
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

        if top30:
            score_hi = top30[0].score
            score_lo = top30[-1].score
            logger.info(
                f"Step 6 top 30: score range {score_hi:.4f}→{score_lo:.4f} | "
                f"types={[c.payload.get('chunk_type','?') for c in top30[:8]]}"
            )
        else:
            logger.info("Step 6 top 30: 0 chunks (Qdrant unreachable or empty collection)")

        # ── Step 7: Match depth boost ─────────────────────────────────
        query_tokens = _get_query_legal_tokens(stage2b)
        if query_tokens and top30:
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

        # ── Step 8: Insert pinned chunks (citation + name + case) ─────
        # All sources are merged and deduplicated by citation before inserting.
        # One judgment split into 10 chunks has the same citation in every chunk.
        # Keeping all 10 wastes 9 pinned slots with identical case_note content.
        # After dedup, one chunk per unique citation is kept — the one with the
        # longest case_note. Multiple citations (e.g. 2019 HC + 2024 SC for the
        # same case) are kept as separate entries since they are different orders.
        all_pinned: List[ScoredChunk] = []

        # Collect all candidate chunks from every pinned source
        all_pinned_candidates: List[Tuple[str, Dict, float, Set[str]]] = []
        # (chunk_id, payload, score, source_sets)

        if pinned_citation_chunks:
            for chunk in pinned_citation_chunks:
                cid = str(chunk.get("id", chunk.get("_chunk_id", "")))
                all_pinned_candidates.append((cid, chunk, 1.0, {"citation"}))

        for cid, payload, reason in pinned_pairs:
            all_pinned_candidates.append((cid, payload, 0.99, {"name_case_search"}))

        # Dedup all candidates by citation — one chunk per unique citation
        seen_citations: Dict[str, int] = {}   # citation → index in all_pinned
        seen_chunk_ids: Set[str] = set()

        for cid, payload, score, source_sets in all_pinned_candidates:
            if cid in seen_chunk_ids:
                continue
            ext      = payload.get("ext") or {}
            citation = str(ext.get("citation") or "").strip()

            if citation and citation in seen_citations:
                # Same citation already added — keep the one with better case_note
                existing_idx  = seen_citations[citation]
                existing_note = str(
                    (all_pinned[existing_idx].payload.get("ext") or {}).get("case_note") or ""
                )
                new_note = str(ext.get("case_note") or "")
                if len(new_note) > len(existing_note):
                    # Replace with the richer chunk
                    old_cid = all_pinned[existing_idx].chunk_id
                    seen_chunk_ids.discard(old_cid)
                    reason_label = (
                        f"citation '{pinned_citation}'" if "citation" in source_sets
                        else "party/name or case number match"
                    )
                    _append_retrieval_reason(payload, reason_label)
                    all_pinned[existing_idx] = ScoredChunk(
                        chunk_id=cid,
                        payload=payload,
                        score=score,
                        source_sets=source_sets,
                        pinned=True,
                    )
                    seen_chunk_ids.add(cid)
                # Either way skip adding a new entry for this citation
                continue

            # New citation (or chunk without citation) — add it
            reason_label = (
                f"citation '{pinned_citation}'" if "citation" in source_sets
                else "party/name or case number match"
            )
            _append_retrieval_reason(payload, reason_label)
            sc = ScoredChunk(
                chunk_id=cid,
                payload=payload,
                score=score,
                source_sets=source_sets,
                pinned=True,
            )
            if citation:
                seen_citations[citation] = len(all_pinned)
            seen_chunk_ids.add(cid)
            all_pinned.append(sc)

        if all_pinned:
            unique_citations = list(seen_citations.keys())
            logger.info(
                f"Pinned: {len(all_pinned)} unique chunks after citation dedup "
                f"(from {len(all_pinned_candidates)} raw candidates) | "
                f"citations: {unique_citations[:6]}"
            )

        # ── Step 9: Dedup by chunk id → final top 25 ─────────────────
        seen: Set[str] = set()
        final: List[ScoredChunk] = []

        for chunk in all_pinned:
            if chunk.chunk_id not in seen:
                seen.add(chunk.chunk_id)
                final.append(chunk)

        for chunk in top30:
            if chunk.chunk_id not in seen and len(final) < _FINAL_N:
                seen.add(chunk.chunk_id)
                final.append(chunk)

        # ── Final log — guarded against empty list ────────────────────
        # When Qdrant is unreachable all search calls return [] and final
        # is empty.  Accessing final[0] / final[-1] on [] raises IndexError.
        if final:
            logger.info(
                f"Stage 4 complete: {len(final)} chunks for LLM | "
                f"pinned={sum(1 for c in final if c.pinned)} | "
                f"score range: {final[0].score:.4f}→{final[-1].score:.4f} | "
                f"types: {[c.payload.get('chunk_type','?') for c in final]}"
            )
        else:
            logger.warning(
                "Stage 4 complete: 0 chunks retrieved — "
                "Qdrant may be unreachable or the collection is empty. "
                "LLM will respond from context alone."
            )

        return final

    # ── Vector search ─────────────────────────────────────────────────

    def _vector_search(
        self, query_vector: List[float]
    ) -> List[Tuple[str, Dict, float]]:
        try:
            results = self._qdrant.search(
                collection_name=self._col,
                query_vector=qmodels.NamedVector(
                    name=CONFIG.qdrant.text_vector_name,
                    vector=query_vector,
                ),
                limit=_TOP_N,
                with_payload=True,
            )
            return [(str(r.id), r.payload or {}, r.score) for r in results]
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            return []

    # ── BM25 search ───────────────────────────────────────────────────

    def _bm25_search(
        self, indices: List[int], values: List[float]
    ) -> List[Tuple[str, Dict, float]]:
        if not indices:
            logger.warning("BM25: empty sparse vector — skipping")
            return []
        try:
            results = self._qdrant.search(
                collection_name=self._col,
                query_vector=qmodels.NamedSparseVector(
                    name=CONFIG.qdrant.sparse_vector_name,
                    vector=qmodels.SparseVector(indices=indices, values=values),
                ),
                limit=_TOP_N,
                with_payload=True,
            )
            return [(str(r.id), r.payload or {}, r.score) for r in results]
        except Exception as e:
            logger.error(f"BM25 search failed: {e}")
            return []

    # ── Scroll 1 — direct ext field match ────────────────────────────

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

    # ── Scroll 2 — cross-reference match ─────────────────────────────

    def _scroll2(self, stage2b: Stage2BResult) -> List[Tuple[str, Dict]]:
        """
        Cross-reference match scroll.
        Previously all variant loops ran sequentially — each self._scroll() call
        waited for the previous one before starting.  For a query like
        "section 17" this produced 9+ sequential Qdrant round-trips (~9s).

        Fix: collect all (conditions, limit, label) tuples first, then run
        every scroll call in parallel via ThreadPoolExecutor.  Total time =
        slowest single call (~1s) instead of sum of all calls (~9s).
        """
        # ── Step 1: Build all scroll call specs ───────────────────────
        calls: List[Tuple[List, int, str]] = []  # (conditions, limit, label)

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
                calls.append((
                    [qmodels.FieldCondition(key="cross_references.sections",
                                            match=qmodels.MatchValue(value=v))],
                    15, f"s2_sec_{num}"
                ))
                calls.append((
                    [qmodels.FieldCondition(key="ext.sections_referred",
                                            match=qmodels.MatchValue(value=v))],
                    10, f"s2_sec_ext_{num}"
                ))
            for n in list(dict.fromkeys(filter(None, [num, num_only if num_only != num else None]))):
                calls.append((
                    [qmodels.FieldCondition(key="chunk_type",
                                            match=qmodels.MatchValue(value="judgment")),
                     qmodels.FieldCondition(key="ext.sections_in_dispute",
                                            match=qmodels.MatchText(text=n))],
                    15, f"s2_jud_sec_{n}"
                ))

        for rule in stage2b.rules:
            num = _bare_number(rule)
            if not num:
                continue
            for v in [num, f"Rule {num}", f"rule {num}", f"{num}(1)", f"Rule {num}(1)"]:
                calls.append((
                    [qmodels.FieldCondition(key="cross_references.rules",
                                            match=qmodels.MatchValue(value=v))],
                    15, f"s2_rule_{num}"
                ))
                calls.append((
                    [qmodels.FieldCondition(key="ext.rules_referred",
                                            match=qmodels.MatchValue(value=v))],
                    10, f"s2_rule_ext_{num}"
                ))

        for notif in stage2b.notifications:
            num = _normalise_notif_num(notif)
            if num:
                calls.append((
                    [qmodels.FieldCondition(key="cross_references.notifications",
                                            match=qmodels.MatchValue(value=num))],
                    15, f"s2_notif_{num}"
                ))

        for circ in stage2b.circulars:
            num = _normalise_circ_num(circ)
            if num:
                calls.append((
                    [qmodels.FieldCondition(key="cross_references.circulars",
                                            match=qmodels.MatchValue(value=num))],
                    15, f"s2_circ_{num}"
                ))

        for form in ([stage2b.form_name] if stage2b.form_name else []) + \
                    ([stage2b.form_number] if stage2b.form_number else []):
            calls.append((
                [qmodels.FieldCondition(key="cross_references.forms",
                                        match=qmodels.MatchValue(value=form))],
                10, f"s2_form_{form}"
            ))
            calls.append((
                [qmodels.FieldCondition(key="ext.forms_prescribed",
                                        match=qmodels.MatchValue(value=form))],
                10, f"s2_form_ext_{form}"
            ))

        if not calls:
            return []

        # ── Step 2: Run all scroll calls in parallel ───────────────────
        results: List[Tuple[str, Dict]] = []
        with ThreadPoolExecutor(max_workers=min(12, len(calls))) as ex:
            future_map = {
                ex.submit(self._scroll, conditions, limit, label): label
                for conditions, limit, label in calls
            }
            for future in as_completed(future_map):
                try:
                    results += future.result()
                except Exception as e:
                    logger.debug(f"scroll2 parallel call failed: {e}")

        return results

    # ── Scroll 3 — keyword match ──────────────────────────────────────

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

    # ── Name search (pinned) ──────────────────────────────────────────

    def _name_search(self, names: List[str]) -> List[Tuple[str, Dict]]:
        """
        Search for judgments by party name / company name.
        All scroll calls (3 fields × N words) run in parallel.
        Results are deduplicated by citation — keeping the best chunk
        (longest case_note) per unique citation, capped at 5 citations.
        This prevents a single judgment split into 10 chunks from consuming
        20 pinned slots and overloading the LLM prompt.
        """
        words = _name_words(names)
        if not words:
            return []

        # Build all scroll call specs
        calls = []
        for word in words[:6]:
            for key in ("ext.case_name", "ext.petitioner", "ext.respondent"):
                calls.append((word, key))

        if not calls:
            return []

        # Run all in parallel
        raw: List[Tuple[str, Dict]] = []
        with ThreadPoolExecutor(max_workers=min(12, len(calls))) as ex:
            future_map = {
                ex.submit(
                    self._scroll,
                    [qmodels.FieldCondition(key="chunk_type",
                                            match=qmodels.MatchValue(value="judgment")),
                     qmodels.FieldCondition(key=key,
                                            match=qmodels.MatchText(text=word))],
                    10, f"name_{word[:12]}_{key.split('.')[-1]}"
                ): (word, key)
                for word, key in calls
            }
            for future in as_completed(future_map):
                try:
                    raw += future.result()
                except Exception as e:
                    logger.debug(f"Name search failed: {e}")

        return _dedup_by_citation(raw, max_citations=5)

    # ── Case number search (pinned) ───────────────────────────────────

    def _case_number_search(self, case_number: str) -> List[Tuple[str, Dict]]:
        """
        Search by case number. Deduplicates by citation keeping one chunk
        per unique citation (the one with the best case_note).
        """
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
            deduped = _dedup_by_citation(results, max_citations=5)
            logger.info(
                f"  case_search: {len(results)} raw → "
                f"{len(deduped)} after citation dedup for primary='{primary}'"
            )
            return deduped
        except Exception as e:
            logger.warning(f"Case number search failed: {e}")
            return []

    # ── Scroll helper ─────────────────────────────────────────────────

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


# ── Text index setup ──────────────────────────────────────────────────────────

def ensure_text_indexes(qdrant: QdrantClient):
    fields = [
        "ext.case_name", "ext.petitioner", "ext.respondent",
        "ext.case_number", "ext.sections_in_dispute", "keywords",
    ]
    for field in fields:
        try:
            qdrant.create_payload_index(
                collection_name=CONFIG.qdrant.collection_name,
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


# ── Match depth boost ─────────────────────────────────────────────────────────

def _match_depth_boost(payload: Dict, query_tokens: List[str]) -> float:
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
        if in_xref or in_kw:
            boost += 0.08

    return min(boost, 0.24)


# ── Cross-reference parsers ───────────────────────────────────────────────────

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
            m = re.match(r"(\d+[/\-]\d+(?:[/\-]\d+)?)", part.strip(" ,-–"))
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


# ── Retrieval reason annotation ───────────────────────────────────────────────

def _append_retrieval_reason(payload: Dict, reason: str):
    current = str(payload.get("text") or "").strip()
    tag = f"\n\n[PINNED — retrieved because: {reason}]"
    payload["text"] = current + tag


# ── Helpers ───────────────────────────────────────────────────────────────────

def _scroll_bonus(
    cid: str,
    scroll1_ids: Set[str],
    scroll2_ids: Set[str],
    scroll3_ids: Set[str],
) -> float:
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


def _dedup_by_citation(
    items: List[Tuple[str, Dict]],
    max_citations: int = 5,
) -> List[Tuple[str, Dict]]:
    """
    Deduplicate judgment chunks by citation, keeping one chunk per unique
    citation — the chunk with the longest case_note (most informative).

    Why one per citation:
      A single judgment is split into multiple chunks (chunk_index 1, 2, 3...).
      The case_note in ext is a complete summary of the whole judgment.
      Sending 10 chunks from the same citation wastes 9 prompt slots with
      duplicate context. One chunk with the case_note is sufficient for the LLM
      to understand and cite the judgment correctly.

    Why prefer longest case_note:
      All chunks of the same citation share the same case_note in ext.
      But some chunks may have a richer text field. Pick the one with the
      most content (case_note length as proxy).

    max_citations: cap on unique citations returned — prevents a broad name
      search from flooding the pinned slots.
    """
    # Group by citation
    by_citation: Dict[str, Tuple[str, Dict]] = {}
    no_citation: List[Tuple[str, Dict]] = []

    for chunk_id, payload in items:
        ext = payload.get("ext") or {}
        citation = str(ext.get("citation") or "").strip()
        if not citation:
            no_citation.append((chunk_id, payload))
            continue
        existing = by_citation.get(citation)
        if existing is None:
            by_citation[citation] = (chunk_id, payload)
        else:
            # Keep the chunk with the longer case_note
            existing_note = str((existing[1].get("ext") or {}).get("case_note") or "")
            new_note      = str(ext.get("case_note") or "")
            if len(new_note) > len(existing_note):
                by_citation[citation] = (chunk_id, payload)

    # Combine: citation-deduped first, then no-citation (no cap on those)
    result = list(by_citation.values())[:max_citations]
    result += no_citation
    return result


def _bare_number(ref: str) -> Optional[str]:
    m = re.search(r"(\d+[A-Za-z]{0,2})", str(ref))
    return m.group(1) if m else None


def _normalise_notif_num(ref: str) -> Optional[str]:
    m = re.search(r"(\d+\s*/\s*\d+)", ref)
    return re.sub(r"\s*", "", m.group(1)) if m else None


def _normalise_circ_num(ref: str) -> Optional[str]:
    ref = re.sub(r"circular\s*no\.?\s*[-–]?\s*", "", ref, flags=re.IGNORECASE).strip()
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