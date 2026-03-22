"""
retrieval/responder.py
Stage 6 — Cross-reference enrichment + LLM response generation.

System prompt built dynamically from Stage 2C response_hierarchy.
Sends top 25 chunks to LLM.

CrossRefEnricher change: all scroll lookups now run in parallel via
ThreadPoolExecutor so they don't block the streaming path sequentially.
MAX_CROSS_REFS reduced 3 → 2 to further reduce pre-stream latency.
"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

from config import CONFIG
from retrieval.bedrock_llm import BedrockLLMClient
from retrieval.models import (
    CitationResult, FinalResponse, IntentResult,
    ScoredChunk, SessionMessage, Stage2BResult,
)
from utils.logger import get_logger

logger = get_logger("responder")

_MAX_CROSS_REFS  = 2          # reduced from 3 — fewer blocking Qdrant calls
_MAX_TOKENS_RESP = 4096
_ENRICH_WORKERS  = 6          # parallel scroll calls inside enrich()
_TOP_CHUNKS_FOR_ENRICH = 5    # only inspect top-5 chunks for cross-refs


# ── Hierarchy label map ───────────────────────────────────────────────────────

_HIERARCHY_LABELS = {
    "act": (
        "STATUTORY PROVISION\n"
        "State precisely what the Act provides on this matter. "
        "Quote the exact language of the provision where relevant. "
        "Cite the section number and Act name for every statement."
    ),
    "rules": (
        "RULES\n"
        "State what the applicable rules prescribe. "
        "Quote rule text or sub-rules directly where they are material. "
        "Cite the rule number for every statement."
    ),
    "notification_circular_faq": (
        "NOTIFICATIONS / CIRCULARS / FAQs\n"
        "State what CBIC or CBDT has clarified, prescribed, or amended. "
        "Quote the operative portion of the notification or circular where relevant. "
        "Cite the notification/circular number and date for every statement."
    ),
    "case_scenario_illustration": (
        "CASE SCENARIOS / ILLUSTRATIONS\n"
        "Present practical examples or illustrations from the context. "
        "Describe each scenario clearly and state the applicable treatment."
    ),
    "judgment": (
        "JUDICIAL PRECEDENTS\n"
        "For each relevant judgment present:\n"
        "  Case name | Court | Citation\n"
        "  Facts: the material facts relevant to the issue\n"
        "  Issue: the precise legal question the court decided\n"
        "  Held: the court's decision — quote the ratio decidendi directly if available\n"
        "  Relevance: how this ruling applies to the query"
    ),
    "analytical_review": (
        "ANALYTICAL PERSPECTIVE\n"
        "Present the broader legal position, unresolved areas, conflicting views, "
        "or practical implications. Where the law is settled, state it clearly. "
        "Where it is contested, present both sides without personal opinion."
    ),
    "summary": (
        "SUMMARY\n"
        "Provide a concise, plain-language conclusion of the legal position. "
        "This must synthesise all the above into a clear, actionable answer. "
        "It must be more useful and clearer than any analytical review above."
    ),
    "rate": (
        "GST RATE\n"
        "State the applicable rate, the HSN/SAC code, the notification prescribing "
        "the rate, any conditions or exemptions, and amendments in reverse "
        "chronological order (most recent first)."
    ),
}

_BASE_RULES = """
RULES — follow these strictly:

1. STRUCTURE: Present sections in the order given above.
   If no relevant context exists for a section, skip it silently — do not mention its absence.
   Every section that IS presented must be substantive and directly answer the query.

2. CITATIONS: Every legal statement must cite its source.
   Acts: cite section number and Act name.
   Rules: cite rule number.
   Notifications/Circulars: cite number and date.
   Judgments: cite case name, court, and citation.

3. LANGUAGE: Use precise legal and professional language throughout.
   Quote the exact text of provisions, notifications, or judicial holdings
   where the precise wording is legally material.
   Do not paraphrase where quoting adds clarity.

4. QUALITY BENCHMARK: Your answer must be more detailed, accurate, and useful
   than any analytical review present in the context.
   The analytical review is the floor — not the ceiling.

5. COMPLETENESS: Prepare a detailed and complete response.
   Do not truncate or summarise prematurely.
   The user expects a thorough professional answer.

6. BOUNDARIES: Never fabricate, infer, or speculate beyond the provided context.
   If the context does not address a specific aspect of the query, state clearly:
   "The available material does not address [specific aspect]."

7. TONE: Professional, direct, and clear.
   Do not hedge unnecessarily. State the law as it is.
"""


def _build_system_prompt(hierarchy: List[str], insufficient: bool = False) -> str:
    numbered = []
    for i, key in enumerate(hierarchy, 1):
        label = _HIERARCHY_LABELS.get(key)
        if label:
            numbered.append(f"{i}. {label}")

    structure = "\n\n".join(numbered)
    base = (
        "You are a senior Indian tax law professional providing expert legal guidance.\n\n"
        "Structure your response in the following order. "
        "Include a section only if the provided context contains relevant material for it. "
        "SUMMARY is always the final section.\n\n"
        + structure
        + "\n\n"
        + _BASE_RULES
    )
    if insufficient:
        base += (
            "\n\nNOTE: The retrieved context may not fully address this query. "
            "Answer as completely as possible from the available material. "
            "State clearly which aspects are not covered and suggest the user "
            "provide more detail or rephrase."
        )
    return base


# ── Cross-reference enrichment ────────────────────────────────────────────────

class CrossRefEnricher:
    """
    Fetches up to _MAX_CROSS_REFS supporting chunks from cross-references
    embedded in the top retrieved chunks.

    All Qdrant scroll calls are executed in parallel (ThreadPoolExecutor)
    so the total wait time equals the slowest single call, not the sum.
    Capped at _TOP_CHUNKS_FOR_ENRICH source chunks and _ENRICH_WORKERS
    parallel workers to keep latency bounded.
    """

    def __init__(self, qdrant: QdrantClient):
        self._qdrant = qdrant
        self._col    = CONFIG.qdrant.collection_name

    def enrich(self, top_chunks: List[ScoredChunk]) -> List[Dict[str, Any]]:
        # Step 1: collect all (qdrant_key, qdrant_value, cache_key) lookup tuples
        # from the top _TOP_CHUNKS_FOR_ENRICH chunks.  Deduplicate so we never
        # issue the same Qdrant scroll twice.
        lookups: List[tuple] = []        # (field_key, field_value, cache_key)
        seen_cache_keys = set()

        for chunk in top_chunks[:_TOP_CHUNKS_FOR_ENRICH]:
            xrefs = chunk.payload.get("cross_references") or {}

            for sec in (xrefs.get("sections") or []):
                num = _bare_number(sec)
                if num:
                    ck = f"sec_{num}"
                    if ck not in seen_cache_keys:
                        lookups.append(("ext.section_number", num, ck))
                        seen_cache_keys.add(ck)

            for rule in (xrefs.get("rules") or []):
                num = _bare_number(rule)
                if num:
                    ck = f"rule_{num}"
                    if ck not in seen_cache_keys:
                        lookups.append(("ext.rule_number_full", f"Rule {num}", ck))
                        seen_cache_keys.add(ck)

            for notif in (xrefs.get("notifications") or []):
                m = re.search(r"(\d+\s*/\s*\d+)", notif)
                if m:
                    num = re.sub(r"\s*", "", m.group(1))
                    ck  = f"notif_{num}"
                    if ck not in seen_cache_keys:
                        lookups.append(("ext.notification_number", num, ck))
                        seen_cache_keys.add(ck)

            for circ in (xrefs.get("circulars") or []):
                m = re.search(r"(\d+[/\-]\d+(?:[/\-]\d+)?)", circ)
                if m:
                    num = m.group(1)
                    ck  = f"circ_{num}"
                    if ck not in seen_cache_keys:
                        lookups.append(("ext.circular_number", num, ck))
                        seen_cache_keys.add(ck)

        if not lookups:
            return []

        # Step 2: run all scroll calls in parallel — time = slowest single call
        results: Dict[str, Dict] = {}
        with ThreadPoolExecutor(max_workers=min(_ENRICH_WORKERS, len(lookups))) as ex:
            future_map = {
                ex.submit(self._scroll, key, value): cache_key
                for key, value, cache_key in lookups
            }
            for future in as_completed(future_map):
                cache_key = future_map[future]
                try:
                    chunks_found = future.result()
                except Exception as e:
                    logger.debug(f"Cross-ref fetch failed [{cache_key}]: {e}")
                    continue

                if chunks_found and cache_key not in results:
                    results[cache_key] = chunks_found[0]
                    if len(results) >= _MAX_CROSS_REFS:
                        # Cancel remaining futures eagerly — we have enough
                        for f in future_map:
                            f.cancel()
                        break

        result_list = list(results.values())[:_MAX_CROSS_REFS]
        logger.info(f"Cross-ref enrichment: {len(result_list)} extra chunks "
                    f"(from {len(lookups)} parallel lookups)")
        return result_list

    def _scroll(self, key: str, value: str) -> List[Dict]:
        try:
            results, _ = self._qdrant.scroll(
                collection_name=self._col,
                scroll_filter=qmodels.Filter(must=[
                    qmodels.FieldCondition(
                        key=key, match=qmodels.MatchValue(value=value)
                    )
                ]),
                limit=2,
                with_payload=True,
                with_vectors=False,
            )
            res = []
            for r in results:
                if r.payload:
                    r.payload["id"] = r.id
                    res.append(r.payload)
            return res
        except Exception as e:
            logger.debug(f"Cross-ref scroll {key}={value} failed: {e}")
            return []


# ── LLM Responder ─────────────────────────────────────────────────────────────

class LLMResponder:

    def __init__(self, llm: BedrockLLMClient):
        self._llm = llm

    def generate(
        self,
        final_query: str,
        session_history: List[SessionMessage],
        top_chunks: List[ScoredChunk],
        cross_ref_chunks: List[Dict[str, Any]],
        citation_result: Optional[CitationResult],
        intent: IntentResult,
    ) -> FinalResponse:
        """Non-streaming generation — returns complete FinalResponse."""
        insufficient = bool(
            top_chunks and max(c.score for c in top_chunks) < 0.005
        )
        system_prompt = _build_system_prompt(intent.response_hierarchy, insufficient)
        user_message  = self._build_context(
            final_query, session_history, top_chunks,
            cross_ref_chunks, citation_result
        )
        logger.info(
            f"Stage 6: generating response "
            f"(intent={intent.intent} conf={intent.confidence} "
            f"chunks={len(top_chunks)} cross_refs={len(cross_ref_chunks)} "
            f"insufficient={insufficient})"
        )
        answer = self._llm.call(
            system_prompt=system_prompt,
            user_message=user_message,
            max_tokens=_MAX_TOKENS_RESP,
            temperature=0.1,
            label="stage6",
        )
        if not answer:
            answer = "Unable to generate a response at this time. Please try again."
        retrieved_docs = _build_docs_list(top_chunks, cross_ref_chunks, citation_result)
        return FinalResponse(
            answer=answer,
            retrieved_documents=retrieved_docs,
            intent=intent.intent,
            confidence=intent.confidence,
        )

    def generate_stream(
        self,
        final_query: str,
        session_history: List[SessionMessage],
        top_chunks: List[ScoredChunk],
        cross_ref_chunks: List[Dict[str, Any]],
        citation_result: Optional[CitationResult],
        intent: IntentResult,
    ):
        """
        Streaming generation — yields text chunks as they arrive from Bedrock.
        cross_ref_chunks are passed in pre-computed (done during stages 1-5 threadpool)
        so this function can start streaming immediately without any Qdrant calls.
        """
        insufficient = bool(
            top_chunks and max(c.score for c in top_chunks) < 0.005
        )
        system_prompt = _build_system_prompt(intent.response_hierarchy, insufficient)
        user_message  = self._build_context(
            final_query, session_history, top_chunks,
            cross_ref_chunks, citation_result
        )
        logger.info(
            f"Stage 6 stream: generating "
            f"(intent={intent.intent} conf={intent.confidence} "
            f"chunks={len(top_chunks)} cross_refs={len(cross_ref_chunks)})"
        )
        retrieved_docs = _build_docs_list(top_chunks, cross_ref_chunks, citation_result)

        for chunk in self._llm.call_stream(
            system_prompt=system_prompt,
            user_message=user_message,
            max_tokens=_MAX_TOKENS_RESP,
            temperature=0.1,
            label="stage6_stream",
        ):
            yield chunk

        import json as _json
        meta = {
            "__meta__": True,
            "intent":      intent.intent,
            "confidence":  intent.confidence,
            "retrieved_documents": retrieved_docs,
        }
        yield "\n\n__META__" + _json.dumps(meta)

    def _build_context(
        self,
        final_query: str,
        history: List[SessionMessage],
        top_chunks: List[ScoredChunk],
        cross_refs: List[Dict],
        citation_result: Optional[CitationResult],
    ) -> str:
        parts = []

        if history:
            parts.append("=== CONVERSATION HISTORY (last 3 turns) ===")
            for msg in history[-3:]:
                parts.append(f"User: {msg.user_query}")
                parts.append(
                    f"Assistant: {msg.llm_response[:400]}"
                    f"{'...' if len(msg.llm_response) > 400 else ''}"
                )
            parts.append("")

        if citation_result and citation_result.found:
            parts.append(
                f"=== PINNED JUDGMENT [{citation_result.citation}] ==="
            )
            for chunk in citation_result.chunks:
                parts.append(_format_chunk(chunk, "[PINNED]", pinned=True))
            parts.append("")

        if top_chunks:
            parts.append("=== PRIMARY RETRIEVED CONTEXT ===")
            for i, sc in enumerate(top_chunks, 1):
                label = f"[{i}] {sc.payload.get('chunk_type', '?').upper()}"
                parts.append(_format_chunk(sc.payload, label, pinned=sc.pinned))
            parts.append("")

        if cross_refs:
            parts.append("=== SUPPORTING REFERENCES ===")
            for i, chunk in enumerate(cross_refs, 1):
                label = f"[REF-{i}] {chunk.get('chunk_type', '?').upper()}"
                parts.append(_format_chunk(chunk, label))
            parts.append("")

        parts.append("=== USER QUERY ===")
        parts.append(final_query)
        return "\n".join(parts)


# ── Chunk formatting ──────────────────────────────────────────────────────────

def _format_chunk(chunk: Dict, label: str = "", pinned: bool = False) -> str:
    """
    Format a single chunk payload for the LLM prompt.

    pinned=True  : chunk was retrieved by citation / party name / case number.
                   For judgments the full case_note is sent — this is the exact
                   case the user asked for and completeness matters.
    pinned=False : chunk was retrieved by vector/BM25/scroll.
                   For judgments case_note is capped at 800 chars to keep the
                   total prompt manageable across 25 chunks (~5k tokens vs ~18k).

    Rules:
    - summary is NEVER sent to the LLM for any chunk type.
    - sac_code chunks include ext.services when present.
    - No truncation on any chunk type other than non-pinned judgments.
    """
    lines = [f"--- {label} ---"] if label else []
    ext        = chunk.get("ext") or {}
    chunk_type = chunk.get("chunk_type", "")

    if chunk.get("parent_doc"):
        lines.append(f"Source: {chunk['parent_doc']}")

    if chunk_type == "judgment":
        for f, k in [("Case", "case_name"), ("Citation", "citation"),
                     ("Court", "court"), ("Decision", "decision")]:
            v = ext.get(k)
            if v:
                lines.append(f"{f}: {v.replace('_', ' ') if k == 'decision' else v}")
        case_note = str(ext.get("case_note") or "").strip()
        if case_note:
            # Pinned = exact case retrieved by citation/name/case-number search
            #          → send full case_note (user asked for this specific case)
            # Not pinned = one of up to 25 retrieved judgments
            #          → cap at 800 chars to control prompt size
            lines.append(f"Case Note:{case_note if pinned else case_note[:800]}")
        else:
            text = str(chunk.get("text") or "").strip()
            if text:
                lines.append(f"Text:{text if pinned else text[:800]}")

    elif chunk_type in ("notification", "circular"):
        num  = ext.get("notification_number") or ext.get("circular_number", "")
        date = ext.get("circular_date") or ext.get("year", "")
        subj = ext.get("subject", "")
        if num:
            lines.append(f"Number: {num} ({date})")
        if subj:
            lines.append(f"Subject: {subj}")
        text = str(chunk.get("text") or "").strip()
        if text:
            lines.append(f"Text:{text}")

    elif chunk_type in ("cgst_rule", "igst_rule", "gstat_rule"):
        lines.append(
            f"Rule: {ext.get('rule_number_full', '')} — "
            f"{ext.get('rule_title', '')}"
        )
        text = str(chunk.get("text") or "").strip()
        if text:
            lines.append(f"Text:{text}")

    elif chunk_type in ("cgst_section", "igst_section"):
        lines.append(
            f"Section {ext.get('section_number', '')} — "
            f"{ext.get('section_title', '')} "
            f"({ext.get('act', '')})"
        )
        text = str(chunk.get("text") or "").strip()
        if text:
            lines.append(f"Text:{text}")

    elif chunk_type == "hsn_code":
        lines.append(f"HSN Code: {ext.get('hsn_code', '')}")
        text = str(chunk.get("text") or "").strip()
        if text:
            lines.append(f"Text:{text}")

    elif chunk_type == "sac_code":
        lines.append(f"SAC Code: {ext.get('sac_code', '')}")
        # Include service descriptions — the LLM needs these to determine
        # the correct GST rate and classification for the service.
        services = ext.get("services")
        if services:
            if isinstance(services, list):
                lines.append("Services:" + "".join(f"  - {s}" for s in services if s))
            else:
                lines.append(f"Services:{services}")
        text = str(chunk.get("text") or "").strip()
        if text:
            lines.append(f"Text:{text}")

    else:
        text = str(chunk.get("text") or "").strip()
        if text:
            lines.append(f"Content:{text}")

    # summary is intentionally NOT sent to the LLM — it duplicates the text /
    # case_note and adds tokens without adding information the LLM needs.

    return "".join(lines)
def _build_docs_list(
    top_chunks: List[ScoredChunk],
    cross_refs: List[Dict],
    citation_result: Optional[CitationResult],
) -> List[Dict]:
    docs = []
    if citation_result and citation_result.found:
        for chunk in citation_result.chunks:
            docs.append(_doc_summary(chunk, "pinned", 1.0, chunk.get("id")))
    for sc in top_chunks:
        docs.append(_doc_summary(sc.payload, "retrieved", sc.score, sc.chunk_id))
    for chunk in cross_refs:
        docs.append(_doc_summary(chunk, "cross_reference", 0.0, chunk.get("id")))
    return docs


def _doc_summary(payload: Dict, label: str, score: float, doc_id: Optional[Any] = None) -> Dict:
    ext = payload.get("ext") or {}
    return {
        "id":         str(doc_id) if doc_id is not None else "",
        "label":      label,
        "score":      round(score, 4),
        "chunk_type": payload.get("chunk_type", ""),
        "parent_doc": payload.get("parent_doc", ""),
        "chunk_index": payload.get("chunk_index"),
        "source":     (payload.get("provenance") or {}).get("source_file", ""),
        "identifier": (
            ext.get("citation") or ext.get("section_number") or
            ext.get("rule_number_full") or ext.get("notification_number") or
            ext.get("circular_number") or ext.get("form_name") or
            ext.get("hsn_code") or ext.get("sac_code") or ""
        ),
        "summary": str(payload.get("summary") or "")[:200],
    }


def _bare_number(ref: str) -> Optional[str]:
    m = re.search(r"(\d+[A-Za-z]{0,2})", str(ref))
    return m.group(1) if m else None
