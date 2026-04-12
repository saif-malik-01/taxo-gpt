"""
apps/api/src/services/rag/retrieval/responder.py
Stage 6 — Cross-reference enrichment + LLM response generation.
"""

import re
import json as _json
from typing import Any, Dict, List, Optional

from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models as qmodels

import asyncio
from apps.api.src.core.config import settings
from apps.api.src.services.llm.bedrock import AsyncBedrockLLMClient
from apps.api.src.services.rag.models import (
    CitationResult, FinalResponse, IntentResult,
    ScoredChunk, SessionMessage,
)
import logging

logger = logging.getLogger(__name__)

_MAX_CROSS_REFS        = 2    # max cross-ref chunks injected into final answer
_MAX_TOKENS_RESP       = 4096
_TOP_CHUNKS_FOR_ENRICH = 5    # only inspect top-5 chunks for cross-refs


# ── System prompt ────────────────────────────────────────────────────────────

def _build_system_prompt(hierarchy: List[str], insufficient: bool = False, intent_str: str = "GENERAL") -> str:
    """
    Builds a free-form system prompt.
    The hierarchy is used only to hint which types of material are in the
    context — the LLM is NOT forced into a fixed section structure.
    It answers the user query directly using whatever is in the retrieved context.
    """
    if intent_str == "CHIT_CHAT":
        return """You are Taxobuddy, an expert AI Indian GST & Tax Assistant. The user is engaging in conversational small talk.
        
RULES FOR YOUR RESPONSE:
1. PERSONALIZE: If the context includes a USER PROFILE with their name, use it naturally (e.g., "Hello Saif!"). If they ask "What is my name?", answer using the profile.
2. TAILOR TO EXPECTATION: 
   - If they say "Thank you" or give praise, express polite gratitude.
   - If they are frustrated, confused, or use insults, apologize professionally and ask how you can provide better clarification.
   - If they ask who you are, introduce yourself as Taxobuddy, an AI specialized in Indian Tax Law.
3. STEER BACK: Always keep your response brief and conclude by asking how you can assist them with their GST or Tax queries today.
4. SAFEGUARD: Do not provide complex tax advice or hallucinate sections in this conversational mode."""
        
    if intent_str == "OUT_OF_SCOPE":
        return """You are Taxobuddy, an expert AI Indian GST & Tax Assistant. The user has asked a question completely outside the scope of Indian Tax (e.g., coding, medical, history, politics, jokes) or provided a malicious prompt.

RULES FOR YOUR RESPONSE:
1. FIRM BUT POLITE DECLINE: Refuse to answer the prompt directly. 
2. EXPLAIN BOUNDARIES: State clearly that you are exclusively programmed for and specialized in Indian GST, Customs, and Tax laws.
3. ABSOLUTE NEUTRALITY: Never express opinions on political, religious, or controversial topics, even as a joke.
4. REDIRECT: Professionaly invite them to ask any relevant tax-related questions. Sound like a helpful legal consultant setting polite boundaries."""
    # Map hierarchy keys to plain-language hints about what context is available
    _CONTENT_HINTS = {
        "act":                       "statutory provisions",
        "rules":                     "rules and sub-rules",
        "notification_circular_faq": "CBIC notifications, circulars, and FAQs",
        "case_scenario_illustration": "case scenarios and illustrations",
        "judgment":                  "court judgments and tribunal orders",
        "analytical_review":         "analytical commentary",
        "rate":                      "GST rate information, HSN/SAC codes",
        "summary":                   "summaries",
    }
    available = [_CONTENT_HINTS[k] for k in hierarchy if k in _CONTENT_HINTS]
    context_hint = (
        f"The retrieved context includes: {', '.join(available)}."
        if available else ""
    )

    base = (
        "You are a senior Indian tax law professional providing expert legal guidance.\n\n"
        + context_hint
        + ("\n\n" if context_hint else "")
        + """Answer the user's query in detail using the retrieved context provided below.

RULES:
1. Answer directly and completely. Do not impose section headings or a fixed structure unless the query itself asks for a breakdown.
2. Use the retrieved context as your source. Quote exact statutory text or judicial language where the wording is legally material.
3. Cite every legal statement — section number, rule number, notification number and date, or case name and citation as applicable.
4. Do NOT say things like "the context does not address this" or "no notification was found" or "this is not in the provided material". Simply use what is available and answer as fully as possible.
5. If multiple judgments are provided, discuss each one that is relevant to the query. Do not skip judgments.
6. Be thorough. Do not truncate or summarise prematurely. The user expects a detailed professional answer.
7. Maintain a professional, respectful tone throughout. Never be dismissive."""
    )

    if insufficient:
        base += (
            "\n\nNOTE: The retrieved context may not fully address this query. "
            "Answer as completely as possible from what is available. "
            "Clearly state which specific aspects are not covered."
        )
    return base


# ── Cross-reference enrichment ────────────────────────────────────────────────

class CrossRefEnricher:
    """
    Fetches up to _MAX_CROSS_REFS supporting chunks from cross-references
    embedded in the top retrieved chunks.

    All Qdrant scroll calls are submitted to the shared global rag_executor
    and collected with as_completed(), so total wait = slowest single call.
    Capped at _TOP_CHUNKS_FOR_ENRICH source chunks to keep latency bounded.
    """

    def __init__(self, qdrant: AsyncQdrantClient):
        self._qdrant = qdrant
        self._col    = settings.QDRANT_COLLECTION

    async def enrich(self, top_chunks: List[ScoredChunk]) -> List[Dict[str, Any]]:
        lookups: List[tuple] = []
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

        results: Dict[str, Dict] = {}
        # Launch all scrolls in parallel using asyncio.gather
        coros = [self._scroll(key, value) for key, value, _ in lookups]
        fetched_results = await asyncio.gather(*coros, return_exceptions=True)

        for i, chunks_found in enumerate(fetched_results):
            if isinstance(chunks_found, Exception):
                logger.debug(f"Cross-ref fetch failed [{lookups[i][2]}]: {chunks_found}")
                continue

            cache_key = lookups[i][2]
            if chunks_found and cache_key not in results:
                results[cache_key] = chunks_found[0]
                if len(results) >= _MAX_CROSS_REFS:
                    break

        result_list = list(results.values())[:_MAX_CROSS_REFS]
        logger.info(f"Cross-ref enrichment: {len(result_list)} extra chunks "
                    f"(from {len(lookups)} parallel lookups)")
        return result_list

    async def _scroll(self, key: str, value: str) -> List[Dict]:
        try:
            results, _ = await self._qdrant.scroll(
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
            chunks = []
            for r in results:
                if r.payload:
                    p = r.payload
                    p["_point_id"] = str(r.id)
                    chunks.append(p)
            return chunks
        except Exception as e:
            logger.debug(f"Cross-ref scroll {key}={value} failed: {e}")
            return []


# ── LLM Responder ─────────────────────────────────────────────────────────────

class LLMResponder:

    def __init__(self, llm: AsyncBedrockLLMClient, qdrant: Optional[AsyncQdrantClient] = None):
        self._llm = llm
        self._qdrant = qdrant

    async def generate(
        self,
        final_query: str,
        session_history: List[SessionMessage],
        top_chunks: List[ScoredChunk],
        cross_ref_chunks: List[Dict[str, Any]],
        citation_result: Optional[CitationResult],
        intent: IntentResult,
        profile_summary: Optional[str] = None,
    ) -> FinalResponse:
        """Non-streaming generation — returns complete FinalResponse."""
        insufficient = bool(
            top_chunks and max(c.score for c in top_chunks) < 0.005
        )
        system_prompt = _build_system_prompt(intent.response_hierarchy, insufficient, intent.intent)
        user_message  = self._build_context(
            final_query, session_history, top_chunks,
            cross_ref_chunks, citation_result, profile_summary
        )
        logger.info(
            f"Stage 6: generating response "
            f"(intent={intent.intent} conf={intent.confidence} "
            f"chunks={len(top_chunks)} cross_refs={len(cross_ref_chunks)} "
            f"insufficient={insufficient})"
        )
        answer = await self._llm.call(
            system_prompt=system_prompt,
            user_message=user_message,
            max_tokens=_MAX_TOKENS_RESP,
            temperature=0.1,
            label="stage6",
        )
        if not answer:
            answer = "Unable to generate a response at this time. Please try again."
        retrieved_docs = await _build_docs_list(top_chunks, cross_ref_chunks, citation_result, self._qdrant)
        return FinalResponse(
            answer=answer,
            retrieved_documents=retrieved_docs,
            intent=intent.intent,
            confidence=intent.confidence,
        )

    async def generate_stream(
        self,
        final_query: str,
        session_history: List[SessionMessage],
        top_chunks: List[ScoredChunk],
        cross_ref_chunks: List[Dict[str, Any]],
        citation_result: Optional[CitationResult],
        intent: IntentResult,
        profile_summary: Optional[str] = None,
    ):
        """
        Streaming generation — yields text chunks directly from Bedrock.
        No thread bridge. No queue. No blocking. Pure async generator.
        """
        insufficient = bool(
            top_chunks and max(c.score for c in top_chunks) < 0.005
        )
        system_prompt = _build_system_prompt(intent.response_hierarchy, insufficient, intent.intent)
        user_message  = self._build_context(
            final_query, session_history, top_chunks,
            cross_ref_chunks, citation_result, profile_summary
        )
        logger.info(
            f"Stage 6 stream: generating "
            f"(intent={intent.intent} conf={intent.confidence} "
            f"chunks={len(top_chunks)} cross_refs={len(cross_ref_chunks)})"
        )
        retrieved_docs = await _build_docs_list(top_chunks, cross_ref_chunks, citation_result, self._qdrant)

        usage_data: dict = {}
        async for chunk in self._llm.call_stream(
            system_prompt=system_prompt,
            user_message=user_message,
            max_tokens=_MAX_TOKENS_RESP,
            temperature=0.1,
            label="stage6_stream",
        ):
            if isinstance(chunk, str) and chunk.startswith("\n\n__USAGE__"):
                try:
                    usage_data = _json.loads(chunk[len("\n\n__USAGE__"):])
                except Exception:
                    pass
            else:
                yield chunk

        meta = {
            "intent": intent.intent,
            "confidence": intent.confidence,
            "retrieved_documents": retrieved_docs,
            "usage": usage_data,
        }
        yield "\n\n__META__" + _json.dumps(meta)

    def _build_context(
        self,
        final_query: str,
        history: List[SessionMessage],
        top_chunks: List[ScoredChunk],
        cross_refs: List[Dict],
        citation_result: Optional[CitationResult],
        profile_summary: Optional[str] = None,
    ) -> str:
        parts = []

        if profile_summary:
            parts.append("=== USER PROFILE ===")
            parts.append(profile_summary)
            parts.append("")

        if history:
            parts.append("=== CONVERSATION HISTORY (for context only — use ONLY if the current query explicitly references a prior exchange; do not let history bias or anchor the current answer) ===")
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

        # Cross-refs not sent to LLM — they appear in sources list only (frontend)
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

    # Source sent for all chunk types except judgment
    # (judgment identity comes from Case/Citation/Petitioner/Respondent fields)
    if chunk.get("parent_doc") and chunk_type not in ("judgment", "circular", "gst_form", "gstat_form"):
        lines.append(f"Source: {chunk['parent_doc']}")

    if chunk_type == "judgment":
        # Always send identifying metadata
        for f, k in [("Case", "case_name"), ("Citation", "citation"),
                     ("Court", "court"), ("Decision", "decision")]:
            v = ext.get(k)
            if v:
                lines.append(f"{f}: {v.replace('_', ' ') if k == 'decision' else v}")

        if pinned:
            case_note = str(ext.get("case_note") or "").strip()
            if case_note:
                lines.append(f"Case Note:\n{case_note}")
            text = str(chunk.get("text") or "").strip()
            if text:
                lines.append(f"Text:\n{text}")
            for lbl, key in [
                ("Sections in Dispute", "sections_in_dispute"),
                ("Act",                 "act"),
                ("Petitioner",          "petitioner"),
                ("Respondent",          "respondent"),
                ("Bench",               "bench"),
                ("Judgment Date",       "judgment_date"),
            ]:
                v = ext.get(key)
                if v:
                    lines.append(f"{lbl}: {v}")
        else:
            for f2, k2 in [("Citation",   "citation"),
                           ("Decision",   "decision"),
                           ("Petitioner", "petitioner"),
                           ("Respondent", "respondent"),
                           ("Year",       "year")]:
                v2 = ext.get(k2)
                if v2:
                    lines.append(f"{f2}: {v2.replace('_', ' ') if k2 == 'decision' else v2}")
            text = str(chunk.get("text") or "").strip()
            if text:
                lines.append(f"Text:\n{text}")

    elif chunk_type == "notification":
        # Source: already carries full identity from parent_doc
        # e.g. "Notification No.26/2017 – Central Tax"
        for label2, key2 in [
            ("Issued By", "issued_by"),
            ("Period",    "applicable_period"),
        ]:
            v2 = ext.get(key2)
            if v2:
                lines.append(f"{label2}: {v2}")
        headers = ext.get("table_headers")
        if headers:
            if isinstance(headers, list):
                lines.append("Table Headers: " + " | ".join(str(h) for h in headers if h))
            else:
                lines.append(f"Table Headers: {headers}")
        row_data = ext.get("row_data")
        if row_data:
            if isinstance(row_data, dict):
                # dict: {column_name: cell_value} — format as key: value pairs
                lines.append("Row Data: " + " | ".join(
                    f"{k}: {v}" for k, v in row_data.items() if v is not None and str(v).strip()
                ))
            elif isinstance(row_data, list):
                lines.append("Table Data:\n" + "\n".join(str(r) for r in row_data if r))
        text = str(chunk.get("text") or "").strip()
        if text:
            lines.append(f"Text:\n{text}")

    elif chunk_type == "circular":
        # Circular: no number field — only subject, table_headers, row_data, text
        subj = ext.get("subject", "")
        if subj:
            lines.append(f"Subject: {subj}")
        headers = ext.get("table_headers")
        if headers:
            if isinstance(headers, list):
                lines.append("Table Headers: " + " | ".join(str(h) for h in headers if h))
            else:
                lines.append(f"Table Headers: {headers}")
        row_data = ext.get("row_data")
        if row_data:
            if isinstance(row_data, list):
                lines.append("Table Data:\n" + "\n".join(str(r) for r in row_data if r))
            else:
                lines.append(f"Table Data:\n{row_data}")
        text = str(chunk.get("text") or "").strip()
        if text:
            lines.append(f"Text:\n{text}")

    elif chunk_type in ("cgst_rule", "igst_rule", "gstat_rule"):
        lines.append(
            f"Rule: {ext.get('rule_number_full', '')} — "
            f"{ext.get('rule_title', '')}"
        )
        text = str(chunk.get("text") or "").strip()
        if text:
            lines.append(f"Text:\n{text}")

    elif chunk_type in ("cgst_section", "igst_section"):
        lines.append(
            f"Section {ext.get('section_number', '')} — "
            f"{ext.get('section_title', '')} "
            f"({ext.get('act', '')})"
        )
        text = str(chunk.get("text") or "").strip()
        if text:
            lines.append(f"Text:\n{text}")

    elif chunk_type == "hsn_code":
        lines.append(f"HSN Code: {ext.get('hsn_code', '')}")
        chapter = ext.get("chapter_title", "")
        sub     = ext.get("sub_chapter_title", "")
        if chapter:
            lines.append(f"Chapter: {chapter}")
        if sub:
            lines.append(f"Sub-Chapter: {sub}")
        text = str(chunk.get("text") or "").strip()
        if text:
            lines.append(f"Text:\n{text}")

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
            lines.append(f"Text:\n{text}")

    elif chunk_type in ("gst_form", "gstat_form"):
        form_name = ext.get("form_name", "")
        if form_name:
            lines.append(f"Form: {form_name}")
        text = str(chunk.get("text") or "").strip()
        if text:
            lines.append(f"Text:\n{text}")

    else:
        text = str(chunk.get("text") or "").strip()
        if text:
            lines.append(f"Text:\n{text}")

    # summary is intentionally NOT sent to the LLM — it duplicates the text /
    # case_note and adds tokens without adding information the LLM needs.

    return "\n".join(lines)


async def _build_docs_list(
    top_chunks: List[ScoredChunk],
    cross_refs: List[Dict],
    citation_result: Optional[CitationResult],
    qdrant: Optional[AsyncQdrantClient] = None
) -> List[Dict]:
    """
    Build sources list for frontend. Deduplicated by identifier (citation).
    One entry per unique citation.
    """
    docs = []
    seen_identifiers: set = set()
    judgments_to_fetch = {}

    def _add(payload: Dict, label: str, score: float, chunk_id: str = None) -> None:
        ext = payload.get("ext") or {}
        identifier = (
            ext.get("citation") or
            ext.get("section_number") or
            ext.get("rule_number_full") or
            ext.get("notification_number") or
            ext.get("circular_number") or
            ext.get("form_name") or
            ext.get("hsn_code") or
            ext.get("sac_code") or ""
        )
        if not identifier or identifier in seen_identifiers:
            return
            
        seen_identifiers.add(identifier)
        summary_dict = _doc_summary(payload, label, score)
        # Store the original Qdrant point ID
        resolved_id = chunk_id or payload.get("_point_id") or payload.get("chunk_id") or payload.get("id")
        if resolved_id:
            summary_dict["chunk_id"] = str(resolved_id)
        
        if payload.get("chunk_type") == "judgment":
            judgments_to_fetch[identifier] = payload
            
        docs.append(summary_dict)

    if citation_result and citation_result.found:
        for chunk in citation_result.chunks:
            # For pinned chunks, try to find ID in payload
            _add(chunk, "pinned", 1.0)
    for sc in top_chunks:
        _add(sc.payload, "retrieved", sc.score, chunk_id=sc.chunk_id)
    for chunk in cross_refs:
        _add(chunk, "cross_reference", 0.0)
        
    if qdrant and judgments_to_fetch:
        tasks = [
            _fetch_full_judgment(qdrant, payload)
            for ident, payload in judgments_to_fetch.items()
        ]
        fetched_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, (ident, _) in enumerate(judgments_to_fetch.items()):
            full_data = fetched_results[i]
            if isinstance(full_data, Exception):
                logger.error(f"Error fetching full judgment for {ident}: {full_data}")
                continue
                
            if full_data:
                for d in docs:
                    if d.get("identifier") == ident:
                        d["full_judgment"] = full_data
                        break

    return docs

async def _fetch_full_judgment(qdrant: AsyncQdrantClient, payload: Dict) -> Dict:
    file_hash = payload.get("_file_hash")
    citation = payload.get("ext", {}).get("citation")
    
    if not file_hash and not citation:
        return {}

    must_cond = []
    if citation:
        must_cond.append(qmodels.FieldCondition(
            key="ext.citation", match=qmodels.MatchValue(value=citation)
        ))
        
    try:
        results, _ = await qdrant.scroll(
            collection_name=settings.QDRANT_COLLECTION,
            scroll_filter=qmodels.Filter(must=must_cond),
            limit=1000,
            with_payload=True,
            with_vectors=False,
        )
        
        chunks = [r.payload for r in results if r.payload]
        chunks.sort(key=lambda x: x.get("chunk_index", 0))
        full_text = "\n\n".join([str(c.get("text") or "") for c in chunks])
        
        base_meta = chunks[0].get("ext", {}) if chunks else payload.get("ext", {})
        
        return _map_judgment_metadata(base_meta, full_text.strip())
    except Exception as e:
        logger.error(f"Error fetching full judgment: {e}")
        return {}

def _map_judgment_metadata(ext: Dict, full_text: str) -> Dict:
    """
    Maps raw judgment metadata to the client-requested structure.
    """
    # Prefer case_name as title, fallback to Petitioner vs Respondent
    title = ext.get("case_name") or f"{ext.get('petitioner', '')} vs {ext.get('respondent', '')}"
    
    return {
        "citation": ext.get("citation", ""),
        "title": title,
        "case_number": ext.get("case_number", ""),
        "court": ext.get("court", ""),
        "state": ext.get("state", ""),
        "year": str(ext.get("year", "")),
        "judge": ext.get("bench", ""),
        "petitioner": ext.get("petitioner", ""),
        "respondent": ext.get("respondent", ""),
        "decision": ext.get("decision", ""),
        "current_status": ext.get("legal_status", "Closed"),
        "law": ext.get("law", "GST"),
        "act_name": ext.get("act", ""),
        "section_number": ext.get("section_number") or ext.get("sections_in_dispute", ""),
        "rule_name": ext.get("rule_name", ""),
        "rule_number": ext.get("rule_number", ""),
        "notification_number": ext.get("notification_number", ""),
        "case_note": ext.get("case_note", ""),
        "full_text": full_text
    }

def _doc_summary(payload: Dict, label: str, score: float) -> Dict:
    ext = payload.get("ext") or {}
    return {
        "chunk_id":   payload.get("_point_id") or payload.get("chunk_id") or payload.get("id"), # Try to get ID
        "chunk_type": payload.get("chunk_type", ""),
        "text":       payload.get("text", ""),
        "identifier": (
            ext.get("citation") or ext.get("section_number") or
            ext.get("rule_number_full") or ext.get("notification_number") or
            ext.get("circular_number") or ext.get("form_name") or
            ext.get("hsn_code") or ext.get("sac_code") or ""
        ),
        "summary":    str(payload.get("summary") or "")[:200],
    }


def _bare_number(ref: str) -> Optional[str]:
    m = re.search(r"(\d+[A-Za-z]{0,2})", str(ref))
    return m.group(1) if m else None
