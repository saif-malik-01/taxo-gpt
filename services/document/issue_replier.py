"""
services/document/issue_replier.py

Step 8A — Retrieval for each issue using the SAME pipeline as Feature 1.
  query = issue text only (no summary, no parties, no mode)
  Calls pipeline.query_stages_1_to_5() exactly as the simple chatbot does.

Judgment reranking (applied to top 50 after RRF):
  Read chunk.ext.decision (NOT a top-level field).
  Preferred alignment per mode:
    defensive  → in_favour_of_assessee  : +0.2
    in_favour  → in_favour_of_department: +0.2
  Mismatching judgment chunks: -0.3 (floor 0.0)
  Non-judgment chunks: score unchanged.
  Re-sort → take top 20 (any type, no filter).

Step 8B — LLM draft generation.
  NEVER truncate issue text.
  NEVER truncate retrieved chunks.
  Soft components (prior replied pairs, other issue IDs, reference summaries)
  trimmed by COUNT only — never mid-sentence.

Modes:
  MODE_DEFENSIVE = "defensive"
  MODE_IN_FAVOUR = "in_favour"
"""

import asyncio
import logging
import threading
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

MODE_DEFENSIVE = "defensive"
MODE_IN_FAVOUR = "in_favour"

_MATCH_BOOST    =  0.2
_MISMATCH_PENALTY = -0.3
_SCORE_FLOOR    =  0.0

_TOP_POOL  = 50   # retrieve this many before reranking
_TOP_FINAL = 20   # send this many to LLM

# ── Pipeline singleton ────────────────────────────────────────────────────────

_pipeline    = None
_pipe_lock   = threading.Lock()


def set_pipeline(p) -> None:
    global _pipeline
    with _pipe_lock:
        _pipeline = p


def _get_pipeline():
    global _pipeline
    if _pipeline is None:
        with _pipe_lock:
            if _pipeline is None:
                from retrieval.pipeline import RetrievalPipeline
                _pipeline = RetrievalPipeline()
                _pipeline.setup()
                logger.info("RetrievalPipeline initialised from issue_replier")
    return _pipeline


# ── LLM singleton ─────────────────────────────────────────────────────────────

_llm      = None
_llm_lock = threading.Lock()


def _get_llm():
    global _llm
    if _llm is None:
        with _llm_lock:
            if _llm is None:
                from retrieval.bedrock_llm import BedrockLLMClient
                _llm = BedrockLLMClient()
    return _llm


# ═════════════════════════════════════════════════════════════════════════════
# STEP 8A — RETRIEVAL
# ═════════════════════════════════════════════════════════════════════════════

def _rerank_for_mode(chunks: list, mode: str) -> list:
    """
    Judgment reranking.
    Only touches chunk_type == 'judgment' chunks with ext.decision present.
    All other chunks pass through with score unchanged.
    """
    preferred = {
        MODE_DEFENSIVE: "in_favour_of_assessee",
        MODE_IN_FAVOUR: "in_favour_of_department",
    }
    if mode not in preferred:
        return chunks

    want = preferred[mode]
    for chunk in chunks:
        payload = getattr(chunk, "payload", None) or {}
        if payload.get("chunk_type") != "judgment":
            continue
        ext      = payload.get("ext") or {}
        decision = ext.get("decision")
        if not decision:
            continue
        if decision == want:
            chunk.score += _MATCH_BOOST
        else:
            chunk.score += _MISMATCH_PENALTY
            chunk.score  = max(chunk.score, _SCORE_FLOOR)

    chunks.sort(key=lambda c: c.score, reverse=True)
    return chunks


def retrieve_for_issue(
    issue_text: str,
    mode: str,
    stage2b_result=None,
) -> list:
    """
    Step 8A: Retrieve legal material for one issue.
    - Query = issue text only (no summary, no parties, no mode)
    - Uses the SAME pipeline.query_stages_1_to_5() as Feature 1
    - Applies judgment reranking on top 50 → returns top 20

    stage2b_result: cached Stage2BResult from Step 2B — passed to pipeline
                    so it skips re-extraction of legal entities.
    """
    from retrieval.models import SessionMessage
    pipeline = _get_pipeline()
    if pipeline is None:
        logger.warning("Retrieval pipeline not available")
        return []

    try:
        # Call the same stages 1-5 as the simple chatbot
        # Empty session_history — issue retrieval is stateless per-issue
        staged = pipeline.query_stages_1_to_5(
            user_query      = issue_text,   # issue text only — nothing else
            session_history = [],
        )
        # staged = (final_query, session_history, chunks, citation_result, intent, cross_refs)
        _, _, chunks, _, _, _ = staged

        if not chunks:
            return []

        # Apply judgment reranking on full pool (up to _TOP_POOL)
        pool    = chunks[:_TOP_POOL]
        ranked  = _rerank_for_mode(pool, mode)
        top_20  = ranked[:_TOP_FINAL]

        logger.info(
            f"8A retrieval: issue_len={len(issue_text)} "
            f"pool={len(pool)} → top20={len(top_20)} "
            f"types={[c.payload.get('chunk_type') for c in top_20[:5]]}"
        )
        return top_20
    except Exception as e:
        logger.error(f"Retrieval error for issue: {e}", exc_info=True)
        return []


# ═════════════════════════════════════════════════════════════════════════════
# STEP 8B — DRAFT GENERATION
# ═════════════════════════════════════════════════════════════════════════════

_SYSTEM_DEFENSIVE = """\
You are a senior Indian tax law expert specialising in GST, Income Tax, and Customs.
Your task: draft a formal legal reply defending the taxpayer against a specific allegation.

REPLY OBJECTIVE — DEFENSIVE:
1. Identify every statutory provision (sub-section, proviso, exception, condition) that extinguishes or reduces the demand.
2. Cite judgments where the assessee succeeded on similar facts. Apply their ratio to the current facts explicitly — do not merely cite; explain WHY the ratio applies.
3. If an adverse judgment is retrieved, distinguish it on facts — explain concretely how the taxpayer's situation differs from the facts in that judgment.
4. Conclude: why the demand is not legally sustainable and should be dropped.

OUTPUT FORMAT:
Write in formal legal language as paragraph-form prose (not bullet points).
Cite sections as: 'Section X of the CGST Act, 2017' or 'Rule X of the CGST Rules, 2017'.
Cite judgments as: '[Case Name] — [Court] — [Year] — [Citation if available]'.
Do not add subject line, salutation, or closing — only the substantive reply paragraphs."""

_SYSTEM_IN_FAVOUR = """\
You are a senior Indian tax law expert specialising in GST, Income Tax, and Customs.
Your task: draft a formal legal reply establishing why the department's demand is correct and sustainable.

REPLY OBJECTIVE — IN FAVOUR OF DEPARTMENT:
1. Identify the statutory provisions and rules that create the liability.
2. Apply the conditions in those provisions to the taxpayer's specific facts to show all conditions for the demand are satisfied.
3. Cite judgments where the department succeeded on similar facts. Apply their ratio.
4. Address and pre-empt defences the taxpayer might raise — use statutory text or contrary judgments.
5. Conclude: why the demand is legally correct and must be upheld.

OUTPUT FORMAT:
Write in formal legal language as paragraph-form prose (not bullet points).
Cite sections as: 'Section X of the CGST Act, 2017' or 'Rule X of the CGST Rules, 2017'.
Cite judgments as: '[Case Name] — [Court] — [Year] — [Citation if available]'.
Do not add subject line, salutation, or closing — only the substantive reply paragraphs."""


def _format_chunk(chunk) -> str:
    """Format a retrieved chunk for inclusion in the Step 8B prompt."""
    payload = getattr(chunk, "payload", {}) or {}
    chunk_type = payload.get("chunk_type", "document")
    text       = payload.get("text", "")

    if chunk_type == "judgment":
        ext = payload.get("ext") or {}
        header = (
            f"--- JUDGMENT ---\n"
            f"Case: {ext.get('case_name','N/A')} | "
            f"Court: {ext.get('court','N/A')} | "
            f"Year: {ext.get('judgment_date','N/A')}\n"
            f"Citation: {ext.get('citation','N/A')}\n"
            f"Decision: {ext.get('decision','N/A')}\n"
            f"Ratio:"
        )
        return f"{header}\n{text}"
    elif chunk_type == "section":
        refs = payload.get("cross_references") or {}
        section_ref = (refs.get("sections") or [""])[ 0] if refs.get("sections") else ""
        header = f"--- SECTION ---\n{section_ref}:"
        return f"{header}\n{text}"
    elif chunk_type == "rule":
        refs = payload.get("cross_references") or {}
        rule_ref = (refs.get("rules") or [""])[0] if refs.get("rules") else ""
        header = f"--- RULE ---\n{rule_ref}:"
        return f"{header}\n{text}"
    elif chunk_type in ("notification", "circular"):
        refs = payload.get("cross_references") or {}
        notif = (refs.get("notifications") or refs.get("circulars") or [""])[0] or ""
        header = f"--- {chunk_type.upper()} ---\n{notif}:"
        return f"{header}\n{text}"
    else:
        return f"--- {chunk_type.upper()} ---\n{text}"


def _build_draft_prompt(
    issue_text: str,
    top_chunks: list,
    other_issue_summaries: List[str],
    prior_replied_pairs: List[dict],
    reference_doc_summaries: List[dict],
    user_draft_text: Optional[str] = None,
) -> str:
    """
    Build the user-turn message for Step 8B.
    NEVER truncate issue_text or top_chunks.
    Soft components trimmed by count only.
    """
    parts = []

    # ── Core: issue text (NEVER truncated) ────────────────────────────────
    parts.append("## ISSUE TO BE REPLIED")
    parts.append(issue_text)
    parts.append("")

    # ── Core: retrieved legal material (NEVER truncated) ─────────────────
    if top_chunks:
        parts.append("## RETRIEVED LEGAL MATERIAL")
        parts.append("Use the following to build the reply (ordered by relevance):")
        parts.append("")
        for chunk in top_chunks:
            parts.append(_format_chunk(chunk))
            parts.append("")

    # ── Soft: other issues (IDs + 10-word summary — trim by count if budget tight) ──
    if other_issue_summaries:
        parts.append("## OTHER ALLEGATIONS IN THIS NOTICE (for consistency — do NOT address here)")
        for s in other_issue_summaries[:6]:  # cap at 6 to keep budget
            parts.append(s)
        parts.append("")

    # ── Soft: prior replied pairs (cap at 3 most relevant) ────────────────
    if prior_replied_pairs:
        parts.append("## PREVIOUSLY REPLIED ISSUES (maintain consistency — do not contradict)")
        for pair in prior_replied_pairs[:3]:
            parts.append(f"Prior allegation: {pair.get('issue_text','')}")
            parts.append(f"Reply taken:      {pair.get('reply_text','')}")
            parts.append("---")
        parts.append("")

    # ── Soft: reference document summaries ────────────────────────────────
    if reference_doc_summaries:
        parts.append("## REFERENCE DOCUMENTS PROVIDED")
        for ref in reference_doc_summaries[:5]:
            parts.append(f"{ref.get('filename','doc')}: {ref.get('brief_summary','')}")
        parts.append("")

    # ── Soft: user's draft reply (if present — NEVER truncated) ───────────
    if user_draft_text:
        parts.append("## USER'S DRAFT REPLY — IMPROVE WITHOUT CONTRADICTING")
        parts.append(
            "The user has prepared the following draft. "
            "Improve its legal grounding and citations. "
            "Do NOT contradict the core position or remove any argument the user made. "
            "Only add depth, legal citations, and stronger reasoning."
        )
        parts.append(user_draft_text)
        parts.append("")

    return "\n".join(parts)


def _process_single_issue(
    issue_text: str,
    issue_num: int,
    total_issues: int,
    all_issue_texts: List[str],
    mode: str,
    reference_doc_summaries: List[dict],
    prior_replied_pairs: List[dict],
    stage2b_result=None,
    user_draft_text: Optional[str] = None,
) -> Tuple[int, str, list, dict]:
    """
    Synchronous: retrieve + draft for one issue.
    Returns (issue_num, reply_text, sources, usage_dict)
    """
    # Step 8A: retrieve
    top_chunks = retrieve_for_issue(issue_text, mode, stage2b_result)

    # Other issues — IDs + 10-word summaries
    other_summaries = []
    for idx, txt in enumerate(all_issue_texts, 1):
        if txt != issue_text:
            words = txt.split()[:12]
            other_summaries.append(f"Issue {idx}: {' '.join(words)}...")

    # Step 8B: draft
    system_prompt = _SYSTEM_DEFENSIVE if mode == MODE_DEFENSIVE else _SYSTEM_IN_FAVOUR
    user_message  = _build_draft_prompt(
        issue_text              = issue_text,
        top_chunks              = top_chunks,
        other_issue_summaries   = other_summaries,
        prior_replied_pairs     = prior_replied_pairs,
        reference_doc_summaries = reference_doc_summaries,
        user_draft_text         = user_draft_text,
    )

    llm   = _get_llm()
    reply = llm.call(
        system_prompt = system_prompt,
        user_message  = user_message,
        max_tokens    = 8192,
        temperature   = 0.2,
        label         = f"step_8b_issue_{issue_num}",
    )
    reply = (reply or "").strip()
    if not reply:
        reply = (
            "Could not generate a draft reply for this issue. "
            "Please try again or provide additional context."
        )

    # Build sources list for the retrieval event
    sources = []
    for chunk in top_chunks:
        payload = getattr(chunk, "payload", {}) or {}
        ext     = payload.get("ext") or {}
        sources.append({
            "chunk_type": payload.get("chunk_type"),
            "case_name":  ext.get("case_name") or payload.get("summary","")[:80],
            "court":      ext.get("court"),
            "citation":   ext.get("citation"),
            "score":      round(getattr(chunk, "score", 0), 4),
        })

    usage = {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}
    return issue_num, reply, sources, usage


# ═════════════════════════════════════════════════════════════════════════════
# Async streaming driver for multiple issues
# ═════════════════════════════════════════════════════════════════════════════

async def process_issues_streaming(
    issues: List[dict],
    mode: str,
    reference_doc_summaries: List[dict],
    prior_replied_pairs: List[dict],
    stage2b_results: Dict[str, Any],
    max_parallel: int = 3,
) -> AsyncGenerator[Tuple[int, str, list, dict], None]:
    """
    Process up to max_parallel issues concurrently.
    Yields (issue_number, reply_text, sources, usage) in issue order.

    stage2b_results: {filename: Stage2BResult} — from snapshot legal_entities_cache
    """
    from starlette.concurrency import run_in_threadpool

    all_issue_texts = [i.get("issue_text", "") for i in issues]

    semaphore = asyncio.Semaphore(max_parallel)

    async def _process_one(issue: dict, local_num: int) -> Tuple[int, str, list, dict]:
        async with semaphore:
            source_doc    = issue.get("source_doc", "")
            stage2b       = stage2b_results.get(source_doc)
            issue_text    = issue.get("issue_text", "")
            user_draft    = issue.get("user_draft_text")

            num, reply, sources, usage = await run_in_threadpool(
                _process_single_issue,
                issue_text,
                local_num,
                len(issues),
                all_issue_texts,
                mode,
                reference_doc_summaries,
                prior_replied_pairs,
                stage2b,
                user_draft,
            )
            return num, reply, sources, usage

    tasks   = [asyncio.create_task(_process_one(iss, i+1)) for i, iss in enumerate(issues)]
    # Gather but yield in ORDER (not completion order) so UI is stable
    for task in tasks:
        num, reply, sources, usage = await task
        yield num, reply, sources, usage


# ═════════════════════════════════════════════════════════════════════════════
# Build prior replied pairs text (helper for document.py)
# ═════════════════════════════════════════════════════════════════════════════

def build_prior_replied_pairs(case: dict) -> List[dict]:
    """
    Collect replied_issues pairs from all previous_reply and user_draft_reply docs.
    Returns list of {issue_text, reply_text} dicts.
    Capped at 10 most recent.
    """
    pairs = []
    for doc in case.get("docs", []):
        if doc.get("role") in ("previous_reply", "user_draft_reply"):
            for p in doc.get("replied_issues", []):
                if p.get("issue_text") and p.get("reply_text"):
                    pairs.append(p)
    return pairs[-10:]


def build_reference_doc_summaries(case: dict) -> List[dict]:
    """
    Collect brief_summary from reference docs in the case.
    Returns list of {filename, brief_summary} dicts.
    """
    return [
        {"filename": d.get("filename",""), "brief_summary": d.get("brief_summary","")}
        for d in case.get("docs", [])
        if d.get("role") == "reference" and d.get("brief_summary")
    ]