"""
services/document/issue_replier.py

Step 8A — Retrieval for each issue using the SAME pipeline as Feature 1.
Step 8B — LLM draft generation per issue.

FLOW (matches spec exactly):
    Phase 1 — Retrieve ALL issues concurrently (up to 3 at a time)
        query = issue text only (verbatim, no summary, no sender)
        pipeline.query_stages_1_to_5(issue_text, session_history=[])
        → top 50 RRF-ranked chunks

    Phase 2 — Rerank each issue's pool (judgment-only, mode-aware, multiplicative)
        defensive  → in_favour_of_assessee  × BOOST_FACTOR  (1.4)
        defensive  → in_favour_of_department × PENALTY_FACTOR (0.5)
        in_favour  → reversed
        non-judgment chunks: score unchanged
        re-sort → keep top 20

    Phase 3 — Draft ALL issues concurrently (up to 3 at a time)
        One Qwen call per issue.
        Prompt contains (per spec §3.2):
            ISSUE TO BE REPLIED           — verbatim, never truncated
            RETRIEVED LEGAL MATERIAL      — top 20 chunks, all types, never truncated
            OTHER ALLEGATIONS             — other issue IDs + 10-word summary
            PREVIOUSLY REPLIED ISSUES     — for consistency (from previous_reply docs)
            REFERENCE DOCUMENTS           — FULL TEXT from get_reference_texts() DB call
            USER'S DRAFT REPLY            — if user_draft_reply doc present
            DOC SUMMARY                   — case-level summary (snapshot.summary)
            RECIPIENT                     — recipient name only (sender excluded per spec)

    Phases 1 and 3 both use asyncio.Semaphore(max_parallel=3) independently,
    so retrieval for all issues completes before any draft call starts.
    Within each phase, up to 3 run concurrently.

Modes:
    MODE_DEFENSIVE = "defensive"
    MODE_IN_FAVOUR = "in_favour"

Reranking (spec §7 + approach v3 table 18):
    MULTIPLICATIVE on the chunk's existing score.
    Judgment boost/penalty only — sections, rules, notifications untouched.
    Boost factor  : 1.4  (judgment aligned with mode)
    Penalty factor: 0.5  (judgment opposed to mode)
    Score floor   : 0.0  (never go negative)
"""

import asyncio
import logging
import threading
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

MODE_DEFENSIVE = "defensive"
MODE_IN_FAVOUR = "in_favour"

# ── Reranking constants (multiplicative, spec §7 / approach v3 table 18) ─────
_BOOST_FACTOR    = 1.4   # judgment aligned with mode  → score × 1.4
_PENALTY_FACTOR  = 0.5   # judgment opposed to mode    → score × 0.5
_SCORE_FLOOR     = 0.0   # never go below zero

# ── Retrieval pool and final window ──────────────────────────────────────────
_TOP_POOL  = 50   # retrieve this many before reranking
_TOP_FINAL = 20   # send this many to LLM (spec §7)

# ── Concurrency ───────────────────────────────────────────────────────────────
_DEFAULT_MAX_PARALLEL = 3

# ── Pipeline singleton ────────────────────────────────────────────────────────
_pipeline   = None
_pipe_lock  = threading.Lock()


def set_pipeline(p) -> None:
    global _pipeline
    with _pipe_lock:
        _pipeline = p


def _get_pipeline():
    global _pipeline
    if _pipeline is None:
        with _pipe_lock:
            if _pipeline is None:
                from apps.api.src.services.rag.retrieval.pipeline import RetrievalPipeline
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
                from apps.api.src.services.llm.bedrock import BedrockLLMClient
                _llm = BedrockLLMClient()
    return _llm


# ═════════════════════════════════════════════════════════════════════════════
# STEP 8A — RETRIEVAL (per issue, issue text only)
# ═════════════════════════════════════════════════════════════════════════════

def _rerank_for_mode(chunks: list, mode: str) -> list:
    """
    Judgment-only multiplicative reranking.

    Spec §7 / approach v3 table 18:
        defensive → in_favour_of_assessee   × 1.4
        defensive → in_favour_of_department × 0.5
        in_favour → reversed
        All other chunk types: score unchanged.

    Uses chunk.payload.ext.decision field.
    Re-sorts the list descending by score after adjustment.
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
            continue                         # sections/rules/notifications untouched
        ext      = payload.get("ext") or {}
        decision = ext.get("decision")
        if not decision:
            continue
        if decision == want:
            chunk.score = max(chunk.score * _BOOST_FACTOR, _SCORE_FLOOR)
        else:
            chunk.score = max(chunk.score * _PENALTY_FACTOR, _SCORE_FLOOR)

    chunks.sort(key=lambda c: c.score, reverse=True)
    return chunks


def _retrieve_for_issue_sync(issue_text: str, mode: str) -> list:
    """
    Synchronous retrieval for one issue.
    Runs inside run_in_threadpool — never call from async context directly.

    Query = issue text verbatim (no summary, no sender, no case context).
    This is the exact signal the spec mandates for Step 8A.

    Pipeline stages 1-5 run internally (extraction, retrieval, filter, cross-ref).
    The cached Stage2BResult from Step 2B cannot be injected into the existing
    pipeline without modifying pipeline.py — the pipeline re-extracts internally.
    The spec notes this as a future optimisation (pre_built_stage2b parameter).
    For now, the pipeline runs its own lightweight extraction on the issue text
    which is fast (< 1s for regex, 15-25s for LLM — but BM25 index is already warm).

    Returns top 20 reranked chunks, all chunk types (spec §7).
    """
    pipeline = _get_pipeline()
    if pipeline is None:
        logger.warning("Retrieval pipeline not available for issue retrieval")
        return []

    try:
        # Stage 1-5: no history for issue retrieval (stateless per-issue)
        staged = pipeline.query_stages_1_to_5(
            user_query      = issue_text,   # issue text only — spec §8A
            session_history = [],           # no history — each issue is independent
        )
        # staged = (final_query, session_history, chunks, citation_result, intent, cross_refs)
        _, _, chunks, _, _, _ = staged

        if not chunks:
            logger.info(f"8A: no chunks for issue (len={len(issue_text)})")
            return []

        # Rerank pool (top 50) → keep top 20 (spec §7)
        pool   = chunks[:_TOP_POOL]
        ranked = _rerank_for_mode(pool, mode)
        top_20 = ranked[:_TOP_FINAL]

        logger.info(
            f"8A: issue_len={len(issue_text)} pool={len(pool)} "
            f"→ top20={len(top_20)} "
            f"types={[c.payload.get('chunk_type') for c in top_20[:5]]}"
        )
        return top_20

    except Exception as e:
        logger.error(f"8A retrieval error: {e}", exc_info=True)
        return []


# ═════════════════════════════════════════════════════════════════════════════
# STEP 8B — SYSTEM PROMPT (spec §3.1 — single prompt with mode block inserted)
# ═════════════════════════════════════════════════════════════════════════════

_SYSTEM_BASE = """\
You are a senior Indian tax law expert specialising in GST, Income Tax, and Customs.
Your task is to draft a formal legal reply to a tax notice issued by a government authority.
You will be given:
  - The specific allegation (issue) requiring a reply
  - Retrieved legal material: statutes, rules, notifications, circulars, judgments
  - Context: other allegations in the same notice, previously replied issues, reference docs
  - A MODE INSTRUCTION that determines the direction of the reply

=== MODE INSTRUCTION ===
{mode_block}

=== OUTPUT FORMAT ===
Write in formal legal language as paragraph-form prose (not bullet points).
Cite sections as: 'Section X of the CGST Act, 2017' or 'Rule X of the CGST Rules, 2017'.
Cite judgments as: '[Case Name] — [Court] — [Year] — [Citation if available]'.
Do not add a subject line, salutation, or closing — only the substantive reply paragraphs."""

_MODE_BLOCK_DEFENSIVE = """\
REPLY OBJECTIVE: Defend the taxpayer against this allegation.
Your reply must:
  1. Identify and apply every available legal ground that extinguishes or reduces the demand.
  2. Cite the specific sub-section, proviso, exception, or condition in the statute or rule
     that makes the allegation inapplicable to the taxpayer's facts.
  3. Cite judgments where the assessee succeeded on similar facts. Apply their ratio to
     the current facts explicitly. Do not merely cite — explain WHY the ratio applies.
  4. If an adverse judgment is retrieved, distinguish it on facts — explain concretely
     how the taxpayer's situation differs from the facts in that judgment.
  5. Conclude: why the demand is not sustainable and should be dropped."""

_MODE_BLOCK_IN_FAVOUR = """\
REPLY OBJECTIVE: Establish why the department's demand is correct and sustainable.
Your reply must:
  1. Identify the statutory provisions, rules, and notifications that create the liability.
  2. Apply the conditions in those provisions to the taxpayer's specific facts to show
     that all conditions for the demand are met.
  3. Cite judgments where the department succeeded on similar facts. Apply their ratio.
  4. Address any defences the taxpayer might raise — pre-empt them using statutory text
     or contrary judgments.
  5. Conclude: why the demand is legally correct and must be upheld."""


def _build_system_prompt(mode: str) -> str:
    """Single system prompt with mode block inserted at call time (spec §3.1)."""
    mode_block = (
        _MODE_BLOCK_DEFENSIVE if mode == MODE_DEFENSIVE else _MODE_BLOCK_IN_FAVOUR
    )
    return _SYSTEM_BASE.format(mode_block=mode_block)


# ═════════════════════════════════════════════════════════════════════════════
# STEP 8B — USER MESSAGE / PROMPT BODY (spec §3.2)
# ═════════════════════════════════════════════════════════════════════════════

def _format_chunk(chunk) -> str:
    """Format one retrieved chunk for the prompt, labelled by chunk_type."""
    payload    = getattr(chunk, "payload", {}) or {}
    chunk_type = payload.get("chunk_type", "document")
    text       = payload.get("text", "")

    if chunk_type == "judgment":
        ext    = payload.get("ext") or {}
        header = (
            f"--- JUDGMENT ---\n"
            f"Case: {ext.get('case_name','N/A')} | "
            f"Court: {ext.get('court','N/A')} | "
            f"Year: {ext.get('judgment_date','N/A')}\n"
            f"Citation: {ext.get('citation','N/A')}\n"
            f"Decision: {ext.get('decision','N/A')}\n"
            f"Ratio:"
        )
    elif chunk_type in ("cgst_section", "igst_section"):
        refs       = payload.get("cross_references") or {}
        section_ref = (refs.get("sections") or [""])[0]
        header = f"--- SECTION ---\n{section_ref}:"
    elif chunk_type in ("cgst_rule", "igst_rule", "gstat_rule"):
        refs     = payload.get("cross_references") or {}
        rule_ref = (refs.get("rules") or [""])[0]
        header = f"--- RULE ---\n{rule_ref}:"
    elif chunk_type == "notification":
        refs      = payload.get("cross_references") or {}
        notif_ref = (refs.get("notifications") or [""])[0]
        header = f"--- NOTIFICATION ---\n{notif_ref}:"
    elif chunk_type == "circular":
        refs     = payload.get("cross_references") or {}
        circ_ref = (refs.get("circulars") or [""])[0]
        header = f"--- CIRCULAR ---\n{circ_ref}:"
    else:
        header = f"--- {chunk_type.upper()} ---"

    return f"{header}\n{text}"


def _build_draft_prompt(
    *,
    issue_text:             str,
    top_chunks:             list,
    all_issues:             List[dict],       # full issue list from active_case
    current_issue_id:       int,
    prior_replied_pairs:    List[dict],       # {issue_text, reply_text} from previous_reply docs
    reference_doc_full_text: str,             # full text from get_reference_texts() — never truncated
    case_summary:           str,              # case-level summary from snapshot
    recipient_name:         Optional[str],    # recipient name only (sender excluded per spec)
    user_draft_text:        Optional[str],    # user_draft_reply text for this issue (if any)
) -> str:
    """
    Build the user-turn message for Step 8B exactly as spec §3.2.

    Hard constraints (spec):
        - issue_text            : NEVER truncated
        - top_chunks            : NEVER truncated (all 20)
        - reference_doc_full_text: NEVER truncated

    Soft components (trimmed by COUNT only — never mid-sentence):
        - other_issues          : capped at 8 entries, 10-word summaries
        - prior_replied_pairs   : capped at 3 most recent
        - case_summary          : capped at 400 chars (already stored ≤400)
    """
    parts: List[str] = []

    # ── §3.2 preamble: doc summary + recipient (spec table 19) ───────────────
    if case_summary or recipient_name:
        parts.append("## CASE CONTEXT")
        if case_summary:
            parts.append(f"Document summary: {case_summary[:400]}")
        if recipient_name:
            parts.append(f"Recipient (notice recipient / taxpayer): {recipient_name}")
        parts.append("")

    # ── §3.2 block 1: issue text (NEVER truncated) ───────────────────────────
    parts.append("## ISSUE TO BE REPLIED")
    parts.append(issue_text)
    parts.append("")

    # ── §3.2 block 2: retrieved legal material (NEVER truncated) ─────────────
    if top_chunks:
        parts.append("## RETRIEVED LEGAL MATERIAL")
        parts.append(
            "Use the following statutes, rules, notifications, circulars, and judgments "
            "to build the reply. These are ordered by relevance score (highest first)."
        )
        parts.append("")
        for chunk in top_chunks:
            parts.append(_format_chunk(chunk))
            parts.append("")

    # ── §3.2 block 3: other allegations in same notice (consistency) ─────────
    other_issues = [
        i for i in all_issues
        if i.get("id") != current_issue_id
    ]
    if other_issues:
        parts.append(
            "## OTHER ALLEGATIONS IN THIS NOTICE "
            "(for consistency — do NOT address these here)"
        )
        for iss in other_issues[:8]:   # cap at 8 entries
            words   = iss.get("issue_text", "").split()[:10]
            summary = " ".join(words) + ("..." if len(words) == 10 else "")
            parts.append(f"Issue {iss.get('id','?')}: {summary}")
        parts.append("")

    # ── §3.2 block 4: previously replied issues (consistency constraint) ──────
    if prior_replied_pairs:
        parts.append(
            "## PREVIOUSLY REPLIED ISSUES "
            "(maintain consistency — do not contradict)"
        )
        for pair in prior_replied_pairs[:3]:   # cap at 3 most recent
            parts.append(f"Prior allegation: {pair.get('issue_text','')}")
            parts.append(f"Reply taken:      {pair.get('reply_text','')}")
            parts.append("---")
        parts.append("")

    # ── §3.2 block 5: reference documents (FULL TEXT — never truncated) ───────
    if reference_doc_full_text and reference_doc_full_text.strip():
        parts.append("## REFERENCE DOCUMENTS PROVIDED")
        parts.append(
            "Use the following reference material (judgments, circulars, etc. "
            "uploaded by the user) where applicable:"
        )
        parts.append(reference_doc_full_text)
        parts.append("")

    # ── §3.2 block 6: user's draft reply (if present — NEVER truncated) ───────
    if user_draft_text and user_draft_text.strip():
        parts.append("## USER'S DRAFT REPLY — IMPROVE WITHOUT CONTRADICTING")
        parts.append(
            "The user has prepared the following draft reply. "
            "Improve its legal grounding and citations. "
            "Do NOT contradict the core position or remove any argument the user made. "
            "Only add depth, legal citations, and stronger reasoning."
        )
        parts.append(user_draft_text)
        parts.append("")

    return "\n".join(parts)


# ═════════════════════════════════════════════════════════════════════════════
# DRAFT ONE ISSUE (synchronous — runs in ThreadPoolExecutor)
# ═════════════════════════════════════════════════════════════════════════════

def _draft_issue_sync(
    *,
    issue:                  dict,
    issue_num:              int,              # 1-based index within issues_to_draft
    all_issues:             List[dict],
    top_chunks:             list,             # already retrieved and reranked
    mode:                   str,
    case_summary:           str,
    recipient_name:         Optional[str],
    prior_replied_pairs:    List[dict],
    reference_doc_full_text: str,
) -> Tuple[int, str, list]:
    """
    Synchronous LLM draft call for one issue.
    Returns (issue_num, reply_text, sources_list).
    """
    issue_text     = issue.get("issue_text", "")
    current_id     = issue.get("id", issue_num)
    user_draft     = issue.get("user_draft_text")        # set by caller if applicable

    system_prompt = _build_system_prompt(mode)
    user_message  = _build_draft_prompt(
        issue_text              = issue_text,
        top_chunks              = top_chunks,
        all_issues              = all_issues,
        current_issue_id        = current_id,
        prior_replied_pairs     = prior_replied_pairs,
        reference_doc_full_text = reference_doc_full_text,
        case_summary            = case_summary,
        recipient_name          = recipient_name,
        user_draft_text         = user_draft,
    )

    llm   = _get_llm()
    reply = llm.call(
        system_prompt = system_prompt,
        user_message  = user_message,
        max_tokens    = 8192,
        temperature   = 0.2,
        label         = f"step_8b_issue_{current_id}",
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
            "case_name":  ext.get("case_name") or payload.get("summary", "")[:80],
            "court":      ext.get("court"),
            "citation":   ext.get("citation"),
            "score":      round(getattr(chunk, "score", 0), 4),
        })

    return issue_num, reply, sources


# ═════════════════════════════════════════════════════════════════════════════
# MAIN ASYNC ENTRY POINT — retrieve-all-first, then draft-all
# ═════════════════════════════════════════════════════════════════════════════

async def process_issues_streaming(
    *,
    issues:                 List[dict],
    mode:                   str,
    case_summary:           str,
    recipient_name:         Optional[str],
    prior_replied_pairs:    List[dict],
    reference_doc_full_text: str,
    max_parallel:           int = _DEFAULT_MAX_PARALLEL,
) -> AsyncGenerator[Tuple[int, str, list], None]:
    """
    Two-phase async pipeline per the spec and the user's explicit requirement:

        Phase 1: Retrieve for ALL issues concurrently (≤ max_parallel at once).
                 All retrievals complete before any draft starts.

        Phase 2: Draft ALL issues concurrently (≤ max_parallel at once).
                 Results yielded in issue ORDER (not completion order) so the
                 UI displays issues 1, 2, 3 in sequence even if 3 finishes first.

    Yields: (issue_number, reply_text, sources_list)
        issue_number is 1-based index into `issues`.

    Args:
        issues                  — list of issue dicts from active_case["issues"]
        mode                    — MODE_DEFENSIVE or MODE_IN_FAVOUR
        case_summary            — case-level summary text (snapshot level)
        recipient_name          — taxpayer / notice recipient name (sender excluded)
        prior_replied_pairs     — [{issue_text, reply_text}] from previous_reply docs
        reference_doc_full_text — full text of all reference docs (from get_reference_texts())
        max_parallel            — max concurrent retrieval OR draft calls (default 3)
    """
    from starlette.concurrency import run_in_threadpool

    if not issues:
        return

    retrieve_sem = asyncio.Semaphore(max_parallel)
    draft_sem    = asyncio.Semaphore(max_parallel)

    # ── Phase 1: Retrieve for all issues concurrently ─────────────────────────

    async def _retrieve_one(issue: dict, idx: int) -> Tuple[int, list]:
        """Retrieve and rerank for one issue. Returns (idx, top_chunks)."""
        async with retrieve_sem:
            issue_text = issue.get("issue_text", "")
            issue_id   = issue.get("id", idx + 1)
            logger.info(
                f"8A: starting retrieval for issue {issue_id} "
                f"(idx={idx}, len={len(issue_text)})"
            )
            top_chunks = await run_in_threadpool(
                _retrieve_for_issue_sync, issue_text, mode
            )
            logger.info(
                f"8A: done issue {issue_id} → {len(top_chunks)} chunks"
            )
            return idx, top_chunks

    logger.info(
        f"Step 8A: retrieving for {len(issues)} issue(s), "
        f"max_parallel={max_parallel}"
    )
    retrieve_tasks = [
        asyncio.create_task(_retrieve_one(iss, i))
        for i, iss in enumerate(issues)
    ]
    retrieve_results = await asyncio.gather(*retrieve_tasks)

    # Build ordered list: chunks_by_idx[i] = top_chunks for issues[i]
    chunks_by_idx: List[list] = [None] * len(issues)
    for idx, top_chunks in retrieve_results:
        chunks_by_idx[idx] = top_chunks or []

    logger.info("Step 8A: all retrievals complete — starting draft phase (8B)")

    # ── Phase 2: Draft all issues concurrently, yield in order ───────────────

    async def _draft_one(issue: dict, idx: int, top_chunks: list) -> Tuple[int, str, list]:
        """Draft reply for one issue. Returns (idx, reply_text, sources)."""
        async with draft_sem:
            issue_id = issue.get("id", idx + 1)
            logger.info(f"8B: starting draft for issue {issue_id} (idx={idx})")
            issue_num, reply, sources = await run_in_threadpool(
                _draft_issue_sync,
                issue                  = issue,
                issue_num              = idx + 1,
                all_issues             = issues,
                top_chunks             = top_chunks,
                mode                   = mode,
                case_summary           = case_summary,
                recipient_name         = recipient_name,
                prior_replied_pairs    = prior_replied_pairs,
                reference_doc_full_text= reference_doc_full_text,
            )
            logger.info(f"8B: done issue {issue_id}, reply_len={len(reply)}")
            return idx, reply, sources

    draft_tasks = [
        asyncio.create_task(_draft_one(iss, i, chunks_by_idx[i]))
        for i, iss in enumerate(issues)
    ]

    # Gather into a dict keyed by idx, then yield in ORDER
    draft_results: Dict[int, Tuple[str, list]] = {}
    for coro in asyncio.as_completed(draft_tasks):
        idx, reply, sources = await coro
        draft_results[idx] = (reply, sources)

    # Yield in issue order (1-based issue_number)
    for i, issue in enumerate(issues):
        reply, sources = draft_results[i]
        yield i + 1, reply, sources


# ═════════════════════════════════════════════════════════════════════════════
# Helpers used by api/document.py (unchanged public API)
# ═════════════════════════════════════════════════════════════════════════════

def build_prior_replied_pairs(case: dict) -> List[dict]:
    """
    Collect {issue_text, reply_text} pairs from all previous_reply and
    user_draft_reply docs in the case.
    Capped at 10 most recent. Used in Step 8B prompt for consistency.
    """
    pairs: List[dict] = []
    for doc in case.get("docs", []):
        if doc.get("role") in ("previous_reply", "user_draft_reply"):
            for p in doc.get("replied_issues", []):
                if p.get("issue_text") and p.get("reply_text"):
                    pairs.append(p)
    return pairs[-10:]


def build_reference_doc_summaries(case: dict) -> List[dict]:
    """
    Collect brief_summary from reference docs.
    Used for display in the summary block — NOT sent to the draft LLM.
    The draft LLM receives FULL reference text via get_reference_texts().
    """
    return [
        {"filename": d.get("filename", ""), "brief_summary": d.get("brief_summary", "")}
        for d in case.get("docs", [])
        if d.get("role") == "reference" and d.get("brief_summary")
    ]