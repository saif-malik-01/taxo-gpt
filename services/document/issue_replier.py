import asyncio
import logging
from typing import List, Dict, Tuple, Optional
from starlette.concurrency import run_in_threadpool

from services.retrieval.hybrid import retrieve
from services.llm.bedrock_client import call_bedrock
from services.chat.prompt_builder import get_system_prompt

logger = logging.getLogger(__name__)

# ============================================================================
# MODE CONSTANTS
# ============================================================================
MODE_DEFENSIVE = "defensive"
MODE_IN_FAVOUR = "in_favour"

# ============================================================================
# CHUNK TYPE SCORING
# Priority (document issues only — does not affect regular query flow):
#   Priority 1 — judgment                          → 40 pts (+ decision bonus)
#   Priority 2 — draft_reply                       → 35 pts
#   Priority 3 — notification, circular, act,
#                rule, section                     → 25 pts (all same level)
#   Priority 4 — everything else                   → 10 pts (all same level)
# ============================================================================

CHUNK_TYPE_SCORES = {
    "judgment":    40,
    "draft_reply": 35,
}

LEGAL_SOURCE_TYPES = {"notification", "circular", "act", "rule", "section"}
LEGAL_SOURCE_SCORE = 25
DEFAULT_SCORE      = 10  # analytical_review, contemporary_issues, and all others

DECISION_BONUS = {
    MODE_DEFENSIVE: {
        "In favour of assessee": +30,
        "In favour of revenue":  -20,
    },
    MODE_IN_FAVOUR: {
        "In favour of revenue":  +30,
        "In favour of assessee": -20,
    }
}

# ============================================================================
# STATIC RETRIEVAL QUERY TEMPLATES (one per mode — no LLM call needed)
# ============================================================================

DEFENSIVE_TEMPLATE = (
    "Under what conditions or exceptions is a taxpayer not required to {issue} "
    "and what relief or protection is available to the assessee in such cases"
)

IN_FAVOUR_TEMPLATE = (
    "Under what conditions is a taxpayer strictly liable for {issue} "
    "and when is non-compliance or non-payment not excusable under GST law"
)


def build_retrieval_query(issue: str, mode: str) -> str:
    """Generate the static mode-specific retrieval query for an issue."""
    template = DEFENSIVE_TEMPLATE if mode == MODE_DEFENSIVE else IN_FAVOUR_TEMPLATE
    return template.format(issue=issue)


# ============================================================================
# MODE DETECTION — keyword based, default defensive
# ============================================================================

_IN_FAVOUR_KEYWORDS = [
    "in favour of revenue", "in favor of revenue", "in favour of department",
    "in favor of department", "against the taxpayer", "against the assessee",
    "support the notice", "justify the notice", "authority is correct",
    "department is correct", "uphold the allegation", "support the allegation",
    "revenue's position", "department's position", "liability of taxpayer",
]


def detect_mode(question: str) -> str:
    """
    Keyword-based mode detection from user question.
    Checks in-favour keywords — if any match, return MODE_IN_FAVOUR.
    Default — MODE_DEFENSIVE if no keyword matched.
    """
    q = question.lower()
    if any(kw in q for kw in _IN_FAVOUR_KEYWORDS):
        logger.info("Mode detected: in_favour")
        return MODE_IN_FAVOUR
    logger.info("Mode: defensive (default)")
    return MODE_DEFENSIVE


# ============================================================================
# SCORING
# ============================================================================

def score_chunk(chunk: dict, mode: str) -> float:
    """
    Score a chunk based on:
      - Chunk type base score (priority hierarchy)
      - Decision field bonus (judgments only, mode-dependent)
      - Semantic similarity from retrieval (normalised 0–20 pts)
    """
    chunk_type = chunk.get("chunk_type", "").lower()

    # Base score
    if chunk_type == "judgment":
        base = CHUNK_TYPE_SCORES["judgment"]
    elif chunk_type == "draft_reply":
        base = CHUNK_TYPE_SCORES["draft_reply"]
    elif chunk_type in LEGAL_SOURCE_TYPES:
        base = LEGAL_SOURCE_SCORE
    else:
        base = DEFAULT_SCORE  # analytical_review, contemporary_issues, others

    # Decision field bonus — only for judgments
    decision_bonus = 0
    if chunk_type == "judgment":
        decision = chunk.get("metadata", {}).get("decision", "")
        decision_bonus = DECISION_BONUS.get(mode, {}).get(decision, 0)

    # Semantic similarity score — carry forward from retrieval, normalise to 0–20
    similarity       = chunk.get("_score", 0.5)
    similarity_score = min(float(similarity) * 20, 20)

    return base + decision_bonus + similarity_score


# ============================================================================
# SIBLING CHUNK EXPANSION
# ============================================================================

def get_sibling_chunks(retrieved_chunks: list, all_chunks: list) -> list:
    """
    For each retrieved chunk, fetch all other chunks sharing the same
    section_number or source document from all_chunks.

    Purpose: capture provisos, exceptions, and sub-clauses within the same
    provision that semantic search may have missed — these are the most
    common sources of defensive arguments in GST law.
    """
    sibling_keys = set()
    for chunk in retrieved_chunks:
        meta    = chunk.get("metadata", {})
        section = meta.get("section_number") or meta.get("section")
        source  = meta.get("source") or meta.get("source_file")
        if section:
            sibling_keys.add(("section", section))
        if source:
            sibling_keys.add(("source", source))

    existing_ids = {id(c) for c in retrieved_chunks}
    siblings     = []

    for chunk in all_chunks:
        if id(chunk) in existing_ids:
            continue
        meta    = chunk.get("metadata", {})
        section = meta.get("section_number") or meta.get("section")
        source  = meta.get("source") or meta.get("source_file")

        is_sibling = (
            (section and ("section", section) in sibling_keys) or
            (source  and ("source",  source)  in sibling_keys)
        )

        if is_sibling:
            chunk_copy           = dict(chunk)
            chunk_copy["_score"] = 0.4  # conservative default similarity for siblings
            siblings.append(chunk_copy)

    return siblings


# ============================================================================
# RETRIEVAL PIPELINE FOR ONE ISSUE
# ============================================================================

def _retrieve_and_rank_for_issue(
    issue: str,
    mode: str,
    store,
    all_chunks: list,
) -> Tuple[list, list]:
    """
    Three-pass retrieval for a single issue:

    Pass 1 — reframed mode-specific query
        Surfaces exception/condition chunks aligned to defensive or in-favour mode.

    Pass 2 — original issue text
        Surfaces main provisions, judgments, and draft replies relevant to the allegation.

    Pass 3 — sibling chunk expansion on Pass 2 results
        Fetches provisos and exceptions within the same parent section that
        semantic search may have ranked too low to appear in Pass 1 or 2.

    All chunks are deduplicated, scored, and sorted. Top 15 → primary, next 10 → supporting.
    """
    reframed_query = build_retrieval_query(issue, mode)

    # Pass 1
    pass1 = retrieve(query=reframed_query, vector_store=store, all_chunks=all_chunks, k=15)
    # Pass 2
    pass2 = retrieve(query=issue,          vector_store=store, all_chunks=all_chunks, k=15)
    # Pass 3
    siblings = get_sibling_chunks(pass2, all_chunks)

    # Deduplicate across all three passes
    seen          = set()
    all_retrieved = []
    for chunk in pass1 + pass2 + siblings:
        key = chunk.get("id") or chunk.get("text", "")[:120]
        if key not in seen:
            seen.add(key)
            all_retrieved.append(chunk)

    # Score and sort descending
    for chunk in all_retrieved:
        chunk["_final_score"] = score_chunk(chunk, mode)
    all_retrieved.sort(key=lambda x: x["_final_score"], reverse=True)

    primary    = all_retrieved[:15]
    supporting = all_retrieved[15:25]

    logger.info(
        f"   Issue retrieval: pass1={len(pass1)}, pass2={len(pass2)}, "
        f"siblings={len(siblings)}, deduped={len(all_retrieved)}, "
        f"primary={len(primary)}, supporting={len(supporting)}"
    )
    return primary, supporting


# ============================================================================
# PROMPT BUILDER FOR ONE ISSUE
# ============================================================================

def _build_issue_prompt(
    issue: str,
    issue_number: int,
    total_issues: int,
    primary: list,
    supporting: list,
    mode: str,
    recipient: str = None,
    sender: str = None,
    profile_summary: str = None,
) -> str:
    """Build the user-turn prompt for generating a single issue reply."""

    def render(chunks):
        parts = []
        for c in chunks:
            chunk_type = c.get("chunk_type", "source").upper()
            meta   = c.get("metadata", {})
            source = meta.get("source") or meta.get("source_file") or c.get("parent_doc", "source")
            parts.append(f"[{chunk_type} | {source}]\n{c['text']}")
        return "\n\n".join(parts)

    if mode == MODE_DEFENSIVE:
        mode_instruction = (
            "Prepare a strong defensive reply protecting the notice recipient's position for submission to a tax authority.\n\n"
            "- Find every applicable legal exception, proviso, condition, and precedent in the recipient's favour.\n"
            "- Identify the specific condition or exception within the same provision under which the recipient's action was legally permissible.\n"
            "- Prioritise judgments where the decision was 'In favour of assessee' — state the court, citation, and apply the ratio directly to this issue.\n"
            "- Quote statutory wording, circular text, notification language, and judgment extracts verbatim from the retrieved material — exact legal text carries more weight than paraphrasing.\n"
            "- Ground every argument in a specific section, sub-section, proviso, or clause — not general assertion.\n"
            "- Demonstrate that the recipient's position is fully in accordance with applicable law — do not attribute fault to any party.\n"
            "- Conclude by establishing that the allegation is not legally sustainable on the recipient's facts."
        )
    else:
        mode_instruction = (
            "Prepare a strong reply establishing the legal basis for the allegation for submission to a tax authority.\n\n"
            "- Find every applicable provision, condition, and precedent that supports the revenue's position.\n"
            "- Identify the specific section, proviso, or clause under which the recipient's action constitutes non-compliance.\n"
            "- Prioritise judgments where the decision was 'In favour of revenue' — state the court, citation, and apply the ratio directly to this issue.\n"
            "- Quote statutory wording, circular text, notification language, and judgment extracts verbatim from the retrieved material — exact legal text carries more weight than paraphrasing.\n"
            "- Ground every argument in a specific section, sub-section, proviso, or clause — not general assertion.\n"
            "- Conclude by establishing that the obligation squarely applies to the recipient's facts and the allegation is legally sustainable."
        )

    context_lines = []
    if recipient:
        context_lines.append(f"Notice Recipient: {recipient}")
    if sender:
        context_lines.append(f"Issuing Authority: {sender}")
    context_block = "\n".join(context_lines)

    return f"""You are preparing the reply for Issue {issue_number} of {total_issues} from a legal notice.

{context_block}

ISSUE {issue_number}:
{issue}

INSTRUCTION:
{mode_instruction}

Your reply for this issue must:
1. Acknowledge the allegation precisely.
2. Provide counter-arguments using the legal material below.
3. Cite specific sections, provisos, notifications, circulars, or judgments that support the position.
4. For judgments, state the decision and explain how it applies to this issue.
5. Conclude with a clear statement on why this issue should be decided in the client's favour.

PRIMARY LEGAL MATERIAL (MOST RELEVANT):
{render(primary)}

SUPPORTING LEGAL MATERIAL (USE ONLY IF IT ADDS REAL VALUE):
{render(supporting)}

Write the reply for Issue {issue_number} only. Be precise, professional, and legally grounded.
Do NOT add any closing statement, signature block, "Respectfully submitted", "Authorised Signatory", or date at the end. The closing will be added once after all issues are addressed.
"""


# ============================================================================
# SINGLE ISSUE PROCESSOR (runs in thread pool)
# ============================================================================

def _process_single_issue(
    issue: str,
    issue_number: int,
    total_issues: int,
    mode: str,
    store,
    all_chunks: list,
    recipient: str = None,
    sender: str = None,
    profile_summary: str = None,
) -> Tuple[int, str]:
    """
    Full pipeline for one issue — runs in a thread pool:
      retrieve → rank → build prompt → call LLM → return reply text
    Returns (issue_number, reply_text).
    """
    try:
        logger.info(f"🔍 Processing Issue {issue_number}/{total_issues}: {issue[:80]}...")

        primary, supporting = _retrieve_and_rank_for_issue(issue, mode, store, all_chunks)

        system_prompt = get_system_prompt(profile_summary)
        user_prompt   = _build_issue_prompt(
            issue=issue,
            issue_number=issue_number,
            total_issues=total_issues,
            primary=primary,
            supporting=supporting,
            mode=mode,
            recipient=recipient,
            sender=sender,
            profile_summary=profile_summary,
        )

        reply = call_bedrock(
            prompt=user_prompt,
            system_prompts=[system_prompt],
            temperature=0.0
        )

        logger.info(f"✅ Issue {issue_number} reply ready ({len(reply)} chars)")
        return issue_number, reply

    except Exception as e:
        logger.error(f"❌ Issue {issue_number} failed: {e}", exc_info=True)
        return issue_number, f"[Error generating reply for Issue {issue_number}: {str(e)}]"


# ============================================================================
# PARALLEL ORCHESTRATOR
# ============================================================================

async def process_issues_parallel(
    issues: list,
    mode: str,
    store,
    all_chunks: list,
    recipient: str = None,
    sender: str = None,
    profile_summary: str = None,
    max_parallel: int = 3,
) -> Dict[int, str]:
    """
    Process all issues in parallel with a concurrency cap of max_parallel (default 3).

    All issues run simultaneously for retrieval + LLM generation.
    Results are returned as {issue_number: reply_text} dict.
    Streaming to the client is handled separately in main.py — sequentially
    per issue to prevent mixing of streams.
    """
    total     = len(issues)
    semaphore = asyncio.Semaphore(max_parallel)

    async def bounded_process(issue, issue_number):
        async with semaphore:
            return await run_in_threadpool(
                _process_single_issue,
                issue, issue_number, total,
                mode, store, all_chunks,
                recipient, sender, profile_summary
            )

    logger.info(f"🚀 Processing {total} issues in parallel (max {max_parallel} concurrent)")

    tasks     = [bounded_process(issue, i + 1) for i, issue in enumerate(issues)]
    completed = await asyncio.gather(*tasks, return_exceptions=True)

    results = {}
    for result in completed:
        if isinstance(result, Exception):
            logger.error(f"Issue task exception: {result}")
            continue
        issue_num, reply = result
        results[issue_num] = reply

    logger.info(f"✅ All {len(results)}/{total} issues processed")
    return results