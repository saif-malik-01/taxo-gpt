import asyncio
import logging
from typing import List, Dict, Tuple, Optional, AsyncGenerator
from starlette.concurrency import run_in_threadpool

from services.retrieval.hybrid import retrieve
from services.llm.bedrock_client import call_bedrock
from services.chat.prompt_builder import get_system_prompt
from services.chat.engine import get_full_judgments

logger = logging.getLogger(__name__)

# ============================================================================
# MODE CONSTANTS
# ============================================================================
MODE_DEFENSIVE = "defensive"
MODE_IN_FAVOUR = "in_favour"

# ============================================================================
# CHUNK TYPE SCORING
# ============================================================================

CHUNK_TYPE_SCORES = {
    "judgment":    40,
    "draft_reply": 35,
}

LEGAL_SOURCE_TYPES = {"notification", "circular", "act", "rule", "section"}
LEGAL_SOURCE_SCORE = 25
DEFAULT_SCORE      = 10

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
# STATIC RETRIEVAL QUERY TEMPLATES
# ============================================================================

DEFENSIVE_TEMPLATE = (
    "Under what conditions or exceptions is a taxpayer not required to {issue} "
    "and what relief or protection is available to the assessee in such cases"
)

IN_FAVOUR_TEMPLATE = (
    "Under what conditions is a taxpayer strictly liable for {issue} "
    "and when is non-compliance or non-payment not excusable under GST law"
)


def build_retrieval_query(issue: str, mode: str, doc_summary: str = None) -> str:
    """
    Generate the mode-specific retrieval query for an issue.
    If doc_summary is provided, append key facts from it to enrich retrieval.
    """
    template = DEFENSIVE_TEMPLATE if mode == MODE_DEFENSIVE else IN_FAVOUR_TEMPLATE
    base_query = template.format(issue=issue)

    if doc_summary:
        # Append a trimmed summary snippet to ground retrieval in document facts
        # Keep it short — retrieval query should be concise
        summary_snippet = doc_summary[:300].strip()
        return f"{base_query}. Context: {summary_snippet}"

    return base_query


# ============================================================================
# MODE DETECTION
# ============================================================================

_IN_FAVOUR_KEYWORDS = [
    "in favour of revenue", "in favor of revenue", "in favour of department",
    "in favor of department", "against the taxpayer", "against the assessee",
    "support the notice", "justify the notice", "authority is correct",
    "department is correct", "uphold the allegation", "support the allegation",
    "revenue's position", "department's position", "liability of taxpayer",
]


def detect_mode(question: str) -> str:
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
    chunk_type = chunk.get("chunk_type", "").lower()

    if chunk_type == "judgment":
        base = CHUNK_TYPE_SCORES["judgment"]
    elif chunk_type == "draft_reply":
        base = CHUNK_TYPE_SCORES["draft_reply"]
    elif chunk_type in LEGAL_SOURCE_TYPES:
        base = LEGAL_SOURCE_SCORE
    else:
        base = DEFAULT_SCORE

    decision_bonus = 0
    if chunk_type == "judgment":
        decision = chunk.get("metadata", {}).get("decision", "")
        decision_bonus = DECISION_BONUS.get(mode, {}).get(decision, 0)

    similarity       = chunk.get("_score", 0.5)
    similarity_score = min(float(similarity) * 20, 20)

    return base + decision_bonus + similarity_score


# ============================================================================
# SOURCE FORMATTER
# ============================================================================

def _format_sources(chunks: list) -> list:
    sources = []
    for c in chunks:
        sources.append({
            "id":         c.get("id", ""),
            "chunk_type": c.get("chunk_type", ""),
            "text":       c.get("text", ""),
            "metadata":   c.get("metadata", {}),
        })
    return sources


# ============================================================================
# SIBLING CHUNK EXPANSION
# ============================================================================

def get_sibling_chunks(retrieved_chunks: list, all_chunks: list) -> list:
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
            chunk_copy["_score"] = 0.4
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
    doc_summary: str = None,
) -> Tuple[list, list]:
    """
    Three-pass retrieval for a single issue.
    Pass 1 and Pass 2 queries are enriched with doc_summary key facts if available.
    Pass 3 (sibling expansion) is unchanged.
    """
    reframed_query = build_retrieval_query(issue, mode, doc_summary)

    pass1    = retrieve(query=reframed_query, vector_store=store, all_chunks=all_chunks, k=15)
    pass2    = retrieve(query=issue,          vector_store=store, all_chunks=all_chunks, k=15)
    siblings = get_sibling_chunks(pass2, all_chunks)

    seen          = set()
    all_retrieved = []
    for chunk in pass1 + pass2 + siblings:
        key = chunk.get("id") or chunk.get("text", "")[:120]
        if key not in seen:
            seen.add(key)
            all_retrieved.append(chunk)

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
    all_issues: list,
    primary: list,
    supporting: list,
    mode: str,
    recipient: str = None,
    sender: str = None,
    doc_summary: str = None,
    profile_summary: str = None,
) -> str:
    """
    Build the user-turn prompt for a single issue reply.

    Sends to LLM in this order:
      1. Document details — sender, recipient, document type context
      2. Full document summary — all factual details
      3. All other issues in the notice — for consistency across replies
      4. Current issue verbatim
      5. Retrieved legal chunks — primary and supporting
    """

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

    # ---- Document details block ----
    doc_details_lines = []
    if sender:
        doc_details_lines.append(f"Issuing Authority / Sender: {sender}")
    if recipient:
        doc_details_lines.append(f"Notice Recipient: {recipient}")
    doc_details_block = "\n".join(doc_details_lines) if doc_details_lines else "Not specified"

    # ---- Document summary block ----
    doc_summary_block = doc_summary.strip() if doc_summary else "Not available"

    # ---- All other issues block (for consistency awareness) ----
    other_issues = [iss for idx, iss in enumerate(all_issues) if idx != issue_number - 1]
    if other_issues:
        other_issues_block = "\n".join(
            f"{i + 1}. {iss}" for i, iss in enumerate(other_issues)
        )
    else:
        other_issues_block = "This is the only issue in the notice."

    return f"""You are preparing the reply for Issue {issue_number} of {total_issues} from a legal notice.

============================================================
DOCUMENT DETAILS
============================================================
{doc_details_block}

============================================================
DOCUMENT SUMMARY (factual context for this notice)
============================================================
{doc_summary_block}

============================================================
ALL OTHER ISSUES IN THIS NOTICE (for consistency)
============================================================
{other_issues_block}

============================================================
CURRENT ISSUE TO REPLY — Issue {issue_number} of {total_issues}
============================================================
{issue}

============================================================
INSTRUCTION
============================================================
{mode_instruction}

Your reply for this issue must:
1. Acknowledge the allegation precisely, referencing the actual facts from the document summary above.
2. Provide counter-arguments using the legal material below, grounded in the specific facts of this notice.
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
    all_issues: list,
    mode: str,
    store,
    all_chunks: list,
    recipient: str = None,
    sender: str = None,
    doc_summary: str = None,
    profile_summary: str = None,
) -> Tuple[int, str, list, dict]:
    """
    Full pipeline for one issue — runs in a thread pool.
    Now receives all_issues and doc_summary for full document context.
    Returns (issue_number, reply_text, sources, full_judgments).
    """
    try:
        logger.info(f"🔍 Processing Issue {issue_number}/{total_issues}: {issue[:80]}...")

        # Enriched retrieval — pass doc_summary to improve query targeting
        primary, supporting = _retrieve_and_rank_for_issue(
            issue, mode, store, all_chunks, doc_summary=doc_summary
        )

        system_prompt = get_system_prompt(profile_summary)
        user_prompt   = _build_issue_prompt(
            issue=issue,
            issue_number=issue_number,
            total_issues=total_issues,
            all_issues=all_issues,
            primary=primary,
            supporting=supporting,
            mode=mode,
            recipient=recipient,
            sender=sender,
            doc_summary=doc_summary,
            profile_summary=profile_summary,
        )

        reply = call_bedrock(
            prompt=user_prompt,
            system_prompts=[system_prompt],
            temperature=0.0
        )

        sources        = _format_sources(primary)
        full_judgments = get_full_judgments(primary, all_chunks)

        logger.info(
            f"✅ Issue {issue_number} reply ready "
            f"({len(reply)} chars, {len(sources)} sources, {len(full_judgments)} judgments)"
        )
        return issue_number, reply, sources, full_judgments

    except Exception as e:
        logger.error(f"❌ Issue {issue_number} failed: {e}", exc_info=True)
        return issue_number, f"[Error generating reply for Issue {issue_number}: {str(e)}]", [], {}


# ============================================================================
# STREAMING ORCHESTRATOR
# ============================================================================

async def process_issues_streaming(
    issues: list,
    mode: str,
    store,
    all_chunks: list,
    recipient: str = None,
    sender: str = None,
    doc_summary: str = None,
    profile_summary: str = None,
    max_parallel: int = 3,
) -> AsyncGenerator[Tuple[int, str, list, dict], None]:
    """
    Async generator — yields (issue_number, reply_text, sources, full_judgments) in strict order.

    Added parameter: doc_summary — passed through to each issue processor
    so the LLM has full document context when drafting each reply.

    All other behaviour (parallel processing, semaphore, ordered yielding) unchanged.
    """
    total     = len(issues)
    semaphore = asyncio.Semaphore(max_parallel)
    loop      = asyncio.get_event_loop()

    futures: Dict[int, asyncio.Future] = {
        i + 1: loop.create_future() for i in range(total)
    }

    async def bounded_process(issue, issue_number):
        async with semaphore:
            try:
                result = await run_in_threadpool(
                    _process_single_issue,
                    issue,
                    issue_number,
                    total,
                    issues,        # all_issues — full list for consistency
                    mode,
                    store,
                    all_chunks,
                    recipient,
                    sender,
                    doc_summary,   # document summary for factual grounding
                    profile_summary
                )
                futures[issue_number].set_result(result)
            except Exception as e:
                logger.error(f"Issue {issue_number} task failed: {e}", exc_info=True)
                futures[issue_number].set_result(
                    (issue_number, f"[Error generating reply for Issue {issue_number}: {str(e)}]", [], {})
                )

    logger.info(f"🚀 Processing {total} issues in parallel (max {max_parallel} concurrent)")

    tasks = [
        asyncio.create_task(bounded_process(issue, i + 1))
        for i, issue in enumerate(issues)
    ]

    for issue_num in range(1, total + 1):
        issue_number, reply, sources, full_judgments = await futures[issue_num]
        yield issue_number, reply, sources, full_judgments

    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"✅ All {total} issues processed and streamed")