import asyncio
import logging
from typing import List, Dict, Tuple, Optional, AsyncGenerator
from starlette.concurrency import run_in_threadpool

from services.retrieval.hybrid import retrieve
from services.llm.bedrock_client import call_bedrock
from services.chat.prompt_builder import get_system_prompt
from services.chat.engine import get_full_judgments

logger = logging.getLogger(__name__)

MODE_DEFENSIVE = "defensive"
MODE_IN_FAVOUR = "in_favour"

CHUNK_TYPE_SCORES = {"judgment": 40, "draft_reply": 35}
LEGAL_SOURCE_TYPES = {"notification", "circular", "act", "rule", "section"}
LEGAL_SOURCE_SCORE  = 25
DEFAULT_SCORE       = 10

DECISION_BONUS = {
    MODE_DEFENSIVE: {"In favour of assessee": +30, "In favour of revenue": -20},
    MODE_IN_FAVOUR: {"In favour of revenue":  +30, "In favour of assessee": -20},
}

DEFENSIVE_TEMPLATE = (
    "Under what conditions or exceptions is a taxpayer not required to {issue} "
    "and what relief or protection is available to the assessee in such cases"
)
IN_FAVOUR_TEMPLATE = (
    "Under what conditions is a taxpayer strictly liable for {issue} "
    "and when is non-compliance or non-payment not excusable under GST law"
)


def build_retrieval_query(issue: str, mode: str, doc_summary: str = None) -> str:
    template   = DEFENSIVE_TEMPLATE if mode == MODE_DEFENSIVE else IN_FAVOUR_TEMPLATE
    base_query = template.format(issue=issue)
    if doc_summary:
        return f"{base_query}. Context: {doc_summary[:300].strip()}"
    return base_query


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
        return MODE_IN_FAVOUR
    return MODE_DEFENSIVE


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
        decision       = chunk.get("metadata", {}).get("decision", "")
        decision_bonus = DECISION_BONUS.get(mode, {}).get(decision, 0)

    similarity_score = min(float(chunk.get("_score", 0.5)) * 20, 20)
    return base + decision_bonus + similarity_score


def _format_sources(chunks: list) -> list:
    return [
        {"id": c.get("id", ""), "chunk_type": c.get("chunk_type", ""),
         "text": c.get("text", ""), "metadata": c.get("metadata", {})}
        for c in chunks
    ]


def get_sibling_chunks(retrieved_chunks: list, all_chunks: list) -> list:
    sibling_keys = set()
    for chunk in retrieved_chunks:
        meta    = chunk.get("metadata", {})
        section = meta.get("section_number") or meta.get("section")
        source  = meta.get("source") or meta.get("source_file")
        if section: sibling_keys.add(("section", section))
        if source:  sibling_keys.add(("source",  source))

    existing_ids = {id(c) for c in retrieved_chunks}
    siblings     = []
    for chunk in all_chunks:
        if id(chunk) in existing_ids:
            continue
        meta    = chunk.get("metadata", {})
        section = meta.get("section_number") or meta.get("section")
        source  = meta.get("source") or meta.get("source_file")
        if (section and ("section", section) in sibling_keys) or \
           (source  and ("source",  source)  in sibling_keys):
            c           = dict(chunk)
            c["_score"] = 0.4
            siblings.append(c)
    return siblings


def _retrieve_and_rank_for_issue(
    issue: str, mode: str, store, all_chunks: list, doc_summary: str = None
) -> Tuple[list, list]:
    reframed = build_retrieval_query(issue, mode, doc_summary)
    pass1    = retrieve(query=reframed, vector_store=store, all_chunks=all_chunks, k=15)
    pass2    = retrieve(query=issue,    vector_store=store, all_chunks=all_chunks, k=15)
    siblings = get_sibling_chunks(pass2, all_chunks)

    seen, all_retrieved = set(), []
    for chunk in pass1 + pass2 + siblings:
        key = chunk.get("id") or chunk.get("text", "")[:120]
        if key not in seen:
            seen.add(key)
            all_retrieved.append(chunk)

    for chunk in all_retrieved:
        chunk["_final_score"] = score_chunk(chunk, mode)
    all_retrieved.sort(key=lambda x: x["_final_score"], reverse=True)
    return all_retrieved[:15], all_retrieved[15:25]


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
    reference_docs_text: str = None,
) -> str:

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
            "Prepare a strong defensive reply protecting the notice recipient's position.\n\n"
            "- Find every applicable legal exception, proviso, condition, and precedent in the recipient's favour.\n"
            "- Identify the specific condition or exception within the same provision under which the recipient's action was legally permissible.\n"
            "- Prioritise judgments where the decision was 'In favour of assessee'.\n"
            "- Quote statutory wording, circular text, notification language, and judgment extracts verbatim.\n"
            "- Ground every argument in a specific section, sub-section, proviso, or clause.\n"
            "- Conclude by establishing that the allegation is not legally sustainable."
        )
    else:
        mode_instruction = (
            "Prepare a strong reply establishing the legal basis for the allegation.\n\n"
            "- Find every applicable provision, condition, and precedent supporting the revenue's position.\n"
            "- Identify the specific section, proviso, or clause under which non-compliance occurred.\n"
            "- Prioritise judgments where the decision was 'In favour of revenue'.\n"
            "- Quote statutory wording, circular text, notification language, and judgment extracts verbatim.\n"
            "- Ground every argument in a specific section, sub-section, proviso, or clause.\n"
            "- Conclude by establishing that the obligation squarely applies and the allegation is sustainable."
        )

    doc_details = "\n".join(filter(None, [
        f"Issuing Authority / Sender: {sender}"   if sender    else "",
        f"Notice Recipient: {recipient}"           if recipient else "",
    ])) or "Not specified"

    ref_block = ""
    if reference_docs_text and reference_docs_text.strip():
        ref_block = f"""
============================================================
REFERENCE DOCUMENTS (provided by user to strengthen the reply)
============================================================
{reference_docs_text.strip()}

"""

    other_issues = [iss for idx, iss in enumerate(all_issues) if idx != issue_number - 1]
    other_block  = (
        "\n".join(f"{i+1}. {iss}" for i, iss in enumerate(other_issues))
        if other_issues else "This is the only issue."
    )

    return f"""You are preparing the reply for Issue {issue_number} of {total_issues} from a legal notice.

============================================================
DOCUMENT DETAILS
============================================================
{doc_details}

============================================================
DOCUMENT SUMMARY (factual context)
============================================================
{doc_summary.strip() if doc_summary else "Not available"}
{ref_block}
============================================================
ALL OTHER ISSUES IN THIS NOTICE (for consistency)
============================================================
{other_block}

============================================================
CURRENT ISSUE — Issue {issue_number} of {total_issues}
============================================================
{issue}

============================================================
INSTRUCTION
============================================================
{mode_instruction}

Your reply must:
1. Acknowledge the allegation precisely using facts from the document summary.
2. Provide counter-arguments grounded in the specific facts of this notice.
3. Cite specific sections, provisos, notifications, circulars, or judgments.
4. For judgments, state the decision and apply the ratio to this issue.
5. Conclude with a clear statement on why this issue should be decided in the client's favour.

PRIMARY LEGAL MATERIAL:
{render(primary)}

SUPPORTING LEGAL MATERIAL (use only if it adds real value):
{render(supporting)}

Write the reply for Issue {issue_number} only. Be precise, professional, and legally grounded.
Do NOT add closing statement, signature block, or date.
"""


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
    reference_docs_text: str = None,
) -> Tuple[int, str, list, dict, dict]:
    try:
        logger.info(f"🔍 Processing Issue {issue_number}/{total_issues}: {issue[:80]}...")
        primary, supporting = _retrieve_and_rank_for_issue(issue, mode, store, all_chunks, doc_summary)
        system_prompt = get_system_prompt(profile_summary)
        user_prompt   = _build_issue_prompt(
            issue, issue_number, total_issues, all_issues,
            primary, supporting, mode, recipient, sender,
            doc_summary, profile_summary, reference_docs_text,
        )
        reply, usage   = call_bedrock(prompt=user_prompt, system_prompts=[system_prompt], temperature=0.0)
        sources        = _format_sources(primary)
        full_judgments = get_full_judgments(primary, all_chunks)
        logger.info(f"✅ Issue {issue_number} ready ({len(reply)} chars)")
        return issue_number, reply, sources, full_judgments, usage
    except Exception as e:
        logger.error(f"❌ Issue {issue_number} failed: {e}", exc_info=True)
        return (issue_number, f"[Error for Issue {issue_number}: {str(e)}]",
                [], {}, {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0})


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
    reference_docs_text: str = None,
) -> AsyncGenerator[Tuple[int, str, list, dict, dict], None]:
    """
    Async generator — yields (issue_number, reply, sources, full_judgments, usage)
    in strict order (1, 2, 3…) regardless of which finishes first.
    """
    total     = len(issues)
    semaphore = asyncio.Semaphore(max_parallel)
    loop      = asyncio.get_event_loop()
    futures: Dict[int, asyncio.Future] = {i + 1: loop.create_future() for i in range(total)}

    async def bounded_process(issue, issue_number):
        async with semaphore:
            try:
                result = await run_in_threadpool(
                    _process_single_issue,
                    issue, issue_number, total, issues,
                    mode, store, all_chunks,
                    recipient, sender, doc_summary, profile_summary, reference_docs_text,
                )
                futures[issue_number].set_result(result)
            except Exception as e:
                logger.error(f"Issue {issue_number} task failed: {e}", exc_info=True)
                futures[issue_number].set_result((
                    issue_number, f"[Error for Issue {issue_number}: {str(e)}]",
                    [], {}, {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}
                ))

    logger.info(f"🚀 Processing {total} issues (max {max_parallel} concurrent)")
    tasks = [asyncio.create_task(bounded_process(issue, i + 1)) for i, issue in enumerate(issues)]

    for issue_num in range(1, total + 1):
        yield await futures[issue_num]

    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"✅ All {total} issues processed")