"""
services/document/issue_replier.py
Generates draft replies for each extracted issue.
Uses BedrockLLMClient + RetrievalPipeline.

Retrieval priority for draft replies:
    judgment, cgst_rule, igst_rule, notification, circular,
    cgst_section, igst_section, gstat_rule, gst_form
"""

import asyncio
import logging
import threading
from typing import AsyncGenerator, Dict, List, Optional, Tuple

from starlette.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)

MODE_DEFENSIVE = "defensive"
MODE_IN_FAVOUR = "in_favour"

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


# ── LLM client (lazy, thread-safe) ───────────────────────────────────────────

_llm = None
_llm_lock = threading.Lock()


def _get_llm():
    global _llm
    if _llm is None:
        with _llm_lock:
            if _llm is None:
                from retrieval.bedrock_llm import BedrockLLMClient
                _llm = BedrockLLMClient()
    return _llm


# ── Pipeline reference (injected at startup) ──────────────────────────────────

_pipeline_ref = None
_pipeline_lock = threading.Lock()


def _get_pipeline():
    return _pipeline_ref


def set_pipeline(pipeline):
    """Called once from main.py startup — injects the shared pipeline."""
    global _pipeline_ref
    with _pipeline_lock:
        _pipeline_ref = pipeline


# ── Retrieval for draft replies ───────────────────────────────────────────────

def _retrieve_for_issue(issue: str, mode: str, doc_summary: str = None) -> List[dict]:
    """
    Use the RetrievalPipeline (stages 1-5 only) to find relevant legal material
    for this issue. Returns a list of chunk payloads (plain dicts).
    """
    pipeline = _get_pipeline()
    if pipeline is None:
        logger.warning("Pipeline not available for issue retrieval")
        return []

    if mode == MODE_DEFENSIVE:
        query = (
            f"legal exceptions and relief available to assessee regarding {issue}. "
            f"Judgments in favour of assessee. Sections, rules, notifications."
        )
    else:
        query = (
            f"legal basis establishing taxpayer liability for {issue}. "
            f"Judgments in favour of revenue. Sections, rules, notifications."
        )

    if doc_summary:
        query += f" Context: {doc_summary[:200]}"

    try:
        staged = pipeline.query_stages_1_to_5(query, [])
        # staged = (final_query, history, chunks, citation_result, intent, cross_refs)
        chunks = staged[2]  # ScoredChunk list
        return [c.payload for c in chunks[:15]]
    except Exception as e:
        logger.error(f"Issue retrieval failed: {e}")
        return []


# ── Prompt builder ────────────────────────────────────────────────────────────

def _render_chunks(chunks: list) -> str:
    parts = []
    for c in chunks:
        chunk_type = c.get("chunk_type", "source").upper()
        ext        = c.get("ext") or {}
        source     = (
            ext.get("citation") or
            ext.get("notification_number") or
            ext.get("circular_number") or
            ext.get("rule_number_full") or
            c.get("parent_doc", "source")
        )
        parts.append(f"[{chunk_type} | {source}]\n{c.get('text', '')}")
    return "\n\n".join(parts)


def _build_issue_prompt(
    issue: str,
    issue_number: int,
    total_issues: int,
    all_issues: list,
    chunks: list,
    mode: str,
    recipient: str = None,
    sender: str = None,
    doc_summary: str = None,
    reference_docs_text: str = None,
) -> str:

    if mode == MODE_DEFENSIVE:
        mode_instruction = (
            "Prepare a strong defensive reply protecting the notice recipient.\n"
            "- Find every applicable legal exception, proviso, and precedent in the recipient's favour.\n"
            "- Prioritise judgments decided in favour of the assessee.\n"
            "- Quote statutory wording, notifications, and judgment extracts precisely.\n"
            "- Ground every argument in a specific section, sub-section, proviso, or clause.\n"
            "- Conclude by establishing that the allegation is not legally sustainable."
        )
    else:
        mode_instruction = (
            "Prepare a reply establishing the legal basis for the allegation.\n"
            "- Find every provision and precedent supporting the revenue's position.\n"
            "- Prioritise judgments decided in favour of revenue.\n"
            "- Quote statutory wording, notifications, and judgment extracts precisely.\n"
            "- Ground every argument in a specific section, sub-section, proviso, or clause.\n"
            "- Conclude by establishing that the obligation applies and the allegation is sustainable."
        )

    doc_details = "\n".join(filter(None, [
        f"Issuing Authority / Sender: {sender}"   if sender    else "",
        f"Notice Recipient: {recipient}"           if recipient else "",
    ])) or "Not specified"

    ref_block = ""
    if reference_docs_text and reference_docs_text.strip():
        ref_block = (
            "\n============================================================\n"
            "REFERENCE DOCUMENTS\n"
            "============================================================\n"
            + reference_docs_text.strip() + "\n"
        )

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
DOCUMENT SUMMARY
============================================================
{doc_summary.strip() if doc_summary else "Not available"}
{ref_block}
============================================================
OTHER ISSUES IN THIS NOTICE (for consistency)
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
4. For judgments — state the decision and apply the ratio to this issue.
5. Conclude with a clear statement on why this issue should be decided in the client's favour.

LEGAL MATERIAL (judgments, sections, rules, notifications, circulars):
{_render_chunks(chunks)}

Write the reply for Issue {issue_number} only. Professional, precise, legally grounded.
Do NOT add closing statement, signature block, or date.
"""


_DOC_SYSTEM = (
    "You are a senior Indian tax law professional preparing formal legal draft replies "
    "to GST notices, show cause notices, and orders. "
    "Your replies must be legally precise, cite exact provisions, and be professionally worded."
)


# ── Core single-issue processor ───────────────────────────────────────────────

def _process_single_issue(
    issue: str,
    issue_number: int,
    total_issues: int,
    all_issues: list,
    mode: str,
    recipient: str = None,
    sender: str = None,
    doc_summary: str = None,
    reference_docs_text: str = None,
) -> Tuple[int, str, list, dict]:
    """
    Retrieve legal material + generate LLM reply for one issue.
    Returns: (issue_number, reply_text, sources_list, usage_dict)
    Runs in a thread (called via run_in_threadpool).
    """
    try:
        logger.info(f"Processing Issue {issue_number}/{total_issues}: {issue[:80]}...")

        chunks = _retrieve_for_issue(issue, mode, doc_summary)

        prompt = _build_issue_prompt(
            issue, issue_number, total_issues, all_issues,
            chunks, mode, recipient, sender, doc_summary, reference_docs_text,
        )

        reply = _get_llm().call(
            system_prompt=_DOC_SYSTEM,
            user_message=prompt,
            max_tokens=2048,
            temperature=0.0,
            label=f"issue_{issue_number}",
        )
        if not reply:
            reply = f"[Could not generate reply for Issue {issue_number}]"

        # Build sources list for frontend.
        # Qdrant payloads store the original chunk id as "_chunk_id" (set in
        # qdrant_manager.build_point). The Qdrant point id is a UUID5 and lives
        # on the point, not in the payload — so c.get("id") would always be empty.
        sources = [
            {
                "id":         c.get("_chunk_id", ""),   # ← fixed: was c.get("id", "")
                "chunk_type": c.get("chunk_type", ""),
                "text":       (c.get("text") or "")[:300],
                "metadata":   c.get("ext") or {},
            }
            for c in chunks
        ]

        usage = {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}
        logger.info(f"Issue {issue_number} done ({len(reply)} chars)")
        return issue_number, reply, sources, usage

    except Exception as e:
        logger.error(f"Issue {issue_number} failed: {e}", exc_info=True)
        return (
            issue_number,
            f"[Error for Issue {issue_number}: {str(e)}]",
            [],
            {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0},
        )


# ── Async streaming generator ─────────────────────────────────────────────────

async def process_issues_streaming(
    issues: list,
    mode: str,
    recipient: str = None,
    sender: str = None,
    doc_summary: str = None,
    reference_docs_text: str = None,
    max_parallel: int = 3,
) -> AsyncGenerator[Tuple[int, str, list, dict], None]:
    """
    Async generator — yields (issue_number, reply, sources, usage)
    in strict order (1, 2, 3…) regardless of which finishes first.
    All issues up to max_parallel run concurrently in the threadpool.
    """
    total     = len(issues)
    semaphore = asyncio.Semaphore(max_parallel)

    # Use get_running_loop() — get_event_loop() is deprecated since Python 3.10
    loop = asyncio.get_running_loop()

    futures: Dict[int, asyncio.Future] = {
        i + 1: loop.create_future() for i in range(total)
    }

    async def bounded_process(issue, issue_number):
        async with semaphore:
            try:
                result = await run_in_threadpool(
                    _process_single_issue,
                    issue, issue_number, total, issues,
                    mode, recipient, sender, doc_summary, reference_docs_text,
                )
                futures[issue_number].set_result(result)
            except Exception as e:
                logger.error(f"Issue {issue_number} task failed: {e}", exc_info=True)
                futures[issue_number].set_result((
                    issue_number,
                    f"[Error for Issue {issue_number}: {str(e)}]",
                    [],
                    {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0},
                ))

    logger.info(f"Processing {total} issues (max {max_parallel} concurrent)")
    tasks = [
        asyncio.create_task(bounded_process(issue, i + 1))
        for i, issue in enumerate(issues)
    ]

    # Yield results in strict sequential order
    for issue_num in range(1, total + 1):
        yield await futures[issue_num]

    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"All {total} issues processed")