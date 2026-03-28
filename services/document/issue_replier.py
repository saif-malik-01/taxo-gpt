"""
services/document/issue_replier.py

Generates draft replies for each extracted issue using:
  1. Retrieval pipeline (stages 1-5) — finds relevant legal material
  2. Mode-based reranking — boosts/demotes judgment chunks by decision type
  3. LLM generation — drafts a precise, legally grounded reply

Retrieval strategy per issue:
  - The issue text (verbatim, with section/rule/notification references) is sent
    to query_stages_1_to_5. Stage 2A regex extraction picks up section numbers,
    rule numbers, notification/circular numbers from the issue text automatically.
    Stage 4 then does direct field-match scrolls (scroll1/scroll2) for those.
  - No keyword-based filtering — pipeline handles everything.
  - After retrieval, apply mode-based reranking on judgment chunks.

Mode-based reranking:
  defensive: in_favour_of_assessee judgments → score × 1.4
             in_favour_of_revenue judgments  → score × 0.5 (kept, ratio useful)
  in_favour: in_favour_of_revenue judgments  → score × 1.4
             in_favour_of_assessee judgments → score × 0.5

Reference documents (from session_doc_store) are included in the reply prompt.
Previous reply documents (replied_issues from doc_classifier) are ALSO included
so the LLM maintains consistency with positions already taken.
"""

import asyncio
import logging
import threading
from typing import AsyncGenerator, Dict, List, Optional, Tuple

from starlette.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)

MODE_DEFENSIVE = "defensive"
MODE_IN_FAVOUR = "in_favour"

# ── LLM client (lazy, thread-safe) ───────────────────────────────────────────

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


# ── Pipeline reference (injected at startup) ──────────────────────────────────

_pipeline_ref  = None
_pipeline_lock = threading.Lock()


def _get_pipeline():
    return _pipeline_ref


def set_pipeline(pipeline):
    global _pipeline_ref
    with _pipeline_lock:
        _pipeline_ref = pipeline


# ── Mode-based reranking ──────────────────────────────────────────────────────

def _rerank_chunks_for_mode(chunks: list, mode: str) -> list:
    """
    Rerank retrieved chunks based on the reply mode.

    For DEFENSIVE mode:
      Boost judgments decided in favour of assessee/taxpayer.
      Demote (but keep) judgments in favour of revenue — ratio decidendi
      of adverse judgments can still help distinguish the current facts.

    For IN_FAVOUR mode:
      Boost judgments decided in favour of revenue/department.
      Demote judgments in favour of assessee.

    Non-judgment chunks (sections, rules, notifications, circulars) are
    NOT reranked — they are equally relevant regardless of mode.
    """
    if not chunks or mode not in (MODE_DEFENSIVE, MODE_IN_FAVOUR):
        return chunks

    reranked = []
    for chunk in chunks:
        score = getattr(chunk, "score", chunk.get("score", 0) if isinstance(chunk, dict) else 0)
        payload = chunk.payload if hasattr(chunk, "payload") else chunk
        chunk_type = payload.get("chunk_type", "")

        if chunk_type == "judgment":
            decision = (payload.get("ext") or {}).get("decision", "")

            if mode == MODE_DEFENSIVE:
                if "in_favour_of_assessee" in decision:
                    score = score * 1.4
                elif "in_favour_of_revenue" in decision:
                    score = score * 0.5

            elif mode == MODE_IN_FAVOUR:
                if "in_favour_of_revenue" in decision:
                    score = score * 1.4
                elif "in_favour_of_assessee" in decision:
                    score = score * 0.5

        # Store adjusted score back
        if hasattr(chunk, "score"):
            chunk.score = score
        else:
            chunk["score"] = score
        reranked.append(chunk)

    return sorted(reranked, key=lambda c: (
        c.score if hasattr(c, "score") else c.get("score", 0)
    ), reverse=True)


# ── Retrieval for a single issue ──────────────────────────────────────────────

def _retrieve_for_issue(
    issue: str,
    mode: str,
    doc_summary: str = None,
) -> List[dict]:
    """
    Use the RetrievalPipeline (stages 1-5) to find legal material for this issue.

    Query construction:
      - Issue text is passed verbatim so regex extraction (Stage 2A) picks up
        any section/rule/notification numbers embedded in the issue text.
      - Stage 4 scroll1 then does direct field-match searches for those refs.
      - Mode-based framing steers the vector search toward relevant case law.

    After retrieval, apply mode-based reranking on judgment chunks.
    Returns list of chunk payloads (plain dicts), top 15 after reranking.
    """
    pipeline = _get_pipeline()
    if pipeline is None:
        logger.warning("Pipeline not available for issue retrieval")
        return []

    if mode == MODE_DEFENSIVE:
        query = (
            f"Legal exceptions, defences, and relief available to the assessee "
            f"regarding: {issue}. "
            f"Judgments in favour of assessee. Provisos and exceptions in sections. "
            f"Circulars and notifications granting relief or clarification."
        )
    else:
        query = (
            f"Legal basis and provisions establishing taxpayer liability regarding: {issue}. "
            f"Judgments in favour of revenue/department. "
            f"Sections, rules, and notifications confirming taxability or compliance obligation."
        )

    if doc_summary:
        query += f" Context: {doc_summary[:300]}"

    try:
        staged = pipeline.query_stages_1_to_5(query, [])
        # staged = (final_query, history, chunks, citation_result, intent, cross_refs)
        chunks = staged[2]  # ScoredChunk list

        # Apply mode-based reranking
        reranked = _rerank_chunks_for_mode(chunks, mode)

        return [c.payload for c in reranked[:15]]
    except Exception as e:
        logger.error(f"Issue retrieval failed: {e}")
        return []


# ── Chunk renderer for LLM prompt ────────────────────────────────────────────

def _render_chunks(chunks: list) -> str:
    parts = []
    for c in chunks:
        chunk_type = c.get("chunk_type", "source").upper()
        ext        = c.get("ext") or {}
        source     = (
            ext.get("citation")            or
            ext.get("notification_number") or
            ext.get("circular_number")     or
            ext.get("rule_number_full")    or
            ext.get("section_number")      or
            c.get("parent_doc", "source")
        )
        decision = ext.get("decision", "")
        decision_tag = f" [{decision.replace('_', ' ')}]" if decision else ""
        parts.append(
            f"[{chunk_type} | {source}{decision_tag}]\n{c.get('text', '')}"
        )
    return "\n\n".join(parts)


# ── Prompt builder ────────────────────────────────────────────────────────────

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
    previous_replies_text: str = None,
) -> str:

    if mode == MODE_DEFENSIVE:
        mode_instruction = (
            "Prepare a strong defensive reply protecting the notice recipient.\n"
            "- Find every applicable legal exception, proviso, and precedent in the recipient's favour.\n"
            "- Prioritise judgments decided in favour of the assessee.\n"
            "- Quote statutory wording, notifications, and judgment extracts precisely.\n"
            "- Ground every argument in a specific section, sub-section, proviso, or clause.\n"
            "- Address any adverse judgments by distinguishing the facts.\n"
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

    prev_reply_block = ""
    if previous_replies_text and previous_replies_text.strip():
        prev_reply_block = (
            "\n============================================================\n"
            "PREVIOUS REPLIES (for related/older notices in this case)\n"
            "IMPORTANT: Maintain consistency with positions already taken.\n"
            "Do NOT contradict established facts. You may extend arguments.\n"
            "============================================================\n"
            + previous_replies_text.strip() + "\n"
        )

    other_issues = [iss for idx, iss in enumerate(all_issues) if idx != issue_number - 1]
    other_block  = (
        "\n".join(f"{i+1}. {iss}" for i, iss in enumerate(other_issues))
        if other_issues else "This is the only issue."
    )

    return f"""You are preparing the reply for Issue {issue_number} of {total_issues}.

============================================================
DOCUMENT DETAILS
============================================================
{doc_details}

============================================================
DOCUMENT SUMMARY
============================================================
{doc_summary.strip() if doc_summary else "Not available"}
{ref_block}{prev_reply_block}
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
5. Address any conditions mentioned in rules/notifications that affect applicability.
6. Conclude clearly on why this issue should be decided in the client's favour.

LEGAL MATERIAL (judgments, sections, rules, notifications, circulars):
{_render_chunks(chunks) or "No specific legal material retrieved."}

Write the reply for Issue {issue_number} only. Professional, precise, legally grounded.
Do NOT add closing statement, signature block, or date — those are added separately.
"""


_DOC_SYSTEM = (
    "You are a senior Indian tax law professional preparing formal legal draft replies "
    "to GST notices, show cause notices, and orders. "
    "Your replies must be legally precise, cite exact provisions, and be professionally worded. "
    "Conditions in rules, notifications, and circulars that determine applicability "
    "must be identified and used to strengthen the reply."
)


# ── Previous replies text builder ─────────────────────────────────────────────

def _build_previous_replies_text(case: dict) -> str:
    """
    Build a text block of previous replies from uploaded reply documents.
    These are the replied_issues pairs stored in doc entries.
    """
    parts = []
    for doc in (case.get("documents") or []):
        replied_issues = doc.get("replied_issues") or []
        if not replied_issues:
            continue
        doc_type = doc.get("legal_doc_type", "previous reply")
        parts.append(f"[{doc_type.upper()} — Previously submitted reply]")
        for pair in replied_issues:
            parts.append(f"Issue: {pair.get('issue_text', '')}")
            parts.append(f"Reply given: {pair.get('reply_text', '')[:1000]}")
            parts.append("")
    return "\n".join(parts)


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
    previous_replies_text: str = None,
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
            chunks, mode, recipient, sender, doc_summary,
            reference_docs_text, previous_replies_text,
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

        sources = [
            {
                "id":         c.get("_chunk_id", ""),
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
    previous_replies_text: str = None,
    max_parallel: int = 3,
) -> AsyncGenerator[Tuple[int, str, list, dict], None]:
    """
    Async generator — yields (issue_number, reply, sources, usage)
    in strict sequential order (1, 2, 3…) regardless of completion order.
    Up to max_parallel issues run concurrently in threadpool.
    """
    total     = len(issues)
    semaphore = asyncio.Semaphore(max_parallel)
    loop      = asyncio.get_running_loop()
    futures: Dict[int, asyncio.Future] = {
        i + 1: loop.create_future() for i in range(total)
    }

    async def bounded_process(issue, issue_number):
        async with semaphore:
            try:
                result = await run_in_threadpool(
                    _process_single_issue,
                    issue, issue_number, total, issues,
                    mode, recipient, sender, doc_summary,
                    reference_docs_text, previous_replies_text,
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

    for issue_num in range(1, total + 1):
        yield await futures[issue_num]

    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"All {total} issues processed")