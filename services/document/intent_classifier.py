"""
services/document/intent_classifier.py

Intent classification paths:

  classify_intent_no_docs(question, snapshot)
    → Used when NO files are uploaded. Full intent classification using
      snapshot state + user message.

  classify_intent_with_docs() is REMOVED — intent is now extracted inside
  the combined 2A+2C call in doc_classifier.analyze_document(). The intent
  fields (intent, mode, issue_numbers, case_id) come back in the same JSON
  as the document metadata — one Qwen call does both.

  rewrite_query_if_needed(question, history, snapshot)
    → Resolves referential language in text-only requests.

  parse_issue_update(message, current_issues)
    → Parses Case 5 issue list corrections.
"""

import json
import logging
import re
import threading
from typing import List, Optional

logger = logging.getLogger(__name__)

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


def _parse_result(raw: Optional[str], fallback: dict) -> dict:
    if not raw:
        return fallback
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        text = m.group()
    try:
        parsed = json.loads(text)
        logger.info(
            "Intent → %s | mode=%s | issues=%s",
            parsed.get("intent"), parsed.get("mode"), parsed.get("issue_numbers"),
        )
        return parsed
    except json.JSONDecodeError as e:
        logger.error(f"Intent JSON parse error: {e}\nRaw: {raw[:200]}")
        return fallback


_SYSTEM = "You are an intent classifier for a legal document assistant. Return ONLY valid JSON."

# ── Intent option descriptions (used in both prompts) ─────────────────────────

_INTENT_OPTIONS = """
Intent options:
  summarize       — user wants to see/review the document summary and issues list
  draft_all       — generate replies for ALL pending issues
  draft_specific  — generate reply for SPECIFIC issue numbers mentioned
  draft_direct    — user explicitly says "prepare reply / draft reply / reply to this notice"
                    and mode is clear from context or message
  confirm_mode    — user is confirming defensive or in-favour mode
                    defensive signals: "defence", "defensive", "protect", "assessee side",
                                       "taxpayer side", "yes" when state=awaiting_mode
                    in_favour signals: "in favour", "department side", "revenue is right",
                                       "support the notice"
  update_issues   — user wants to merge/add/correct/remove issues, or says issues are missed
  update_reply    — user wants to change the reply for a specific issue
  query_document  — question answerable from the uploaded document content
  query_general   — pure GST/tax knowledge question, no document context needed
  switch_case     — user wants to work on a previously archived case (different case_id)
  new_case        — user explicitly wants to start fresh for a completely different matter
  mark_replied    — user says a document or issue has already been replied to externally
"""

_MODE_RULE = """
mode: "defensive" | "in_favour" | null
  defensive = protecting the notice recipient / assessee / taxpayer
  in_favour = supporting the notice / department / revenue position
  null = not determinable from this message
"""


# ── Path 1: No files uploaded ─────────────────────────────────────────────────

def classify_intent_no_docs(
    question: str,
    snapshot: dict,
) -> dict:
    """
    Intent classification when no files are uploaded in this request.
    The question may be a follow-up, correction, confirmation, or new question.
    """
    from services.document.doc_context import get_active_case, get_user_context_text

    active_case = get_active_case(snapshot)
    fallback = {"intent": "query_general", "mode": None, "issue_numbers": [], "case_id": None}

    if not question or not question.strip():
        return {"intent": "summarize", "mode": None, "issue_numbers": [], "case_id": None}

    # Build session context for LLM
    case_state     = None
    current_mode   = None
    has_issues     = False
    has_pending    = False
    issues_preview = ""
    user_ctx       = ""
    cases_preview  = ""

    if active_case:
        case_state   = active_case.get("state")
        current_mode = active_case.get("mode")
        issues       = active_case.get("issues", [])
        has_issues   = bool(issues)
        has_pending  = any(
            not i.get("reply") and i.get("status") not in ("replied", "has_reply_doc")
            for i in issues
        )
        if issues:
            lines = [
                f"  {i['id']}. [{i.get('status','pending')}] {i['text'][:80]}"
                for i in issues[:8]
            ]
            issues_preview = "\n".join(lines)
        user_ctx = get_user_context_text(active_case, limit=3)

    # List archived cases for switch_case intent
    archived = [
        c for c in snapshot.get("cases", [])
        if c.get("status") == "archived"
    ]
    if archived:
        cases_preview = "Archived cases: " + ", ".join(
            f"Case {c['case_id']} ({(c.get('parties') or {}).get('sender','?')} / "
            f"{(c.get('parties') or {}).get('recipient','?')})"
            for c in archived[:3]
        )

    prompt = f"""Classify the user's intent for this legal document session.

SESSION STATE:
  Active case: {"yes" if active_case else "no"}
  Current state: {case_state or "none"}
  Current mode: {current_mode or "not set"}
  Issues exist: {has_issues}
  Pending (unreplied) issues: {has_pending}
  User's previous instructions:
{user_ctx or "  (none)"}
  Issues (id, status, text):
{issues_preview or "  (none)"}
{cases_preview}

USER MESSAGE: "{question}"

{_INTENT_OPTIONS}
{_MODE_RULE}

Return ONLY:
{{
  "intent": "...",
  "mode": null,
  "issue_numbers": [],
  "case_id": null
}}"""

    raw = _get_llm().call(
        system_prompt=_SYSTEM,
        user_message=prompt,
        max_tokens=256,
        temperature=0.0,
        label="intent_no_docs",
    )
    return _parse_result(raw, fallback)


# ── Path 2: Files were uploaded ───────────────────────────────────────────────

def classify_intent_with_docs(
    question: str,
    doc_analyses: List[dict],
    snapshot: dict,
) -> dict:
    """
    Intent classification after per-doc metadata has been extracted.
    The LLM knows what documents were uploaded and what the user said.

    doc_analyses: list of dicts with keys:
      filename (display only), legal_doc_type, is_primary, is_previous_reply,
      same_case, parties, reference_number, date, brief_summary
    """
    from services.document.doc_context import get_active_case, get_user_context_text

    active_case = get_active_case(snapshot)
    fallback    = {"intent": "summarize", "mode": None, "issue_numbers": [], "case_id": None}

    # Summarise what was uploaded (no filename to LLM — use content description)
    uploaded_summary_lines = []
    for i, da in enumerate(doc_analyses, 1):
        role = "primary notice/order" if da.get("is_primary") else (
               "previous reply doc"  if da.get("is_previous_reply") else "reference material"
        )
        parties = da.get("parties") or {}
        uploaded_summary_lines.append(
            f"  Doc {i}: {da.get('legal_doc_type','unknown')} ({role}) | "
            f"from {parties.get('sender') or '?'} to {parties.get('recipient') or '?'} | "
            f"ref: {da.get('reference_number') or 'none'} | "
            f"date: {da.get('date') or 'unknown'} | "
            f"summary: {(da.get('brief_summary') or '')[:120]}"
        )
    uploaded_desc = "\n".join(uploaded_summary_lines) or "  (no documents analysed)"

    user_ctx = get_user_context_text(active_case, limit=3) if active_case else ""

    case_state   = (active_case or {}).get("state")
    current_mode = (active_case or {}).get("mode")

    prompt = f"""Classify the user's intent after they uploaded document(s).

USER MESSAGE: "{question or '(no message — documents uploaded only)'}"

UPLOADED DOCUMENTS:
{uploaded_desc}

SESSION STATE:
  Active case: {"yes" if active_case else "no"}
  Current state: {case_state or "none"}
  Current mode: {current_mode or "not set"}
  User's previous instructions:
{user_ctx or "  (none)"}

{_INTENT_OPTIONS}
{_MODE_RULE}

NOTE: When documents are uploaded and user says "prepare reply" / "draft reply" /
"reply to this" / "defend against this" → use "draft_direct".
When user just uploads without any draft intent → use "summarize".

Return ONLY:
{{
  "intent": "...",
  "mode": null,
  "issue_numbers": [],
  "case_id": null
}}"""

    raw = _get_llm().call(
        system_prompt=_SYSTEM,
        user_message=prompt,
        max_tokens=256,
        temperature=0.0,
        label="intent_with_docs",
    )
    return _parse_result(raw, fallback)


# ── Issue update parser ───────────────────────────────────────────────────────

def parse_issue_update(message: str, current_issues: list) -> dict:
    """
    Parse a user instruction about changing the issues list.
    Returns: {action, issue_ids, new_text, merge_text}
    """
    issue_lines = [f"{i['id']}. {i['text'][:100]}" for i in current_issues]
    issues_text = "\n".join(issue_lines)

    prompt = f"""Parse this instruction about a legal issues list.

USER INSTRUCTION: "{message}"

CURRENT ISSUES:
{issues_text}

Determine:
  action     : merge | add | correct | remove | reextract
               Use "reextract" when user says issues are missed but does NOT provide specific text.
               Use "add" when user provides specific missing issue text.
  issue_ids  : list of issue IDs involved (integers); empty for reextract/add-new.
  new_text   : verbatim text from user for add or correct; null otherwise.
  merge_text : combined text for merged issue; null otherwise.

Return ONLY:
{{
  "action": "...",
  "issue_ids": [],
  "new_text": null,
  "merge_text": null
}}"""

    raw = _get_llm().call(
        system_prompt=_SYSTEM,
        user_message=prompt,
        max_tokens=256,
        temperature=0.0,
        label="parse_issue_update",
    )
    return _parse_result(raw, {"action": "reextract", "issue_ids": [], "new_text": None, "merge_text": None})


# ── Query rewriter ─────────────────────────────────────────────────────────────

def rewrite_query_if_needed(question: str, history: list, snapshot: dict) -> str:
    """
    If the question references prior context ("that", "this", "same issue"),
    rewrite it into a self-contained question using the last 2 QA pairs.
    Returns the original or rewritten question.
    No-op if history is empty or question has no referential language.
    """
    if not history or not question.strip():
        return question

    # Build last 2 QA pairs
    turns = []
    pending_q = None
    for msg in history:
        role    = msg.get("role") if isinstance(msg, dict) else getattr(msg, "role", "")
        content = msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", "")
        if role == "user":
            pending_q = content
        elif role == "assistant" and pending_q:
            turns.append((pending_q, content[:400]))
            pending_q = None

    if not turns:
        return question

    hist_text = "\n".join(
        f"[{i+1}] User: {q}\n    Assistant: {a}"
        for i, (q, a) in enumerate(turns[-2:])
    )

    prompt = f"""Given this conversation history, determine if the current query references prior context.
If yes, rewrite it as a standalone question. If it's already standalone, return it unchanged.

History (last 2 turns):
{hist_text}

Current query: {question}

Return ONLY valid JSON:
{{"type": "SELF_CONTAINED"|"DEPENDENT", "rewritten_query": null or "..."}}"""

    raw = _get_llm().call(
        system_prompt="You are a query rewriter. Return only valid JSON.",
        user_message=prompt,
        max_tokens=256,
        temperature=0.0,
        label="query_rewrite",
    )
    if not raw:
        return question
    try:
        m = re.search(r"\{.*\}", raw.strip(), re.DOTALL)
        if m:
            parsed = json.loads(m.group())
            if parsed.get("type") in ("DEPENDENT",) and parsed.get("rewritten_query"):
                logger.info(f"Query rewritten: {parsed['rewritten_query'][:80]}")
                return parsed["rewritten_query"]
    except Exception:
        pass
    return question