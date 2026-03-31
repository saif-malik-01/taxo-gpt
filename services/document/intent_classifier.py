"""
services/document/intent_classifier.py

Two public functions used by api/document.py:

  classify_intent_no_docs(question, snapshot) → dict
    Used for Type 3 requests (text only, active case exists).

  rewrite_query_if_needed(question, history, snapshot) → str
    Referential language detector + LLM rewrite for dependent questions.
    Uses last_3_qa_pairs from snapshot — no full conversation history needed.

  parse_issue_update(message, current_issues) → dict
    Parses user instructions about changing the issues list.

Note: classify_intent_with_docs() is NOT here.
  When files are uploaded, intent comes from the combined Step 2A+2C output
  in doc_classifier.analyze_document(). No separate intent LLM call needed.
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


def _parse(raw: Optional[str], fallback: dict) -> dict:
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
        logger.error(f"Intent JSON parse error: {e} | raw[:200]={raw[:200]}")
        return fallback


_SYSTEM = "You are an intent classifier for a legal document assistant. Return ONLY valid JSON."

_INTENT_OPTIONS = """
Intent options (pick the single best match):
  summarize             — show document summary and issues list
  draft_all             — generate replies for ALL pending issues
  draft_specific        — generate reply for SPECIFIC issue numbers mentioned
  draft_direct          — "prepare reply" / "draft reply" / "reply to this" — mode clear from context
  confirm_mode          — user confirming defensive or in-favour mode
                          defensive signals: "defence", "defensive", "protect", "assessee side", "yes" when awaiting mode
                          in_favour signals: "in favour", "department side", "revenue is right", "support the notice"
  explain_issues        — user wants plain-language explanation of one or more issues
                          signals: "explain issue", "what does issue mean", "what is issue", "clarify issue",
                                   "explain these issues", "first explain me", "understand issue"
  update_issues         — merge/add/correct/remove issues, or "issues are missed"
                          signals: "missed", "merge issues", "combine issues X and Y", "add issue", "remove issue",
                                   "correct issue", "issue is wrong", "issues are not right"
  update_reply          — change / redo / improve the reply for a specific issue
                          includes: user providing their own facts/scenario for an issue
                          signals: "redo issue", "update reply", "improve reply", "for issue N, we have...",
                                   "in issue N, the amount is...", "revise issue", "more detail for issue"
  merge_replies         — merge/combine the replies of two or more issues into one consolidated reply
                          signals: "merge replies", "combine replies", "merge issue N and M reply",
                                   "one reply for issues", "single reply for"
  query_document        — question answerable from uploaded document content
  query_general         — pure GST/tax knowledge question, no document context
  switch_case           — work on a previously archived case
  new_case              — start fresh for a completely different matter
  mark_replied          — document or issue has already been replied to externally
  correct_classification — document was misclassified (wrong primary/reference/draft/reply label)
"""

_MODE_RULE = """
mode: "defensive" | "in_favour" | null
  defensive = protecting the notice recipient / assessee / taxpayer
  in_favour = supporting the notice / department / revenue position
  null = not determinable from this message alone
"""


# ─────────────────────────────────────────────────────────────────────────────
# classify_intent_no_docs
# ─────────────────────────────────────────────────────────────────────────────

def classify_intent_no_docs(question: str, snapshot: dict) -> dict:
    """
    Intent classification for Type 3 requests (no files, active case exists).
    Uses only the last 3 QA pairs + issues list + current mode/state.
    """
    from services.document.doc_context import get_active_case, get_user_context_text

    fallback = {"intent": "query_general", "mode": None, "issue_numbers": [], "case_id": None}
    if not question or not question.strip():
        return {"intent": "summarize", "mode": None, "issue_numbers": [], "case_id": None}

    active_case = get_active_case(snapshot)
    case_state  = None
    current_mode = None
    has_pending  = False
    issues_preview = ""
    user_ctx     = ""
    cases_preview = ""

    if active_case:
        case_state   = active_case.get("state")
        current_mode = active_case.get("mode")
        issues       = active_case.get("issues", [])
        has_pending  = any(
            i.get("status") == "pending" and not i.get("reply")
            for i in issues
        )
        if issues:
            lines = [
                f"  {i.get('id','?')}. [{i.get('status','pending')}] {i.get('issue_text','')[:80]}"
                for i in issues[:8]
            ]
            issues_preview = "\n".join(lines)
        user_ctx = get_user_context_text(active_case, limit=3)

    archived = [
        c for cid, c in snapshot.get("cases", {}).items()
        if c.get("session_status") == "archived"
    ]
    if archived:
        cases_preview = "Archived cases: " + ", ".join(
            f"Case {c.get('case_id','?')} ({(c.get('parties') or {}).get('authority','?')} / "
            f"{(c.get('parties') or {}).get('taxpayer_name','?')})"
            for c in archived[:3]
        )

    # Last 3 QA pairs for context
    qa_pairs = snapshot.get("last_3_qa_pairs", [])[-2:]
    qa_text  = ""
    if qa_pairs:
        qa_text = "\nRecent conversation:\n" + "\n".join(
            f"  User: {p.get('user_q','')[:100]}\n  Assistant: {p.get('assistant_a','')[:150]}"
            for p in qa_pairs
        )

    prompt = f"""Classify the user's intent for this legal document session.

SESSION STATE:
  Active case: {"yes" if active_case else "no"}
  Current state: {case_state or "none"}
  Current mode: {current_mode or "not set"}
  Pending (unreplied) issues: {has_pending}
  User's previous instructions:
{user_ctx or "  (none)"}
  Issues (id, status, text snippet):
{issues_preview or "  (none)"}
{cases_preview}
{qa_text}

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
        system_prompt = _SYSTEM,
        user_message  = prompt,
        max_tokens    = 256,
        temperature   = 0.0,
        label         = "intent_no_docs",
    )
    return _parse(raw, fallback)


# ─────────────────────────────────────────────────────────────────────────────
# rewrite_query_if_needed
# ─────────────────────────────────────────────────────────────────────────────

# Referential language patterns — if ANY match, consider rewriting
_REFERENTIAL_PATTERNS = re.compile(
    r"\b(it|that|this|those|them|they|the same|the above|the following|"
    r"second|third|first|last|other|another|the issue|the notice|the section|"
    r"the case|the document|the reply|further|additionally|also|"
    r"what about|how about|explain that|elaborate|regarding the above|"
    r"on the same|in that case)\b",
    re.IGNORECASE,
)


def _is_referential(question: str) -> bool:
    """Quick check — does question contain referential language?"""
    return bool(_REFERENTIAL_PATTERNS.search(question))


def rewrite_query_if_needed(question: str, history: list, snapshot: dict) -> str:
    """
    If question references prior context, rewrite it as a self-contained question.
    Uses last_3_qa_pairs from snapshot (NOT full history — capped at 3 pairs).
    Returns original question if not referential or rewrite fails.
    """
    if not question or not question.strip():
        return question

    if not _is_referential(question):
        return question

    # Build context from snapshot's last_3_qa_pairs
    qa_pairs = snapshot.get("last_3_qa_pairs", [])[-2:]
    if not qa_pairs:
        # Fallback: build from raw history if qa_pairs empty
        pairs = []
        pending_q = None
        for msg in (history or []):
            role    = msg.get("role") if isinstance(msg, dict) else getattr(msg, "role", "")
            content = msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", "")
            if role == "user":
                pending_q = content
            elif role == "assistant" and pending_q:
                pairs.append({"user_q": pending_q[:400], "assistant_a": content[:600]})
                pending_q = None
        qa_pairs = pairs[-2:]

    if not qa_pairs:
        return question

    hist_text = "\n".join(
        f"[Turn {i+1}]\nUser: {p.get('user_q','')}\nAssistant: {p.get('assistant_a','')[:400]}"
        for i, p in enumerate(qa_pairs)
    )

    # Case context for resolution
    from services.document.doc_context import get_active_case
    active_case = get_active_case(snapshot)
    case_ctx = ""
    if active_case:
        p = active_case.get("parties") or {}
        issues = active_case.get("issues", [])
        case_ctx = (
            f"Case: {p.get('authority','?')} → {p.get('taxpayer_name','?')}\n"
            f"Issues: " + ", ".join(
                f"Issue {i.get('id','?')}: {i.get('issue_text','')[:60]}"
                for i in issues[:5]
            )
        )

    prompt = f"""Given this conversation, determine if the current question references prior context.
If yes, rewrite it as a fully standalone question. If already standalone, return it unchanged.

Conversation history:
{hist_text}

Case context:
{case_ctx}

Current question: {question}

Return ONLY valid JSON:
{{"type": "SELF_CONTAINED" | "DEPENDENT", "rewritten_query": "..." or null}}

For SELF_CONTAINED: rewritten_query = null
For DEPENDENT: rewritten_query = the fully standalone rewritten question"""

    raw = _get_llm().call(
        system_prompt = "You are a query rewriter. Return only valid JSON.",
        user_message  = prompt,
        max_tokens    = 300,
        temperature   = 0.0,
        label         = "query_rewrite",
    )
    if not raw:
        return question
    try:
        text = raw.strip()
        m    = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            parsed = json.loads(m.group())
            if parsed.get("type") == "DEPENDENT" and parsed.get("rewritten_query"):
                rewritten = parsed["rewritten_query"]
                logger.info(f"Query rewritten: {rewritten[:100]}")
                return rewritten
    except Exception:
        pass
    return question


# ─────────────────────────────────────────────────────────────────────────────
# parse_issue_update
# ─────────────────────────────────────────────────────────────────────────────

def parse_issue_update(message: str, current_issues: list) -> dict:
    """
    Parse user instruction about changing the issues list.
    Returns: {action, issue_ids, new_text, merge_text}
    """
    issue_lines = "\n".join(
        f"{i.get('id','?')}. {i.get('issue_text','')[:100]}"
        for i in current_issues
    )
    prompt = f"""Parse this instruction about a legal issues list.

USER INSTRUCTION: "{message}"

CURRENT ISSUES:
{issue_lines}

Determine:
  action     : merge | add | correct | remove | reextract
               "reextract" = user says issues are missed but does NOT provide specific text
               "add" = user provides specific missing issue text
               "merge" = user wants to combine two or more issues into one
               "correct" = user wants to change the text of an issue
               "remove" = user wants to delete an issue
  issue_ids  : list of issue IDs (integers) involved; empty for reextract/add-new
  new_text   : verbatim text from user for add or correct; null otherwise
  merge_text : combined text for merged issue; null otherwise

Return ONLY:
{{
  "action": "...",
  "issue_ids": [],
  "new_text": null,
  "merge_text": null
}}"""

    raw = _get_llm().call(
        system_prompt = _SYSTEM,
        user_message  = prompt,
        max_tokens    = 256,
        temperature   = 0.0,
        label         = "parse_issue_update",
    )
    return _parse(raw, {"action": "reextract", "issue_ids": [], "new_text": None, "merge_text": None})