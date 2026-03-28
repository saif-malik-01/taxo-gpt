"""
services/document/doc_context.py

Snapshot stored in Redis per session_id.

Structure:
  active_case_id          which case is currently being worked on
  cases[]                 all cases in this session
    case_id
    status                active | archived
    parties               {sender, recipient} — from latest primary doc
    legal_doc_type        type of latest primary document
    reference_number      of latest primary document
    date                  of latest primary document
    summary               built from brief_summaries of all primary docs (no LLM)
    issues[]              extracted from all primary docs, merged
      id, text, source_doc, reply, status, replied_by_doc
    mode                  defensive | in_favour | null
    state                 awaiting_decision | awaiting_mode |
                          awaiting_classification_confirmation |
                          reply_in_progress | complete
    documents[]           every file uploaded for this case
      filename, legal_doc_type, is_primary, is_latest, is_replied,
      replied_by_doc, parties, reference_number, date, brief_summary,
      classification_confirmed, replied_issues[]
    user_context[]        cumulative user instructions (append-only)
      message, applied_to

  _pending_confirmations  docs waiting for user to confirm classification
"""

import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

DOC_CONTEXT_KEY = "doc_context:{}"
DOC_CONTEXT_TTL = 86400  # 24 hours


# ─── Redis helpers ────────────────────────────────────────────────────────────

async def get_doc_context(session_id: str) -> Optional[dict]:
    try:
        from services.database import get_redis
        redis = await get_redis()
        data  = await redis.get(DOC_CONTEXT_KEY.format(session_id))
        return json.loads(data) if data else None
    except Exception as e:
        logger.warning(f"Redis get_doc_context error: {e}")
    return None


async def set_doc_context(session_id: str, context: dict):
    try:
        from services.database import get_redis
        redis = await get_redis()
        await redis.set(
            DOC_CONTEXT_KEY.format(session_id),
            json.dumps(context, default=str),
            ex=DOC_CONTEXT_TTL,
        )
    except Exception as e:
        logger.warning(f"Redis set_doc_context error: {e}")


async def clear_doc_context(session_id: str):
    try:
        from services.database import get_redis
        redis = await get_redis()
        await redis.delete(DOC_CONTEXT_KEY.format(session_id))
    except Exception as e:
        logger.warning(f"Redis clear_doc_context error: {e}")


# ─── Constructors ─────────────────────────────────────────────────────────────

def create_empty_context() -> dict:
    return {
        "active_case_id":         None,
        "cases":                  [],
        "_pending_confirmations": [],
    }


def create_new_case(case_id: int, parties: dict = None) -> dict:
    return {
        "case_id":        case_id,
        "status":         "active",
        "parties":        parties or {"sender": None, "recipient": None},
        "legal_doc_type": None,
        "reference_number": None,
        "date":           None,
        "summary":        "",
        "issues":         [],
        "mode":           None,
        "state":          "awaiting_decision",
        "documents":      [],
        "user_context":   [],
    }


def create_doc_entry(
    filename: str,
    legal_doc_type: str,
    is_primary: bool,
    is_latest: bool,
    is_replied: bool,
    replied_by_doc: Optional[str],
    parties: dict,
    reference_number: Optional[str],
    date: Optional[str],
    brief_summary: str,
    classification_confirmed: bool,
    replied_issues: list = None,
) -> dict:
    """Create a document entry for case['documents']."""
    return {
        "filename":                 filename,
        "legal_doc_type":           legal_doc_type,
        "is_primary":               is_primary,
        "is_latest":                is_latest,
        "is_replied":               is_replied,
        "replied_by_doc":           replied_by_doc,
        "parties":                  parties,
        "reference_number":         reference_number,
        "date":                     date,
        "brief_summary":            brief_summary,
        "classification_confirmed": classification_confirmed,
        # [{issue_text, reply_text}] extracted from previous_reply docs
        "replied_issues":           replied_issues or [],
    }


# ─── Case accessors ───────────────────────────────────────────────────────────

def get_active_case(context: dict) -> Optional[dict]:
    if not context:
        return None
    active_id = context.get("active_case_id")
    if active_id is None:
        return None
    for case in context.get("cases", []):
        if case["case_id"] == active_id:
            return case
    return None


def get_next_case_id(context: dict) -> int:
    cases = context.get("cases", [])
    return max((c["case_id"] for c in cases), default=0) + 1


def add_case_to_context(context: dict, case: dict):
    """Archive current active case (if any) and set new case as active."""
    for c in context.get("cases", []):
        if c.get("status") == "active":
            c["status"] = "archived"
    context.setdefault("cases", []).append(case)
    context["active_case_id"] = case["case_id"]


def archive_active_case(context: dict):
    active_id = context.get("active_case_id")
    for case in context.get("cases", []):
        if case["case_id"] == active_id:
            case["status"] = "archived"


def switch_active_case(context: dict, case_id: int):
    archive_active_case(context)
    context["active_case_id"] = case_id
    for case in context.get("cases", []):
        if case["case_id"] == case_id:
            case["status"] = "active"


# ─── Document entry helpers ───────────────────────────────────────────────────

def add_document_to_case(case: dict, doc_entry: dict):
    """Append a document entry to case['documents']."""
    case.setdefault("documents", []).append(doc_entry)


def recalculate_is_latest(case: dict):
    """
    Set is_latest=True on the most recently dated primary doc(s).
    If two primary docs share the same date → both get is_latest=True
    (two co-latest notices, distinguished by reference_number).
    Primary docs without a parseable date are never is_latest unless
    they are the only primary doc.
    """
    primary_docs = [d for d in case.get("documents", []) if d.get("is_primary")]
    if not primary_docs:
        return

    # Reset all
    for d in primary_docs:
        d["is_latest"] = False

    # Try to find the latest date
    def _parse(d):
        raw = d.get("date") or ""
        # Accept common date formats
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d.%m.%Y",
                    "%B %d, %Y", "%d %B %Y", "%b %d, %Y"):
            try:
                from datetime import datetime
                return datetime.strptime(raw.strip(), fmt)
            except ValueError:
                pass
        return None

    dated = [(d, _parse(d)) for d in primary_docs if _parse(d)]

    if not dated:
        # No parseable dates — mark all primary as is_latest
        for d in primary_docs:
            d["is_latest"] = True
        return

    latest_dt = max(dt for _, dt in dated)
    for d, dt in dated:
        if dt == latest_dt:
            d["is_latest"] = True


def update_case_level_from_latest(case: dict):
    """
    Refresh case-level parties / legal_doc_type / reference_number / date
    from the is_latest primary document.
    Only overwrites null fields — confirmed values are never changed.
    """
    latest_docs = [
        d for d in case.get("documents", [])
        if d.get("is_primary") and d.get("is_latest")
    ]
    if not latest_docs:
        return
    # Use the first is_latest doc (usually only one)
    src = latest_docs[0]
    for field in ("legal_doc_type", "reference_number", "date"):
        if not case.get(field) and src.get(field):
            case[field] = src[field]
    # Parties: fill nulls only
    for role in ("sender", "recipient"):
        if not (case.get("parties") or {}).get(role) and \
                (src.get("parties") or {}).get(role):
            case.setdefault("parties", {})[role] = src["parties"][role]


def build_case_summary(case: dict) -> str:
    """
    Build case-level summary from brief_summaries of all primary docs.
    No LLM call — pure concatenation with structure.
    Brief summaries already have all legal entities preserved.
    """
    primary_docs = [d for d in case.get("documents", []) if d.get("is_primary")]
    if not primary_docs:
        return ""
    # Sort: latest first, then by date desc
    sorted_docs = sorted(
        primary_docs,
        key=lambda d: (not d.get("is_latest", False), d.get("date") or ""),
        reverse=True,
    )
    parts = []
    for d in sorted_docs:
        tag = f"[{d['legal_doc_type'].upper()} | Ref: {d.get('reference_number') or 'N/A'} | Date: {d.get('date') or 'N/A'}]"
        if d.get("is_latest"):
            tag += " ← LATEST"
        if d.get("is_replied"):
            tag += " [REPLIED ✓]"
        parts.append(f"{tag}\n{d.get('brief_summary', '')}")
    return "\n\n".join(parts)


# ─── User context helpers ─────────────────────────────────────────────────────

def append_user_context(case: dict, message: str, applied_to: Optional[str] = None):
    """Append a user instruction to the case's user_context (append-only)."""
    case.setdefault("user_context", []).append({
        "message":    message,
        "applied_to": applied_to,
    })


def get_user_context_text(case: dict, limit: int = 3) -> str:
    """Return last N user context entries as readable text for LLM prompts."""
    entries = (case.get("user_context") or [])[-limit:]
    if not entries:
        return ""
    lines = []
    for e in entries:
        target = f" (re: {e['applied_to']})" if e.get("applied_to") else ""
        lines.append(f"- {e['message']}{target}")
    return "\n".join(lines)


# ─── Issue helpers ────────────────────────────────────────────────────────────

def get_pending_issues(case: dict) -> list:
    """Issues with no reply and not marked as has_reply_doc."""
    return [
        i for i in case.get("issues", [])
        if not i.get("reply") and i.get("status") not in ("replied", "has_reply_doc")
    ]


def get_draftable_issues(case: dict, requested_ids: list = None) -> list:
    """
    Issues that should be drafted.
    If requested_ids given → include those regardless of status (explicit override).
    Otherwise → only truly pending issues.
    """
    all_issues = case.get("issues", [])
    if requested_ids:
        return [i for i in all_issues if i["id"] in requested_ids]
    return get_pending_issues(case)


def merge_issues(existing: list, new_texts: list, source_doc: str) -> list:
    """
    Merge newly extracted issue texts with existing issues.
    - Issues already replied → preserved unchanged
    - Issues with has_reply_doc → preserved unchanged
    - New issues → added as pending with source_doc tag
    - Near-duplicate (>85% similarity) → skip
    Renumbers sequentially.
    """
    import re

    def _norm(t):
        return re.sub(r'\s+', ' ', t.lower().strip())

    def _similar(a, b):
        na, nb = _norm(a), _norm(b)
        shorter = min(len(na), len(nb))
        if shorter == 0:
            return False
        if na in nb or nb in na:
            return True
        common = sum(1 for x, y in zip(na, nb) if x == y)
        return common / shorter > 0.85

    # Build lookup of existing texts
    existing_texts = [i["text"] for i in existing]

    merged = list(existing)  # preserve all existing (including replied)

    for new_text in new_texts:
        # Skip if too similar to any existing issue
        if any(_similar(new_text, et) for et in existing_texts):
            continue
        merged.append({
            "id":           0,
            "text":         new_text,
            "source_doc":   source_doc,
            "reply":        None,
            "status":       "pending",
            "replied_by_doc": None,
        })
        existing_texts.append(new_text)

    # Renumber
    for idx, issue in enumerate(merged, 1):
        issue["id"] = idx
    return merged


def apply_issue_update(case: dict, update: dict):
    """Apply a parsed issue-list update in-place."""
    issues = case.get("issues", [])
    action = update.get("action")
    ids    = [int(x) for x in (update.get("issue_ids") or [])]

    if action == "merge" and len(ids) >= 2:
        to_merge = [i for i in issues if i["id"] in ids]
        rest     = [i for i in issues if i["id"] not in ids]
        if to_merge:
            merged_text  = update.get("merge_text") or " | ".join(i["text"] for i in to_merge)
            merged_issue = {
                "id":         min(i["id"] for i in to_merge),
                "text":       merged_text,
                "source_doc": to_merge[0].get("source_doc"),
                "reply":      None,
                "status":     "pending",
                "replied_by_doc": None,
            }
            all_issues = sorted(rest + [merged_issue], key=lambda x: x["id"])
            for idx, issue in enumerate(all_issues, 1):
                issue["id"] = idx
            case["issues"] = all_issues

    elif action == "add":
        new_text = (update.get("new_text") or "").strip()
        if new_text:
            new_id = max((i["id"] for i in issues), default=0) + 1
            issues.append({
                "id":         new_id,
                "text":       new_text,
                "source_doc": "user_added",
                "reply":      None,
                "status":     "pending",
                "replied_by_doc": None,
            })

    elif action == "correct" and ids:
        new_text = (update.get("new_text") or "").strip()
        for issue in issues:
            if issue["id"] in ids and new_text:
                issue["text"]   = new_text
                issue["reply"]  = None
                issue["status"] = "pending"

    elif action == "remove" and ids:
        case["issues"] = [i for i in issues if i["id"] not in ids]
        for idx, issue in enumerate(case["issues"], 1):
            issue["id"] = idx


def mark_doc_as_replied(case: dict, filename: str, replied_by: str):
    """Mark a primary document as replied and update associated issue statuses."""
    for doc in case.get("documents", []):
        if doc["filename"] == filename and doc.get("is_primary"):
            doc["is_replied"]    = True
            doc["replied_by_doc"] = replied_by
    # Mark issues from this doc as has_reply_doc (if not already replied via session)
    for issue in case.get("issues", []):
        if issue.get("source_doc") == filename and not issue.get("reply"):
            issue["status"]       = "has_reply_doc"
            issue["replied_by_doc"] = replied_by