"""
services/document/doc_context.py

Session snapshot — the minimal in-memory/Redis state for Feature 2.

What lives here (per our finalized spec):
  - Classification flags (role, temporal_role, is_latest, locked flags)
  - issues[] — the core work product
  - brief_summary per doc (<=400 chars)
  - legal_entities_cache (Stage2BResult) per doc — never recomputed
  - last_3_qa_pairs — for query rewriting only
  - user_context[] — last 10 confirmed user instructions
  - Event log (append-only, full audit)

What does NOT live here:
  - Full extracted text → Postgres (session_doc_store.py)
  - Page images → temp files deleted after Step 1
  - Conversation history beyond last 3 pairs → Postgres (memory.py)

Redis key: doc:ctx:{session_id}
TTL: 4 hours (refreshed on every write)
"""

import json
import logging
import uuid
from datetime import datetime, date
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional

from apps.api.src.db.session import get_redis, AsyncSessionLocal
from sqlalchemy import update, select
from apps.api.src.db.models.base import ChatSession

logger = logging.getLogger(__name__)

_CTX_PREFIX = "doc:ctx:"
_CTX_TTL    = 14400   # 4 hours


# ─────────────────────────────────────────────────────────────────────────────
# Serialisation helpers
# ─────────────────────────────────────────────────────────────────────────────

def _serialise(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Not serialisable: {type(obj)}")


async def get_doc_context(session_id: str) -> Optional[dict]:
    # 1. Try Redis (High Speed)
    redis = await get_redis()
    raw = await redis.get(f"{_CTX_PREFIX}{session_id}")
    if raw:
        try:
            return json.loads(raw)
        except:
            pass

    # 2. Try Postgres Fallback (Persistence / Multi-day Recovery)
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(ChatSession.metadata_snapshot).where(ChatSession.id == session_id))
        doc_meta = res.scalar_one_or_none()
        
        if doc_meta:
            # Re-warm Redis for subsequent high-speed access
            await set_doc_context(session_id, doc_meta, mirror_to_db=False)
            return doc_meta
            
    return None


async def set_doc_context(session_id: str, ctx: dict, mirror_to_db: bool = True) -> None:
    # 1. Always save to Redis (4 hour sliding window)
    redis = await get_redis()
    await redis.setex(
        f"{_CTX_PREFIX}{session_id}",
        _CTX_TTL,
        json.dumps(ctx, default=_serialise, ensure_ascii=False),
    )

    # 2. Mirror to Postgres ChatSession (Permanent Life)
    if mirror_to_db:
        # We use a background-like independent session to ensure persistence
        async with AsyncSessionLocal() as db:
            try:
                await db.execute(
                    update(ChatSession)
                    .where(ChatSession.id == session_id)
                    .values(metadata_snapshot=ctx)
                )
                await db.commit()
            except Exception as e:
                logger.error(f"Failed to mirror doc_context to Postgres for {session_id}: {e}")


async def clear_doc_context(session_id: str) -> None:
    # 1. Clear Redis
    redis = await get_redis()
    await redis.delete(f"{_CTX_PREFIX}{session_id}")

    # 2. Clear Postgres mirror
    async with AsyncSessionLocal() as db:
        await db.execute(
            update(ChatSession)
            .where(ChatSession.id == session_id)
            .values(metadata_snapshot=None)
        )
        await db.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Session / case factories
# ─────────────────────────────────────────────────────────────────────────────

def create_empty_context() -> dict:
    """Top-level session object. Holds multiple cases."""
    return {
        "active_case_id": None,
        "cases": {},                    # case_id -> CaseSnapshot
        "cross_case_references": [],    # doc_ids usable across cases
        "event_log": [],
        "pending_docs_count": 0,
        "snapshot_version": 0,
        "last_3_qa_pairs": [],          # [{user_q, assistant_a}]
        "legal_entities_cache": {},     # filename -> Stage2BResult dict (flattened)
        "_pending_confirmations": [],
    }


def create_new_case(case_id: str, parties: dict) -> dict:
    return {
        "case_id":        case_id,
        "session_status": "active",
        "parties": {
            "taxpayer_name": parties.get("recipient"),
            "authority":     parties.get("sender"),
            "gstin":         parties.get("gstin"),
            "pan":           parties.get("pan"),
        },
        "parties_locked":   False,
        "reference_number": None,
        "docs":             [],
        "issues":           [],
        "mode":             None,
        "user_context":     [],         # last 10 confirmed user instructions
        "summary":          "",
        "state":            "new",
    }


def get_next_case_id(ctx: dict) -> int:
    """
    Returns next case ID as an integer.
    session_document_texts.case_id is INTEGER in Postgres — must be int.
    """
    existing = ctx.get("cases", {})
    if not existing:
        return 1
    try:
        return max(int(k) for k in existing.keys()) + 1
    except (ValueError, TypeError):
        return len(existing) + 1


def get_active_case(ctx: dict) -> Optional[dict]:
    cid = ctx.get("active_case_id")
    if not cid:
        return None
    cases = ctx.get("cases", {})
    # JSON serialisation converts dict keys to strings.
    # active_case_id might be int (from code) or str (from JSON).
    return cases.get(cid) or cases.get(str(cid))


def add_case_to_context(ctx: dict, case: dict) -> None:
    ctx.setdefault("cases", {})[case["case_id"]] = case
    ctx["active_case_id"] = case["case_id"]
    _append_event(ctx, "CASE_CREATED", {"case_id": case["case_id"]})


def switch_active_case(ctx: dict, case_id: str) -> None:
    old_id = ctx.get("active_case_id")
    cases = ctx.setdefault("cases", {})
    
    if old_id:
        old_case = cases.get(old_id) or cases.get(str(old_id))
        if old_case:
            old_case["session_status"] = "archived"
            
    ctx["active_case_id"] = case_id
    new_case = cases.get(case_id) or cases.get(str(case_id))
    if new_case:
        new_case["session_status"] = "active"
    _append_event(ctx, "CASE_SWITCHED", {"from": old_id, "to": case_id})


def archive_active_case(ctx: dict) -> None:
    cid = ctx.get("active_case_id")
    if cid:
        cases = ctx.get("cases", {})
        case = cases.get(cid) or cases.get(str(cid))
        if case:
            case["session_status"] = "archived"
            _append_event(ctx, "CASE_ARCHIVED", {"case_id": cid})
    # Reset session-level state for new case
    ctx["last_3_qa_pairs"] = []


# ─────────────────────────────────────────────────────────────────────────────
# DocEntry factory
# ─────────────────────────────────────────────────────────────────────────────

def create_doc_entry(
    filename: str,
    *,
    role: str = "primary",          # primary | reference | previous_reply | user_draft_reply | informational
    display_type: str = "notice",
    temporal_role: str = "unknown", # current | historical | unknown
    temporal_locked: bool = False,
    role_locked: bool = False,
    is_latest: bool = False,
    date: Optional[str] = None,
    reference_number: Optional[str] = None,
    parties: Optional[dict] = None,
    brief_summary: str = "",
    confidence: int = 0,
    has_issues: bool = False,
    has_replied_issues: bool = False,
    replies_to_doc_id: Optional[str] = None,
    part_doc_ids: Optional[List[str]] = None,
    upload_hints: Optional[List[str]] = None,
) -> dict:
    return {
        "doc_id":            str(uuid.uuid4()),
        "filename":          filename,
        "upload_timestamp":  datetime.utcnow().isoformat(),
        "page_count":        0,
        # Classification
        "role":              role,
        "role_locked":       role_locked,
        "display_type":      display_type,
        "temporal_role":     temporal_role,
        "temporal_locked":   temporal_locked,
        "is_latest":         is_latest,
        # Metadata
        "date":              date,
        "reference_number":  reference_number,
        "parties":           parties or {},
        "brief_summary":     brief_summary,
        "confidence":        confidence,
        # Step 6 flags
        "has_issues":        has_issues,
        "has_replied_issues": has_replied_issues,
        # Cached outputs — never recomputed
        "replied_issues":    [],        # [{issue_text, reply_text}]
        # Linkage
        "replies_to_doc_id": replies_to_doc_id,
        "part_doc_ids":      part_doc_ids or [],
        "upload_hints":      upload_hints or [],
        # Audit
        "user_corrections":  [],
        "pipeline_status":   "pending",
    }


def add_document_to_case(case: dict, doc_entry: dict) -> None:
    case.setdefault("docs", []).append(doc_entry)


# ─────────────────────────────────────────────────────────────────────────────
# Issue management
# ─────────────────────────────────────────────────────────────────────────────

def create_issue(issue_text: str, source_doc_id: str, source_filename: str) -> dict:
    return {
        "issue_id":        str(uuid.uuid4()),
        "source_doc_id":   source_doc_id,
        "source_doc":      source_filename,
        "issue_text":      issue_text,
        "status":          "pending",   # pending | replied | user_provided
        "reply":           None,
        "stale":           False,
    }


def merge_issues(
    existing: List[dict],
    new_texts: List[str],
    source_doc_id: str,
    source_filename: str,
    similarity_threshold: float = 0.85,
) -> List[dict]:
    """
    Merge new issue texts into existing list.
    Dedup using section-number as primary key, string similarity as fallback.
    Assigns sequential integer IDs after merge.
    """
    import re

    def _section_key(text: str) -> str:
        """Extract section numbers as dedup key."""
        sections = re.findall(r"[Ss]ection\s+(\d+[A-Z]?(?:\(\d+\))?)", text)
        return "|".join(sorted(sections))

    result = list(existing)
    for new_text in new_texts:
        new_text = new_text.strip()
        if not new_text:
            continue
        new_sec_key = _section_key(new_text)
        duplicate = False
        for ex in result:
            ex_text = ex.get("issue_text", "")
            # Primary: section number match
            if new_sec_key and _section_key(ex_text) == new_sec_key:
                duplicate = True
                break
            # Fallback: string similarity
            ratio = SequenceMatcher(None, new_text[:200], ex_text[:200]).ratio()
            if ratio >= similarity_threshold:
                duplicate = True
                break
        if not duplicate:
            result.append(create_issue(new_text, source_doc_id, source_filename))

    # Re-assign sequential integer ids for display
    for idx, iss in enumerate(result, 1):
        iss["id"] = idx
    return result


def get_draftable_issues(case: dict, issue_ids: Optional[List[int]] = None) -> List[dict]:
    """
    Return issues that need a reply.
    If issue_ids specified, filter to those. Otherwise all pending.
    """
    issues = case.get("issues", [])
    if issue_ids:
        return [i for i in issues if i.get("id") in issue_ids]
    return [
        i for i in issues
        if i.get("status") == "pending" and not i.get("reply")
    ]


def get_pending_issues(case: dict) -> List[dict]:
    return [i for i in case.get("issues", []) if not i.get("reply") and i.get("status") not in ("replied", "has_reply_doc")]


# ─────────────────────────────────────────────────────────────────────────────
# is_latest determination
# ─────────────────────────────────────────────────────────────────────────────

def recalculate_is_latest(case: dict) -> None:
    """
    Compute is_latest for all primary docs in the case.
    Rule: most recently dated primary doc(s) = is_latest=True.
    Equal dates → all is_latest=True.
    temporal_role=unknown docs: is_latest=False until user clarifies.
    No LLM. Pure date comparison.
    """
    import re

    primary_docs = [d for d in case.get("docs", []) if d.get("role") == "primary"]
    if not primary_docs:
        return

    # Reset all first
    for d in case["docs"]:
        if d.get("role") == "primary":
            d["is_latest"] = False

    def _parse_date(date_str: Optional[str]):
        if not date_str:
            return None
        # Try common formats
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%b-%Y", "%d %b %Y", "%B %d, %Y"):
            try:
                from datetime import datetime as dt
                return dt.strptime(date_str.strip(), fmt).date()
            except Exception:
                pass
        return None

    dated   = [(d, _parse_date(d.get("date"))) for d in primary_docs if _parse_date(d.get("date"))]
    undated = [d for d in primary_docs if not _parse_date(d.get("date"))]

    if dated:
        max_date = max(p for _, p in dated)
        for doc, parsed in dated:
            doc["is_latest"] = (parsed == max_date)
    elif undated:
        # No dates anywhere — only doc is latest by default
        if len(undated) == 1:
            undated[0]["is_latest"] = True
        # Multiple undated primaries — leave all False, Step 4 fires


def mark_replied(case: dict, source_filename: str, reply_filename: str) -> None:
    """Mark docs from source_filename as having a reply doc."""
    for doc in case.get("docs", []):
        if doc.get("filename") == source_filename:
            doc["replied_by_doc"] = reply_filename


# ─────────────────────────────────────────────────────────────────────────────
# Case-level metadata helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_case_summary(case: dict) -> str:
    """
    Concatenate brief_summaries of all primary docs, newest first.
    No LLM call — pure concatenation.
    """
    primary_docs = sorted(
        [d for d in case.get("docs", []) if d.get("role") == "primary"],
        key=lambda d: d.get("date") or "",
        reverse=True,
    )
    parts = []
    for doc in primary_docs:
        s = doc.get("brief_summary", "").strip()
        if s:
            tag = f"[{doc.get('display_type','notice')} | {doc.get('reference_number','N/A')} | {doc.get('date','N/A')}]"
            parts.append(f"{tag} {s}")
    return "\n\n".join(parts)


def update_case_level_from_latest(case: dict) -> None:
    """Update case-level parties and reference_number from is_latest primary docs."""
    latest = [d for d in case.get("docs", []) if d.get("role") == "primary" and d.get("is_latest")]
    if not latest:
        return
    doc = latest[0]
    # Only fill null fields — never overwrite locked values
    if not case.get("parties_locked"):
        p = doc.get("parties", {})
        case.setdefault("parties", {})
        if not case["parties"].get("authority") and p.get("sender"):
            case["parties"]["authority"] = p["sender"]
        if not case["parties"].get("taxpayer_name") and p.get("recipient"):
            case["parties"]["taxpayer_name"] = p["recipient"]
        if not case["parties"].get("gstin") and p.get("gstin"):
            case["parties"]["gstin"] = p["gstin"]
        if not case["parties"].get("pan") and p.get("pan"):
            case["parties"]["pan"] = p["pan"]
    if not case.get("reference_number") and doc.get("reference_number"):
        case["reference_number"] = doc["reference_number"]


# ─────────────────────────────────────────────────────────────────────────────
# User context
# ─────────────────────────────────────────────────────────────────────────────

def append_user_context(case: dict, instruction: str, applied_to: str = "") -> None:
    """Append-only. Capped at 10 entries. Oldest evicted first."""
    ctx = case.setdefault("user_context", [])
    entry = instruction.strip()
    if applied_to:
        entry = f"[{applied_to}] {entry}"
    ctx.append(entry)
    if len(ctx) > 10:
        case["user_context"] = ctx[-10:]


def get_user_context_text(case: Optional[dict], limit: int = 3) -> str:
    if not case:
        return ""
    ctx = case.get("user_context", [])
    return "\n".join(f"  - {c}" for c in ctx[-limit:])


# ─────────────────────────────────────────────────────────────────────────────
# QA pairs for query rewriting
# ─────────────────────────────────────────────────────────────────────────────

def push_qa_pair(ctx: dict, user_q: str, assistant_a: str) -> None:
    """Add a QA pair to last_3_qa_pairs. Cap at 3. Oldest evicted."""
    pairs = ctx.setdefault("last_3_qa_pairs", [])
    pairs.append({"user_q": user_q[:800], "assistant_a": assistant_a[:1200]})
    if len(pairs) > 3:
        ctx["last_3_qa_pairs"] = pairs[-3:]


def get_last_qa_pairs(ctx: dict, n: int = 2) -> List[dict]:
    return ctx.get("last_3_qa_pairs", [])[-n:]


# ─────────────────────────────────────────────────────────────────────────────
# Issue update (user corrections)
# ─────────────────────────────────────────────────────────────────────────────

def apply_issue_update(case: dict, update: dict) -> None:
    """Apply a parsed issue update from parse_issue_update()."""
    action     = update.get("action")
    issue_ids  = update.get("issue_ids", [])
    new_text   = update.get("new_text")
    merge_text = update.get("merge_text")
    issues     = case.setdefault("issues", [])

    if action == "add" and new_text:
        issues.append({
            "id":         len(issues) + 1,
            "issue_id":   str(uuid.uuid4()),
            "issue_text": new_text.strip(),
            "source_doc_id": "user_added",
            "source_doc": "user_added",
            "status":     "user_provided",
            "reply":      None,
            "stale":      False,
        })
    elif action == "remove":
        case["issues"] = [i for i in issues if i.get("id") not in issue_ids]
        for idx, iss in enumerate(case["issues"], 1):
            iss["id"] = idx
    elif action == "correct" and new_text and issue_ids:
        for iss in issues:
            if iss.get("id") in issue_ids:
                iss["issue_text"] = new_text.strip()
                iss["status"]     = "pending"
                iss["reply"]      = None
    elif action == "merge" and merge_text and issue_ids:
        # Replace all merged issues with one combined entry
        first_idx = min(issue_ids)
        case["issues"] = [
            i if i.get("id") not in issue_ids
            else {**i, "issue_text": merge_text, "status": "pending", "reply": None}
            if i.get("id") == first_idx else None
            for i in issues
        ]
        case["issues"] = [i for i in case["issues"] if i is not None]
        for idx, iss in enumerate(case["issues"], 1):
            iss["id"] = idx


# ─────────────────────────────────────────────────────────────────────────────
# Document correction (user feedback on classification)
# ─────────────────────────────────────────────────────────────────────────────

def apply_doc_correction(
    case: dict,
    filename: str,
    field: str,
    new_value: Any,
    ctx: dict,
) -> dict:
    """
    Apply a user correction to a doc entry field.
    Locks the field. Records in user_corrections audit log.
    Returns {reruns: list[str]} indicating which pipeline steps need re-running.
    """
    reruns = []
    for doc in case.get("docs", []):
        if doc.get("filename") != filename:
            continue
        old_value = doc.get(field)
        doc[field] = new_value
        doc[f"{field}_locked"] = True
        doc.setdefault("user_corrections", []).append({
            "timestamp":    datetime.utcnow().isoformat(),
            "field":        field,
            "from_value":   old_value,
            "to_value":     new_value,
            "corrected_by": "user",
        })
        # Determine cascade
        if field == "role":
            if new_value == "primary" and old_value != "primary":
                reruns.append("step_6_issue_extraction")
            if old_value == "primary" and new_value != "primary":
                reruns.append("remove_issues_from_doc")
            reruns.append("recalculate_is_latest")
        elif field == "temporal_role":
            reruns.append("recalculate_is_latest")
        elif field in ("parties.gstin", "parties.pan", "parties.taxpayer_name"):
            reruns.append("same_case_rescore")
        # Log to ctx event log
        _append_event(ctx, "USER_CORRECTION", {
            "doc":       filename,
            "field":     field,
            "from":      str(old_value),
            "to":        str(new_value),
            "reruns":    reruns,
        })
        break
    recalculate_is_latest(case)
    return {"reruns": reruns}


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot version
# ─────────────────────────────────────────────────────────────────────────────

def bump_version(ctx: dict) -> int:
    ctx["snapshot_version"] = ctx.get("snapshot_version", 0) + 1
    return ctx["snapshot_version"]


# ─────────────────────────────────────────────────────────────────────────────
# Event log
# ─────────────────────────────────────────────────────────────────────────────

def _append_event(ctx: dict, event_type: str, payload: dict) -> None:
    ctx.setdefault("event_log", []).append({
        "ts":      datetime.utcnow().isoformat(),
        "type":    event_type,
        "payload": payload,
    })
    # Keep event log from growing unbounded in memory (last 200 events)
    if len(ctx["event_log"]) > 200:
        ctx["event_log"] = ctx["event_log"][-200:]


# ─────────────────────────────────────────────────────────────────────────────
# Display helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_classification_summary(case: dict) -> str:
    """Human-readable block shown after document upload for user verification."""
    docs = case.get("docs", [])
    if not docs:
        return ""
    lines = ["\n\n---\n\n**📋 Document Classification:**\n"]
    for doc in docs:
        fn  = doc.get("filename", "document")
        dt  = doc.get("display_type", "document")
        ref = doc.get("reference_number")
        dt_str = doc.get("date")
        role = doc.get("role", "reference")

        if role == "user_draft_reply":
            role_label = "YOUR DRAFT REPLY"
        elif role == "previous_reply":
            role_label = "PREVIOUS REPLY (submitted)"
        elif role == "primary" and doc.get("is_latest"):
            role_label = "**PRIMARY — CURRENT NOTICE**"
        elif role == "primary":
            role_label = "PRIMARY (historical notice)"
        elif role == "informational":
            role_label = "INFORMATIONAL (Q&A only)"
        else:
            role_label = "REFERENCE"

        temporal = doc.get("temporal_role", "")
        meta_parts = [dt]
        if ref:
            meta_parts.append(f"Ref: {ref}")
        if dt_str:
            meta_parts.append(f"Date: {dt_str}")
        if temporal and temporal != "unknown":
            meta_parts.append(f"({temporal})")
        meta = " | ".join(meta_parts)
        lines.append(f"• `{fn}` → {role_label}  ({meta})")

    lines.append(
        "\n_If any classification is wrong, tell me:_\n"
        "_e.g. 'notice.pdf is a reference judgment'_  or  "
        "_'judgment.pdf is my previous reply'_"
    )
    return "\n".join(lines)


def snapshot_for_display(case: dict) -> dict:
    """Minimal dict for document_analysis field in retrieval event."""
    return {
        "summary": case.get("summary"),
        "issues": [
            {"id": i.get("id"), "issue_text": i.get("issue_text","")[:200],
             "status": i.get("status"), "has_reply": bool(i.get("reply"))}
            for i in case.get("issues", [])
        ],
        "parties": case.get("parties"),
        "documents": [
            {
                "filename":         d.get("filename"),
                "display_type":     d.get("display_type"),
                "role":             d.get("role"),
                "is_latest":        d.get("is_latest"),
                "temporal_role":    d.get("temporal_role"),
                "date":             d.get("date"),
                "reference_number": d.get("reference_number"),
            }
            for d in case.get("docs", [])
        ],
    }