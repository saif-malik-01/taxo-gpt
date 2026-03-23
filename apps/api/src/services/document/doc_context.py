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
        data = await redis.get(DOC_CONTEXT_KEY.format(session_id))
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
            ex=DOC_CONTEXT_TTL
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


# ─── Structure ────────────────────────────────────────────────────────────────
# Redis stores ONLY: summary, issues, parties, mode, state
# Full extracted text lives in DB (SessionDocumentText table)

def create_empty_context() -> dict:
    return {"active_case_id": None, "cases": []}


def create_new_case(case_id: int, parties: dict = None) -> dict:
    return {
        "case_id": case_id,
        "status":  "active",
        "parties": parties or {"sender": None, "recipient": None},
        "summary": "",
        "issues":  [],
        "mode":    None,
        "state":   "awaiting_decision",
    }


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
    for c in context.get("cases", []):
        if c.get("status") == "active":
            c["status"] = "archived"
    context["cases"].append(case)
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


# ─── Issue helpers ────────────────────────────────────────────────────────────

def get_pending_issues(case: dict) -> list:
    return [i for i in case.get("issues", []) if not i.get("reply")]


def merge_new_issues_with_existing(existing_issues: list, new_issues_raw: list) -> list:
    """
    Merge freshly-extracted issues with existing ones.
    Issues that already have a reply are preserved.
    New ones added as pending. Renumbers sequentially.
    """
    replied_by_text = {
        i["text"][:120]: i
        for i in existing_issues
        if i.get("reply")
    }
    merged = []
    for issue_text in new_issues_raw:
        key = issue_text[:120]
        if key in replied_by_text:
            merged.append(replied_by_text[key])
        else:
            merged.append({"id": 0, "text": issue_text, "reply": None, "status": "pending"})

    for idx, issue in enumerate(merged, 1):
        issue["id"] = idx
    return merged


def apply_issue_update(case: dict, update: dict):
    """Apply a parsed issue-list update in place."""
    issues = case.get("issues", [])
    action = update.get("action")
    ids    = [int(x) for x in (update.get("issue_ids") or [])]

    if action == "merge" and len(ids) >= 2:
        to_merge = [i for i in issues if i["id"] in ids]
        rest     = [i for i in issues if i["id"] not in ids]
        if to_merge:
            merged_text  = update.get("merge_text") or " | ".join(i["text"] for i in to_merge)
            merged_issue = {"id": min(i["id"] for i in to_merge), "text": merged_text, "reply": None, "status": "pending"}
            all_issues   = sorted(rest + [merged_issue], key=lambda x: x["id"])
            for idx, issue in enumerate(all_issues, 1):
                issue["id"] = idx
            case["issues"] = all_issues

    elif action == "add":
        new_text = (update.get("new_text") or "").strip()
        if new_text:
            new_id = max((i["id"] for i in issues), default=0) + 1
            issues.append({"id": new_id, "text": new_text, "reply": None, "status": "pending"})

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