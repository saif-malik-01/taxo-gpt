"""
api/document.py

Document feature endpoint — Feature 2.

Pipeline (per our finalized spec):

  Step 0 : Request type detection + pre-classification signal capture
  Step 1 : Page extraction (per doc, parallel, global semaphore)
           → Multi-part notice merge if same reference_number detected
  Step 2 : Parallel tracks per doc, all docs simultaneous:
             Track 2A+2C — metadata + intent (combined Qwen call)
             Track 2B    — legal entity extraction (Qwen + regex)
  Step 2 post: Temporal role adjustment via cross-doc date comparison
  Step 3 : Same-case determination (ref# → party exact → summary sim)
  Step 4 : Confirmation for ambiguous routing (emits prompt, stops)
  Step 5 : Apply routing + DB save (parallel with Step 6)
  Step 6 : Issue extraction (per primary doc, parallel)
           Replied-issue extraction (per reply doc, parallel)
  Step 6c: Document Understanding Summary block
  Step 7 : Intent routing → case handler
  Step 8 : Draft generation (up to 3 issues concurrent)
  Step 9 : Persist snapshot (always in finally)

UX CONTRACT (enforced by _emit_summary_and_issues_header):
  Every response in the document feature STARTS with:
    1. Case header (From / To / Ref)
    2. Brief summary (from primary doc(s))
    3. Numbered issues list with status icons
  Then the substantive response follows (draft, explanation, update, etc.)
  The assumptions note always appears at the END of the response.
"""

import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import time
import uuid
from typing import AsyncGenerator, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from services.auth.deps import auth_guard
from services.chat.memory_updater import auto_update_profile
from services.database import get_db
from services.document.doc_classifier import (
    adjust_temporal_roles,
    analyze_document,
    determine_route,
    extract_issues,
    extract_legal_entities,
    extract_replied_issues,
    merge_multipart_docs,
    reextract_missed_issues,
)
from services.document.doc_context import (
    add_case_to_context,
    add_document_to_case,
    append_user_context,
    apply_doc_correction,
    apply_issue_update,
    archive_active_case,
    build_case_summary,
    bump_version,
    create_doc_entry,
    create_empty_context,
    create_new_case,
    get_active_case,
    get_doc_context,
    get_draftable_issues,
    get_last_qa_pairs,
    get_next_case_id,
    get_pending_issues,
    get_user_context_text,
    mark_replied,
    merge_issues,
    push_qa_pair,
    recalculate_is_latest,
    set_doc_context,
    snapshot_for_display,
    switch_active_case,
    update_case_level_from_latest,
)
from services.document.global_semaphore import get_page_semaphore
from services.document.intent_classifier import (
    classify_intent_no_docs,
    parse_issue_update,
    rewrite_query_if_needed,
)
from services.document.issue_replier import (
    MODE_DEFENSIVE,
    MODE_IN_FAVOUR,
    build_prior_replied_pairs,
    build_reference_doc_summaries,
    process_issues_streaming,
    set_pipeline,
)
from services.document.processor import extract_document_pages
from services.document.session_doc_store import (
    delete_session_documents,
    get_primary_texts,
    get_reference_texts,
    get_text_by_filename,
    save_document_text,
)
from services.memory import add_message, check_credits, get_session_history, track_usage
from services.models import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/document", tags=["Document"])

SUPPORTED = {
    ".pdf", ".docx", ".pptx", ".xlsx", ".html",
    ".png", ".jpg", ".jpeg", ".tiff", ".bmp",
}
_MIN_WORDS_FOR_PROFILE = 8


# ─────────────────────────────────────────────────────────────────────────────
# Startup: inject pipeline into issue_replier
# ─────────────────────────────────────────────────────────────────────────────

def init_pipeline():
    from retrieval.pipeline import RetrievalPipeline
    p = RetrievalPipeline()
    p.setup()
    set_pipeline(p)
    logger.info("RetrievalPipeline injected into issue_replier")


# ─────────────────────────────────────────────────────────────────────────────
# NDJSON helpers
# ─────────────────────────────────────────────────────────────────────────────

def _emit(data: dict) -> str:
    return json.dumps(data, ensure_ascii=False) + "\n"

def _content(text: str) -> str:
    return _emit({"type": "content", "delta": text})

def _retrieval_event(session_id: str, message_id=None, sources=None, document_analysis=None) -> str:
    return _emit({
        "type": "retrieval",
        "sources": sources or [],
        "message_id": message_id,
        "session_id": session_id,
        "id": message_id,
        "document_analysis": document_analysis,
    })

def _should_update_profile(q: str) -> bool:
    return len(q.strip().split()) >= _MIN_WORDS_FOR_PROFILE


# ─────────────────────────────────────────────────────────────────────────────
# Helper: get DB user
# ─────────────────────────────────────────────────────────────────────────────

async def _get_db_user(email: str, db: AsyncSession):
    result = await db.execute(select(User).where(func.lower(User.email) == email.lower()))
    db_user = result.scalars().first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user


# ─────────────────────────────────────────────────────────────────────────────
# STEP 0 — Pre-classification signal capture
# ─────────────────────────────────────────────────────────────────────────────

def _extract_upload_hints(filename: str, user_message: str) -> List[str]:
    """Extract soft role/temporal signals from filename and user message."""
    hints = []
    fname_lower = filename.lower()
    msg_lower   = (user_message or "").lower()

    # Filename hints
    if any(k in fname_lower for k in ("new_", "current_", "latest_", "received_")):
        hints.append("filename:current")
    if any(k in fname_lower for k in ("old_", "prev_", "earlier_", "prior_", "hist_")):
        hints.append("filename:historical")
    if any(k in fname_lower for k in ("reply_", "response_", "my_reply", "draft_")):
        hints.append("filename:reply")
    if any(k in fname_lower for k in ("judgment_", "order_", "hc_", "sc_", "tribunal_")):
        hints.append("filename:reference")
    if any(k in fname_lower for k in ("circular_", "notif_", "notification_")):
        hints.append("filename:reference")

    # Message hints
    if any(k in msg_lower for k in ("just received", "current notice", "new notice", "latest notice", "received today", "got today")):
        hints.append("msg:current_notice")
    if any(k in msg_lower for k in ("old notice", "previous notice", "earlier notice", "old case", "prior notice", "already replied")):
        hints.append("msg:historical_notice")
    if any(k in msg_lower for k in ("my reply", "my draft", "i prepared", "draft reply", "prepared by me")):
        hints.append("msg:user_draft")
    if any(k in msg_lower for k in ("for reference", "reference document", "judgment", "circular", "for context")):
        hints.append("msg:reference")
    if any(k in msg_lower for k in ("defend", "defensive", "protection", "protect")):
        hints.append("msg:defensive")
    if any(k in msg_lower for k in ("in favour", "in favor", "department side", "revenue side")):
        hints.append("msg:in_favour")
    if any(k in msg_lower for k in ("different case", "new case", "other matter", "different client", "another notice")):
        hints.append("msg:new_case")

    return hints


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Extract all uploaded documents (parallel)
# ─────────────────────────────────────────────────────────────────────────────

async def _extract_all_documents(
    temp_file_paths: List[tuple],  # [(tmp_path, ext, filename)]
    user_message: str,
) -> tuple:  # (extracted_docs, errors)
    errors = []

    async def _extract_one(tmp_path: str, ext: str, filename: str) -> Optional[dict]:
        full_text, page_count, error = await extract_document_pages(tmp_path, filename)
        if error:
            errors.append(error)
            return None
        hints = _extract_upload_hints(filename, user_message)
        return {
            "filename":    filename,
            "full_text":   full_text,
            "page_count":  page_count,
            "upload_hints": hints,
        }

    t0      = time.monotonic()
    tasks   = [_extract_one(tp, ext, fn) for tp, ext, fn in temp_file_paths]
    results = await asyncio.gather(*tasks)
    extracted = [r for r in results if r is not None]
    logger.info(
        f"Step 1 complete: {len(extracted)}/{len(temp_file_paths)} docs "
        f"in {time.monotonic()-t0:.1f}s"
    )
    return extracted, errors


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Parallel tracks 2A+2C and 2B
# ─────────────────────────────────────────────────────────────────────────────

async def _run_step2(
    extracted_docs: List[dict],
    resolved_question: str,
    snapshot: dict,
) -> tuple:  # (analyses_list, entities_cache)
    active_case    = get_active_case(snapshot)
    user_ctx_text  = get_user_context_text(active_case, limit=3) if active_case else ""
    active_info    = None
    if active_case:
        active_info = {
            "parties":          active_case.get("parties"),
            "reference_number": active_case.get("reference_number"),
            "existing_docs": [
                {"date": d.get("date"), "reference_number": d.get("reference_number"),
                 "role": d.get("role"), "brief_summary": d.get("brief_summary","")}
                for d in active_case.get("docs", [])[:5]
            ],
        }

    async def _analyze_one(doc: dict) -> dict:
        t0 = time.monotonic()
        r  = await run_in_threadpool(
            analyze_document,
            doc["full_text"],
            resolved_question,
            user_ctx_text,
            active_info,
            doc.get("upload_hints", []),
        )
        r["filename"]    = doc["filename"]
        r["_page_count"] = doc.get("page_count", 0)
        logger.info(
            f"2A+2C '{doc['filename']}': role={r.get('role')} "
            f"temporal={r.get('temporal_role')} has_issues={r.get('has_issues')} "
            f"({time.monotonic()-t0:.1f}s)"
        )
        return r

    async def _entities_one(doc: dict) -> tuple:
        t0 = time.monotonic()
        e  = await run_in_threadpool(extract_legal_entities, doc["full_text"])
        logger.info(f"2B '{doc['filename']}': sections={len(e.get('sections',[]))} ({time.monotonic()-t0:.1f}s)")
        return doc["filename"], e

    t0 = time.monotonic()
    all_results = await asyncio.gather(
        *[_analyze_one(d) for d in extracted_docs],
        *[_entities_one(d) for d in extracted_docs],
        return_exceptions=True,
    )
    n = len(extracted_docs)

    analyses_list = []
    for r in all_results[:n]:
        if isinstance(r, Exception):
            logger.error(f"2A+2C error: {r}")
            analyses_list.append(None)
        else:
            analyses_list.append(r)

    entities_cache: Dict[str, dict] = {}
    for r in all_results[n:]:
        if isinstance(r, Exception):
            logger.error(f"2B error: {r}")
        else:
            filename, entities = r
            entities_cache[filename] = entities

    logger.info(f"Step 2 total: {time.monotonic()-t0:.1f}s")
    return [a for a in analyses_list if a is not None], entities_cache


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Apply routing + DB save
# ─────────────────────────────────────────────────────────────────────────────

async def _apply_routing(
    routing_plan: List[dict],
    extracted_docs: List[dict],
    snapshot: dict,
    session_id: str,
) -> None:
    text_map  = {d["filename"]: d["full_text"] for d in extracted_docs}
    confirmed = [r for r in routing_plan if not r.get("needs_confirmation")]

    # Process new-case routes first (so we have a case to attach to)
    new_routes = [r for r in confirmed if "new_case" in r["route"]]
    add_routes = [r for r in confirmed if "new_case" not in r["route"]]

    for entry in new_routes + add_routes:
        route    = entry["route"]
        analysis = entry["analysis"]
        filename = entry["filename"]
        role     = analysis.get("role", "reference")

        if "new_case" in route:
            if get_active_case(snapshot):
                archive_active_case(snapshot)
            cid   = get_next_case_id(snapshot)
            p     = analysis.get("parties") or {}
            case  = create_new_case(cid, p)
            add_case_to_context(snapshot, case)

        active_case = get_active_case(snapshot)
        if not active_case:
            cid  = get_next_case_id(snapshot)
            p    = analysis.get("parties") or {}
            case = create_new_case(cid, p)
            add_case_to_context(snapshot, case)
            active_case = case

        doc_entry = create_doc_entry(
            filename         = filename,
            role             = role,
            role_locked      = analysis.get("role_locked", False),
            display_type     = analysis.get("display_type", "notice"),
            temporal_role    = analysis.get("temporal_role", "unknown"),
            temporal_locked  = analysis.get("temporal_locked", False),
            is_latest        = False,   # recalculated below
            date             = analysis.get("date"),
            reference_number = analysis.get("reference_number"),
            parties          = analysis.get("parties") or {},
            brief_summary    = analysis.get("brief_summary", ""),
            confidence       = analysis.get("confidence", 0),
            has_issues       = analysis.get("has_issues", False),
            has_replied_issues = analysis.get("has_replied_issues", False),
            part_doc_ids     = analysis.get("part_filenames", []),
            upload_hints     = analysis.get("upload_hints", []),
        )
        doc_entry["page_count"]       = analysis.get("_page_count", 0)
        doc_entry["pipeline_status"]  = "committed"
        add_document_to_case(active_case, doc_entry)

        # Reply linkage
        if role in ("previous_reply", "user_draft_reply"):
            ref_num = analysis.get("reference_number", "").strip()
            for primary in active_case.get("docs", []):
                if primary.get("role") == "primary":
                    if ref_num and primary.get("reference_number", "").strip() == ref_num:
                        doc_entry["replies_to_doc_id"] = primary["doc_id"]
                        mark_replied(active_case, primary["filename"], filename)
                        break

        if role == "primary":
            for field in ("authority", "taxpayer_name", "gstin", "pan"):
                p = analysis.get("parties") or {}
                if not (active_case.get("parties") or {}).get(field):
                    active_case.setdefault("parties", {})[field] = (
                        p.get("sender")    if field == "authority"
                        else p.get("recipient") if field == "taxpayer_name"
                        else p.get(field)
                    )

    # Recalculate is_latest and update case-level metadata
    for case in snapshot.get("cases", {}).values():
        recalculate_is_latest(case)
        update_case_level_from_latest(case)
        case["summary"] = build_case_summary(case)

    bump_version(snapshot)

    # DB save in parallel
    save_tasks = []
    for entry in confirmed:
        filename  = entry["filename"]
        analysis  = entry["analysis"]
        full_text = text_map.get(filename, "")
        if not full_text:
            continue
        role = analysis.get("role", "reference")
        ac   = get_active_case(snapshot)
        if ac:
            doc_type = (
                "reply_reference" if role in ("previous_reply", "user_draft_reply")
                else "primary"    if role == "primary"
                else "reference"
            )
            save_tasks.append(
                save_document_text(session_id, ac["case_id"], filename, doc_type, full_text)
            )
    if save_tasks:
        await asyncio.gather(*save_tasks)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Issue + replied-issue extraction
# ─────────────────────────────────────────────────────────────────────────────

async def _extract_all_issues(
    routing_plan: List[dict],
    snapshot: dict,
    session_id: str,
) -> None:
    active_case = get_active_case(snapshot)
    if not active_case:
        return

    primary_entries = [
        r for r in routing_plan
        if not r.get("needs_confirmation")
        and r["analysis"].get("role") == "primary"
        and r["analysis"].get("has_issues") is not False
    ]
    reply_entries = [
        r for r in routing_plan
        if not r.get("needs_confirmation")
        and r["analysis"].get("role") in ("previous_reply", "user_draft_reply")
        and r["analysis"].get("has_replied_issues")
    ]

    async def _issues_one(entry: dict):
        fn       = entry["filename"]
        analysis = entry["analysis"]
        if not analysis.get("has_issues"):
            return fn, []
        t0       = time.monotonic()
        full_txt = await get_text_by_filename(session_id, active_case["case_id"], fn)
        if not full_txt:
            full_txt = await get_primary_texts(session_id, active_case["case_id"])
        if not full_txt:
            return fn, []
        new_issues = await run_in_threadpool(extract_issues, full_txt, active_case.get("issues", []))
        logger.info(f"Step 6a '{fn}': {len(new_issues)} issues ({time.monotonic()-t0:.1f}s)")
        return fn, new_issues

    async def _replied_one(entry: dict):
        fn  = entry["filename"]
        t0  = time.monotonic()
        txt = await get_text_by_filename(session_id, active_case["case_id"], fn)
        if not txt:
            return fn, []
        pairs = await run_in_threadpool(extract_replied_issues, txt)
        logger.info(f"Step 6b '{fn}': {len(pairs)} pairs ({time.monotonic()-t0:.1f}s)")
        return fn, pairs

    results = await asyncio.gather(
        *[_issues_one(r) for r in primary_entries],
        *[_replied_one(r) for r in reply_entries],
        return_exceptions=True,
    )

    n_primary = len(primary_entries)
    for r in results[:n_primary]:
        if isinstance(r, Exception):
            logger.error(f"Step 6a error: {r}")
            continue
        fn, new_issues = r
        if new_issues:
            source_doc_id = next(
                (d["doc_id"] for d in active_case.get("docs", []) if d["filename"] == fn),
                fn,
            )
            active_case["issues"] = merge_issues(
                active_case.get("issues", []), new_issues, source_doc_id, fn
            )

    for r in results[n_primary:]:
        if isinstance(r, Exception):
            logger.error(f"Step 6b error: {r}")
            continue
        fn, replied_pairs = r
        if replied_pairs:
            for doc in active_case.get("docs", []):
                if doc.get("filename") == fn:
                    doc["replied_issues"] = replied_pairs
                    break
            for pair in replied_pairs:
                pt = pair.get("issue_text", "")
                for iss in active_case.get("issues", []):
                    a, b   = pt[:80].lower(), iss.get("issue_text", "")[:80].lower()
                    shorter = min(len(a), len(b))
                    if shorter > 0:
                        common = sum(1 for x, y in zip(a, b) if x == y)
                        if common / shorter > 0.80:
                            if not iss.get("reply"):
                                iss["status"]         = "has_reply_doc"
                                iss["replied_by_doc"] = fn
                            break

    active_case["summary"] = build_case_summary(active_case)
    bump_version(snapshot)


# ─────────────────────────────────────────────────────────────────────────────
# UNIVERSAL HEADER — emitted at the start of EVERY document feature response
# ─────────────────────────────────────────────────────────────────────────────

def _build_summary_and_issues_header(active_case: dict) -> str:
    """
    Build the persistent header block that ALWAYS appears at the top of every
    document feature response.

    Structure:
        [Case header: From / To / Ref]

        [Brief summary from primary doc(s)]

        **Issues / Allegations:**
        1. <issue text> [status icon]
        2. <issue text> [status icon]
        ...

    Status icons:
        ✅  — reply generated
        📄  — covered by an uploaded reply doc
        (no icon) — pending

    This block is compact — issue texts are truncated to 180 chars so the
    header does not dwarf the actual response.
    """
    if not active_case:
        return ""

    lines = []

    # ── Case header ───────────────────────────────────────────────────────────
    p = active_case.get("parties", {}) or {}
    header_parts = []
    if p.get("authority"):
        header_parts.append(f"**From:** {p['authority']}")
    if p.get("taxpayer_name"):
        header_parts.append(f"**To:** {p['taxpayer_name']}")
    if active_case.get("reference_number"):
        header_parts.append(f"**Ref:** {active_case['reference_number']}")
    if header_parts:
        lines.append("  ".join(header_parts))
        lines.append("")

    # ── Brief summary ─────────────────────────────────────────────────────────
    summary = (active_case.get("summary") or "").strip()
    if summary:
        lines.append(summary)
        lines.append("")

    # ── Issues list ───────────────────────────────────────────────────────────
    issues = active_case.get("issues", [])
    if issues:
        lines.append("**Issues / Allegations:**")
        lines.append("")
        for iss in issues:
            has_reply = bool(iss.get("reply"))
            has_doc   = iss.get("status") == "has_reply_doc"
            if has_reply:
                icon = " ✅"
            elif has_doc:
                icon = " 📄"
            else:
                icon = ""
            text = iss.get("issue_text", "")
            if len(text) > 180:
                text = text[:177] + "..."
            lines.append(f"**{iss.get('id', '?')}.** {text}{icon}")
            lines.append("")

    # Pending / replied counts as a one-liner
    if issues:
        pending_count = sum(
            1 for i in issues
            if not i.get("reply") and i.get("status") not in ("replied", "has_reply_doc")
        )
        replied_count = sum(1 for i in issues if i.get("reply"))
        if pending_count > 0:
            lines.append(
                f"_{pending_count} issue(s) pending reply  |  "
                f"{replied_count} replied_"
            )
        else:
            lines.append(f"_All {replied_count} issue(s) replied_")
        lines.append("")

    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def _build_assumptions_note(active_case: dict, mode: str = None) -> str:
    """
    Short assumptions note — always at the END of the response.
    Only shown when there is something worth noting.
    """
    docs  = active_case.get("docs", []) if active_case else []
    parts = []
    for d in docs:
        fn       = d.get("filename", "document")
        role     = d.get("role", "reference")
        temporal = d.get("temporal_role", "")
        if role == "primary" and temporal == "current":
            parts.append(f"`{fn}` — current notice requiring reply")
        elif role == "primary" and temporal == "historical":
            parts.append(f"`{fn}` — historical notice (context only)")
        elif role == "primary":
            parts.append(f"`{fn}` — primary notice")
        elif role == "previous_reply":
            parts.append(f"`{fn}` — previously submitted reply")
        elif role == "user_draft_reply":
            parts.append(f"`{fn}` — your draft reply (not yet submitted)")
        elif role == "reference":
            parts.append(f"`{fn}` — reference document")
        elif role == "informational":
            parts.append(f"`{fn}` — informational document")
    if mode:
        mode_label = (
            "Defensive (protecting the taxpayer)"
            if mode == MODE_DEFENSIVE
            else "In Favour (supporting the department)"
        )
        parts.append(f"reply mode: {mode_label}")
    if not parts:
        return ""
    lines = ["\n---\n_Assumptions:_"]
    for pt in parts:
        lines.append(f"_{pt}_")
    lines.append("_Tell me if any assumption is wrong._")
    return "\n".join(lines)


async def _stream_header(active_case: dict) -> AsyncGenerator[str, None]:
    """Yield the summary+issues header as chunked content events."""
    header = _build_summary_and_issues_header(active_case)
    if not header:
        return
    for i in range(0, len(header), 400):
        yield _content(header[i : i + 400])


# ─────────────────────────────────────────────────────────────────────────────
# Case handlers
# ─────────────────────────────────────────────────────────────────────────────

async def _handle_show_summary(
    active_case: dict, session_id: str, user_id: int
) -> AsyncGenerator[str, None]:
    """
    Shown when user uploads with no question, or explicitly asks for summary.

    Output order:
        [Header: case header + summary + issues list]   ← _build_summary_and_issues_header
        [Mode question / what you can do next]
        [Assumptions note]
    """
    issues        = active_case.get("issues", [])
    pending_count = sum(
        1 for i in issues
        if not i.get("reply") and i.get("status") not in ("replied", "has_reply_doc")
    )
    replied_count = sum(1 for i in issues if i.get("reply"))

    # 1. Emit the universal header
    async for chunk in _stream_header(active_case):
        yield chunk

    # 2. What to do next
    body_lines = []
    if issues:
        if pending_count > 0:
            body_lines.append(
                f"**{pending_count}** issue(s) need a reply. "
                "Specify the mode to start drafting:\n"
                "- **Defensive** — protect the taxpayer / notice recipient\n"
                "- **In Favour** — support the notice / department position\n\n"
                "You can also:\n"
                "- **Explain** any issue before replying — _'explain issue 3'_\n"
                "- Tell me if an issue is **missing** — _'you missed the interest issue'_\n"
                "- **Merge** two issues — _'merge issues 3 and 5'_"
            )
        elif replied_count == len(issues):
            body_lines.append(
                "All issues have replies.\n\n"
                "You can:\n"
                "- **Improve** any reply — _'redo issue 2 with more detail'_\n"
                "- Add your own facts — _'for issue 4, we have all invoices — redo'_\n"
                "- **Merge** two replies — _'merge replies for issues 3 and 5'_"
            )
        else:
            body_lines.append(
                f"{pending_count} issue(s) pending. "
                "Say the mode (**Defensive** or **In Favour**) to start drafting."
            )
    else:
        body_lines.append("No specific allegations found in this document.")

    body_lines.append(_build_assumptions_note(active_case, active_case.get("mode")))
    body = "\n\n".join(body_lines)

    for i in range(0, len(body), 400):
        yield _content(body[i : i + 400])

    full_text = _build_summary_and_issues_header(active_case) + body
    active_case["state"] = "awaiting_decision"
    asst = await add_message(session_id, "assistant", full_text, user_id)
    yield _retrieval_event(
        session_id, getattr(asst, "id", None),
        document_analysis=snapshot_for_display(active_case),
    )


async def _handle_draft_issues(
    active_case: dict,
    issues_to_draft: List[dict],
    session_id: str,
    user_id: int,
    question: str,
    background_tasks: BackgroundTasks,
    db: AsyncSession,
    snapshot: dict,
    skip_confirmation: bool = False,
) -> AsyncGenerator[str, None]:
    """
    Draft replies for specified issues.

    Output order:
        [Header: case header + summary + issues list]
        [Issue confirmation step — if not already confirmed]
         OR
        [Draft reply header]
        [Per-issue: issue text → streamed reply paragraphs]
        [Closing]
        [Assumptions note]
    """
    mode       = active_case.get("mode", MODE_DEFENSIVE)
    mode_label = "Defensive" if mode == MODE_DEFENSIVE else "In Favour"
    total      = len(active_case.get("issues", []))
    prior_replied = build_prior_replied_pairs(active_case)

    # Case-level summary and recipient for the draft prompt (spec §3.2)
    case_summary   = (active_case.get("summary") or "").strip()
    recipient_name = (active_case.get("parties") or {}).get("taxpayer_name") or ""

    # Full reference document text from DB — passed verbatim to draft prompt (spec §3.2)
    # Falls back to empty string if no reference docs uploaded
    reference_doc_full_text = ""
    try:
        reference_doc_full_text = await get_reference_texts(
            session_id, active_case.get("case_id", 0)
        )
    except Exception as _ref_err:
        logger.warning(f"Could not fetch reference texts: {_ref_err}")

    # ── Always emit the persistent header first ───────────────────────────────
    async for chunk in _stream_header(active_case):
        yield chunk

    # ── Issue confirmation step ───────────────────────────────────────────────
    if not skip_confirmation and active_case.get("state") != "issue_confirmation_sent":
        conf_lines = [
            f"I'll prepare replies for these **{len(issues_to_draft)}** "
            f"issue(s) in **{mode_label}** mode:\n"
        ]
        for iss in issues_to_draft:
            conf_lines.append(f"**{iss.get('id','?')}.** {iss.get('issue_text','')}")
            conf_lines.append("")
        conf_lines.append(
            "\nIf any issue is missing or incorrect, tell me now. "
            "Otherwise reply **'go ahead'** and I'll start drafting."
        )
        conf_msg = "\n".join(conf_lines)
        for i in range(0, len(conf_msg), 400):
            yield _content(conf_msg[i : i + 400])
        active_case["state"]                = "awaiting_issue_confirmation"
        active_case["_pending_draft_ids"]   = [iss.get("id") for iss in issues_to_draft]
        full_text = _build_summary_and_issues_header(active_case) + conf_msg
        asst = await add_message(session_id, "assistant", full_text, user_id)
        yield _retrieval_event(
            session_id, getattr(asst, "id", None),
            document_analysis=snapshot_for_display(active_case),
        )
        return

    # ── Draft reply header ────────────────────────────────────────────────────
    draft_hdr  = f"## Draft Reply\n\n"
    draft_hdr += f"**Mode:** {mode_label}  "
    draft_hdr += f"**Issues addressed:** {len(issues_to_draft)} of {total}\n\n"
    draft_hdr += "**Issues being replied:**\n"
    for iss in issues_to_draft:
        txt = iss.get("issue_text", "")
        draft_hdr += (
            f"- Issue {iss.get('id','?')}: "
            f"{txt[:140]}{'...' if len(txt) > 140 else ''}\n"
        )
    draft_hdr += "\n---\n"

    for i in range(0, len(draft_hdr), 400):
        yield _content(draft_hdr[i : i + 400])

    # ── Stream replies ────────────────────────────────────────────────────────
    active_case["state"]              = "reply_in_progress"
    active_case["_pending_draft_ids"] = None

    all_sources     = []
    full_reply_text = _build_summary_and_issues_header(active_case) + draft_hdr

    async for iss_num, reply, sources in process_issues_streaming(
        issues                  = issues_to_draft,
        mode                    = mode,
        case_summary            = case_summary,
        recipient_name          = recipient_name,
        prior_replied_pairs     = prior_replied,
        reference_doc_full_text = reference_doc_full_text,
        max_parallel            = 3,
    ):
        iss_obj   = issues_to_draft[iss_num - 1]
        global_id = iss_obj.get("id", iss_num)
        iss_text  = iss_obj.get("issue_text", "")

        header = (
            f"\n\n---\n\n### Issue {global_id} of {total}\n\n"
            f"> {iss_text}\n\n"
        )
        yield _content(header)
        yield _emit({
            "type": "issue_start",
            "issue_number": global_id,
            "issue_text": iss_text,
            "total_issues": total,
        })

        for i in range(0, len(reply), 80):
            yield _content(reply[i : i + 80])
        yield _emit({"type": "issue_end", "issue_number": global_id})

        full_reply_text += f"\n\n### Issue {global_id}: {iss_text}\n\n{reply}"

        for iss in active_case.get("issues", []):
            if iss.get("id") == global_id:
                iss["reply"]  = reply
                iss["status"] = "replied"
                break
        all_sources.extend(sources)

    # ── Closing + assumptions ─────────────────────────────────────────────────
    closing = (
        "\n\n---\n\n**Respectfully submitted.**\n\n"
        "*For the Taxpayer / Assessee*\n\n"
        "Authorised Signatory / Chartered Accountant / Legal Representative"
        "\n\nDate: [Insert Date]"
    )
    closing += _build_assumptions_note(active_case, mode)
    for i in range(0, len(closing), 80):
        yield _content(closing[i : i + 80])
    full_reply_text += closing

    active_case["state"] = "complete"
    push_qa_pair(snapshot, question, full_reply_text[:1200])
    asst = await add_message(session_id, "assistant", full_reply_text, user_id)
    yield _retrieval_event(
        session_id, getattr(asst, "id", None),
        sources=all_sources,
        document_analysis=snapshot_for_display(active_case),
    )
    if _should_update_profile(question):
        background_tasks.add_task(auto_update_profile, user_id, question, full_reply_text)


async def _handle_update_issues(
    active_case: dict, question: str, session_id: str, user_id: int
) -> AsyncGenerator[str, None]:
    """
    Handle issue list corrections: missed issues, merge, add, remove, correct.

    Output order:
        [Header: case header + summary + updated issues list]
        [Action result + updated list]
    """
    update = await run_in_threadpool(
        parse_issue_update, question, active_case.get("issues", [])
    )
    action = update.get("action")

    if action == "reextract":
        full_text = await get_primary_texts(session_id, active_case["case_id"])
        if not full_text.strip():
            # Emit header then error
            async for chunk in _stream_header(active_case):
                yield chunk
            msg = (
                "Could not find the original document text. "
                "Please describe the missing issue directly."
            )
            yield _content(msg)
            await add_message(session_id, "assistant",
                              _build_summary_and_issues_header(active_case) + msg, user_id)
            return
        new_texts = await run_in_threadpool(
            reextract_missed_issues, full_text, active_case.get("issues", [])
        )
        if new_texts:
            latest_primary = next(
                (d for d in active_case.get("docs", [])
                 if d.get("role") == "primary" and d.get("is_latest")),
                None,
            )
            src_id = latest_primary["doc_id"]   if latest_primary else "reextracted"
            src_fn = latest_primary["filename"] if latest_primary else "reextracted"
            active_case["issues"] = merge_issues(
                active_case.get("issues", []), new_texts, src_id, src_fn
            )
            suffix = "\n\nShould I generate replies for the new issues?"
        else:
            suffix = (
                "\n\nI re-read the document but found no additional issues. "
                "Could you describe the missing issue? "
                "Please tell me which paragraph or section it appears in."
            )
    else:
        apply_issue_update(active_case, update)
        action_label = {
            "merge":   "Merged issues",
            "add":     "Added issue",
            "remove":  "Removed issue",
            "correct": "Corrected issue",
        }.get(action, "Updated issues")
        suffix = f"\n\n{action_label} successfully."
        if get_pending_issues(active_case):
            suffix += " Should I generate replies for the updated issue(s)?"

    # Emit updated header (includes the now-modified issues list)
    async for chunk in _stream_header(active_case):
        yield chunk

    for i in range(0, len(suffix), 400):
        yield _content(suffix[i : i + 400])

    full_text = _build_summary_and_issues_header(active_case) + suffix
    asst = await add_message(session_id, "assistant", full_text, user_id)
    yield _retrieval_event(session_id, getattr(asst, "id", None))


async def _handle_explain_issues(
    active_case: dict,
    issue_ids: List[int],
    question: str,
    session_id: str,
    user_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession,
    snapshot: dict,
) -> AsyncGenerator[str, None]:
    """
    Explain specific issues in plain language.

    Output order:
        [Header]
        [Per-issue: issue text → explanation]
        [Closing prompt]
    """
    from services.document.issue_replier import retrieve_for_issue, _get_llm as _get_issue_llm

    issues = active_case.get("issues", [])
    if issue_ids:
        targets = [i for i in issues if i.get("id") in issue_ids]
    else:
        targets = [i for i in issues if i.get("status") == "pending" and not i.get("reply")]
    if not targets:
        async for chunk in _stream_header(active_case):
            yield chunk
        msg = "I couldn't find the specified issue(s). Please check the issue number(s)."
        yield _content(msg)
        await add_message(session_id, "assistant",
                          _build_summary_and_issues_header(active_case) + msg, user_id)
        return

    mode = active_case.get("mode", MODE_DEFENSIVE)

    # Emit persistent header
    async for chunk in _stream_header(active_case):
        yield chunk

    full_response = _build_summary_and_issues_header(active_case)

    for iss in targets:
        iss_id   = iss.get("id", "?")
        iss_text = iss.get("issue_text", "")

        header = (
            f"\n\n---\n\n### Issue {iss_id} — Explanation\n\n"
            f"**Allegation:**\n> {iss_text}\n\n"
        )
        yield _content(header)
        full_response += header

        top_chunks = await run_in_threadpool(retrieve_for_issue, iss_text, mode)
        chunks_text = "\n\n".join(
            f"[{c.payload.get('chunk_type','').upper()}] {c.payload.get('text','')[:600]}"
            for c in top_chunks[:8]
        )
        explain_prompt = (
            f"Explain this tax allegation in plain language for a taxpayer:\n\n"
            f"ALLEGATION:\n{iss_text}\n\n"
            f"RELEVANT LEGAL MATERIAL:\n{chunks_text}\n\n"
            f"Explain:\n"
            f"1. What this allegation means in simple terms\n"
            f"2. Which law / section / rule it invokes\n"
            f"3. What the key legal question is\n"
            f"4. What a typical defence for this type of allegation looks like\n\n"
            f"Keep the explanation concise and use plain language."
        )
        explanation = await run_in_threadpool(
            _get_issue_llm().call,
            "You are a tax law expert explaining legal matters in plain language.",
            explain_prompt,
            1024,
            0.1,
            f"explain_issue_{iss_id}",
        )
        explanation = (explanation or "Could not generate explanation.").strip()

        for i in range(0, len(explanation), 80):
            yield _content(explanation[i : i + 80])
        full_response += explanation

    closing = "\n\n---\n\nWould you like me to now draft a reply for any of these issues?"
    yield _content(closing)
    full_response += closing

    asst = await add_message(session_id, "assistant", full_response, user_id)
    yield _retrieval_event(
        session_id, getattr(asst, "id", None),
        document_analysis=snapshot_for_display(active_case),
    )


async def _handle_update_reply_with_scenario(
    active_case: dict,
    issue_id: int,
    user_scenario: str,
    question: str,
    session_id: str,
    user_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession,
    snapshot: dict,
) -> AsyncGenerator[str, None]:
    """
    Redo one issue's reply incorporating user's specific facts/scenario.
    Uses the same retrieve → rerank → draft flow as process_issues_streaming,
    but for a single issue with an optional user scenario appended.

    Output order:
        [Header]
        [Issue header + user scenario note]
        [Revised reply]
    """
    from services.document.issue_replier import (
        _retrieve_for_issue_sync,
        _build_draft_prompt,
        _build_system_prompt,
        _get_llm as _get_issue_llm,
        build_prior_replied_pairs as _bpp,
    )

    all_issues = active_case.get("issues", [])
    target     = next((i for i in all_issues if i.get("id") == issue_id), None)
    if not target:
        async for chunk in _stream_header(active_case):
            yield chunk
        msg = f"Issue {issue_id} not found."
        yield _content(msg)
        await add_message(session_id, "assistant",
                          _build_summary_and_issues_header(active_case) + msg, user_id)
        return

    mode     = active_case.get("mode", MODE_DEFENSIVE)
    iss_text = target.get("issue_text", "")

    # Emit persistent header
    async for chunk in _stream_header(active_case):
        yield chunk

    issue_hdr = f"\n\n### Updated Reply — Issue {issue_id}\n\n> {iss_text}\n\n"
    if user_scenario:
        issue_hdr += f"_Your context: {user_scenario}_\n\n"
    yield _content(issue_hdr)

    # Retrieve + rerank using same pipeline as process_issues_streaming
    top_chunks = await run_in_threadpool(_retrieve_for_issue_sync, iss_text, mode)

    # Fetch full reference text from DB
    reference_doc_full_text = ""
    try:
        reference_doc_full_text = await get_reference_texts(
            session_id, active_case.get("case_id", 0)
        )
    except Exception as _e:
        logger.warning(f"Could not fetch reference texts for update_reply: {_e}")

    prior_replied  = _bpp(active_case)
    case_summary   = (active_case.get("summary") or "").strip()
    recipient_name = (active_case.get("parties") or {}).get("taxpayer_name") or ""

    # Inject user scenario as the user_draft_text slot (spec §3.2 block 6)
    user_draft_text = (
        f"USER'S SPECIFIC FACTS / SCENARIO FOR THIS ISSUE:\n{user_scenario}\n\n"
        "Incorporate these specific facts into the reply. "
        "Tailor all legal arguments to these exact facts."
    ) if user_scenario else None

    system_prompt = _build_system_prompt(mode)
    user_message  = _build_draft_prompt(
        issue_text              = iss_text,
        top_chunks              = top_chunks,
        all_issues              = all_issues,
        current_issue_id        = issue_id,
        prior_replied_pairs     = prior_replied,
        reference_doc_full_text = reference_doc_full_text,
        case_summary            = case_summary,
        recipient_name          = recipient_name,
        user_draft_text         = user_draft_text,
    )

    reply = await run_in_threadpool(
        _get_issue_llm().call,
        system_prompt, user_message, 8192, 0.2, f"update_reply_{issue_id}",
    )
    reply = (reply or "").strip()

    for i in range(0, len(reply), 80):
        yield _content(reply[i : i + 80])

    for iss in all_issues:
        if iss.get("id") == issue_id:
            iss["reply"]  = reply
            iss["status"] = "replied"
            break

    full_response = _build_summary_and_issues_header(active_case) + issue_hdr + reply
    push_qa_pair(snapshot, question, reply[:800])
    asst = await add_message(session_id, "assistant", full_response, user_id)
    yield _retrieval_event(
        session_id, getattr(asst, "id", None),
        document_analysis=snapshot_for_display(active_case),
    )


async def _handle_merge_replies(
    active_case: dict,
    issue_ids: List[int],
    question: str,
    session_id: str,
    user_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession,
    snapshot: dict,
) -> AsyncGenerator[str, None]:
    """
    Merge two or more issues and their replies into one consolidated reply.
    Retrieves for the combined issue text, then drafts one flowing reply.

    Output order:
        [Header]
        [Merge header listing involved issues]
        [Consolidated reply]
    """
    from services.document.issue_replier import (
        _retrieve_for_issue_sync,
        _build_system_prompt,
        _get_llm as _get_issue_llm,
        build_prior_replied_pairs as _bpp,
    )

    all_issues = active_case.get("issues", [])
    targets    = [i for i in all_issues if i.get("id") in issue_ids]
    if len(targets) < 2:
        async for chunk in _stream_header(active_case):
            yield chunk
        msg = "Please specify at least 2 issue numbers to merge (e.g. 'merge issues 3 and 5')."
        yield _content(msg)
        await add_message(session_id, "assistant",
                          _build_summary_and_issues_header(active_case) + msg, user_id)
        return

    mode    = active_case.get("mode", MODE_DEFENSIVE)
    ids_str = " & ".join(str(t.get("id", "?")) for t in targets)

    # Emit persistent header
    async for chunk in _stream_header(active_case):
        yield chunk

    merge_hdr = f"\n\n### Merged Reply — Issues {ids_str}\n\n"
    for t in targets:
        merge_hdr += f"**Issue {t.get('id','?')}:** {t.get('issue_text','')}\n\n"
    yield _content(merge_hdr)

    # Combine all allegation texts — this is the retrieval query
    combined_text = "\n\n".join(
        f"ALLEGATION {i+1}:\n{t.get('issue_text','')}"
        for i, t in enumerate(targets)
    )

    # Retrieve for combined query using same pipeline (retrieve + rerank)
    top_chunks = await run_in_threadpool(_retrieve_for_issue_sync, combined_text, mode)

    # Fetch full reference text from DB
    reference_doc_full_text = ""
    try:
        reference_doc_full_text = await get_reference_texts(
            session_id, active_case.get("case_id", 0)
        )
    except Exception as _e:
        logger.warning(f"Could not fetch reference texts for merge_replies: {_e}")

    prior_replied  = _bpp(active_case)
    case_summary   = (active_case.get("summary") or "").strip()
    recipient_name = (active_case.get("parties") or {}).get("taxpayer_name") or ""

    # Existing replies as the "user draft" slot — improve and consolidate
    existing_replies_text = "\n\n".join(
        f"Issue {t.get('id','?')} existing reply:\n{t.get('reply','(none)')}"
        for t in targets if t.get("reply")
    )
    user_draft_text = (
        f"PREVIOUSLY DRAFTED REPLIES — improve and consolidate into one:\n\n"
        f"{existing_replies_text}\n\n"
        "Write ONE coherent consolidated reply that addresses all allegations together. "
        "Do not treat them as separate numbered points — integrate into one flowing "
        "legal argument."
    ) if existing_replies_text else (
        "Write ONE coherent consolidated reply that addresses all allegations together. "
        "Do not treat them as separate numbered points — integrate into one flowing "
        "legal argument."
    )

    # Build the merged prompt using the standard _build_draft_prompt
    # We pass combined_text as the issue_text (the thing to be replied)
    from services.document.issue_replier import _build_draft_prompt
    system_prompt = _build_system_prompt(mode)
    user_message  = _build_draft_prompt(
        issue_text              = combined_text,
        top_chunks              = top_chunks,
        all_issues              = [i for i in all_issues if i.get("id") not in issue_ids],
        current_issue_id        = -1,       # sentinel — merged issue has no single ID
        prior_replied_pairs     = prior_replied,
        reference_doc_full_text = reference_doc_full_text,
        case_summary            = case_summary,
        recipient_name          = recipient_name,
        user_draft_text         = user_draft_text,
    )

    reply = await run_in_threadpool(
        _get_issue_llm().call,
        system_prompt, user_message, 8192, 0.2, "merge_replies",
    )
    reply = (reply or "").strip()

    for i in range(0, len(reply), 80):
        yield _content(reply[i : i + 80])

    # Collapse targets into one merged issue entry, re-number
    merged_issue = {
        "id":            min(t.get("id", 0) for t in targets),
        "issue_id":      str(uuid.uuid4()),
        "issue_text":    f"[Merged: Issues {ids_str}]\n\n" + combined_text,
        "source_doc_id": targets[0].get("source_doc_id", "merged"),
        "source_doc":    targets[0].get("source_doc", "merged"),
        "status":        "replied",
        "reply":         reply,
        "stale":         False,
    }
    merged_ids = {t.get("id") for t in targets}
    active_case["issues"] = [i for i in all_issues if i.get("id") not in merged_ids]
    active_case["issues"].append(merged_issue)
    for idx, iss in enumerate(active_case["issues"], 1):
        iss["id"] = idx

    full_response = _build_summary_and_issues_header(active_case) + merge_hdr + reply
    push_qa_pair(snapshot, question, reply[:800])
    asst = await add_message(session_id, "assistant", full_response, user_id)
    yield _retrieval_event(
        session_id, getattr(asst, "id", None),
        document_analysis=snapshot_for_display(active_case),
    )


async def _handle_query_fallback(
    question: str,
    session_id: str,
    user_id: int,
    history: list,
    background_tasks: BackgroundTasks,
    db: AsyncSession,
    active_case: Optional[dict] = None,
    snapshot: Optional[dict] = None,
) -> AsyncGenerator[str, None]:
    """
    Fallback to the retrieval pipeline for Q&A queries.
    If an active case exists, the header is emitted first.

    Output order:
        [Header — only if active_case exists]
        [Pipeline answer]
    """
    from retrieval.models import SessionMessage
    from services.document.issue_replier import _get_pipeline

    pipeline = _get_pipeline()
    if pipeline is None:
        msg = "Pipeline not ready. Please try again."
        if active_case:
            async for chunk in _stream_header(active_case):
                yield chunk
        yield _content(msg)
        await add_message(session_id, "assistant", msg, user_id)
        return

    # Emit header when there is an active case
    if active_case:
        async for chunk in _stream_header(active_case):
            yield chunk

    # Build pipeline history
    pipeline_history = []
    _pq = None
    for msg in (history or []):
        role    = msg.get("role")    if isinstance(msg, dict) else getattr(msg, "role", "")
        content = msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", "")
        if role == "user":
            _pq = content
        elif role == "assistant" and _pq:
            pipeline_history.append(
                SessionMessage(user_query=_pq, llm_response=content)
            )
            _pq = None

    # Augment question with active case context if available
    augmented = question
    if active_case and active_case.get("summary"):
        doc_ctx   = active_case["summary"][:2000]
        augmented = f"[DOCUMENT CONTEXT]\n{doc_ctx}\n\n[USER QUESTION]\n{question}"

    answer_parts = []
    try:
        staged = await run_in_threadpool(
            pipeline.query_stages_1_to_5, augmented, pipeline_history[-3:]
        )
        for chunk in pipeline.query_stage_6_stream(*staged):
            if chunk.startswith("\n\n__META__"):
                try:
                    meta = json.loads(chunk[len("\n\n__META__"):])
                except Exception:
                    meta = {}
                full_answer = "".join(answer_parts)
                if snapshot:
                    push_qa_pair(snapshot, question, full_answer[:1200])
                await add_message(session_id, "assistant", full_answer, user_id)
                if _should_update_profile(question):
                    background_tasks.add_task(auto_update_profile, user_id, question, full_answer)
                yield _emit({
                    "type":       "retrieval",
                    "sources":    meta.get("retrieved_documents", []),
                    "session_id": session_id,
                    "document_analysis": (
                        snapshot_for_display(active_case) if active_case else None
                    ),
                })
            else:
                answer_parts.append(chunk)
                yield _content(chunk)
    except Exception as e:
        logger.error(f"Query fallback error: {e}", exc_info=True)
        msg = "An error occurred. Please try again."
        yield _content(msg)
        await add_message(session_id, "assistant", msg, user_id)


async def _handle_correct_classification(
    active_case: dict,
    question: str,
    session_id: str,
    user_id: int,
    snapshot: dict,
) -> AsyncGenerator[str, None]:
    docs    = active_case.get("docs", [])
    q_lower = question.lower()

    changed_doc = None
    for doc in docs:
        fn      = doc.get("filename", "").lower()
        fn_base = fn.rsplit(".", 1)[0]
        if fn in q_lower or fn_base in q_lower:
            changed_doc = doc
            break

    # Emit header first
    async for chunk in _stream_header(active_case):
        yield chunk

    if not changed_doc:
        msg = (
            "I couldn't identify which document to reclassify. "
            "Please mention the filename, e.g. 'notice.pdf is reference'."
        )
        yield _content(msg)
        await add_message(session_id, "assistant",
                          _build_summary_and_issues_header(active_case) + msg, user_id)
        return

    new_role = None
    if any(w in q_lower for w in ["primary", "my notice", "main notice", "needs reply"]):
        new_role = "primary"
    elif any(w in q_lower for w in ["my draft", "my reply", "draft reply", "i prepared"]):
        new_role = "user_draft_reply"
    elif any(w in q_lower for w in ["previous reply", "old reply", "already replied", "submitted"]):
        new_role = "previous_reply"
    elif any(w in q_lower for w in ["reference", "judgment", "circular", "for context"]):
        new_role = "reference"
    elif any(w in q_lower for w in ["informational", "for understanding", "gst return", "itr"]):
        new_role = "informational"

    if new_role is None:
        msg = (
            f"What should `{changed_doc['filename']}` be classified as?\n\n"
            "Options: **primary notice** | **reference** | "
            "**my draft reply** | **previous reply** | **informational**"
        )
        yield _content(msg)
        await add_message(session_id, "assistant",
                          _build_summary_and_issues_header(active_case) + msg, user_id)
        return

    reruns = apply_doc_correction(
        active_case, changed_doc["filename"], "role", new_role, snapshot
    )["reruns"]

    if "step_6_issue_extraction" in reruns:
        full_txt = await get_text_by_filename(
            session_id, active_case["case_id"], changed_doc["filename"]
        )
        if full_txt:
            yield _content(f"\nExtracting issues from `{changed_doc['filename']}`...\n")
            new_issues = await run_in_threadpool(extract_issues, full_txt, [])
            if new_issues:
                src_id = changed_doc["doc_id"]
                src_fn = changed_doc["filename"]
                active_case["issues"] = merge_issues(
                    active_case.get("issues", []), new_issues, src_id, src_fn
                )

    if "remove_issues_from_doc" in reruns:
        fn = changed_doc["filename"]
        active_case["issues"] = [
            i for i in active_case.get("issues", []) if i.get("source_doc") != fn
        ]
        for idx, iss in enumerate(active_case["issues"], 1):
            iss["id"] = idx

    active_case["summary"] = build_case_summary(active_case)
    bump_version(snapshot)

    pending = get_pending_issues(active_case)
    note    = _build_assumptions_note(active_case)
    msg = (
        f"Updated. `{changed_doc['filename']}` is now treated as **{new_role}**."
        + (f"\n\n{len(pending)} issue(s) pending reply." if pending else "")
        + note
    )
    for i in range(0, len(msg), 300):
        yield _content(msg[i : i + 300])
    asst = await add_message(
        session_id, "assistant",
        _build_summary_and_issues_header(active_case) + msg, user_id,
    )
    yield _retrieval_event(
        session_id, getattr(asst, "id", None),
        document_analysis=snapshot_for_display(active_case),
    )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENDPOINT
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/stream")
async def document_stream(
    background_tasks: BackgroundTasks,
    question:    str            = Form(default=""),
    session_id:  Optional[str]  = Form(default=None),
    files:       List[UploadFile] = File(default=[]),
    user         = Depends(auth_guard),
    db: AsyncSession = Depends(get_db),
):
    db_user    = await _get_db_user(user.get("sub"), db)
    user_id    = db_user.id
    session_id = session_id or str(uuid.uuid4())
    has_files  = bool(files and any(f.filename for f in files))

    allowed, error_msg = await check_credits(
        user_id, session_id, has_files, db,
        chat_mode="draft" if has_files else "simple",
    )
    if not allowed:
        raise HTTPException(status_code=402, detail=error_msg)

    # Validate and save uploaded files to temp
    temp_file_paths = []
    if has_files:
        valid_files = [f for f in files if f.filename]
        if len(valid_files) > 10:
            raise HTTPException(status_code=400, detail="Maximum 10 files per upload.")
        for f in valid_files:
            ext = os.path.splitext(f.filename)[1].lower()
            if ext not in SUPPORTED:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Unsupported format: {f.filename}. "
                        f"Supported: {', '.join(sorted(SUPPORTED))}"
                    ),
                )
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                shutil.copyfileobj(f.file, tmp)
                temp_file_paths.append((tmp.name, ext, f.filename))

    async def stream_generator() -> AsyncGenerator[str, None]:
        snapshot_ref = [None]

        try:
            history    = await get_session_history(session_id)
            is_new     = len(history) == 0
            snapshot   = await get_doc_context(session_id) or create_empty_context()
            snapshot_ref[0] = snapshot

            # Save user message
            user_msg = question
            if temp_file_paths:
                fnames    = [tp[2] for tp in temp_file_paths]
                user_msg += f"\n\n[Documents: {', '.join(fnames)}]"
            await add_message(
                session_id, "user", user_msg, user_id,
                chat_mode="draft" if has_files else "simple",
            )
            await track_usage(user_id, session_id, db, force_deduct=is_new)

            active_case = get_active_case(snapshot)

            # ── Handle pending confirmation response ──────────────────────────
            if snapshot.get("_pending_confirmations") and not has_files:
                pending = snapshot["_pending_confirmations"]
                q_lower = question.lower()
                opt1 = any(w in q_lower for w in ["option 1", "reference", "same case", "current case", "add"])
                opt2 = any(w in q_lower for w in ["option 2", "new case", "separate", "different", "other"])
                yesw = any(w in q_lower for w in ["yes", "correct", "right", "ok", "confirm"])

                resolved_pendings = []
                for pend in pending:
                    meta   = pend["proposed_metadata"]
                    oroute = pend.get("original_route", "")
                    if opt1 and "different_case" in oroute:
                        meta["role"] = "reference"
                        resolved_pendings.append({**pend, "route": "add_to_case_reference", "analysis": meta})
                    elif opt2 and "different_case" in oroute:
                        resolved_pendings.append({**pend, "route": "new_case_primary", "analysis": meta})
                    elif yesw:
                        resolved_pendings.append({
                            **pend,
                            "route":    pend.get("proposed_route", "add_to_case_primary"),
                            "analysis": meta,
                        })

                if resolved_pendings:
                    snapshot["_pending_confirmations"] = []
                    fake_docs = []
                    for r in resolved_pendings:
                        txt = await get_primary_texts(
                            session_id, (active_case or {}).get("case_id", 0)
                        )
                        fake_docs.append({"filename": r["filename"], "full_text": txt, "page_count": 0})
                    await _apply_routing(resolved_pendings, fake_docs, snapshot, session_id)
                    await _extract_all_issues(resolved_pendings, snapshot, session_id)
                    active_case = get_active_case(snapshot)
                    if active_case:
                        async for chunk in _handle_show_summary(active_case, session_id, user_id):
                            yield chunk
                        return
                else:
                    msg = pending[0].get("confirmation_message", "Please clarify your choice.")
                    yield _content(msg)
                    asst = await add_message(session_id, "assistant", msg, user_id)
                    yield _retrieval_event(session_id, getattr(asst, "id", None))
                    return

            # ── Step 0: Query rewrite ─────────────────────────────────────────
            resolved_question = question
            if question.strip() and history and not has_files:
                resolved_question = await run_in_threadpool(
                    rewrite_query_if_needed, question, history, snapshot
                )

            # ── No docs, no active case → pure chatbot ────────────────────────
            if not has_files and not active_case:
                if question.strip():
                    async for chunk in _handle_query_fallback(
                        question, session_id, user_id, history,
                        background_tasks, db, None, snapshot,
                    ):
                        yield chunk
                return

            # ── STEP 1: Extract pages ─────────────────────────────────────────
            extracted_docs = []
            if has_files:
                extracted_docs, errors = await _extract_all_documents(
                    temp_file_paths, resolved_question
                )
                for err in errors:
                    yield _content(f"⚠️ {err}\n\n")

                if not extracted_docs:
                    msg = "No documents could be processed. Please check the files and try again."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)
                    return

                if resolved_question.strip():
                    ac = get_active_case(snapshot)
                    if ac:
                        append_user_context(ac, resolved_question, "session")

            # ── STEP 2: Analysis ──────────────────────────────────────────────
            doc_analyses: List[dict] = []
            entities_cache: Dict    = {}
            intent_result: dict     = {}

            if has_files:
                doc_analyses, entities_cache = await _run_step2(
                    extracted_docs, resolved_question, snapshot
                )
                # Cross-doc temporal role adjustment
                doc_analysis_pairs = list(zip(extracted_docs, doc_analyses))
                adjust_temporal_roles(doc_analysis_pairs, get_active_case(snapshot))

                # Multi-part notice merge (same reference_number)
                merged_pairs   = merge_multipart_docs(doc_analysis_pairs)
                extracted_docs = [d for d, _ in merged_pairs]
                doc_analyses   = [a for _, a in merged_pairs]

                primary_a = next(
                    (a for a in doc_analyses if a.get("role") == "primary"),
                    doc_analyses[0] if doc_analyses else {},
                )
                intent_result = {
                    "intent":        primary_a.get("intent", "summarize"),
                    "mode":          primary_a.get("mode"),
                    "issue_numbers": primary_a.get("issue_numbers", []),
                }
            else:
                intent_result = await run_in_threadpool(
                    classify_intent_no_docs, resolved_question, snapshot
                )

            intent = intent_result.get("intent", "summarize")
            mode   = intent_result.get("mode")

            # issue_numbers from doc analysis are document paragraph numbers, NOT
            # our sequential issue IDs — only use for text-only (Type 3) requests.
            if has_files:
                issue_numbers = []
            else:
                issue_numbers = [
                    int(x) for x in (intent_result.get("issue_numbers") or [])
                ]

            # ── STEP 3: Routing ───────────────────────────────────────────────
            routing_plan: List[dict] = []
            if has_files:
                for doc, analysis in zip(extracted_docs, doc_analyses):
                    route = determine_route(
                        analysis, get_active_case(snapshot), doc.get("upload_hints")
                    )
                    nc = False
                    cm = None
                    if route == "different_case_confirm":
                        nc = True
                        p_existing = (get_active_case(snapshot) or {}).get("parties", {})
                        p_new      = analysis.get("parties") or {}
                        cm = (
                            f"The document appears to involve different parties "
                            f"({p_new.get('sender','?')} → {p_new.get('recipient','?')}) "
                            f"from your current case "
                            f"({p_existing.get('authority','?')} → {p_existing.get('taxpayer_name','?')}).\n\n"
                            f"**Option 1** — Add as reference material for current case\n\n"
                            f"**Option 2** — Start a new case for this document"
                        )
                        route = "pending"
                    routing_plan.append({
                        "filename":             doc["filename"],
                        "analysis":             analysis,
                        "route":                route,
                        "needs_confirmation":   nc,
                        "confirmation_message": cm,
                        "proposed_route": (
                            "add_to_case_primary"
                            if analysis.get("role") == "primary"
                            else "add_to_case_reference"
                        ),
                        "proposed_metadata": analysis,
                    })

            # ── STEP 4: Confirmation-needed docs ──────────────────────────────
            need_conf = [r for r in routing_plan if r.get("needs_confirmation")]
            confirmed = [r for r in routing_plan if not r.get("needs_confirmation")]

            if need_conf:
                if confirmed:
                    snapshot.setdefault("legal_entities_cache", {}).update(entities_cache)
                    await _apply_routing(confirmed, extracted_docs, snapshot, session_id)
                    await _extract_all_issues(confirmed, snapshot, session_id)

                snapshot["_pending_confirmations"] = [
                    {
                        "filename":             r["filename"],
                        "proposed_metadata":    r["analysis"],
                        "original_route":       "different_case_confirm",
                        "proposed_route":       r["proposed_route"],
                        "confirmation_message": r["confirmation_message"],
                    }
                    for r in need_conf
                ]
                msg = "\n\n".join(
                    r["confirmation_message"]
                    for r in need_conf if r["confirmation_message"]
                )
                yield _content(msg)
                asst = await add_message(session_id, "assistant", msg, user_id)
                yield _retrieval_event(session_id, getattr(asst, "id", None))
                return

            # ── STEP 5: Apply routing + cache entities ────────────────────────
            if confirmed:
                snapshot.setdefault("legal_entities_cache", {}).update(entities_cache)
                await _apply_routing(confirmed, extracted_docs, snapshot, session_id)

            active_case = get_active_case(snapshot)

            # ── STEP 6: Issue extraction ──────────────────────────────────────
            if has_files and confirmed:
                await _extract_all_issues(confirmed, snapshot, session_id)
                active_case = get_active_case(snapshot)

            # ── STEP 7+8: Intent routing ──────────────────────────────────────

            # Mode from upload hints
            if not mode and has_files:
                all_hints = [h for doc in extracted_docs for h in doc.get("upload_hints", [])]
                if "msg:defensive" in all_hints:
                    mode = MODE_DEFENSIVE
                elif "msg:in_favour" in all_hints:
                    mode = MODE_IN_FAVOUR

            # When files are uploaded and no mode is set yet:
            # Always route to summarize first so the user sees the issues
            # and is asked for the mode. Exception: if mode IS already set,
            # proceed directly to drafting.
            if has_files and intent in ("draft_direct", "draft_all") and not mode:
                intent = "summarize"

            # State: awaiting issue confirmation
            if (
                active_case
                and active_case.get("state") == "awaiting_issue_confirmation"
                and not has_files
            ):
                q_lower = resolved_question.lower()
                if any(w in q_lower for w in ["go ahead", "proceed", "yes", "ok", "start", "draft", "continue"]):
                    pending_ids     = active_case.get("_pending_draft_ids") or []
                    issues_to_draft = (
                        [i for i in active_case["issues"] if i.get("id") in pending_ids]
                        if pending_ids else get_draftable_issues(active_case)
                    )
                    if issues_to_draft:
                        async for chunk in _handle_draft_issues(
                            active_case, issues_to_draft, session_id, user_id,
                            resolved_question, background_tasks, db, snapshot,
                            skip_confirmation=True,
                        ):
                            yield chunk
                    else:
                        yield _content("No pending issues to draft.")
                    return
                else:
                    intent = "update_issues"

            logger.info(
                f"Intent={intent} mode={mode} issues={issue_numbers} "
                f"session={session_id[:8]}"
            )

            # ── correct_classification ────────────────────────────────────────
            if intent == "correct_classification" or (
                not has_files and active_case
                and any(w in resolved_question.lower() for w in [
                    "is primary", "is reference", "is my draft", "is my reply",
                    "is previous reply", "should be primary", "should be reference",
                    "classify", "reclassify", "wrong classification",
                ])
            ):
                if active_case:
                    async for chunk in _handle_correct_classification(
                        active_case, resolved_question, session_id, user_id, snapshot
                    ):
                        yield chunk
                else:
                    yield _content("No active case to reclassify documents for.")
                return

            # ── summarize ─────────────────────────────────────────────────────
            if intent == "summarize":
                if not active_case:
                    yield _content(
                        "No document uploaded yet. "
                        "Please upload a document to get started."
                    )
                else:
                    async for chunk in _handle_show_summary(active_case, session_id, user_id):
                        yield chunk

            # ── draft_direct / confirm_mode / draft_all ───────────────────────
            elif intent in ("draft_direct", "confirm_mode", "draft_all"):
                if not active_case:
                    yield _content("No active case. Please upload a document first.")
                else:
                    if mode:
                        active_case["mode"] = mode
                    if not active_case.get("mode"):
                        # Emit header first, then ask for mode
                        async for chunk in _stream_header(active_case):
                            yield chunk
                        msg = (
                            "Should I prepare the draft reply in "
                            "**Defensive** (protecting the recipient) "
                            "or **In Favour** of the notice?"
                        )
                        yield _content(msg)
                        active_case["state"] = "awaiting_mode"
                        full_text = _build_summary_and_issues_header(active_case) + msg
                        await add_message(session_id, "assistant", full_text, user_id)
                    else:
                        issues_to_draft = get_draftable_issues(active_case, issue_numbers or None)
                        if not issues_to_draft:
                            async for chunk in _stream_header(active_case):
                                yield chunk
                            yield _content(
                                "All issues already have replies. "
                                "Ask me to update any specific one."
                            )
                        else:
                            async for chunk in _handle_draft_issues(
                                active_case, issues_to_draft, session_id, user_id,
                                resolved_question, background_tasks, db, snapshot,
                            ):
                                yield chunk

            # ── draft_specific ────────────────────────────────────────────────
            elif intent == "draft_specific":
                if not active_case:
                    yield _content("No active case. Please upload a document first.")
                else:
                    if mode:
                        active_case["mode"] = mode
                    if not active_case.get("mode"):
                        async for chunk in _stream_header(active_case):
                            yield chunk
                        msg = "Should I prepare the reply in **Defensive** or **In Favour**?"
                        yield _content(msg)
                        active_case["state"]             = "awaiting_mode"
                        active_case["_pending_issue_nums"] = issue_numbers
                        full_text = _build_summary_and_issues_header(active_case) + msg
                        await add_message(session_id, "assistant", full_text, user_id)
                    else:
                        issues_to_draft = get_draftable_issues(active_case, issue_numbers or None)
                        if not issues_to_draft:
                            async for chunk in _stream_header(active_case):
                                yield chunk
                            yield _content("No matching issues. Check issue numbers.")
                        else:
                            async for chunk in _handle_draft_issues(
                                active_case, issues_to_draft, session_id, user_id,
                                resolved_question, background_tasks, db, snapshot,
                            ):
                                yield chunk

            # ── update_issues ─────────────────────────────────────────────────
            elif intent == "update_issues":
                if not active_case:
                    yield _content("No active case.")
                else:
                    q_lower = resolved_question.lower()
                    if any(w in q_lower for w in [
                        "explain", "what does", "what is issue",
                        "meaning of issue", "clarify issue",
                    ]):
                        nums = [
                            int(x) for x in re.findall(r"\d+", resolved_question)
                            if int(x) <= len(active_case.get("issues", []))
                        ]
                        async for chunk in _handle_explain_issues(
                            active_case, nums, resolved_question,
                            session_id, user_id, background_tasks, db, snapshot,
                        ):
                            yield chunk
                    else:
                        async for chunk in _handle_update_issues(
                            active_case, resolved_question, session_id, user_id
                        ):
                            yield chunk

            # ── explain_issues ────────────────────────────────────────────────
            elif intent == "explain_issues":
                if not active_case:
                    yield _content("No active case.")
                else:
                    nums = [
                        int(x) for x in re.findall(r"\d+", resolved_question)
                        if int(x) <= len(active_case.get("issues", []))
                    ]
                    async for chunk in _handle_explain_issues(
                        active_case, nums, resolved_question,
                        session_id, user_id, background_tasks, db, snapshot,
                    ):
                        yield chunk

            # ── update_reply ──────────────────────────────────────────────────
            elif intent == "update_reply":
                if not active_case:
                    yield _content("No active case.")
                elif not issue_numbers:
                    found_nums = [
                        int(x) for x in re.findall(r"\d+", resolved_question)
                        if int(x) <= len(active_case.get("issues", []))
                    ]
                    if not found_nums:
                        async for chunk in _stream_header(active_case):
                            yield chunk
                        msg = (
                            "Please specify which issue number to update "
                            "(e.g. 'redo issue 2' or 'update issue 3 with my scenario')."
                        )
                        yield _content(msg)
                        await add_message(
                            session_id, "assistant",
                            _build_summary_and_issues_header(active_case) + msg, user_id,
                        )
                    else:
                        issue_numbers = found_nums
                        target_id = issue_numbers[0]
                        target    = next(
                            (i for i in active_case.get("issues", []) if i.get("id") == target_id),
                            None,
                        )
                        if not target:
                            async for chunk in _stream_header(active_case):
                                yield chunk
                            yield _content(f"Issue {target_id} not found.")
                        else:
                            if mode:
                                active_case["mode"] = mode
                            scenario_match = re.split(
                                rf"(?:issue|Issue)\s+{target_id}[\s,\-:]+",
                                resolved_question, maxsplit=1,
                            )
                            user_scenario = scenario_match[1].strip() if len(scenario_match) > 1 else ""
                            async for chunk in _handle_update_reply_with_scenario(
                                active_case, target_id, user_scenario, resolved_question,
                                session_id, user_id, background_tasks, db, snapshot,
                            ):
                                yield chunk
                else:
                    target_id = issue_numbers[0]
                    target    = next(
                        (i for i in active_case.get("issues", []) if i.get("id") == target_id),
                        None,
                    )
                    if not target:
                        async for chunk in _stream_header(active_case):
                            yield chunk
                        yield _content(f"Issue {target_id} not found.")
                    else:
                        if mode:
                            active_case["mode"] = mode
                        scenario_match = re.split(
                            rf"(?:issue|Issue)\s+{target_id}[\s,\-:]+",
                            resolved_question, maxsplit=1,
                        )
                        user_scenario = scenario_match[1].strip() if len(scenario_match) > 1 else ""
                        async for chunk in _handle_update_reply_with_scenario(
                            active_case, target_id, user_scenario, resolved_question,
                            session_id, user_id, background_tasks, db, snapshot,
                        ):
                            yield chunk

            # ── merge_replies ─────────────────────────────────────────────────
            elif intent == "merge_replies":
                if not active_case:
                    yield _content("No active case.")
                else:
                    nums = [
                        int(x) for x in re.findall(r"\d+", resolved_question)
                        if int(x) <= len(active_case.get("issues", []))
                    ]
                    if len(nums) < 2:
                        async for chunk in _stream_header(active_case):
                            yield chunk
                        yield _content(
                            "Please specify at least 2 issue numbers to merge "
                            "(e.g. 'merge replies for issues 3 and 5')."
                        )
                    else:
                        async for chunk in _handle_merge_replies(
                            active_case, nums, resolved_question,
                            session_id, user_id, background_tasks, db, snapshot,
                        ):
                            yield chunk

            # ── query_document / query_general ────────────────────────────────
            elif intent in ("query_document", "query_general", "query_mixed"):
                async for chunk in _handle_query_fallback(
                    resolved_question, session_id, user_id,
                    history, background_tasks, db, active_case, snapshot,
                ):
                    yield chunk

            # ── switch_case ───────────────────────────────────────────────────
            elif intent == "switch_case":
                target_cid = intent_result.get("case_id")
                if target_cid and target_cid in snapshot.get("cases", {}):
                    switch_active_case(snapshot, target_cid)
                    switched = get_active_case(snapshot)
                    p        = (switched or {}).get("parties", {})
                    msg      = (
                        f"Switched to Case {target_cid} — "
                        f"{p.get('authority','?')} / {p.get('taxpayer_name','?')}.\n\n"
                        f"{(switched or {}).get('summary','')[:300]}"
                    )
                else:
                    cases = snapshot.get("cases", {})
                    if len(cases) > 1:
                        lines = [
                            f"- Case {cid} ({c.get('session_status','?')}): "
                            f"{(c.get('parties') or {}).get('authority','?')} / "
                            f"{(c.get('parties') or {}).get('taxpayer_name','?')}"
                            for cid, c in cases.items()
                        ]
                        msg = (
                            "Available cases:\n" + "\n".join(lines)
                            + "\n\nWhich case to switch to?"
                        )
                    else:
                        msg = "Only one case exists in this session."
                yield _content(msg)
                await add_message(session_id, "assistant", msg, user_id)

            # ── new_case ──────────────────────────────────────────────────────
            elif intent == "new_case":
                archive_active_case(snapshot)
                snapshot["active_case_id"] = None
                msg = "Starting fresh. Please upload the documents for the new case."
                yield _content(msg)
                await add_message(session_id, "assistant", msg, user_id)

            # ── mark_replied ──────────────────────────────────────────────────
            elif intent == "mark_replied":
                if not active_case:
                    yield _content("No active case.")
                else:
                    marked = 0
                    for iss in active_case.get("issues", []):
                        if not iss.get("reply") and iss.get("status") not in ("replied", "has_reply_doc"):
                            iss["status"] = "has_reply_doc"
                            marked += 1
                    async for chunk in _stream_header(active_case):
                        yield chunk
                    msg = f"Marked {marked} issue(s) as externally replied."
                    yield _content(msg)
                    await add_message(
                        session_id, "assistant",
                        _build_summary_and_issues_header(active_case) + msg, user_id,
                    )

            # ── fallback ──────────────────────────────────────────────────────
            else:
                async for chunk in _handle_query_fallback(
                    resolved_question, session_id, user_id,
                    history, background_tasks, db, active_case, snapshot,
                ):
                    yield chunk

        except Exception as e:
            logger.error(f"Document stream error: {e}", exc_info=True)
            yield _emit({"type": "error", "message": "An error occurred. Please try again."})

        finally:
            # Step 9: Always persist snapshot
            if snapshot_ref[0] is not None:
                try:
                    await set_doc_context(session_id, snapshot_ref[0])
                except Exception as ctx_err:
                    logger.warning(f"Failed to save snapshot: {ctx_err}")

            # Cleanup temp files
            for tmp_path, *_ in temp_file_paths:
                if os.path.exists(tmp_path):
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass

    return StreamingResponse(stream_generator(), media_type="application/x-ndjson")