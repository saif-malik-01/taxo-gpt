"""
orchestrator.py — The core decision-making and orchestration logic for the 
advanced Legal Draft Architecture (Feature 2).

This file contains the business logic formerly in api/v1/document.py.
It is integrated into the main chat router to provide a unified drafting experience.
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
from typing import AsyncGenerator, Dict, List, Optional, Tuple

from fastapi import BackgroundTasks, UploadFile, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from apps.api.src.db.session import get_db
from apps.api.src.services.auth.deps import auth_guard
from apps.api.src.services.chat.memory_updater import auto_update_profile
from apps.api.src.services.document.doc_classifier import (
    adjust_temporal_roles,
    analyze_document,
    determine_route,
    extract_issues,
    extract_legal_entities,
    extract_replied_issues,
    merge_multipart_docs,
    reextract_missed_issues,
)
from apps.api.src.services.document.doc_context import (
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
from apps.api.src.services.document.global_semaphore import get_page_semaphore
from apps.api.src.services.document.intent_classifier import (
    classify_intent_no_docs,
    parse_issue_update,
    rewrite_query_if_needed,
)
from apps.api.src.services.document.issue_replier import (
    MODE_DEFENSIVE,
    MODE_IN_FAVOUR,
    build_prior_replied_pairs,
    build_reference_doc_summaries,
    process_issues_streaming,
    set_pipeline,
)
from apps.api.src.services.document.processor import extract_document_pages
from apps.api.src.services.document.session_doc_store import (
    delete_session_documents,
    get_primary_texts,
    get_reference_texts,
    get_text_by_filename,
    save_document_text,
)
from apps.api.src.services.memory import add_message, check_credits, get_session_history, track_usage
from apps.api.src.db.models.base import User

logger = logging.getLogger(__name__)

SUPPORTED = {
    ".pdf", ".docx", ".pptx", ".xlsx", ".html",
    ".png", ".jpg", ".jpeg", ".tiff", ".bmp",
}
_MIN_WORDS_FOR_PROFILE = 8


# ─────────────────────────────────────────────────────────────────────────────
# NDJSON helpers
# ─────────────────────────────────────────────────────────────────────────────

def _emit(data: dict) -> str:
    return json.dumps(data, ensure_ascii=False) + "\n"

def _content(text: str) -> str:
    return _emit({"type": "content", "delta": text})

def _event_msg(message: str) -> str:
    return _emit({"type": "event", "message": message})

def _retrieval_event(document_analysis=None, sources=None) -> str:
    data = {"type": "retrieval"}
    if document_analysis is not None:
        data["document_analysis"] = document_analysis
    if sources is not None:
        data["sources"] = sources
    return _emit(data)

def _should_update_profile(q: str) -> bool:
    return len(q.strip().split()) >= _MIN_WORDS_FOR_PROFILE


# ─────────────────────────────────────────────────────────────────────────────
# STEP 0 — Pre-classification signal capture
# ─────────────────────────────────────────────────────────────────────────────

def _extract_upload_hints(filename: str, user_message: str) -> List[str]:
    hints = []
    fname_lower = filename.lower()
    msg_lower   = (user_message or "").lower()

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

async def _extract_all_documents(temp_file_paths: List[tuple], user_message: str) -> Tuple[List[dict], List[str]]:
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
    logger.info(f"Step 1 complete: {len(extracted)}/{len(temp_file_paths)} docs in {time.monotonic()-t0:.1f}s")
    return extracted, errors


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Parallel tracks 2A+2C and 2B
# ─────────────────────────────────────────────────────────────────────────────

async def _run_step2(extracted_docs: List[dict], resolved_question: str, snapshot: dict) -> Tuple[List[dict], Dict[str, dict]]:
    active_case = get_active_case(snapshot)
    user_ctx_text = get_user_context_text(active_case, limit=3) if active_case else ""
    active_info = None
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
        logger.info(f"2A+2C '{doc['filename']}': role={r.get('role')} temporal={r.get('temporal_role')} ({time.monotonic()-t0:.1f}s)")
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

    return [a for a in analyses_list if a is not None], entities_cache


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Apply routing + DB save
# ─────────────────────────────────────────────────────────────────────────────

async def _apply_routing(routing_plan: List[dict], extracted_docs: List[dict], snapshot: dict, session_id: str) -> None:
    text_map = {d["filename"]: d["full_text"] for d in extracted_docs}
    confirmed = [r for r in routing_plan if not r.get("needs_confirmation")]
    new_routes = [r for r in confirmed if "new_case" in r["route"]]
    add_routes = [r for r in confirmed if "new_case" not in r["route"]]

    for entry in new_routes + add_routes:
        route = entry["route"]
        analysis = entry["analysis"]
        filename = entry["filename"]
        role = analysis.get("role", "reference")

        if "new_case" in route:
            if get_active_case(snapshot):
                archive_active_case(snapshot)
            cid = get_next_case_id(snapshot)
            p = analysis.get("parties") or {}
            case = create_new_case(cid, p)
            add_case_to_context(snapshot, case)

        active_case = get_active_case(snapshot)
        if not active_case:
            cid = get_next_case_id(snapshot)
            p = analysis.get("parties") or {}
            case = create_new_case(cid, p)
            add_case_to_context(snapshot, case)
            active_case = case

        doc_entry = create_doc_entry(
            filename=filename,
            role=role,
            role_locked=analysis.get("role_locked", False),
            display_type=analysis.get("display_type", "notice"),
            temporal_role=analysis.get("temporal_role", "unknown"),
            temporal_locked=analysis.get("temporal_locked", False),
            is_latest=False,
            date=analysis.get("date"),
            reference_number=analysis.get("reference_number"),
            parties=analysis.get("parties") or {},
            brief_summary=analysis.get("brief_summary", ""),
            confidence=analysis.get("confidence", 0),
            has_issues=analysis.get("has_issues", False),
            has_replied_issues=analysis.get("has_replied_issues", False),
            part_doc_ids=analysis.get("part_filenames", []),
            upload_hints=analysis.get("upload_hints", []),
        )
        doc_entry["page_count"] = analysis.get("_page_count", 0)
        doc_entry["pipeline_status"] = "committed"
        add_document_to_case(active_case, doc_entry)

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
                        p.get("sender") if field == "authority"
                        else p.get("recipient") if field == "taxpayer_name"
                        else p.get(field)
                    )

    for case in snapshot.get("cases", {}).values():
        recalculate_is_latest(case)
        update_case_level_from_latest(case)
        case["summary"] = build_case_summary(case)
    bump_version(snapshot)

    save_tasks = []
    for entry in confirmed:
        filename = entry["filename"]
        analysis = entry["analysis"]
        full_text = text_map.get(filename, "")
        if not full_text:
            continue
        role = analysis.get("role", "reference")
        ac = get_active_case(snapshot)
        if ac:
            doc_type = (
                "reply_reference" if role in ("previous_reply", "user_draft_reply")
                else "primary" if role == "primary"
                else "reference"
            )
            save_tasks.append(save_document_text(session_id, ac["case_id"], filename, doc_type, full_text))
    if save_tasks:
        await asyncio.gather(*save_tasks)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Issue + replied-issue extraction
# ─────────────────────────────────────────────────────────────────────────────

async def _extract_all_issues(routing_plan: List[dict], snapshot: dict, session_id: str) -> None:
    active_case = get_active_case(snapshot)
    if not active_case:
        return
    primary_entries = [r for r in routing_plan if not r.get("needs_confirmation") and r["analysis"].get("role") == "primary" and r["analysis"].get("has_issues") is not False]
    reply_entries = [r for r in routing_plan if not r.get("needs_confirmation") and r["analysis"].get("role") in ("previous_reply", "user_draft_reply") and r["analysis"].get("has_replied_issues")]

    async def _issues_one(entry: dict):
        fn = entry["filename"]
        full_txt = await get_text_by_filename(session_id, active_case["case_id"], fn)
        if not full_txt:
            full_txt = await get_primary_texts(session_id, active_case["case_id"])
        if not full_txt:
            return fn, []
        new_issues = await run_in_threadpool(extract_issues, full_txt, active_case.get("issues", []))
        return fn, new_issues

    async def _replied_one(entry: dict):
        fn = entry["filename"]
        txt = await get_text_by_filename(session_id, active_case["case_id"], fn)
        if not txt:
            return fn, []
        pairs = await run_in_threadpool(extract_replied_issues, txt)
        return fn, pairs

    results = await asyncio.gather(*[_issues_one(r) for r in primary_entries], *[_replied_one(r) for r in reply_entries], return_exceptions=True)
    n_primary = len(primary_entries)
    for r in results[:n_primary]:
        if isinstance(r, Exception):
            continue
        fn, new_issues = r
        if new_issues:
            source_doc_id = next((d["doc_id"] for d in active_case.get("docs", []) if d["filename"] == fn), fn)
            active_case["issues"] = merge_issues(active_case.get("issues", []), new_issues, source_doc_id, fn)
    for r in results[n_primary:]:
        if isinstance(r, Exception):
            continue
        fn, replied_pairs = r
        if replied_pairs:
            for d in active_case.get("docs", []):
                if d.get("filename") == fn:
                    d["replied_issues"] = replied_pairs
                    break
            for pair in replied_pairs:
                pt = pair.get("issue_text", "")
                for iss in active_case.get("issues", []):
                    a, b = pt[:80].lower(), iss.get("issue_text", "")[:80].lower()
                    shorter = min(len(a), len(b))
                    if shorter > 0 and (sum(1 for x, y in zip(a, b) if x == y) / shorter > 0.80) and not iss.get("reply"):
                        iss["status"] = "has_reply_doc"
                        iss["replied_by_doc"] = fn
                        break
    active_case["summary"] = build_case_summary(active_case)
    bump_version(snapshot)


# ─────────────────────────────────────────────────────────────────────────────
# UX Block Builders
# ─────────────────────────────────────────────────────────────────────────────

def _build_summary_and_issues_header(active_case: dict) -> str:
    if not active_case:
        return ""
    lines = []
    p = active_case.get("parties", {}) or {}
    h_parts = []
    if p.get("authority"):
        h_parts.append(f"**From:** {p['authority']}")
    if p.get("taxpayer_name"):
        h_parts.append(f"**To:** {p['taxpayer_name']}")
    if active_case.get("reference_number"):
        h_parts.append(f"**Ref:** {active_case['reference_number']}")
    if h_parts:
        lines.append("  ".join(h_parts))
        lines.append("")
    s = (active_case.get("summary") or "").strip()
    if s:
        lines.append(s)
        lines.append("")
    issues = active_case.get("issues", [])
    if issues:
        lines.append("**Issues / Allegations:**")
        lines.append("")
        for iss in issues:
            icon = " ✅" if iss.get("reply") else (" 📄" if iss.get("status") == "has_reply_doc" else "")
            text = iss.get("issue_text", "")
            if len(text) > 2000:
                text = text[:1997] + "..."
            lines.append(f"**Issue {iss.get('id', '?')}:**\n{text}{icon}")
            lines.append("")
        pending = sum(1 for i in issues if not i.get("reply") and i.get("status") not in ("replied", "has_reply_doc"))
        replied = sum(1 for i in issues if i.get("reply"))
        if pending > 0:
            lines.append(f"_{pending} issue(s) pending reply  |  {replied} replied_")
        else:
            lines.append(f"_All {replied} issue(s) replied_")
        lines.append("")
    lines.append("---")
    lines.append("")
    return "\n".join(lines)

def _build_assumptions_note(active_case: dict, mode: str = None) -> str:
    docs  = active_case.get("docs", []) if active_case else []
    parts = []
    for d in docs:
        fn, role, temporal = d.get("filename", "document"), d.get("role", "reference"), d.get("temporal_role", "")
        if role == "primary" and temporal == "current":
            parts.append(f"`{fn}` — current notice")
        elif role == "primary":
            parts.append(f"`{fn}` — historical/primary notice")
        elif role == "previous_reply":
            parts.append(f"`{fn}` — previous reply")
        elif role == "reference":
            parts.append(f"`{fn}` — reference material")
    if mode:
        parts.append(f"mode: {'Defensive' if mode == MODE_DEFENSIVE else 'In Favour'}")
    if not parts:
        return ""
    lines = ["\n---\n_Assumptions:_"] + [f"_{p}_" for p in parts] + ["_Tell me if any assumption is wrong._"]
    return "\n".join(lines)

async def _stream_header(active_case: dict) -> AsyncGenerator[str, None]:
    h = _build_summary_and_issues_header(active_case)
    if h:
        for i in range(0, len(h), 400):
            yield _content(h[i:i+400])


# ─────────────────────────────────────────────────────────────────────────────
# Action Handlers
# ─────────────────────────────────────────────────────────────────────────────

async def _handle_show_summary(active_case: dict, session_id: str, user_id: int) -> AsyncGenerator[str, None]:
    async for chunk in _stream_header(active_case):
        yield chunk
    issues = active_case.get("issues", [])
    pending = sum(1 for i in issues if not i.get("reply") and i.get("status") not in ("replied", "has_reply_doc"))
    body = f"**{pending}** issues pending." if issues else "No issues found."
    body += "\n\n" + _build_assumptions_note(active_case, active_case.get("mode"))
    for i in range(0, len(body), 400):
        yield _content(body[i:i+400])
    full = _build_summary_and_issues_header(active_case) + body
    active_case["state"] = "awaiting_decision"
    asst = await add_message(session_id, "assistant", full, user_id)
    yield _retrieval_event(document_analysis=snapshot_for_display(active_case))
    yield _emit({"type": "completion", "session_id": session_id, "message_id": asst.id})

async def _handle_draft_issues(active_case: dict, issues_to_draft: List[dict], session_id: str, user_id: int, question: str, background_tasks: BackgroundTasks, snapshot: dict, skip_confirmation: bool = False) -> AsyncGenerator[str, None]:
    mode = active_case.get("mode", MODE_DEFENSIVE)
    # Direct drafting — no confirmation gate
    active_case["state"] = "drafting"
    hdr = f"Drafting {len(issues_to_draft)} Issue(s)...\n\n"
    yield _content(hdr)
    full = hdr
    all_src = []
    async for i_num, reply, src in process_issues_streaming(issues=issues_to_draft, mode=mode, case_summary=active_case.get("summary",""), recipient_name="", prior_replied_pairs=[], reference_doc_full_text="", max_parallel=3):
        target_issue = None
        if 0 <= i_num - 1 < len(issues_to_draft):
            target_issue = issues_to_draft[i_num-1]
        
        display_num = target_issue.get("id", i_num) if target_issue else i_num
        
        header = f"\n\n---\n\n## Reply {display_num}\n\n"
        yield _content(header)
        full += header
        for i in range(0, len(reply), 100):
            yield _content(reply[i:i+100])
        full += reply
        all_src.extend(src)
        # Update the actual issue state in the context
        if target_issue:
            target_issue["reply"] = reply
            target_issue["status"] = "replied"
    full += _build_assumptions_note(active_case, mode)
    active_case["state"] = "complete"
    push_qa_pair(snapshot, question, full[:1200])
    source_ids = [s["chunk_id"] for s in all_src if "chunk_id" in s]
    asst = await add_message(session_id, "assistant", full, user_id, source_ids=source_ids)
    yield _retrieval_event(sources=all_src, document_analysis=snapshot_for_display(active_case))
    yield _emit({"type": "completion", "session_id": session_id, "message_id": asst.id})

async def _handle_update_issues(active_case: dict, question: str, session_id: str, user_id: int) -> AsyncGenerator[str, None]:
    update = await run_in_threadpool(parse_issue_update, question, active_case.get("issues", []))
    apply_issue_update(active_case, update)
    msg = "Issues updated successfully."
    yield _content(msg)
    asst = await add_message(session_id, "assistant", msg, user_id)
    yield _retrieval_event(document_analysis=snapshot_for_display(active_case))
    yield _emit({"type": "completion", "session_id": session_id, "message_id": asst.id})

async def _handle_query_fallback(question: str, session_id: str, user_id: int, history: list, background_tasks: BackgroundTasks, active_case: Optional[dict] = None, snapshot: Optional[dict] = None) -> AsyncGenerator[str, None]:
    from apps.api.src.services.rag.models import SessionMessage
    from apps.api.src.services.document.issue_replier import _get_pipeline
    pipeline = _get_pipeline()
    if not pipeline:
        yield _content("System warming up...")
        return

    aug = f"[CASE CONTEXT]\n{active_case['summary'][:1500]}\n\n[USER]\n{question}" if active_case else question
    answer_parts = []
    try:
        staged = await pipeline.query_stages_1_to_5(aug, [])
        async for chunk in pipeline.query_stage_6_stream(*staged):
            if chunk.startswith("\n\n__META__"):
                m = "".join(answer_parts)
                try:
                    meta = json.loads(chunk.replace("\n\n__META__", ""))
                    sources = meta.get("retrieved_documents", [])
                    source_ids = [s["chunk_id"] for s in sources if s.get("chunk_id")]
                except Exception:
                    sources = []
                    source_ids = []
                asst = await add_message(session_id, "assistant", m, user_id, source_ids=source_ids)
                yield _retrieval_event(sources=sources, document_analysis=snapshot_for_display(active_case) if active_case else None)
                yield _emit({"type": "completion", "session_id": session_id, "message_id": asst.id})
            else:
                answer_parts.append(chunk)
                yield _content(chunk)
    except Exception as e:
        logger.error(f"Fallback query error: {e}")
        yield _content("Query error.")
