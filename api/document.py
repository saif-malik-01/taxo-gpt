"""
api/document.py
Document feature router — v3 final architecture.

Request flow:
  0. Setup: load history + snapshot; detect request type; query rewrite if needed
  1. Page extraction (files only): global Semaphore(25), DPI=150, all pages parallel
  2. Parallel tracks:
       Track 2A+2C: combined Qwen call per doc (metadata + intent) — all docs parallel
       Track 2B:    Qwen legal entity extraction + regex per doc — all docs parallel
                    Both tracks fire simultaneously via asyncio.gather
  3. Routing: pure logic per document (no LLM)
  4. Confirmation: emit prompt for ambiguous docs, save pending in snapshot
  5. Snapshot update + DB save (parallel with Step 6)
  6. Issue extraction: dedicated Qwen calls for docs with has_issues=True (parallel)
                       Replied-issues extraction for has_replied_issues=True (parallel)
                       Both run parallel with DB save from Step 5
  7. Intent routing: dispatch to case handler using intent from Track 2A+2C
  8. Case handler: stream reply with pre-cached Stage2BResult for fast retrieval
  9. Persist snapshot (always in finally)

Key changes vs previous version:
  - Intent comes from combined 2A+2C result — no separate classify_intent_with_docs call
  - Track 2B result cached in snapshot.legal_entities_cache[filename]
  - Step 6 issue extraction is separate and parallel (not inside Step 2)
  - process_issues_streaming receives stage2b_results dict — no re-extraction per issue
  - sender removed from draft prompt, doc summary removed from retrieval query
  - Binary classification: primary | reference (granular type stored for display only)
"""

import asyncio
import json
import logging
import os
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
    analyze_document,
    determine_route,
    entities_to_stage2b_result,
    extract_issues,
    extract_legal_entities,
    extract_replied_issues,
    reextract_missed_issues,
)
from services.document.doc_context import (
    add_case_to_context, add_document_to_case, append_user_context,
    apply_issue_update, archive_active_case, build_case_summary,
    clear_doc_context, create_doc_entry, create_empty_context, create_new_case,
    get_active_case, get_doc_context, get_draftable_issues, get_next_case_id,
    get_pending_issues, get_user_context_text, mark_doc_as_replied, merge_issues,
    recalculate_is_latest, set_doc_context, switch_active_case,
    update_case_level_from_latest,
)
from services.document.global_semaphore import get_page_semaphore
from services.document.intent_classifier import (
    classify_intent_no_docs,
    parse_issue_update,
    rewrite_query_if_needed,
)
from services.document.issue_replier import (
    MODE_DEFENSIVE, MODE_IN_FAVOUR, _build_previous_replies_text,
    process_issues_streaming, set_pipeline,
)
from services.document.processor import get_document_processor
from services.document.session_doc_store import (
    delete_session_documents, get_primary_texts, get_reference_texts,
    get_text_by_filename, save_document_text,
)
from services.memory import (
    add_message, check_credits, get_session_history, track_usage,
)
from services.models import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/document", tags=["Document"])

SUPPORTED = {
    ".pdf", ".docx", ".pptx", ".xlsx", ".html",
    ".png", ".jpg", ".jpeg", ".tiff", ".bmp",
}

_MIN_WORDS_FOR_PROFILE_UPDATE = 8


# ─── Helpers ──────────────────────────────────────────────────────────────────

async def _get_db_user(email: str, db: AsyncSession):
    result  = await db.execute(
        select(User).where(func.lower(User.email) == email.lower())
    )
    db_user = result.scalars().first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user


def _emit(data: dict) -> str:
    return json.dumps(data, ensure_ascii=False) + "\n"


def _content(text: str) -> str:
    return _emit({"type": "content", "delta": text})


def _retrieval_event(
    session_id: str,
    message_id=None,
    sources=None,
    document_analysis=None,
) -> str:
    return _emit({
        "type":              "retrieval",
        "sources":           sources or [],
        "message_id":        message_id,
        "session_id":        session_id,
        "id":                message_id,
        "document_analysis": document_analysis,
    })


def _should_update_profile(question: str) -> bool:
    return len(question.strip().split()) >= _MIN_WORDS_FOR_PROFILE_UPDATE


def _snapshot_for_display(active_case: dict) -> dict:
    """Minimal snapshot dict for document_analysis field in retrieval event."""
    return {
        "summary": active_case.get("summary"),
        "issues":  active_case.get("issues"),
        "parties": active_case.get("parties"),
        "documents": [
            {
                "filename":       d["filename"],
                "legal_doc_type": d["legal_doc_type"],
                "is_primary":     d["is_primary"],
                "is_latest":      d.get("is_latest"),
                "is_replied":     d.get("is_replied"),
                "date":           d.get("date"),
                "reference_number": d.get("reference_number"),
            }
            for d in (active_case.get("documents") or [])
        ],
    }


# ─── Page extraction with global semaphore ────────────────────────────────────

async def _extract_all_documents(
    temp_file_paths: list,
    snapshot: dict,
) -> tuple:
    """
    Extract text from all uploaded files.
    All pages across all documents use the global semaphore (12 slots default).

    Returns: (extracted_docs, error_messages)
      extracted_docs: [{filename, full_text, page_count}]
      error_messages: [str]  — user-visible errors for failed files
    """
    import time
    doc_processor = get_document_processor()
    semaphore     = get_page_semaphore()
    errors        = []
    t0            = time.monotonic()

    async def extract_one_doc(tmp_path: str, ext: str, filename: str) -> Optional[dict]:
        # Convert to page images (fast, no LLM — done in threadpool)
        try:
            page_images = await run_in_threadpool(
                doc_processor.get_page_images, tmp_path
            )
        except Exception as e:
            errors.append(f"Could not open '{filename}': {e}")
            return None

        if not page_images:
            errors.append(
                f"'{filename}' appears to be empty or image-only (scanned/protected). "
                "Please upload a text-searchable PDF."
            )
            return None

        page_count = len(page_images)
        if page_count > 200:
            errors.append(
                f"'{filename}' has {page_count} pages. Maximum is 200 pages per document."
            )
            return None

        logger.info(f"Extracting '{filename}': {page_count} page(s) via Nova Lite")

        # Extract all pages using global semaphore
        async def extract_page(page_idx: int, page_image):
            t_page = time.monotonic()
            async with semaphore:
                result = await run_in_threadpool(
                    doc_processor._extract_page_text, page_idx, page_image
                )
            logger.debug(
                f"  '{filename}' page {page_idx+1}/{page_count} done "
                f"({time.monotonic()-t_page:.1f}s)"
            )
            return result

        page_tasks   = [extract_page(i, img) for i, img in enumerate(page_images)]
        page_results = await asyncio.gather(*page_tasks, return_exceptions=True)

        # Assemble in page order
        content_blocks = []
        for i, result in enumerate(page_results):
            if isinstance(result, Exception):
                logger.warning(f"Page {i+1} of '{filename}' failed: {result}")
                continue
            page_idx, page_text = result
            if page_text and page_text.strip():
                block = f"[PAGE {page_idx + 1}]\n{page_text}" if page_count > 1 else page_text
                content_blocks.append(block)

        full_text = "\n\n".join(content_blocks)

        # Detect scanned/image-only (extracted very little text for a large doc)
        if len(full_text.strip()) < 50 and page_count >= 1:
            errors.append(
                f"'{filename}' appears to be scanned or image-only. "
                "Very little text could be extracted. "
                "Please upload a text-searchable PDF."
            )
            return None

        elapsed = time.monotonic() - t0
        logger.info(
            f"Extraction complete: '{filename}' — {page_count} page(s), "
            f"{len(full_text)} chars, {elapsed:.1f}s total"
        )
        return {
            "filename":   filename,
            "full_text":  full_text,
            "page_count": page_count,
        }

    # Launch all documents simultaneously — pages share the global semaphore
    logger.info(
        f"Starting extraction: {len(temp_file_paths)} file(s), "
        f"semaphore={get_page_semaphore()._value if hasattr(get_page_semaphore(), '_value') else 'N/A'} slots"
    )
    doc_tasks    = [extract_one_doc(tp, ext, fn) for tp, ext, fn in temp_file_paths]
    doc_results  = await asyncio.gather(*doc_tasks)
    extracted    = [r for r in doc_results if r is not None]
    logger.info(
        f"All extraction done: {len(extracted)}/{len(temp_file_paths)} file(s) succeeded "
        f"in {time.monotonic()-t0:.1f}s"
    )
    return extracted, errors


# ─── Parallel document analysis ───────────────────────────────────────────────

async def _run_tracks_2ac_and_2b(
    extracted_docs: list,
    resolved_question: str,
    snapshot: dict,
) -> tuple:
    """
    Step 2: Two parallel tracks for all uploaded documents.

    Track 2A+2C (combined per doc, all docs parallel):
      Single Qwen call → document metadata + intent classification together.
      Intent fields (intent, mode, issue_numbers) come from this call.
      No separate classify_intent_with_docs needed.

    Track 2B (per doc, all docs parallel, independent of 2A+2C):
      Qwen legal entity extraction + Stage2A regex simultaneously per doc.
      Results cached in snapshot.legal_entities_cache[filename].
      Used in Step 8A retrieval — no re-extraction per issue.

    Both tracks fire simultaneously via asyncio.gather.
    Total wait = max(slowest 2A+2C doc, slowest 2B doc).
    2A+2C always dominates, so 2B adds zero time to critical path.

    Returns: (analyses_list, entities_cache_dict)
      analyses_list:      [{...metadata + intent fields, filename}] one per doc
      entities_cache_dict: {filename: raw_entities_dict} for snapshot caching
    """
    active_case      = get_active_case(snapshot)
    user_ctx_text    = get_user_context_text(active_case, limit=3) if active_case else ""
    active_snap_info = None
    if active_case:
        active_snap_info = {
            "parties":          active_case.get("parties"),
            "reference_number": active_case.get("reference_number"),
            "legal_doc_type":   active_case.get("legal_doc_type"),
        }

    # ── Track 2A+2C ───────────────────────────────────────────────────────────
    async def _analyze_one(doc: dict) -> dict:
        t0 = time.monotonic()
        logger.info(
            f"Track 2A+2C: analysing '{doc['filename']}' "
            f"({len(doc['full_text'])} chars)"
        )
        result = await run_in_threadpool(
            analyze_document,
            doc["full_text"],
            resolved_question,
            user_ctx_text,
            active_snap_info,
        )
        result["filename"] = doc["filename"]
        logger.info(
            f"Track 2A+2C done: '{doc['filename']}' — "
            f"classification={result.get('classification')} "
            f"has_issues={result.get('has_issues')} "
            f"intent={result.get('intent')} "
            f"({time.monotonic()-t0:.1f}s)"
        )
        return result

    # ── Track 2B ──────────────────────────────────────────────────────────────
    async def _extract_entities_one(doc: dict) -> tuple:
        t0 = time.monotonic()
        logger.info(f"Track 2B: extracting entities from '{doc['filename']}'")
        entities = await run_in_threadpool(
            extract_legal_entities,
            doc["full_text"],
        )
        logger.info(
            f"Track 2B done: '{doc['filename']}' — "
            f"sections={len(entities.get('sections',[]))} "
            f"notifications={len(entities.get('notifications',[]))} "
            f"({time.monotonic()-t0:.1f}s)"
        )
        return doc["filename"], entities

    t_all = time.monotonic()
    logger.info(
        f"Step 2: starting {len(extracted_docs)} doc(s) — "
        f"Track 2A+2C + Track 2B in parallel"
    )

    # Fire both tracks simultaneously
    analyses_coros  = [_analyze_one(doc) for doc in extracted_docs]
    entities_coros  = [_extract_entities_one(doc) for doc in extracted_docs]

    all_results = await asyncio.gather(
        *analyses_coros,
        *entities_coros,
        return_exceptions=True,
    )

    n = len(extracted_docs)
    raw_analyses = all_results[:n]
    raw_entities = all_results[n:]

    # Process 2A+2C results
    analyses_list = []
    for r in raw_analyses:
        if isinstance(r, Exception):
            logger.error(f"Track 2A+2C failed for a doc: {r}")
            analyses_list.append(None)
        else:
            analyses_list.append(r)

    # Process 2B results
    entities_cache = {}
    for r in raw_entities:
        if isinstance(r, Exception):
            logger.error(f"Track 2B failed for a doc: {r}")
        else:
            filename, entities = r
            entities_cache[filename] = entities

    logger.info(
        f"Step 2 complete: {len(extracted_docs)} doc(s) in "
        f"{time.monotonic()-t_all:.1f}s"
    )
    return analyses_list, entities_cache


# ─── Routing decision (pure logic) ────────────────────────────────────────────

def _build_routing_plan(
    doc_analyses: list,
    snapshot: dict,
    resolved_question: str,
) -> list:
    """
    Determine route for each document. Pure logic, no I/O.

    Each routing entry:
      filename, analysis, route, needs_confirmation, confirmation_message
    """
    active_case   = get_active_case(snapshot)
    has_case      = active_case is not None

    # Detect high-level signals from resolved_question (LLM already embedded these
    # as analysis.same_case and analysis.is_primary, so this is just for
    # explicit user overrides that override LLM classification)
    q_lower = (resolved_question or "").lower()

    plan = []
    for analysis in doc_analyses:
        filename = analysis.get("filename", "document")

        # User said explicitly → extract from question context
        # These were signalled by intent_classifier/user_context, already embedded
        # in analysis.same_case via the LLM call. No keyword matching here.
        route = determine_route(
            analysis,
            has_existing_case=has_case,
        )

        needs_confirmation    = False
        confirmation_message  = None

        if route in ("different_parties", "needs_confirmation"):
            needs_confirmation = True
            if route == "different_parties" and active_case:
                p_existing = active_case.get("parties", {})
                p_new      = analysis.get("parties", {})
                confirmation_message = (
                    f"The document appears to involve different parties "
                    f"({p_new.get('sender') or '?'} → {p_new.get('recipient') or '?'}) "
                    f"from your current case "
                    f"({p_existing.get('sender') or '?'} → {p_existing.get('recipient') or '?'}).\n\n"
                    f"**Option 1** — Add as reference material for current case\n\n"
                    f"**Option 2** — Start a new case for this document"
                )
            else:
                dt = analysis.get("legal_doc_type", "document")
                ps = analysis.get("parties", {})
                confirmation_message = (
                    f"I've identified this as a **{dt}**"
                    f"{(' from ' + ps['sender']) if ps.get('sender') else ''}"
                    f"{(' to ' + ps['recipient']) if ps.get('recipient') else ''}.\n\n"
                    f"Summary: {analysis.get('brief_summary','')[:200]}\n\n"
                    f"Is this correct? Please confirm or describe what this document is."
                )
            route = "pending"

        plan.append({
            "filename":             filename,
            "analysis":             analysis,
            "route":                route,
            "needs_confirmation":   needs_confirmation,
            "confirmation_message": confirmation_message,
        })

    return plan


# ─── Apply routing to snapshot + DB ──────────────────────────────────────────

async def _apply_routing_and_save(
    routing_plan: list,
    extracted_docs: list,
    snapshot: dict,
    session_id: str,
) -> None:
    """
    Apply all confirmed routes:
      - Create/update cases in snapshot
      - Save document texts to DB (parallel)
      - Update is_latest flags and case-level metadata
    """
    # Build filename → full_text map
    text_map = {d["filename"]: d["full_text"] for d in extracted_docs}

    confirmed = [r for r in routing_plan if not r["needs_confirmation"]]

    # Process in route-type order: new cases first, then additions
    # so we have a case to add to
    new_case_routes = [r for r in confirmed if r["route"] in ("new_case_primary", "new_case_reference")]
    add_routes      = [r for r in confirmed if r["route"] not in ("new_case_primary", "new_case_reference")]

    # ── New cases ──────────────────────────────────────────────────────────────
    for entry in new_case_routes:
        analysis  = entry["analysis"]
        route     = entry["route"]
        filename  = entry["filename"]
        is_primary = analysis.get("is_primary", False)
        is_prev_reply = analysis.get("is_previous_reply", False)

        # Archive current active case if it exists
        if get_active_case(snapshot):
            archive_active_case(snapshot)

        new_case_id = get_next_case_id(snapshot)
        parties     = analysis.get("parties") or {"sender": None, "recipient": None}
        new_case    = create_new_case(new_case_id, parties)
        add_case_to_context(snapshot, new_case)

        doc_entry = create_doc_entry(
            filename=filename,
            legal_doc_type=analysis.get("legal_doc_type", "other"),
            is_primary=is_primary and not is_prev_reply,
            is_latest=is_primary,  # first doc in a new case is always latest
            is_replied=is_prev_reply,
            replied_by_doc=None,
            parties=analysis.get("parties") or {},
            reference_number=analysis.get("reference_number"),
            date=analysis.get("date"),
            brief_summary=analysis.get("brief_summary", ""),
            classification_confirmed=True,
            replied_issues=analysis.get("replied_issues") or [],
        )
        add_document_to_case(new_case, doc_entry)

        # Update case-level fields
        if is_primary:
            for field in ("legal_doc_type", "reference_number", "date"):
                if not new_case.get(field) and analysis.get(field):
                    new_case[field] = analysis[field]
            for role in ("sender", "recipient"):
                if not (new_case.get("parties") or {}).get(role):
                    new_case.setdefault("parties", {})[role] = (
                        analysis.get("parties") or {}
                    ).get(role)

    # ── Additions to existing case ─────────────────────────────────────────────
    for entry in add_routes:
        analysis  = entry["analysis"]
        route     = entry["route"]
        filename  = entry["filename"]
        is_primary    = analysis.get("is_primary", False)
        is_prev_reply = analysis.get("is_previous_reply", False)

        active_case = get_active_case(snapshot)
        if not active_case:
            # No active case — create one on the fly
            new_case_id = get_next_case_id(snapshot)
            parties     = analysis.get("parties") or {"sender": None, "recipient": None}
            active_case = create_new_case(new_case_id, parties)
            add_case_to_context(snapshot, active_case)

        if route == "mark_primary_replied":
            # Find matching primary doc by reference_number
            ref = analysis.get("reference_number")
            matched_fn = None
            for doc in active_case.get("documents", []):
                if doc.get("is_primary") and (
                    (ref and doc.get("reference_number") == ref) or
                    (not ref)  # if no ref number, match first unmatched primary
                ):
                    matched_fn = doc["filename"]
                    break
            if matched_fn:
                mark_doc_as_replied(active_case, matched_fn, filename)
            # Still add this reply doc to the documents list as reference
            doc_entry = create_doc_entry(
                filename=filename,
                legal_doc_type="previous_reply",
                is_primary=False,
                is_latest=False,
                is_replied=False,
                replied_by_doc=None,
                parties=analysis.get("parties") or {},
                reference_number=analysis.get("reference_number"),
                date=analysis.get("date"),
                brief_summary=analysis.get("brief_summary", ""),
                classification_confirmed=True,
                replied_issues=analysis.get("replied_issues") or [],
            )
            add_document_to_case(active_case, doc_entry)

        else:
            doc_is_primary = is_primary and not is_prev_reply
            doc_entry = create_doc_entry(
                filename=filename,
                legal_doc_type=analysis.get("legal_doc_type", "other"),
                is_primary=doc_is_primary,
                is_latest=False,  # recalculated below
                is_replied=is_prev_reply,
                replied_by_doc=None,
                parties=analysis.get("parties") or {},
                reference_number=analysis.get("reference_number"),
                date=analysis.get("date"),
                brief_summary=analysis.get("brief_summary", ""),
                classification_confirmed=True,
                replied_issues=analysis.get("replied_issues") or [],
            )
            add_document_to_case(active_case, doc_entry)

            # Update case-level parties from new primary (only fill nulls)
            if doc_is_primary:
                for role in ("sender", "recipient"):
                    if not (active_case.get("parties") or {}).get(role):
                        active_case.setdefault("parties", {})[role] = (
                            analysis.get("parties") or {}
                        ).get(role)

    # ── Recalculate is_latest and case-level metadata for every modified case ──
    for case in snapshot.get("cases", []):
        recalculate_is_latest(case)
        update_case_level_from_latest(case)

    # ── Save texts to DB in parallel ───────────────────────────────────────────
    save_tasks = []
    for entry in confirmed:
        filename  = entry["filename"]
        analysis  = entry["analysis"]
        full_text = text_map.get(filename, "")
        if not full_text:
            continue
        is_primary    = analysis.get("is_primary", False)
        is_prev_reply = analysis.get("is_previous_reply", False)
        route         = entry["route"]

        active_case = get_active_case(snapshot)
        if active_case:
            if is_prev_reply or route == "mark_primary_replied":
                doc_type = "reply_reference"
            elif is_primary:
                doc_type = "primary"
            else:
                doc_type = "reference"

            save_tasks.append(
                save_document_text(
                    session_id, active_case["case_id"], filename, doc_type, full_text
                )
            )

    if save_tasks:
        await asyncio.gather(*save_tasks)


# ─── Issue merge from analyses ────────────────────────────────────────────────

async def _extract_all_issues(
    routing_plan: list,
    snapshot: dict,
    session_id: str,
) -> None:
    """
    Step 6: Extract issues from primary documents with has_issues=True.
    Extract replied-issue pairs from docs with has_replied_issues=True.
    Both run in parallel per document and across documents.
    Also runs parallel with DB save from Step 5.

    Results merged into active case's issues list in snapshot.
    """
    active_case = get_active_case(snapshot)
    if not active_case:
        return

    confirmed_primary = [
        r for r in routing_plan
        if not r.get("needs_confirmation")
        and r["analysis"].get("is_primary")
        and not r["analysis"].get("is_previous_reply")
        and not r["analysis"].get("is_user_draft_reply")
    ]
    reply_docs = [
        r for r in routing_plan
        if not r.get("needs_confirmation")
        and (r["analysis"].get("is_previous_reply") or
             r["analysis"].get("is_user_draft_reply") or
             r["analysis"].get("has_replied_issues"))
    ]

    async def _extract_issues_one(entry: dict):
        filename = entry["filename"]
        analysis = entry["analysis"]
        if not analysis.get("has_issues"):
            logger.debug(f"Step 6: skipping '{filename}' (has_issues=False)")
            return filename, []
        t0 = time.monotonic()
        logger.info(f"Step 6: extracting issues from '{filename}'")
        # Fetch this specific doc's text for focused extraction
        full_text = await get_text_by_filename(session_id, active_case["case_id"], filename)
        if not full_text:
            # Fallback to consolidated primary text
            full_text = await get_primary_texts(session_id, active_case["case_id"])
        if not full_text:
            return filename, []
        new_issues = await run_in_threadpool(
            extract_issues,
            full_text,
            active_case.get("issues", []),
        )
        logger.info(
            f"Step 6 done: '{filename}' — {len(new_issues)} new issues "
            f"({time.monotonic()-t0:.1f}s)"
        )
        return filename, new_issues

    async def _extract_replied_one(entry: dict):
        filename = entry["filename"]
        analysis = entry["analysis"]
        if not analysis.get("has_replied_issues"):
            return filename, []
        t0 = time.monotonic()
        logger.info(f"Step 6: extracting replied-issue pairs from '{filename}'")
        full_text = await get_text_by_filename(session_id, active_case["case_id"], filename)
        if not full_text:
            return filename, []
        replied = await run_in_threadpool(extract_replied_issues, full_text)
        logger.info(
            f"Step 6 replied done: '{filename}' — {len(replied)} pairs "
            f"({time.monotonic()-t0:.1f}s)"
        )
        return filename, replied

    # Fire all extractions in parallel
    issue_coros  = [_extract_issues_one(r) for r in confirmed_primary]
    replied_coros = [_extract_replied_one(r) for r in reply_docs]
    all_coros    = issue_coros + replied_coros

    if not all_coros:
        return

    results = await asyncio.gather(*all_coros, return_exceptions=True)

    # Merge new issues into snapshot
    for r in results[:len(issue_coros)]:
        if isinstance(r, Exception):
            logger.error(f"Step 6 issue extraction error: {r}")
            continue
        filename, new_issues = r
        if new_issues:
            from services.document.doc_context import merge_issues
            active_case["issues"] = merge_issues(
                active_case.get("issues", []),
                new_issues,
                source_doc=filename,
            )

    # Store replied-issue pairs on the matching doc entry
    for r in results[len(issue_coros):]:
        if isinstance(r, Exception):
            logger.error(f"Step 6 replied-issue extraction error: {r}")
            continue
        filename, replied_pairs = r
        if replied_pairs:
            for doc_entry in active_case.get("documents", []):
                if doc_entry["filename"] == filename:
                    doc_entry["replied_issues"] = replied_pairs
                    break
            # Mark matching issues as has_reply_doc
            for pair in replied_pairs:
                issue_text = pair.get("issue_text", "")
                for iss in active_case.get("issues", []):
                    a = issue_text[:80].lower()
                    b = iss["text"][:80].lower()
                    shorter = min(len(a), len(b))
                    if shorter > 0:
                        common = sum(1 for x, y in zip(a, b) if x == y)
                        if common / shorter > 0.80:
                            if not iss.get("reply"):
                                iss["status"]       = "has_reply_doc"
                                iss["replied_by_doc"] = filename
                            break

    # Rebuild case summary
    active_case["summary"] = build_case_summary(active_case)


# ─── Issue extraction helper (single doc text) ────────────────────────────────


# ─── Pending confirmation resolution ─────────────────────────────────────────

async def _resolve_pending_confirmations(
    question: str,
    snapshot: dict,
    session_id: str,
    user_id: int,
) -> AsyncGenerator[str, None]:
    """
    Handle user's response to a pending classification confirmation.
    """
    pending = snapshot.get("_pending_confirmations", [])
    if not pending:
        return

    q_lower = question.lower()

    # Check if this is an Option 1 / Option 2 response for different-parties
    is_option1 = any(w in q_lower for w in ["option 1", "reference", "same case", "current case"])
    is_option2 = any(w in q_lower for w in ["option 2", "new case", "separate", "different case"])

    resolved = []
    still_pending = []

    for pend in pending:
        proposed = pend["proposed_metadata"]

        if is_option1 and "different_parties" in pend.get("original_route", ""):
            proposed["is_primary"] = False
            proposed["is_previous_reply"] = False
            resolved.append({
                "filename":            pend["filename"],
                "analysis":            proposed,
                "route":               "add_to_case_reference",
                "needs_confirmation":  False,
                "confirmation_message": None,
            })
        elif is_option2 and "different_parties" in pend.get("original_route", ""):
            resolved.append({
                "filename":            pend["filename"],
                "analysis":            proposed,
                "route":               "new_case_primary",
                "needs_confirmation":  False,
                "confirmation_message": None,
            })
        elif any(w in q_lower for w in ["yes", "correct", "right", "ok", "confirm"]):
            proposed["classification_confirmed"] = True
            resolved.append({
                "filename":           pend["filename"],
                "analysis":           proposed,
                "route":              pend.get("proposed_route", "add_to_case_primary"),
                "needs_confirmation": False,
                "confirmation_message": None,
            })
        else:
            # Keep pending — user response not clear
            still_pending.append(pend)

    snapshot["_pending_confirmations"] = still_pending

    if resolved:
        # Get full texts from DB (saved during initial upload)
        fake_extracted = []
        for r in resolved:
            full_text = await get_primary_texts(session_id, 0)  # fallback
            fake_extracted.append({"filename": r["filename"], "full_text": full_text})

        await _apply_routing_and_save(resolved, fake_extracted, snapshot, session_id)
        await _extract_all_issues(resolved, snapshot, session_id)

    if still_pending:
        msg = pending[0].get("confirmation_message", "Could you please clarify?")
        yield _content(msg)
        asst = await add_message(session_id, "assistant", msg, user_id)
        yield _retrieval_event(session_id, getattr(asst, "id", None))


# ─── Query chatbot fallback ───────────────────────────────────────────────────

async def _handle_query_fallback(
    question: str,
    session_id: str,
    user_id: int,
    history: list,
    background_tasks: BackgroundTasks,
    db: AsyncSession,
) -> AsyncGenerator[str, None]:
    from retrieval import SessionMessage
    from services.document.issue_replier import _get_pipeline

    pipeline = _get_pipeline()
    if pipeline is None:
        msg = "Pipeline not ready. Please try again."
        yield _content(msg)
        await add_message(session_id, "assistant", msg, user_id)
        return

    pipeline_history = []
    pending_q = None
    for msg in (history or []):
        role    = msg.get("role") if isinstance(msg, dict) else getattr(msg, "role", "")
        content = msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", "")
        if role == "user":
            pending_q = content
        elif role == "assistant" and pending_q:
            pipeline_history.append(SessionMessage(user_query=pending_q, llm_response=content))
            pending_q = None

    answer_parts = []
    try:
        staged = await run_in_threadpool(
            pipeline.query_stages_1_to_5, question, pipeline_history[-3:]
        )
        for chunk in pipeline.query_stage_6_stream(*staged):
            if chunk.startswith("\n\n__META__"):
                try:
                    meta = json.loads(chunk[len("\n\n__META__"):])
                except Exception:
                    meta = {}
                full_answer = "".join(answer_parts)
                await add_message(session_id, "assistant", full_answer, user_id)
                if _should_update_profile(question):
                    background_tasks.add_task(auto_update_profile, user_id, question, full_answer)
                yield _emit({
                    "type":    "retrieval",
                    "sources": meta.get("retrieved_documents", []),
                    "session_id": session_id,
                    "document_analysis": None,
                })
            else:
                answer_parts.append(chunk)
                yield _content(chunk)
    except Exception as e:
        logger.error(f"Query fallback error: {e}", exc_info=True)
        msg = "An error occurred. Please try again."
        yield _content(msg)
        await add_message(session_id, "assistant", msg, user_id)


# ─── Snapshot display (Case 1) ────────────────────────────────────────────────

async def _handle_show_snapshot(
    active_case: dict,
    session_id: str,
    user_id: int,
) -> AsyncGenerator[str, None]:
    """Stream the case summary, documents, and issues."""
    parties = active_case.get("parties", {})
    summary = active_case.get("summary", "")
    issues  = active_case.get("issues", [])
    docs    = active_case.get("documents", [])

    lines = []
    if parties.get("sender"):
        lines.append(f"**From:** {parties['sender']}")
    if parties.get("recipient"):
        lines.append(f"**To:** {parties['recipient']}")
    if active_case.get("reference_number"):
        lines.append(f"**Reference:** {active_case['reference_number']}")
    if active_case.get("date"):
        lines.append(f"**Date:** {active_case['date']}")
    if lines:
        lines.append("")

    if summary:
        lines.append(summary)
        lines.append("")

    # Documents list
    primary_docs   = [d for d in docs if d.get("is_primary")]
    reference_docs = [d for d in docs if not d.get("is_primary")]

    if primary_docs:
        lines.append("**Documents:**")
        for d in primary_docs:
            tags = []
            if d.get("is_latest"):
                tags.append("LATEST")
            if d.get("is_replied"):
                tags.append("REPLIED ✓")
            tag_str = f" [{', '.join(tags)}]" if tags else ""
            lines.append(
                f"• {d['filename']} — {d.get('legal_doc_type','document')} | "
                f"Date: {d.get('date','N/A')} | Ref: {d.get('reference_number','N/A')}{tag_str}"
            )
    if reference_docs:
        lines.append("\n**Reference Material:**")
        for d in reference_docs:
            lines.append(f"• {d['filename']} ({d.get('legal_doc_type','reference')})")

    if issues:
        lines.append("\n\n**Issues / Allegations:**\n")
        for i in issues:
            status_tag = ""
            if i.get("reply"):
                status_tag = " ✅"
            elif i.get("status") == "has_reply_doc":
                status_tag = " 📄 (reply doc provided)"
            lines.append(f"{i['id']}. {i['text']}{status_tag}")

        pending = get_pending_issues(active_case)
        if pending:
            lines.append(
                f"\n\n{len(pending)} issue(s) pending reply. "
                "Should I prepare draft replies? "
                "Please specify: **Defence** (protect the recipient) or **In Favour** of the notice."
            )
        else:
            lines.append("\n\nAll issues have replies. Ask me to update any specific one.")
    else:
        lines.append("\n\nNo specific issues or allegations found in this document.")

    full_text  = "\n".join(lines)
    chunk_size = 300
    for i in range(0, len(full_text), chunk_size):
        yield _content(full_text[i:i + chunk_size])

    active_case["state"] = "awaiting_decision"
    asst = await add_message(session_id, "assistant", full_text, user_id)
    yield _retrieval_event(
        session_id,
        message_id=getattr(asst, "id", None),
        document_analysis=_snapshot_for_display(active_case),
    )


# ─── Draft issues (Cases 2, 4) ────────────────────────────────────────────────

async def _handle_draft_issues(
    active_case: dict,
    issues_to_draft: list,
    session_id: str,
    user_id: int,
    question: str,
    background_tasks: BackgroundTasks,
    db: AsyncSession,
    snapshot: dict = None,
) -> AsyncGenerator[str, None]:
    mode        = active_case.get("mode", MODE_DEFENSIVE)
    recipient   = (active_case.get("parties") or {}).get("recipient")
    doc_summary = active_case.get("summary", "")
    total_global = len(active_case["issues"])

    ref_text         = await get_reference_texts(session_id, active_case["case_id"])
    previous_replies = _build_previous_replies_text(active_case)

    # Build Stage2BResult cache from snapshot for each issue's source_doc
    stage2b_results: Dict[str, object] = {}
    entities_cache = (snapshot or {}).get("legal_entities_cache", {})
    for filename, raw_entities in entities_cache.items():
        try:
            stage2b_results[filename] = entities_to_stage2b_result(raw_entities)
        except Exception as e:
            logger.debug(f"Could not build Stage2BResult for '{filename}': {e}")

    all_sources     = []
    full_reply_text = ""
    active_case["state"] = "reply_in_progress"

    async for issue_number, reply, sources, usage in process_issues_streaming(
        issues=issues_to_draft,          # list of issue dicts with source_doc
        mode=mode,
        recipient=recipient,
        doc_summary=doc_summary,
        reference_docs_text=ref_text,
        previous_replies_text=previous_replies,
        stage2b_results=stage2b_results,
        max_parallel=3,
    ):
        await track_usage(user_id, session_id, db, usage=usage)
        issue_obj  = issues_to_draft[issue_number - 1]
        global_id  = issue_obj["id"]
        issue_text = issue_obj["text"]

        header = f"\n\n---\n\n### Issue {global_id} of {total_global}\n\n> {issue_text}\n\n"
        yield _content(header)
        yield _emit({
            "type": "issue_start", "issue_number": global_id,
            "issue_text": issue_text, "total_issues": total_global,
        })

        for i in range(0, len(reply), 50):
            yield _content(reply[i:i + 50])
        yield _emit({"type": "issue_end", "issue_number": global_id})

        full_reply_text += f"\n\n### Issue {global_id}: {issue_text}\n\n{reply}"

        for iss in active_case["issues"]:
            if iss["id"] == global_id:
                iss["reply"]  = reply
                iss["status"] = "replied"
                break

        all_sources.extend(sources)

    closing = (
        "\n\n---\n\n**Respectfully submitted.**\n\n"
        f"*For {recipient or 'the Taxpayer'}*\n\n"
        "Authorised Signatory / Chartered Accountant / Legal Representative"
        "\n\nDate: [Insert Date]"
    )
    for i in range(0, len(closing), 50):
        yield _content(closing[i:i + 50])
    full_reply_text += closing

    active_case["state"] = "complete"
    asst = await add_message(session_id, "assistant", full_reply_text, user_id)
    yield _retrieval_event(
        session_id,
        message_id=getattr(asst, "id", None),
        sources=all_sources,
        document_analysis=_snapshot_for_display(active_case),
    )
    if _should_update_profile(question):
        background_tasks.add_task(auto_update_profile, user_id, question, full_reply_text)


# ─── Update issues (Case 5) ───────────────────────────────────────────────────

async def _handle_update_issues(
    active_case: dict,
    question: str,
    session_id: str,
    user_id: int,
) -> AsyncGenerator[str, None]:
    current_issues = active_case.get("issues", [])
    update = await run_in_threadpool(parse_issue_update, question, current_issues)
    action = update.get("action")

    if action == "reextract":
        full_text = await get_primary_texts(session_id, active_case["case_id"])
        if not full_text.strip():
            msg = (
                "Could not find the original document text to re-analyse. "
                "Please describe the missing issue directly."
            )
            yield _content(msg)
            await add_message(session_id, "assistant", msg, user_id)
            return

        new_texts = await run_in_threadpool(reextract_missed_issues, full_text, current_issues)
        if new_texts:
            active_case["issues"] = merge_issues(current_issues, new_texts, "reextracted")
            lines = ["I found additional issues:\n"]
            for t in new_texts:
                lines.append(f"- {t}")
            lines.append("\n\nUpdated issues list:\n")
            for i in active_case["issues"]:
                tag = " ✅" if i.get("reply") else ""
                lines.append(f"{i['id']}. {i['text']}{tag}")
            lines.append("\n\nShould I generate replies for the new issues?")
            response_text = "\n".join(lines)
        else:
            response_text = (
                "I re-read the document but found no additional issues. "
                "Could you describe the missing issue?"
            )
    else:
        apply_issue_update(active_case, update)
        lines = ["Issues list updated:\n"]
        for i in active_case.get("issues", []):
            tag = " ✅" if i.get("reply") else ""
            lines.append(f"{i['id']}. {i['text']}{tag}")
        if get_pending_issues(active_case):
            lines.append("\n\nShould I generate replies for the updated issue(s)?")
        response_text = "\n".join(lines)

    for i in range(0, len(response_text), 300):
        yield _content(response_text[i:i + 300])
    asst = await add_message(session_id, "assistant", response_text, user_id)
    yield _retrieval_event(session_id, message_id=getattr(asst, "id", None))


# ─── Update single reply (Case 6) ─────────────────────────────────────────────

async def _handle_update_reply(
    active_case: dict,
    issue_id: int,
    session_id: str,
    user_id: int,
    background_tasks: BackgroundTasks,
    snapshot: dict = None,
) -> AsyncGenerator[str, None]:
    from services.document.issue_replier import _process_single_issue

    all_issues = active_case.get("issues", [])
    target     = next((i for i in all_issues if i["id"] == issue_id), None)

    if not target:
        msg = f"Issue {issue_id} not found."
        yield _content(msg)
        await add_message(session_id, "assistant", msg, user_id)
        return

    mode        = active_case.get("mode", MODE_DEFENSIVE)
    recipient   = (active_case.get("parties") or {}).get("recipient")
    doc_summary = active_case.get("summary", "")
    ref_text    = await get_reference_texts(session_id, active_case["case_id"])
    prev_replies = _build_previous_replies_text(active_case)
    all_texts    = [i["text"] for i in all_issues]
    issue_num    = (all_texts.index(target["text"]) + 1) if target["text"] in all_texts else 1

    # Get cached Stage2BResult for this issue's source document
    stage2b = None
    source_doc = target.get("source_doc")
    if source_doc and snapshot:
        raw_ent = snapshot.get("legal_entities_cache", {}).get(source_doc)
        if raw_ent:
            try:
                stage2b = entities_to_stage2b_result(raw_ent)
            except Exception:
                pass

    header = f"\n\n---\n\n### Updated Reply — Issue {issue_id}\n\n> {target['text']}\n\n"
    yield _content(header)

    _, reply, sources, _ = await run_in_threadpool(
        _process_single_issue,
        target["text"], issue_num, len(all_issues), all_texts,
        mode, recipient, doc_summary, ref_text, prev_replies, stage2b,
    )

    for i in range(0, len(reply), 50):
        yield _content(reply[i:i + 50])

    for iss in all_issues:
        if iss["id"] == issue_id:
            iss["reply"]  = reply
            iss["status"] = "user_edited"
            break

    asst = await add_message(session_id, "assistant", reply, user_id)
    yield _retrieval_event(
        session_id,
        message_id=getattr(asst, "id", None),
        sources=sources,
    )


# ─── Query with document context (Case 3) ────────────────────────────────────

async def _handle_query_with_doc(
    active_case: Optional[dict],
    question: str,
    session_id: str,
    user_id: int,
    history: list,
    background_tasks: BackgroundTasks,
    db: AsyncSession,
) -> AsyncGenerator[str, None]:
    from retrieval import SessionMessage
    from services.document.issue_replier import _get_pipeline

    doc_ctx = None
    if active_case:
        full_doc = await get_primary_texts(session_id, active_case["case_id"])
        doc_ctx  = full_doc[:4000] if full_doc.strip() else active_case.get("summary", "")

    pipeline = _get_pipeline()
    if pipeline is None:
        msg = "Pipeline not ready. Please try again."
        yield _content(msg)
        await add_message(session_id, "assistant", msg, user_id)
        return

    pipeline_history = []
    pending_q = None
    for msg in (history or []):
        role    = msg.get("role") if isinstance(msg, dict) else getattr(msg, "role", "")
        content = msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", "")
        if role == "user":
            pending_q = content
        elif role == "assistant" and pending_q:
            pipeline_history.append(SessionMessage(user_query=pending_q, llm_response=content))
            pending_q = None

    augmented = question
    if doc_ctx:
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
                await add_message(session_id, "assistant", full_answer, user_id)
                await track_usage(user_id, session_id, db)
                if _should_update_profile(question):
                    background_tasks.add_task(auto_update_profile, user_id, question, full_answer)
                yield _emit({
                    "type":    "retrieval",
                    "sources": meta.get("retrieved_documents", []),
                    "session_id": session_id,
                })
            else:
                answer_parts.append(chunk)
                yield _content(chunk)
    except Exception as e:
        logger.error(f"Query with doc error: {e}", exc_info=True)
        msg = "An error occurred while generating the response."
        yield _content(msg)
        await add_message(session_id, "assistant", msg, user_id)


# ─── Main endpoint ────────────────────────────────────────────────────────────

@router.post("/stream")
async def document_stream(
    background_tasks: BackgroundTasks,
    question:   str              = Form(default=""),
    session_id: Optional[str]    = Form(default=None),
    files:      List[UploadFile] = File(default=[]),
    user=Depends(auth_guard),
    db: AsyncSession = Depends(get_db),
):
    """
    Document feature — all cases handled here.
    Returns application/x-ndjson stream.
    """
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

    # Save and validate uploaded files
    temp_file_paths = []
    if has_files:
        if len([f for f in files if f.filename]) > 10:
            raise HTTPException(status_code=400, detail="Maximum 10 files per upload.")
        for f in files:
            if not f.filename:
                continue
            ext = os.path.splitext(f.filename)[1].lower()
            if ext not in SUPPORTED:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported format: {f.filename}. Supported: {', '.join(sorted(SUPPORTED))}",
                )
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                shutil.copyfileobj(f.file, tmp)
                temp_file_paths.append((tmp.name, ext, f.filename))

    async def stream_generator() -> AsyncGenerator[str, None]:
        snapshot_ref = [None]  # for finally block

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

            # Credit deduction (once per session)
            await track_usage(user_id, session_id, db, force_deduct=is_new)

            # ── Handle pending confirmation response ───────────────────────
            if snapshot.get("_pending_confirmations") and not has_files:
                async for chunk in _resolve_pending_confirmations(
                    question, snapshot, session_id, user_id
                ):
                    yield chunk
                return

            # ── No docs, no active case → pure query chatbot ───────────────
            active_case = get_active_case(snapshot)
            if not has_files and not active_case:
                if question.strip():
                    async for chunk in _handle_query_fallback(
                        question, session_id, user_id, history, background_tasks, db
                    ):
                        yield chunk
                return

            # ── Step 0: Query rewrite (text-only requests with history) ────
            resolved_question = question
            if question.strip() and history and not has_files:
                resolved_question = await run_in_threadpool(
                    rewrite_query_if_needed, question, history, snapshot
                )

            # ── Step 1: Page extraction ────────────────────────────────────
            extracted_docs = []
            if has_files:
                extracted_docs, extraction_errors = await _extract_all_documents(
                    temp_file_paths, snapshot
                )
                for err_msg in extraction_errors:
                    yield _content(f"⚠️ {err_msg}\n\n")

                if not extracted_docs:
                    msg = "No documents could be processed. Please check the files and try again."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)
                    return

                # Append user instruction to active case user_context
                if resolved_question.strip():
                    ac = get_active_case(snapshot)
                    if ac:
                        append_user_context(ac, resolved_question, applied_to="session")

            # ── Step 2: Analysis ───────────────────────────────────────────
            doc_analyses  = []
            entities_cache: Dict[str, dict] = {}
            intent_result = {}

            if has_files:
                # Track 2A+2C + Track 2B — fire simultaneously
                doc_analyses, entities_cache = await _run_tracks_2ac_and_2b(
                    extracted_docs, resolved_question, snapshot
                )
                # Remove None results from failed analyses
                doc_analyses = [a for a in doc_analyses if a is not None]

                # Intent comes from combined 2A+2C result.
                # Use the first primary doc's intent, or the first doc's intent.
                primary_analysis = next(
                    (a for a in doc_analyses if a.get("is_primary")),
                    doc_analyses[0] if doc_analyses else {}
                )
                intent_result = {
                    "intent":       primary_analysis.get("intent", "summarize"),
                    "mode":         primary_analysis.get("mode"),
                    "issue_numbers": primary_analysis.get("issue_numbers", []),
                    "case_id":      primary_analysis.get("case_id"),
                }
            else:
                # Text-only request — intent classification only
                intent_result = await run_in_threadpool(
                    classify_intent_no_docs, resolved_question, snapshot
                )

            intent        = intent_result.get("intent", "summarize")
            mode          = intent_result.get("mode")
            issue_numbers = [int(x) for x in (intent_result.get("issue_numbers") or [])]
            target_case_id = intent_result.get("case_id")

            # ── Step 3: Routing ────────────────────────────────────────────
            routing_plan = []
            if has_files:
                routing_plan = _build_routing_plan(
                    doc_analyses, snapshot, resolved_question
                )

            # ── Step 4: Handle confirmation-needed docs ────────────────────
            need_conf = [r for r in routing_plan if r["needs_confirmation"]]
            confirmed = [r for r in routing_plan if not r["needs_confirmation"]]

            if need_conf:
                # Process confirmed docs immediately
                if confirmed:
                    await _apply_routing_and_save(
                        confirmed, extracted_docs, snapshot, session_id
                    )
                    await _extract_all_issues(confirmed, snapshot, session_id)

                # Store pending in snapshot
                snapshot["_pending_confirmations"] = [
                    {
                        "filename":        r["filename"],
                        "proposed_metadata": r["analysis"],
                        "original_route":  r["route"],
                        "proposed_route":  "add_to_case_primary" if r["analysis"].get("is_primary") else "add_to_case_reference",
                        "confirmation_message": r["confirmation_message"],
                    }
                    for r in need_conf
                ]

                # Emit all confirmation messages
                all_conf_msgs = [r["confirmation_message"] for r in need_conf if r["confirmation_message"]]
                msg = "\n\n".join(all_conf_msgs)
                yield _content(msg)
                asst = await add_message(session_id, "assistant", msg, user_id)
                yield _retrieval_event(session_id, getattr(asst, "id", None))
                return

            # ── Step 5: Apply routing ──────────────────────────────────────
            if confirmed:
                # Cache Stage2BResult entities in snapshot for Step 8
                snapshot.setdefault("legal_entities_cache", {})
                for filename, raw_entities in entities_cache.items():
                    snapshot["legal_entities_cache"][filename] = raw_entities

                # DB save + issue extraction run in parallel
                await asyncio.gather(
                    _apply_routing_and_save(
                        confirmed, extracted_docs, snapshot, session_id
                    ),
                    # Step 6 fires here in parallel with DB save
                    _extract_all_issues(confirmed, snapshot, session_id),
                )

            # Reload active case after routing
            active_case = get_active_case(snapshot)

            # ── Step 7+8: Intent routing to handlers ───────────────────────

            # Override intent when files were in this request
            if has_files:
                if intent in ("draft_direct", "draft_all", "summarize_then_draft"):
                    pass  # keep as-is
                elif intent in ("query_general", "query_document", "query_mixed"):
                    intent = "query_with_doc"
                else:
                    intent = "summarize"

            logger.info(
                f"Doc intent={intent} mode={mode} issues={issue_numbers} "
                f"session={session_id[:8]}"
            )

            # ── INTENT: summarize ──────────────────────────────────────────
            if intent == "summarize":
                if not active_case:
                    yield _content("No document uploaded yet. Please upload a document to get started.")
                    await add_message(session_id, "assistant", "No document uploaded yet.", user_id)
                else:
                    async for chunk in _handle_show_snapshot(active_case, session_id, user_id):
                        yield chunk

            # ── INTENT: draft_direct ───────────────────────────────────────
            elif intent == "draft_direct":
                if not active_case:
                    msg = "No active case. Please upload a document first."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    if mode:
                        active_case["mode"] = mode
                    if not active_case.get("mode"):
                        msg = (
                            "\n\nShould I prepare the reply in "
                            "**Defence** (protecting the recipient) or **In Favour** of the notice?"
                        )
                        yield _content(msg)
                        active_case["state"] = "awaiting_mode"
                        await add_message(session_id, "assistant", msg, user_id)
                    else:
                        issues_to_draft = get_draftable_issues(active_case, issue_numbers or None)
                        if not issues_to_draft:
                            msg = "All issues already have replies. Ask me to update any specific one."
                            yield _content(msg)
                            await add_message(session_id, "assistant", msg, user_id)
                        else:
                            async for chunk in _handle_draft_issues(
                                active_case, issues_to_draft, session_id, user_id,
                                resolved_question, background_tasks, db, snapshot
                            ):
                                yield chunk

            # ── INTENT: confirm_mode / draft_all ──────────────────────────
            elif intent in ("confirm_mode", "draft_all"):
                if not active_case:
                    msg = "No active case. Please upload a document first."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    if mode:
                        active_case["mode"] = mode
                    if not active_case.get("mode"):
                        msg = (
                            "Should I prepare the reply in "
                            "**Defence** or **In Favour** of the notice?"
                        )
                        yield _content(msg)
                        active_case["state"] = "awaiting_mode"
                        await add_message(session_id, "assistant", msg, user_id)
                    else:
                        issues_to_draft = get_draftable_issues(active_case, issue_numbers or None)
                        if not issues_to_draft:
                            msg = "All issues already have replies. Ask me to update any specific one."
                            yield _content(msg)
                            await add_message(session_id, "assistant", msg, user_id)
                        else:
                            async for chunk in _handle_draft_issues(
                                active_case, issues_to_draft, session_id, user_id,
                                resolved_question, background_tasks, db, snapshot
                            ):
                                yield chunk

            # ── INTENT: draft_specific ─────────────────────────────────────
            elif intent == "draft_specific":
                if not active_case:
                    msg = "No active case. Please upload a document first."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    if mode:
                        active_case["mode"] = mode
                    if not active_case.get("mode"):
                        msg = "Should I prepare the reply in **Defence** or **In Favour**?"
                        yield _content(msg)
                        active_case["state"] = "awaiting_mode"
                        active_case["_pending_issue_nums"] = issue_numbers
                        await add_message(session_id, "assistant", msg, user_id)
                    else:
                        issues_to_draft = get_draftable_issues(active_case, issue_numbers or None)
                        if not issues_to_draft:
                            msg = "No matching issues found. Check issue numbers."
                            yield _content(msg)
                            await add_message(session_id, "assistant", msg, user_id)
                        else:
                            async for chunk in _handle_draft_issues(
                                active_case, issues_to_draft, session_id, user_id,
                                resolved_question, background_tasks, db, snapshot
                            ):
                                yield chunk

            # ── INTENT: update_issues ──────────────────────────────────────
            elif intent == "update_issues":
                if not active_case:
                    msg = "No active case."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    async for chunk in _handle_update_issues(
                        active_case, resolved_question, session_id, user_id
                    ):
                        yield chunk

            # ── INTENT: update_reply ───────────────────────────────────────
            elif intent == "update_reply":
                if not active_case:
                    msg = "No active case."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)
                elif not issue_numbers:
                    msg = "Please specify which issue number to update (e.g., 'update issue 2')."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    async for chunk in _handle_update_reply(
                        active_case, issue_numbers[0], session_id, user_id, background_tasks, snapshot
                    ):
                        yield chunk

            # ── INTENT: query_with_doc / query_document ────────────────────
            elif intent in ("query_with_doc", "query_document", "query_mixed"):
                async for chunk in _handle_query_with_doc(
                    active_case, resolved_question, session_id, user_id,
                    history, background_tasks, db
                ):
                    yield chunk

            # ── INTENT: query_general ──────────────────────────────────────
            elif intent == "query_general":
                async for chunk in _handle_query_fallback(
                    resolved_question, session_id, user_id,
                    history, background_tasks, db
                ):
                    yield chunk

            # ── INTENT: switch_case ────────────────────────────────────────
            elif intent == "switch_case":
                if target_case_id:
                    switch_active_case(snapshot, target_case_id)
                    switched = get_active_case(snapshot)
                    if switched:
                        p   = switched.get("parties", {})
                        msg = (
                            f"Switched to **Case {target_case_id}** — "
                            f"{p.get('sender', '?')} / {p.get('recipient', '?')}.\n\n"
                            f"{(switched.get('summary') or '')[:300]}"
                        )
                    else:
                        msg = f"Case {target_case_id} not found."
                else:
                    cases = snapshot.get("cases", [])
                    if len(cases) > 1:
                        lines = [
                            f"- Case {c['case_id']} ({c['status']}): "
                            f"{(c.get('parties') or {}).get('sender','?')} / "
                            f"{(c.get('parties') or {}).get('recipient','?')}"
                            for c in cases
                        ]
                        msg = "Available cases:\n" + "\n".join(lines) + "\n\nWhich case to switch to?"
                    else:
                        msg = "Only one case exists in this session."
                yield _content(msg)
                await add_message(session_id, "assistant", msg, user_id)

            # ── INTENT: new_case ───────────────────────────────────────────
            elif intent == "new_case":
                archive_active_case(snapshot)
                snapshot["active_case_id"] = None
                msg = "Starting fresh. Please upload the documents for the new case."
                yield _content(msg)
                await add_message(session_id, "assistant", msg, user_id)

            # ── INTENT: mark_replied ───────────────────────────────────────
            elif intent == "mark_replied":
                if not active_case:
                    msg = "No active case."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    # Mark all pending issues from latest doc as has_reply_doc
                    # (user said they've already replied externally)
                    marked = 0
                    for iss in active_case.get("issues", []):
                        if not iss.get("reply") and iss.get("status") not in ("replied", "has_reply_doc"):
                            iss["status"] = "has_reply_doc"
                            marked += 1
                    msg = f"Marked {marked} issue(s) as externally replied."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)

            # ── Fallback ───────────────────────────────────────────────────
            else:
                async for chunk in _handle_query_with_doc(
                    active_case, resolved_question, session_id, user_id,
                    history, background_tasks, db
                ):
                    yield chunk

        except Exception as e:
            logger.error(f"Document stream error: {e}", exc_info=True)
            yield _emit({"type": "error", "message": "An error occurred. Please try again."})

        finally:
            # Always persist snapshot
            if snapshot_ref[0] is not None:
                try:
                    await set_doc_context(session_id, snapshot_ref[0])
                except Exception as ctx_err:
                    logger.warning(f"Failed to save snapshot in finally: {ctx_err}")

            # Clean up temp files
            for tmp_path, *_ in temp_file_paths:
                if os.path.exists(tmp_path):
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass

    return StreamingResponse(stream_generator(), media_type="application/x-ndjson")