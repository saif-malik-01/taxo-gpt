"""
api/document.py
Document feature router — separate from the query chatbot.

Handles all 11 cases:
    Case 1  Upload + no question / summary request
    Case 2  Upload + draft reply intent
    Case 3  Upload + specific question about document
    Case 4  User confirms draft reply after summary shown
    Case 5  User corrects issues list (merge/add/remove/correct/reextract)
    Case 6  User updates a specific issue reply
    Case 7  Upload another doc, same case same matter → merge issues
    Case 8  Upload another doc, same parties different matter → new case
    Case 9  Upload new doc, different parties → ask user: reference or new case
    Case 10 User switches back to a previous case
    Case 11 User uploads reference document

Extra:
    If user opens document feature but types without uploading → query chatbot behaviour
    Different parties ambiguity → ask user Sub-case 9A (reference) or 9B (new case)

Fixes applied vs original:
  1. force_deduct=is_new only (not `is_new or has_files`) — prevents charging
     a draft credit on every subsequent file upload in the same session.
  2. set_doc_context() moved into finally block — context is always persisted
     even when an exception aborts the stream mid-way.
  3. DocumentProcessor now uses module-level singleton (get_document_processor())
     instead of creating a new instance (and boto3 client) on every request.
  4. auto_update_profile is only scheduled when the user message is substantive
     (> 8 words) — avoids a wasted Bedrock call for "yes", "Option 1", etc.
     The memory_updater itself also has its own trivial-message guard.
"""

import json
import logging
import os
import shutil
import tempfile
import uuid
from typing import AsyncGenerator, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from services.auth.deps import auth_guard
from services.chat.memory_updater import auto_update_profile
from services.database import get_db
from services.document.doc_analyzer import analyze_document, reextract_missed_issues
from services.document.doc_classifier import classify_document, determine_routing
from services.document.doc_context import (
    add_case_to_context, apply_issue_update, clear_doc_context,
    create_empty_context, create_new_case, get_active_case,
    get_doc_context, get_next_case_id, get_pending_issues,
    merge_new_issues_with_existing, set_doc_context, switch_active_case,
)
from services.document.intent_classifier import classify_intent, parse_issue_update
from services.document.issue_replier import (
    MODE_DEFENSIVE, MODE_IN_FAVOUR,
    process_issues_streaming,
)
from services.document.processor import get_document_processor
from services.document.session_doc_store import (
    delete_session_documents, get_primary_texts,
    get_reference_texts, save_document_text,
)
from services.memory import (
    add_message, check_credits, get_session_history, track_usage,
)
from services.models import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/document", tags=["Document"])

SUPPORTED = {'.pdf', '.docx', '.pptx', '.xlsx', '.html', '.png', '.jpg', '.jpeg', '.tiff', '.bmp'}

# Minimum word count for a user message to be worth sending to auto_update_profile.
# Short confirmations ("yes", "ok", "Option 1") cannot contain user profile facts.
_MIN_WORDS_FOR_PROFILE_UPDATE = 8


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _get_db_user(email: str, db: AsyncSession):
    result  = await db.execute(
        select(User).where(func.lower(User.email) == email.lower())
    )
    db_user = result.scalars().first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user


def _emit(data: dict) -> str:
    return json.dumps(data) + "\n"


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


def _is_option1(text: str) -> bool:
    """User chose reference (Option 1 / sub-case 9A)."""
    t = text.lower()
    return any(w in t for w in [
        "option 1", "1", "reference", "refer", "similar", "supporting",
        "same case", "current case", "this case",
    ])


def _is_option2(text: str) -> bool:
    """User chose new draft case (Option 2 / sub-case 9B)."""
    t = text.lower()
    return any(w in t for w in [
        "option 2", "2", "new case", "new", "draft", "reply",
        "different case", "separate", "another case",
    ])


def _should_update_profile(question: str) -> bool:
    """Return True only if the message is substantive enough to contain user facts."""
    return len(question.strip().split()) >= _MIN_WORDS_FOR_PROFILE_UPDATE


# ─────────────────────────────────────────────────────────────────────────────
# Main streaming endpoint
# ─────────────────────────────────────────────────────────────────────────────

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
    Document feature — all 11 cases handled here.
    Returns application/x-ndjson stream.

    If no files ever uploaded in this session → behaves like query chatbot.
    """
    db_user    = await _get_db_user(user.get("sub"), db)
    user_id    = db_user.id
    session_id = session_id or str(uuid.uuid4())

    has_files = bool(files and any(f.filename for f in files))

    allowed, error_msg = await check_credits(
        user_id, session_id, has_files, db,
        chat_mode="draft" if has_files else "simple",
    )
    if not allowed:
        raise HTTPException(status_code=402, detail=error_msg)

    # Save uploaded files to temp paths
    temp_file_paths = []   # list of (tmp_path, ext, filename)
    if has_files:
        for f in files:
            if not f.filename:
                continue
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
        # Keep a reference to the doc_context so we can save it in the
        # finally block even if the stream is aborted by an exception.
        doc_context_ref = [None]

        try:
            history     = await get_session_history(session_id)
            is_new      = len(history) == 0
            doc_context = await get_doc_context(session_id) or create_empty_context()
            doc_context_ref[0] = doc_context

            # ── Save user message ──────────────────────────────────────────
            user_msg = question
            if temp_file_paths:
                fnames    = [tp[2] for tp in temp_file_paths]
                user_msg += f"\n\n[Documents: {', '.join(fnames)}]"
            await add_message(
                session_id, "user", user_msg, user_id,
                chat_mode="draft" if has_files else "simple",
            )

            # ── Credit deduction ───────────────────────────────────────────
            # Deduct only once per session (is_new=True on the first message).
            # NOT on every file upload — a user uploading 3 documents over 3
            # turns should not be charged 3 credits before any reply is made.
            await track_usage(
                user_id, session_id, db,
                force_deduct=is_new,
            )

            # ── FIX 1: No document ever uploaded → query chatbot behaviour ──
            if not has_files and not get_active_case(doc_context) \
                    and not doc_context.get("_pending_different_parties"):
                logger.info("Doc feature: no document yet — routing to query chatbot")
                async for chunk in _handle_query_fallback(
                    question, session_id, user_id, history, background_tasks, db
                ):
                    yield chunk
                return

            # ── FIX 2: Handle pending different-parties clarification ───────
            pending_diff = doc_context.get("_pending_different_parties")
            if pending_diff and not has_files:
                async for chunk in _resolve_different_parties(
                    pending_diff, question, session_id, user_id,
                    doc_context, background_tasks, db
                ):
                    yield chunk
                return

            # ── Process uploaded files ─────────────────────────────────────
            if temp_file_paths:
                async for event in _process_files(
                    temp_file_paths, question, session_id, doc_context
                ):
                    if event.get("type") == "error":
                        yield _emit(event)
                        return
                    elif event.get("type") == "clarification_needed":
                        subtype = event.get("subtype", "")

                        if subtype == "different_parties":
                            doc_context["_pending_different_parties"] = event.get("pending_files", [])
                            msg = event.get("message", "")
                            yield _content(msg)
                            asst = await add_message(session_id, "assistant", msg, user_id)
                            yield _retrieval_event(session_id, getattr(asst, "id", None))
                            return
                        else:
                            msg = event.get("message", "Could you clarify?")
                            yield _content(msg)
                            asst = await add_message(session_id, "assistant", msg, user_id)
                            yield _retrieval_event(session_id, getattr(asst, "id", None))
                            return

            # ── Reload active case after file processing ───────────────────
            active_case = get_active_case(doc_context)

            # ── Classify intent ────────────────────────────────────────────
            intent_result  = await run_in_threadpool(
                classify_intent, question, active_case, bool(temp_file_paths)
            )
            intent         = intent_result.get("intent", "query_general")
            intent_mode    = intent_result.get("mode")
            issue_nums     = [int(x) for x in (intent_result.get("issue_numbers") or [])]
            target_case_id = intent_result.get("case_id")

            # Override intent when files were uploaded in this request
            if temp_file_paths:
                if intent in ("query_document", "query_mixed", "query_general"):
                    intent = "query_with_doc"
                elif intent in ("draft_all", "draft_specific", "confirm_mode"):
                    intent = "summarize_then_draft"
                else:
                    intent = "summarize"

            logger.info(f"Doc intent: {intent} | mode={intent_mode} | issues={issue_nums}")

            # ── Route to handler ───────────────────────────────────────────

            if intent == "summarize_then_draft":
                if not active_case:
                    yield _content("No document found. Please upload a document first.")
                    await add_message(session_id, "assistant", "No document found.", user_id)
                else:
                    async for chunk in _handle_summarize(active_case, session_id, user_id):
                        yield chunk
                    if intent_mode:
                        active_case["mode"] = intent_mode
                        pending = get_pending_issues(active_case)
                        if pending:
                            ref_text = await get_reference_texts(session_id, active_case["case_id"])
                            async for chunk in _handle_draft_issues(
                                active_case, pending, session_id, user_id,
                                question, background_tasks, ref_text, db
                            ):
                                yield chunk

            elif intent == "query_with_doc":
                async for chunk in _handle_query_with_doc(
                    active_case, question, session_id, user_id,
                    history, background_tasks, db
                ):
                    yield chunk

            elif intent == "summarize":
                if not active_case:
                    yield _content("No document uploaded yet.")
                    await add_message(session_id, "assistant", "No document uploaded yet.", user_id)
                else:
                    async for chunk in _handle_summarize(active_case, session_id, user_id):
                        yield chunk

            elif intent in ("confirm_mode", "draft_all"):
                if not active_case:
                    msg = "No active case. Please upload a document first."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    if intent_mode and not active_case.get("mode"):
                        active_case["mode"] = intent_mode
                    if not active_case.get("mode"):
                        msg = (
                            "\n\nShould I prepare the reply in "
                            "**Defence** (protecting the recipient) or "
                            "**In Favour** of the notice?"
                        )
                        yield _content(msg)
                        active_case["state"] = "awaiting_mode"
                        await add_message(session_id, "assistant", msg, user_id)
                    else:
                        pending = get_pending_issues(active_case)
                        if not pending:
                            msg = "All issues already have replies. Ask me to update any specific one."
                            yield _content(msg)
                            await add_message(session_id, "assistant", msg, user_id)
                        else:
                            ref_text = await get_reference_texts(session_id, active_case["case_id"])
                            async for chunk in _handle_draft_issues(
                                active_case, pending, session_id, user_id,
                                question, background_tasks, ref_text, db
                            ):
                                yield chunk

            elif intent == "draft_specific":
                if not active_case:
                    msg = "No active case. Please upload a document first."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    if intent_mode and not active_case.get("mode"):
                        active_case["mode"] = intent_mode
                    if not active_case.get("mode"):
                        msg = "Should I prepare the reply in **Defence** or **In Favour** of the notice?"
                        yield _content(msg)
                        active_case["state"] = "awaiting_mode"
                        active_case["_pending_issue_nums"] = issue_nums
                        await add_message(session_id, "assistant", msg, user_id)
                    else:
                        all_issues    = active_case.get("issues", [])
                        target_issues = (
                            [i for i in all_issues if i["id"] in issue_nums]
                            if issue_nums else get_pending_issues(active_case)
                        )
                        if not target_issues:
                            msg = "No matching issues found. Check issue numbers."
                            yield _content(msg)
                            await add_message(session_id, "assistant", msg, user_id)
                        else:
                            ref_text = await get_reference_texts(session_id, active_case["case_id"])
                            async for chunk in _handle_draft_issues(
                                active_case, target_issues, session_id, user_id,
                                question, background_tasks, ref_text, db
                            ):
                                yield chunk

            elif intent == "update_issues":
                if not active_case:
                    msg = "No active case."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    async for chunk in _handle_update_issues(
                        active_case, question, session_id, user_id
                    ):
                        yield chunk

            elif intent == "update_reply":
                if not active_case:
                    msg = "No active case."
                    yield _content(msg)
                    await add_message(session_id, "assistant", msg, user_id)
                else:
                    target_id = issue_nums[0] if issue_nums else None
                    if not target_id:
                        msg = "Please specify which issue number to update."
                        yield _content(msg)
                        await add_message(session_id, "assistant", msg, user_id)
                    else:
                        ref_text = await get_reference_texts(session_id, active_case["case_id"])
                        async for chunk in _handle_update_reply(
                            active_case, target_id, session_id, user_id,
                            background_tasks, ref_text
                        ):
                            yield chunk

            elif intent == "switch_case":
                if target_case_id:
                    switch_active_case(doc_context, target_case_id)
                    switched = get_active_case(doc_context)
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
                    cases = doc_context.get("cases", [])
                    if len(cases) > 1:
                        lines = [
                            f"- Case {c['case_id']} ({c['status']}): "
                            f"{c.get('parties',{}).get('sender','?')} / "
                            f"{c.get('parties',{}).get('recipient','?')}"
                            for c in cases
                        ]
                        msg = "Available cases:\n" + "\n".join(lines) + "\n\nWhich case to switch to?"
                    else:
                        msg = "Only one case exists in this session."
                yield _content(msg)
                await add_message(session_id, "assistant", msg, user_id)

            elif intent == "new_case":
                from services.document.doc_context import archive_active_case
                archive_active_case(doc_context)
                doc_context["active_case_id"] = None
                msg = "Starting fresh. Please upload the documents for the new case."
                yield _content(msg)
                await add_message(session_id, "assistant", msg, user_id)

            else:
                async for chunk in _handle_query_with_doc(
                    active_case, question, session_id, user_id,
                    history, background_tasks, db
                ):
                    yield chunk

        except Exception as e:
            logger.error(f"document stream error: {e}", exc_info=True)
            yield _emit({"type": "error", "message": "An error occurred. Please try again."})

        finally:
            # Always persist the doc context — even if an exception aborted the
            # stream mid-way.  Without this, partial mutations (e.g. a new case
            # that was added before the crash) would be silently lost.
            if doc_context_ref[0] is not None:
                try:
                    await set_doc_context(session_id, doc_context_ref[0])
                except Exception as ctx_err:
                    logger.warning(f"Failed to save doc_context in finally: {ctx_err}")

            # Clean up temp files
            for tmp_path, *_ in temp_file_paths:
                if os.path.exists(tmp_path):
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass

    return StreamingResponse(stream_generator(), media_type="application/x-ndjson")


# ─────────────────────────────────────────────────────────────────────────────
# FIX 1 — Query fallback when no document uploaded yet
# ─────────────────────────────────────────────────────────────────────────────

async def _handle_query_fallback(
    question, session_id, user_id, history, background_tasks, db
):
    from services.document.issue_replier import _get_pipeline
    from retrieval import SessionMessage

    pipeline = _get_pipeline()
    if pipeline is None:
        msg = "Pipeline not ready. Please try again."
        yield _content(msg)
        await add_message(session_id, "assistant", msg, user_id)
        return

    pipeline_history = []
    pending_q = None
    for msg in (history or []):
        role    = msg.get("role")    if isinstance(msg, dict) else getattr(msg, "role",    "")
        content = msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", "")
        if role == "user":
            pending_q = content
        elif role == "assistant" and pending_q:
            pipeline_history.append(SessionMessage(user_query=pending_q, llm_response=content))
            pending_q = None

    answer_parts = []
    try:
        staged = await run_in_threadpool(
            pipeline.query_stages_1_to_5,
            question,
            pipeline_history[-3:],
        )
        for chunk in pipeline.query_stage_6_stream(*staged):
            if chunk.startswith("\n\n__META__"):
                raw_meta = chunk[len("\n\n__META__"):]
                try:
                    meta = json.loads(raw_meta)
                except Exception:
                    meta = {}
                full_answer = "".join(answer_parts)
                await add_message(session_id, "assistant", full_answer, user_id)
                if _should_update_profile(question):
                    background_tasks.add_task(auto_update_profile, user_id, question, full_answer)
                yield _emit({
                    "type":              "retrieval",
                    "sources":           meta.get("retrieved_documents", []),
                    "session_id":        session_id,
                    "document_analysis": None,
                })
            else:
                answer_parts.append(chunk)
                yield _content(chunk)
    except Exception as e:
        logger.error(f"query fallback error: {e}", exc_info=True)
        msg = "An error occurred. Please try again."
        yield _content(msg)
        await add_message(session_id, "assistant", msg, user_id)


# ─────────────────────────────────────────────────────────────────────────────
# FIX 2 — Resolve different-parties clarification (Case 9A / 9B)
# ─────────────────────────────────────────────────────────────────────────────

async def _resolve_different_parties(
    pending_files, question, session_id, user_id,
    doc_context, background_tasks, db
):
    active_case = get_active_case(doc_context)

    if _is_option1(question):
        if active_case:
            for item in pending_files:
                await save_document_text(
                    session_id, active_case["case_id"],
                    item["filename"], "reference", item["text"]
                )
            doc_context.pop("_pending_different_parties", None)
            msg = (
                f"Got it. I've saved **{', '.join(i['filename'] for i in pending_files)}** "
                "as reference material for your current case. "
                "It will be included when drafting replies."
            )
        else:
            doc_context.pop("_pending_different_parties", None)
            msg = "No active case to add this reference to. Please upload your primary document first."
        yield _content(msg)
        asst = await add_message(session_id, "assistant", msg, user_id)
        yield _retrieval_event(session_id, getattr(asst, "id", None))

    elif _is_option2(question):
        if active_case:
            active_case["status"] = "archived"

        new_case_id = get_next_case_id(doc_context)
        new_case    = create_new_case(new_case_id, {"sender": None, "recipient": None})
        add_case_to_context(doc_context, new_case)
        active_case = new_case

        for item in pending_files:
            await save_document_text(
                session_id, active_case["case_id"],
                item["filename"], "primary", item["text"]
            )

        consolidated = await get_primary_texts(session_id, active_case["case_id"])
        if consolidated.strip():
            analysis = await run_in_threadpool(analyze_document, consolidated, "")
            active_case["summary"] = analysis.get("summary", "")
            active_case["parties"] = {
                "sender":    analysis.get("sender"),
                "recipient": analysis.get("recipient"),
            }
            active_case["issues"] = merge_new_issues_with_existing(
                [], analysis.get("issues", [])
            )

        doc_context.pop("_pending_different_parties", None)

        async for chunk in _handle_summarize(active_case, session_id, user_id):
            yield chunk

    else:
        msg = (
            "Please clarify:\n\n"
            "**Option 1** — Use as a **reference** for your current case\n\n"
            "**Option 2** — Start a **new case** for this document (separate draft reply)"
        )
        yield _content(msg)
        await add_message(session_id, "assistant", msg, user_id)


# ─────────────────────────────────────────────────────────────────────────────
# File processing
# ─────────────────────────────────────────────────────────────────────────────

async def _process_files(temp_file_paths, question, session_id, doc_context):
    """
    Extract → classify → route → save text to DB → update Redis doc_context.
    Uses the module-level DocumentProcessor singleton.
    """
    doc_processor = get_document_processor()

    try:
        # Step 1: Extract text from all files
        file_data = []
        for tmp_path, ext, filename in temp_file_paths:
            try:
                text = await run_in_threadpool(doc_processor.extract_text, tmp_path)
                file_data.append((filename, text or ""))
            except Exception as e:
                yield {"type": "error", "message": f"Could not extract {filename}: {e}"}
                return

        active_case = get_active_case(doc_context)

        # Step 2: Classify each file
        classified = []
        ambiguous  = []

        for filename, text in file_data:
            classification = await run_in_threadpool(
                classify_document, text, filename, question, active_case
            )
            routing = determine_routing(classification, has_existing_case=bool(active_case))

            if routing == "ask_user":
                ambiguous.append((filename, text, classification))
            else:
                classified.append((filename, text, classification, routing))

        # All ambiguous → generic clarification
        if ambiguous and not classified:
            names = [f[0] for f in ambiguous]
            yield {
                "type":    "clarification_needed",
                "subtype": "generic",
                "message": (
                    f"I see you've uploaded **{', '.join(names)}**. "
                    "Is this related to the existing case or a new case?"
                ),
            }
            return

        # Treat remaining ambiguous as add_to_existing
        for filename, text, classification in ambiguous:
            classified.append((filename, text, classification, "add_to_existing"))

        # Step 3: Bucket by routing and primacy
        new_primary   = [(f,t,c) for f,t,c,r in classified if r == "new_case"        and     c.get("is_primary", True)]
        new_ref       = [(f,t,c) for f,t,c,r in classified if r == "new_case"        and not c.get("is_primary", True)]
        exist_primary = [(f,t,c) for f,t,c,r in classified if r == "add_to_existing" and     c.get("is_primary", True)]
        exist_ref     = [(f,t,c) for f,t,c,r in classified if r == "add_to_existing" and not c.get("is_primary", True)]

        # Step 4: Handle new_case bucket
        if new_primary:
            if active_case:
                names = [fp[0] for fp in new_primary]
                yield {
                    "type":    "clarification_needed",
                    "subtype": "different_parties",
                    "message": (
                        f"The document **{', '.join(names)}** appears to involve "
                        f"different parties from your current case "
                        f"({active_case.get('parties',{}).get('sender','?')} / "
                        f"{active_case.get('parties',{}).get('recipient','?')}).\n\n"
                        "How should I use it?\n\n"
                        "**Option 1** — Use as a **reference** for your current case "
                        "(e.g. a similar judgment, previous notice, or supporting material)\n\n"
                        "**Option 2** — Start a **new case** for this document and prepare a separate draft reply"
                    ),
                    "pending_files": [
                        {"filename": fp[0], "text": fp[1], "is_primary": True}
                        for fp in new_primary
                    ],
                }
                return
            else:
                new_case_id = get_next_case_id(doc_context)
                parties     = new_primary[0][2].get("parties") or {"sender": None, "recipient": None}
                new_case    = create_new_case(new_case_id, parties)
                add_case_to_context(doc_context, new_case)
                active_case = new_case

                for fname, text, _ in new_primary:
                    await save_document_text(session_id, active_case["case_id"], fname, "primary", text)
                for fname, text, _ in new_ref:
                    await save_document_text(session_id, active_case["case_id"], fname, "reference", text)

        # Step 5: Handle add_to_existing bucket
        if exist_primary:
            if not active_case:
                new_case_id = get_next_case_id(doc_context)
                parties     = exist_primary[0][2].get("parties") or {"sender": None, "recipient": None}
                active_case = create_new_case(new_case_id, parties)
                add_case_to_context(doc_context, active_case)
            for fname, text, _ in exist_primary:
                await save_document_text(session_id, active_case["case_id"], fname, "primary", text)

        # Case 11: Reference documents
        if exist_ref and active_case:
            for fname, text, _ in exist_ref:
                await save_document_text(session_id, active_case["case_id"], fname, "reference", text)

        # Step 6: Analyze consolidated primary text
        if active_case:
            consolidated = await get_primary_texts(session_id, active_case["case_id"])
            if consolidated.strip():
                analysis = await run_in_threadpool(analyze_document, consolidated, question)
                active_case["summary"] = analysis.get("summary", "")
                for field in ("sender", "recipient"):
                    if not active_case["parties"].get(field) and analysis.get(field):
                        active_case["parties"][field] = analysis[field]
                active_case["issues"] = merge_new_issues_with_existing(
                    active_case.get("issues", []),
                    analysis.get("issues", []),
                )

    except Exception as e:
        logger.error(f"_process_files error: {e}", exc_info=True)
        yield {"type": "error", "message": f"Document processing error: {e}"}


# ─────────────────────────────────────────────────────────────────────────────
# Case handlers
# ─────────────────────────────────────────────────────────────────────────────

async def _handle_summarize(active_case, session_id, user_id):
    """Cases 1, 2 — Show summary + issues, ask for mode."""
    summary = active_case.get("summary", "")
    issues  = active_case.get("issues", [])
    parties = active_case.get("parties", {})

    lines = []
    if parties.get("sender"):
        lines.append(f"**From:** {parties['sender']}")
    if parties.get("recipient"):
        lines.append(f"**To:** {parties['recipient']}")
    if lines:
        lines.append("")

    lines.append(summary or "Summary not available.")

    if issues:
        lines.append("\n\n**Issues / Allegations:**\n")
        for i in issues:
            status_tag = " ✅" if i.get("reply") else ""
            lines.append(f"{i['id']}. {i['text']}{status_tag}")
        pending_count = sum(1 for i in issues if not i.get("reply"))
        if pending_count:
            lines.append(
                f"\n\nShould I prepare draft replies for these {pending_count} issue(s)? "
                "If yes — **Defence** (protecting the recipient) or **In Favour** of the notice?"
            )
        else:
            lines.append("\n\nAll issues already have replies. You can ask me to update any specific one.")
    else:
        lines.append("\n\nNo specific issues or allegations were found in this document.")

    full_text  = "\n".join(lines)
    chunk_size = 200
    for i in range(0, len(full_text), chunk_size):
        yield _content(full_text[i:i+chunk_size])

    active_case["state"] = "awaiting_decision"
    asst = await add_message(session_id, "assistant", full_text, user_id)
    yield _retrieval_event(
        session_id,
        message_id=getattr(asst, "id", None),
        document_analysis={
            "summary": active_case.get("summary"),
            "issues":  active_case.get("issues"),
            "parties": active_case.get("parties"),
        },
    )


async def _handle_draft_issues(
    active_case, issues_to_draft, session_id, user_id,
    question, background_tasks, ref_text, db
):
    """Cases 2, 4 — Generate draft replies for each issue."""
    mode         = active_case.get("mode", MODE_DEFENSIVE)
    recipient    = active_case.get("parties", {}).get("recipient")
    sender       = active_case.get("parties", {}).get("sender")
    doc_summary  = active_case.get("summary", "")
    total_global = len(active_case["issues"])

    all_sources     = []
    full_reply_text = ""

    active_case["state"] = "reply_in_progress"

    async for issue_number, reply, sources, usage in process_issues_streaming(
        issues=[i["text"] for i in issues_to_draft],
        mode=mode,
        recipient=recipient,
        sender=sender,
        doc_summary=doc_summary,
        reference_docs_text=ref_text,
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

        chunk_size = 50
        for i in range(0, len(reply), chunk_size):
            yield _content(reply[i:i+chunk_size])
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
        yield _content(closing[i:i+50])
    full_reply_text += closing

    active_case["state"] = "complete"
    asst = await add_message(session_id, "assistant", full_reply_text, user_id)
    yield _retrieval_event(
        session_id,
        message_id=getattr(asst, "id", None),
        sources=all_sources,
        document_analysis={
            "summary": active_case.get("summary"),
            "issues":  active_case.get("issues"),
            "parties": active_case.get("parties"),
        },
    )
    # Only schedule profile update for substantive questions (not "yes"/"defence")
    if _should_update_profile(question):
        background_tasks.add_task(auto_update_profile, user_id, question, full_reply_text)


async def _handle_update_issues(active_case, question, session_id, user_id):
    """Case 5 — Merge, add, remove, correct, or reextract issues."""
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

        new_issue_texts = await run_in_threadpool(
            reextract_missed_issues, full_text, current_issues
        )

        if new_issue_texts:
            next_id = max((i["id"] for i in current_issues), default=0) + 1
            for text in new_issue_texts:
                current_issues.append({
                    "id": next_id, "text": text,
                    "reply": None, "status": "pending",
                })
                next_id += 1
            lines = ["I found the following additional issues:\n"]
            for t in new_issue_texts:
                lines.append(f"- {t}")
            lines.append("\n\nUpdated issues list:\n")
            for i in current_issues:
                status_tag = " ✅" if i.get("reply") else ""
                lines.append(f"{i['id']}. {i['text']}{status_tag}")
            lines.append("\n\nShould I generate replies for the new issues?")
            response_text = "\n".join(lines)
        else:
            response_text = (
                "I re-read the document carefully but could not find any additional issues. "
                "Could you point me to the missing issue — quote the text or mention the paragraph?"
            )
    else:
        apply_issue_update(active_case, update)
        issues = active_case.get("issues", [])
        lines  = ["Issues list updated. Current issues:\n"]
        for i in issues:
            status_tag = " ✅" if i.get("reply") else ""
            lines.append(f"{i['id']}. {i['text']}{status_tag}")
        if get_pending_issues(active_case):
            lines.append("\n\nShould I generate replies for the updated issue(s)?")
        response_text = "\n".join(lines)

    chunk_size = 200
    for i in range(0, len(response_text), chunk_size):
        yield _content(response_text[i:i+chunk_size])
    asst = await add_message(session_id, "assistant", response_text, user_id)
    yield _retrieval_event(session_id, message_id=getattr(asst, "id", None))


async def _handle_update_reply(
    active_case, issue_id, session_id, user_id, background_tasks, ref_text
):
    """Case 6 — Regenerate reply for one specific issue."""
    from services.document.issue_replier import _process_single_issue

    all_issues = active_case.get("issues", [])
    target     = next((i for i in all_issues if i["id"] == issue_id), None)

    if not target:
        msg = f"Issue {issue_id} not found."
        yield _content(msg)
        await add_message(session_id, "assistant", msg, user_id)
        return

    mode        = active_case.get("mode", MODE_DEFENSIVE)
    recipient   = active_case.get("parties", {}).get("recipient")
    sender      = active_case.get("parties", {}).get("sender")
    doc_summary = active_case.get("summary", "")
    all_texts   = [i["text"] for i in all_issues]
    issue_num   = (all_texts.index(target["text"]) + 1) if target["text"] in all_texts else 1

    header = f"\n\n---\n\n### Updated Reply — Issue {issue_id}\n\n> {target['text']}\n\n"
    yield _content(header)

    _, reply, sources, usage = await run_in_threadpool(
        _process_single_issue,
        target["text"], issue_num, len(all_issues), all_texts,
        mode, recipient, sender, doc_summary, ref_text,
    )

    chunk_size = 50
    for i in range(0, len(reply), chunk_size):
        yield _content(reply[i:i+chunk_size])

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
    if _should_update_profile(target["text"]):
        background_tasks.add_task(auto_update_profile, user_id, target["text"], reply)


async def _handle_query_with_doc(
    active_case, question, session_id, user_id, history, background_tasks, db
):
    """Case 3 — Question about the document or mixed query."""
    from retrieval import SessionMessage
    from services.document.issue_replier import _get_pipeline

    doc_ctx = None
    if active_case:
        full_doc = await get_primary_texts(session_id, active_case["case_id"])
        doc_ctx  = full_doc[:8000] if full_doc.strip() else active_case.get("summary", "")

    pipeline = _get_pipeline()
    if pipeline is None:
        msg = "Pipeline not ready. Please try again."
        yield _content(msg)
        await add_message(session_id, "assistant", msg, user_id)
        return

    pipeline_history = []
    pending_q = None
    for msg in (history or []):
        role    = msg.get("role")    if isinstance(msg, dict) else getattr(msg, "role",    "")
        content = msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", "")
        if role == "user":
            pending_q = content
        elif role == "assistant" and pending_q:
            pipeline_history.append(SessionMessage(user_query=pending_q, llm_response=content))
            pending_q = None

    augmented = question
    if doc_ctx:
        augmented = f"[DOCUMENT CONTEXT]\n{doc_ctx[:4000]}\n\n[USER QUESTION]\n{question}"

    answer_parts = []
    try:
        staged = await run_in_threadpool(
            pipeline.query_stages_1_to_5,
            augmented,
            pipeline_history[-3:],
        )
        for chunk in pipeline.query_stage_6_stream(*staged):
            if chunk.startswith("\n\n__META__"):
                raw_meta = chunk[len("\n\n__META__"):]
                try:
                    meta = json.loads(raw_meta)
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
        logger.error(f"query_with_doc error: {e}", exc_info=True)
        msg = "An error occurred while generating the response."
        yield _content(msg)
        await add_message(session_id, "assistant", msg, user_id)