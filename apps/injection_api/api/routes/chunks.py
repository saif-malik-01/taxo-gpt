"""
ingestion_api/api/routes/chunks.py

POST /chunks/submit          — validate, dedup-check, queue ingestion
POST /chunks/autofill        — LLM autofill for empty fields
GET  /chunks/schema/{type}   — return field spec for a chunk type
GET  /chunks/dedup/{type}    — check if a value already exists (called as user types)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from api.deps import get_current_user
from api.req_models import (
    AutofillRequest,
    AutofillResponse,
    ChunkSubmitRequest,
    ChunkSubmitResponse,
    ChunkTypeSchema,
    DedupCheckResponse,
    FieldSpec,
)
from autofill.bedrock_caller import AutofillBedrockCaller
from schemas.chunk_type_spec import (
    AUTHORITY_LEVELS,
    CHUNK_TYPE_SPECS,
    get_spec,
    inject_system_fields,
    get_nested,
)
from worker.tasks import ingest_chunk_task

router = APIRouter(prefix="/chunks", tags=["chunks"])


# ── Schema endpoint ───────────────────────────────────────────────────────────

@router.get(
    "/schema/{chunk_type}",
    response_model=ChunkTypeSchema,
    summary="Get form field spec for a chunk type",
)
def get_schema(
    chunk_type: str,
    current_user: str = Depends(get_current_user),
) -> ChunkTypeSchema:
    """
    Returns the complete field specification for a given chunk_type.
    The frontend uses this to render the form dynamically — no hardcoded
    field lists in the UI.
    """
    try:
        spec = get_spec(chunk_type)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    al = AUTHORITY_LEVELS[spec["authority_level"]]

    # Build unified field list: anchors first, then autofill fields
    fields: list[FieldSpec] = []

    for f in spec["anchor_fields"]:
        fields.append(FieldSpec(
            path=f["path"],
            label=f["label"],
            type=f["type"],
            required=f.get("required", False),
            placeholder=f.get("placeholder"),
            hint=f.get("hint"),
            options=f.get("options"),
            default=f.get("default"),
            tier="anchor",
        ))

    for f in spec["autofill_fields"]:
        fields.append(FieldSpec(
            path=f["path"],
            label=f["label"],
            type=f["type"],
            required=False,   # autofill fields are never required from user
            placeholder=f.get("placeholder"),
            hint=f.get("hint"),
            options=f.get("options"),
            default=f.get("default"),
            tier="autofill",
        ))

    sup = spec["supersession_check"]
    return ChunkTypeSchema(
        chunk_type=chunk_type,
        display_name=spec["ui_display_name"],
        description=spec["ui_description"],
        authority_level=spec["authority_level"],
        authority_label=al["label"],
        namespace=spec["namespace"],
        fields=fields,
        dedup_key=spec.get("dedup_key"),
        dedup_action=spec.get("dedup_action", "warn"),
        has_supersession=sup.get("enabled", False),
        supersession_warning=sup.get("warning_text"),
    )


# ── Dedup check — called while user types, not just on submit ─────────────────

@router.get(
    "/dedup/{chunk_type}",
    response_model=DedupCheckResponse,
    summary="Check if a chunk already exists (live dedup check)",
)
def dedup_check(
    chunk_type: str,
    key_value: str = Query(..., description="Value to check for dedup"),
    current_user: str = Depends(get_current_user),
) -> DedupCheckResponse:
    """
    Called while the user is typing in the anchor key field.
    For cgst_section this is the section_number field.
    Returns a warning if a chunk with that key already exists in Qdrant.
    """
    try:
        spec = get_spec(chunk_type)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    dedup_key = spec.get("dedup_key")
    if not dedup_key:
        return DedupCheckResponse(duplicate_found=False)

    # Import here to avoid circular at module load
    from core_models.qdrant_manager import QdrantManager
    qdrant = QdrantManager()

    existing = qdrant.search_by_payload(
        filters={
            "must": [
                {
                    "key":   dedup_key,
                    "match": {"value": key_value},
                }
            ]
        },
        limit=1,
    )

    if not existing:
        return DedupCheckResponse(duplicate_found=False)

    existing_chunk = existing[0]
    sup = spec["supersession_check"]

    return DedupCheckResponse(
        duplicate_found=True,
        existing_chunk_id=existing_chunk.get("id"),
        existing_summary=existing_chunk.get("summary", ""),
        warning_text=sup.get("warning_text", "").format(
            section_number=key_value,
        ) if sup.get("enabled") else (
            f"A chunk with {dedup_key}='{key_value}' already exists. "
            "Submitting will create a duplicate."
        ),
        supersession_info={
            "action":        sup.get("action"),
            "existing_id":   existing_chunk.get("id"),
            "existing_title": existing_chunk.get("ext", {}).get("section_title", ""),
        } if sup.get("enabled") else None,
    )


# ── Autofill ──────────────────────────────────────────────────────────────────

@router.post(
    "/autofill",
    response_model=AutofillResponse,
    summary="LLM autofill for empty fields",
)
async def autofill(
    body: AutofillRequest,
    current_user: str = Depends(get_current_user),
) -> AutofillResponse:
    """
    Given the anchor fields filled by the user, returns LLM-suggested values
    for all autofill fields.

    The frontend:
      1. Shows an "Autofill remaining fields" button once anchor fields are filled
      2. Calls this endpoint
      3. Pre-fills the autofill form fields with the response
      4. Marks each pre-filled field with an amber "AI suggested" badge
      5. User reviews, edits if needed, then submits
    """
    try:
        get_spec(body.chunk_type)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    caller = AutofillBedrockCaller()
    response = await caller.autofill(
        chunk_type=body.chunk_type,
        anchor_data=body.anchor_data,
        split=body.split,
    )
    return response


# ── Submit ────────────────────────────────────────────────────────────────────

@router.post(
    "/submit",
    response_model=ChunkSubmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a chunk for ingestion",
)
def submit_chunk(
    body: ChunkSubmitRequest,
    current_user: str = Depends(get_current_user),
) -> ChunkSubmitResponse:
    """
    Validate the chunk, run a dedup check, then queue it for async ingestion.

    Returns immediately with a job_id. Poll GET /jobs/{job_id} for progress.

    If a duplicate exists and force_override=False, returns 409 with
    dedup info so the frontend can show a confirmation dialog.
    Use force_override=True on the second submit to proceed anyway.
    """
    try:
        spec = get_spec(body.chunk_type)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    chunk = body.data

    # ── 1. Validate anchor fields are present ────────────────────────────────
    missing = _validate_anchor_fields(chunk, spec)
    if missing:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": "Required anchor fields are missing.",
                "missing_fields": missing,
            },
        )

    # ── 2. Dedup check (skip if force_override) ──────────────────────────────
    if not body.force_override:
        dedup_result = _run_dedup_check(chunk, spec)
        if dedup_result and spec.get("dedup_action") == "block":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": "Duplicate chunk detected.",
                    "dedup": dedup_result,
                },
            )
        if dedup_result and spec.get("dedup_action") == "warn":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message":       "Duplicate detected. Submit with force_override=true to proceed.",
                    "dedup":         dedup_result,
                    "force_override_hint": True,
                },
            )

    # ── 3. Assign UUID if not provided ───────────────────────────────────────
    if not chunk.get("id"):
        chunk["id"] = str(uuid.uuid4())
    chunk_id = chunk["id"]

    # ── 4. Inject system fields ───────────────────────────────────────────────
    inject_system_fields(chunk, body.chunk_type)

    # ── 5. Add provenance ─────────────────────────────────────────────────────
    prov = chunk.setdefault("provenance", {})
    prov.setdefault("ingestion_date", datetime.now(timezone.utc).date().isoformat())
    prov.setdefault("submitted_by",   current_user)

    # ── 6. Queue Celery task ──────────────────────────────────────────────────
    task = ingest_chunk_task.apply_async(
        kwargs={
            "chunk":        chunk,
            "chunk_type":   body.chunk_type,
            "submitted_by": current_user,
            "submitted_at": datetime.now(timezone.utc).isoformat(),
        }
    )

    return ChunkSubmitResponse(
        job_id=task.id,
        chunk_id=chunk_id,
        chunk_type=body.chunk_type,
        status="queued",
        message=f"Chunk '{chunk_id}' queued for ingestion. Poll /jobs/{task.id} for status.",
    )


# ── Private helpers ───────────────────────────────────────────────────────────

def _validate_anchor_fields(chunk: dict, spec: dict) -> list[str]:
    """Returns list of missing required anchor field paths."""
    missing = []
    for field_def in spec["anchor_fields"]:
        if not field_def.get("required", False):
            continue
        path = field_def["path"]
        value = get_nested(chunk, path)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(path)
    return missing


def _run_dedup_check(chunk: dict, spec: dict) -> dict | None:
    """
    Check Qdrant for an existing chunk with the same dedup_key value.
    Returns the existing chunk payload if found, None otherwise.
    """
    dedup_key = spec.get("dedup_key")
    if not dedup_key:
        return None

    key_value = get_nested(chunk, dedup_key)
    if not key_value:
        return None

    from core_models.qdrant_manager import QdrantManager
    qdrant = QdrantManager()
    existing = qdrant.search_by_payload(
        filters={
            "must": [{"key": dedup_key, "match": {"value": str(key_value)}}]
        },
        limit=1,
    )
    return existing[0] if existing else None