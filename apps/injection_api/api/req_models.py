"""
ingestion_api/api/models.py

All Pydantic v2 request and response models.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# ── Auth ─────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    expires_in:   int  # seconds


# ── Chunk submission ──────────────────────────────────────────────────────────

class ChunkSubmitRequest(BaseModel):
    """
    The body POSTed to /chunks/submit.

    chunk_type must be a known type from CHUNK_TYPE_SPECS.
    data is the free-form chunk dict — the server validates required anchor
    fields and injects system fields before queuing.
    force_override bypasses the dedup warning (user clicked "submit anyway").
    """
    chunk_type:     str
    data:           Dict[str, Any]
    force_override: bool = False   # True = user acknowledged dedup/supersession warning

    @field_validator("chunk_type")
    @classmethod
    def chunk_type_must_be_known(cls, v: str) -> str:
        from schemas.chunk_type_spec import CHUNK_TYPE_SPECS
        if v not in CHUNK_TYPE_SPECS:
            raise ValueError(
                f"Unknown chunk_type '{v}'. "
                f"Valid types: {sorted(CHUNK_TYPE_SPECS.keys())}"
            )
        return v


class ChunkSubmitResponse(BaseModel):
    job_id:     str
    chunk_id:   str   # pre-generated UUID returned immediately
    chunk_type: str
    status:     str = "queued"
    message:    str = "Chunk queued for ingestion."


# ── Autofill ──────────────────────────────────────────────────────────────────

class AutofillRequest(BaseModel):
    """
    Body for POST /chunks/autofill.

    chunk_type tells the server which prompt template to use.
    anchor_data contains the fields the user has already filled in.
    The response returns suggested values for all autofill_fields.
    """
    chunk_type:  str
    anchor_data: Dict[str, Any]
    split:       bool = False   # True = break full text into multiple clauses/chunks

    @field_validator("chunk_type")
    @classmethod
    def chunk_type_must_be_known(cls, v: str) -> str:
        from schemas.chunk_type_spec import CHUNK_TYPE_SPECS
        if v not in CHUNK_TYPE_SPECS:
            raise ValueError(f"Unknown chunk_type '{v}'")
        return v


class AutofillField(BaseModel):
    path:       str        # dot-notation UKC path e.g. "ext.provision_type"
    value:      Any        # suggested value
    confidence: str = "llm_suggested"  # always "llm_suggested" for UI badge


class AutofillResponse(BaseModel):
    chunk_type:       str
    fields:           List[AutofillField] = [] # populated if split=False
    suggested_chunks: Optional[List[Dict[str, Any]]] = None # populated if split=True
    model_used:       str
    latency_ms:       int
    raw_llm_response: Optional[str] = None   # only in debug mode


# ── Job status ────────────────────────────────────────────────────────────────

class JobStatus(BaseModel):
    job_id:     str
    chunk_id:   str
    chunk_type: str
    status:     str     # queued | processing | success | failed
    progress:   int = 0 # 0–100
    submitted_by: str
    submitted_at: datetime
    completed_at: Optional[datetime] = None
    result:     Optional[Dict[str, Any]] = None   # on success: {chunk_id, supersession_log}
    error:      Optional[str]           = None    # on failure


# ── Dedup check response ──────────────────────────────────────────────────────

class DedupCheckResponse(BaseModel):
    """
    Returned before final submit if a duplicate is detected.
    Frontend shows this as a warning with a "submit anyway" confirm button.
    """
    duplicate_found:  bool
    existing_chunk_id: Optional[str]   = None
    existing_summary:  Optional[str]   = None
    warning_text:      Optional[str]   = None
    supersession_info: Optional[Dict]  = None   # populated for supersession types


# ── Schema endpoint ───────────────────────────────────────────────────────────

class FieldSpec(BaseModel):
    path:        str
    label:       str
    type:        str            # text | textarea | select | multi_select | tag_list | json_list | boolean
    required:    bool = False
    placeholder: Optional[str] = None
    hint:        Optional[str] = None
    options:     Optional[List[str]] = None
    default:     Optional[Any] = None
    tier:        str = "anchor"  # anchor | autofill | system


class ChunkTypeSchema(BaseModel):
    """
    Full schema for one chunk type, returned by GET /chunks/schema/{chunk_type}.
    Frontend uses this to render the form dynamically.
    """
    chunk_type:      str
    display_name:    str
    description:     str
    authority_level: int
    authority_label: str
    namespace:       str
    fields:          List[FieldSpec]    # anchor + autofill fields combined, in order
    dedup_key:       Optional[str]
    dedup_action:    str                # "warn" or "block"
    has_supersession: bool
    supersession_warning: Optional[str]


# ── History / audit ───────────────────────────────────────────────────────────

class AuditEntry(BaseModel):
    job_id:       str
    chunk_id:     str
    chunk_type:   str
    submitted_by: str
    submitted_at: datetime
    status:       str
    supersession_log: Optional[List[Dict]] = None