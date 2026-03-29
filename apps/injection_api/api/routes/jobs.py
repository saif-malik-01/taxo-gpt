"""
ingestion_api/api/routes/jobs.py

GET /jobs/{job_id}  →  poll Celery task status
"""

from __future__ import annotations

from datetime import datetime, timezone

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, status

from api.deps import get_current_user
from api.req_models import JobStatus
from worker.celery_app import celery_app

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get(
    "/{job_id}",
    response_model=JobStatus,
    summary="Poll the status of an ingestion job",
)
def get_job_status(
    job_id: str,
    current_user: str = Depends(get_current_user),
) -> JobStatus:
    """
    Poll job status by job_id (Celery task ID returned from /chunks/submit).

    Status values:
      queued      — task accepted, not yet picked up by a worker
      processing  — worker is actively processing
      success     — chunk indexed in Qdrant
      failed      — all retries exhausted, see `error` field

    Frontend should poll every 2 seconds until status is success or failed.
    """
    result = AsyncResult(job_id, app=celery_app)
    celery_state = result.state   # PENDING | STARTED | SUCCESS | FAILURE | RETRY

    # Map Celery states to our API states
    state_map = {
        "PENDING": "queued",
        "STARTED": "processing",
        "RETRY":   "processing",
        "SUCCESS": "success",
        "FAILURE": "failed",
    }
    api_status = state_map.get(celery_state, "queued")

    # Celery stores task meta in result.info
    info = result.info or {}

    # On FAILURE, result.info is the exception
    error_msg = None
    if celery_state == "FAILURE":
        error_msg = str(result.info) if result.info else "Unknown error"

    # Progress is stored in task meta via update_state()
    progress = 100 if api_status == "success" else info.get("progress", 0)

    # chunk_id and submitted_by are stored in task meta
    chunk_id      = info.get("chunk_id", "") if isinstance(info, dict) else ""
    chunk_type    = info.get("chunk_type", "") if isinstance(info, dict) else ""
    submitted_by  = info.get("submitted_by", "unknown") if isinstance(info, dict) else "unknown"
    submitted_at  = info.get("submitted_at") if isinstance(info, dict) else None
    completed_at  = info.get("completed_at") if isinstance(info, dict) else None
    task_result   = info.get("result") if isinstance(info, dict) and api_status == "success" else None

    # Parse datetimes if stored as ISO strings
    if isinstance(submitted_at, str):
        try:
            submitted_at = datetime.fromisoformat(submitted_at)
        except ValueError:
            submitted_at = datetime.now(timezone.utc)
    if submitted_at is None:
        submitted_at = datetime.now(timezone.utc)

    if isinstance(completed_at, str):
        try:
            completed_at = datetime.fromisoformat(completed_at)
        except ValueError:
            completed_at = None

    return JobStatus(
        job_id=job_id,
        chunk_id=chunk_id,
        chunk_type=chunk_type,
        status=api_status,
        progress=progress,
        submitted_by=submitted_by,
        submitted_at=submitted_at,
        completed_at=completed_at,
        result=task_result,
        error=error_msg,
    )