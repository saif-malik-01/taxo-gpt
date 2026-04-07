"""
worker.py

STANDALONE BACKGROUND WORKER for decentralized document extraction.
Runs as a separate Docker task. Handles Poppler conversion and Bedrock OCR.
"""

import asyncio
import json
import logging
import os
import tempfile
import boto3
from datetime import datetime

from apps.api.src.db.session import AsyncSessionLocal, get_redis
from apps.api.src.core.config import settings
from apps.api.src.services.document.processor import extract_document_pages
from apps.api.src.services.document.pipeline import _run_step2, _apply_routing, _extract_all_issues
from apps.api.src.services.document.doc_context import get_doc_context, set_doc_context, get_active_case
from apps.api.src.services.document.doc_classifier import determine_route

# Configure Logging for the Worker
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] Worker: %(message)s")
logger = logging.getLogger(__name__)

# --- S3 Client ---
s3_client = boto3.client(
    "s3",
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    region_name=settings.AWS_REGION
)

async def _process_task(payload: dict):
    """
    Core extraction logic for a single document set.
    """
    user_id = payload["user_id"]
    session_id = payload["session_id"]
    documents = payload["documents"]
    
    logger.info(f"Processing session {session_id} for user {user_id}...")
    
    # ── Refresh Snapshot ──────────────────────────────────────────────────────────
    snapshot = await get_doc_context(session_id)
    if not snapshot:
        logger.error(f"Snapshot missing for session {session_id}. Aborting.")
        return

    extracted_docs = []
    
    # ── Extraction Loop ──────────────────────────────────────────────────────────
    for doc in documents:
        s3_key = doc["s3_key"]
        filename = doc["filename"]
        ext = doc.get("ext", ".pdf")
        
        # 1. Download from S3 to Temp
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            logger.info(f"Downloading {s3_key} to {tmp.name}")
            s3_client.download_fileobj(settings.AWS_S3_BUCKET, s3_key, tmp)
            tmp_path = tmp.name
        
        try:
            # 2. Extract Text (Poppler + Bedrock Nova Lite)
            # This is the CPU-heavy part
            full_text, page_count, error = await extract_document_pages(tmp_path, filename)
            
            if error:
                logger.error(f"Extraction error for {filename}: {error}")
                # We still append so the pipeline knows this file failed
                extracted_docs.append({
                    "filename": filename,
                    "full_text": f"Error: {error}",
                    "page_count": 0,
                    "s3_key": s3_key,
                    "error": error
                })
                continue

            extracted_docs.append({
                "filename": filename,
                "full_text": full_text,
                "page_count": page_count,
                "s3_key": s3_key
            })
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    # ── Step 2 & 3: Intelligence & Routing ─────────────────────────────────────
    # This runs within the worker to save API cycles
    res_q = "Please process these legal documents and identify case facts."
    doc_analyses, entity_cache = await _run_step2(extracted_docs, res_q, snapshot)
    snapshot.setdefault("legal_entities_cache", {}).update(entity_cache)
    
    active_case = get_active_case(snapshot)
    routing_plan = []
    for doc, analysis in zip(extracted_docs, doc_analyses):
        route = determine_route(analysis, active_case)
        routing_plan.append({"filename": doc["filename"], "analysis": analysis, "route": route})
    
    await _apply_routing(routing_plan, extracted_docs, snapshot, session_id)
    await _extract_all_issues(routing_plan, snapshot, session_id)
    
    # ── Finalize & Commit ────────────────────────────────────────────────────────
    # Updates BOTH Redis and Postgres Snapshot
    await set_doc_context(session_id, snapshot)
    logger.info(f"Successfully committed extraction for session {session_id}.")


async def worker_loop():
    """
    Main polling loop: Wait for tasks in Redis and process them one by one.
    """
    logger.info("Worker started. Listening on doc:queue:extraction...")
    redis = await get_redis()
    
    while True:
        try:
            # BRPOP: Blocking right pop. Waits until a task is available.
            # Timeout 0 means wait forever.
            _, data = await redis.brpop("doc:queue:extraction", timeout=0)
            payload = json.loads(data)
            
            # Run the process
            await _process_task(payload)
            
        except Exception as e:
            logger.error(f"Worker Loop Error: {e}", exc_info=True)
            await asyncio.sleep(2) # Cooldown on failure


if __name__ == "__main__":
    asyncio.run(worker_loop())
