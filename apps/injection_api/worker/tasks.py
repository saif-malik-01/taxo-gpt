"""
ingestion_api/worker/tasks.py

Celery task: ingest_chunk_task

Wires directly into your existing pipeline components:
  - BM25Vectorizer      (bm25_vectorizer.py)
  - Layer1Extractor     (layer1_extractor.py)
  - Layer3Qwen          (layer3_qwen.py)
  - KeywordMerger       (keyword_merger.py)
  - TitanEmbeddingGenerator (core_models/embedding_generator.py)
  - QdrantManager       (core_models/qdrant_manager.py)

No changes to any of those files — they are imported and used exactly
as IndexingPipeline uses them.

BM25 corpus stats update note:
  For single-chunk submissions, we load → compute → update → save corpus stats.
  The save is done under a filelock to avoid race conditions when multiple
  workers run simultaneously.
"""

from __future__ import annotations

import os
import sys

# Ensure the root directory is in sys.path for Celery worker
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

import portalocker
import uuid
from datetime import datetime, timezone
from typing import Any

from celery import Task

from worker.celery_app import celery_app
from schemas.chunk_type_spec import get_spec
from worker.supersession import SupersessionEngine
from flow.config import CONFIG
from utils.logger import get_logger

# Import these at top level to verify availability at load-time
from core_models.embedding_generator import TitanEmbeddingGenerator
from core_models.qdrant_manager import QdrantManager
from flow.layer1_extractor import Layer1Extractor
from flow.layer3_qwen import Layer3Qwen
from flow.keyword_merger import KeywordMerger
from flow.bm25_vectorizer import BM25Vectorizer

logger = get_logger("ingest_task")
logger.info(f"Worker sys.path: {sys.path}")

# Lock file to serialize corpus_stats.json writes across workers
_CORPUS_LOCK_PATH = CONFIG.paths.corpus_stats_file + ".lock"


class IngestTask(Task):
    """
    Custom Task base that holds expensive singleton objects across task calls
    within a single worker process (not across workers).

    Celery reuses Task instances within a process — this avoids re-initialising
    QdrantManager / BM25Vectorizer on every task call.
    """
    _qdrant  = None
    _bm25    = None

    @property
    def qdrant(self):
        if self._qdrant is None:
            self._qdrant = QdrantManager()
        return self._qdrant

    @property
    def bm25_base(self):
        """
        Returns a fresh BM25Vectorizer loaded from corpus stats.
        We load fresh each task because another worker may have updated
        the stats file since this worker started.
        """
        bm25 = BM25Vectorizer()
        bm25.load_corpus_stats(CONFIG.paths.corpus_stats_file)
        return bm25


@celery_app.task(
    bind=True,
    base=IngestTask,
    name="ingestion.ingest_chunk",
    max_retries=3,
    default_retry_delay=5,
)
def ingest_chunk_task(
    self,
    chunk:        dict[str, Any],
    chunk_type:   str,
    submitted_by: str,
    submitted_at: str,    # ISO string from API
) -> dict[str, Any]:
    """
    Full ingestion pipeline for a single chunk:
      1.  Load BM25 corpus stats
      2.  Extract L1 tokens (metadata fields)
      3.  Extract L3 tokens (regex fallback on text)
      4.  Merge → keyword document
      5.  Compute BM25 sparse vector
      6.  Update + save corpus stats (under file lock)
      7.  Embed text + summary via Titan (AWS Bedrock)
      8.  Run supersession engine
      9.  Build PointStruct
      10. Upsert to Qdrant
    """
    chunk_id = chunk.get("id") or str(uuid.uuid4())
    chunk["id"] = chunk_id

    logger.info(f"[{chunk_id}] Starting ingestion — type={chunk_type} by={submitted_by}")

    # Store task metadata so /jobs/{id} can display it
    self.update_state(
        state="STARTED",
        meta={
            "chunk_id":     chunk_id,
            "chunk_type":   chunk_type,
            "submitted_by": submitted_by,
            "submitted_at": submitted_at,
            "progress":     5,
        },
    )

    try:
        spec = get_spec(chunk_type)

        # ── Step 1-4: Build keyword document ─────────────────────────────────
        self.update_state(state="STARTED", meta={
            "chunk_id": chunk_id, "chunk_type": chunk_type,
            "submitted_by": submitted_by, "submitted_at": submitted_at,
            "progress": 15,
        })

        # These are now imported at top-level for robustness

        l1       = Layer1Extractor()
        l3       = Layer3Qwen()
        merger   = KeywordMerger()

        l1_tokens    = l1.extract(chunk)
        l3_data      = l3.extract(chunk.get("text", ""))
        merge_result = merger.merge(
            l1_tokens     = l1_tokens,
            l3_data       = l3_data,
            chunk_text    = chunk.get("text", ""),
            chunk_summary = chunk.get("summary", ""),
        )
        kw_doc = merge_result.keyword_document

        logger.debug(
            f"[{chunk_id}] Keyword doc: L1={merge_result.l1_count} "
            f"L3={merge_result.l3_count} discarded={merge_result.discarded_count}"
        )

        # ── Step 5: Compute sparse vector ─────────────────────────────────────
        self.update_state(state="STARTED", meta={
            "chunk_id": chunk_id, "chunk_type": chunk_type,
            "submitted_by": submitted_by, "submitted_at": submitted_at,
            "progress": 30,
        })

        bm25 = self.bm25_base   # fresh load from corpus_stats.json
        sparse_indices, sparse_values = bm25.compute_sparse_vector(kw_doc)

        # ── Step 6: Update corpus stats (serialised write) ────────────────────
        _update_corpus_stats_safe(kw_doc)

        # ── Step 7: Embed ─────────────────────────────────────────────────────
        self.update_state(state="STARTED", meta={
            "chunk_id": chunk_id, "chunk_type": chunk_type,
            "submitted_by": submitted_by, "submitted_at": submitted_at,
            "progress": 50,
        })

        # TitanEmbeddingGenerator and QdrantManager are now top-level
        embedder = TitanEmbeddingGenerator()
        text_vec, summary_vec = embedder.embed_both(
            chunk.get("text", ""),
            chunk.get("summary", ""),
        )

        if text_vec is None:
            raise ValueError(f"Titan text embedding returned None for chunk {chunk_id}")
        if summary_vec is None:
            logger.warning(f"[{chunk_id}] Summary vector failed — falling back to text vector")
            summary_vec = text_vec

        logger.debug(f"[{chunk_id}] Embedded — text_vec dim={len(text_vec)}")

        # ── Step 8: Supersession engine ───────────────────────────────────────
        self.update_state(state="STARTED", meta={
            "chunk_id": chunk_id, "chunk_type": chunk_type,
            "submitted_by": submitted_by, "submitted_at": submitted_at,
            "progress": 70,
        })

        engine = SupersessionEngine(self.qdrant)
        supersession_log = engine.check_and_apply(chunk, spec)

        if supersession_log:
            logger.info(
                f"[{chunk_id}] Supersession applied — "
                f"{len(supersession_log)} chunk(s) updated"
            )

        # ── Step 9-10: Build point and upsert ─────────────────────────────────
        self.update_state(state="STARTED", meta={
            "chunk_id": chunk_id, "chunk_type": chunk_type,
            "submitted_by": submitted_by, "submitted_at": submitted_at,
            "progress": 85,
        })

        file_hash = f"manual:{submitted_by}:{submitted_at}"
        point = self.qdrant.build_point(
            chunk_id       = chunk_id,
            text_vector    = text_vec,
            summary_vector = summary_vec,
            sparse_indices = sparse_indices,
            sparse_values  = sparse_values,
            payload        = chunk,
            file_hash      = file_hash,
        )

        results = self.qdrant.upsert_batch([point])
        if not any(results):
            raise RuntimeError(f"Qdrant upsert returned failure for chunk {chunk_id}")

        logger.info(f"[{chunk_id}] Ingestion complete.")

        completed_at = datetime.now(timezone.utc).isoformat()
        return {
            "chunk_id":         chunk_id,
            "chunk_type":       chunk_type,
            "submitted_by":     submitted_by,
            "submitted_at":     submitted_at,
            "completed_at":     completed_at,
            "progress":         100,
            "result": {
                "chunk_id":        chunk_id,
                "supersession_log": supersession_log,
                "sparse_dims":     len(sparse_indices),
            },
        }

    except Exception as exc:
        logger.error(f"[{chunk_id}] Ingestion failed: {exc}", exc_info=True)
        try:
            raise self.retry(exc=exc, countdown=5)
        except self.MaxRetriesExceededError:
            # All retries exhausted — let Celery mark task as FAILURE
            raise


# ── Corpus stats file-locking helper ─────────────────────────────────────────

def _update_corpus_stats_safe(kw_doc: str) -> None:
    """
    Update corpus_stats.json under an exclusive file lock.
    Prevents race conditions when multiple Celery workers run simultaneously.
    """
    # BM25Vectorizer is now top-level

    lock_path = _CORPUS_LOCK_PATH
    stats_path = CONFIG.paths.corpus_stats_file

    with open(lock_path, "w") as lock_f:
        try:
            portalocker.lock(lock_f, portalocker.LOCK_EX)

            # Re-load inside the lock (another worker may have updated since we read)
            bm25 = BM25Vectorizer()
            bm25.load_corpus_stats(stats_path)

            bm25.update_corpus_stats(kw_doc)
            bm25.save_corpus_stats(stats_path)

        finally:
            # portalocker.unlock(lock_f) happens automatically on close in many systems
            # but we explicitly unlock for clarity if using portalocker.lock()
            portalocker.unlock(lock_f)