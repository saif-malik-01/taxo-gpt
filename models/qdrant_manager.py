"""
models/qdrant_manager.py
Qdrant collection management and upsert.
Uses a fixed pool of reusable clients — eliminates per-chunk TCP handshake overhead.
Batch upsert reduces total Qdrant round-trips from ~195k to ~4k.
"""

import queue
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
from qdrant_client.http.exceptions import UnexpectedResponse

from config import CONFIG
from utils.logger import get_logger

logger = get_logger("qdrant_manager")

PAYLOAD_INDEXES = [
    ("chunk_type",                   "keyword"),
    ("retrieval.tax_type",           "keyword"),
    ("ext.issued_by",                "keyword"),
    ("temporal.is_current",          "bool"),
    ("legal_status.current_status",  "keyword"),
    ("temporal.financial_year",      "keyword"),
    ("ext.court_level",              "keyword"),
    ("ext.decision",                 "keyword"),
]

# Number of persistent Qdrant connections in the pool
_CLIENT_POOL_SIZE = 10


def _make_client() -> QdrantClient:
    """Create a fresh QdrantClient."""
    cfg = CONFIG.qdrant
    return QdrantClient(
        host    = cfg.host,
        port    = cfg.port,
        https   = cfg.https,
        api_key = cfg.api_key,
        timeout = cfg.timeout
    )


class QdrantManager:
    """
    Collection setup uses a shared single client (main thread only).
    Upserts use a fixed pool of reusable clients — no per-chunk TCP handshake.
    Workers call build_point() to construct PointStructs without any network I/O.
    The pipeline flushes accumulated points via upsert_batch().
    """

    def __init__(self):
        self._client: Optional[QdrantClient] = None
        # Pre-create pool of reusable clients
        self._pool: queue.Queue = queue.Queue()
        for _ in range(_CLIENT_POOL_SIZE):
            self._pool.put(_make_client())

    def _get_client(self) -> QdrantClient:
        if self._client is None:
            self._client = _make_client()
            logger.info(
                f"Qdrant connected → "
                f"{CONFIG.qdrant.host}:{CONFIG.qdrant.port}"
            )
        return self._client

    def _acquire(self) -> QdrantClient:
        """Borrow a client from the pool (blocks if all in use)."""
        return self._pool.get()

    def _release(self, client: QdrantClient):
        """Return a client to the pool."""
        self._pool.put(client)

    # ── Collection setup (main thread only) ──────────────────────────

    def ensure_collection(self):
        client = self._get_client()
        cfg    = CONFIG.qdrant

        existing = [c.name for c in client.get_collections().collections]
        if cfg.collection_name in existing:
            logger.info(f"Collection '{cfg.collection_name}' already exists.")
            return

        logger.info(f"Creating collection '{cfg.collection_name}'...")
        client.create_collection(
            collection_name = cfg.collection_name,
            vectors_config  = {
                cfg.text_vector_name: qmodels.VectorParams(
                    size=cfg.vector_size, distance=qmodels.Distance.COSINE
                ),
                cfg.summary_vector_name: qmodels.VectorParams(
                    size=cfg.vector_size, distance=qmodels.Distance.COSINE
                ),
            },
            sparse_vectors_config = {
                cfg.sparse_vector_name: qmodels.SparseVectorParams(
                    index=qmodels.SparseIndexParams(on_disk=False)
                )
            },
        )
        logger.info(f"Collection '{cfg.collection_name}' created.")
        self._create_payload_indexes()

    def _create_payload_indexes(self):
        client = self._get_client()
        for field_name, field_type in PAYLOAD_INDEXES:
            try:
                schema = (
                    qmodels.PayloadSchemaType.KEYWORD
                    if field_type == "keyword"
                    else qmodels.PayloadSchemaType.BOOL
                )
                client.create_payload_index(
                    collection_name = CONFIG.qdrant.collection_name,
                    field_name      = field_name,
                    field_schema    = schema,
                )
            except Exception as e:
                logger.warning(f"  Payload index {field_name} skipped: {e}")

    def delete_points(self, chunk_ids: List[str]):
        if not chunk_ids:
            return
        try:
            point_uuids = [self._to_uuid(cid) for cid in chunk_ids]
            self._get_client().delete(
                collection_name = CONFIG.qdrant.collection_name,
                points_selector = qmodels.PointIdsList(points=point_uuids),
            )
            logger.info(f"  Deleted {len(chunk_ids)} old point(s).")
        except Exception as e:
            logger.error(f"  Delete failed: {e}")

    # ── Point construction (called from worker threads — no network I/O) ──

    @staticmethod
    def _to_uuid(chunk_id: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, chunk_id))

    def build_point(
        self,
        chunk_id:       str,
        text_vector:    List[float],
        summary_vector: List[float],
        sparse_indices: List[int],
        sparse_values:  List[float],
        payload:        Dict[str, Any],
        file_hash:      str,
    ) -> qmodels.PointStruct:
        """
        Build a PointStruct in memory — no Qdrant connection needed.
        Called from worker threads. Network I/O happens later in upsert_batch().
        """
        cfg = CONFIG.qdrant
        payload = dict(payload)
        payload["_chunk_id"]   = chunk_id
        payload["_file_hash"]  = file_hash
        payload["_indexed_at"] = datetime.now(timezone.utc).isoformat()

        return qmodels.PointStruct(
            id     = self._to_uuid(chunk_id),
            vector = {
                cfg.text_vector_name:    text_vector,
                cfg.summary_vector_name: summary_vector,
                cfg.sparse_vector_name:  qmodels.SparseVector(
                    indices=sparse_indices,
                    values=sparse_values,
                ),
            },
            payload = payload,
        )

    # ── Batch upsert (called from main thread) ────────────────────────

    def upsert_batch(
        self, points: List[qmodels.PointStruct]
    ) -> List[bool]:
        """
        Upsert a batch of pre-built PointStructs using a pooled client.
        Returns a list of bools (True = success) aligned to input points.
        A single Qdrant call handles the entire batch — ~50x fewer round-trips
        compared to per-chunk upserts.
        """
        if not points:
            return []

        client = self._acquire()
        try:
            for attempt in range(1, CONFIG.pipeline.max_retries + 1):
                try:
                    client.upsert(
                        collection_name = CONFIG.qdrant.collection_name,
                        points          = points,
                        wait            = True,
                    )
                    return [True] * len(points)

                except UnexpectedResponse as e:
                    logger.error(
                        f"  Qdrant batch upsert attempt {attempt} "
                        f"({len(points)} points): {e}"
                    )
                    if attempt == CONFIG.pipeline.max_retries:
                        return [False] * len(points)
                    time.sleep(CONFIG.pipeline.retry_delay_seconds * attempt)

                except Exception as e:
                    logger.error(f"  Qdrant unexpected error attempt {attempt}: {e}")
                    if attempt == CONFIG.pipeline.max_retries:
                        return [False] * len(points)
                    time.sleep(CONFIG.pipeline.retry_delay_seconds * attempt)

        finally:
            self._release(client)

        return [False] * len(points)