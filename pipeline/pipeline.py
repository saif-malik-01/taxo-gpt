"""
pipeline/pipeline.py
Two-pass indexing pipeline orchestrator.
Pass 1 — build keyword documents, update BM25 corpus stats (serial).
Pass 2 — generate vectors in parallel (ThreadPoolExecutor), then batch upsert to Qdrant.

Key changes vs original:
  - Workers call build_point() — pure CPU/network to Bedrock, zero Qdrant I/O
  - Main thread accumulates built points and flushes in batches of QDRANT_BATCH
  - TitanEmbeddingGenerator instantiated locally per worker — no shared state race
"""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from qdrant_client.http import models as qmodels

from config import CONFIG
from models.embedding_generator import TitanEmbeddingGenerator
from models.qdrant_manager import QdrantManager
from pipeline.bm25_vectorizer import BM25Vectorizer
from pipeline.file_tracker import FileState, FileTracker
from pipeline.keyword_merger import KeywordMerger, MergeResult
from pipeline.layer1_extractor import Layer1Extractor
from pipeline.layer3_qwen import Layer3Qwen
from utils.logger import get_logger

logger = get_logger("pipeline")

# Parallel workers for Bedrock embedding (one boto3 client per worker)
PASS2_WORKERS = 75

# Points accumulated before a single Qdrant upsert call
# 50 points × ~4KB payload ≈ 200KB per request — well within Qdrant limits
QDRANT_BATCH = 50


class IndexingPipeline:

    def __init__(self):
        self.tracker = FileTracker(CONFIG.paths.tracker_file)
        self.qdrant  = QdrantManager()
        self.l1      = Layer1Extractor()
        self.l3      = Layer3Qwen()
        self.merger  = KeywordMerger()
        self.bm25    = BM25Vectorizer()

        # Thread-safety for progress counter only
        self._progress_lock = threading.Lock()
        self._done_count    = 0
        self._total_count   = 0

    def setup(self):
        self.qdrant.ensure_collection()
        self.bm25.load_corpus_stats(CONFIG.paths.corpus_stats_file)
        logger.info("Pipeline ready.")

    def run(self):
        logger.info("=" * 70)
        logger.info("INDEXING PIPELINE STARTED")
        logger.info("=" * 70)

        files_to_process = self.tracker.scan(CONFIG.paths.chunks_dir)
        if not files_to_process:
            logger.info("Nothing to process — all files unchanged.")
            return

        # ── Pass 1: build keyword docs + BM25 corpus stats ───────────
        # Must be serial — BM25 IDF depends on full corpus seen in order
        logger.info(f"Pass 1: {len(files_to_process)} file(s) to process...")
        file_data: Dict[str, List[Tuple]] = {}

        for filepath, _ in files_to_process:
            chunks = self._load_chunks(filepath)
            if not chunks:
                continue
            file_data[filepath] = []
            for chunk in chunks:
                kw_doc, merge_result = self._build_keyword_doc(chunk)
                self.bm25.update_corpus_stats(kw_doc)
                file_data[filepath].append((chunk, kw_doc, merge_result))

        self.bm25.save_corpus_stats(CONFIG.paths.corpus_stats_file)
        logger.info(
            f"Pass 1 done — corpus: {self.bm25._corpus_docs} docs, "
            f"vocab: {len(self.bm25._vocab)} tokens"
        )

        # ── Pass 2: embed (parallel) → batch upsert (main thread) ────
        total_chunks = sum(len(v) for v in file_data.values())
        self._total_count = total_chunks
        self._done_count  = 0

        logger.info(
            f"Pass 2: {total_chunks} chunks across "
            f"{len(file_data)} file(s) — "
            f"{PASS2_WORKERS} workers, batch_size={QDRANT_BATCH}"
        )
        pass2_start = time.time()

        total_ok = total_fail = 0

        for filepath, state in files_to_process:
            if filepath not in file_data:
                continue

            fname     = Path(filepath).name
            file_hash = FileTracker.compute_hash(filepath)

            # Delete old points for modified files before re-indexing
            if state == FileState.MODIFIED:
                old_ids = self.tracker.get_old_chunk_ids(fname)
                if old_ids:
                    self.qdrant.delete_points(old_ids)

            chunks_data  = file_data[filepath]
            success_ids: List[str] = []
            any_failed   = False
            file_ok      = 0
            file_fail    = 0

            # Accumulate built points here — flushed to Qdrant in batches
            pending_points: List[qmodels.PointStruct] = []
            pending_ids:    List[str]                  = []

            with ThreadPoolExecutor(max_workers=PASS2_WORKERS) as executor:
                future_to_id = {}
                for chunk, kw_doc, merge_result in chunks_data:
                    chunk_id = chunk.get("id")
                    if not chunk_id:
                        logger.warning(f"  Chunk missing 'id' in {fname} — skipping")
                        continue
                    future = executor.submit(
                        self._process_chunk_safe,
                        chunk, kw_doc, merge_result, file_hash, fname
                    )
                    future_to_id[future] = chunk_id

                for future in as_completed(future_to_id):
                    chunk_id = future_to_id[future]
                    try:
                        point: Optional[qmodels.PointStruct] = future.result()
                    except Exception as e:
                        logger.error(f"  Worker exception for {chunk_id}: {e}")
                        point = None

                    if point is not None:
                        pending_points.append(point)
                        pending_ids.append(chunk_id)
                    else:
                        any_failed = True
                        file_fail  += 1
                        total_fail += 1
                        logger.error(f"  FAILED (embed): {chunk_id}")

                    # Flush batch when full
                    if len(pending_points) >= QDRANT_BATCH:
                        results = self.qdrant.upsert_batch(pending_points)
                        for cid, ok in zip(pending_ids, results):
                            if ok:
                                success_ids.append(cid)
                                file_ok   += 1
                                total_ok  += 1
                            else:
                                any_failed = True
                                file_fail  += 1
                                total_fail += 1
                                logger.error(f"  FAILED (upsert): {cid}")
                        pending_points.clear()
                        pending_ids.clear()

                    # Progress every 100 chunks
                    with self._progress_lock:
                        self._done_count += 1
                        if self._done_count % 100 == 0:
                            elapsed = time.time() - pass2_start
                            rate    = self._done_count / elapsed if elapsed > 0 else 0
                            remain  = (self._total_count - self._done_count) / rate if rate > 0 else 0
                            logger.info(
                                f"  Progress: {self._done_count}/{self._total_count} "
                                f"({rate:.1f}/s) — ETA: {remain/60:.0f}m"
                            )

            # Flush any remaining points after all futures complete
            if pending_points:
                results = self.qdrant.upsert_batch(pending_points)
                for cid, ok in zip(pending_ids, results):
                    if ok:
                        success_ids.append(cid)
                        file_ok   += 1
                        total_ok  += 1
                    else:
                        any_failed = True
                        file_fail  += 1
                        total_fail += 1
                        logger.error(f"  FAILED (upsert): {cid}")

            logger.info(f"  [{fname}] done — ok:{file_ok} failed:{file_fail}")

            if not any_failed:
                self.tracker.mark_success(fname, file_hash, success_ids)
            else:
                self.tracker.mark_failed(
                    fname, file_hash, "one or more chunks failed"
                )

        self.tracker.save()

        elapsed_total = time.time() - pass2_start
        logger.info("=" * 70)
        logger.info(
            f"DONE — success: {total_ok}  failed: {total_fail}  "
            f"time: {elapsed_total/60:.1f}m"
        )
        logger.info("=" * 70)

    # ── Helpers ──────────────────────────────────────────────────────

    def _load_chunks(self, filepath: str) -> List[Dict[str, Any]]:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
            logger.error(f"Unexpected JSON structure in {filepath}")
            return []
        except Exception as e:
            logger.error(f"Failed to load {filepath}: {e}")
            return []

    def _build_keyword_doc(
        self, chunk: Dict[str, Any]
    ) -> Tuple[str, MergeResult]:
        text    = chunk.get("text", "")
        summary = chunk.get("summary", "")

        l1_tokens = self.l1.extract(chunk)
        l3_data   = self.l3.extract(text) if text else {}

        merge_result = self.merger.merge(
            l1_tokens     = l1_tokens,
            l3_data       = l3_data,
            chunk_text    = text,
            chunk_summary = summary,
        )
        return merge_result.keyword_document, merge_result

    def _process_chunk_safe(
        self,
        chunk:        Dict[str, Any],
        kw_doc:       str,
        merge_result: MergeResult,
        file_hash:    str,
        fname:        str,
    ) -> Optional[qmodels.PointStruct]:
        """
        Thread-safe worker: creates a local TitanEmbeddingGenerator (own boto3
        client) — no shared state. Returns a built PointStruct on success,
        None on failure. No Qdrant I/O happens here.
        """
        chunk_id = chunk.get("id", "unknown")
        try:
            local_embedder = TitanEmbeddingGenerator()
            return self._process_chunk(chunk, kw_doc, merge_result, file_hash, local_embedder)
        except Exception as e:
            logger.error(f"  Exception in {fname}/{chunk_id}: {e}")
            return None

    def _process_chunk(
        self,
        chunk:         Dict[str, Any],
        kw_doc:        str,
        merge_result:  MergeResult,
        file_hash:     str,
        embedder:      TitanEmbeddingGenerator,
    ) -> Optional[qmodels.PointStruct]:
        chunk_id = chunk.get("id", "unknown")
        text     = chunk.get("text", "")
        summary  = chunk.get("summary", "")

        # Embed text and summary in parallel (two concurrent Bedrock calls)
        text_vector, summary_vector = embedder.embed_both(text, summary)
        if text_vector is None:
            logger.error(f"    text_vector failed for {chunk_id}")
            return None
        if summary_vector is None:
            summary_vector = text_vector

        # BM25 sparse vector — read-only after Pass 1, thread-safe
        sparse_indices, sparse_values = self.bm25.compute_sparse_vector(kw_doc)

        if CONFIG.pipeline.write_debug_tokens:
            self.bm25.write_debug_file(
                chunk_id         = chunk_id,
                merge_result     = merge_result,
                keyword_document = kw_doc,
                indices          = sparse_indices,
                values           = sparse_values,
                debug_dir        = CONFIG.paths.debug_tokens_dir,
            )

        # Build and return PointStruct — no network I/O to Qdrant
        return self.qdrant.build_point(
            chunk_id       = chunk_id,
            text_vector    = text_vector,
            summary_vector = summary_vector,
            sparse_indices = sparse_indices,
            sparse_values  = sparse_values,
            payload        = dict(chunk),
            file_hash      = file_hash,
        )