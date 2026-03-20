"""
pipeline/bm25_vectorizer.py
BM25 sparse vector computation + debug token file writer.
"""
from __future__ import annotations


import json
import math
import os
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

from apps.api.src.services.rag.config import CONFIG
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from apps.api.src.services.rag.pipeline.keyword_merger import MergeResult
import logging

logger = logging.getLogger(__name__)


class BM25Vectorizer:

    def __init__(self):
        self._vocab:     Dict[str, int] = {}
        self._vocab_rev: Dict[int, str] = {}
        self._doc_freq:  Counter        = Counter()
        self._corpus_docs: int          = 0
        self._k1 = CONFIG.bm25.k1
        self._b  = CONFIG.bm25.b

    # â”€â”€ Vocabulary â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _get_or_add(self, token: str) -> int:
        if token not in self._vocab:
            vid = len(self._vocab)
            self._vocab[token]   = vid
            self._vocab_rev[vid] = token
        return self._vocab[token]

    def update_corpus_stats(self, keyword_document: str):
        """Call for every chunk in Pass 1 to build IDF weights."""
        tokens = keyword_document.split()
        for token in set(tokens):
            self._get_or_add(token)
            self._doc_freq[token] += 1
        self._corpus_docs += 1

    def _idf(self, token: str) -> float:
        N  = max(self._corpus_docs, 1)
        df = self._doc_freq.get(token, 0)
        return math.log((N - df + 0.5) / (df + 0.5) + 1)

    def _avg_dl(self) -> float:
        total = sum(self._doc_freq.values())
        return max(total / max(self._corpus_docs, 1), 1)

    # â”€â”€ Sparse vector â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def compute_sparse_vector(
        self, keyword_document: str
    ) -> Tuple[List[int], List[float]]:
        if not keyword_document.strip():
            return [], []

        tokens     = keyword_document.split()
        tf_counter = Counter(tokens)
        doc_len    = len(tokens)
        avg_dl     = self._avg_dl()

        indices: List[int]   = []
        values:  List[float] = []
        seen_vids = set()

        for token, tf in tf_counter.items():
            vid = self._vocab.get(token)
            if vid is None:
                vid = self._get_or_add(token)
            if vid in seen_vids:
                continue
            seen_vids.add(vid)

            idf = self._idf(token)
            tf_norm = (tf * (self._k1 + 1)) / (
                tf + self._k1 * (1 - self._b + self._b * doc_len / avg_dl)
            )
            score = idf * tf_norm
            if score > 0:
                indices.append(vid)
                values.append(round(score, 6))

        return indices, values

    # â”€â”€ Debug file â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def write_debug_file(
        self,
        chunk_id:         str,
        merge_result:     MergeResult,
        keyword_document: str,
        indices:          List[int],
        values:           List[float],
        debug_dir:        str,
    ):
        Path(debug_dir).mkdir(parents=True, exist_ok=True)
        filepath = Path(debug_dir) / f"{chunk_id}_tokens.txt"

        tokens     = keyword_document.split()
        tf_counter = Counter(tokens)
        doc_len    = len(tokens)
        avg_dl     = self._avg_dl()

        score_map = {
            self._vocab_rev.get(idx, f"<id:{idx}>"): val
            for idx, val in zip(indices, values)
        }

        lines = []
        sep = "=" * 110

        lines += [
            sep,
            f"BM25 TOKEN DEBUG  â€”  Chunk: {chunk_id}",
            sep,
            f"Corpus docs: {self._corpus_docs}  |  Vocab size: {len(self._vocab)}  |  "
            f"Doc length: {doc_len}  |  Unique tokens in doc: {len(tf_counter)}",
            f"L1 tokens: {merge_result.l1_count}  |  "
            f"L3 tokens: {merge_result.l3_count}  |  "
            f"Discarded (grounding): {merge_result.discarded_count}",
            "",
            f"{'TOKEN':<45} {'LAYER':>5} {'WEIGHT':>6} "
            f"{'TF_NORM':>8} {'IDF':>8} {'BM25':>10} {'STATUS':>12}",
            "-" * 110,
        ]

        sorted_records = sorted(
            merge_result.token_records,
            key=lambda r: score_map.get(r.token, 0.0),
            reverse=True,
        )

        for rec in sorted_records:
            tf  = tf_counter.get(rec.token, 0)
            idf = round(self._idf(rec.token), 4)
            bm25_score = round(score_map.get(rec.token, 0.0), 6)
            tf_norm = round(
                (tf * (self._k1 + 1)) / (
                    tf + self._k1 * (1 - self._b + self._b * doc_len / max(avg_dl, 1))
                ), 4
            ) if tf > 0 else 0.0

            status = "OK" if rec.grounded else "DISCARDED"
            lines.append(
                f"{rec.token:<45} {'L'+str(rec.layer):>5} {rec.weight:>6} "
                f"{tf_norm:>8.4f} {idf:>8.4f} {bm25_score:>10.6f} {status:>12}"
            )

        # Discarded section
        discarded = [r for r in merge_result.token_records if not r.grounded]
        if discarded:
            lines += ["", "â”€â”€ DISCARDED (failed grounding check) â”€â”€"]
            for r in discarded:
                lines.append(f"  {r.token}  (L{r.layer})")

        # Hypothetical queries
        if merge_result.hypothetical_queries:
            lines += ["", "â”€â”€ HYPOTHETICAL QUERIES (appended as full phrases) â”€â”€"]
            for i, hq in enumerate(merge_result.hypothetical_queries, 1):
                lines.append(f"  {i}. {hq}")

        # Keyword document preview
        lines += [
            "",
            "â”€â”€ KEYWORD DOCUMENT (first 600 chars) â”€â”€",
            keyword_document[:600] + ("..." if len(keyword_document) > 600 else ""),
        ]

        # Sparse vector â€” top 20
        lines += ["", f"â”€â”€ SPARSE VECTOR â€” {len(indices)} non-zero dimensions (top 20) â”€â”€"]
        top20 = sorted(zip(indices, values), key=lambda x: x[1], reverse=True)[:20]
        for vid, val in top20:
            token = self._vocab_rev.get(vid, f"<id:{vid}>")
            lines.append(f"  vocab_id={vid:>6}  score={val:.6f}  token={token}")

        lines.append(sep)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        logger.debug(f"  Debug file â†’ {filepath}")

    # â”€â”€ Persistence â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def save_corpus_stats(self, path: str):
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "vocab":       self._vocab,
                "doc_freq":    dict(self._doc_freq),
                "corpus_docs": self._corpus_docs,
            }, f)
        logger.info(f"Corpus stats saved â†’ {path}  (vocab={len(self._vocab)}, docs={self._corpus_docs})")

    def load_corpus_stats(self, path: str):
        if not Path(path).exists():
            logger.info("No existing corpus stats â€” starting fresh.")
            return
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self._vocab       = data.get("vocab", {})
        self._vocab_rev   = {int(v): k for k, v in self._vocab.items()}
        self._doc_freq    = Counter(data.get("doc_freq", {}))
        self._corpus_docs = data.get("corpus_docs", 0)
        logger.info(f"Corpus stats loaded â€” vocab={len(self._vocab)} docs={self._corpus_docs}")
