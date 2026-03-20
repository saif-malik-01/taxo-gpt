"""
pipeline/file_tracker.py
SHA256-based incremental file tracker.
"""

import hashlib
import json
import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Tuple

from utils.logger import get_logger

logger = get_logger("file_tracker")


class FileState(Enum):
    NEW       = "new"
    MODIFIED  = "modified"
    UNCHANGED = "unchanged"


class FileTracker:
    def __init__(self, tracker_path: str):
        self.tracker_path = Path(tracker_path)
        self._data: Dict[str, dict] = {}
        self._load()

    def _load(self):
        if self.tracker_path.exists():
            try:
                with open(self.tracker_path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                self._data = raw.get("indexed_files", {})
                logger.info(f"Tracker loaded — {len(self._data)} files previously indexed")
            except Exception as e:
                logger.error(f"Failed to load tracker: {e}. Starting fresh.")
                self._data = {}
        else:
            logger.info("No tracker found — starting fresh.")

    def save(self):
        self.tracker_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "indexed_files": self._data,
            "last_scan": datetime.now(timezone.utc).isoformat(),
            "total_chunks_indexed": sum(
                e.get("chunk_count", 0)
                for e in self._data.values()
                if e.get("status") == "success"
            ),
        }
        tmp = str(self.tracker_path) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        os.replace(tmp, self.tracker_path)

    @staticmethod
    def compute_hash(filepath: str) -> str:
        sha = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha.update(chunk)
        return f"sha256:{sha.hexdigest()}"

    def scan(self, chunks_dir: str) -> List[Tuple[str, FileState]]:
        chunks_path = Path(chunks_dir)
        if not chunks_path.exists():
            raise FileNotFoundError(f"Chunks directory not found: {chunks_dir}")

        results = []
        json_files = sorted(chunks_path.glob("*.json"))

        if not json_files:
            logger.warning(f"No JSON files found in {chunks_dir}")
            return results

        for filepath in json_files:
            fname    = filepath.name
            cur_hash = self.compute_hash(str(filepath))

            if fname not in self._data:
                logger.info(f"NEW: {fname}")
                results.append((str(filepath), FileState.NEW))
            elif self._data[fname].get("file_hash") != cur_hash:
                logger.info(f"MODIFIED: {fname}")
                results.append((str(filepath), FileState.MODIFIED))
            else:
                logger.debug(f"UNCHANGED: {fname}")

        logger.info(f"Scan complete — {len(results)} file(s) to process out of {len(json_files)}")
        return results

    def get_old_chunk_ids(self, filename: str) -> List[str]:
        return self._data.get(filename, {}).get("chunk_ids", [])

    def mark_success(self, filename: str, file_hash: str, chunk_ids: List[str]):
        self._data[filename] = {
            "file_hash":   file_hash,
            "indexed_at":  datetime.now(timezone.utc).isoformat(),
            "chunk_ids":   chunk_ids,
            "chunk_count": len(chunk_ids),
            "status":      "success",
        }

    def mark_failed(self, filename: str, file_hash: str, error: str):
        existing = self._data.get(filename, {})
        self._data[filename] = {
            "file_hash":   file_hash,
            "indexed_at":  datetime.now(timezone.utc).isoformat(),
            "chunk_ids":   existing.get("chunk_ids", []),
            "chunk_count": existing.get("chunk_count", 0),
            "status":      f"failed: {error}",
        }