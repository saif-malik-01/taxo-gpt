"""
services/document/doc_analyzer.py
Delegates to DocumentAnalyzer in processor.py (full two-pass, verbatim extraction).

Public API (unchanged — callers in document.py do not need to change):
    analyze_document(text, user_question) -> dict
    reextract_missed_issues(full_text, existing_issues) -> list[str]
"""

import logging
import threading

logger = logging.getLogger(__name__)

# ── Lazy singleton — thread-safe ──────────────────────────────────────────────
_analyzer = None
_analyzer_lock = threading.Lock()


def _get_analyzer():
    global _analyzer
    if _analyzer is None:
        with _analyzer_lock:
            if _analyzer is None:          # double-checked locking
                from services.document.processor import DocumentAnalyzer
                _analyzer = DocumentAnalyzer()
                logger.info("DocumentAnalyzer initialised")
    return _analyzer


# ── Public functions ──────────────────────────────────────────────────────────

def analyze_document(text: str, user_question: str = None) -> dict:
    """
    Extract summary, parties, and all issues/allegations from document text.

    Uses DocumentAnalyzer which:
      - Handles full document text (two-pass for docs > 80 000 chars)
      - Extracts issues VERBATIM with all entities (amounts, sections, periods)
      - Deduplicates overlapping issues (85 % similarity threshold)

    Returns:
        summary  : str
        sender   : str | None
        recipient: str | None
        issues   : list[str]  — verbatim, with all identifying details
    """
    if not text or not text.strip():
        return {
            "summary": "No document text available.",
            "sender": None,
            "recipient": None,
            "issues": [],
        }

    try:
        result = _get_analyzer().analyze(text, user_question)

        # Normalise to the flat dict shape callers expect
        return {
            "summary":   result.get("summary", ""),
            "sender":    result.get("sender"),
            "recipient": result.get("recipient"),
            "issues":    result.get("issues") or [],
        }

    except Exception as e:
        logger.error(f"Document analysis error: {e}", exc_info=True)
        return {
            "summary": "Could not analyze the document automatically.",
            "sender": None,
            "recipient": None,
            "issues": [],
        }


def reextract_missed_issues(full_text: str, existing_issues: list) -> list:
    """
    Re-read the full document to find issues missed in the initial extraction.

    full_text       : complete extracted text (fetched fresh from DB by caller)
    existing_issues : list of issue dicts with at least {"text": str}

    Returns list[str] — NEW issue texts only, not already in existing_issues.
    """
    if not full_text or not full_text.strip():
        return []

    try:
        return _get_analyzer().reextract_missed_issues(full_text, existing_issues)
    except Exception as e:
        logger.error(f"Reextract issues error: {e}", exc_info=True)
        return []