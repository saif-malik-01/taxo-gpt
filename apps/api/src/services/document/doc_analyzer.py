"""
services/document/doc_analyzer.py

Thin delegation layer kept for backward compatibility.
All actual analysis now happens in doc_classifier.py via analyze_document().

Public API (unchanged from callers' perspective):
  analyze_document(text, user_question) -> dict
  reextract_missed_issues(full_text, existing_issues) -> list[str]
"""

import logging

logger = logging.getLogger(__name__)


def analyze_document(text: str, user_question: str = None) -> dict:
    """
    Delegate to doc_classifier.analyze_document.
    Returns: {summary, sender, recipient, issues}
    """
    from apps.api.src.services.document.doc_classifier import analyze_document as _analyze

    if not text or not text.strip():
        return {
            "summary":   "No document text available.",
            "sender":    None,
            "recipient": None,
            "issues":    [],
        }

    try:
        result = _analyze(
            full_text=text,
            user_message=user_question or "",
        )
        return {
            "summary":   result.get("brief_summary", ""),
            "sender":    result.get("parties", {}).get("sender"),
            "recipient": result.get("parties", {}).get("recipient"),
            "issues":    result.get("issues") or [],
        }
    except Exception as e:
        logger.error(f"analyze_document error: {e}", exc_info=True)
        return {
            "summary":   "Could not analyze the document automatically.",
            "sender":    None,
            "recipient": None,
            "issues":    [],
        }


def reextract_missed_issues(full_text: str, existing_issues: list) -> list:
    """
    Re-read the full document to find missed issues.
    Returns list[str] of NEW issue texts.
    """
    from apps.api.src.services.document.doc_classifier import reextract_missed_issues as _reextract

    if not full_text or not full_text.strip():
        return []

    try:
        return _reextract(full_text, existing_issues)
    except Exception as e:
        logger.error(f"reextract_missed_issues error: {e}", exc_info=True)
        return []