"""
pipeline/layer3_qwen.py
Layer 3 — Regex-based extraction only.
LLM removed. Regex handles sections, rules, notifications, circulars.
Full text whitespace split covers everything else.
"""

from typing import Dict, List

from pipeline.regex_fallback import extract_fallback
from utils.logger import get_logger

logger = get_logger("layer3")


class Layer3Qwen:
    """
    Name kept as Layer3Qwen for pipeline compatibility.
    Internally uses regex fallback only — no LLM calls.
    """

    def extract(self, text: str) -> Dict[str, List[str]]:
        if not text or not text.strip():
            return {}
        return extract_fallback(text)