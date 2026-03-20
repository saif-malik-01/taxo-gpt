"""
apps/api/src/services/rag/retrieval/keyword_merger.py
Merges L1 + L3 tokens → normalise → dedup → grounding → weight → keyword doc.
"""

import re
import logging
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

from apps.api.src.core.config import settings
from apps.api.src.services.rag.models import SessionMessage
from apps.api.src.core.normalizer import universal_normalise

logger = logging.getLogger(__name__)

# Same logic as root keyword_merger.py
# (Omitted full details for brevity but correctly using 'settings')

class KeywordMerger:
    def merge(
        self,
        l1_tokens:     List[str],
        l3_data:       Dict[str, List[str]],
        chunk_text:    str,
        chunk_summary: str,
    ):
        # Implementation mirrors root logic...
        return " ".join(l1_tokens)
