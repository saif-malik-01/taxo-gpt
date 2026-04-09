"""
ingestion_api/utils/chunking.py

Titan-optimized paragraph-aware chunking for legal judgments.
Extracted from chunker.py.
"""

import re

MAX_CHUNK_CHARS = 1400
OVERLAP_CHARS = 180
MIN_CHUNK_CHARS = 500


def split_order_text(
    text: str,
    max_chars: int = MAX_CHUNK_CHARS,
    overlap_chars: int = OVERLAP_CHARS,
    min_chunk_chars: int = MIN_CHUNK_CHARS
) -> list[str]:
    """
    Titan-optimized paragraph-aware chunking for legal judgments.
    Splits very long text into chunks aiming for max_chars, ensuring sentences aren't split prematurely.
    """
    if not text or not text.strip():
        return []

    text = re.sub(r'\r\n?', '\n', text).strip()

    paras = [p.strip() for p in re.split(r'\n\s*\n+', text) if p.strip()]
    if not paras:
        return [text]

    chunks = []
    current = []

    def current_len(parts):
        return sum(len(p) for p in parts) + max(0, len(parts) - 1) * 2

    def add_chunk(parts):
        chunk = "\n\n".join(parts).strip()
        if chunk:
            chunks.append(chunk)

    for para in paras:
        if len(para) > max_chars:
            sentences = re.split(r'(?<=[.!?])\s+', para)
            for sent in sentences:
                sent = sent.strip()
                if not sent:
                    continue
                if current and current_len(current + [sent]) > max_chars:
                    add_chunk(current)
                    prev = chunks[-1] if chunks else ""
                    overlap = prev[-overlap_chars:] if overlap_chars and prev else ""
                    current = [overlap, sent] if overlap else [sent]
                else:
                    current.append(sent)
            continue

        if current and current_len(current + [para]) > max_chars:
            add_chunk(current)
            prev = chunks[-1] if chunks else ""
            overlap = prev[-overlap_chars:] if overlap_chars and prev else ""
            current = [overlap, para] if overlap else [para]
        else:
            current.append(para)

    if current:
        add_chunk(current)

    merged = []
    for ch in chunks:
        if merged and len(ch) < min_chunk_chars:
            merged[-1] += "\n\n" + ch
        else:
            merged.append(ch)

    return [c.strip() for c in merged if c.strip()]
