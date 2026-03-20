"""
apps/api/src/core/normalizer.py
Single source of truth for normalisation — applied identically
at index time and query time.
"""

import re
import logging
from typing import List, Optional
from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)

def universal_normalise(token: str) -> Optional[str]:
    """
    1. Strip whitespace
    2. Preserve known uppercase acronyms as-is
    3. Check authority map → replace with canonical acronym
    4. Lowercase
    5. Replace spaces / hyphens / slashes → underscore
    6. Remove all punctuation except underscore
    7. Collapse multiple underscores, strip edges
    8. Return None if empty
    """
    if not token or not token.strip():
        return None

    token = token.strip()

    if token.upper() in settings.PRESERVED_UPPERCASE:
        return token.upper()

    lower = token.lower()
    if lower in settings.AUTHORITY_MAP:
        return settings.AUTHORITY_MAP[lower]

    token = token.lower()
    token = re.sub(r"[\s\-/\\]", "_", token)
    token = re.sub(r"[^a-z0-9_]", "", token)
    token = re.sub(r"_+", "_", token)
    token = token.strip("_")

    return token if token else None


def whitespace_split_normalise(text: str) -> List[str]:
    if not text or not text.strip():
        return []
    return [n for t in text.split() if (n := universal_normalise(t))]


# ── Cross reference normalisers ──────────────────────────────────────────────

def _split_compound(value: str) -> List[str]:
    """Split on & and comma."""
    return [p.strip() for p in re.split(r"[&,]", value) if p.strip()]


def _canonicalise_ref_number(value: str) -> str:
    """14(a) → 14_a  |  80C(2)(xiii) → 80c_2_xiii"""
    value = value.lower()
    value = re.sub(r"\(([^)]*)\)", r"_\1", value)
    value = re.sub(r"[\s\-/\\]", "_", value)
    value = re.sub(r"[^a-z0-9_]", "", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def _extract_ref_number(stripped: str) -> str:
    m = re.match(r"(\d+[A-Za-z]{0,2}(?:\([^)]*\))*)", stripped.strip())
    if not m:
        return ""
    raw = m.group(1).lower()
    raw = re.sub(r"\(([^)]*)\)", r"_\1", raw)
    raw = re.sub(r"[^a-z0-9_]", "", raw)
    raw = re.sub(r"_+", "_", raw)
    return raw.strip("_")


def normalise_section_list(items: List[str]) -> List[str]:
    tokens = []
    for item in items:
        if not item:
            continue
        for part in _split_compound(str(item)):
            part = part.strip()

            sub_m = re.match(
                r"sub[-\s]?section\s*\(([^)]+)\)\s*of\s*(?:section|sec|sect|s)?\s*\.?\s*(\d+[A-Za-z]*)",
                part, re.IGNORECASE
            )
            if sub_m:
                sub = re.sub(r"[^a-z0-9]", "", sub_m.group(1).lower())
                num = re.sub(r"[^a-z0-9]", "", sub_m.group(2).lower())
                tokens.append(f"section_{num}_{sub}")
                continue

            slash_m = re.match(
                r"(?:section|sect|sec|s)?\s*\.?\s*(\d+[A-Za-z]*)\s*/\s*(\d+[A-Za-z]*)",
                part, re.IGNORECASE
            )
            if slash_m:
                for grp in (slash_m.group(1), slash_m.group(2)):
                    c = _canonicalise_ref_number(grp)
                    if c:
                        tokens.append(f"section_{c}")
                continue

            stripped = re.sub(
                r"^(sub[-\s]?section|section|sect|sec|s)\s*\.?\s*", "", part,
                flags=re.IGNORECASE,
            ).strip()
            if stripped:
                canonical = _extract_ref_number(stripped)
                if canonical:
                    tokens.append(f"section_{canonical}")
    return tokens


def normalise_rule_list(items: List[str]) -> List[str]:
    tokens = []
    for item in items:
        if not item:
            continue
        for part in _split_compound(str(item)):
            part = part.strip()

            sub_m = re.match(
                r"sub[-\s]?rule\s*\(([^)]+)\)\s*of\s*rule\s*\.?\s*(\d+[A-Za-z]*)",
                part, re.IGNORECASE
            )
            if sub_m:
                sub = re.sub(r"[^a-z0-9]", "", sub_m.group(1).lower())
                num = re.sub(r"[^a-z0-9]", "", sub_m.group(2).lower())
                tokens.append(f"rule_{num}_{sub}")
                continue

            stripped = re.sub(
                r"^(rule|r)\s*\.?\s*", "", part,
                flags=re.IGNORECASE,
            ).strip()
            if stripped:
                canonical = _extract_ref_number(stripped)
                if canonical:
                    tokens.append(f"rule_{canonical}")
    return tokens


def normalise_notification_list(items: List[str]) -> List[str]:
    tokens = []
    for item in items:
        if not item:
            continue
        for part in _split_compound(str(item)):
            stripped = re.sub(
                r"^(notification|notif|no)\s*\.?\s*", "", part.strip(),
                flags=re.IGNORECASE,
            ).strip()
            if stripped:
                v = stripped.lower()
                v = re.sub(r"[\s\-/\\]", "_", v)
                v = re.sub(r"[^a-z0-9_]", "", v)
                v = re.sub(r"_+", "_", v).strip("_")
                if v:
                    tokens.append(f"notification_{v}")
    return tokens


def normalise_circular_list(items: List[str]) -> List[str]:
    tokens = []
    for item in items:
        if not item:
            continue
        for part in _split_compound(str(item)):
            stripped = re.sub(
                r"^(circular|circ|cir)\s*\.?\s*", "", part.strip(),
                flags=re.IGNORECASE,
            ).strip()
            if stripped:
                v = stripped.lower()
                v = re.sub(r"[\s\-/\\]", "_", v)
                v = re.sub(r"[^a-z0-9_]", "", v)
                v = re.sub(r"_+", "_", v).strip("_")
                if v:
                    tokens.append(f"circular_{v}")
    return tokens


def normalise_gazette(value: str) -> Optional[str]:
    if not value:
        return None
    v = value.lower()
    v = re.sub(r"g\.?s\.?r\.?\s*", "gsr_", v, flags=re.IGNORECASE)
    v = re.sub(r"[().\s]", "", v)
    v = re.sub(r"_+", "_", v).strip("_")
    return v if v else None
