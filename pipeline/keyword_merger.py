"""
pipeline/keyword_merger.py
Merges L1 + L3 tokens → normalise → dedup → grounding → weight → keyword doc.

Field-specific token handling:
  authorities   → whitespace split raw form (lowercase) + keep acronym uppercase
  parties       → whitespace split into individual lowercase words (no underscore)
  person_names  → whitespace split into individual lowercase words (no underscore)
  topics        → lowercase only, whitespace split (no underscore)
  legal_concepts→ lowercase_underscore kept as atomic token (do NOT split)
  all others    → universal normalise (lowercase + underscore)

Grounding check — L3 only, category-aware:
  L1 tokens              → always pass (trusted metadata source)
  dates/amounts/AY/FY    → exempt (normalised form ≠ raw text)
  authorities            → exempt (acronyms won't appear as full form)
  legal_concepts         → exempt (semantic addition from Qwen)
  topics                 → exempt (semantic addition from Qwen)
  parties/person_names   → individual words checked loosely
  section_*/rule_* etc   → numeric parts checked in text
"""

import re
from dataclasses import dataclass
from typing import Dict, List, Set

from config import CONFIG, PRESERVED_UPPERCASE
from utils.logger import get_logger
from utils.normalizer import universal_normalise

logger = get_logger("keyword_merger")

# Normalised token prefixes exempt from grounding check
GROUNDING_EXEMPT_PREFIXES = (
    "date_",
    "rs_",
    "AY_",
    "FY_",
    "gsr_",
)

# Qwen fields whose tokens are split into individual words (no underscore join)
WORD_SPLIT_FIELDS = {"authorities", "parties", "person_names", "topics", "case_citations"}

# Qwen fields whose tokens are kept as atomic underscore-joined concepts
ATOMIC_FIELDS = {"legal_concepts"}

# Short stop-words to discard
DISCARD_TOKENS: Set[str] = {
    "the", "of", "and", "in", "to", "a", "an", "for", "or",
    "is", "are", "was", "be", "by", "on", "at", "as", "it",
    "its", "with", "from", "that", "this", "which", "not", "also",
}


@dataclass
class TokenRecord:
    token:    str
    layer:    int
    weight:   int
    grounded: bool = True
    field:    str  = ""    # which Qwen field this came from (for debug)


@dataclass
class MergeResult:
    keyword_document:     str
    token_records:        List[TokenRecord]
    hypothetical_queries: List[str]
    l1_count:             int = 0
    l3_count:             int = 0
    discarded_count:      int = 0


class KeywordMerger:

    def merge(
        self,
        l1_tokens:     List[str],
        l3_data:       Dict[str, List[str]],
        chunk_text:    str,
        chunk_summary: str,
    ) -> MergeResult:

        # ── Extract hypothetical queries before processing ────────────
        hyp_queries = l3_data.pop("hypothetical_queries", [])

        # ── Step 1: Normalise L1 tokens ──────────────────────────────
        l1_norm = [
            n for t in l1_tokens
            if (n := universal_normalise(t)) and n not in DISCARD_TOKENS and len(n) > 1
        ]

        # ── Step 2: Process L3 tokens field-by-field ─────────────────
        l3_processed: List[tuple] = []  # (token, field_name)

        for field, values in l3_data.items():
            for value in values:
                if not value or not value.strip():
                    continue
                tokens = self._process_l3_token(field, value.strip())
                for t in tokens:
                    if t and len(t) > 1 and t not in DISCARD_TOKENS:
                        l3_processed.append((t, field))

        # ── Step 3: Dedup — L1 takes priority over L3 ────────────────
        seen: Dict[str, tuple] = {}  # token → (layer, field)

        for t in l1_norm:
            if t not in seen:
                seen[t] = (1, "metadata")

        for t, field in l3_processed:
            if t not in seen:
                seen[t] = (3, field)

        # ── Step 4: Category-aware grounding check ────────────────────
        combined  = (chunk_text + " " + chunk_summary).lower()
        records:   List[TokenRecord] = []
        discarded = 0

        for token, (layer, field) in seen.items():
            weight = CONFIG.bm25.l1_weight if layer == 1 else CONFIG.bm25.l3_weight

            if layer == 1:
                grounded = True
            else:
                grounded = self._is_grounded(token, field, combined)

            records.append(TokenRecord(
                token=token, layer=layer, weight=weight,
                grounded=grounded, field=field
            ))
            if not grounded:
                discarded += 1
                logger.debug(f"  Grounding FAIL: '{token}' (field={field})")

        # ── Step 5: Build keyword document ───────────────────────────
        parts = []
        for rec in records:
            if rec.grounded:
                parts += [rec.token] * rec.weight

        for hq in hyp_queries:
            if hq and hq.strip():
                parts.append(hq.strip())

        result = MergeResult(
            keyword_document     = " ".join(parts),
            token_records        = records,
            hypothetical_queries = hyp_queries,
            l1_count             = sum(1 for r in records if r.layer == 1),
            l3_count             = sum(1 for r in records if r.layer == 3),
            discarded_count      = discarded,
        )

        logger.debug(
            f"  Merge — L1:{result.l1_count} L3:{result.l3_count} "
            f"discarded:{result.discarded_count} hyp:{len(hyp_queries)}"
        )
        return result

    # ── Field-specific token processing ──────────────────────────────

    def _process_l3_token(self, field: str, value: str) -> List[str]:
        """
        Process a single Qwen output value based on which field it came from.

        authorities  → whitespace split, lowercase each word,
                       EXCEPT known acronyms stay uppercase
        parties      → whitespace split, lowercase each word
        person_names → whitespace split, lowercase each word
        topics       → whitespace split, lowercase each word
        legal_concepts → lowercase + underscore (atomic, do not split)
        all others   → universal_normalise
        """

        if field in ("parties", "person_names", "case_citations"):
            # Split into individual lowercase words — no underscore join
            # Partial name/citation search must work
            # Handle initials: "R.K." → "rk" before splitting
            # Keep court acronyms uppercase: SC, HC, ITAT, AAR
            value = re.sub(r"\.(?=[A-Za-z]\.?)", "", value)
            words = re.split(r"[\s,\-&\[\]]+", value)
            result = []
            for w in words:
                w = w.strip(".")
                if not w or len(w) <= 1:
                    continue
                if w.upper() in PRESERVED_UPPERCASE:
                    result.append(w.upper())   # SC, HC, ITAT stay uppercase
                elif w.lower() not in DISCARD_TOKENS:
                    result.append(w.lower())
            return result

        if field == "topics":
            # Lowercase only, whitespace split — no underscore
            words = value.lower().split()
            return [
                w for w in words
                if w and len(w) > 1 and w not in DISCARD_TOKENS
            ]

        if field == "authorities":
            # Split into individual words
            # Known acronyms stay uppercase, everything else lowercase
            words = re.split(r"[\s,.\-&]+", value)
            result = []
            for w in words:
                if not w or len(w) <= 1:
                    continue
                if w.upper() in PRESERVED_UPPERCASE:
                    result.append(w.upper())   # CBIC, CBDT etc — keep uppercase
                elif w.lower() not in DISCARD_TOKENS:
                    result.append(w.lower())   # individual words lowercase
            return result

        if field == "legal_concepts":
            # Lowercase + underscore — keep as atomic compound token
            v = value.lower()
            v = re.sub(r"[\s\-/\\]", "_", v)
            v = re.sub(r"[^a-z0-9_]", "", v)
            v = re.sub(r"_+", "_", v).strip("_")
            return [v] if v and len(v) > 1 else []

        # All other fields — universal normalise
        n = universal_normalise(value)
        return [n] if n and len(n) > 1 else []

    # ── Grounding check ───────────────────────────────────────────────

    def _is_grounded(self, token: str, field: str, combined_text: str) -> bool:
        """
        Category-aware grounding for L3 tokens only.
        """
        # Exempt prefixes — normalised form never appears verbatim in raw text
        if token.startswith(GROUNDING_EXEMPT_PREFIXES):
            return True

        # Known acronyms — exempt
        if token.upper() in PRESERVED_UPPERCASE:
            return True

        # Semantic fields — exempt (Qwen derives these from understanding,
        # not word-for-word copying from text)
        if field in ("legal_concepts", "topics", "authorities"):
            return True

        # Specific legal refs — check numeric/identifier parts only
        for prefix in ("section_", "rule_", "notification_", "circular_",
                        "act_", "form_"):
            if token.startswith(prefix):
                return self._check_parts(token, combined_text, skip=1)

        # Case citations handled as word-split — individual words exempt above

        # Parties, person names, case citations — exempt from grounding
        # Individual words extracted by Qwen directly from the text
        # they are by definition present (Qwen read the text to extract them)
        if field in ("parties", "person_names", "case_citations"):
            return True

        # Everything else — loose check
        bare = token.replace("_", " ")
        return (
            token.lower() in combined_text
            or bare.lower() in combined_text
            or any(p in combined_text for p in token.split("_") if len(p) >= 3)
        )

    def _check_parts(
        self,
        token:   str,
        text:    str,
        skip:    int = 1,
        min_len: int = 2,
    ) -> bool:
        parts = token.split("_")[skip:]
        meaningful = [p for p in parts if len(p) >= min_len]
        if not meaningful:
            return True
        for p in meaningful:
            if p in text:
                return True
            # Check bracket forms: section_4_1a → "(1a)" or "(1A)" in text
            if f"({p})" in text or f"({p.upper()})" in text:
                return True
            # Digits-only fallback: "1a" → check "1" exists in text
            digits_only = re.sub(r"[^0-9]", "", p)
            if digits_only and digits_only in text:
                return True
        return False