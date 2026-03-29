"""
pipeline/layer1_extractor.py
Layer 1 — Metadata field extraction. Whitespace split + universal normalise.
Covers all known ext fields across all chunk types.
"""

from typing import Any, Dict, List

from utils.logger import get_logger
from utils.normalizer import (
    whitespace_split_normalise,
    normalise_section_list,
    normalise_rule_list,
    normalise_notification_list,
    normalise_circular_list,
    normalise_gazette,
    universal_normalise,
)

logger = get_logger("layer1_extractor")


def _str_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if v and str(v).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _str(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    return "" if s.lower() in ("null", "none", "") else s


class Layer1Extractor:

    def extract(self, chunk: Dict[str, Any]) -> List[str]:
        tokens: List[str] = []

        tokens += self._keywords(chunk)
        tokens += self._primary_topics(chunk)
        tokens += self._parent_doc(chunk)
        tokens += self._query_categories(chunk)
        tokens += self._cross_references(chunk)
        tokens += self._ext_fields(chunk)

        tokens = [t for t in tokens if t]
        logger.debug(f"  L1 extracted {len(tokens)} raw tokens")
        return tokens

    # ── Always present ────────────────────────────────────────────────

    def _keywords(self, chunk: Dict) -> List[str]:
        tokens = []
        for kw in _str_list(chunk.get("keywords")):
            tokens += whitespace_split_normalise(kw)
        return tokens

    def _primary_topics(self, chunk: Dict) -> List[str]:
        tokens = []
        retrieval = chunk.get("retrieval") or {}
        for t in _str_list(retrieval.get("primary_topics")):
            tokens += whitespace_split_normalise(t)
        return tokens

    def _parent_doc(self, chunk: Dict) -> List[str]:
        return whitespace_split_normalise(_str(chunk.get("parent_doc")))

    def _query_categories(self, chunk: Dict) -> List[str]:
        retrieval = chunk.get("retrieval") or {}
        return [c for c in _str_list(retrieval.get("query_categories")) if c]

    # ── Cross references ──────────────────────────────────────────────

    def _cross_references(self, chunk: Dict) -> List[str]:
        tokens = []
        xref = chunk.get("cross_references") or {}

        sections = _str_list(xref.get("sections"))
        if sections:
            tokens += normalise_section_list(sections)

        rules = _str_list(xref.get("rules"))
        if rules:
            tokens += normalise_rule_list(rules)

        notifs = _str_list(xref.get("notifications"))
        if notifs:
            tokens += normalise_notification_list(notifs)

        circs = _str_list(xref.get("circulars"))
        if circs:
            tokens += normalise_circular_list(circs)

        for jid in _str_list(xref.get("judgment_ids")):
            tokens += whitespace_split_normalise(jid)

        for form in _str_list(xref.get("forms")):
            tokens += whitespace_split_normalise(form)

        return tokens

    # ── ext fields ────────────────────────────────────────────────────

    def _ext_fields(self, chunk: Dict) -> List[str]:
        tokens = []
        ext = chunk.get("ext") or {}

        # ── Section / Act chunks ─────────────────────────────────────
        for field in ("act", "chapter_number", "chapter_title", "section_title"):
            v = _str(ext.get(field))
            if v:
                tokens += whitespace_split_normalise(v)

        if ext.get("section_number"):
            v = _str(ext["section_number"])
            if v:
                tokens += normalise_section_list([v])

        # ── Notification chunks ──────────────────────────────────────
        for field in ("notification_number", "notification_type",
                      "taxpayer_category", "applicable_period"):
            v = _str(ext.get(field))
            if v:
                tokens += whitespace_split_normalise(v)

        if ext.get("issued_by"):
            n = universal_normalise(_str(ext["issued_by"]))
            if n:
                tokens.append(n)

        if ext.get("gazette_reference"):
            gaz = normalise_gazette(_str(ext["gazette_reference"]))
            if gaz:
                tokens.append(gaz)

        if ext.get("amends_notification"):
            v = _str(ext["amends_notification"])
            if v:
                tokens += normalise_notification_list([v])

        # ── Circular chunks ──────────────────────────────────────────
        for field in ("circular_number", "circular_date", "subject"):
            v = _str(ext.get(field))
            if v:
                tokens += whitespace_split_normalise(v)

        # ── Form chunks ──────────────────────────────────────────────
        for field in ("form_number", "form_name", "form_full_title"):
            v = _str(ext.get(field))
            if v:
                tokens += whitespace_split_normalise(v)

        if ext.get("prescribed_under_rule"):
            v = _str(ext["prescribed_under_rule"])
            if v:
                tokens += normalise_rule_list([v])

        for col in _str_list(ext.get("register_columns")):
            tokens += whitespace_split_normalise(col)

        # ── Judgement chunks ─────────────────────────────────────────
        for field in ("case_name", "court", "citation",
                      "judgment_date", "petitioner", "respondent"):
            v = _str(ext.get(field))
            if v:
                tokens += whitespace_split_normalise(v)

        if ext.get("court_level"):
            n = universal_normalise(_str(ext["court_level"]))
            if n:
                tokens.append(n)

        if ext.get("decision"):
            d = _str(ext["decision"])
            if d:
                tokens.append(d)

        # ── HSN chunks ───────────────────────────────────────────────
        if ext.get("hsn_code"):
            v = _str(ext["hsn_code"])
            if v:
                tokens.append(v)              # 01013090 — keep as-is

        if ext.get("hsn_code_raw"):
            v = _str(ext["hsn_code_raw"])
            if v:
                tokens += whitespace_split_normalise(v)   # 0101 30 90 → individual parts

        if ext.get("chapter_code"):
            v = _str(ext["chapter_code"])
            if v:
                tokens.append(v)              # 01 — keep as-is

        for field in ("sub_chapter_title", "description"):
            v = _str(ext.get(field))
            if v:
                tokens += whitespace_split_normalise(v)

        # chapter_title shared across HSN and section chunks
        # already handled above in Section/Act block

        # ── SAC chunks ───────────────────────────────────────────────
        if ext.get("sac_code"):
            v = _str(ext["sac_code"])
            if v:
                tokens.append(v)              # 996321 — keep as-is

        # ext["section"] in SAC chunks — normalise as section ref
        if ext.get("section"):
            v = _str(ext["section"])
            if v:
                tokens += normalise_section_list([v])

        # description shared with HSN — already handled above

        return tokens