"""
services/document/doc_classifier.py  — patched

Only the issue-extraction section is changed vs original. All other functions
(analyze_document, extract_legal_entities, determine_route, etc.) are identical.

Root causes of the 40-page SCN stall fixed:
  1. max_tokens 8192→16384 for single-call path  (8192 truncated at ~15 issues)
  2. Two-pass serial replaced with segmented parallel extraction
     - 25 000-char segments, 3 000-char overlap, ThreadPoolExecutor(max_workers=4)
     - All segments run concurrently; results deduplicated at 85 % similarity
  3. Per-issue 800-word cap added to prompt
     - Prevents runaway verbatim output on very long allegation paragraphs
  4. _extract_issues_once() kept as a no-op shim for backward compatibility
  5. reextract_missed_issues() uses same segmented approach + focused prompt
"""

import json
import logging
import re
import threading
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── LLM singleton ─────────────────────────────────────────────────────────────

_llm      = None
_llm_lock = threading.Lock()


def _get_llm():
    global _llm
    if _llm is None:
        with _llm_lock:
            if _llm is None:
                from apps.api.src.services.rag.retrieval.bedrock_llm import BedrockLLMClient
                _llm = BedrockLLMClient()
    return _llm


def _parse_json(raw: Optional[str], fallback: dict) -> dict:
    if not raw:
        return fallback
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        text = m.group()
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e} | raw[:200]={raw[:200]}")

    # Truncation recovery for {"issues": [...]}
    try:
        array_match = re.search(r'"issues"\s*:\s*\[(.+)', text, re.DOTALL)
        if array_match:
            array_body = array_match.group(1)
            recovered = re.findall(r'"((?:[^"\\]|\\.)*)"', array_body)
            recovered = [s for s in recovered if len(s) >= 20]
            if recovered:
                logger.warning(
                    f"JSON truncated — recovered {len(recovered)} partial issues "
                    f"from {len(raw)} chars of output"
                )
                return {"issues": recovered}
    except Exception:
        pass

    # Truncation recovery for {"replied_issues": [...]}
    try:
        array_match = re.search(r'"replied_issues"\s*:\s*\[(.+)', text, re.DOTALL)
        if array_match:
            obj_matches = re.findall(
                r'\{\s*"issue_text"\s*:\s*"((?:[^"\\]|\\.)*)"\s*,\s*"reply_text"\s*:\s*"((?:[^"\\]|\\.)*)"\s*\}',
                array_match.group(1),
            )
            if obj_matches:
                logger.warning(f"Replied-issues JSON truncated — recovered {len(obj_matches)} pairs")
                return {
                    "replied_issues": [
                        {"issue_text": it, "reply_text": rt}
                        for it, rt in obj_matches
                    ]
                }
    except Exception:
        pass

    return fallback


# ── Party name normalisation ──────────────────────────────────────────────────

def _normalise_party(name: str) -> str:
    if not name:
        return ""
    n = name.lower().strip()
    n = re.sub(r"\bm/s\b\.?", "", n)
    n = re.sub(r"\bprivate\b", "pvt", n)
    n = re.sub(r"\blimited\b", "ltd", n)
    n = re.sub(r"\bincorporated\b", "inc", n)
    n = re.sub(r"[^a-z0-9 ]", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


_GENERIC_WHITELIST = re.compile(
    r"\b(court|high court|supreme court|itat|cestat|gstat|aar|aaar|"
    r"cbdt|cbic|commissioner|principal commissioner|joint commissioner|"
    r"deputy commissioner|assistant commissioner|officer)\b",
    re.IGNORECASE,
)


def _is_generic_party(name: str) -> bool:
    return bool(_GENERIC_WHITELIST.search(name or ""))


# ═══════════════════════════════════════════════════════════════════
# STEP 2A+2C — Combined metadata + intent  (unchanged)
# ═══════════════════════════════════════════════════════════════════

_2AC_SYSTEM = (
    "You are a legal document classifier for Indian tax proceedings. "
    "Return ONLY valid JSON — no markdown, no explanation."
)

_2AC_PROMPT = """Analyse this tax/legal document and classify it. Return ONLY valid JSON.

DOCUMENT TEXT:
{doc_text}

USER MESSAGE (may be empty): "{user_message}"
UPLOAD HINTS (filename signals): {upload_hints}
USER CONTEXT (prior instructions): {user_context}
ACTIVE CASE SNAPSHOT (other documents already in this case):
{active_case_info}

CLASSIFICATION PRIORITY ORDER:
1. Explicit user statement in USER MESSAGE about this document's role/timing -> locked=true
2. UPLOAD HINTS (filename signals) -> soft confirmation
3. Cross-document date comparison against ACTIVE CASE SNAPSHOT primaries
4. Document self-reference language
5. Procedural stage language

ROLE DEFINITIONS:
  primary         -- notice/SCN/order/demand that requires a formal reply
  previous_reply  -- a reply/response already submitted to the authority
  user_draft_reply -- user's own prepared reply, NOT yet submitted
  reference       -- judgment/circular/notification/rule for legal support
  informational   -- GST return/ITR/P&L/balance sheet (Q&A only, no drafting)

TEMPORAL ROLE:
  current = active obligation; historical = prior stage; unknown = insufficient signal

Return this JSON:
{{
  "role": "primary|previous_reply|user_draft_reply|reference|informational",
  "role_locked": false,
  "display_type": "notice|SCN|show_cause_notice|order|demand_order|assessment_order|judgment|circular|notification|rule_text|faq|gst_return|itr|other",
  "temporal_role": "current|historical|unknown",
  "temporal_locked": false,
  "has_issues": true/false,
  "has_replied_issues": true/false,
  "parties": {{
    "sender": "authority/court name or null",
    "recipient": "taxpayer name or null",
    "gstin": "15-char GSTIN or null",
    "pan": "10-char PAN or null"
  }},
  "reference_number": "verbatim reference number or null",
  "date": "DD-MM-YYYY or null",
  "brief_summary": "<=400 chars, all legal entities preserved",
  "confidence": 0-100,
  "intent": "summarize|draft_direct|draft_all|draft_specific|query_document|query_general|update_issues|confirm_mode|null",
  "mode": "defensive|in_favour|null",
  "issue_numbers": []
}}"""


def analyze_document(
    full_text: str,
    user_message: str = "",
    user_context: str = "",
    active_case_info: Optional[dict] = None,
    upload_hints: Optional[List[str]] = None,
) -> dict:
    fallback = {
        "role": "primary", "role_locked": False,
        "display_type": "notice", "temporal_role": "unknown",
        "temporal_locked": False, "has_issues": False,
        "has_replied_issues": False,
        "parties": {"sender": None, "recipient": None, "gstin": None, "pan": None},
        "reference_number": None, "date": None, "brief_summary": "",
        "confidence": 50, "intent": "summarize", "mode": None,
        "issue_numbers": [],
    }
    if not full_text or not full_text.strip():
        return fallback

    text_for_llm = full_text[:80000]
    hints_str    = json.dumps(upload_hints or [])
    case_str     = json.dumps(active_case_info or {})

    prompt = _2AC_PROMPT.format(
        doc_text         = text_for_llm,
        user_message     = user_message or "",
        upload_hints     = hints_str,
        user_context     = user_context or "(none)",
        active_case_info = case_str,
    )

    raw = _get_llm().call(
        system_prompt = _2AC_SYSTEM,
        user_message  = prompt,
        max_tokens    = 1024,
        temperature   = 0.0,
        label         = "step_2ac",
    )
    result = _parse_json(raw, fallback)

    if result.get("role") not in ("primary", "previous_reply", "user_draft_reply", "reference", "informational"):
        result["role"] = fallback["role"]
    if result.get("temporal_role") not in ("current", "historical", "unknown"):
        result["temporal_role"] = "unknown"

    # has_issues false-negative mitigation
    if result.get("role") == "primary" and not result.get("has_issues"):
        allegation_kw = re.compile(
            r"\b(alleged|allegation|demand|short.?payment|excess.?itc|mismatch|"
            r"recoverable|charge|show.?cause|violation|contravention|liability|"
            r"not paid|non.?payment|wrongly|erroneously)\b",
            re.IGNORECASE,
        )
        hits = len(allegation_kw.findall(text_for_llm[:10000]))
        if hits >= 2:
            result["has_issues"] = True
            logger.info(f"has_issues overridden to True via keyword check ({hits} hits)")

    logger.info(
        f"2A+2C: role={result.get('role')} temporal={result.get('temporal_role')} "
        f"has_issues={result.get('has_issues')} intent={result.get('intent')} "
        f"conf={result.get('confidence')}"
    )
    return result


# ═══════════════════════════════════════════════════════════════════
# STEP 2B — Legal entity extraction  (unchanged)
# ═══════════════════════════════════════════════════════════════════

_entity_extractor_lock = threading.Lock()
_stage2a_regex         = None
_stage2b_llm           = None


def _get_entity_extractors():
    global _stage2a_regex, _stage2b_llm
    if _stage2a_regex is None:
        with _entity_extractor_lock:
            if _stage2a_regex is None:
                from apps.api.src.services.rag.retrieval.extractor import Stage2ARegex, Stage2BLLM
                _stage2a_regex = Stage2ARegex()
                _stage2b_llm   = Stage2BLLM(_get_llm())
                logger.info("Stage2ARegex + Stage2BLLM initialised for Step 2B")
    return _stage2a_regex, _stage2b_llm


def extract_legal_entities(full_text: str) -> dict:
    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
    from apps.api.src.services.rag.retrieval.models import Stage2AResult, Stage2BResult

    empty = {
        "sections": [], "rules": [], "notifications": [], "circulars": [],
        "acts": [], "keywords": [], "topics": [], "keywords_raw": [],
        "form_name": None, "form_number": None, "case_name": None,
        "parties": [], "person_names": [], "case_number": None,
        "court": None, "court_level": None, "citation": None,
        "decision_type": None, "hsn_code": None, "sac_code": None,
        "issued_by": None,
        "_stage2a_normalised": [], "_stage2a_raw": [],
    }
    if not full_text or not full_text.strip():
        return empty

    stage2a_obj, stage2b_llm = _get_entity_extractors()
    text_for_llm = full_text[:60000]

    results: dict = {}
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = {
            ex.submit(stage2a_obj.extract, full_text[:30000]): "2a",
            ex.submit(stage2b_llm.extract, text_for_llm):      "2b",
        }
        for fut in _as_completed(futs):
            key = futs[fut]
            try:
                results[key] = fut.result()
            except Exception as e:
                logger.error(f"Step 2B stage {key} failed: {e}")

    stage2a: Stage2AResult = results.get("2a") or Stage2AResult([], [], None)
    stage2b: Stage2BResult = results.get("2b") or Stage2BResult()

    import re as _re
    sec_tokens   = [t for t in stage2a.normalised_tokens if _re.search(r"section", t, _re.I)]
    rule_tokens  = [t for t in stage2a.normalised_tokens if _re.search(r"rule",    t, _re.I)]
    notif_tokens = [t for t in stage2a.normalised_tokens if _re.search(r"notif",   t, _re.I)]
    circ_tokens  = [t for t in stage2a.normalised_tokens if _re.search(r"circ",    t, _re.I)]

    merged = {
        "sections":      list(dict.fromkeys((stage2b.sections or []) + sec_tokens)),
        "rules":         list(dict.fromkeys((stage2b.rules or []) + rule_tokens)),
        "notifications": list(dict.fromkeys((stage2b.notifications or []) + notif_tokens)),
        "circulars":     list(dict.fromkeys((stage2b.circulars or []) + circ_tokens)),
        "acts":          list(stage2b.acts or []),
        "keywords":      list(stage2b.keywords or []),
        "topics":        list(stage2b.topics or []),
        "keywords_raw":  list(stage2a.raw_tokens or []),
        "form_name":     stage2b.form_name,
        "form_number":   stage2b.form_number,
        "case_name":     stage2b.case_name,
        "parties":       list(stage2b.parties or []),
        "person_names":  list(stage2b.person_names or []),
        "case_number":   stage2b.case_number,
        "court":         stage2b.court,
        "court_level":   stage2b.court_level,
        "citation":      stage2b.citation or stage2a.citation,
        "decision_type": stage2b.decision_type,
        "hsn_code":      stage2b.hsn_code,
        "sac_code":      stage2b.sac_code,
        "issued_by":     stage2b.issued_by,
        "_stage2a_normalised": list(stage2a.normalised_tokens or []),
        "_stage2a_raw":        list(stage2a.raw_tokens or []),
    }
    logger.info(
        f"2B: sections={len(merged['sections'])} rules={len(merged['rules'])} "
        f"notifications={len(merged['notifications'])} circulars={len(merged['circulars'])}"
    )
    return merged


def entities_to_stage2b_result(raw_entities: dict):
    try:
        from apps.api.src.services.rag.retrieval.models import Stage2BResult, Stage2AResult
        from apps.api.src.services.rag.retrieval.extractor import build_bm25_keyword_document

        stage2b = Stage2BResult(
            sections      = raw_entities.get("sections", []),
            rules         = raw_entities.get("rules", []),
            notifications = raw_entities.get("notifications", []),
            circulars     = raw_entities.get("circulars", []),
            acts          = raw_entities.get("acts", []),
            keywords      = raw_entities.get("keywords", []),
            topics        = raw_entities.get("topics", []),
            form_name     = raw_entities.get("form_name"),
            form_number   = raw_entities.get("form_number"),
            case_name     = raw_entities.get("case_name"),
            parties       = raw_entities.get("parties", []),
            person_names  = raw_entities.get("person_names", []),
            case_number   = raw_entities.get("case_number"),
            court         = raw_entities.get("court"),
            court_level   = raw_entities.get("court_level"),
            citation      = raw_entities.get("citation"),
            decision_type = raw_entities.get("decision_type"),
            hsn_code      = raw_entities.get("hsn_code"),
            sac_code      = raw_entities.get("sac_code"),
            issued_by     = raw_entities.get("issued_by"),
        )
        stage2a = Stage2AResult(
            normalised_tokens = raw_entities.get("_stage2a_normalised", []),
            raw_tokens        = raw_entities.get("_stage2a_raw", []),
            citation          = raw_entities.get("citation"),
        )
        stage2b._bm25_keyword_doc = build_bm25_keyword_document(stage2a, stage2b)
        return stage2b
    except Exception as e:
        logger.warning(f"Could not reconstruct Stage2BResult: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
# MULTI-PART NOTICE DETECTION  (unchanged)
# ═══════════════════════════════════════════════════════════════════

def detect_multipart_notices(docs: List[dict]) -> List[dict]:
    return docs


def merge_multipart_docs(
    docs_with_analysis: List[Tuple[dict, dict]]
) -> List[Tuple[dict, dict]]:
    groups: Dict[str, List[Tuple[dict, dict]]] = {}
    no_ref = []
    for doc, analysis in docs_with_analysis:
        ref = (analysis.get("reference_number") or "").strip()
        if ref:
            groups.setdefault(ref, []).append((doc, analysis))
        else:
            no_ref.append((doc, analysis))

    result = []
    for ref, group in groups.items():
        if len(group) == 1:
            result.append(group[0])
        else:
            merged_doc      = dict(group[0][0])
            merged_analysis = dict(group[0][1])
            all_texts  = [d["full_text"] for d, _ in group]
            all_fnames = [d["filename"]  for d, _ in group]
            merged_doc["full_text"] = "\n\n".join(
                f"[PART: {fn}]\n{txt}"
                for fn, txt in zip(all_fnames, all_texts)
            )
            merged_doc["page_count"]     = sum(d.get("page_count", 0) for d, _ in group)
            merged_doc["filename"]       = " + ".join(all_fnames)
            merged_doc["part_filenames"] = all_fnames
            summaries = [a.get("brief_summary", "") for _, a in group]
            merged_analysis["brief_summary"] = max(summaries, key=len)
            logger.info(f"Merged {len(group)} parts: ref={ref} files={all_fnames}")
            result.append((merged_doc, merged_analysis))

    result.extend(no_ref)
    return result


# ═══════════════════════════════════════════════════════════════════
# STEP 3 — Same-case determination  (unchanged)
# ═══════════════════════════════════════════════════════════════════

def compute_summary_similarity(summary_a: str, summary_b: str) -> float:
    if not summary_a or not summary_b:
        return 0.0
    tokens_a = set(re.findall(r"\b\w+\b", summary_a.lower()))
    tokens_b = set(re.findall(r"\b\w+\b", summary_b.lower()))
    stops = {
        "the","a","an","of","in","to","for","and","or","is","are","was",
        "be","by","on","at","as","it","its","this","that","which","with",
        "from","has","have",
    }
    tokens_a -= stops
    tokens_b -= stops
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / len(tokens_a | tokens_b)


def determine_route(
    analysis: dict,
    case: Optional[dict],
    upload_hints: Optional[List[str]] = None,
) -> str:
    role    = analysis.get("role", "reference")
    ref_num = (analysis.get("reference_number") or "").strip()
    parties = analysis.get("parties") or {}
    summary = analysis.get("brief_summary", "")

    if not case:
        return "new_case_primary" if role == "primary" else "new_case_reference"

    case_ref = (case.get("reference_number") or "").strip()
    if ref_num and case_ref and ref_num == case_ref:
        return _route_for_role(role)

    incoming_names = [parties.get("recipient", ""), parties.get("sender", "")]
    case_parties   = case.get("parties") or {}
    case_names     = [case_parties.get("taxpayer_name", ""), case_parties.get("authority", "")]

    for inc in incoming_names:
        if not inc or _is_generic_party(inc):
            continue
        norm_inc = _normalise_party(inc)
        if not norm_inc:
            continue
        if parties.get("gstin") and case_parties.get("gstin"):
            if parties["gstin"].upper() == case_parties["gstin"].upper():
                return _route_for_role(role)
        if parties.get("pan") and case_parties.get("pan"):
            if parties["pan"].upper() == case_parties["pan"].upper():
                return _route_for_role(role)
        for case_name in case_names:
            if not case_name or _is_generic_party(case_name):
                continue
            norm_case = _normalise_party(case_name)
            if norm_inc and norm_case and (
                norm_inc == norm_case
                or norm_inc in norm_case
                or norm_case in norm_inc
            ):
                return _route_for_role(role)

    case_summaries = [
        d.get("brief_summary", "")
        for d in case.get("docs", [])
        if d.get("role") == "primary"
    ]
    max_sim   = max((compute_summary_similarity(summary, cs) for cs in case_summaries), default=0.0)
    party_sim = _party_similarity(parties, case_parties)
    combined  = party_sim * 0.70 + max_sim * 0.30

    if combined >= 0.88 and party_sim >= 0.65:
        return _route_for_role(role)
    if combined >= 0.75 and party_sim >= 0.55:
        return "add_to_case_reference"
    return "different_case_confirm"


def _route_for_role(role: str) -> str:
    if role == "primary":
        return "add_to_case_primary"
    if role == "previous_reply":
        return "add_to_case_reply"
    if role == "user_draft_reply":
        return "add_to_case_draft_reply"
    return "add_to_case_reference"


def _party_similarity(p1: dict, p2: dict) -> float:
    names_1 = [
        v for k, v in (p1 or {}).items()
        if k in ("sender", "recipient") and v and not _is_generic_party(v)
    ]
    names_2 = [
        v for k, v in (p2 or {}).items()
        if k in ("taxpayer_name", "authority") and v and not _is_generic_party(v)
    ]
    if not names_1 or not names_2:
        return 0.0
    from difflib import SequenceMatcher
    max_sim = 0.0
    for n1 in names_1:
        for n2 in names_2:
            s = SequenceMatcher(None, _normalise_party(n1), _normalise_party(n2)).ratio()
            max_sim = max(max_sim, s)
    return max_sim


# ═══════════════════════════════════════════════════════════════════
# TEMPORAL ROLE — cross-doc adjustment  (unchanged)
# ═══════════════════════════════════════════════════════════════════

def adjust_temporal_roles(
    docs_with_analysis: List[Tuple[dict, dict]],
    case: Optional[dict],
) -> None:
    from datetime import datetime as dt

    def _parse(date_str):
        if not date_str:
            return None
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%b-%Y", "%d %b %Y", "%B %d, %Y"):
            try:
                return dt.strptime(date_str.strip(), fmt).date()
            except Exception:
                pass
        return None

    primaries = [
        (analysis, _parse(analysis.get("date")))
        for _, analysis in docs_with_analysis
        if analysis.get("role") == "primary" and not analysis.get("temporal_locked")
    ]
    if case:
        for doc in case.get("docs", []):
            if doc.get("role") == "primary":
                primaries.append((doc, _parse(doc.get("date"))))

    dated = [(a, d) for a, d in primaries if d is not None]
    if not dated:
        return
    max_date = max(d for _, d in dated)
    for analysis, parsed in dated:
        if not analysis.get("temporal_locked"):
            analysis["temporal_role"] = "current" if parsed == max_date else "historical"


# ═══════════════════════════════════════════════════════════════════
# STEP 6a — Issue extraction  *** REWRITTEN ***
# ═══════════════════════════════════════════════════════════════════
#
# Changes vs original:
#   - max_tokens: 8192 → 16384 (single-call) / 6000 (per segment)
#   - Two-pass serial → segmented parallel (ThreadPoolExecutor, max_workers=4)
#   - Per-issue 800-word cap in prompt to prevent runaway verbatim output
#   - _extract_issues_once() kept as shim for backward compatibility
# ═══════════════════════════════════════════════════════════════════

_SEG_SIZE       = 25000   # chars per segment
_SEG_OVERLAP    = 3000    # overlap between adjacent segments
_SEG_MAX_TOK    = 6000    # max_tokens per segment call
_SINGLE_MAX_TOK = 16384   # max_tokens for docs that fit in one call

_ISSUE_SYSTEM = (
    "You are a legal issues extractor for Indian tax proceedings. "
    "Return ONLY valid JSON — no markdown, no explanation."
)

_ISSUE_PROMPT = """\
Extract all allegations, charges, and observations from this {segment_label}\
of a tax notice/order that require a formal reply.

DOCUMENT TEXT:
{doc_text}

ALREADY EXTRACTED ISSUES (do NOT re-extract — skip anything substantially similar):
{existing_issues}

MANDATORY RULES:
1. Extract VERBATIM — preserve exact wording, ALL amounts, ALL section numbers, \
ALL GSTINs, ALL periods, ALL rates.
2. Do NOT split. One issue = one allegation as framed by the authority.
3. Do NOT merge. Separate numbered paragraphs or clearly different allegations = \
separate issues.
4. Do NOT extract procedural text (reply-by dates, officer signatures, date headers, \
acknowledgments).
5. Only extract substantive allegations requiring a reply.
6. Per-issue size limit: keep each issue under 800 words. If an allegation is longer, \
preserve the core legal charge and all key figures (amounts, sections, periods); \
condense only narrative repetition.
7. If an annexure is referenced, include the total demand amount and tax period within \
the issue text.
8. Return an empty list if this segment contains no new allegations.

Return ONLY valid JSON:
{{
  "issues": [
    "Full verbatim text of issue 1 (under 800 words)...",
    "Full verbatim text of issue 2..."
  ]
}}"""

_REEXTRACT_PROMPT = """\
Re-read this {segment_label}very carefully. The user believes more issues exist.

DOCUMENT TEXT:
{doc_text}

PREVIOUSLY EXTRACTED ISSUES (do NOT re-extract these):
{existing_issues}

Pay special attention to:
- Numbered or lettered sub-paragraphs (1., 2., (a), (b), (i), (ii))
- Paragraphs beginning with 'Further', 'Additionally', 'It is also observed'
- Tables with multiple rows each containing separate demands
- Appendices or annexures listing additional amounts or periods
- Any allegation not covered in the previously extracted list

Extract ONLY the additional issues not already listed.
Per-issue size limit: 800 words.
Return empty list if no additional issues found.

Return ONLY valid JSON:
{{"issues": ["Additional issue text..."]}}"""


def _make_segments(text: str) -> List[str]:
    """Overlapping windows of _SEG_SIZE chars with _SEG_OVERLAP char overlap."""
    segments: List[str] = []
    pos = 0
    n   = len(text)
    while pos < n:
        end = min(pos + _SEG_SIZE, n)
        segments.append(text[pos:end])
        if end == n:
            break
        pos = end - _SEG_OVERLAP
    return segments


def _dedup_issues(candidates: List[str], already_seen: List[str]) -> List[str]:
    """
    Remove near-duplicates using 85 % SequenceMatcher threshold.
    Compares only the first 300 chars of each text for speed.
    """
    from difflib import SequenceMatcher
    seen   = list(already_seen)
    result = []
    for text in candidates:
        is_dup = any(
            SequenceMatcher(None, text[:300].lower(), s[:300].lower()).ratio() >= 0.85
            for s in seen
        )
        if not is_dup:
            result.append(text)
            seen.append(text)
    return result


def _extract_segment(
    segment_text: str,
    existing_texts: str,
    label: str = "step_6a_seg",
    segment_label: str = "segment ",
    max_tokens: int = _SEG_MAX_TOK,
) -> List[str]:
    """One LLM call for a single segment. Returns list of issue strings."""
    prompt = _ISSUE_PROMPT.format(
        doc_text       = segment_text,
        existing_issues = existing_texts,
        segment_label  = segment_label,
    )
    raw = _get_llm().call(
        system_prompt = _ISSUE_SYSTEM,
        user_message  = prompt,
        max_tokens    = max_tokens,
        temperature   = 0.0,
        label         = label,
    )
    parsed = _parse_json(raw, {"issues": []})
    issues = parsed.get("issues") or []
    if isinstance(issues, list):
        result = [str(i).strip() for i in issues if str(i).strip()]
        logger.debug(f"{label}: {len(result)} issues from {len(segment_text)} chars")
        return result
    return []


def extract_issues(full_text: str, existing_issues: List[dict]) -> List[str]:
    """
    Step 6a: Extract all new allegations from a primary document.

    Docs <= 80 000 chars  → single LLM call, max_tokens=16384
    Docs >  80 000 chars  → overlapping 25 000-char segments, all parallel via
                            ThreadPoolExecutor(max_workers=4), max_tokens=6000 each
                            Results merged in order and deduplicated at 85 %.
    """
    import concurrent.futures as _cf

    if not full_text or not full_text.strip():
        return []

    existing_texts = "\n".join(
        f"{i+1}. {iss.get('issue_text', '')[:200]}"
        for i, iss in enumerate(existing_issues)
    ) or "(none)"

    # ── Single-call path ──────────────────────────────────────────
    if len(full_text) <= 80000:
        prompt = _ISSUE_PROMPT.format(
            doc_text        = full_text,
            existing_issues = existing_texts,
            segment_label   = "document ",
        )
        raw = _get_llm().call(
            system_prompt = _ISSUE_SYSTEM,
            user_message  = prompt,
            max_tokens    = _SINGLE_MAX_TOK,
            temperature   = 0.0,
            label         = "step_6a_single",
        )
        parsed = _parse_json(raw, {"issues": []})
        issues = parsed.get("issues") or []
        result = [str(i).strip() for i in issues if str(i).strip()]
        logger.info(
            f"extract_issues (single): {len(result)} issues from {len(full_text)} chars"
        )
        return result

    # ── Segmented parallel path ───────────────────────────────────
    segments = _make_segments(full_text)
    logger.info(
        f"extract_issues (segmented): {len(full_text)} chars "
        f"→ {len(segments)} segments of ~{_SEG_SIZE} chars"
    )

    seg_results: Dict[int, List[str]] = {}
    with _cf.ThreadPoolExecutor(max_workers=min(len(segments), 4)) as ex:
        futs = {
            ex.submit(
                _extract_segment,
                seg,
                existing_texts,
                f"step_6a_seg{idx}",
                f"segment {idx + 1}/{len(segments)} ",
                _SEG_MAX_TOK,
            ): idx
            for idx, seg in enumerate(segments)
        }
        for fut in _cf.as_completed(futs):
            idx = futs[fut]
            try:
                seg_results[idx] = fut.result()
            except Exception as e:
                logger.error(f"Segment {idx} extraction failed: {e}", exc_info=True)
                seg_results[idx] = []

    # Merge in segment order
    all_candidates: List[str] = []
    for idx in sorted(seg_results.keys()):
        all_candidates.extend(seg_results[idx])

    existing_texts_list = [iss.get("issue_text", "") for iss in existing_issues]
    result = _dedup_issues(all_candidates, existing_texts_list)

    logger.info(
        f"extract_issues (segmented): {len(result)} unique issues "
        f"from {len(segments)} segments "
        f"(raw candidates before dedup: {len(all_candidates)})"
    )
    return result


def _extract_issues_once(text: str, existing_texts: str) -> List[str]:
    """
    Backward-compat shim. Delegates to _extract_segment / single-call path.
    Kept so any callers that weren't updated still work.
    """
    if len(text) <= 80000:
        prompt = _ISSUE_PROMPT.format(
            doc_text        = text,
            existing_issues = existing_texts,
            segment_label   = "document ",
        )
        raw = _get_llm().call(
            system_prompt = _ISSUE_SYSTEM,
            user_message  = prompt,
            max_tokens    = _SINGLE_MAX_TOK,
            temperature   = 0.0,
            label         = "step_6a_compat",
        )
        parsed = _parse_json(raw, {"issues": []})
        issues = parsed.get("issues") or []
        return [str(i).strip() for i in issues if str(i).strip()]
    return _extract_segment(text[:_SEG_SIZE], existing_texts, "step_6a_compat_seg")


# ═══════════════════════════════════════════════════════════════════
# STEP 6b — Replied-issue pair extraction  (unchanged)
# ═══════════════════════════════════════════════════════════════════

_REPLIED_SYSTEM = "You are a legal document parser. Return ONLY valid JSON."

_REPLIED_PROMPT = """This document is a reply/response to a tax notice. Extract all issue+reply pairs.

DOCUMENT TEXT:
{doc_text}

Return ONLY:
{{
  "replied_issues": [
    {{"issue_text": "The allegation as described...", "reply_text": "The reply/response given..."}},
    ...
  ]
}}"""


def extract_replied_issues(full_text: str) -> List[dict]:
    if not full_text or not full_text.strip():
        return []
    prompt = _REPLIED_PROMPT.format(doc_text=full_text[:80000])
    raw    = _get_llm().call(
        system_prompt = _REPLIED_SYSTEM,
        user_message  = prompt,
        max_tokens    = 8192,
        temperature   = 0.0,
        label         = "step_6b_replied",
    )
    parsed = _parse_json(raw, {"replied_issues": []})
    pairs  = parsed.get("replied_issues") or []
    if isinstance(pairs, list):
        return [
            {"issue_text": p.get("issue_text", ""), "reply_text": p.get("reply_text", "")}
            for p in pairs
            if isinstance(p, dict) and p.get("issue_text") and p.get("reply_text")
        ]
    return []


# ═══════════════════════════════════════════════════════════════════
# STEP 6 RE-EXTRACTION  *** REWRITTEN — segmented parallel ***
# ═══════════════════════════════════════════════════════════════════

def reextract_missed_issues(full_text: str, existing_issues: List[dict]) -> List[str]:
    """
    Enhanced re-extraction when user says issues were missed.
    Uses the same segmented parallel strategy as extract_issues()
    with a focused prompt that highlights common miss patterns.
    """
    import concurrent.futures as _cf

    if not full_text or not full_text.strip():
        return []

    existing_texts = "\n".join(
        f"{i+1}. {iss.get('issue_text', '')[:250]}"
        for i, iss in enumerate(existing_issues)
    ) or "(none)"

    # ── Single-call path ──────────────────────────────────────────
    if len(full_text) <= 80000:
        prompt = _REEXTRACT_PROMPT.format(
            doc_text        = full_text,
            existing_issues = existing_texts,
            segment_label   = "document ",
        )
        raw = _get_llm().call(
            system_prompt = _ISSUE_SYSTEM,
            user_message  = prompt,
            max_tokens    = _SINGLE_MAX_TOK,
            temperature   = 0.0,
            label         = "step_6_reextract_single",
        )
        parsed = _parse_json(raw, {"issues": []})
        issues = parsed.get("issues") or []
        result = [str(i).strip() for i in issues if str(i).strip()]
        logger.info(f"reextract_missed_issues (single): {len(result)} additional issues")
        return result

    # ── Segmented parallel path ───────────────────────────────────
    segments = _make_segments(full_text)
    logger.info(
        f"reextract_missed_issues (segmented): {len(segments)} segments "
        f"from {len(full_text)} chars"
    )

    def _reextract_one(seg: str, idx: int) -> List[str]:
        prompt = _REEXTRACT_PROMPT.format(
            doc_text        = seg,
            existing_issues = existing_texts,
            segment_label   = f"segment {idx + 1}/{len(segments)} ",
        )
        raw = _get_llm().call(
            system_prompt = _ISSUE_SYSTEM,
            user_message  = prompt,
            max_tokens    = _SEG_MAX_TOK,
            temperature   = 0.0,
            label         = f"step_6_reextract_seg{idx}",
        )
        parsed = _parse_json(raw, {"issues": []})
        issues = parsed.get("issues") or []
        return [str(i).strip() for i in issues if str(i).strip()]

    seg_results: Dict[int, List[str]] = {}
    with _cf.ThreadPoolExecutor(max_workers=min(len(segments), 4)) as ex:
        futs = {ex.submit(_reextract_one, seg, idx): idx for idx, seg in enumerate(segments)}
        for fut in _cf.as_completed(futs):
            idx = futs[fut]
            try:
                seg_results[idx] = fut.result()
            except Exception as e:
                logger.error(f"Reextract segment {idx} error: {e}", exc_info=True)
                seg_results[idx] = []

    all_candidates: List[str] = []
    for idx in sorted(seg_results.keys()):
        all_candidates.extend(seg_results[idx])

    existing_texts_list = [iss.get("issue_text", "") for iss in existing_issues]
    result = _dedup_issues(all_candidates, existing_texts_list)

    logger.info(
        f"reextract_missed_issues (segmented): {len(result)} additional issues "
        f"from {len(segments)} segments"
    )
    return result