"""
services/document/doc_classifier.py

Two main LLM calls per document (all in parallel across documents):

  Track 2A+2C  — combined single Qwen call:
    Input : full doc text (up to 60k chars) + user message + user_context
            + active case info (parties, reference_number for same_case check)
    Output: document metadata (binary classification primary|reference,
            legal_doc_type for display, parties, reference_number, date,
            brief_summary max 400 chars, has_issues bool,
            has_replied_issues bool, is_previous_reply, is_user_draft_reply,
            same_case, confidence)
            + intent (intent string, mode, issue_numbers, case_id)

  Track 2B  — separate parallel Qwen call + Stage2A regex (both in parallel):
    Input : full doc text (up to 60k chars)
    Output: Stage2BResult-compatible dict cached in snapshot for Step 8A

  Step 6 (called from api/document.py, separate from Track 2):
    extract_issues()        — if has_issues=True (verbatim allegations)
    extract_replied_issues() — if has_replied_issues=True (issue+reply pairs)
    Both run parallel per document and across documents.

Binary routing classification:
  primary   — has allegations/charges/demands requiring a formal legal reply
  reference — provides legal authority or supporting context

same_case priority:
  1. reference_number exact match → "yes"
  2. Non-generic taxpayer exact match → "yes"
  3. Similarity 0.90+ → "yes", 0.75–0.90 → "reference", < 0.75 → "unclear"
  Generic entities (courts, authorities) excluded from matching.
"""

import json
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
from typing import List, Optional

logger = logging.getLogger(__name__)

_TWO_PASS_THRESHOLD = 60_000   # chars sent to combined 2A+2C call
_FULL_THRESHOLD     = 80_000   # chars threshold for two-pass issue extraction
_SPLIT_OVERLAP      = 2_000

# ── Lazy LLM client ───────────────────────────────────────────────────────────
_llm      = None
_llm_lock = threading.Lock()

def _get_llm():
    global _llm
    if _llm is None:
        with _llm_lock:
            if _llm is None:
                from retrieval.bedrock_llm import BedrockLLMClient
                _llm = BedrockLLMClient()
    return _llm


# ── Generic entity whitelist (excluded from same_case party matching) ──────────
_GENERIC_PATTERNS = [
    "supreme court", "high court", "itat", "cestat", "gstat", "aar", "nclat",
    "cbdt", "cbic", "nclt", "drt", "tribunal",
    "commissioner", "joint commissioner", "deputy commissioner",
    "assistant commissioner", "superintendent", "inspector",
    "authority", "department", "ministry", "government",
    "union of india", "revenue", "state of", "officer",
]

def _is_generic(name: str) -> bool:
    n = name.lower().strip()
    return any(g in n for g in _GENERIC_PATTERNS)

def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()

def _same_case_deterministic(
    new_ref: str,
    new_sender: str,
    new_recipient: str,
    existing_case: dict,
) -> Optional[str]:
    """
    Deterministic same_case check. Returns "yes"|"reference"|"unclear"|None.
    None means indeterminate — fall back to LLM inference.
    """
    if not existing_case:
        return "no_existing_case"

    ex_parties = existing_case.get("parties") or {}
    ex_ref     = (existing_case.get("reference_number") or "").strip()
    ex_sender  = (ex_parties.get("sender") or "").strip()
    ex_recip   = (ex_parties.get("recipient") or "").strip()

    # 1. Reference number exact match
    if new_ref and ex_ref and new_ref.strip() == ex_ref:
        return "yes"

    # 2. Non-generic party matching
    best_sim = 0.0
    for new_p in [new_sender, new_recipient]:
        if not new_p or _is_generic(new_p):
            continue
        for ex_p in [ex_sender, ex_recip]:
            if not ex_p or _is_generic(ex_p):
                continue
            if new_p.lower().strip() == ex_p.lower().strip():
                return "yes"
            sim = _similarity(new_p, ex_p)
            best_sim = max(best_sim, sim)

    if best_sim >= 0.90:
        return "yes"
    if best_sim >= 0.75:
        return "reference"
    if best_sim > 0:
        return "unclear"

    return None  # No non-generic parties found to compare


# ── JSON parser ────────────────────────────────────────────────────────────────
def _parse_json(raw: Optional[str]) -> dict:
    if not raw:
        return {}
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
        logger.error(f"JSON parse error: {e}\nRaw: {raw[:200]}")
        return {}

def _opt(v):
    if v is None or str(v).lower() in ("null", "none", ""):
        return None
    return str(v).strip() or None

def _lst(v):
    if not v or not isinstance(v, list):
        return []
    return [str(x).strip() for x in v if x and str(x).strip()]


# ═══════════════════════════════════════════════════════════════════════════════
# TRACK 2A+2C — Combined metadata + intent
# ═══════════════════════════════════════════════════════════════════════════════

_COMBINED_SYSTEM = (
    "You are a legal document analyst and intent classifier for an Indian GST/Tax "
    "case management system. Return ONLY valid JSON — no explanation, no preamble."
)

_INTENT_OPTIONS = """Intent options (choose exactly one):
  summarize       — show/review document summary and issues list
  draft_all       — generate replies for ALL pending issues
  draft_specific  — generate reply for specific issue numbers mentioned
  draft_direct    — user explicitly says 'prepare reply / draft reply / reply to this notice'
  confirm_mode    — user confirming defensive or in-favour mode
  update_issues   — user wants to merge/add/correct/remove/reextract issues
  update_reply    — user wants to change the reply for a specific issue
  query_document  — question answerable from the uploaded document
  query_general   — pure GST/tax question, no document context needed
  switch_case     — user wants to work on a previously archived case
  new_case        — user explicitly starting fresh for a different matter
  mark_replied    — user says a doc/issue has already been replied to externally"""


def _build_combined_prompt(
    doc_text: str,
    user_message: str,
    user_context_text: str,
    active_case_snapshot: Optional[dict],
) -> str:
    existing_block = ""
    if active_case_snapshot:
        p = active_case_snapshot.get("parties") or {}
        existing_block = (
            "\nEXISTING ACTIVE CASE IN THIS SESSION:\n"
            f"  Sender: {p.get('sender') or 'unknown'}\n"
            f"  Recipient: {p.get('recipient') or 'unknown'}\n"
            f"  Reference: {active_case_snapshot.get('reference_number') or 'unknown'}\n"
        )

    user_signals = ""
    if user_message and user_message.strip():
        user_signals += (
            f"\nUSER'S CURRENT MESSAGE (highest priority — overrides document inference):\n"
            f"\"{user_message.strip()}\"\n"
        )
    if user_context_text:
        user_signals += (
            f"\nUSER'S ACCUMULATED INSTRUCTIONS FROM THIS SESSION:\n{user_context_text}\n"
        )

    return f"""Analyze this legal document. Then classify the user's intent.
{existing_block}{user_signals}
DOCUMENT TEXT:
{doc_text}

═══ PART 1: DOCUMENT METADATA ═══

CLASSIFICATION (BINARY — for routing):
  primary   — any document containing allegations/charges/demands/orders/findings
              that require a formal legal reply. Includes: notice, SCN, order,
              demand order, assessment order, any finding-based document.
  reference — provides legal authority or supporting context. Includes: judgment,
              circular, notification, rule text, previous reply already submitted,
              user's own prepared draft reply (is_user_draft_reply=true).

OVERRIDE: If user says "prepare reply for this" / "this is my notice" → primary.
OVERRIDE: If user says "this is reference" / "for reference only" → reference.

PARTIES: Extract ONLY if explicitly stated. Sender = who issued/signed.
Recipient = to whom addressed. DO NOT infer.

BRIEF SUMMARY: max 400 chars. MUST preserve ALL monetary amounts, section
numbers, GSTIN/PAN, notification numbers, assessment periods, demands, findings.
Dense entity-rich text.

BOOLEAN FLAGS:
  is_primary          = true if classification=primary
  is_previous_reply   = true if this doc IS a previously submitted reply/response
  is_user_draft_reply = true if user's message says this is THEIR OWN draft
                        (e.g. "my draft", "I prepared this", "check my reply")
  has_issues          = true if doc has allegations/charges/observations to respond to
                        (only relevant for primary docs)
  has_replied_issues  = true if doc contains BOTH the allegation AND a response
                        within itself (e.g. tribunal order with findings)

SAME_CASE (compare with existing active case shown above):
  "yes"              — same case (ref number match OR taxpayer match)
  "reference"        — related but different proceeding (partial party match)
  "no"               — clearly different parties on both sides
  "unclear"          — cannot determine from document content
  "no_existing_case" — no active case exists in this session

confidence: 0–100 (confidence in is_primary and same_case assessment)

═══ PART 2: INTENT CLASSIFICATION ═══
(Use your Part 1 metadata + the user message above to classify intent)

{_INTENT_OPTIONS}

mode: "defensive"|"in_favour"|null
  defensive = protecting the notice recipient / assessee / taxpayer
  in_favour = supporting the notice / department / revenue position

Return ONLY this JSON (no markdown, no explanation):
{{
  "classification": "primary",
  "legal_doc_type": "notice",
  "parties": {{"sender": null, "recipient": null}},
  "reference_number": null,
  "date": null,
  "brief_summary": "...",
  "is_primary": true,
  "is_previous_reply": false,
  "is_user_draft_reply": false,
  "has_issues": true,
  "has_replied_issues": false,
  "same_case": "no_existing_case",
  "confidence": 80,
  "intent": "summarize",
  "mode": null,
  "issue_numbers": [],
  "case_id": null
}}"""


def analyze_document(
    full_text: str,
    user_message: str = "",
    user_context_text: str = "",
    active_case_snapshot: Optional[dict] = None,
) -> dict:
    """
    Track 2A+2C: combined document metadata + intent classification.

    Sends first _TWO_PASS_THRESHOLD chars (60k) — sufficient for metadata
    from the header and early sections of any legal document.
    Issue extraction (Step 6) uses full text from DB separately.

    Returns dict with all metadata + intent fields.
    After LLM call, applies deterministic same_case override using
    party similarity logic.
    """
    if not full_text or not full_text.strip():
        return _empty_result()

    text_for_call = full_text[:_TWO_PASS_THRESHOLD]

    try:
        prompt = _build_combined_prompt(
            text_for_call, user_message, user_context_text, active_case_snapshot
        )
        raw = _get_llm().call(
            system_prompt=_COMBINED_SYSTEM,
            user_message=prompt,
            max_tokens=1024,
            temperature=0.0,
            label="doc_analyze_combined",
        )
        parsed = _parse_json(raw)
        result = _safe_parse_combined(parsed)

        # Apply deterministic same_case override
        if active_case_snapshot and result["same_case"] not in ("yes", "no_existing_case"):
            det = _same_case_deterministic(
                new_ref       = result.get("reference_number") or "",
                new_sender    = (result["parties"].get("sender") or ""),
                new_recipient = (result["parties"].get("recipient") or ""),
                existing_case = active_case_snapshot,
            )
            if det == "yes":
                result["same_case"] = "yes"
            elif det == "reference" and result["same_case"] == "unclear":
                result["same_case"] = "reference"
            elif det and det not in ("unclear", None):
                result["same_case"] = det

        return result

    except Exception as e:
        logger.error(f"analyze_document error: {e}", exc_info=True)
        return _empty_result(is_primary=True)


def _safe_parse_combined(raw: dict) -> dict:
    classification = _opt(raw.get("classification")) or "primary"
    legal_doc_type = _opt(raw.get("legal_doc_type")) or "other"
    is_primary     = bool(raw.get("is_primary", True))

    # Enforce: reference legal_doc_types → always reference classification
    _ref_types = {
        "judgment", "circular", "notification", "rule_text",
        "reference_material", "previous_reply", "user_draft_reply"
    }
    if legal_doc_type in _ref_types:
        is_primary     = False
        classification = "reference"

    return {
        "classification":      classification,
        "legal_doc_type":      legal_doc_type,
        "parties": {
            "sender":    _opt((raw.get("parties") or {}).get("sender")),
            "recipient": _opt((raw.get("parties") or {}).get("recipient")),
        },
        "reference_number":    _opt(raw.get("reference_number")),
        "date":                _opt(raw.get("date")),
        "brief_summary":       _opt(raw.get("brief_summary")) or "",
        "is_primary":          is_primary,
        "is_previous_reply":   bool(raw.get("is_previous_reply", False)),
        "is_user_draft_reply": bool(raw.get("is_user_draft_reply", False)),
        "has_issues":          bool(raw.get("has_issues", True)) and is_primary,
        "has_replied_issues":  bool(raw.get("has_replied_issues", False)),
        "same_case":           _opt(raw.get("same_case")) or "unclear",
        "confidence":          max(0, min(100, int(raw.get("confidence", 70)))),
        "intent":              _opt(raw.get("intent")) or "summarize",
        "mode":                _opt(raw.get("mode")),
        "issue_numbers":       _lst(raw.get("issue_numbers")),
        "case_id":             raw.get("case_id"),
    }


def _empty_result(is_primary: bool = False) -> dict:
    return {
        "classification": "primary" if is_primary else "reference",
        "legal_doc_type": "other",
        "parties": {"sender": None, "recipient": None},
        "reference_number": None, "date": None, "brief_summary": "",
        "is_primary": is_primary, "is_previous_reply": False,
        "is_user_draft_reply": False,
        "has_issues": is_primary, "has_replied_issues": False,
        "same_case": "no_existing_case", "confidence": 0,
        "intent": "summarize", "mode": None,
        "issue_numbers": [], "case_id": None,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TRACK 2B — Legal entity extraction (parallel with 2A+2C, independent)
# ═══════════════════════════════════════════════════════════════════════════════

_ENTITY_SYSTEM = (
    "You are a precise legal entity extractor for an Indian GST/tax system. "
    "Return ONLY valid JSON — no preamble, no explanation."
)

_ENTITY_PROMPT_TMPL = """Extract all legal references from this document.

DOCUMENT:
{text}

Return ONLY this JSON:
{{
  "sections": [], "rules": [], "notifications": [], "circulars": [],
  "acts": [], "keywords": [], "topics": [],
  "form_name": null, "form_number": null,
  "case_name": null, "parties": [], "person_names": [],
  "case_number": null, "court": null, "court_level": null,
  "citation": null, "decision_type": null,
  "hsn_code": null, "sac_code": null, "issued_by": null
}}

court_level: "HC"|"SC"|"ITAT"|"CESTAT"|"GSTAT"|"AAR"|"Other"|null
decision_type: "in_favour_of_assessee"|"in_favour_of_revenue"|"remanded"|"dismissed"|null"""


def extract_legal_entities(full_text: str) -> dict:
    """
    Track 2B: Extract legal entities from document text.
    Two sub-tracks run in parallel:
      - Qwen LLM: structured JSON extraction (sections, rules, notifications, etc.)
      - Stage2A regex: normalised tokens for BM25 (section_17a, rule_89, etc.)

    Returns merged dict for building Stage2BResult and BM25 keyword document.
    Runs independent of Track 2A+2C — no shared data needed.
    """
    if not full_text or not full_text.strip():
        return _empty_entities()

    def _llm_extract():
        raw = _get_llm().call(
            system_prompt=_ENTITY_SYSTEM,
            user_message=_ENTITY_PROMPT_TMPL.format(text=full_text[:_TWO_PASS_THRESHOLD]),
            max_tokens=1024,
            temperature=0.0,
            label="doc_entity_extract",
        )
        return _parse_json(raw)

    def _regex_extract():
        try:
            from pipeline.regex_fallback import extract_fallback
            result = extract_fallback(full_text)
            normalised = []
            for field in ("sections", "rules", "notifications", "circulars"):
                normalised.extend(result.get(field, []))
            return {
                "normalised_tokens": normalised,
                "raw_tokens":        result.get("topics", []),
                "citation":          _extract_citation_regex(full_text),
            }
        except Exception as e:
            logger.debug(f"Regex extraction failed: {e}")
            return {"normalised_tokens": [], "raw_tokens": [], "citation": None}

    llm_result, regex_result = {}, {}
    with ThreadPoolExecutor(max_workers=2) as ex:
        fut_llm   = ex.submit(_llm_extract)
        fut_regex = ex.submit(_regex_extract)
        try:
            llm_result = fut_llm.result()
        except Exception as e:
            logger.error(f"Entity LLM failed: {e}")
        try:
            regex_result = fut_regex.result()
        except Exception as e:
            logger.error(f"Entity regex failed: {e}")

    # Regex citation is more reliable for taxo.online/MANU patterns
    citation = regex_result.get("citation") or _opt(llm_result.get("citation"))

    return {
        "sections":          _lst(llm_result.get("sections")),
        "rules":             _lst(llm_result.get("rules")),
        "notifications":     _lst(llm_result.get("notifications")),
        "circulars":         _lst(llm_result.get("circulars")),
        "acts":              _lst(llm_result.get("acts")),
        "keywords":          _lst(llm_result.get("keywords")),
        "topics":            _lst(llm_result.get("topics")),
        "form_name":         _opt(llm_result.get("form_name")),
        "form_number":       _opt(llm_result.get("form_number")),
        "case_name":         _opt(llm_result.get("case_name")),
        "parties":           _lst(llm_result.get("parties")),
        "person_names":      _lst(llm_result.get("person_names")),
        "case_number":       _opt(llm_result.get("case_number")),
        "court":             _opt(llm_result.get("court")),
        "court_level":       _opt(llm_result.get("court_level")),
        "citation":          citation,
        "decision_type":     _opt(llm_result.get("decision_type")),
        "hsn_code":          _opt(llm_result.get("hsn_code")),
        "sac_code":          _opt(llm_result.get("sac_code")),
        "issued_by":         _opt(llm_result.get("issued_by")),
        "normalised_tokens": regex_result.get("normalised_tokens", []),
        "raw_tokens":        regex_result.get("raw_tokens", []),
    }


def _empty_entities() -> dict:
    return {
        "sections":[], "rules":[], "notifications":[], "circulars":[],
        "acts":[], "keywords":[], "topics":[], "form_name":None,
        "form_number":None, "case_name":None, "parties":[], "person_names":[],
        "case_number":None, "court":None, "court_level":None, "citation":None,
        "decision_type":None, "hsn_code":None, "sac_code":None, "issued_by":None,
        "normalised_tokens":[], "raw_tokens":[],
    }


def _extract_citation_regex(text: str) -> Optional[str]:
    m = re.search(r"MANU/[A-Z]+/\d+/\d+", text, re.IGNORECASE)
    if m: return m.group(0).upper()
    m = re.search(r"(\d{4})\s+SCC\s+Online\s+(\d+)", text, re.IGNORECASE)
    if m: return f"{m.group(1)} SCC Online {m.group(2)}"
    m = re.search(r"(\d{2,4})\s+taxo[\s.\-]?online\s+(\d+)", text, re.IGNORECASE)
    if m:
        yr = m.group(1)
        year = yr if len(yr)==4 else (f"20{yr}" if int(yr)<50 else f"19{yr}")
        return f"{year} Taxo.online {m.group(2)}"
    m = re.search(r"taxo[\s.\-]?(?:online\s+)?(\d+)(?!\d)", text, re.IGNORECASE)
    if m: return f"Taxo.online {m.group(1)}"
    return None


def entities_to_stage2b_result(entities: dict):
    """Convert extract_legal_entities() output to Stage2BResult for Step 8A."""
    from retrieval.models import Stage2BResult
    return Stage2BResult(
        sections      = entities.get("sections", []),
        rules         = entities.get("rules", []),
        notifications = entities.get("notifications", []),
        circulars     = entities.get("circulars", []),
        acts          = entities.get("acts", []),
        keywords      = entities.get("keywords", []),
        topics        = entities.get("topics", []),
        form_name     = entities.get("form_name"),
        form_number   = entities.get("form_number"),
        case_name     = entities.get("case_name"),
        parties       = entities.get("parties", []),
        person_names  = entities.get("person_names", []),
        case_number   = entities.get("case_number"),
        court         = entities.get("court"),
        court_level   = entities.get("court_level"),
        citation      = entities.get("citation"),
        decision_type = entities.get("decision_type"),
        hsn_code      = entities.get("hsn_code"),
        sac_code      = entities.get("sac_code"),
        issued_by     = entities.get("issued_by"),
    )


def build_bm25_doc_from_entities(entities: dict) -> str:
    """Build BM25 keyword document from extract_legal_entities() output."""
    try:
        from utils.normalizer import universal_normalise, whitespace_split_normalise
    except ImportError:
        return ""

    parts = list(entities.get("normalised_tokens", []))
    parts += list(entities.get("raw_tokens", []))
    l1 = []
    for v in entities.get("sections", []):
        t_ = universal_normalise(v)
        if t_: l1.append(t_)
    for v in entities.get("rules", []):
        t_ = universal_normalise(v)
        if t_: l1.append(t_)
    for v in entities.get("notifications", []):
        t_ = universal_normalise(v)
        if t_: l1.append(t_)
    for v in entities.get("circulars", []):
        t_ = universal_normalise(v)
        if t_: l1.append(t_)
    for vals in (entities.get("acts",[]), entities.get("keywords",[]), entities.get("topics",[])):
        for v in vals:
            l1 += whitespace_split_normalise(v)
    for v in (entities.get("form_name"), entities.get("case_name"),
              entities.get("court"), entities.get("citation"), entities.get("issued_by")):
        if v: l1 += whitespace_split_normalise(v)
    for v in (entities.get("hsn_code"), entities.get("sac_code")):
        if v: l1.append(v)
    if entities.get("court_level"):
        l1.append(entities["court_level"])
    for name in entities.get("parties",[]) + entities.get("person_names",[]):
        l1 += whitespace_split_normalise(name)
    parts += l1 * 3
    return " ".join(t_ for t_ in parts if t_)


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6 — Issue extraction (dedicated focused calls)
# ═══════════════════════════════════════════════════════════════════════════════

_ISSUE_SYSTEM = "You are extracting legal issues from an Indian tax document. Return ONLY valid JSON."

_ISSUE_INSTRUCTION = """
Extract every issue, allegation, discrepancy, observation, ground, or charge.
RULES:
- Verbatim — word for word, no paraphrasing or summarising.
- Each issue MUST include ALL details in its context: exact amounts, tax periods,
  section/rule/notification numbers, GSTINs, invoice numbers, percentages, rates.
- Each issue must be self-contained — someone reading only that issue must have
  every fact needed to respond to it without referring back to the document.
- Do NOT filter — include every issue including minor observations.
- If no issues exist → return issues: []
"""


def extract_issues(full_text: str, existing_issues: list = None) -> list:
    """
    Step 6: Extract verbatim issues/allegations from primary document text.
    Only called when has_issues=True from Track 2A+2C.
    Two-pass for documents > _FULL_THRESHOLD chars.
    """
    if not full_text or not full_text.strip():
        return []

    existing_preview = ""
    if existing_issues:
        existing_preview = "Already extracted (do NOT repeat):\n" + "\n".join(
            f"{i+1}. {iss['text'][:120]}" for i, iss in enumerate(existing_issues[:20])
        )

    def _run(chunk: str) -> list:
        prompt = (
            f"Extract all issues from this legal document.\n\nDOCUMENT:\n{chunk}\n\n"
            f"{_ISSUE_INSTRUCTION}\n{existing_preview}\n\n"
            'Return ONLY: {"issues": ["verbatim issue with all entities", ...] or []}'
        )
        raw = _get_llm().call(
            system_prompt=_ISSUE_SYSTEM, user_message=prompt,
            max_tokens=4096, temperature=0.0, label="issue_extract",
        )
        parsed = _parse_json(raw)
        return [str(i) for i in (parsed.get("issues") or []) if i]

    if len(full_text) <= _FULL_THRESHOLD:
        return _dedup_issues(_run(full_text))

    mid = len(full_text) // 2
    nl  = full_text.rfind("\n", mid - 500, mid + 500)
    if nl != -1: mid = nl
    first  = _run(full_text[:mid + _SPLIT_OVERLAP])
    second = _run(full_text[mid - _SPLIT_OVERLAP:])
    return _dedup_issues(first + second)


def extract_replied_issues(full_text: str) -> list:
    """
    Step 6: Extract issue+reply pairs from previous_reply / tribunal order.
    Only called when has_replied_issues=True from Track 2A+2C.
    Returns list of {issue_text, reply_text} dicts.
    """
    if not full_text or not full_text.strip():
        return []
    prompt = (
        f"Extract allegation-response pairs from this document.\n\nDOCUMENT:\n"
        f"{full_text[:_TWO_PASS_THRESHOLD]}\n\n"
        "Extract pairs where both the allegation AND a response/finding appear in the same document.\n"
        "Return ONLY: {\"replied_issues\": [{\"issue_text\": \"...\", \"reply_text\": \"...\"}] or []}"
    )
    raw = _get_llm().call(
        system_prompt=_ISSUE_SYSTEM, user_message=prompt,
        max_tokens=4096, temperature=0.0, label="replied_issue_extract",
    )
    parsed = _parse_json(raw)
    return [
        {"issue_text": str(r.get("issue_text","")), "reply_text": str(r.get("reply_text",""))}
        for r in (parsed.get("replied_issues") or []) if r.get("issue_text")
    ]


def reextract_missed_issues(full_text: str, existing_issues: list) -> list:
    """Case 5: Re-read full document to find missed issues."""
    return extract_issues(full_text, existing_issues=existing_issues)


def _dedup_issues(issues: list) -> list:
    seen, unique = [], []
    for issue in issues:
        n = re.sub(r"\s+", " ", issue.lower().strip())
        dup = False
        for s in seen:
            shorter = min(len(n), len(s))
            if shorter == 0: continue
            if n in s or s in n: dup = True; break
            if sum(1 for a,b in zip(n,s) if a==b) / shorter > 0.85: dup = True; break
        if not dup:
            seen.append(n)
            unique.append(issue)
    return unique


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTING — pure logic, no LLM
# ═══════════════════════════════════════════════════════════════════════════════

def determine_route(
    analysis: dict,
    has_existing_case: bool,
    user_said_new_case: bool = False,
    user_said_reference: bool = False,
) -> str:
    """
    Determine routing for one document. Pure logic, no LLM.
    Returns:
      new_case_primary | add_to_case_primary | add_to_case_reference |
      new_case_reference | mark_primary_replied | different_parties | needs_confirmation
    """
    if user_said_new_case:
        return "new_case_primary" if analysis.get("is_primary") else "new_case_reference"

    if user_said_reference:
        return "add_to_case_reference" if has_existing_case else "new_case_reference"

    if analysis.get("is_previous_reply") or analysis.get("is_user_draft_reply"):
        return "mark_primary_replied"

    classification = analysis.get("classification", "primary")
    is_primary     = analysis.get("is_primary", True)
    same_case      = analysis.get("same_case", "unclear")
    confidence     = analysis.get("confidence", 70)

    # Reference classification → always reference routing
    if classification == "reference":
        return "add_to_case_reference" if has_existing_case else "new_case_reference"

    if not has_existing_case:
        return "new_case_primary" if is_primary else "new_case_reference"

    # Existing case present
    if same_case == "yes":
        return "add_to_case_primary" if is_primary else "add_to_case_reference"

    if same_case == "reference":
        return "add_to_case_reference"

    if same_case == "no":
        return "different_parties" if is_primary else "add_to_case_reference"

    # "unclear"
    if confidence < 70:
        return "needs_confirmation"

    return "add_to_case_primary" if is_primary else "add_to_case_reference"