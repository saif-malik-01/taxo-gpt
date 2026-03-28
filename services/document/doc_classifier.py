"""
services/document/doc_classifier.py

Single comprehensive LLM call per document that extracts:
  - Metadata (parties, reference_number, date, brief_summary, legal_doc_type)
  - Role classification (primary / reference / previous_reply)
  - Relationship to existing case in this session
  - Issues (if is_primary=True, verbatim with all legal entities)
  - Replied issues pairs (if is_previous_reply=True)

Design decisions:
  - No filename sent to LLM (not legally meaningful)
  - No keyword/regex based classification — fully LLM
  - User's current message + snapshot user_context are the highest-priority
    signals for role classification (user's words override LLM inference)
  - For long documents (>80k chars) the analysis runs two-pass: first half
    and second half. Metadata comes from first pass; issues are merged.
  - brief_summary: max 400 chars, ALL legal entities (amounts, sections,
    GSTINs, periods, reference numbers) must be preserved verbatim.
"""

import json
import logging
import re
import threading
from typing import Optional

logger = logging.getLogger(__name__)

_TWO_PASS_THRESHOLD = 80_000
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


# ── Prompt builder ─────────────────────────────────────────────────────────────

_SYSTEM = (
    "You are a legal document analyst for an Indian GST/Tax case management system. "
    "Extract information ONLY from the document text provided. "
    "Return ONLY valid JSON — no explanation, no preamble."
)


def _build_prompt(
    doc_text: str,
    user_message: str,
    user_context_text: str,
    active_case_snapshot: Optional[dict],
    extract_issues: bool,
    already_found_issues: Optional[list] = None,
) -> str:
    """
    Build the comprehensive analysis prompt.

    extract_issues=True  → full document text, extract issues section
    extract_issues=False → metadata-only pass (first chunk of long doc)
    already_found_issues → when set, second-pass mode: find ONLY issues
                           not already in this list (prevents duplicates)
    """
    # Existing case context for relationship assessment
    existing_block = ""
    if active_case_snapshot:
        p   = active_case_snapshot.get("parties", {})
        ref = active_case_snapshot.get("reference_number") or "unknown"
        dt  = active_case_snapshot.get("legal_doc_type") or "unknown"
        existing_block = (
            "\nEXISTING ACTIVE CASE IN THIS SESSION:\n"
            f"  Type: {dt}\n"
            f"  Sender: {p.get('sender') or 'unknown'}\n"
            f"  Recipient: {p.get('recipient') or 'unknown'}\n"
            f"  Reference: {ref}\n"
        )

    user_signals = ""
    if user_message and user_message.strip():
        user_signals += f"\nUSER'S CURRENT MESSAGE:\n\"{user_message.strip()}\"\n"
    if user_context_text:
        user_signals += f"\nUSER'S PREVIOUS INSTRUCTIONS IN THIS SESSION:\n{user_context_text}\n"

    issues_instruction = ""
    if extract_issues:
        if already_found_issues:
            # Second-pass: only NEW issues not already in the list
            already_block = "\n".join(
                f"  {i+1}. {iss[:120]}" for i, iss in enumerate(already_found_issues[:30])
            )
            issues_instruction = f"""
SECTION 5 — ISSUES EXTRACTION (NEW ONLY — second pass):
The following issues were already extracted from the first half of this document.
Do NOT repeat them. Find only issues that are GENUINELY NEW — not rephrasing of these.

ALREADY EXTRACTED (do NOT repeat):
{already_block}

RULES FOR NEW ISSUES:
- Verbatim — word for word, no paraphrasing.
- Each issue MUST include ALL details: exact amounts, tax periods,
  section/rule/notification numbers, GSTINs, invoice numbers, percentages.
- If the same para appears in both a table summary and a detailed elaboration,
  keep ONLY the more detailed version. Do NOT add the table-row version if the
  detailed version is already in the ALREADY EXTRACTED list above.
- EXCLUDE: paras marked "settled" / "completely settled", demand-aggregation
  summary paras, generic interest/penalty recitations, procedural/legal-provision
  paras, directions to appear for hearing.
- If ALL issues in this section were already captured above → set issues to [].
- For previous_reply documents: extract NEW replied pairs only.

"""
        else:
            issues_instruction = """
SECTION 5 — ISSUES EXTRACTION (verbatim, all entities):
Extract every issue, allegation, discrepancy, observation, ground, or charge that
REQUIRES A SUBSTANTIVE LEGAL REPLY from the noticee/taxpayer.

INCLUDE:
- Specific factual allegations (e.g. excess ITC availed, short payment of GST, non-reversal).
- Each issue MUST include ALL details in context: exact amounts, tax periods,
  section/rule/notification numbers, GSTINs, invoice numbers, percentages.
- If the same para appears twice (once as a summary row in a table and again as a
  detailed elaboration), extract ONLY the more detailed version — do NOT include both.

EXCLUDE — do NOT extract these as issues:
- Any para or item explicitly marked as "settled", "completely settled", "has been settled",
  "para is completely settled", or where the noticee has already deposited the full amount.
- Demand-summary / aggregation paras that merely re-state totals of earlier paras
  (e.g. "Total demand of Rs. X as detailed in Para 1, 2, 4…").
- Interest/penalty demands under Section 50 / Section 74 stated generically
  ("interest and penalty as applicable") without a specific new factual allegation.
- Procedural or legal-provision recitation paras (grounds for invoking Section 74,
  extended limitation grounds, legal provisions, non-appropriation requests).
- Directions to the noticee to appear for personal hearing or submit reply.

If no issues remain after filtering → set issues to [].
For previous_reply documents: extract pairs of {issue_text, reply_text}.
  issue_text = the allegation being addressed, reply_text = the response given.
  Set replied_issues to [{"issue_text": "...", "reply_text": "..."}].
  Set issues to [].

"""
    else:
        issues_instruction = "\nSECTION 5 — ISSUES: Do not extract issues in this pass (metadata only). Set issues to [].\n"

    prompt = f"""Analyze the legal document below.
{existing_block}{user_signals}
DOCUMENT TEXT:
{doc_text}

INSTRUCTIONS:

SECTION 1 — LEGAL DOCUMENT TYPE:
Classify into one of:
  notice | show_cause_notice | order | demand_order | assessment_order |
  previous_reply | judgment | circular | notification | rule_text |
  reference_material | other

SECTION 2 — PARTIES:
Extract sender and recipient ONLY if explicitly stated in the document.
Sender = who issued/signed/sent it.
Recipient = to whom it is addressed.
Do NOT infer. Set null if not explicitly present.

SECTION 3 — METADATA:
  reference_number: notice/order/case number as it appears in the document
  date: document date as it appears (preserve original format)
  brief_summary: max 400 chars. MUST preserve ALL: monetary amounts,
    section numbers, GSTIN/PAN, notification numbers, assessment periods,
    demands, findings. Write dense, entity-rich text.

SECTION 4 — ROLE CLASSIFICATION:
  is_primary: true if this document has allegations/charges/demands/findings
              that require a formal legal reply or response.
              Signals: notice, SCN, order, demand, assessment order,
              allegations, "you are directed to", "show cause why", etc.
  is_previous_reply: true if this document IS a reply/response/rejoinder
              that was previously submitted to a notice or order.
              Signals: "In response to", "reply to notice", "we submit",
              "on behalf of", "grounds of appeal", "our response".
  same_case:  "yes" | "no" | "unclear"
              Compare against EXISTING ACTIVE CASE above (if any).
              "yes": same parties (sender/recipient match) OR same reference_number.
              "no": clearly different parties on both sender AND recipient sides.
              "unclear": only partial match or cannot determine.
              "no_existing_case": if no active case exists in session.
  confidence: 0-100 (your confidence in is_primary and same_case assessments)
{issues_instruction}
Return ONLY this JSON (no markdown, no preamble):
{{
  "legal_doc_type": "...",
  "parties": {{"sender": null, "recipient": null}},
  "reference_number": null,
  "date": null,
  "brief_summary": "...",
  "is_primary": true,
  "is_previous_reply": false,
  "same_case": "no_existing_case",
  "confidence": 80,
  "issues": [],
  "replied_issues": []
}}"""
    return prompt


# ── Core analysis function ─────────────────────────────────────────────────────

def _run_analysis_call(
    text: str,
    user_message: str,
    user_context_text: str,
    active_case_snapshot: Optional[dict],
    extract_issues: bool,
    already_found_issues: Optional[list] = None,
) -> dict:
    """Single LLM call. Returns raw parsed dict."""
    prompt = _build_prompt(
        text, user_message, user_context_text,
        active_case_snapshot, extract_issues,
        already_found_issues=already_found_issues,
    )
    raw = _get_llm().call(
        system_prompt=_SYSTEM,
        user_message=prompt,
        max_tokens=8192 if extract_issues else 1024,  # 4096→8192: verbatim issues need room
        temperature=0.0,
        label="doc_classifier",
    )
    if not raw:
        raise ValueError("Empty LLM response from doc_classifier")

    # Extract JSON (handle markdown fences)
    text_clean = raw.strip()
    if text_clean.startswith("```"):
        text_clean = re.sub(r"^```[a-z]*\n?", "", text_clean)
        text_clean = re.sub(r"\n?```$", "", text_clean)
    m = re.search(r"\{.*\}", text_clean, re.DOTALL)
    if m:
        text_clean = m.group()
    return json.loads(text_clean)


def _safe_parse(raw: dict) -> dict:
    """Normalise and validate the LLM output dict."""
    def _str(v):
        if not v or str(v).lower() in ("null", "none", ""):
            return None
        return str(v).strip()

    return {
        "legal_doc_type":   _str(raw.get("legal_doc_type")) or "other",
        "parties": {
            "sender":    _str((raw.get("parties") or {}).get("sender")),
            "recipient": _str((raw.get("parties") or {}).get("recipient")),
        },
        "reference_number": _str(raw.get("reference_number")),
        "date":             _str(raw.get("date")),
        "brief_summary":    _str(raw.get("brief_summary")) or "",
        "is_primary":       bool(raw.get("is_primary", True)),
        "is_previous_reply": bool(raw.get("is_previous_reply", False)),
        "same_case":        _str(raw.get("same_case")) or "unclear",
        "confidence":       max(0, min(100, int(raw.get("confidence", 70)))),
        "issues":           [str(i) for i in (raw.get("issues") or []) if i],
        "replied_issues":   [
            {"issue_text": str(r.get("issue_text", "")),
             "reply_text":  str(r.get("reply_text", ""))}
            for r in (raw.get("replied_issues") or [])
            if r.get("issue_text")
        ],
    }


# ── Two-pass deduplication ────────────────────────────────────────────────────

def _issue_tokens(text: str) -> set:
    """Normalise and return word-token set for Jaccard comparison."""
    return set(re.findall(r'\b\w+\b', re.sub(r'\s+', ' ', text.lower().strip())))


def _jaccard(a: str, b: str) -> float:
    """Word-level Jaccard similarity between two strings."""
    ta, tb = _issue_tokens(a), _issue_tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _dedup_issues(issues: list) -> list:
    """
    Deduplicate using Jaccard word-token similarity (threshold 0.50).

    Why Jaccard instead of the old positional-char zip:
      zip(n, s) only counts characters at the SAME index — two strings that
      share content but differ in prefix score near zero.  Jaccard counts
      shared word tokens regardless of order, so "Excess availment of ITC …
      Rs. 1,41,891" and "Excess ITC availment … Amount: Rs. 1,41,891/-"
      correctly score ~0.72 and are flagged as duplicates.

    Threshold 0.50: catches near-verbatim repeats, table-row summaries vs their
    detailed elaboration later in the document, and paraphrases with the same
    key amounts/section numbers, while keeping genuinely distinct issues.
    Previously 0.60 — lowered because SCN-style documents repeat each para
    twice (once as a gist row in Table A, once as a detailed numbered section)
    and the two forms score ~0.55 Jaccard.
    """
    seen: list[str] = []
    unique: list[str] = []
    for issue in issues:
        n = re.sub(r'\s+', ' ', issue.lower().strip())
        is_dup = False
        for idx, s in enumerate(seen):
            if n in s or s in n or _jaccard(issue, s) >= 0.50:
                is_dup = True
                # Replace the stored version if this one is longer (more detailed)
                if len(issue) > len(unique[idx]):
                    seen[idx]   = n
                    unique[idx] = issue
                break
        if not is_dup:
            seen.append(n)
            unique.append(issue)
    return unique


# ── Public API ────────────────────────────────────────────────────────────────

def analyze_document(
    full_text: str,
    user_message: str = "",
    user_context_text: str = "",
    active_case_snapshot: Optional[dict] = None,
) -> dict:
    """
    Comprehensive single-document analysis.

    Returns dict with:
      legal_doc_type, parties, reference_number, date, brief_summary,
      is_primary, is_previous_reply, same_case, confidence,
      issues (verbatim, all entities), replied_issues

    For long documents (>80k chars): two-pass.
      Pass 1 (first half): metadata + is_primary determination + first half issues
      Pass 2 (second half): additional issues only
      Metadata comes from pass 1; issues from both passes are merged and deduped.
    """
    if not full_text or not full_text.strip():
        return {
            "legal_doc_type": "other", "parties": {"sender": None, "recipient": None},
            "reference_number": None, "date": None, "brief_summary": "",
            "is_primary": False, "is_previous_reply": False,
            "same_case": "no_existing_case", "confidence": 0,
            "issues": [], "replied_issues": [],
        }

    try:
        if len(full_text) <= _TWO_PASS_THRESHOLD:
            # Short document: single comprehensive call
            raw = _run_analysis_call(
                full_text, user_message, user_context_text,
                active_case_snapshot, extract_issues=True,
            )
            return _safe_parse(raw)

        # Long document: two-pass
        mid = len(full_text) // 2
        nl  = full_text.rfind("\n", mid - 500, mid + 500)
        if nl != -1:
            mid = nl

        first_half  = full_text[:mid + _SPLIT_OVERLAP]
        second_half = full_text[mid - _SPLIT_OVERLAP:]

        # Pass 1: full analysis on first half (metadata + issues)
        raw1 = _run_analysis_call(
            first_half, user_message, user_context_text,
            active_case_snapshot, extract_issues=True,
        )
        result = _safe_parse(raw1)

        # Pass 2: second half — find ONLY issues NOT already in pass 1.
        # Giving pass-1 issues to the LLM prevents it from re-extracting
        # the same issues it can see in the overlap zone.
        raw2 = _run_analysis_call(
            second_half, user_message, user_context_text,
            active_case_snapshot, extract_issues=True,
            already_found_issues=result["issues"] if result["issues"] else None,
        )
        parsed2 = _safe_parse(raw2)

        # Merge issues from both passes
        all_issues = result["issues"] + parsed2["issues"]
        result["issues"] = _dedup_issues(all_issues)

        # Merge replied_issues
        all_replied = result["replied_issues"] + parsed2.get("replied_issues", [])
        # Dedup replied_issues by issue_text
        seen_itexts = set()
        deduped_replied = []
        for r in all_replied:
            key = r["issue_text"][:80].lower()
            if key not in seen_itexts:
                seen_itexts.add(key)
                deduped_replied.append(r)
        result["replied_issues"] = deduped_replied

        return result

    except Exception as e:
        logger.error(f"analyze_document error: {e}", exc_info=True)
        return {
            "legal_doc_type": "other", "parties": {"sender": None, "recipient": None},
            "reference_number": None, "date": None, "brief_summary": "",
            "is_primary": True,  # default to primary to avoid silent data loss
            "is_previous_reply": False,
            "same_case": "unclear", "confidence": 0,
            "issues": [], "replied_issues": [],
        }


def reextract_missed_issues(full_text: str, existing_issues: list) -> list:
    """
    Re-read the full document to find issues missed in initial extraction.
    Returns list of NEW issue texts only.
    """
    if not full_text or not full_text.strip():
        return []

    existing_preview = "\n".join(
        f"{i+1}. {iss['text'][:120]}"
        for i, iss in enumerate(existing_issues[:20])
    )

    system = (
        "You are re-analyzing a legal document to find missed issues. "
        "Return ONLY valid JSON."
    )
    prompt = f"""Re-read this document and find issues NOT already extracted.

DOCUMENT:
{full_text[:_TWO_PASS_THRESHOLD]}

ALREADY EXTRACTED ISSUES (do NOT repeat):
{existing_preview or "None"}

Find any allegations, charges, observations, discrepancies that are genuinely
NEW (not a rephrasing of the above). Extract verbatim with all legal entities.

Return ONLY:
{{
    "new_issues": ["verbatim new issue with all entities", ...] or []
}}"""

    try:
        raw = _get_llm().call(
            system_prompt=system, user_message=prompt,
            max_tokens=2048, temperature=0.0, label="reextract_issues"
        )
        if not raw:
            return []
        m = re.search(r"\{.*\}", raw.strip(), re.DOTALL)
        if m:
            parsed = json.loads(m.group())
            return [str(i) for i in (parsed.get("new_issues") or []) if i]
    except Exception as e:
        logger.error(f"reextract_missed_issues error: {e}")
    return []


# ── Routing logic (pure, no LLM) ─────────────────────────────────────────────

def determine_route(
    analysis: dict,
    has_existing_case: bool,
    user_said_new_case: bool = False,
    user_said_reference: bool = False,
) -> str:
    """
    Determine routing for one document. Pure logic, no LLM.

    Returns:
      "new_case_primary"         — create new case, this is the primary doc
      "add_to_case_primary"      — add primary doc to existing active case
      "add_to_case_reference"    — add reference doc to existing active case
      "new_case_reference"       — no primary doc yet, reference-only case
      "mark_primary_replied"     — this is a reply doc for an existing primary
      "different_parties"        — needs user confirmation (Case 9)
      "needs_confirmation"       — ambiguous classification
    """
    if user_said_new_case:
        if analysis.get("is_primary"):
            return "new_case_primary"
        return "new_case_reference"

    if user_said_reference:
        return "add_to_case_reference" if has_existing_case else "new_case_reference"

    if analysis.get("is_previous_reply"):
        return "mark_primary_replied"

    is_primary     = analysis.get("is_primary", True)
    same_case      = analysis.get("same_case", "unclear")
    confidence     = analysis.get("confidence", 70)
    legal_doc_type = analysis.get("legal_doc_type", "other")

    # Reference material types — never need an active case match
    reference_types = {"judgment", "circular", "notification", "rule_text", "reference_material"}
    if legal_doc_type in reference_types and not is_primary:
        return "add_to_case_reference" if has_existing_case else "new_case_reference"

    if not has_existing_case:
        if is_primary:
            return "new_case_primary"
        return "new_case_reference"

    # Existing case present
    if same_case == "yes":
        return "add_to_case_primary" if is_primary else "add_to_case_reference"

    if same_case == "no":
        if is_primary:
            return "different_parties"  # Case 9 — needs user confirmation
        return "add_to_case_reference"  # Reference docs always go to current case

    # "unclear" or low confidence
    if confidence < 70:
        return "needs_confirmation"

    # Default: if parties partially match, treat as same case
    return "add_to_case_primary" if is_primary else "add_to_case_reference"