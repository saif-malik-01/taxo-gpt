"""
ingestion_api/autofill/prompt_builder.py

Builds per-type structured prompts for the autofill LLM call.
"""

from __future__ import annotations

import json
from typing import Any

# ── System prompt (same for all types) ───────────────────────────────────────

_SYSTEM_PROMPT = """You are a senior GST law expert and legal data engineer.
Your task is to analyse a provided GST legal text and fill in structured metadata fields.

RULES:
1. Return ONLY a valid JSON object — no preamble, no explanation, no markdown fences.
2. Use ONLY information that can be directly inferred from the provided text.
3. If a field cannot be reliably inferred, set it to null (for scalars) or [] (for lists).
4. Do NOT invent citations, section numbers, or notification numbers not present in the text.
5. For keywords: provide 5-15 specific, retrieval-useful terms. Avoid generic words.
6. For summary: write 1-3 plain-English sentences a non-lawyer can understand.
7. For cross_references: extract only references explicitly mentioned in the text.
8. If 'split' is requested, break the law into logical chunks (Overview followed by sub-sections/clauses). Each chunk must be an object in a JSON list. Identify each with a logical 'id' and 'ext.sub_section' tag."""


# ── Per-type prompts ──────────────────────────────────────────────────────────

def build_prompt(chunk_type: str, anchor_data: dict[str, Any], split: bool = True) -> tuple[str, str]:
    """
    Returns (system_prompt, user_prompt) for the autofill LLM call.
    Default to split=True as requested.
    """
    builders = {
        "cgst_section": _cgst_section_prompt,
        "igst_section": _igst_section_prompt,
        "circular":     _circular_prompt,
        "notification": _notification_prompt,
        "igst_rule":    _igst_rule_prompt,
        "cgst_rule":    _cgst_rule_prompt,
        "judgment":     _judgment_prompt,
    }
    builder = builders.get(chunk_type)
    if builder is None:
        raise ValueError(f"No autofill prompt builder for chunk_type='{chunk_type}'")

    return _SYSTEM_PROMPT, builder(anchor_data, split)


def _cgst_section_prompt(anchor: dict[str, Any], split: bool = True) -> str:
    section_number = anchor.get("ext", {}).get("section_number") or anchor.get("ext.section_number", "")
    section_title  = anchor.get("ext", {}).get("section_title")  or anchor.get("ext.section_title", "")
    chapter_number = anchor.get("ext", {}).get("chapter_number") or anchor.get("ext.chapter_number", "")
    chapter_title  = anchor.get("ext", {}).get("chapter_title")  or anchor.get("ext.chapter_title", "")
    text           = anchor.get("text", "")

    example_structure = {
        "id": f"cgst-s{section_number}-overview",
        "chunk_type": "cgst_section",
        "parent_doc": "Central Goods and Services Tax Act, 2017",
        "chunk_index": 1,
        "total_chunks": 3,
        "text": "...chunk text...",
        "summary": "plain-English summary",
        "keywords": ["tag1", "tag2"],
        "authority": {"level": 1, "label": "Parliamentary Statute", "is_statutory": True, "is_binding": True, "can_be_cited": True},
        "temporal": {"effective_date": "2017-06-22", "superseded_date": None, "is_current": True, "financial_year": "2017-18"},
        "legal_status": {"is_disputed": False, "dispute_note": None, "current_status": "active", "overruled_by": None},
        "cross_references": {
          "sections": ["16", "17(5)"],
          "rules": ["36", "89"],
          "notifications": [],
          "circulars": [],
          "forms": [],
          "hsn_codes": [],
          "sac_codes": [],
          "judgment_ids": [],
          "parent_chunk_id": f"cgst-s{section_number}-overview"
        },
        "retrieval": {
          "primary_topics": ["ITC", "input_tax_credit"],
          "tax_type": "CGST",
          "applicable_to": "both",
          "query_categories": ["compliance_procedure", "registration"],
          "boost_score": 0.82
        },
        "provenance": {"source_file": "cgst_act_2017.json", "page_range": None, "ingestion_date": "2026-04-12", "version": "1.0"},
        "ext": {
          "act": "CGST Act, 2017",
          "chapter_number": chapter_number,
          "chapter_title": chapter_title,
          "section_number": section_number,
          "section_title": section_title,
          "sub_section": "overview",
          "hierarchy_level": 3,
          "provision_type": "procedure",
          "has_proviso": False,
          "proviso_text": None,
          "proviso_implication": None,
          "has_explanation": False,
          "has_illustration": False,
          "amendment_history": []
        }
    }

    if split:
        return f"""Break the following CGST Act section into logical chunks (Overview + individual sub-sections).
Return a JSON LIST of objects. Each object must represent one chunk.

SECTION CONTEXT:
  Section number : {section_number}
  Section title  : {section_title}
  Chapter        : {chapter_number} — {chapter_title}

FULL SECTION TEXT:
{text}

For EACH chunk in the list, provide this EXACT structure:
{json.dumps(example_structure, indent=2)}

Important for splitting:
- Usually, the section name and opening preamble go into an 'overview' chunk.
- Every numbered sub-section (1), (2), etc., should be its own chunk.
- For sub-sections, update 'id' (e.g. ss1, ss2), 'ext.sub_section' (e.g. (1), (2)), and 'chunk_index'.
- Leave temporal.effective_date as 2017-06-22 unless the text says otherwise.
"""

    return f"""Analyse the following CGST Act section and fill in the metadata fields.
Return a SINGLE JSON object matching this structure:
{json.dumps(example_structure, indent=2)}

FULL SECTION TEXT:
{text}
"""


def _igst_section_prompt(anchor: dict[str, Any], split: bool = True) -> str:
    section_number = anchor.get("ext", {}).get("section_number") or anchor.get("ext.section_number", "")
    section_title  = anchor.get("ext", {}).get("section_title")  or anchor.get("ext.section_title", "")
    chapter_number = anchor.get("ext", {}).get("chapter_number") or anchor.get("ext.chapter_number", "")
    chapter_title  = anchor.get("ext", {}).get("chapter_title")  or anchor.get("ext.chapter_title", "")
    text           = anchor.get("text", "")

    example_structure = {
        "id": f"igst-s{section_number}-overview",
        "chunk_type": "igst_section",
        "parent_doc": "Integrated Goods and Services Tax Act, 2017",
        "chunk_index": 1,
        "total_chunks": 2,
        "text": "...chunk text...",
        "summary": "plain-English summary",
        "keywords": ["tag1", "tag2"],
        "authority": {"level": 1, "label": "Parliamentary Statute", "is_statutory": True, "is_binding": True, "can_be_cited": True},
        "temporal": {"effective_date": "2017-06-22", "superseded_date": None, "is_current": True, "financial_year": "2017-18"},
        "legal_status": {"is_disputed": False, "dispute_note": None, "current_status": "active", "overruled_by": None},
        "cross_references": {
          "sections": ["3", "5"],
          "rules": [],
          "notifications": [],
          "circulars": [],
          "forms": [],
          "hsn_codes": [],
          "sac_codes": [],
          "judgment_ids": [],
          "parent_chunk_id": f"igst-s{section_number}-overview"
        },
        "retrieval": {
          "primary_topics": ["inter-state", "place_of_supply"],
          "tax_type": "IGST",
          "applicable_to": "both",
          "query_categories": ["compliance_procedure"],
          "boost_score": 0.65
        },
        "provenance": {"source_file": "igst_act_2017.json", "page_range": None, "ingestion_date": "2026-04-12", "version": "1.0"},
        "ext": {
          "act": "IGST Act, 2017",
          "chapter_number": chapter_number,
          "chapter_title": chapter_title,
          "section_number": section_number,
          "section_title": section_title,
          "sub_section": "overview",
          "hierarchy_level": 3,
          "provision_type": "procedure",
          "has_proviso": False,
          "proviso_text": None,
          "proviso_implication": None,
          "has_explanation": False,
          "has_illustration": False,
          "igst_specific": {
            "involves_inter_state": False,
            "involves_place_of_supply": False,
            "involves_import_export": False,
            "involves_zero_rating": False,
            "involves_apportionment": False,
            "cgst_cross_ref": []
          },
          "amendment_history": []
        }
    }

    if split:
        return f"""Break the following IGST Act section into logical chunks (Overview + individual sub-sections).
Return a JSON LIST of objects. Each object must represent one chunk.

SECTION CONTEXT:
  Section number : {section_number}
  Section title  : {section_title}
  Chapter        : {chapter_number} — {chapter_title}

FULL SECTION TEXT:
{text}

For EACH chunk in the list, provide this EXACT structure:
{json.dumps(example_structure, indent=2)}

Important for IGST:
- Identify if the clause involves inter-state supply, export/import, or zero-rating.
- Populate ext.igst_specific flags accordingly.
- For sub-sections, update 'id' (e.g. ss1, ss2), 'ext.sub_section' (e.g. (1), (2)), and 'chunk_index'.
"""

    return f"""Analyse the following IGST Act section and fill in the metadata fields.
Return a SINGLE JSON object matching this structure:
{json.dumps(example_structure, indent=2)}

FULL SECTION TEXT:
{text}
"""


def _circular_prompt(anchor: dict[str, Any], split: bool = True) -> str:
    circ_num  = anchor.get("ext", {}).get("circular_number") or anchor.get("ext.circular_number", "")
    circ_date = anchor.get("ext", {}).get("circular_date")   or anchor.get("ext.circular_date", "")
    subject   = anchor.get("ext", {}).get("subject")         or anchor.get("ext.subject", "")
    text      = anchor.get("text", "")

    example_structure = {
        "id": f"circ-{circ_num}-overview".replace("/", "-"),
        "chunk_type": "circular",
        "parent_doc": f"Circular No. {circ_num}",
        "chunk_index": 1,
        "total_chunks": 4,
        "text": "...chunk text...",
        "summary": "plain-English summary",
        "keywords": ["tag1", "tag2"],
        "authority": {"level": 4, "label": "Administrative Instructions", "is_statutory": False, "is_binding": True, "can_be_cited": True},
        "temporal": {"effective_date": circ_date, "superseded_date": None, "is_current": True, "financial_year": "2017-18"},
        "legal_status": {"is_disputed": False, "dispute_note": None, "current_status": "active", "overruled_by": None},
        "cross_references": {
          "sections": [],
          "rules": [],
          "notifications": [],
          "circulars": [circ_num],
          "parent_chunk_id": f"circ-{circ_num}-overview".replace("/", "-")
        },
        "retrieval": {
          "primary_topics": ["clarification"],
          "tax_type": "CGST",
          "applicable_to": "both",
          "query_categories": ["compliance_procedure"]
        },
        "ext": {
          "circular_number": circ_num,
          "circular_date": circ_date,
          "subject": subject,
          "year": "2017",
          "chunk_subtype": "overview",  # overview | paragraph | table_row
          "para_number": None,
          "table_index": None,
          "row_number": None,
          "table_headers": None,
          "row_data": None,
          "entities_covered": []
        }
    }

    if split:
        return f"""Break the following GST Circular into logical chunks.
Return a JSON LIST of objects.

CIRCULAR CONTEXT:
  Number: {circ_num}
  Date:   {circ_date}
  Subject: {subject}

FULL TEXT:
{text}

For EACH chunk, use this EXACT structure:
{json.dumps(example_structure, indent=2)}

Splitting Strategy:
1. 'overview': Everything from the letterhead down to the 'Madam/Sir' greeting.
2. 'paragraph': Individual numbered points (e.g., 2., 3.1).
3. 'table_row': If the circular has a table (like Sl. No | Issue | Clarification), convert each ROW into a chunk.
   - For table_row, populate 'ext.table_headers' and 'ext.row_data' (JSON dict of headers to values).
4. For Corrigenda: link the parent circular in 'cross_references.circulars'.
"""

    return f"""Analyse the following GST Circular and fill in the metadata.
Return a SINGLE JSON object matching this structure:
{json.dumps(example_structure, indent=2)}

FULL TEXT:
{text}
"""


def _notification_prompt(anchor: dict[str, Any], split: bool = True) -> str:
    notif_num  = anchor.get("ext", {}).get("notification_number") or anchor.get("ext.notification_number", "")
    notif_type = anchor.get("ext", {}).get("notification_type")   or anchor.get("ext.notification_type", "")
    year       = anchor.get("ext", {}).get("year")                or anchor.get("ext.year", "")
    text       = anchor.get("text", "")

    example_structure = {
        "id": f"notif-{notif_num}-{year}-overview".replace("/", "-"),
        "chunk_type": "notification",
        "parent_doc": f"Notification No. {notif_num}-{notif_type}",
        "chunk_index": 1,
        "total_chunks": 5,
        "text": "...chunk text...",
        "summary": "plain-English summary of the notification or clause",
        "keywords": ["tag1", "tax rate"],
        "authority": {"level": 3, "label": "Executive Orders", "is_statutory": False, "is_binding": True, "can_be_cited": True},
        "temporal": {"effective_date": "2018-01-25", "superseded_date": None, "is_current": True, "financial_year": f"{year}-{int(year)+1-2000}"},
        "legal_status": {"is_disputed": False, "index_note": None, "current_status": "active", "overruled_by": None},
        "cross_references": {
          "sections": ["11"],
          "rules": [],
          "notifications": ["12/2017"],
          "sac_codes": ["9965"],
          "parent_chunk_id": f"notif-{notif_num}-{year}-overview".replace("/", "-")
        },
        "retrieval": {
          "primary_topics": ["tax rate updates"],
          "tax_type": "CGST",
          "applicable_to": "services",
          "query_categories": ["rate_lookup"]
        },
        "ext": {
          "notification_number": notif_num,
          "notification_type": notif_type,
          "amends_notification": None,
          "rescinds_notification": None,
          "year": year,
          "chunk_subtype": "overview",  # overview | clause | rate_entry
          "clause_number": None,
          "table_headers": None,
          "row_data": None,
          "taxpayer_category": None,
          "interest_rate": None,
          "has_proviso": False
        }
    }

    if split:
        return f"""Break the following GST Notification into logical chunks (Overview + individual Clauses + Rate Entries).
Return a JSON LIST of objects.

NOTIFICATION CONTEXT:
  Number: {notif_num}
  Type:   {notif_type}
  Year:   {year}

FULL TEXT:
{text}

For EACH chunk, use this EXACT structure:
{json.dumps(example_structure, indent=2)}

Splitting Strategy:
1. 'overview': Letterhead, Preamble (e.g., 'Now, therefore, in exercise of powers...'), and opening sentence.
2. 'clause': Numbered or lettered amendment paragraphs (e.g., (a), (b), (i)). 
3. 'rate_entry': If a clause specifies a substitution/insertion in a table, break each table ROW onto a chunk.
   - For rate_entry, populate 'ext.table_headers' and 'ext.row_data' (JSON).
   - Extract SAC/HSN codes into 'cross_references.sac_codes' or 'hsn_codes'.
4. Identify 'ext.amends_notification' or 'ext.rescinds_notification' if the text mentions modifying a previous notification.
"""

    return f"""Analyse the following GST Notification and fill in the metadata.
Return a SINGLE JSON object matching this structure:
{json.dumps(example_structure, indent=2)}

FULL TEXT:
{text}
"""


def _igst_rule_prompt(anchor: dict[str, Any], split: bool = True) -> str:
    rule_num   = anchor.get("ext", {}).get("rule_number")   or anchor.get("ext.rule_number", "")
    rule_title = anchor.get("ext", {}).get("rule_title")    or anchor.get("ext.rule_title", "")
    text       = anchor.get("text", "")

    example_structure = {
        "id": f"rule-{rule_num}-overview",
        "chunk_type": "igst_rule",
        "parent_doc": "Integrated Goods and Services Tax Rules, 2017",
        "chunk_index": 1,
        "total_chunks": 3,
        "text": "...chunk text...",
        "summary": "plain-English summary of the rule or sub-rule",
        "keywords": ["tag1"],
        "authority": {"level": 2, "label": "Subordinate Legislation", "is_statutory": True, "is_binding": True, "can_be_cited": True},
        "temporal": {"effective_date": "2017-07-01", "superseded_date": None, "is_current": True, "financial_year": "2017-18"},
        "legal_status": {"is_disputed": False, "dispute_note": None, "current_status": "active", "overruled_by": None},
        "cross_references": {
          "sections": [],
          "rules": [],
          "notifications": [],
          "forms": ["GSTR-1"],
          "parent_chunk_id": f"rule-{rule_num}-overview"
        },
        "retrieval": {
          "primary_topics": ["place of supply"],
          "tax_type": "IGST",
          "applicable_to": "both",
          "query_categories": ["compliance_procedure"]
        },
        "ext": {
          "rule_number": rule_num,
          "rule_number_full": f"Rule {rule_num}",
          "rule_title": rule_title,
          "category": "igst-rules",
          "year": "2017",
          "hierarchy_level": 2,
          "chunk_subtype": "overview",  # overview | sub_rule
          "sub_rule_id": None,
          "has_proviso": False,
          "has_explanation": False,
          "forms_prescribed": [],
          "sections_referred": [],
          "rules_referred": []
        }
    }

    if split:
        return f"""Break the following IGST Rule into logical chunks (Overview followed by individual Sub-rules).
Return a JSON LIST of objects.

RULE CONTEXT:
  Number: {rule_num}
  Title:  {rule_title}

FULL TEXT:
{text}

For EACH chunk, use this EXACT structure:
{json.dumps(example_structure, indent=2)}

Splitting Strategy:
1. 'overview': The rule title and any opening preamble before sub-rule (1).
2. 'sub_rule': Each individual numbered sub-rule (e.g., (1), (2), (3)).
   - For sub_rule, update 'id' to 'rule-{rule_num}-sr1', 'rule-{rule_num}-sr2', etc.
   - Set 'ext.chunk_subtype' to 'sub_rule' and 'ext.sub_rule_id' to '(1)', '(2)', etc.
3. Identify 'ext.forms_prescribed' (e.g., GSTR-1) and 'ext.sections_referred' (e.g., Section 12) from the text.
"""

    return f"""Analyse the following IGST Rule and fill in the metadata.
Return a SINGLE JSON object matching this structure:
{json.dumps(example_structure, indent=2)}

FULL TEXT:
{text}
"""


def _cgst_rule_prompt(anchor: dict[str, Any], split: bool = True) -> str:
    rule_num   = anchor.get("ext", {}).get("rule_number")   or anchor.get("ext.rule_number", "")
    rule_title = anchor.get("ext", {}).get("rule_title")    or anchor.get("ext.rule_title", "")
    text       = anchor.get("text", "")

    example_structure = {
        "id": f"rule-{rule_num}-overview",
        "chunk_type": "cgst_rule",
        "parent_doc": "Central Goods and Services Tax Rules, 2017",
        "chunk_index": 1,
        "total_chunks": 3,
        "text": "...chunk text...",
        "summary": "plain-English summary of the rule or sub-rule",
        "keywords": ["tag1"],
        "authority": {"level": 2, "label": "Subordinate Legislation", "is_statutory": True, "is_binding": True, "can_be_cited": True},
        "temporal": {"effective_date": "2017-07-01", "superseded_date": None, "is_current": True, "financial_year": "2017-18"},
        "legal_status": {"is_disputed": False, "dispute_note": None, "current_status": "active", "overruled_by": None},
        "cross_references": {
          "sections": [],
          "rules": [],
          "notifications": [],
          "forms": ["GSTR-3B"],
          "parent_chunk_id": f"rule-{rule_num}-overview"
        },
        "retrieval": {
          "primary_topics": ["recovery"],
          "tax_type": "CGST",
          "applicable_to": "both",
          "query_categories": ["compliance_procedure"]
        },
        "ext": {
          "rule_number": rule_num,
          "rule_number_full": f"Rule {rule_num}",
          "rule_title": rule_title,
          "category": "cgst-rules",
          "year": "2017",
          "hierarchy_level": 2,
          "chunk_subtype": "overview",  # overview | sub_rule
          "sub_rule_id": None,
          "has_proviso": False,
          "has_explanation": False,
          "forms_prescribed": [],
          "sections_referred": [],
          "rules_referred": []
        }
    }

    if split:
        return f"""Break the following CGST Rule into logical chunks (Overview followed by individual Sub-rules).
Return a JSON LIST of objects.

RULE CONTEXT:
  Number: {rule_num}
  Title:  {rule_title}

FULL TEXT:
{text}

For EACH chunk, use this EXACT structure:
{json.dumps(example_structure, indent=2)}

Splitting Strategy:
1. 'overview': The rule title and any opening preamble before sub-rule (1).
2. 'sub_rule': Each individual numbered sub-rule (e.g., (1), (2), (10), (2A)).
   - For sub_rule, update 'id' to 'rule-{rule_num}-sr1', 'rule-{rule_num}-sr2', etc.
   - Set 'ext.chunk_subtype' to 'sub_rule' and 'ext.sub_rule_id' to '(1)', '(2)', etc.
3. Identify 'ext.forms_prescribed' (e.g., DRC-07, GSTR-3B) and 'ext.sections_referred' (e.g., Section 73) from the text.
"""

    return f"""Analyse the following CGST Rule and fill in the metadata.
Return a SINGLE JSON object matching this structure:
{json.dumps(example_structure, indent=2)}

FULL TEXT:
{text}
"""


def _judgment_prompt(anchor: dict[str, Any], split: bool = False) -> str:
    """
    Autofill prompt for judgment chunk type.
    split is not used for judgments — a single case maps to a single chunk.
    Boost score is calibrated by court:
      Supreme Court  -> 0.95
      High Court     -> 0.88
      CESTAT / GSTAT -> 0.80
      AAR / AAAR     -> 0.72
    """
    # ── Extract anchor values (supports both nested and dot-key formats) ──────
    def _get(key: str, sub: str = ""):
        if sub:
            return (
                anchor.get("ext", {}).get(sub)
                or anchor.get(f"ext.{sub}", "")
                or ""
            )
        return anchor.get(key, "")

    case_id       = _get("", "case_id")
    citation      = _get("", "citation")
    case_number   = _get("", "case_number")
    petitioner    = _get("", "petitioner")
    respondent    = _get("", "respondent")
    court         = _get("", "court")
    state         = _get("", "state")
    judgment_date = _get("", "judgment_date")
    judge         = _get("", "judge")
    decision      = _get("", "decision")
    law           = _get("", "law")
    act_name      = _get("", "act_name")
    rule_name     = _get("", "rule_name")
    notification  = _get("", "notification_number")
    case_note     = _get("", "case_note")
    text          = anchor.get("text", "")

    # Cross-refs supplied by user
    sections = (
        anchor.get("cross_references", {}).get("sections")
        or anchor.get("cross_references.sections", [])
        or []
    )
    rules = (
        anchor.get("cross_references", {}).get("rules")
        or anchor.get("cross_references.rules", [])
        or []
    )

    # Boost calibration
    boost_map = {
        "Supreme Court": 0.95,
        "High Court": 0.88,
        "CESTAT": 0.80,
        "GSTAT": 0.80,
        "AAR": 0.72,
        "AAAR": 0.72,
        "Authority for Advance Ruling": 0.72,
    }
    boost_score  = boost_map.get(court, 0.80)
    court_label  = f"{court}, {state}" if state else court
    case_name    = f"{petitioner} vs. {respondent}" if petitioner and respondent else petitioner

    example_structure = {
        "id": f"judg-{case_id}-overview" if case_id else "judg-unknown-overview",
        "chunk_type": "judgment",
        "parent_doc": "GST Judgments",
        "chunk_index": 1,
        "total_chunks": 1,
        "text": "...full judgment text...",
        "summary": "2-3 plain-English sentences summarising the legal issue, holding, and impact",
        "keywords": [
            "ITC refund", "inverted duty", "Section 54",
            f"{petitioner}", court_label, decision
        ],
        "authority": {
            "level": 5,
            "label": "Judicial Interpretations",
            "is_statutory": False,
            "is_binding": True,
            "can_be_cited": True,
        },
        "temporal": {
            "effective_date": judgment_date,
            "superseded_date": None,
            "is_current": True,
            "financial_year": None,
        },
        "legal_status": {
            "is_disputed": False,
            "dispute_note": None,
            "current_status": "active",
            "overruled_by": None,
        },
        "cross_references": {
            "sections": sections,
            "rules": rules,
            "notifications": [],
            "circulars": [],
            "forms": [],
            "hsn_codes": [],
            "sac_codes": [],
            "judgment_ids": [],
            "parent_chunk_id": None,
        },
        "retrieval": {
            "primary_topics": ["ITC refund", "inverted duty structure"],
            "tax_type": "CGST",
            "applicable_to": "both",
            "query_categories": ["itc_eligibility", "notice_defence"],
            "boost_score": boost_score,
        },
        "provenance": {
            "source_file": "judgments.csv",
            "page_range": None,
            "ingestion_date": "2026-04-09",
            "version": "1.0",
        },
        "ext": {
            "case_id": case_id,
            "case_name": case_name,
            "court": court,
            "court_level": "HC",  # AI should keep this as-is
            "state": state,
            "judgment_date": judgment_date,
            "citation": citation,
            "petitioner": petitioner,
            "respondent": respondent,
            "decision": decision,
            "law": law,
            "act_name": act_name,
            "judge": judge,
            "case_number": case_number,
            "rule_name": rule_name,
            "notification_number": notification,
            "title": anchor.get("ext", {}).get("title", ""),
            "case_note": case_note,
            # AI fills the fields below:
            "principle": "1-sentence ratio decidendi extracted from the judgment",
            "sections_in_dispute": ["54"],
            "rules_in_dispute": [],
            "current_status": "active",
            "overruled_by": None,
            "followed_in": [],
        },
    }

    return f"""You are a senior GST law expert. Analyse the following court judgment and fill in ALL metadata fields.

CASE CONTEXT (provided by user — do NOT change these values):
  Case ID      : {case_id}
  Citation     : {citation}
  Case Number  : {case_number}
  Court        : {court_label}
  Judge        : {judge}
  Petitioner   : {petitioner}
  Respondent   : {respondent}
  Date         : {judgment_date}
  Decision     : {decision}
  Law / Act    : {law} / {act_name}
  Sections     : {', '.join(sections) if sections else 'see text'}
  Rules        : {', '.join(rules) if rules else 'see text'}

CASE NOTE (headnote from source):
{case_note}

FULL JUDGMENT TEXT (for deep analysis):
{text}

Return a SINGLE JSON object matching this EXACT structure. For the fields marked '# AI fills':
1. summary       — write 2-3 plain-English sentences covering: legal issue, holding, and practical impact.
2. keywords      — 8-15 specific retrieval terms (section names, tax concepts, court, outcome).
3. retrieval.*   — infer tax_type, applicable_to, query_categories, primary_topics from text.
4. ext.principle — extract the 1-sentence ratio decidendi (the core legal rule laid down).
5. ext.sections_in_dispute — sections explicitly argued/contested (not just mentioned).
6. ext.rules_in_dispute — rules explicitly contested.
7. ext.overruled_by — if text mentions this judgment IS overruled by another, extract that citation. Otherwise null.
8. ext.followed_in — citations of cases that explicitly followed THIS judgment (if mentioned). Otherwise [].
9. cross_references.judgment_ids — ALL case citations mentioned anywhere in the text.
10. cross_references.notifications — notification numbers cited.
11. cross_references.circulars — circular numbers cited.
12. retrieval.boost_score — use exactly {boost_score} (pre-calibrated for {court}).

Do NOT change: id, chunk_type, parent_doc, authority, temporal, legal_status, provenance, or any ext fields already filled from context above.

FULL STRUCTURE TO RETURN:
{json.dumps(example_structure, indent=2)}
"""