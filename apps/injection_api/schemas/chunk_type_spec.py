"""
ingestion_api/schemas/chunk_type_specs.py

Single source of truth for all 21 chunk types.
Drives: API validation · UI form generation · system field injection ·
        BM25/embed pipeline · supersession logic · autofill prompts.
"""

from typing import Any

# ── Authority level labels and namespace routing ─────────────────────────────

AUTHORITY_LEVELS: dict[int, dict] = {
    1: { "label": "Parliamentary statute", "namespace": "statutory_law", "color": "purple" },
    2: { "label": "Subordinate legislation", "namespace": "statutory_law", "color": "blue" },
    3: { "label": "Executive order", "namespace": "live_updates", "color": "amber" },
    4: { "label": "Administrative circular", "namespace": "clarifications", "color": "teal" },
    5: { "label": "Judicial interpretation", "namespace": "judicial", "color": "coral" },
    6: { "label": "Knowledge / context", "namespace": "knowledge", "color": "gray" },
}

def _base_system_fields(level: int) -> dict[str, Any]:
    al = AUTHORITY_LEVELS[level]
    return {
        "authority.level":              level,
        "authority.label":              al["label"],
        "authority.is_statutory":       level <= 2,
        "authority.is_binding":         level <= 5,
        "authority.can_be_cited":       level <= 5,
        "temporal.is_current":          True,
        "legal_status.current_status":  "active",
        "legal_status.is_disputed":     False,
        "legal_status.dispute_note":    None,
    }

# ── CGST Section ─────────────────────────────────────────────────────────────

CGST_SECTION_SPEC: dict[str, Any] = {
    "chunk_type":       "cgst_section",
    "ui_display_name":  "CGST Act — Section",
    "ui_description":   "A section or sub-section of the Central Goods and Services Tax Act, 2017",
    "authority_level":  1,
    "namespace":        "statutory_law",

    "anchor_fields": [
        { "path": "ext.section_number", "label": "Section number", "type": "text", "required": True, "placeholder": "e.g. 16" },
        { "path": "ext.section_title",  "label": "Section title",  "type": "text", "required": True },
        { "path": "ext.chapter_number", "label": "Chapter number", "type": "text", "required": True },
        { "path": "ext.chapter_title",  "label": "Chapter title",  "type": "text", "required": True },
        { "path": "text",               "label": "Section text",   "type": "textarea", "required": True },
    ],

    "autofill_fields": [
        { "path": "summary", "label": "Summary", "type": "textarea", "hint": "1-3 sentences" },
        { "path": "keywords", "label": "Keywords", "type": "tag_list", "hint": "3-15 keywords" },
        { "path": "ext.provision_type", "label": "Provision type", "type": "select", "options": ["definition", "levy", "exemption", "penalty", "procedure", "appeal", "registration", "return", "itc", "refund", "assessment", "other"] },
        { "path": "ext.sub_section", "label": "Sub-section", "type": "text", "hint": "e.g. (1)(a)" },
        { "path": "ext.amendment_history", "label": "Amendment history", "type": "json_list" },
        { "path": "cross_references.sections", "label": "Cross-referenced sections", "type": "tag_list" },
        { "path": "cross_references.rules", "label": "Cross-referenced rules", "type": "tag_list" },
        { "path": "cross_references.notifications", "label": "Cross-referenced notifications", "type": "tag_list" },
        { "path": "retrieval.primary_topics", "label": "Primary topics", "type": "tag_list" },
        { "path": "retrieval.query_categories", "label": "Query categories", "type": "multi_select", "options": ["rate_lookup", "compliance_procedure", "notice_defence", "itc_eligibility", "definition_lookup", "grey_area", "appeal_procedure", "form_filing", "general"] },
        { "path": "retrieval.applicable_to", "label": "Applicable to", "type": "select", "options": ["goods", "services", "both"] },
        { "path": "retrieval.tax_type", "label": "Tax type", "type": "select", "options": ["CGST", "IGST", "SGST", "ALL"], "default": "CGST" },
        { "path": "ext.has_proviso",     "label": "Has proviso?",     "type": "boolean" },
        { "path": "ext.has_explanation", "label": "Has explanation?", "type": "boolean" },
        { "path": "ext.has_illustration", "label": "Has illustration?", "type": "boolean" },
    ],

    "system_fields": {
        **_base_system_fields(1),
        "ext.act":            "CGST Act, 2017",
        "chunk_type":         "cgst_section",
        "parent_doc":         "Central Goods and Services Tax Act, 2017",
        "hierarchy_level":    3,
        "provenance.version": "1.0",
    },

    "supersession_check": {
        "enabled":       True,
        "type":          "amendment",
        "match_field":   "ext.section_number",
        "action":        "flip_old_is_current",
    },

    "dedup_key":    "ext.section_number",
    "dedup_action": "warn",
}

# ── IGST Section ─────────────────────────────────────────────────────────────

IGST_SECTION_SPEC: dict[str, Any] = {
    "chunk_type":       "igst_section",
    "ui_display_name":  "IGST Act — Section",
    "authority_level":  1,
    "namespace":        "statutory_law",
    "anchor_fields": CGST_SECTION_SPEC["anchor_fields"],
    "autofill_fields": CGST_SECTION_SPEC["autofill_fields"] + [
        { "path": "ext.igst_specific.involves_inter_state", "label": "Inter-state?", "type": "boolean" },
        { "path": "ext.igst_specific.involves_place_of_supply", "label": "Place of Supply?", "type": "boolean" },
        { "path": "ext.igst_specific.cgst_cross_ref", "label": "CGST cross-refs", "type": "tag_list" },
    ],
    "system_fields": {
        **_base_system_fields(1),
        "ext.act":            "IGST Act, 2017",
        "chunk_type":         "igst_section",
        "parent_doc":         "Integrated Goods and Services Tax Act, 2017",
        "hierarchy_level":    3,
        "provenance.version": "1.0",
        "retrieval.tax_type": "IGST",
    },
    "supersession_check": CGST_SECTION_SPEC["supersession_check"],
    "dedup_key":    "ext.section_number",
    "dedup_action": "warn",
}

# ── Circular ─────────────────────────────────────────────────────────────────

CIRCULAR_SPEC: dict[str, Any] = {
    "chunk_type":       "circular",
    "ui_display_name":  "GST Circular",
    "authority_level":  4,
    "namespace":        "clarifications",
    "anchor_fields": [
        { "path": "ext.circular_number", "label": "Circular Number", "type": "text", "required": True },
        { "path": "ext.circular_date",   "label": "Issue Date", "type": "text", "required": True },
        { "path": "ext.subject",         "label": "Subject", "type": "textarea", "required": True },
        { "path": "text",                "label": "Direct Text", "type": "textarea", "required": True },
    ],
    "autofill_fields": [
        { "path": "summary", "label": "Summary", "type": "textarea" },
        { "path": "keywords", "label": "Keywords", "type": "tag_list" },
        { "path": "ext.chunk_subtype", "label": "Chunk Subtype", "type": "select", "options": ["overview", "paragraph", "table_row", "table_header"] },
        { "path": "ext.row_data", "label": "Table Row Data", "type": "json" },
        { "path": "cross_references.circulars", "label": "Linked Circulars", "type": "tag_list" },
        { "path": "retrieval.tax_type", "label": "Tax Type", "type": "select", "options": ["CGST", "IGST", "SGST", "ALL"] },
    ],
    "system_fields": {
        **_base_system_fields(4),
        "chunk_type": "circular",
        "provenance.version": "1.0",
    },
    "supersession_check": {
        "enabled": True,
        "type": "circular_supersession",
        "match_field": "ext.circular_number",
    },
    "dedup_key": "ext.circular_number",
    "dedup_action": "warn",
}

# ── Notification ─────────────────────────────────────────────────────────────

NOTIFICATION_SPEC: dict[str, Any] = {
    "chunk_type":       "notification",
    "ui_display_name":  "GST Notification",
    "authority_level":  3,
    "namespace":        "live_updates",
    "anchor_fields": [
        { "path": "ext.notification_number", "label": "Notif Number", "type": "text", "required": True },
        { "path": "ext.notification_type",   "label": "Notif Type",   "type": "select", "options": ["Central Tax", "Integrated Tax", "Central Tax (Rate)", "Integrated Tax (Rate)"], "required": True },
        { "path": "ext.year",                "label": "Year", "type": "text", "required": True },
        { "path": "text",                    "label": "Notification Text", "type": "textarea", "required": True },
    ],
    "autofill_fields": [
        { "path": "summary", "label": "Summary", "type": "textarea" },
        { "path": "keywords", "label": "Keywords", "type": "tag_list" },
        { "path": "ext.amends_notification", "label": "Amends Notif", "type": "text" },
        { "path": "ext.rescinds_notification", "label": "Rescinds Notif", "type": "text" },
        { "path": "ext.chunk_subtype", "label": "Chunk Subtype", "type": "select", "options": ["overview", "clause", "rate_entry"] },
        { "path": "ext.row_data", "label": "Table Row Data", "type": "json" },
        { "path": "retrieval.tax_type", "label": "Tax Type", "type": "select", "options": ["CGST", "IGST", "SGST", "ALL"] },
    ],
    "system_fields": {
        **_base_system_fields(3),
        "chunk_type": "notification",
        "provenance.version": "1.0",
    },
    "supersession_check": {
        "enabled": True,
        "type": "notification_supersession",
        "match_field": "ext.notification_number",
    },
    "dedup_key":    None,
}

# ── IGST Rule ───────────────────────────────────────────────────────────────

IGST_RULE_SPEC: dict[str, Any] = {
    "chunk_type":       "igst_rule",
    "ui_display_name":  "IGST Rule",
    "authority_level":  2,
    "namespace":        "statutory_law",
    "anchor_fields": [
        { "path": "ext.rule_number", "label": "Rule Number", "type": "text", "required": True },
        { "path": "ext.rule_title",  "label": "Rule Title",  "type": "text", "required": True },
        { "path": "text",            "label": "Rule Text",   "type": "textarea", "required": True },
    ],
    "autofill_fields": [
        { "path": "summary", "label": "Summary", "type": "textarea" },
        { "path": "keywords", "label": "Keywords", "type": "tag_list" },
        { "path": "ext.chunk_subtype", "label": "Chunk Subtype", "type": "select", "options": ["overview", "sub_rule"] },
        { "path": "ext.sub_rule_id", "label": "Sub-rule ID", "type": "text" },
        { "path": "ext.forms_prescribed", "label": "Forms Prescribed", "type": "tag_list" },
        { "path": "ext.sections_referred", "label": "Sections Referred", "type": "tag_list" },
        { "path": "retrieval.tax_type", "label": "Tax Type", "type": "select", "options": ["IGST", "CGST", "ALL"], "default": "IGST" },
    ],
    "system_fields": {
        **_base_system_fields(2),
        "chunk_type": "igst_rule",
        "parent_doc": "Integrated Goods and Services Tax Rules, 2017",
        "provenance.version": "1.0",
        "retrieval.tax_type": "IGST",
    },
    "supersession_check": {
        "enabled":       True,
        "type":          "amendment",
        "match_field":   "ext.rule_number",
    },
    "dedup_key":    "ext.rule_number",
    "dedup_action": "warn",
}

# ── CGST Rule ───────────────────────────────────────────────────────────────

CGST_RULE_SPEC: dict[str, Any] = {
    "chunk_type":       "cgst_rule",
    "ui_display_name":  "CGST Rule",
    "authority_level":  2,
    "namespace":        "statutory_law",
    "anchor_fields": [
        { "path": "ext.rule_number", "label": "Rule Number", "type": "text", "required": True },
        { "path": "ext.rule_title",  "label": "Rule Title",  "type": "text", "required": True },
        { "path": "text",            "label": "Rule Text",   "type": "textarea", "required": True },
    ],
    "autofill_fields": [
        { "path": "summary", "label": "Summary", "type": "textarea" },
        { "path": "keywords", "label": "Keywords", "type": "tag_list" },
        { "path": "ext.rule_number_full", "label": "Full Rule No", "type": "text" },
        { "path": "ext.chunk_subtype", "label": "Chunk Subtype", "type": "select", "options": ["overview", "sub_rule"] },
        { "path": "ext.sub_rule_id", "label": "Sub-rule ID", "type": "text" },
        { "path": "ext.forms_prescribed", "label": "Forms Prescribed", "type": "tag_list" },
        { "path": "ext.sections_referred", "label": "Sections Referred", "type": "tag_list" },
        { "path": "retrieval.tax_type", "label": "Tax Type", "type": "select", "options": ["CGST", "IGST", "SGST", "ALL"], "default": "CGST" },
    ],
    "system_fields": {
        **_base_system_fields(2),
        "chunk_type": "cgst_rule",
        "parent_doc": "Central Goods and Services Tax Rules, 2017",
        "provenance.version": "1.0",
        "retrieval.tax_type": "CGST",
    },
    "supersession_check": {
        "enabled":       True,
        "type":          "amendment",
        "match_field":   "ext.rule_number",
    },
    "dedup_key":    "ext.rule_number",
    "dedup_action": "warn",
}

# ── Master registry ───────────────────────────────────────────────────────────

CHUNK_TYPE_SPECS: dict[str, dict[str, Any]] = {
    "cgst_section": CGST_SECTION_SPEC,
    "igst_section": IGST_SECTION_SPEC,
    "circular":     CIRCULAR_SPEC,
    "notification": NOTIFICATION_SPEC,
    "igst_rule":    IGST_RULE_SPEC,
    "cgst_rule":    CGST_RULE_SPEC,
}

# ── Helpers used by API and worker ────────────────────────────────────────────

def get_spec(chunk_type: str) -> dict[str, Any]:
    if chunk_type not in CHUNK_TYPE_SPECS:
        raise KeyError(f"Unknown chunk_type: '{chunk_type}'")
    return CHUNK_TYPE_SPECS[chunk_type]

def inject_system_fields(chunk: dict, chunk_type: str) -> dict:
    spec = get_spec(chunk_type)
    for path, value in spec["system_fields"].items():
        _set_nested(chunk, path, value)
    return chunk

def get_anchor_paths(chunk_type: str) -> list[str]:
    return [f["path"] for f in get_spec(chunk_type)["anchor_fields"]]

def get_autofill_paths(chunk_type: str) -> list[str]:
    return [f["path"] for f in get_spec(chunk_type)["autofill_fields"]]

def _set_nested(d: dict, path: str, value: Any) -> None:
    keys = path.split(".")
    cursor = d
    for k in keys[:-1]:
        if k not in cursor or not isinstance(cursor[k], dict):
            cursor[k] = {}
        cursor = cursor[k]
    final_key = keys[-1]
    if final_key not in cursor:
        cursor[final_key] = value

def get_nested(d: dict, path: str, default: Any = None) -> Any:
    keys = path.split(".")
    cursor = d
    for k in keys:
        if not isinstance(cursor, dict) or k not in cursor:
            return default
        cursor = cursor[k]
    return cursor