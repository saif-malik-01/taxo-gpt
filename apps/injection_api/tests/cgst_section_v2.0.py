import os
import re
import json
from qdrant_client import QdrantClient
from qdrant_client.http import models
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

QDRANT_HOST = os.getenv("QDRANT_HOST")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", 6333))
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "tax_chunks")

# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION PATTERNS & ENUMS (From CGST Schema v2.0)
# ─────────────────────────────────────────────────────────────────────────────

ID_PATTERN = re.compile(r'^cgst-s\d+[a-zA-Z]?(-ss[a-zA-Z0-9\(\)]+|-overview)$')
DATE_PATTERN = re.compile(r'^\d{2}-\d{2}-\d{4}$')
FY_PATTERN = re.compile(r'^\d{4}-\d{2}$')
ISO_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
ISO_DATETIME_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$')
SUB_SECTION_PATTERN = re.compile(r'^(\(\d+[A-Z]?\)|overview|Explanation|Illustration|\([a-z]{1,2}\))$')

PRIMARY_TOPICS = {
    "input_tax_credit", "registration", "returns", "payment", "refund",
    "assessment", "audit", "appeals", "penalties", "interest",
    "composition_scheme", "definitions", "transitional_provisions",
    "exemptions", "valuation", "time_of_supply", "anti_profiteering",
    "accounts_records", "tax_invoice", "job_work", "electronic_commerce",
    "assessment_audit", "demands_recovery", "offences_penalties",
    "levy_collection", "scope_of_supply"
}

QUERY_CATEGORIES = {
    "rate_lookup", "compliance_procedure", "notice_defence", "itc_eligibility", 
    "definition_lookup", "grey_area", "appeal_procedure", "form_filing", "general",
    "applicability", "historical", "compliance", "procedure", "transitional",
    "calculation", "documentation", "timeline", "penalty", "exemption", "refund", "rate"
}

PROVISION_TYPES = {
    "commencement", "extent", "definition", "administrative", "levy",
    "composition_scheme", "input_tax_credit", "registration",
    "tax_invoice", "accounts_records", "returns", "payment", "refund",
    "assessment", "audit", "inspection_search_seizure", "demands_recovery",
    "liability_to_pay", "advance_ruling", "appeals_revision",
    "offences_penalties", "transitional", "miscellaneous", "procedure", "condition"
}

def validate_chunk(payload):
    errors = []
    
    # --- 1. Core Identity Fields ---
    if not payload.get("id"): errors.append("Missing id")
    elif not ID_PATTERN.match(str(payload["id"])): errors.append(f"Invalid id format: {payload['id']}")
    
    if payload.get("chunk_type") != "cgst_section": errors.append(f"Invalid chunk_type: {payload.get('chunk_type')}")
    if payload.get("parent_doc") != "Central Goods and Services Tax Act, 2017": errors.append("Invalid parent_doc")
    
    for field in ["chunk_index", "total_chunks"]:
        if not isinstance(payload.get(field), int): errors.append(f"{field} must be int")

    # --- 2. Content Fields ---
    if not payload.get("text") or len(str(payload["text"])) < 10: errors.append("text missing or <10 chars")
    if not payload.get("summary") or len(str(payload["summary"])) < 20: errors.append("summary missing or <20 chars")
    
    keywords = payload.get("keywords")
    if not isinstance(keywords, list) or not (3 <= len(keywords) <= 40):
        errors.append(f"keywords must be list [3-40], got {len(keywords) if keywords else 'None'}")

    # --- 3. Authority Object ---
    auth = payload.get("authority", {})
    if auth.get("level") != 1: errors.append("authority.level must be 1")
    if auth.get("label") != "Parliamentary Statute": errors.append("authority.label must be 'Parliamentary Statute'")
    for b_field in ["is_statutory", "is_binding", "can_be_cited"]:
        if auth.get(b_field) is not True: errors.append(f"authority.{b_field} must be True")

    # --- 4. Temporal Object ---
    temp = payload.get("temporal", {})
    effective_date = temp.get("effective_date")
    if effective_date and not DATE_PATTERN.match(str(effective_date)): errors.append("Invalid effective_date")
    if "is_current" not in temp: errors.append("Missing temporal.is_current")
    if not FY_PATTERN.match(str(temp.get("financial_year", ""))): errors.append("Invalid financial_year")
    
    # Superseded logic
    if temp.get("superseded_date"):
        if temp.get("is_current") is True: errors.append("is_current=True but superseded_date exists")

    # --- 5. Legal Status Object ---
    ls = payload.get("legal_status", {})
    if ls.get("current_status") not in ["active", "repealed", "suspended", "stayed", "amended"]:
        errors.append(f"Invalid current_status: {ls.get('current_status')}")
    if ls.get("is_disputed") is True and not ls.get("dispute_note"):
        errors.append("is_disputed is True but dispute_note is missing")

    # --- 6. Cross References Object ---
    xr = payload.get("cross_references", {})
    cref_patterns = {
        "sections": re.compile(r'^Section \d+[a-zA-Z]?$'),
        "rules": re.compile(r'^Rule \d+[a-zA-Z]?$'),
        "notifications": re.compile(r'^\d+/\d+(-(CT|ST|IT|UT))?(\(Rate\))?$')
    }
    
    for f, pattern in cref_patterns.items():
        vals = xr.get(f, [])
        if not isinstance(vals, list):
            errors.append(f"cross_references.{f} must be a list")
        else:
            for v in vals:
                if not pattern.match(str(v)):
                    errors.append(f"Invalid pattern in cross_references.{f}: '{v}'")
    
    # Generic list check for others
    for f in ["circulars", "forms", "hsn_codes", "sac_codes", "judgment_ids"]:
        if not isinstance(xr.get(f), list):
            errors.append(f"cross_references.{f} must be a list")
    
    if not str(xr.get("parent_chunk_id", "")).endswith("-overview"):
        errors.append(f"Invalid parent_chunk_id: {xr.get('parent_chunk_id')}")

    # --- 7. Retrieval Object ---
    ret = payload.get("retrieval", {})
    if ret.get("tax_type") != "CGST": errors.append("retrieval.tax_type must be 'CGST'")
    if ret.get("applicable_to") not in ["goods", "services", "both"]: errors.append("Invalid applicable_to")
    
    topics = ret.get("primary_topics", [])
    if not topics: errors.append("Missing primary_topics")
    for t in topics:
        if t not in PRIMARY_TOPICS: errors.append(f"Unknown topic: {t}")
        
    cats = ret.get("query_categories", [])
    if not cats: errors.append("Missing query_categories")
    for c in cats:
        if c not in QUERY_CATEGORIES: errors.append(f"Unknown category: {c}")

    # --- 8. Provenance Object ---
    prov = payload.get("provenance", {})
    if not prov.get("source_file"): errors.append("Missing provenance.source_file")
    if not ISO_DATE_PATTERN.match(str(prov.get("ingestion_date", ""))): errors.append("Invalid ingestion_date")
    if not prov.get("version"): errors.append("Missing provenance.version")

    # --- 9. Ext Object ---
    ext = payload.get("ext", {})
    if ext.get("act") != "CGST Act, 2017": errors.append("ext.act mismatch")
    if not ext.get("chapter_number"): errors.append("Missing chapter_number")
    if not ext.get("section_number"): errors.append("Missing section_number")
    
    sub_sec = str(ext.get("sub_section", ""))
    if not SUB_SECTION_PATTERN.match(sub_sec):
        errors.append(f"Invalid sub_section format: '{sub_sec}'")
        
    if ext.get("provision_type") not in PROVISION_TYPES:
        errors.append(f"Invalid provision_type: {ext.get('provision_type')}")
        
    if ext.get("hierarchy_level") not in [3, 4, 5, 6]:
        errors.append(f"Invalid hierarchy_level: {ext.get('hierarchy_level')}")

    # Conditional Component Checks
    for comp in ["proviso", "explanation", "illustration"]:
        has_flag = ext.get(f"has_{comp}")
        text_field = f"{comp}_text"
        if has_flag is True:
            if not ext.get(text_field): errors.append(f"has_{comp}=True but {text_field} is empty")
            if comp == "proviso" and not ext.get("proviso_implication"):
                errors.append("has_proviso=True but proviso_implication is empty")

    # Amendment History
    hist = ext.get("amendment_history", [])
    if not isinstance(hist, list):
        errors.append("amendment_history must be a list")
    else:
        for entry in hist:
            if not isinstance(entry, dict):
                errors.append(f"Amendment entry must be a dict, got {type(entry).__name__}")
            elif not entry.get("amendment_act"):
                errors.append("Amendment entry missing amendment_act")

    return errors

def run_validation():
    print(f"Connecting to Qdrant at {QDRANT_HOST}:{QDRANT_PORT}...")
    try:
        client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, api_key=QDRANT_API_KEY, https=False)
        client.get_collection(QDRANT_COLLECTION)
    except Exception as e:
        print(f"ERROR: {e}")
        return

    total = 0
    invalid_chunks = []
    
    print(f"Validating 'cgst_section' chunks in {QDRANT_COLLECTION}...\n")
    
    offset = None
    while True:
        response = client.scroll(
            collection_name=QDRANT_COLLECTION,
            scroll_filter=models.Filter(must=[models.FieldCondition(key="chunk_type", match=models.MatchValue(value="cgst_section"))]),
            limit=100,
            offset=offset
        )
        points, offset = response
        for p in points:
            total += 1
            errs = validate_chunk(p.payload)
            if errs:
                invalid_chunks.append({"id": p.payload.get("id"), "errors": errs})
        if not offset: break
            
    print("="*60)
    print("             CGST COMPREHENSIVE VALIDATION REPORT")
    print("="*60)
    print(f"Total Chunks Scanned: {total}")
    print(f"Total Valid:         {total - len(invalid_chunks)}")
    print(f"Total Invalid:       {len(invalid_chunks)}")
    print("="*60)
    
    if invalid_chunks:
        print("\nERRORS DETECTED:")
        for item in invalid_chunks[:20]: # Show first 20
            print(f"\nID: {item['id']}")
            for e in item['errors']:
                print(f"  - {e}")
        if len(invalid_chunks) > 20:
            print(f"\n... and {len(invalid_chunks)-20} more chunks have errors.")
    else:
        print("\n✅ SUCCESS: All chunks are 100% compliant with the schema!")

if __name__ == "__main__":
    run_validation()
