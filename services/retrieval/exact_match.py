import re
from services.retrieval.citation_matcher import get_index

def exact_match(query, chunks):
    index = get_index(chunks)
    q = query.lower()

    # SECTION + SUBSECTION
    sec = re.search(r"section\s+(\d+)\s*\(?\s*(\d+)\s*\)?", q)
    if sec:
        key = (sec.group(1), sec.group(2))
        return index.by_section.get(key, [])

    # SECTION ONLY (Matching all subsections for that section)
    sec_only = re.search(r"section\s+(\d+)", q)
    if sec_only:
        # Collect all from by_section where section_number matches
        results = []
        for (s, ss), clist in index.by_section.items():
            if s == sec_only.group(1):
                results.extend(clist)
        return results

    # RULE
    rule = re.search(r"rule\s+(\d+[a-z]?)", q)
    if rule:
        return index.by_rule.get(rule.group(1), [])

    # HSN
    hsn = re.search(r"hsn\s+(\d+)", q)
    if hsn:
        return index.by_hsn.get(hsn.group(1), [])

    # SAC
    sac = re.search(r"sac\s+(\d+)", q)
    if sac:
        return index.by_sac.get(sac.group(1), [])

    # GSTAT FORM (matches "GSTAT Form 01", "form 01", "GSTAT-FORM-01", etc.)
    gstat_form = re.search(r"(?:gstat[-\s]*)?form[-\s]*(\d+)", q, re.IGNORECASE)
    if gstat_form:
        form_number = gstat_form.group(1).lstrip('0') or '0'
        # Try both '01' and '1' formats
        return index.by_gstat_form.get(form_number.zfill(2), []) or index.by_gstat_form.get(form_number, [])

    # GSTAT RULE (matches "GSTAT Rule 15", "rule 15", etc.)
    gstat_rule = re.search(r"(?:gstat[-\s]*)?rule[-\s]*(\d+)", q, re.IGNORECASE)
    if gstat_rule:
        rule_number = gstat_rule.group(1).lstrip('0') or '0'
        return index.by_gstat_rule.get(rule_number, [])

    # GSTAT CDR/Register (matches "GSTAT CDR 01", "CDR-01", etc.)
    gstat_cdr = re.search(r"(?:gstat[-\s]*)?(?:cdr|register)[-\s]*(\d+)", q, re.IGNORECASE)
    if gstat_cdr:
        cdr_number = gstat_cdr.group(1).lstrip('0') or '0'
        # Try both '05' and '5' formats
        return index.by_gstat_cdr.get(cdr_number.zfill(2), []) or index.by_gstat_cdr.get(cdr_number, [])

    # Council Meeting (matches "53rd council meeting", "53 meeting", etc.)
    council = re.search(r"(\d+)(?:st|nd|rd|th)?\s+council\s+(?:meeting|minutes)", q, re.IGNORECASE)
    if not council:
        council = re.search(r"council\s+meeting\s+(\d+)", q, re.IGNORECASE)
    
    if council:
        meeting_number = council.group(1)
        return index.by_council_meeting.get(meeting_number, [])

    return []
