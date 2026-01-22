import re

def exact_match(query, chunks):
    q = query.lower()

    # SECTION + SUBSECTION
    sec = re.search(r"section\s+(\d+)\s*\(?\s*(\d+)\s*\)?", q)
    if sec:
        return [
            c for c in chunks
            if c.get("section_number") == sec.group(1)
            and str(c.get("subsection")) == sec.group(2)
        ]

    # SECTION ONLY
    sec_only = re.search(r"section\s+(\d+)", q)
    if sec_only:
        return [
            c for c in chunks
            if c.get("section_number") == sec_only.group(1)
        ]

    # RULE
    rule = re.search(r"rule\s+(\d+[a-z]?)", q)
    if rule:
        return [
            c for c in chunks
            if c.get("rule_number") == rule.group(1)
        ]

    # HSN
    hsn = re.search(r"hsn\s+(\d+)", q)
    if hsn:
        return [
            c for c in chunks
            if c.get("metadata", {}).get("hsn_code") == hsn.group(1)
        ]

    # SAC
    sac = re.search(r"sac\s+(\d+)", q)
    if sac:
        return [
            c for c in chunks
            if c.get("metadata", {}).get("sac_code") == sac.group(1)
        ]

    return []
