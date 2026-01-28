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

    return []
