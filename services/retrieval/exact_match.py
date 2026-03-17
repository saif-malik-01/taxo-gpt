import re
from services.retrieval.citation_matcher import get_index

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_JUDGMENT_KEYWORDS = {
    "judgment", "judgement", "case law", "case laws", "ruling", "rulings",
    "court", "order", "orders", "decision", "decisions", "held", "verdict",
}


def _wants_judgments(query: str) -> bool:
    """Return True when the query is asking for case-law / judgments."""
    q = query.lower()
    return any(kw in q for kw in _JUDGMENT_KEYWORDS)


def _build_judgment_match_info(chunk: dict) -> dict:
    """
    Build a lightweight match_info dict (compatible with
    create_complete_judgment_chunk) from a judgment chunk.
    """
    metadata = chunk.get("metadata", {})
    return {
        "external_id": str(metadata.get("external_id", "")).strip(),
        "score": 0.90,
        "matched_field": "section_metadata",
        "matched_value": metadata.get("section_number", ""),
        "citation": metadata.get("citation", ""),
        "case_number": metadata.get("case_number", ""),
        "petitioner": metadata.get("petitioner", ""),
        "respondent": metadata.get("respondent", ""),
        "title": metadata.get("title", ""),
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def exact_match(query: str, chunks: list) -> list:
    """
    Exact / metadata-driven retrieval.

    Returns a list of chunks.  For section-based queries the list
    will contain either:
      - statutory Act / Rule chunks  (normal section query), OR
      - judgment first-chunks        (when query asks for judgments)

    Other callers (hybrid.py) detect chunk_type == "judgment" and assemble
    complete judgment chunks from these first-chunks.
    """
    index = get_index(chunks)
    q = query.lower()
    want_judgments = _wants_judgments(q)

    # ------------------------------------------------------------------ #
    #  1.  SECTION + SUBSECTION                                           #
    # ------------------------------------------------------------------ #
    sec_sub = re.search(r"section\s+(\d+[a-z]?)\s*\(\s*(\d+)\s*\)", q)
    if sec_sub:
        sec_num = sec_sub.group(1)
        sub_num = sec_sub.group(2)
        key = (sec_num, sub_num)

        if want_judgments:
            # Return judgment chunks that reference this section
            judgment_chunks = list(
                index.judgments_by_section.get(sec_num.upper(), {}).values()
            )
            statutory = index.by_section.get(key, [])
            return judgment_chunks + statutory   # judgments first

        return index.by_section.get(key, [])

    # ------------------------------------------------------------------ #
    #  2.  SECTION ONLY                                                   #
    # ------------------------------------------------------------------ #
    sec_only = re.search(r"section\s+(\d+[a-z]?)", q)
    if sec_only:
        sec_num = sec_only.group(1)

        if want_judgments:
            # Primary: judgment chunks tagged with this section in metadata
            judgment_chunks = list(
                index.judgments_by_section.get(sec_num.upper(), {}).values()
            )
            if judgment_chunks:
                return judgment_chunks

            # Fallback: statutory chunks for that section (less ideal but still
            # relevant context the LLM can use)
            statutory = []
            for (s, ss), clist in index.by_section.items():
                if s == sec_num:
                    statutory.extend(clist)
            return statutory

        # Plain statutory lookup for all subsections
        results = []
        for (s, ss), clist in index.by_section.items():
            if s == sec_num:
                results.extend(clist)
        return results

    # ------------------------------------------------------------------ #
    #  3.  RULE                                                           #
    # ------------------------------------------------------------------ #
    rule = re.search(r"rule\s+(\d+[a-z]?)", q)
    if rule:
        rule_num = rule.group(1)
        if want_judgments:
            judgment_chunks = [
                chunk
                for chunk in index.judgments_unique.values()
                if str(chunk.get("metadata", {}).get("rule_number", "")).strip() == rule_num
            ]
            statutory = index.by_rule.get(rule_num, [])
            return judgment_chunks + statutory
        return index.by_rule.get(rule_num, [])

    # ------------------------------------------------------------------ #
    #  4.  HSN                                                            #
    # ------------------------------------------------------------------ #
    hsn = re.search(r"hsn\s+(\d+)", q)
    if hsn:
        return index.by_hsn.get(hsn.group(1), [])

    # ------------------------------------------------------------------ #
    #  5.  SAC                                                            #
    # ------------------------------------------------------------------ #
    sac = re.search(r"sac\s+(\d+)", q)
    if sac:
        return index.by_sac.get(sac.group(1), [])

    # ------------------------------------------------------------------ #
    #  6.  NOTIFICATION                                                   #
    # ------------------------------------------------------------------ #
    # Match: notification 32/2017, notif 32/2017, notification no 32/2017
    # Pattern designed to be robust to spaces: 32 / 2017
    notif = re.search(r"(?:notification|notif)(?:\s+no\.?)?\s*(\d+\s*/\s*\d{4})", q, re.IGNORECASE)
    # Generic number/year fallback (e.g. 32/2017)
    if not notif:
        notif = re.search(r"\b(\d{1,4}\s*/\s*20\d{2})\b", q)

    if notif:
        n_num = notif.group(1).replace(" ", "").lower()
        exact_results = index.by_notification.get(n_num, [])
        
        # Also find AMENDMENTS or notifications that MENTION this one
        mentions = index.mentions_notification.get(n_num, [])
        
        # Merge results, keeping exact matches at the top
        results = exact_results + [m for m in mentions if m not in exact_results]
        if results:
            return results

    # ------------------------------------------------------------------ #
    #  7.  GSTAT FORM                                                     #
    # ------------------------------------------------------------------ #
    gstat_form = re.search(r"(?:gstat[-\s]*)?form[-\s]*(\d+)", q, re.IGNORECASE)
    if gstat_form:
        form_number = gstat_form.group(1).lstrip("0") or "0"
        return (
            index.by_gstat_form.get(form_number.zfill(2), [])
            or index.by_gstat_form.get(form_number, [])
        )

    # ------------------------------------------------------------------ #
    #  7.  GSTAT RULE                                                     #
    # ------------------------------------------------------------------ #
    gstat_rule = re.search(r"(?:gstat[-\s]*)?rule[-\s]*(\d+)", q, re.IGNORECASE)
    if gstat_rule:
        rule_number = gstat_rule.group(1).lstrip("0") or "0"
        return index.by_gstat_rule.get(rule_number, [])

    # ------------------------------------------------------------------ #
    #  8.  GSTAT CDR / REGISTER                                          #
    # ------------------------------------------------------------------ #
    gstat_cdr = re.search(
        r"(?:gstat[-\s]*)?(?:cdr|register)[-\s]*(\d+)", q, re.IGNORECASE
    )
    if gstat_cdr:
        cdr_number = gstat_cdr.group(1).lstrip("0") or "0"
        return (
            index.by_gstat_cdr.get(cdr_number.zfill(2), [])
            or index.by_gstat_cdr.get(cdr_number, [])
        )

    # ------------------------------------------------------------------ #
    #  9.  COUNCIL MEETING                                                #
    # ------------------------------------------------------------------ #
    council = re.search(
        r"(\d+)(?:st|nd|rd|th)?\s+council\s+(?:meeting|minutes)", q, re.IGNORECASE
    )
    if not council:
        council = re.search(r"council\s+meeting\s+(\d+)", q, re.IGNORECASE)
    if council:
        meeting_number = council.group(1)
        return index.by_council_meeting.get(meeting_number, [])

    # ------------------------------------------------------------------ #
    # 10.  JUDGMENT FILTER-ONLY QUERIES (no section / rule mentioned)    #
    #      e.g. "Gujarat High Court judgments in favour of assessee 2022" #
    # ------------------------------------------------------------------ #
    if want_judgments:
        results_map = {}  # external_id -> chunk (to avoid duplicates)

        # Decision filter
        favour_assessee = re.search(
            r"(?:in\s+favour\s+of\s+(?:assessee|taxpayer|appellant)|pro[- ]assessee)",
            q, re.IGNORECASE
        )
        favour_dept = re.search(
            r"(?:in\s+favour\s+of\s+(?:department|revenue|govt)|pro[- ](?:department|revenue))",
            q, re.IGNORECASE
        )
        if favour_assessee:
            for eid, chunk in index.judgments_by_decision.get(
                "in favour of assessee", {}
            ).items():
                results_map[eid] = chunk
        elif favour_dept:
            for eid, chunk in index.judgments_by_decision.get(
                "in favour of department", {}
            ).items():
                results_map[eid] = chunk

        # Year filter
        year_match = re.search(r"\b(20\d{2})\b", q)
        if year_match:
            year_key = year_match.group(1)
            year_chunks = index.judgments_by_year.get(year_key, {})
            if results_map:
                # Intersect: keep only judgments that also match the year
                results_map = {
                    eid: c for eid, c in results_map.items() if eid in year_chunks
                }
                if not results_map:
                    results_map = dict(year_chunks)  # fallback to year-only
            else:
                results_map = dict(year_chunks)

        # State filter
        state_keywords = {
            "gujarat": "gujarat",
            "maharashtra": "maharashtra",
            "delhi": "delhi",
            "karnataka": "karnataka",
            "rajasthan": "rajasthan",
            "kerala": "kerala",
            "tamil nadu": "tamil nadu",
            "andhra pradesh": "andhra pradesh",
            "telangana": "telangana",
            "madhya pradesh": "madhya pradesh",
            "uttar pradesh": "uttar pradesh",
            "west bengal": "west bengal",
            "punjab": "punjab",
            "haryana": "haryana",
        }
        for keyword, state_key in state_keywords.items():
            if keyword in q:
                state_chunks = index.judgments_by_state.get(state_key, {})
                if results_map:
                    intersected = {
                        eid: c for eid, c in results_map.items()
                        if eid in state_chunks
                    }
                    results_map = intersected if intersected else results_map
                else:
                    results_map = dict(state_chunks)
                break  # only the first state match

        # Court type filter
        if "high court" in q or "hc" in q:
            court_chunks = index.judgments_by_court.get("high court", {})
            if results_map:
                intersected = {
                    eid: c for eid, c in results_map.items()
                    if eid in court_chunks
                }
                results_map = intersected if intersected else results_map
            else:
                results_map = dict(court_chunks)
        elif "supreme court" in q or "sc" in q:
            court_chunks = index.judgments_by_court.get("supreme court", {})
            if results_map:
                intersected = {
                    eid: c for eid, c in results_map.items()
                    if eid in court_chunks
                }
                results_map = intersected if intersected else results_map
            else:
                results_map = dict(court_chunks)

        if results_map:
            return list(results_map.values())

    return []
