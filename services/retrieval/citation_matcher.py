import re
import logging

logger = logging.getLogger(__name__)


class MetadataIndex:
    def __init__(self, chunks):
        self.by_section = {}
        self.by_rule = {}
        self.by_hsn = {}
        self.by_sac = {}
        self.by_citation = {}
        self.by_case_num = {}
        self.judgment_by_external_id = {}
        self.judgments_unique = {}  # external_id -> first_chunk (for metadata search)
        # GSTAT specific indices
        self.by_gstat_form = {}
        self.by_gstat_rule = {}
        self.by_gstat_cdr = {}
        self.by_council_meeting = {}
        self._build(chunks)

    def _build(self, chunks):
        count = 0
        judgments_count = 0
        for chunk in chunks:
            count += 1
            chunk_type = chunk.get("chunk_type")
            metadata = chunk.get("metadata", {})
            
            # 1. Statutory Indexing (Sections & Subsections)
            sec = chunk.get("section_number") or metadata.get("section_number")
            subsec = chunk.get("subsection") or metadata.get("subsection")
            
            if sec and isinstance(sec, (str, int)):
                s = str(sec).strip()
                ss = str(subsec).strip() if subsec else ""
                if not ss or ss.lower() in ["none", "null"]:
                    ss = ""
                
                key = (s, ss)
                if key not in self.by_section:
                    self.by_section[key] = []
                self.by_section[key].append(chunk)

            # 2. Rules Indexing
            rule = chunk.get("rule_number") or metadata.get("rule_number")
            if rule and isinstance(rule, (str, int)):
                r = str(rule).strip()
                if r not in self.by_rule:
                    self.by_rule[r] = []
                self.by_rule[r].append(chunk)

            # 3. HSN/SAC Indexing
            hsn = chunk.get("hsn_code") or metadata.get("hsn_code")
            if hsn and isinstance(hsn, (str, int)):
                h = str(hsn).strip()
                if h not in self.by_hsn:
                    self.by_hsn[h] = []
                self.by_hsn[h].append(chunk)
                
            sac = chunk.get("sac_code") or metadata.get("sac_code")
            if sac and isinstance(sac, (str, int)):
                sa = str(sac).strip()
                if sa not in self.by_sac:
                    self.by_sac[sa] = []
                self.by_sac[sa].append(chunk)

            # 4. Judgment Indexing
            if chunk_type == "judgment":
                ext_id = metadata.get("external_id")
                if ext_id:
                    eid = str(ext_id).strip()
                    
                    # Group chunks by external_id
                    if eid not in self.judgment_by_external_id:
                        self.judgment_by_external_id[eid] = []
                        self.judgments_unique[eid] = chunk  # Store first chunk for metadata
                        judgments_count += 1
                    self.judgment_by_external_id[eid].append(chunk)
                    
                    # Index by Citation
                    citation = metadata.get("citation")
                    if citation:
                        cit_norm = normalize_citation(citation)
                        if cit_norm:
                            if cit_norm not in self.by_citation:
                                self.by_citation[cit_norm] = []
                            if eid not in [c.get("metadata", {}).get("external_id") for c in self.by_citation[cit_norm]]:
                                self.by_citation[cit_norm].append(chunk)

                    # Index by Case Number
                    case_num = metadata.get("case_number")
                    if case_num:
                        cn_norm = extract_core_case_number(case_num)
                        if cn_norm:
                            if cn_norm not in self.by_case_num:
                                self.by_case_num[cn_norm] = []
                            if eid not in [c.get("metadata", {}).get("external_id") for c in self.by_case_num[cn_norm]]:
                                self.by_case_num[cn_norm].append(chunk)

            # 5. GSTAT Indexing (Forms, Rules, Registers/CDR)
            doc_type = chunk.get("doc_type")
            structure = chunk.get("structure", {})
            
            if doc_type == "Form":
                # Index GSTAT Forms
                form_number = structure.get("number")
                if form_number:
                    form_num_str = str(form_number).strip().lstrip('0') or '0'
                    if form_num_str not in self.by_gstat_form:
                        self.by_gstat_form[form_num_str] = []
                    self.by_gstat_form[form_num_str].append(chunk)
            
            elif doc_type == "Register" or (doc_type == "Other" and structure.get("title") and 
                                            ("register" in structure.get("title", "").lower() or 
                                             "cdr" in chunk.get("text", "").lower())):
                # Index GSTAT Registers/CDR
                cdr_number = structure.get("number")
                if cdr_number and cdr_number != "UNKNOWN":
                    cdr_num_str = str(cdr_number).strip().lstrip('0') or '0'
                    if cdr_num_str not in self.by_gstat_cdr:
                        self.by_gstat_cdr[cdr_num_str] = []
                    self.by_gstat_cdr[cdr_num_str].append(chunk)
            
            elif doc_type == "Rule" and chunk.get("parent_doc") == "GSTAT Rules 2025":
                # Index GSTAT Rules
                rule_number = structure.get("rule_number")
                if rule_number:
                    rule_num_str = str(rule_number).strip().lstrip('0') or '0'
                    if rule_num_str not in self.by_gstat_rule:
                        self.by_gstat_rule[rule_num_str] = []
                    self.by_gstat_rule[rule_num_str].append(chunk)

            elif doc_type == "Council Minutes":
                # Index Council Meetings
                meeting_number = structure.get("meeting_number")
                if meeting_number:
                    meeting_num_str = str(meeting_number).strip()
                    if meeting_num_str not in self.by_council_meeting:
                        self.by_council_meeting[meeting_num_str] = []
                    self.by_council_meeting[meeting_num_str].append(chunk)

        logger.info(f"🚀 Built MetadataIndex with {count} chunks")
        logger.info(f"   Sections: {len(self.by_section)} | Rules: {len(self.by_rule)} | Judgments: {judgments_count}")
        logger.info(f"   Citations: {len(self.by_citation)} | Case Numbers: {len(self.by_case_num)}")
        logger.info(f"   GSTAT Forms: {len(self.by_gstat_form)} | GSTAT Rules: {len(self.by_gstat_rule)} | GSTAT CDR: {len(self.by_gstat_cdr)} | Council Meetings: {len(self.by_council_meeting)}")


_INDEX_CACHE = None

def get_index(chunks):
    """Get or build the metadata index (singleton)"""
    global _INDEX_CACHE
    if _INDEX_CACHE is None:
        _INDEX_CACHE = MetadataIndex(chunks)
    return _INDEX_CACHE


def normalize_citation(text: str) -> str:
    """Normalize citation for matching"""
    if not text:
        return ""
    
    text = text.lower()
    
    # Remove noise words
    for word in ['no', 'number', 'of']:
        text = re.sub(r'\b' + word + r'\b', '', text)
    
    # Remove special characters
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '', text)
    
    return text


def extract_core_case_number(text: str) -> str:
    """
    Extract core case number by removing date and normalizing
    Also removes "& ORS." and similar suffixes
    """
    if not text:
        return ""
    
    # Remove "& ORS." and variations
    text = re.sub(r'\s*[&,]\s*ors\.?', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*&\s*others?', '', text, flags=re.IGNORECASE)
    
    # Remove date patterns
    date_patterns = [
        r'\s+dated\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',
        r'\s+dt\.?\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',
        r'\s+on\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',
    ]
    
    for pattern in date_patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    
    return normalize_citation(text)


def normalize_party_name(name: str) -> str:
    """
    Normalize party name - removes titles, suffixes, & ORS., Pvt Ltd, etc.
    
    Examples:
    "Safari Retreat Pvt. Ltd." → "safari retreat"
    "Mr. Sharma & Ors." → "sharma"
    "ABC Private Limited" → "abc"
    """
    if not name:
        return ""
    
    name = name.lower()
    
    # Remove "& ORS." and variations
    name = re.sub(r'\s*[&,]\s*ors\.?', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*&\s*others?', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*and\s+others?', '', name, flags=re.IGNORECASE)
    
    # Remove titles
    titles = [
        r'\bmr\.?\b', r'\bmrs\.?\b', r'\bms\.?\b', r'\bmiss\.?\b',
        r'\bdr\.?\b', r'\bprof\.?\b', r'\bhon\.?\b',
        r'\bjustice\.?\b', r'\bj\.?\b',
        r'\bsri\.?\b', r'\bsmt\.?\b', r'\bshri\.?\b',
        r'\bm/s\.?\b', r'\bmessrs\.?\b',
    ]
    
    for title in titles:
        name = re.sub(title, '', name, flags=re.IGNORECASE)
    
    # Remove legal suffixes (including variations)
    suffixes = [
        r'\bpvt\.?\s*ltd\.?', r'\bprivate\s+limited\b',
        r'\bltd\.?\b', r'\blimited\b',
        r'\binc\.?\b', r'\bincorporated\b',
        r'\bllc\.?\b', r'\bllp\.?\b',
        r'\bco\.?\b', r'\bcompany\b',
        r'\bcorp\.?\b', r'\bcorporation\b',
    ]
    
    for suffix in suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
    # Clean up
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name


def check_both_parties_match(party_names_norm: list, db_petitioner_norm: str, 
                              db_respondent_norm: str) -> bool:
    """
    Check if BOTH extracted party names match (one as petitioner, one as respondent)
    
    This is for cases like: "Safari Retreat vs State of Karnataka"
    Where we extract 2 names and both should match
    """
    if len(party_names_norm) < 2:
        return False
    
    # Get first 2 names
    name1 = party_names_norm[0]
    name2 = party_names_norm[1]
    
    if not name1 or not name2 or len(name1) < 3 or len(name2) < 3:
        return False
    
    # Check if name1=petitioner AND name2=respondent
    if name1 == db_petitioner_norm and name2 == db_respondent_norm:
        return True
    
    # Check if name1=respondent AND name2=petitioner
    if name1 == db_respondent_norm and name2 == db_petitioner_norm:
        return True
    
    return False


def find_matching_judgments(extracted: dict, all_chunks: list) -> dict:
    """
    Find matching judgments using pre-built index for speed
    """
    exact_matches = {}
    partial_matches = {}
    
    # Get index
    index = get_index(all_chunks)
    
    # Extract and normalize
    citation_norm = normalize_citation(extracted.get("citation", ""))
    
    # Handle multiple case numbers
    case_numbers = extracted.get("case_numbers", [])
    full_case_number = extracted.get("full_case_number", "")
    
    # Normalize all case numbers
    case_nums_norm = []
    if case_numbers:
        case_nums_norm = [extract_core_case_number(cn) for cn in case_numbers if cn]
    elif full_case_number:
        case_nums_norm = [extract_core_case_number(full_case_number)]
    
    party_names = extracted.get("party_names", [])
    party_names_norm = [normalize_party_name(p) for p in party_names if p]
    
    if not citation_norm and not case_nums_norm and not party_names_norm:
        logger.info("No citation, case numbers, or party names extracted")
        return {"exact_matches": [], "partial_matches": [], "substring_matches": []}
    
    logger.info(f"🔍 Searching using INDEX - Citation: '{citation_norm}', "
                f"Case Numbers: {case_nums_norm}, Parties: {party_names_norm}")
    
    # Track what matched to skip partial matching
    party_exact_found = False

    # helper to add to exact_matches
    def add_exact_match(chunk, field_name, matched_value):
        metadata = chunk.get("metadata", {})
        external_id = metadata.get("external_id")
        if not external_id or external_id in exact_matches:
            return
        
        exact_matches[external_id] = {
            "external_id": external_id,
            "matched_field": field_name,
            "matched_value": matched_value,
            "score": 1.0,
            "chunks": [], # Will be populated at the end
            "citation": metadata.get("citation", ""),
            "case_number": metadata.get("case_number", ""),
            "petitioner": metadata.get("petitioner", ""),
            "respondent": metadata.get("respondent", ""),
            "title": metadata.get("title", "")
        }

    # ========== 1. CITATION EXACT MATCH (via Index) ==========
    if citation_norm:
        matches = index.by_citation.get(citation_norm, [])
        for chunk in matches:
            add_exact_match(chunk, "citation", chunk.get("metadata", {}).get("citation"))

    # ========== 2. CASE NUMBER EXACT MATCH (via Index) ==========
    if case_nums_norm:
        for cn_norm in case_nums_norm:
            if not cn_norm: continue
            matches = index.by_case_num.get(cn_norm, [])
            for chunk in matches:
                add_exact_match(chunk, "case_number", chunk.get("metadata", {}).get("case_number"))

    # ========== 3. PARTY MATCHES (Scan unique judgments instead of all chunks) ==========
    # This is much faster than scanning all_chunks
    for external_id, chunk in index.judgments_unique.items():
        if external_id in exact_matches:
            continue
            
        metadata = chunk.get("metadata", {})
        db_petitioner = metadata.get("petitioner", "")
        db_respondent = metadata.get("respondent", "")
        
        db_petitioner_norm = normalize_party_name(db_petitioner)
        db_respondent_norm = normalize_party_name(db_respondent)

        # === BOTH PARTIES EXACT MATCH ===
        if len(party_names_norm) >= 2:
            if check_both_parties_match(party_names_norm, db_petitioner_norm, db_respondent_norm):
                party_exact_found = True
                add_exact_match(chunk, "both_parties", f"{db_petitioner} vs {db_respondent}")
                continue

        # === SINGLE PARTY EXACT MATCH ===
        if party_names_norm:
            for party_norm in party_names_norm:
                if not party_norm or len(party_norm) < 3:
                    continue
                
                if party_norm == db_petitioner_norm:
                    party_exact_found = True
                    add_exact_match(chunk, "petitioner", db_petitioner)
                    break
                elif party_norm == db_respondent_norm:
                    party_exact_found = True
                    add_exact_match(chunk, "respondent", db_respondent)
                    break

    # ========== 4. PARTIAL MATCHES (Substring) ==========
    if party_names_norm and not party_exact_found:
        for external_id, chunk in index.judgments_unique.items():
            if external_id in exact_matches or external_id in partial_matches:
                continue
                
            metadata = chunk.get("metadata", {})
            db_petitioner = metadata.get("petitioner", "")
            db_respondent = metadata.get("respondent", "")
            
            db_petitioner_norm = normalize_party_name(db_petitioner)
            db_respondent_norm = normalize_party_name(db_respondent)
            
            for party_norm in party_names_norm:
                if not party_norm or len(party_norm) < 3:
                    continue
                
                # Substring match (but not exact - exact handled above)
                if (party_norm in db_petitioner_norm) or (party_norm in db_respondent_norm):
                    partial_matches[external_id] = {
                        "external_id": external_id,
                        "matched_field": "party_substring",
                        "matched_value": f"{db_petitioner} / {db_respondent}",
                        "score": 0.65,
                        "chunks": [],
                        "citation": metadata.get("citation", ""),
                        "case_number": metadata.get("case_number", ""),
                        "petitioner": db_petitioner,
                        "respondent": db_respondent,
                        "title": metadata.get("title", "")
                    }
                    break

    # ========== BUILD FINAL LISTS (Attach all chunks) ==========
    exact_match_list = []
    for eid, match_info in exact_matches.items():
        match_info["chunks"] = index.judgment_by_external_id.get(eid, [])
        exact_match_list.append(match_info)
        logger.info(f"✅ EXACT (1.0) - {match_info['matched_field'].upper()}='{match_info['matched_value']}', ID={eid}")

    partial_match_list = []
    for eid, match_info in partial_matches.items():
        match_info["chunks"] = index.judgment_by_external_id.get(eid, [])
        partial_match_list.append(match_info)
        logger.info(f"⚠️  PARTIAL (0.65) - {match_info['matched_field'].upper()}='{match_info['matched_value']}', ID={eid}")

    return {
        "exact_matches": exact_match_list,
        "partial_matches": partial_match_list,
        "substring_matches": []
    }
