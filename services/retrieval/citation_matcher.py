import re
import logging

logger = logging.getLogger(__name__)


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
    Find matching judgments with optimized logic
    
    NEW LOGIC:
    1. If 2 party names extracted and BOTH match (one=petitioner, one=respondent) → EXACT (1.0)
    2. Citation exact match → EXACT (1.0), skip partial for citation
    3. Case number exact match (any individual number) → EXACT (1.0), skip partial for case_number
    4. Party exact match → EXACT (1.0), skip partial for that party
    5. Party partial match (only if no exact) → PARTIAL (0.65)
    
    Returns:
    {
        "exact_matches": [{score: 1.0, ...}],
        "partial_matches": [{score: 0.65, ...}],
        "substring_matches": [{score: 0.1, ...}]
    }
    """
    
    exact_matches = {}
    partial_matches = {}
    
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
    
    logger.info(f"🔍 Searching - Citation: '{citation_norm}', "
                f"Case Numbers: {case_nums_norm}, Parties: {party_names_norm}")
    
    # Track what matched to skip partial matching
    citation_exact_found = False
    case_num_exact_found = False
    party_exact_found = False
    
    # ========== EXACT MATCHES ==========
    
    for chunk in all_chunks:
        if chunk.get("chunk_type") != "judgment":
            continue
        
        metadata = chunk.get("metadata", {})
        external_id = metadata.get("external_id")
        if not external_id:
            continue
        
        db_citation = metadata.get("citation", "")
        db_case_num = metadata.get("case_number", "")
        db_petitioner = metadata.get("petitioner", "")
        db_respondent = metadata.get("respondent", "")
        
        # === CITATION EXACT MATCH ===
        if citation_norm:
            db_citation_norm = normalize_citation(db_citation)
            if citation_norm == db_citation_norm:
                citation_exact_found = True
                if external_id not in exact_matches:
                    exact_matches[external_id] = {
                        "external_id": external_id,
                        "matched_field": "citation",
                        "matched_value": db_citation,
                        "score": 1.0,
                        "chunks": [],
                        "citation": db_citation,
                        "case_number": db_case_num,
                        "petitioner": db_petitioner,
                        "respondent": db_respondent
                    }
                exact_matches[external_id]["chunks"].append(chunk)
                logger.debug(f"✅ Citation exact match: {external_id}")
        
        # === CASE NUMBER EXACT MATCH (any individual number) ===
        if case_nums_norm:
            db_case_num_core = extract_core_case_number(db_case_num)
            
            # Check if ANY extracted case number matches
            for case_norm in case_nums_norm:
                if case_norm and case_norm == db_case_num_core:
                    case_num_exact_found = True
                    if external_id not in exact_matches:
                        exact_matches[external_id] = {
                            "external_id": external_id,
                            "matched_field": "case_number",
                            "matched_value": db_case_num,
                            "score": 1.0,
                            "chunks": [],
                            "citation": db_citation,
                            "case_number": db_case_num,
                            "petitioner": db_petitioner,
                            "respondent": db_respondent
                        }
                    exact_matches[external_id]["chunks"].append(chunk)
                    logger.debug(f"✅ Case number exact match: {external_id}")
                    break
        
        # === BOTH PARTIES EXACT MATCH (2 names: one=petitioner, one=respondent) ===
        if len(party_names_norm) >= 2:
            db_petitioner_norm = normalize_party_name(db_petitioner)
            db_respondent_norm = normalize_party_name(db_respondent)
            
            if check_both_parties_match(party_names_norm, db_petitioner_norm, db_respondent_norm):
                party_exact_found = True
                if external_id not in exact_matches:
                    exact_matches[external_id] = {
                        "external_id": external_id,
                        "matched_field": "both_parties",
                        "matched_value": f"{db_petitioner} vs {db_respondent}",
                        "score": 1.0,
                        "chunks": [],
                        "citation": db_citation,
                        "case_number": db_case_num,
                        "petitioner": db_petitioner,
                        "respondent": db_respondent
                    }
                exact_matches[external_id]["chunks"].append(chunk)
                logger.debug(f"✅ Both parties exact match: {external_id}")
                continue  # Skip single party check
        
        # === SINGLE PARTY EXACT MATCH ===
        if party_names_norm and not party_exact_found:
            db_petitioner_norm = normalize_party_name(db_petitioner)
            db_respondent_norm = normalize_party_name(db_respondent)
            
            for party_norm in party_names_norm:
                if not party_norm or len(party_norm) < 3:
                    continue
                
                # Exact match in petitioner
                if party_norm == db_petitioner_norm:
                    party_exact_found = True
                    if external_id not in exact_matches:
                        exact_matches[external_id] = {
                            "external_id": external_id,
                            "matched_field": "petitioner",
                            "matched_value": db_petitioner,
                            "score": 1.0,
                            "chunks": [],
                            "citation": db_citation,
                            "case_number": db_case_num,
                            "petitioner": db_petitioner,
                            "respondent": db_respondent
                        }
                    exact_matches[external_id]["chunks"].append(chunk)
                    logger.debug(f"✅ Petitioner exact match: {external_id}")
                    break
                
                # Exact match in respondent
                if party_norm == db_respondent_norm:
                    party_exact_found = True
                    if external_id not in exact_matches:
                        exact_matches[external_id] = {
                            "external_id": external_id,
                            "matched_field": "respondent",
                            "matched_value": db_respondent,
                            "score": 1.0,
                            "chunks": [],
                            "citation": db_citation,
                            "case_number": db_case_num,
                            "petitioner": db_petitioner,
                            "respondent": db_respondent
                        }
                    exact_matches[external_id]["chunks"].append(chunk)
                    logger.debug(f"✅ Respondent exact match: {external_id}")
                    break
    
    # ========== PARTIAL MATCHES (only if no exact match) ==========
    
    if party_names_norm and not party_exact_found:
        for chunk in all_chunks:
            if chunk.get("chunk_type") != "judgment":
                continue
            
            metadata = chunk.get("metadata", {})
            external_id = metadata.get("external_id")
            
            if not external_id or external_id in exact_matches:
                continue
            
            db_citation = metadata.get("citation", "")
            db_case_num = metadata.get("case_number", "")
            db_petitioner = metadata.get("petitioner", "")
            db_respondent = metadata.get("respondent", "")
            db_petitioner_norm = normalize_party_name(db_petitioner)
            db_respondent_norm = normalize_party_name(db_respondent)
            
            for party_norm in party_names_norm:
                if not party_norm or len(party_norm) < 3:
                    continue
                
                # Substring in petitioner (but NOT exact)
                if party_norm in db_petitioner_norm and party_norm != db_petitioner_norm:
                    if external_id not in partial_matches:
                        partial_matches[external_id] = {
                            "external_id": external_id,
                            "matched_field": "petitioner",
                            "matched_value": db_petitioner,
                            "score": 0.65,
                            "chunks": [],
                            "citation": db_citation,
                            "case_number": db_case_num,
                            "petitioner": db_petitioner,
                            "respondent": db_respondent
                        }
                    partial_matches[external_id]["chunks"].append(chunk)
                    logger.debug(f"⚠️  Partial petitioner match: {external_id}")
                    break
                
                # Substring in respondent (but NOT exact)
                if party_norm in db_respondent_norm and party_norm != db_respondent_norm:
                    if external_id not in partial_matches:
                        partial_matches[external_id] = {
                            "external_id": external_id,
                            "matched_field": "respondent",
                            "matched_value": db_respondent,
                            "score": 0.65,
                            "chunks": [],
                            "citation": db_citation,
                            "case_number": db_case_num,
                            "petitioner": db_petitioner,
                            "respondent": db_respondent
                        }
                    partial_matches[external_id]["chunks"].append(chunk)
                    logger.debug(f"⚠️  Partial respondent match: {external_id}")
                    break
    
    # ========== BUILD RESULTS ==========
    
    exact_match_list = []
    for external_id, match_info in exact_matches.items():
        # Get ALL chunks for this judgment
        all_judgment_chunks = [
            c for c in all_chunks
            if c.get("chunk_type") == "judgment" and 
               c.get("metadata", {}).get("external_id") == external_id
        ]
        
        match_info["chunks"] = all_judgment_chunks
        exact_match_list.append(match_info)
        
        logger.info(f"✅ EXACT (1.0) - {match_info['matched_field'].upper()}='{match_info['matched_value']}', "
                   f"ID={external_id}, chunks={len(all_judgment_chunks)}")
    
    partial_match_list = []
    for external_id, match_info in partial_matches.items():
        all_judgment_chunks = [
            c for c in all_chunks
            if c.get("chunk_type") == "judgment" and 
               c.get("metadata", {}).get("external_id") == external_id
        ]
        
        match_info["chunks"] = all_judgment_chunks
        partial_match_list.append(match_info)
        
        logger.info(f"⚠️  PARTIAL (0.65) - {match_info['matched_field'].upper()}='{match_info['matched_value']}', "
                   f"ID={external_id}, chunks={len(all_judgment_chunks)}")
    
    return {
        "exact_matches": exact_match_list,
        "partial_matches": partial_match_list,
        "substring_matches": []
    }