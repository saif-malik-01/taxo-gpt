import re
import logging

logger = logging.getLogger(__name__)


def normalize_citation(text: str) -> str:
    """Normalize citation for matching"""
    if not text:
        return ""
    
    text = text.lower()
    noise_words = ['no', 'number', 'of']
    for word in noise_words:
        text = re.sub(r'\b' + word + r'\b', '', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '', text)
    
    return text


def extract_core_case_number(text: str) -> str:
    """Extract core case number by removing date and normalizing"""
    if not text:
        return ""
    
    date_patterns = [
        r'\s+dated\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',
        r'\s+dt\.?\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',
        r'\s+on\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',
    ]
    
    text_without_date = text
    for pattern in date_patterns:
        text_without_date = re.sub(pattern, '', text_without_date, flags=re.IGNORECASE)
    
    return normalize_citation(text_without_date)


def normalize_party_name(name: str) -> str:
    """
    Normalize party name for matching
    Removes titles, legal suffixes, extra whitespace
    """
    if not name:
        return ""
    
    name = name.lower()
    
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
    
    # Remove legal suffixes
    suffixes = [
        r'\bpvt\.?\s*ltd\.?\b', r'\bprivate\s+limited\b',
        r'\bltd\.?\b', r'\blimited\b',
        r'\binc\.?\b', r'\bllc\.?\b', r'\bllp\.?\b',
        r'\bco\.?\b', r'\bcorp\.?\b',
    ]
    
    for suffix in suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
    # Clean up
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.strip()
    
    return name


def find_matching_judgments(extracted: dict, all_chunks: list) -> dict:
    """
    Find judgments matching citation, case number, OR party names in METADATA
    
    ALL MATCHING HAPPENS IN METADATA FIELDS:
    - metadata.citation
    - metadata.case_number
    - metadata.petitioner
    - metadata.respondent
    
    UPDATED PARTY MATCHING LOGIC:
    - ANY exact party name match → EXACT (1.0)
    - Only substring match (not exact) → PARTIAL (0.5)
    
    Returns:
    {
        "exact_matches": [{...}],     # Score 1.0
        "partial_matches": [{...}],   # Score 0.5
        "substring_matches": [{...}]  # Score 0.1 (not used)
    }
    """
    
    exact_matches = {}
    partial_matches = {}
    substring_chunk_ids = {}
    
    # Extract and normalize
    extracted_citation = extracted.get("citation", "")
    extracted_case_num = extracted.get("case_number", "")
    party_names = extracted.get("party_names", [])
    
    citation_norm = normalize_citation(extracted_citation)
    case_num_core = extract_core_case_number(extracted_case_num) if extracted_case_num else ""
    party_names_norm = [normalize_party_name(name) for name in party_names if name]
    
    if not citation_norm and not case_num_core and not party_names_norm:
        logger.info("No citation, case number, or party names extracted")
        return {"exact_matches": [], "partial_matches": [], "substring_matches": []}
    
    logger.info(f"🔍 Searching METADATA - Citation: '{citation_norm}', Case#: '{case_num_core}', "
                f"Parties: {party_names_norm} (count: {len(party_names_norm)})")
    
    # Track exact matches found
    citation_exact_found = False
    case_num_exact_found = False
    
    # ========== FIRST PASS: EXACT MATCHES IN METADATA ==========
    for chunk in all_chunks:
        if chunk.get("chunk_type") != "judgment":
            continue
        
        metadata = chunk.get("metadata", {})
        external_id = metadata.get("external_id")
        
        if not external_id:
            continue
        
        # Get metadata fields (NOT chunk text)
        db_citation = metadata.get("citation", "")
        db_case_num = metadata.get("case_number", "")
        db_petitioner = metadata.get("petitioner", "")
        db_respondent = metadata.get("respondent", "")
        
        # === CITATION EXACT MATCH (metadata.citation) ===
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
        
        # === CASE NUMBER EXACT MATCH (metadata.case_number) ===
        if case_num_core:
            db_case_num_core = extract_core_case_number(db_case_num)
            if case_num_core == db_case_num_core and len(case_num_core) > 0:
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
        
        # === PARTY NAMES EXACT MATCH (metadata.petitioner & metadata.respondent) ===
        # NEW LOGIC: ANY exact party name match = EXACT (1.0)
        
        if party_names_norm:
            db_petitioner_norm = normalize_party_name(db_petitioner)
            db_respondent_norm = normalize_party_name(db_respondent)
            
            # Track what matched
            matched_both = False
            matched_petitioner = False
            matched_respondent = False
            
            # Check if BOTH parties match (for 2+ names)
            if len(party_names_norm) >= 2:
                matched_count = 0
                for party_norm in party_names_norm:
                    if not party_norm or len(party_norm) < 3:
                        continue
                    if party_norm == db_petitioner_norm or party_norm == db_respondent_norm:
                        matched_count += 1
                
                if matched_count >= 2:
                    matched_both = True
            
            # If not both, check individual exact matches
            if not matched_both:
                for party_norm in party_names_norm:
                    if not party_norm or len(party_norm) < 3:
                        continue
                    
                    if party_norm == db_petitioner_norm:
                        matched_petitioner = True
                        break
                    elif party_norm == db_respondent_norm:
                        matched_respondent = True
                        break
            
            # Add to EXACT matches (score 1.0)
            if matched_both:
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
                logger.debug(f"✅ BOTH parties exact match in judgment {external_id}")
            
            elif matched_petitioner:
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
                logger.debug(f"✅ Petitioner exact match in judgment {external_id}")
            
            elif matched_respondent:
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
                logger.debug(f"✅ Respondent exact match in judgment {external_id}")
    
    # ========== SECOND PASS: PARTIAL MATCHES (0.5) IN METADATA ==========
    # Only SUBSTRING matches (not exact)
    
    if party_names_norm:
        for chunk in all_chunks:
            if chunk.get("chunk_type") != "judgment":
                continue
            
            metadata = chunk.get("metadata", {})
            external_id = metadata.get("external_id")
            
            # Skip if already exact matched
            if not external_id or external_id in exact_matches:
                continue
            
            db_citation = metadata.get("citation", "")
            db_case_num = metadata.get("case_number", "")
            db_petitioner = metadata.get("petitioner", "")
            db_respondent = metadata.get("respondent", "")
            db_petitioner_norm = normalize_party_name(db_petitioner)
            db_respondent_norm = normalize_party_name(db_respondent)
            
            # Check for SUBSTRING matches (not exact)
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
                            "score": 0.5,
                            "chunks": [],
                            "citation": db_citation,
                            "case_number": db_case_num,
                            "petitioner": db_petitioner,
                            "respondent": db_respondent
                        }
                    partial_matches[external_id]["chunks"].append(chunk)
                    logger.debug(f"⚠️  Substring match in metadata.petitioner for judgment {external_id}")
                    break
                
                # Substring in respondent (but NOT exact)
                elif party_norm in db_respondent_norm and party_norm != db_respondent_norm:
                    if external_id not in partial_matches:
                        partial_matches[external_id] = {
                            "external_id": external_id,
                            "matched_field": "respondent",
                            "matched_value": db_respondent,
                            "score": 0.5,
                            "chunks": [],
                            "citation": db_citation,
                            "case_number": db_case_num,
                            "petitioner": db_petitioner,
                            "respondent": db_respondent
                        }
                    partial_matches[external_id]["chunks"].append(chunk)
                    logger.debug(f"⚠️  Substring match in metadata.respondent for judgment {external_id}")
                    break
    
    # ========== BUILD FINAL RESULTS ==========
    
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
        
        logger.info(f"✅ EXACT MATCH (1.0) - {match_info['matched_field'].upper()}='{match_info['matched_value']}', "
                   f"external_id={external_id}, chunks={len(all_judgment_chunks)}")
    
    partial_match_list = []
    for external_id, match_info in partial_matches.items():
        # Get ALL chunks for this judgment
        all_judgment_chunks = [
            c for c in all_chunks
            if c.get("chunk_type") == "judgment" and 
               c.get("metadata", {}).get("external_id") == external_id
        ]
        
        match_info["chunks"] = all_judgment_chunks
        partial_match_list.append(match_info)
        
        logger.info(f"⚠️  PARTIAL MATCH (0.5) - {match_info['matched_field'].upper()}='{match_info['matched_value']}', "
                   f"external_id={external_id}, chunks={len(all_judgment_chunks)}")
    
    substring_match_list = list(substring_chunk_ids.values())
    
    if not exact_match_list and not partial_match_list and not substring_match_list:
        logger.info("❌ NO MATCHES in metadata")
    
    return {
        "exact_matches": exact_match_list,
        "partial_matches": partial_match_list,
        "substring_matches": substring_match_list
    }