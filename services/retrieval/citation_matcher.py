# import re
# import logging

# logger = logging.getLogger(__name__)


# def normalize_citation(text: str) -> str:
#     """
#     Normalize citation/case number for matching
#     Removes spaces, brackets, dots, slashes, special characters
    
#     Examples:
#     "2023 (12) TMI 456" → "202312tmi456"
#     "Civil Appeal No. 1234/2023" → "civilappealno12342023"
#     "C.A. 1234 of 2023" → "ca12342023"
#     "2017 Taxo.online 42" → "2017taxoonline42"
#     """
#     if not text:
#         return ""
    
#     # Convert to lowercase
#     text = text.lower()
    
#     # Remove common words that add noise
#     noise_words = ['no', 'number', 'of']
#     for word in noise_words:
#         text = re.sub(r'\b' + word + r'\b', '', text)
    
#     # Remove all special characters and punctuation
#     text = re.sub(r'[^\w\s]', '', text)
    
#     # Remove all whitespace
#     text = re.sub(r'\s+', '', text)
    
#     return text


# def extract_core_case_number(text: str) -> str:
#     """
#     Extract the core case number by removing date and everything after it.
#     Then normalize it.
    
#     This allows matching with or without the date portion:
#     Input: "WP(C). No. 34021 of 2017 (C) dated 26.10.2017"
#     Step 1: Remove date → "WP(C). No. 34021 of 2017 (C)"
#     Step 2: Normalize → "wpc34021of2017c"
    
#     Input: "WP(C). No. 34021 of 2017 (C)"
#     Step 1: No date to remove → "WP(C). No. 34021 of 2017 (C)"
#     Step 2: Normalize → "wpc34021of2017c"
    
#     Both result in the same normalized string!
    
#     Date patterns matched (case-insensitive):
#     - "dated DD.MM.YYYY"
#     - "dt. DD.MM.YYYY"
#     - "dt DD.MM.YYYY"
#     - "on DD.MM.YYYY"
#     - Supports separators: . - /
#     """
#     if not text:
#         return ""
    
#     # Remove date patterns and everything after them
#     # Pattern explanation: Match "dated"/"dt"/"on" followed by a date, then everything after
#     date_patterns = [
#         r'\s+dated\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',  # dated 26.10.2017 ...
#         r'\s+dt\.?\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',   # dt. 26.10.2017 ... or dt 26.10.2017
#         r'\s+on\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',      # on 26.10.2017 ...
#     ]
    
#     text_without_date = text
#     for pattern in date_patterns:
#         text_without_date = re.sub(pattern, '', text_without_date, flags=re.IGNORECASE)
    
#     # Now normalize the case number without date
#     return normalize_citation(text_without_date)


# def find_matching_judgments(extracted: dict, all_chunks: list) -> dict:
#     """
#     Find judgments matching extracted citation AND/OR case number
    
#     LOGIC FOR CASE NUMBERS:
#     - Core case number must match (ignoring dates)
#     - "WP(C). No. 34021 of 2017 (C)" == "WP(C). No. 34021 of 2017 (C) dated 26.10.2017" → EXACT MATCH
#     - Date portion is completely ignored for matching
    
#     SCORING:
#     - Exact matches: score 1.0
#     - Substring matches: score 0.1
    
#     OPTIMIZATION:
#     - If exact match found for citation → skip substring matching for citation
#     - If exact match found for case_number → skip substring matching for case_number
#     - These checks are independent
    
#     Returns:
#     {
#         "exact_matches": [
#             {
#                 "external_id": "J001",
#                 "chunks": [all chunks of that judgment],
#                 "matched_field": "citation" or "case_number",
#                 "matched_value": "original value from metadata",
#                 "score": 1.0
#             }
#         ],
#         "substring_matches": [
#             {
#                 "chunk": chunk_object,
#                 "match_type": "substring_citation" or "substring_case_number",
#                 "matched_value": "original value from metadata",
#                 "score": 0.1
#             }
#         ]
#     }
#     """
    
#     exact_matches = {}  # external_id -> match info
#     substring_chunk_ids = {}  # chunk_id -> match info
    
#     # Get extracted values
#     extracted_citation = extracted.get("citation", "")
#     extracted_case_num = extracted.get("case_number", "")
    
#     # Normalize citation (standard normalization)
#     citation_norm = normalize_citation(extracted_citation)
    
#     # For case number, extract core (without date) and normalize
#     case_num_core = extract_core_case_number(extracted_case_num) if extracted_case_num else ""
    
#     # If nothing extracted, return empty
#     if not citation_norm and not case_num_core:
#         logger.info("No citation or case number extracted from query")
#         return {"exact_matches": [], "substring_matches": []}
    
#     logger.info(f"Searching for - Citation: '{extracted_citation}' (normalized: '{citation_norm}'), "
#                 f"Case Number: '{extracted_case_num}' (core: '{case_num_core}')")
    
#     # Track if we found exact matches
#     citation_exact_found = False
#     case_num_exact_found = False
    
#     # ========== FIRST PASS: CHECK FOR EXACT MATCHES ==========
#     for chunk in all_chunks:
#         if chunk.get("chunk_type") != "judgment":
#             continue
        
#         metadata = chunk.get("metadata", {})
#         external_id = metadata.get("external_id")
        
#         if not external_id:
#             continue
        
#         db_citation = metadata.get("citation", "")
#         db_case_num = metadata.get("case_number", "")
        
#         # ===== EXACT MATCH - Citation =====
#         if citation_norm:
#             db_citation_norm = normalize_citation(db_citation)
            
#             if citation_norm == db_citation_norm:
#                 citation_exact_found = True
#                 if external_id not in exact_matches:
#                     exact_matches[external_id] = {
#                         "external_id": external_id,
#                         "matched_field": "citation",
#                         "matched_value": db_citation,
#                         "score": 1.0,
#                         "chunks": []
#                     }
#                 exact_matches[external_id]["chunks"].append(chunk)
#                 logger.debug(f"Exact citation match: '{db_citation}' for external_id '{external_id}'")
        
#         # ===== EXACT MATCH - Case Number (DATE-AGNOSTIC) =====
#         if case_num_core:
#             # Extract core from database case number (remove date if present)
#             db_case_num_core = extract_core_case_number(db_case_num)
            
#             # Compare core case numbers (both without dates, both normalized)
#             if case_num_core == db_case_num_core and len(case_num_core) > 0:
#                 case_num_exact_found = True
#                 if external_id not in exact_matches:
#                     exact_matches[external_id] = {
#                         "external_id": external_id,
#                         "matched_field": "case_number",
#                         "matched_value": db_case_num,  # Return original DB value (with date if present)
#                         "score": 1.0,
#                         "chunks": []
#                     }
#                 exact_matches[external_id]["chunks"].append(chunk)
#                 logger.debug(f"Exact case number match (date-agnostic): Query core='{case_num_core}' matches "
#                            f"DB '{db_case_num}' (core='{db_case_num_core}') for external_id '{external_id}'")
    
#     # ========== SECOND PASS: SUBSTRING MATCHES (ONLY IF NO EXACT MATCH) ==========
    
#     # Log skipping behavior
#     if citation_exact_found:
#         logger.info("✅ Exact citation match found - SKIPPING substring citation matching")
    
#     if case_num_exact_found:
#         logger.info("✅ Exact case number match found - SKIPPING substring case_number matching")
    
#     # Only do substring matching if exact match NOT found for that specific field
#     if not citation_exact_found or not case_num_exact_found:
#         for chunk in all_chunks:
#             if chunk.get("chunk_type") != "judgment":
#                 continue
            
#             metadata = chunk.get("metadata", {})
#             external_id = metadata.get("external_id")
            
#             if not external_id:
#                 continue
            
#             # Skip if this judgment already has exact match
#             if external_id in exact_matches:
#                 continue
            
#             db_citation = metadata.get("citation", "")
#             db_case_num = metadata.get("case_number", "")
            
#             # ===== SUBSTRING MATCH - Citation =====
#             if not citation_exact_found and citation_norm and len(citation_norm) > 5:
#                 db_citation_norm = normalize_citation(db_citation)
                
#                 if citation_norm in db_citation_norm or db_citation_norm in citation_norm:
#                     if chunk["id"] not in substring_chunk_ids:
#                         substring_chunk_ids[chunk["id"]] = {
#                             "chunk": chunk,
#                             "match_type": "substring_citation",
#                             "matched_value": db_citation,
#                             "score": 0.1  # Substring bonus score
#                         }
#                         logger.debug(f"Substring citation match: '{db_citation}' (score: 0.1)")
#                     continue  # Don't check case_number if citation matched
            
#             # ===== SUBSTRING MATCH - Case Number =====
#             if not case_num_exact_found and case_num_core and len(case_num_core) > 5:
#                 # Extract core from database case number
#                 db_case_num_core = extract_core_case_number(db_case_num)
                
#                 # Check substring match on core case numbers
#                 if case_num_core in db_case_num_core or db_case_num_core in case_num_core:
#                     if chunk["id"] not in substring_chunk_ids:
#                         substring_chunk_ids[chunk["id"]] = {
#                             "chunk": chunk,
#                             "match_type": "substring_case_number",
#                             "matched_value": db_case_num,
#                             "score": 0.1  # Substring bonus score
#                         }
#                         logger.debug(f"Substring case number match: '{db_case_num}' (score: 0.1)")
    
#     # ========== GET ALL CHUNKS FOR EXACT MATCHES ==========
    
#     exact_match_list = []
#     for external_id, match_info in exact_matches.items():
#         # Find ALL chunks belonging to this judgment
#         all_judgment_chunks = [
#             c for c in all_chunks
#             if c.get("chunk_type") == "judgment" and 
#                c.get("metadata", {}).get("external_id") == external_id
#         ]
        
#         # Update with complete chunk list
#         match_info["chunks"] = all_judgment_chunks
#         exact_match_list.append(match_info)
        
#         logger.info(f"✅ EXACT MATCH (score: 1.0) - {match_info['matched_field'].upper()}='{match_info['matched_value']}', "
#                    f"external_id={external_id}, total_chunks={len(all_judgment_chunks)}")
    
#     substring_match_list = list(substring_chunk_ids.values())
    
#     if substring_match_list:
#         logger.info(f"⚠️  SUBSTRING MATCHES (score: 0.1) - Found {len(substring_match_list)} chunks")
    
#     if not exact_match_list and not substring_match_list:
#         logger.info("❌ NO MATCHES - Citation/case number not found in database")
    
#     return {
#         "exact_matches": exact_match_list,
#         "substring_matches": substring_match_list
#     }


import re
import logging

logger = logging.getLogger(__name__)


def normalize_citation(text: str) -> str:
    """
    Normalize citation/case number for matching
    Removes spaces, brackets, dots, slashes, special characters
    
    Examples:
    "2023 (12) TMI 456" → "202312tmi456"
    "Civil Appeal No. 1234/2023" → "civilappealno12342023"
    "C.A. 1234 of 2023" → "ca12342023"
    "2017 Taxo.online 42" → "2017taxoonline42"
    """
    if not text:
        return ""
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove common words that add noise
    noise_words = ['no', 'number', 'of']
    for word in noise_words:
        text = re.sub(r'\b' + word + r'\b', '', text)
    
    # Remove all special characters and punctuation
    text = re.sub(r'[^\w\s]', '', text)
    
    # Remove all whitespace
    text = re.sub(r'\s+', '', text)
    
    return text


def extract_core_case_number(text: str) -> str:
    """
    Extract the core case number by removing date and everything after it.
    Then normalize it.
    
    This allows matching with or without the date portion:
    Input: "WP(C). No. 34021 of 2017 (C) dated 26.10.2017"
    Step 1: Remove date → "WP(C). No. 34021 of 2017 (C)"
    Step 2: Normalize → "wpc34021of2017c"
    
    Input: "WP(C). No. 34021 of 2017 (C)"
    Step 1: No date to remove → "WP(C). No. 34021 of 2017 (C)"
    Step 2: Normalize → "wpc34021of2017c"
    
    Both result in the same normalized string!
    
    Date patterns matched (case-insensitive):
    - "dated DD.MM.YYYY"
    - "dt. DD.MM.YYYY"
    - "dt DD.MM.YYYY"
    - "on DD.MM.YYYY"
    - Supports separators: . - /
    """
    if not text:
        return ""
    
    # Remove date patterns and everything after them
    date_patterns = [
        r'\s+dated\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',
        r'\s+dt\.?\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',
        r'\s+on\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',
    ]
    
    text_without_date = text
    for pattern in date_patterns:
        text_without_date = re.sub(pattern, '', text_without_date, flags=re.IGNORECASE)
    
    # Now normalize the case number without date
    return normalize_citation(text_without_date)


def find_matching_judgments(extracted: dict, all_chunks: list) -> dict:
    """
    Find judgments matching extracted citation AND/OR case number
    
    LOGIC FOR CASE NUMBERS:
    - Core case number must match (ignoring dates)
    - "WP(C). No. 34021 of 2017 (C)" == "WP(C). No. 34021 of 2017 (C) dated 26.10.2017" → EXACT MATCH
    - Date portion is completely ignored for matching
    
    SCORING:
    - Exact matches: score 1.0
    - Substring matches: score 0.1
    
    FOR EXACT MATCHES:
    - The matched citation/case_number is prepended to the complete judgment text
    - This prepended text is stored in 'judgment_text_for_llm' field
    
    OPTIMIZATION:
    - If exact match found for citation → skip substring matching for citation
    - If exact match found for case_number → skip substring matching for case_number
    - These checks are independent
    
    Returns:
    {
        "exact_matches": [
            {
                "external_id": "J001",
                "chunks": [all chunks of that judgment],
                "matched_field": "citation" or "case_number",
                "matched_value": "original value from metadata",
                "score": 1.0,
                "judgment_text_for_llm": "Citation: 2017 Taxo.online 42\n\n[Complete judgment text...]"
            }
        ],
        "substring_matches": [
            {
                "chunk": chunk_object,
                "match_type": "substring_citation" or "substring_case_number",
                "matched_value": "original value from metadata",
                "score": 0.1
            }
        ]
    }
    """
    
    exact_matches = {}  # external_id -> match info
    substring_chunk_ids = {}  # chunk_id -> match info
    
    # Get extracted values
    extracted_citation = extracted.get("citation", "")
    extracted_case_num = extracted.get("case_number", "")
    
    # Normalize citation (standard normalization)
    citation_norm = normalize_citation(extracted_citation)
    
    # For case number, extract core (without date) and normalize
    case_num_core = extract_core_case_number(extracted_case_num) if extracted_case_num else ""
    
    # If nothing extracted, return empty
    if not citation_norm and not case_num_core:
        logger.info("No citation or case number extracted from query")
        return {"exact_matches": [], "substring_matches": []}
    
    logger.info(f"Searching for - Citation: '{extracted_citation}' (normalized: '{citation_norm}'), "
                f"Case Number: '{extracted_case_num}' (core: '{case_num_core}')")
    
    # Track if we found exact matches
    citation_exact_found = False
    case_num_exact_found = False
    
    # ========== FIRST PASS: CHECK FOR EXACT MATCHES ==========
    for chunk in all_chunks:
        if chunk.get("chunk_type") != "judgment":
            continue
        
        metadata = chunk.get("metadata", {})
        external_id = metadata.get("external_id")
        
        if not external_id:
            continue
        
        db_citation = metadata.get("citation", "")
        db_case_num = metadata.get("case_number", "")
        
        # ===== EXACT MATCH - Citation =====
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
                        "chunks": []
                    }
                exact_matches[external_id]["chunks"].append(chunk)
                logger.debug(f"Exact citation match: '{db_citation}' for external_id '{external_id}'")
        
        # ===== EXACT MATCH - Case Number (DATE-AGNOSTIC) =====
        if case_num_core:
            # Extract core from database case number (remove date if present)
            db_case_num_core = extract_core_case_number(db_case_num)
            
            # Compare core case numbers (both without dates, both normalized)
            if case_num_core == db_case_num_core and len(case_num_core) > 0:
                case_num_exact_found = True
                if external_id not in exact_matches:
                    exact_matches[external_id] = {
                        "external_id": external_id,
                        "matched_field": "case_number",
                        "matched_value": db_case_num,
                        "score": 1.0,
                        "chunks": []
                    }
                exact_matches[external_id]["chunks"].append(chunk)
                logger.debug(f"Exact case number match (date-agnostic): Query core='{case_num_core}' matches "
                           f"DB '{db_case_num}' (core='{db_case_num_core}') for external_id '{external_id}'")
    
    # ========== SECOND PASS: SUBSTRING MATCHES (ONLY IF NO EXACT MATCH) ==========
    
    # Log skipping behavior
    if citation_exact_found:
        logger.info("✅ Exact citation match found - SKIPPING substring citation matching")
    
    if case_num_exact_found:
        logger.info("✅ Exact case number match found - SKIPPING substring case_number matching")
    
    # Only do substring matching if exact match NOT found for that specific field
    if not citation_exact_found or not case_num_exact_found:
        for chunk in all_chunks:
            if chunk.get("chunk_type") != "judgment":
                continue
            
            metadata = chunk.get("metadata", {})
            external_id = metadata.get("external_id")
            
            if not external_id:
                continue
            
            # Skip if this judgment already has exact match
            if external_id in exact_matches:
                continue
            
            db_citation = metadata.get("citation", "")
            db_case_num = metadata.get("case_number", "")
            
            # ===== SUBSTRING MATCH - Citation =====
            if not citation_exact_found and citation_norm and len(citation_norm) > 5:
                db_citation_norm = normalize_citation(db_citation)
                
                if citation_norm in db_citation_norm or db_citation_norm in citation_norm:
                    if chunk["id"] not in substring_chunk_ids:
                        substring_chunk_ids[chunk["id"]] = {
                            "chunk": chunk,
                            "match_type": "substring_citation",
                            "matched_value": db_citation,
                            "score": 0.1
                        }
                        logger.debug(f"Substring citation match: '{db_citation}' (score: 0.1)")
                    continue
            
            # ===== SUBSTRING MATCH - Case Number =====
            if not case_num_exact_found and case_num_core and len(case_num_core) > 5:
                db_case_num_core = extract_core_case_number(db_case_num)
                
                if case_num_core in db_case_num_core or db_case_num_core in case_num_core:
                    if chunk["id"] not in substring_chunk_ids:
                        substring_chunk_ids[chunk["id"]] = {
                            "chunk": chunk,
                            "match_type": "substring_case_number",
                            "matched_value": db_case_num,
                            "score": 0.1
                        }
                        logger.debug(f"Substring case number match: '{db_case_num}' (score: 0.1)")
    
    # ========== PREPARE EXACT MATCHES WITH PREPENDED TEXT ==========
    
    exact_match_list = []
    for external_id, match_info in exact_matches.items():
        # Find ALL chunks belonging to this judgment
        all_judgment_chunks = [
            c for c in all_chunks
            if c.get("chunk_type") == "judgment" and 
               c.get("metadata", {}).get("external_id") == external_id
        ]
        
        # Build complete judgment text from all chunks
        judgment_texts = [chunk.get('text', '') for chunk in all_judgment_chunks]
        complete_judgment = "\n\n".join(judgment_texts)
        
        # Prepend matched citation/case_number to judgment text
        matched_field = match_info['matched_field']
        matched_value = match_info['matched_value']
        
        if matched_field == "citation":
            header = f"Citation: {matched_value}\n\n"
        elif matched_field == "case_number":
            header = f"Case Number: {matched_value}\n\n"
        else:
            header = ""
        
        judgment_text_for_llm = header + complete_judgment
        
        # Update match info
        match_info["chunks"] = all_judgment_chunks
        match_info["judgment_text_for_llm"] = judgment_text_for_llm
        exact_match_list.append(match_info)
        
        logger.info(f"✅ EXACT MATCH (score: 1.0) - {matched_field.upper()}='{matched_value}', "
                   f"external_id={external_id}, total_chunks={len(all_judgment_chunks)}, "
                   f"text_length={len(judgment_text_for_llm)} chars")
    
    substring_match_list = list(substring_chunk_ids.values())
    
    if substring_match_list:
        logger.info(f"⚠️  SUBSTRING MATCHES (score: 0.1) - Found {len(substring_match_list)} chunks")
    
    if not exact_match_list and not substring_match_list:
        logger.info("❌ NO MATCHES - Citation/case number not found in database")
    
    return {
        "exact_matches": exact_match_list,
        "substring_matches": substring_match_list
    }