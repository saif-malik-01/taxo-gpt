# """
# Ultimate Citation Extractor with Edge Case Handling and LLM Re-attribution
# """

# import json
# import re
# import logging
# from typing import List, Dict, Tuple, Optional
# import os
# import boto3
# from botocore.config import Config

# logger = logging.getLogger(__name__)


# def get_bedrock_client():
#     """Initialize AWS Bedrock client"""
#     config = Config(
#         region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
#         signature_version='v4',
#         retries={'max_attempts': 3, 'mode': 'standard'}
#     )
    
#     return boto3.client(
#         'bedrock-runtime',
#         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
#         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
#         config=config
#     )


# def call_bedrock_for_extraction(prompt: str, max_tokens: int = 1500) -> str:
#     """Call AWS Bedrock with Qwen model"""
#     try:
#         client = get_bedrock_client()
#         model_id = "qwen.qwen3-next-80b-a3b"
        
#         request_body = {
#             "max_tokens": max_tokens,
#             "messages": [{"role": "user", "content": prompt}],
#             "temperature": 0.0
#         }
        
#         response = client.invoke_model(
#             modelId=model_id,
#             body=json.dumps(request_body)
#         )
        
#         response_body = json.loads(response['body'].read())
#         return response_body['choices'][0]['message']['content']
        
#     except Exception as e:
#         logger.error(f"Bedrock API error: {e}")
#         raise


# def extract_party_pairs_from_response(llm_response: str) -> List[Tuple[str, str]]:
#     """
#     Extract ALL party name pairs with better handling of abbreviations and variations
#     """
    
#     try:
#         prompt = f"""Extract ALL party name pairs from this legal text. Be very careful with variations.

# CRITICAL RULES:
# 1. Extract pairs connected by "vs", "v.", "v", "versus"
# 2. Keep full names as written (we'll normalize later)
# 3. Include abbreviations like "AC GST", "DGST", "State of Gujarat"
# 4. Don't skip incomplete-looking names
# 5. Return empty if no clear pairs

# Return ONLY this JSON:
# {{
#     "pairs": [
#         ["Party 1", "Party 2"],
#         ["Party 3", "Party 4"]
#     ]
# }}

# Examples:

# Input: "Shree Govind Alloys Pvt. Ltd. v. AC GST (2022)"
# Output: {{"pairs": [["Shree Govind Alloys Pvt. Ltd.", "AC GST"]]}}

# Input: "In Commissioner of GST vs ABC Company case..."
# Output: {{"pairs": [["Commissioner of GST", "ABC Company"]]}}

# Input: "State of Gujarat & Ors. v. XYZ Ltd."
# Output: {{"pairs": [["State of Gujarat & Ors.", "XYZ Ltd."]]}}

# Text:
# {llm_response}

# JSON:"""
        
#         content = call_bedrock_for_extraction(prompt)
        
#         # Clean response
#         content = content.strip()
#         if content.startswith("```"):
#             lines = content.split("\n")
#             content = "\n".join(lines[1:-1]) if len(lines) > 2 else content
#             if content.startswith("json"):
#                 content = content[4:].strip()
        
#         logger.debug(f"Raw extraction: {content}")
        
#         extracted = json.loads(content.strip())
        
#         pairs = []
#         for pair_list in extracted.get("pairs", []):
#             if isinstance(pair_list, list) and len(pair_list) == 2:
#                 p1 = pair_list[0].strip() if pair_list[0] else ""
#                 p2 = pair_list[1].strip() if pair_list[1] else ""
#                 if p1 and p2:
#                     pairs.append((p1, p2))
        
#         logger.info(f"✅ Extracted {len(pairs)} party pairs")
#         for i, (p1, p2) in enumerate(pairs, 1):
#             logger.info(f"   {i}. '{p1}' <-> '{p2}'")
        
#         return pairs
        
#     except Exception as e:
#         logger.warning(f"LLM extraction failed: {e}")
#         return regex_extract_party_pairs(llm_response)


# def regex_extract_party_pairs(text: str) -> List[Tuple[str, str]]:
#     """Enhanced regex extraction"""
#     pairs = []
    
#     # Patterns for various "vs" formats
#     patterns = [
#         r'([A-Z][A-Za-z\s&.,()]+?)\s+v\.?\s+([A-Z][A-Za-z\s&.,()]+?)(?:\s+\(|$|\.|,|\s+case|\s+judgment)',
#         r'([A-Z][A-Za-z\s&.,()]+?)\s+vs\.?\s+([A-Z][A-Za-z\s&.,()]+?)(?:\s+\(|$|\.|,|\s+case|\s+judgment)',
#         r'([A-Z][A-Za-z\s&.,()]+?)\s+versus\s+([A-Z][A-Za-z\s&.,()]+?)(?:\s+\(|$|\.|,|\s+case|\s+judgment)',
#     ]
    
#     for pattern in patterns:
#         matches = re.finditer(pattern, text)
#         for match in matches:
#             p1 = match.group(1).strip()
#             p2 = match.group(2).strip()
#             if p1 and p2:
#                 pairs.append((p1, p2))
    
#     # Deduplicate
#     seen = set()
#     unique = []
#     for pair in pairs:
#         key = tuple(sorted([p.lower() for p in pair]))
#         if key not in seen:
#             seen.add(key)
#             unique.append(pair)
    
#     return unique


# def normalize_party_name(name: str) -> str:
#     """
#     ULTIMATE normalization - handles ALL edge cases
#     """
#     if not name:
#         return ""
    
#     original = name
#     name = name.lower()
    
#     # STEP 1: Remove ORG, & ORS., & OTHERS first (highest priority)
#     name = re.sub(r'\s*org\.?\s*', ' ', name)
#     name = re.sub(r'\s*[&,]\s*ors\.?\s*', '', name)
#     name = re.sub(r'\s*&\s*others?\s*', '', name)
#     name = re.sub(r'\s*and\s+others?\s*', '', name)
    
#     # STEP 2: Remove comprehensive title list
#     titles = [
#         r'\bm/s\.?\s*', r'\bmessrs\.?\s*',
#         r'\bmr\.?\s*', r'\bmrs\.?\s*', r'\bms\.?\s*', r'\bmiss\.?\s*',
#         r'\bdr\.?\s*', r'\bprof\.?\s*', r'\bprofessor\.?\s*',
#         r'\bhon\.?\s*', r'\bhonourable\.?\s*', r'\bhon\'ble\.?\s*',
#         r'\bjustice\.?\s*', r'\bj\.?\s*',
#         r'\bsri\.?\s*', r'\bsmt\.?\s*', r'\bshri\.?\s*', r'\bshree\.?\s*',
#         r'\bmr\b', r'\bmrs\b', r'\bdr\b', r'\bms\b',
#     ]
    
#     for title in titles:
#         name = re.sub(title, '', name)
    
#     # STEP 3: Remove legal entity suffixes
#     suffixes = [
#         r'\bpvt\.?\s*ltd\.?\b', r'\bprivate\s+limited\b',
#         r'\bp\.?\s*ltd\.?\b', r'\bpvt\b',
#         r'\bltd\.?\b', r'\blimited\b',
#         r'\binc\.?\b', r'\bincorporated\b',
#         r'\bllc\.?\b', r'\bllp\.?\b',
#         r'\bco\.?\b', r'\bcompany\b',
#         r'\bcorp\.?\b', r'\bcorporation\b',
#     ]
    
#     for suffix in suffixes:
#         name = re.sub(suffix, '', name)
    
#     # STEP 4: Handle government entity abbreviations
#     # Keep these patterns but normalize them
#     name = re.sub(r'\bcommissioner\s+of\s+gst\b', 'commissioner gst', name)
#     name = re.sub(r'\bac\s+gst\b', 'assistant commissioner gst', name)
#     name = re.sub(r'\bdgst\b', 'director general gst', name)
#     name = re.sub(r'\bccgst\b', 'chief commissioner gst', name)
#     name = re.sub(r'\bstate\s+of\s+', 'state ', name)
    
#     # STEP 5: Remove ALL punctuation
#     name = re.sub(r'[^\w\s]', ' ', name)
    
#     # STEP 6: Clean up spaces
#     name = re.sub(r'\s+', ' ', name).strip()
    
#     logger.debug(f"Normalized: '{original}' → '{name}'")
    
#     return name


# def fuzzy_match_score(str1: str, str2: str) -> float:
#     """
#     Calculate fuzzy match score between two strings
#     Returns 1.0 for exact match, 0.0-1.0 for partial matches
#     """
#     if str1 == str2:
#         return 1.0
    
#     # Check if one is substring of other
#     if str1 in str2 or str2 in str1:
#         # Calculate overlap ratio
#         shorter = min(len(str1), len(str2))
#         longer = max(len(str1), len(str2))
#         return shorter / longer
    
#     # Check word overlap
#     words1 = set(str1.split())
#     words2 = set(str2.split())
    
#     if not words1 or not words2:
#         return 0.0
    
#     common = words1 & words2
#     total = words1 | words2
    
#     return len(common) / len(total) if total else 0.0


# def find_citations_for_party_pairs(
#     party_pairs: List[Tuple[str, str]], 
#     all_chunks: list,
#     fuzzy_threshold: float = 0.6
# ) -> Dict[Tuple[str, str], List[Dict]]:
#     """
#     Find citations with fuzzy matching for edge cases
#     """
    
#     results = {}
    
#     for party1, party2 in party_pairs:
#         party1_norm = normalize_party_name(party1)
#         party2_norm = normalize_party_name(party2)
        
#         if not party1_norm or not party2_norm:
#             logger.warning(f"⚠️  Skipping invalid pair: '{party1}' <-> '{party2}'")
#             continue
        
#         logger.info(f"\n{'='*80}")
#         logger.info(f"🔍 Searching for: '{party1}' <-> '{party2}'")
#         logger.info(f"   Normalized: '{party1_norm}' <-> '{party2_norm}'")
        
#         matching_citations = []
#         seen_external_ids = set()
#         best_matches = {}  # external_id -> (score, citation_info)
        
#         for chunk in all_chunks:
#             if chunk.get("chunk_type") != "judgment":
#                 continue
            
#             metadata = chunk.get("metadata", {})
#             external_id = metadata.get("external_id")
            
#             if not external_id or external_id in seen_external_ids:
#                 continue
            
#             db_petitioner = metadata.get("petitioner", "")
#             db_respondent = metadata.get("respondent", "")
            
#             db_pet_norm = normalize_party_name(db_petitioner)
#             db_resp_norm = normalize_party_name(db_respondent)
            
#             logger.debug(f"Checking {external_id}:")
#             logger.debug(f"  DB Pet: '{db_petitioner}' → '{db_pet_norm}'")
#             logger.debug(f"  DB Resp: '{db_respondent}' → '{db_resp_norm}'")
            
#             # EXACT MATCH (both parties)
#             exact_forward = (party1_norm == db_pet_norm and party2_norm == db_resp_norm)
#             exact_reverse = (party1_norm == db_resp_norm and party2_norm == db_pet_norm)
            
#             if exact_forward or exact_reverse:
#                 seen_external_ids.add(external_id)
#                 citation_info = build_citation_info(metadata, external_id)
#                 matching_citations.append(citation_info)
                
#                 logger.info(f"   ✅ EXACT MATCH - {external_id}")
#                 logger.info(f"      Pet: '{db_petitioner}'")
#                 logger.info(f"      Resp: '{db_respondent}'")
#                 logger.info(f"      Citation: {citation_info['citation']}")
#                 continue
            
#             # FUZZY MATCH
#             # Check if one party matches exactly and other fuzzy
#             score1_pet = fuzzy_match_score(party1_norm, db_pet_norm)
#             score1_resp = fuzzy_match_score(party1_norm, db_resp_norm)
#             score2_pet = fuzzy_match_score(party2_norm, db_pet_norm)
#             score2_resp = fuzzy_match_score(party2_norm, db_resp_norm)
            
#             # Forward: party1→petitioner, party2→respondent
#             forward_score = min(score1_pet, score2_resp)
#             # Reverse: party1→respondent, party2→petitioner
#             reverse_score = min(score1_resp, score2_pet)
            
#             best_score = max(forward_score, reverse_score)
            
#             if best_score >= fuzzy_threshold:
#                 citation_info = build_citation_info(metadata, external_id)
                
#                 if external_id not in best_matches or best_score > best_matches[external_id][0]:
#                     best_matches[external_id] = (best_score, citation_info)
                    
#                     logger.info(f"   🎯 FUZZY MATCH - {external_id} (score: {best_score:.2f})")
#                     logger.info(f"      Pet: '{db_petitioner}' (match: {max(score1_pet, score1_resp):.2f})")
#                     logger.info(f"      Resp: '{db_respondent}' (match: {max(score2_pet, score2_resp):.2f})")
#                     logger.info(f"      Citation: {citation_info['citation']}")
        
#         # Add best fuzzy matches
#         for external_id, (score, citation_info) in best_matches.items():
#             if external_id not in seen_external_ids:
#                 matching_citations.append(citation_info)
#                 seen_external_ids.add(external_id)
        
#         if matching_citations:
#             results[(party1, party2)] = matching_citations
#             logger.info(f"\n   📊 TOTAL: {len(matching_citations)} citation(s) found")
#         else:
#             logger.warning(f"\n   ❌ NO MATCHES for this pair")
        
#         logger.info(f"{'='*80}\n")
    
#     return results


# def build_citation_info(metadata: dict, external_id: str) -> dict:
#     """Build citation info dict from metadata"""
#     return {
#         "citation": metadata.get("citation", ""),
#         "case_number": metadata.get("case_number", ""),
#         "petitioner": metadata.get("petitioner", ""),
#         "respondent": metadata.get("respondent", ""),
#         "external_id": external_id,
#         "court": metadata.get("court", ""),
#         "year": metadata.get("year", ""),
#         "decision": metadata.get("decision", "")
#     }


# def reattribute_citations_in_response(
#     original_response: str,
#     party_citations: Dict[Tuple[str, str], List[Dict]]
# ) -> str:
#     """
#     Use LLM to correctly insert citations into the response where party names are mentioned
#     """
    
#     if not party_citations:
#         return original_response
    
#     # Build citation mapping for LLM
#     citation_mapping = []
#     for (party1, party2), citations in party_citations.items():
#         for cit in citations:
#             citation_mapping.append({
#                 "party1": party1,
#                 "party2": party2,
#                 "citation": cit['citation'],
#                 "case_number": cit['case_number']
#             })
    
#     prompt = f"""You are a legal citation editor. Your task is to add citations to a legal response WHERE the cases are mentioned.

# ORIGINAL RESPONSE:
# {original_response}

# AVAILABLE CITATIONS:
# {json.dumps(citation_mapping, indent=2)}

# INSTRUCTIONS:
# 1. Find where each party pair is mentioned in the response
# 2. Add the citation immediately after the first mention: "Party1 vs Party2 (Citation)"
# 3. Keep ALL other text EXACTLY as written
# 4. Do NOT change wording, explanations, or analysis
# 5. Do NOT add citations at the end - only inline where cases are mentioned
# 6. If a case is mentioned multiple times, cite it only at first mention

# Return the COMPLETE response with citations added inline.

# RESPONSE:"""
    
#     try:
#         reattributed = call_bedrock_for_extraction(prompt, max_tokens=3000)
        
#         # Clean markdown artifacts if any
#         reattributed = reattributed.strip()
#         if reattributed.startswith("```"):
#             lines = reattributed.split("\n")
#             reattributed = "\n".join(lines[1:-1]) if len(lines) > 2 else reattributed
        
#         logger.info("\n" + "="*100)
#         logger.info("✨ LLM RE-ATTRIBUTION COMPLETE")
#         logger.info("="*100)
        
#         return reattributed
        
#     except Exception as e:
#         logger.error(f"Re-attribution failed: {e}")
#         # Fallback to appending citations at end
#         return original_response + format_citation_section(party_citations)


# def format_citation_section(party_citations: Dict[Tuple[str, str], List[Dict]]) -> str:
#     """Format citations as appendix section (fallback)"""
#     if not party_citations:
#         return ""
    
#     lines = ["\n\n---\n", "\n**Citations Referenced:**\n"]
    
#     for (p1, p2), citations in party_citations.items():
#         lines.append(f"\n**{p1} vs {p2}:**")
#         for cit in citations:
#             case_num = re.sub(r'\s+dated.*$', '', cit['case_number'])
#             if case_num:
#                 lines.append(f"- {cit['citation']} ({case_num})")
#             else:
#                 lines.append(f"- {cit['citation']}")
    
#     return "\n".join(lines)


# def extract_and_attribute_citations(llm_response: str, all_chunks: list) -> Tuple[str, Dict]:
#     """
#     ULTIMATE citation extraction and attribution with complete logging
#     """
    
#     print("\n" + "="*100)
#     print(" "*35 + "🚀 CITATION ATTRIBUTION SYSTEM")
#     print("="*100)
    
#     # STEP 1: Print original response
#     print("\n📝 STEP 1: ORIGINAL LLM RESPONSE")
#     print("-"*100)
#     print(llm_response)
#     print("-"*100)
    
#     # STEP 2: Extract party pairs
#     print("\n🔍 STEP 2: EXTRACTING PARTY PAIRS")
#     print("-"*100)
    
#     party_pairs = extract_party_pairs_from_response(llm_response)
    
#     print(f"\n✅ Extracted {len(party_pairs)} party pair(s):")
#     for i, (p1, p2) in enumerate(party_pairs, 1):
#         print(f"   {i}. '{p1}' <-> '{p2}'")
    
#     if not party_pairs:
#         print("\n⚠️  No party pairs found - returning original response")
#         print("="*100 + "\n")
#         return llm_response, {}
    
#     # STEP 3: Find citations
#     print("\n🔍 STEP 3: FINDING CITATIONS IN DATABASE")
#     print("-"*100)
    
#     party_citations = find_citations_for_party_pairs(party_pairs, all_chunks)
    
#     print(f"\n📊 FOUND {len(party_citations)} matching party pair(s):")
#     for (p1, p2), citations in party_citations.items():
#         print(f"\n   '{p1}' vs '{p2}':")
#         for cit in citations:
#             print(f"      ✅ {cit['citation']}")
#             print(f"         Petitioner: {cit['petitioner']}")
#             print(f"         Respondent: {cit['respondent']}")
    
#     if not party_citations:
#         print("\n⚠️  No citations found - returning original response")
#         print("="*100 + "\n")
#         return llm_response, {}
    
#     # STEP 4: Re-attribute with LLM
#     print("\n✨ STEP 4: RE-ATTRIBUTING CITATIONS IN RESPONSE")
#     print("-"*100)
    
#     enhanced_response = reattribute_citations_in_response(llm_response, party_citations)
    
#     print("\n📝 FINAL ENHANCED RESPONSE:")
#     print("-"*100)
#     print(enhanced_response)
#     print("-"*100)
    
#     # STEP 5: Summary
#     print("\n📊 SUMMARY:")
#     print(f"   Party pairs extracted: {len(party_pairs)}")
#     print(f"   Citations found: {sum(len(cits) for cits in party_citations.values())}")
#     print(f"   Original length: {len(llm_response)} chars")
#     print(f"   Enhanced length: {len(enhanced_response)} chars")
    
#     print("\n" + "="*100)
#     print(" "*30 + "✅ CITATION ATTRIBUTION COMPLETE")
#     print("="*100 + "\n")
    
#     return enhanced_response, party_citations





# """
# Ultimate Citation Extractor - FIXED for real-world edge cases
# """

# import json
# import re
# import logging
# from typing import List, Dict, Tuple, Optional
# import os
# import boto3
# from botocore.config import Config

# logger = logging.getLogger(__name__)


# def get_bedrock_client():
#     """Initialize AWS Bedrock client"""
#     config = Config(
#         region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
#         signature_version='v4',
#         retries={'max_attempts': 3, 'mode': 'standard'}
#     )
    
#     return boto3.client(
#         'bedrock-runtime',
#         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
#         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
#         config=config
#     )


# def call_bedrock_for_extraction(prompt: str, max_tokens: int = 1500) -> str:
#     """Call AWS Bedrock with Qwen model"""
#     try:
#         client = get_bedrock_client()
#         model_id = "qwen.qwen3-next-80b-a3b"
        
#         request_body = {
#             "max_tokens": max_tokens,
#             "messages": [{"role": "user", "content": prompt}],
#             "temperature": 0.0
#         }
        
#         response = client.invoke_model(
#             modelId=model_id,
#             body=json.dumps(request_body)
#         )
        
#         response_body = json.loads(response['body'].read())
#         return response_body['choices'][0]['message']['content']
        
#     except Exception as e:
#         logger.error(f"Bedrock API error: {e}")
#         raise


# def extract_party_pairs_from_response(llm_response: str) -> List[Tuple[str, str]]:
#     """
#     Extract party name pairs - IMPROVED to avoid hallucinations
#     """
    
#     try:
#         prompt = f"""Extract ONLY actual party names (companies/persons) from legal text. Do NOT extract case descriptions.

# STRICT RULES:
# 1. Extract pairs ONLY from patterns: "Party1 vs/v./v Party2"
# 2. Party names are companies, persons, or government entities (State of X, Commissioner, etc.)
# 3. Do NOT extract: case descriptions, court names, legal issues
# 4. Ignore case citations like "(2022)" - extract only names
# 5. Return empty array if no clear party pairs found

# BAD EXAMPLES (do NOT extract these):
# - "Gujarat HC on numeric error in e-way bill" ❌ (this is a description)
# - "Court held that..." ❌ (not parties)
# - "The judgment in..." ❌ (not parties)

# GOOD EXAMPLES (extract these):
# - "Shree Govind Alloys Pvt. Ltd. v. State of Gujarat" ✓
# - "Modern Traders vs State of U.P." ✓
# - "ABC Company v. Commissioner of GST" ✓

# Return ONLY this JSON:
# {{
#     "pairs": [
#         ["Party 1 Name", "Party 2 Name"]
#     ]
# }}

# Text:
# {llm_response}

# JSON:"""
        
#         content = call_bedrock_for_extraction(prompt)
        
#         # Clean response
#         content = content.strip()
#         if content.startswith("```"):
#             lines = content.split("\n")
#             content = "\n".join(lines[1:-1]) if len(lines) > 2 else content
#             if content.startswith("json"):
#                 content = content[4:].strip()
        
#         logger.debug(f"Raw extraction: {content}")
        
#         extracted = json.loads(content.strip())
        
#         pairs = []
#         for pair_list in extracted.get("pairs", []):
#             if isinstance(pair_list, list) and len(pair_list) == 2:
#                 p1 = pair_list[0].strip() if pair_list[0] else ""
#                 p2 = pair_list[1].strip() if pair_list[1] else ""
                
#                 # Filter out invalid pairs
#                 if p1 and p2 and is_valid_party_name(p1) and is_valid_party_name(p2):
#                     pairs.append((p1, p2))
#                 else:
#                     logger.warning(f"Filtered invalid pair: '{p1}' <-> '{p2}'")
        
#         logger.info(f"✅ Extracted {len(pairs)} party pairs")
#         for i, (p1, p2) in enumerate(pairs, 1):
#             logger.info(f"   {i}. '{p1}' <-> '{p2}'")
        
#         return pairs
        
#     except Exception as e:
#         logger.warning(f"LLM extraction failed: {e}")
#         return regex_extract_party_pairs(llm_response)


# def is_valid_party_name(name: str) -> bool:
#     """Check if extracted name is a valid party name (not a description)"""
    
#     # Invalid patterns
#     invalid_patterns = [
#         r'^\s*on\s+',  # "on numeric error"
#         r'^\s*in\s+',  # "in the case of"
#         r'^\s*the\s+',  # "the judgment"
#         r'HC\s+on\s+',  # "HC on ..."
#         r'court\s+',  # "court held"
#         r'judgment\s+',  # "judgment in"
#         r'case\s+of\s+',  # "case of"
#     ]
    
#     for pattern in invalid_patterns:
#         if re.search(pattern, name, re.IGNORECASE):
#             return False
    
#     # Must have at least one letter
#     if not re.search(r'[a-zA-Z]', name):
#         return False
    
#     return True


# def regex_extract_party_pairs(text: str) -> List[Tuple[str, str]]:
#     """Enhanced regex extraction with better patterns"""
#     pairs = []
    
#     # More specific patterns - require capitalized names
#     patterns = [
#         r'([A-Z][A-Za-z\s&.,()]+?)\s+v\.?\s+([A-Z][A-Za-z\s&.,()]+?)(?:\s+\(|$|\s+case|\s+judgment)',
#         r'([A-Z][A-Za-z\s&.,()]+?)\s+vs\.?\s+([A-Z][A-Za-z\s&.,()]+?)(?:\s+\(|$|\s+case|\s+judgment)',
#     ]
    
#     for pattern in patterns:
#         matches = re.finditer(pattern, text)
#         for match in matches:
#             p1 = match.group(1).strip()
#             p2 = match.group(2).strip()
            
#             if p1 and p2 and is_valid_party_name(p1) and is_valid_party_name(p2):
#                 pairs.append((p1, p2))
    
#     # Deduplicate
#     seen = set()
#     unique = []
#     for pair in pairs:
#         key = tuple(sorted([p.lower() for p in pair]))
#         if key not in seen:
#             seen.add(key)
#             unique.append(pair)
    
#     return unique


# def normalize_party_name(name: str) -> str:
#     """
#     FIXED normalization - preserves important name parts
#     """
#     if not name:
#         return ""
    
#     original = name
#     name = name.lower()
    
#     # STEP 1: Remove ORG, & ORS., & OTHERS (but AFTER main normalization)
#     name = re.sub(r'\s*[&,]\s*ors\.?\s*$', '', name)  # Only at end
#     name = re.sub(r'\s*&\s*others?\s*$', '', name)  # Only at end
#     name = re.sub(r'\s*and\s+\d+\s+others?\s*$', '', name)  # "and 3 others"
    
#     # STEP 2: Remove titles ONLY at beginning
#     titles_start = [
#         r'^m/s\.?\s*', r'^messrs\.?\s*',
#         r'^mr\.?\s*', r'^mrs\.?\s*', r'^ms\.?\s*', r'^dr\.?\s*',
#         r'^prof\.?\s*', r'^hon\.?\s*', r'^justice\.?\s*',
#         r'^sri\.?\s*', r'^smt\.?\s*', r'^shri\.?\s*',
#     ]
    
#     for title in titles_start:
#         name = re.sub(title, '', name)
    
#     # STEP 3: Remove legal suffixes ONLY at end
#     suffixes_end = [
#         r'\s+pvt\.?\s*ltd\.?\s*$', r'\s+private\s+limited\s*$',
#         r'\s+p\.?\s*ltd\.?\s*$',
#         r'\s+ltd\.?\s*$', r'\s+limited\s*$',
#         r'\s+inc\.?\s*$', r'\s+llc\.?\s*$', r'\s+llp\.?\s*$',
#         r'\s+co\.?\s*$', r'\s+company\s*$', r'\s+corp\.?\s*$',
#     ]
    
#     for suffix in suffixes_end:
#         name = re.sub(suffix, '', name)
    
#     # STEP 4: Normalize common government entities (but keep "State of X")
#     # Keep structure, just normalize spacing
#     name = re.sub(r'\s+', ' ', name)
    
#     # STEP 5: Remove only excessive punctuation (keep important ones)
#     name = re.sub(r'\.+', ' ', name)  # Multiple dots
#     name = re.sub(r',+', ' ', name)  # Commas
    
#     # STEP 6: Final cleanup
#     name = re.sub(r'\s+', ' ', name).strip()
    
#     logger.debug(f"Normalized: '{original}' → '{name}'")
    
#     return name


# def fuzzy_match_score(str1: str, str2: str) -> float:
#     """Calculate fuzzy match score between normalized strings"""
    
#     if str1 == str2:
#         return 1.0
    
#     # Token-based matching (word overlap)
#     words1 = set(str1.split())
#     words2 = set(str2.split())
    
#     if not words1 or not words2:
#         return 0.0
    
#     # Count important word matches
#     common = words1 & words2
    
#     # For party names, if most significant words match, it's a good match
#     # Weight by importance (longer words are more distinctive)
#     common_importance = sum(len(w) for w in common if len(w) > 2)
#     total1_importance = sum(len(w) for w in words1 if len(w) > 2)
#     total2_importance = sum(len(w) for w in words2 if len(w) > 2)
    
#     if total1_importance == 0 or total2_importance == 0:
#         return len(common) / len(words1 | words2)
    
#     # Score based on important word overlap
#     score1 = common_importance / total1_importance if total1_importance > 0 else 0
#     score2 = common_importance / total2_importance if total2_importance > 0 else 0
    
#     return (score1 + score2) / 2


# def find_citations_for_party_pairs(
#     party_pairs: List[Tuple[str, str]], 
#     all_chunks: list,
#     fuzzy_threshold: float = 0.5  # Lowered for better recall
# ) -> Dict[Tuple[str, str], List[Dict]]:
#     """
#     Find citations with improved fuzzy matching
#     """
    
#     results = {}
    
#     for party1, party2 in party_pairs:
#         party1_norm = normalize_party_name(party1)
#         party2_norm = normalize_party_name(party2)
        
#         if not party1_norm or not party2_norm:
#             logger.warning(f"⚠️  Skipping invalid pair: '{party1}' <-> '{party2}'")
#             continue
        
#         logger.info(f"\n{'='*100}")
#         logger.info(f"🔍 Searching for: '{party1}' <-> '{party2}'")
#         logger.info(f"   Normalized: '{party1_norm}' <-> '{party2_norm}'")
        
#         matching_citations = []
#         seen_external_ids = set()
#         best_matches = {}
        
#         for chunk in all_chunks:
#             if chunk.get("chunk_type") != "judgment":
#                 continue
            
#             metadata = chunk.get("metadata", {})
#             external_id = metadata.get("external_id")
            
#             if not external_id or external_id in seen_external_ids:
#                 continue
            
#             db_petitioner = metadata.get("petitioner", "")
#             db_respondent = metadata.get("respondent", "")
            
#             if not db_petitioner or not db_respondent:
#                 continue
            
#             db_pet_norm = normalize_party_name(db_petitioner)
#             db_resp_norm = normalize_party_name(db_respondent)
            
#             logger.debug(f"\nChecking {external_id}:")
#             logger.debug(f"  Pet: '{db_petitioner}' → '{db_pet_norm}'")
#             logger.debug(f"  Resp: '{db_respondent}' → '{db_resp_norm}'")
            
#             # EXACT MATCH
#             exact_forward = (party1_norm == db_pet_norm and party2_norm == db_resp_norm)
#             exact_reverse = (party1_norm == db_resp_norm and party2_norm == db_pet_norm)
            
#             if exact_forward or exact_reverse:
#                 seen_external_ids.add(external_id)
#                 citation_info = build_citation_info(metadata, external_id)
#                 matching_citations.append(citation_info)
                
#                 logger.info(f"   ✅ EXACT MATCH - {external_id}")
#                 logger.info(f"      Petitioner: '{db_petitioner}'")
#                 logger.info(f"      Respondent: '{db_respondent}'")
#                 logger.info(f"      Citation: {citation_info['citation']}")
#                 continue
            
#             # FUZZY MATCH - Match petitioner with either party
#             score1_vs_pet = fuzzy_match_score(party1_norm, db_pet_norm)
#             score1_vs_resp = fuzzy_match_score(party1_norm, db_resp_norm)
#             score2_vs_pet = fuzzy_match_score(party2_norm, db_pet_norm)
#             score2_vs_resp = fuzzy_match_score(party2_norm, db_resp_norm)
            
#             # Best match for party1
#             party1_best_score = max(score1_vs_pet, score1_vs_resp)
#             party1_matches_pet = (score1_vs_pet > score1_vs_resp)
            
#             # Best match for party2
#             party2_best_score = max(score2_vs_pet, score2_vs_resp)
#             party2_matches_pet = (score2_vs_pet > score2_vs_resp)
            
#             # Check if they match opposite parties (one to pet, one to resp)
#             valid_pairing = (party1_matches_pet != party2_matches_pet)
            
#             # Overall score (both parties must match well)
#             overall_score = min(party1_best_score, party2_best_score) if valid_pairing else 0
            
#             if overall_score >= fuzzy_threshold:
#                 citation_info = build_citation_info(metadata, external_id)
                
#                 if external_id not in best_matches or overall_score > best_matches[external_id][0]:
#                     best_matches[external_id] = (overall_score, citation_info)
                    
#                     logger.info(f"   🎯 FUZZY MATCH - {external_id} (score: {overall_score:.2f})")
#                     logger.info(f"      Petitioner: '{db_petitioner}' (party1={score1_vs_pet:.2f}, party2={score2_vs_pet:.2f})")
#                     logger.info(f"      Respondent: '{db_respondent}' (party1={score1_vs_resp:.2f}, party2={score2_vs_resp:.2f})")
#                     logger.info(f"      Citation: {citation_info['citation']}")
        
#         # Add best fuzzy matches
#         for external_id, (score, citation_info) in best_matches.items():
#             if external_id not in seen_external_ids:
#                 matching_citations.append(citation_info)
#                 seen_external_ids.add(external_id)
        
#         if matching_citations:
#             results[(party1, party2)] = matching_citations
#             logger.info(f"\n   📊 TOTAL: {len(matching_citations)} citation(s) found")
#         else:
#             logger.warning(f"\n   ❌ NO MATCHES for this pair")
        
#         logger.info(f"{'='*100}\n")
    
#     return results


# def build_citation_info(metadata: dict, external_id: str) -> dict:
#     """Build citation info dict from metadata"""
#     return {
#         "citation": metadata.get("citation", ""),
#         "case_number": metadata.get("case_number", ""),
#         "petitioner": metadata.get("petitioner", ""),
#         "respondent": metadata.get("respondent", ""),
#         "external_id": external_id,
#         "court": metadata.get("court", ""),
#         "year": metadata.get("year", ""),
#         "decision": metadata.get("decision", "")
#     }


# def reattribute_citations_in_response(
#     original_response: str,
#     party_citations: Dict[Tuple[str, str], List[Dict]]
# ) -> str:
#     """Use LLM to insert citations inline"""
    
#     if not party_citations:
#         return original_response
    
#     citation_mapping = []
#     for (party1, party2), citations in party_citations.items():
#         for cit in citations:
#             citation_mapping.append({
#                 "party1": party1,
#                 "party2": party2,
#                 "petitioner_full": cit['petitioner'],
#                 "respondent_full": cit['respondent'],
#                 "citation": cit['citation'],
#                 "case_number": cit['case_number']
#             })
    
#     prompt = f"""Add citations to the response where cases are mentioned.

# ORIGINAL RESPONSE:
# {original_response}

# AVAILABLE CITATIONS:
# {json.dumps(citation_mapping, indent=2)}

# INSTRUCTIONS:
# 1. Find where parties/cases are mentioned
# 2. Add citation after first mention: "Party1 v. Party2 (Citation)"
# 3. Match flexibly - "Shree Govind Alloys" matches "SHREE GOVIND ALLOYS PVT. LTD"
# 4. Keep ALL other text exactly as written
# 5. Only cite at first mention
# 6. Return COMPLETE response with citations inline

# RESPONSE:"""
    
#     try:
#         reattributed = call_bedrock_for_extraction(prompt, max_tokens=4000)
        
#         reattributed = reattributed.strip()
#         if reattributed.startswith("```"):
#             lines = reattributed.split("\n")
#             reattributed = "\n".join(lines[1:-1]) if len(lines) > 2 else reattributed
        
#         return reattributed
        
#     except Exception as e:
#         logger.error(f"Re-attribution failed: {e}")
#         return original_response + format_citation_section(party_citations)


# def format_citation_section(party_citations: Dict[Tuple[str, str], List[Dict]]) -> str:
#     """Fallback: append citations at end"""
#     if not party_citations:
#         return ""
    
#     lines = ["\n\n---\n", "\n**Citations Referenced:**\n"]
    
#     for (p1, p2), citations in party_citations.items():
#         lines.append(f"\n**{p1} vs {p2}:**")
#         for cit in citations:
#             case_num = re.sub(r'\s+dated.*$', '', cit['case_number'])
#             if case_num:
#                 lines.append(f"- {cit['citation']} ({case_num})")
#             else:
#                 lines.append(f"- {cit['citation']}")
    
#     return "\n".join(lines)


# def extract_and_attribute_citations(llm_response: str, all_chunks: list) -> Tuple[str, Dict]:
#     """Main function with complete logging"""
    
#     print("\n" + "="*100)
#     print(" "*30 + "🚀 CITATION ATTRIBUTION SYSTEM")
#     print("="*100)
    
#     # STEP 1
#     print("\n📝 STEP 1: ORIGINAL LLM RESPONSE")
#     print("-"*100)
#     print(llm_response)
#     print("-"*100)
    
#     # STEP 2
#     print("\n🔍 STEP 2: EXTRACTING PARTY PAIRS")
#     print("-"*100)
    
#     party_pairs = extract_party_pairs_from_response(llm_response)
    
#     print(f"\n✅ Extracted {len(party_pairs)} party pair(s):")
#     for i, (p1, p2) in enumerate(party_pairs, 1):
#         print(f"   {i}. '{p1}' <-> '{p2}'")
    
#     if not party_pairs:
#         print("\n⚠️  No party pairs - returning original")
#         print("="*100 + "\n")
#         return llm_response, {}
    
#     # STEP 3
#     print("\n🔍 STEP 3: FINDING CITATIONS IN DATABASE")
#     print("-"*100)
    
#     party_citations = find_citations_for_party_pairs(party_pairs, all_chunks)
    
#     print(f"\n📊 FOUND {len(party_citations)} matching pair(s):")
#     for (p1, p2), citations in party_citations.items():
#         print(f"\n   '{p1}' vs '{p2}':")
#         for cit in citations:
#             print(f"      ✅ {cit['citation']}")
#             print(f"         Pet: {cit['petitioner']}")
#             print(f"         Resp: {cit['respondent']}")
    
#     if not party_citations:
#         print("\n⚠️  No citations found - returning original")
#         print("="*100 + "\n")
#         return llm_response, {}
    
#     # STEP 4
#     print("\n✨ STEP 4: RE-ATTRIBUTING CITATIONS")
#     print("-"*100)
    
#     enhanced_response = reattribute_citations_in_response(llm_response, party_citations)
    
#     print("\n📝 FINAL ENHANCED RESPONSE:")
#     print("-"*100)
#     print(enhanced_response)
#     print("-"*100)
    
#     print("\n📊 SUMMARY:")
#     print(f"   Pairs extracted: {len(party_pairs)}")
#     print(f"   Citations found: {sum(len(c) for c in party_citations.values())}")
#     print(f"   Original: {len(llm_response)} chars")
#     print(llm_response)
#     print(f"   Enhanced: {len(enhanced_response)} chars")
#     print(enha)
#     print("\n" + "="*100)
#     print(" "*25 + "✅ CITATION ATTRIBUTION COMPLETE")
#     print("="*100 + "\n")
    
#     return enhanced_response, party_citations




import json
import re
import logging
from typing import List, Dict, Tuple, Optional
import os
import boto3
from botocore.config import Config

logger = logging.getLogger(__name__)


def get_bedrock_client():
    """Initialize AWS Bedrock client"""
    config = Config(
        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
        signature_version='v4',
        retries={'max_attempts': 3, 'mode': 'standard'}
    )
    
    return boto3.client(
        'bedrock-runtime',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        config=config
    )


def call_bedrock_for_extraction(prompt: str, max_tokens: int = 1500) -> str:
    """Call AWS Bedrock with Qwen model"""
    try:
        client = get_bedrock_client()
        model_id = "qwen.qwen3-next-80b-a3b"
        
        request_body = {
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0
        }
        
        response = client.invoke_model(
            modelId=model_id,
            body=json.dumps(request_body)
        )
        
        response_body = json.loads(response['body'].read())
        return response_body['choices'][0]['message']['content']
        
    except Exception as e:
        logger.error(f"Bedrock API error: {e}")
        raise


def extract_party_pairs_from_response(llm_response: str) -> List[Tuple[str, str]]:
    """
    Extract party name pairs - IMPROVED to avoid hallucinations
    """
    
    try:
        prompt = f"""Extract ONLY actual party names (companies/persons) from legal text. Do NOT extract case descriptions.

STRICT RULES:
1. Extract pairs ONLY from patterns: "Party1 vs/v./v Party2"
2. Party names are companies, persons, or government entities (State of X, Commissioner, etc.)
3. Do NOT extract: case descriptions, court names, legal issues
4. Ignore case citations like "(2022)" - extract only names
5. Return empty array if no clear party pairs found

BAD EXAMPLES (do NOT extract these):
- "Gujarat HC on numeric error in e-way bill" ❌ (this is a description)
- "Court held that..." ❌ (not parties)
- "The judgment in..." ❌ (not parties)

GOOD EXAMPLES (extract these):
- "Shree Govind Alloys Pvt. Ltd. v. State of Gujarat" ✓
- "Modern Traders vs State of U.P." ✓
- "ABC Company v. Commissioner of GST" ✓

Return ONLY this JSON:
{{
    "pairs": [
        ["Party 1 Name", "Party 2 Name"]
    ]
}}

Text:
{llm_response}

JSON:"""
        
        content = call_bedrock_for_extraction(prompt)
        
        # Clean response
        content = content.strip()
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(lines[1:-1]) if len(lines) > 2 else content
            if content.startswith("json"):
                content = content[4:].strip()
        
        logger.debug(f"Raw extraction: {content}")
        
        extracted = json.loads(content.strip())
        
        pairs = []
        for pair_list in extracted.get("pairs", []):
            if isinstance(pair_list, list) and len(pair_list) == 2:
                p1 = pair_list[0].strip() if pair_list[0] else ""
                p2 = pair_list[1].strip() if pair_list[1] else ""
                
                # Filter out invalid pairs
                if p1 and p2 and is_valid_party_name(p1) and is_valid_party_name(p2):
                    pairs.append((p1, p2))
                else:
                    logger.warning(f"Filtered invalid pair: '{p1}' <-> '{p2}'")
        
        logger.info(f"✅ Extracted {len(pairs)} party pairs")
        for i, (p1, p2) in enumerate(pairs, 1):
            logger.info(f"   {i}. '{p1}' <-> '{p2}'")
        
        return pairs
        
    except Exception as e:
        logger.warning(f"LLM extraction failed: {e}")
        return regex_extract_party_pairs(llm_response)


def is_valid_party_name(name: str) -> bool:
    """Check if extracted name is a valid party name (not a description)"""
    
    # Invalid patterns
    invalid_patterns = [
        r'^\s*on\s+',  # "on numeric error"
        r'^\s*in\s+',  # "in the case of"
        r'^\s*the\s+',  # "the judgment"
        r'HC\s+on\s+',  # "HC on ..."
        r'court\s+',  # "court held"
        r'judgment\s+',  # "judgment in"
        r'case\s+of\s+',  # "case of"
    ]
    
    for pattern in invalid_patterns:
        if re.search(pattern, name, re.IGNORECASE):
            return False
    
    # Must have at least one letter
    if not re.search(r'[a-zA-Z]', name):
        return False
    
    return True


def regex_extract_party_pairs(text: str) -> List[Tuple[str, str]]:
    """Enhanced regex extraction with better patterns"""
    pairs = []
    
    # More specific patterns - require capitalized names
    patterns = [
        r'([A-Z][A-Za-z\s&.,()]+?)\s+v\.?\s+([A-Z][A-Za-z\s&.,()]+?)(?:\s+\(|$|\s+case|\s+judgment)',
        r'([A-Z][A-Za-z\s&.,()]+?)\s+vs\.?\s+([A-Z][A-Za-z\s&.,()]+?)(?:\s+\(|$|\s+case|\s+judgment)',
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, text)
        for match in matches:
            p1 = match.group(1).strip()
            p2 = match.group(2).strip()
            
            if p1 and p2 and is_valid_party_name(p1) and is_valid_party_name(p2):
                pairs.append((p1, p2))
    
    # Deduplicate
    seen = set()
    unique = []
    for pair in pairs:
        key = tuple(sorted([p.lower() for p in pair]))
        if key not in seen:
            seen.add(key)
            unique.append(pair)
    
    return unique


def normalize_party_name(name: str) -> str:
    """
    FIXED normalization - preserves important name parts
    """
    if not name:
        return ""
    
    original = name
    name = name.lower()
    
    # STEP 1: Remove ORG, & ORS., & OTHERS (but AFTER main normalization)
    name = re.sub(r'\s*[&,]\s*ors\.?\s*$', '', name)  # Only at end
    name = re.sub(r'\s*&\s*others?\s*$', '', name)  # Only at end
    name = re.sub(r'\s*and\s+\d+\s+others?\s*$', '', name)  # "and 3 others"
    
    # STEP 2: Remove titles ONLY at beginning
    titles_start = [
        r'^m/s\.?\s*', r'^messrs\.?\s*',
        r'^mr\.?\s*', r'^mrs\.?\s*', r'^ms\.?\s*', r'^dr\.?\s*',
        r'^prof\.?\s*', r'^hon\.?\s*', r'^justice\.?\s*',
        r'^sri\.?\s*', r'^smt\.?\s*', r'^shri\.?\s*',
    ]
    
    for title in titles_start:
        name = re.sub(title, '', name)
    
    # STEP 3: Remove legal suffixes ONLY at end
    suffixes_end = [
        r'\s+pvt\.?\s*ltd\.?\s*$', r'\s+private\s+limited\s*$',
        r'\s+p\.?\s*ltd\.?\s*$',
        r'\s+ltd\.?\s*$', r'\s+limited\s*$',
        r'\s+inc\.?\s*$', r'\s+llc\.?\s*$', r'\s+llp\.?\s*$',
        r'\s+co\.?\s*$', r'\s+company\s*$', r'\s+corp\.?\s*$',
    ]
    
    for suffix in suffixes_end:
        name = re.sub(suffix, '', name)
    
    # STEP 4: Normalize common government entities (but keep "State of X")
    # Keep structure, just normalize spacing
    name = re.sub(r'\s+', ' ', name)
    
    # STEP 5: Remove only excessive punctuation (keep important ones)
    name = re.sub(r'\.+', ' ', name)  # Multiple dots
    name = re.sub(r',+', ' ', name)  # Commas
    
    # STEP 6: Final cleanup
    name = re.sub(r'\s+', ' ', name).strip()
    
    logger.debug(f"Normalized: '{original}' → '{name}'")
    
    return name


def fuzzy_match_score(str1: str, str2: str) -> float:
    """Calculate fuzzy match score between normalized strings"""
    
    if str1 == str2:
        return 1.0
    
    # Token-based matching (word overlap)
    words1 = set(str1.split())
    words2 = set(str2.split())
    
    if not words1 or not words2:
        return 0.0
    
    # Count important word matches
    common = words1 & words2
    
    # For party names, if most significant words match, it's a good match
    # Weight by importance (longer words are more distinctive)
    common_importance = sum(len(w) for w in common if len(w) > 2)
    total1_importance = sum(len(w) for w in words1 if len(w) > 2)
    total2_importance = sum(len(w) for w in words2 if len(w) > 2)
    
    if total1_importance == 0 or total2_importance == 0:
        return len(common) / len(words1 | words2)
    
    # Score based on important word overlap
    score1 = common_importance / total1_importance if total1_importance > 0 else 0
    score2 = common_importance / total2_importance if total2_importance > 0 else 0
    
    return (score1 + score2) / 2


def find_citations_for_party_pairs(
    party_pairs: List[Tuple[str, str]], 
    all_chunks: list,
    fuzzy_threshold: float = 0.5  # Lowered for better recall
) -> Dict[Tuple[str, str], List[Dict]]:
    """
    Find citations with improved fuzzy matching
    """
    
    results = {}
    
    for party1, party2 in party_pairs:
        party1_norm = normalize_party_name(party1)
        party2_norm = normalize_party_name(party2)
        
        if not party1_norm or not party2_norm:
            logger.warning(f"⚠️  Skipping invalid pair: '{party1}' <-> '{party2}'")
            continue
        
        logger.info(f"\n{'='*100}")
        logger.info(f"🔍 Searching for: '{party1}' <-> '{party2}'")
        logger.info(f"   Normalized: '{party1_norm}' <-> '{party2_norm}'")
        
        matching_citations = []
        seen_external_ids = set()
        best_matches = {}
        
        for chunk in all_chunks:
            if chunk.get("chunk_type") != "judgment":
                continue
            
            metadata = chunk.get("metadata", {})
            external_id = metadata.get("external_id")
            
            if not external_id or external_id in seen_external_ids:
                continue
            
            db_petitioner = metadata.get("petitioner", "")
            db_respondent = metadata.get("respondent", "")
            
            if not db_petitioner or not db_respondent:
                continue
            
            db_pet_norm = normalize_party_name(db_petitioner)
            db_resp_norm = normalize_party_name(db_respondent)
            
            logger.debug(f"\nChecking {external_id}:")
            logger.debug(f"  Pet: '{db_petitioner}' → '{db_pet_norm}'")
            logger.debug(f"  Resp: '{db_respondent}' → '{db_resp_norm}'")
            
            # EXACT MATCH
            exact_forward = (party1_norm == db_pet_norm and party2_norm == db_resp_norm)
            exact_reverse = (party1_norm == db_resp_norm and party2_norm == db_pet_norm)
            
            if exact_forward or exact_reverse:
                seen_external_ids.add(external_id)
                citation_info = build_citation_info(metadata, external_id)
                matching_citations.append(citation_info)
                
                logger.info(f"   ✅ EXACT MATCH - {external_id}")
                logger.info(f"      Petitioner: '{db_petitioner}'")
                logger.info(f"      Respondent: '{db_respondent}'")
                logger.info(f"      Citation: {citation_info['citation']}")
                continue
            
            # FUZZY MATCH - Match petitioner with either party
            score1_vs_pet = fuzzy_match_score(party1_norm, db_pet_norm)
            score1_vs_resp = fuzzy_match_score(party1_norm, db_resp_norm)
            score2_vs_pet = fuzzy_match_score(party2_norm, db_pet_norm)
            score2_vs_resp = fuzzy_match_score(party2_norm, db_resp_norm)
            
            # Best match for party1
            party1_best_score = max(score1_vs_pet, score1_vs_resp)
            party1_matches_pet = (score1_vs_pet > score1_vs_resp)
            
            # Best match for party2
            party2_best_score = max(score2_vs_pet, score2_vs_resp)
            party2_matches_pet = (score2_vs_pet > score2_vs_resp)
            
            # Check if they match opposite parties (one to pet, one to resp)
            valid_pairing = (party1_matches_pet != party2_matches_pet)
            
            # Overall score (both parties must match well)
            overall_score = min(party1_best_score, party2_best_score) if valid_pairing else 0
            
            if overall_score >= fuzzy_threshold:
                citation_info = build_citation_info(metadata, external_id)
                
                if external_id not in best_matches or overall_score > best_matches[external_id][0]:
                    best_matches[external_id] = (overall_score, citation_info)
                    
                    logger.info(f"   🎯 FUZZY MATCH - {external_id} (score: {overall_score:.2f})")
                    logger.info(f"      Petitioner: '{db_petitioner}' (party1={score1_vs_pet:.2f}, party2={score2_vs_pet:.2f})")
                    logger.info(f"      Respondent: '{db_respondent}' (party1={score1_vs_resp:.2f}, party2={score2_vs_resp:.2f})")
                    logger.info(f"      Citation: {citation_info['citation']}")
        
        # Add best fuzzy matches
        for external_id, (score, citation_info) in best_matches.items():
            if external_id not in seen_external_ids:
                matching_citations.append(citation_info)
                seen_external_ids.add(external_id)
        
        if matching_citations:
            results[(party1, party2)] = matching_citations
            logger.info(f"\n   📊 TOTAL: {len(matching_citations)} citation(s) found")
        else:
            logger.warning(f"\n   ❌ NO MATCHES for this pair")
        
        logger.info(f"{'='*100}\n")
    
    return results


def build_citation_info(metadata: dict, external_id: str) -> dict:
    """Build citation info dict from metadata"""
    return {
        "citation": metadata.get("citation", ""),
        "case_number": metadata.get("case_number", ""),
        "petitioner": metadata.get("petitioner", ""),
        "respondent": metadata.get("respondent", ""),
        "external_id": external_id,
        "court": metadata.get("court", ""),
        "year": metadata.get("year", ""),
        "decision": metadata.get("decision", "")
    }


def reattribute_citations_in_response(
    original_response: str,
    party_citations: Dict[Tuple[str, str], List[Dict]]
) -> str:
    """
    IMPROVED: Use LLM to ONLY update citations, preserving everything else
    """
    
    if not party_citations:
        return original_response
    
    citation_mapping = []
    for (party1, party2), citations in party_citations.items():
        for cit in citations:
            citation_mapping.append({
                "party1": party1,
                "party2": party2,
                "petitioner_full": cit['petitioner'],
                "respondent_full": cit['respondent'],
                "citation": cit['citation'],
                "case_number": cit['case_number']
            })
    
    # IMPROVED PROMPT - Much more explicit about preserving content
    prompt = f"""CRITICAL RULES - READ CAREFULLY:

1. You MUST return the EXACT original response below
2. ONLY change/add citations for cases that appear in the "CORRECT CITATIONS" list
3. Do NOT rephrase, restructure, or modify ANY other text
4. For cases NOT in the list below: KEEP their existing citations unchanged
5. Do NOT add explanations or commentary

ORIGINAL RESPONSE (PRESERVE EXACTLY):
---START---
{original_response}
---END---

CORRECT CITATIONS (Only update these specific cases):
{json.dumps(citation_mapping, indent=2)}

INSTRUCTIONS:
- Find where each case/party pair from the citation list is mentioned
- Match flexibly: "Shree Govind" matches "SHREE GOVIND ALLOYS PVT. LTD."
- If the case already has a citation: REPLACE with the correct one from the list
- If the case has NO citation: ADD the correct citation from the list
- Format: "Party1 v. Party2 (Citation)" or "Party1 vs Party2 (Citation)"
- For all other cases not in the list: DO NOT TOUCH their citations
- Return the COMPLETE response with only these specific citation updates

EXAMPLE:
Original text: "In ABC Corp v. XYZ Ltd (wrong citation), the court held that..."
Citation list has ABC Corp → "2024 TMI 123"
Corrected: "In ABC Corp v. XYZ Ltd (2024 TMI 123), the court held that..."

NOW RETURN THE COMPLETE RESPONSE WITH ONLY THE SPECIFIED CITATION CORRECTIONS:"""
    
    try:
        reattributed = call_bedrock_for_extraction(prompt, max_tokens=5000)
        
        # Clean markdown formatting
        reattributed = reattributed.strip()
        if reattributed.startswith("```"):
            lines = reattributed.split("\n")
            reattributed = "\n".join(lines[1:-1]) if len(lines) > 2 else reattributed
        
        # VALIDATION: Check if response was drastically modified
        orig_len = len(original_response)
        new_len = len(reattributed)
        length_ratio = new_len / orig_len if orig_len > 0 else 0
        
        # If length changed by more than 30%, likely the LLM rephrased - use fallback
        if length_ratio < 0.7 or length_ratio > 1.3:
            logger.warning(f"⚠️ Response length changed significantly ({orig_len} → {new_len} chars, ratio={length_ratio:.2f})")
            logger.warning(f"   This suggests the LLM rephrased the content. Using fallback method.")
            return original_response + format_citation_section(party_citations)
        
        logger.info(f"✅ Re-attribution successful (length: {orig_len} → {new_len}, ratio={length_ratio:.2f})")
        return reattributed
        
    except Exception as e:
        logger.error(f"Re-attribution failed: {e}")
        return original_response + format_citation_section(party_citations)


def format_citation_section(party_citations: Dict[Tuple[str, str], List[Dict]]) -> str:
    """Fallback: append citations at end"""
    if not party_citations:
        return ""
    
    lines = ["\n\n---\n", "\n**Citations Referenced:**\n"]
    
    for (p1, p2), citations in party_citations.items():
        lines.append(f"\n**{p1} vs {p2}:**")
        for cit in citations:
            case_num = re.sub(r'\s+dated.*$', '', cit['case_number'])
            if case_num:
                lines.append(f"- {cit['citation']} ({case_num})")
            else:
                lines.append(f"- {cit['citation']}")
    
    return "\n".join(lines)


def extract_and_attribute_citations(llm_response: str, all_chunks: list) -> Tuple[str, Dict]:
    """Main function with complete logging"""
    
    print("\n" + "="*100)
    print(" "*30 + "🚀 CITATION ATTRIBUTION SYSTEM")
    print("="*100)
    
    # STEP 1
    print("\n📝 STEP 1: ORIGINAL LLM RESPONSE")
    print("-"*100)
    print(llm_response)
    print("-"*100)
    
    # STEP 2
    print("\n🔍 STEP 2: EXTRACTING PARTY PAIRS")
    print("-"*100)
    
    party_pairs = extract_party_pairs_from_response(llm_response)
    
    print(f"\n✅ Extracted {len(party_pairs)} party pair(s):")
    for i, (p1, p2) in enumerate(party_pairs, 1):
        print(f"   {i}. '{p1}' <-> '{p2}'")
    
    if not party_pairs:
        print("\n⚠️  No party pairs - returning original")
        print("="*100 + "\n")
        return llm_response, {}
    
    # STEP 3
    print("\n🔍 STEP 3: FINDING CITATIONS IN DATABASE")
    print("-"*100)
    
    party_citations = find_citations_for_party_pairs(party_pairs, all_chunks)
    
    print(f"\n📊 FOUND {len(party_citations)} matching pair(s):")
    for (p1, p2), citations in party_citations.items():
        print(f"\n   '{p1}' vs '{p2}':")
        for cit in citations:
            print(f"      ✅ {cit['citation']}")
            print(f"         Pet: {cit['petitioner']}")
            print(f"         Resp: {cit['respondent']}")
    
    if not party_citations:
        print("\n⚠️  No citations found - returning original")
        print("="*100 + "\n")
        return llm_response, {}
    
    # STEP 4
    print("\n✨ STEP 4: RE-ATTRIBUTING CITATIONS (PRESERVING ORIGINAL CONTENT)")
    print("-"*100)
    
    enhanced_response = reattribute_citations_in_response(llm_response, party_citations)
    
    print("\n📝 FINAL ENHANCED RESPONSE:")
    print("-"*100)
    print(enhanced_response)
    print("-"*100)
    
    print("\n📊 SUMMARY:")
    print(f"   Pairs extracted: {len(party_pairs)}")
    print(f"   Citations found: {sum(len(c) for c in party_citations.values())}")
    print(f"   Original: {len(llm_response)} chars")
    print("i am printing the actual response .............................")
    print(llm_response)
    print(f"   Enhanced: {len(enhanced_response)} chars")
    print(" i am printing the enhanced_response")
    print(enhanced_response)
    print(f"   Length ratio: {len(enhanced_response)/len(llm_response):.2f}")
    
    print("\n" + "="*100)
    print(" "*25 + "✅ CITATION ATTRIBUTION COMPLETE")
    print("="*100 + "\n")
    
    return enhanced_response, party_citations