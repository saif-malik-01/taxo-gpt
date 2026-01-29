# # """
# # Extract party pairs and citations from LLM response
# # """

# # import json
# # import re
# # import logging
# # from typing import List, Dict, Tuple
# # import os
# # import boto3
# # from botocore.config import Config

# # logger = logging.getLogger(__name__)


# # def get_bedrock_client():
# #     """Initialize AWS Bedrock client"""
# #     config = Config(
# #         region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
# #         signature_version='v4',
# #         retries={'max_attempts': 3, 'mode': 'standard'}
# #     )
    
# #     return boto3.client(
# #         'bedrock-runtime',
# #         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
# #         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
# #         config=config
# #     )


# # def call_bedrock_for_extraction(prompt: str) -> str:
# #     """Call AWS Bedrock with Qwen model"""
# #     try:
# #         client = get_bedrock_client()
# #         model_id = "qwen.qwen3-next-80b-a3b"
        
# #         request_body = {
# #             "max_tokens": 1000,
# #             "messages": [{"role": "user", "content": prompt}],
# #             "temperature": 0.0
# #         }
        
# #         response = client.invoke_model(
# #             modelId=model_id,
# #             body=json.dumps(request_body)
# #         )
        
# #         response_body = json.loads(response['body'].read())
# #         return response_body['choices'][0]['message']['content']
        
# #     except Exception as e:
# #         logger.error(f"Bedrock API error: {e}")
# #         raise


# # def extract_party_pairs_from_response(llm_response: str) -> List[Tuple[str, str]]:
# #     """
# #     Extract all party name pairs from LLM response
    
# #     Examples:
# #     "Safari Retreat vs State of Karnataka" → [("Safari Retreat", "State of Karnataka")]
# #     "In ABC vs XYZ and PQR vs LMN cases..." → [("ABC", "XYZ"), ("PQR", "LMN")]
    
# #     Returns:
# #         List of tuples: [(party1, party2), ...]
# #         Note: We don't know which is petitioner/respondent, just that they're a pair
# #     """
    
# #     try:
# #         prompt = f"""Extract ALL party name pairs from this legal text. Return ONLY valid JSON.

# # Text: {llm_response}

# # A party pair is two names connected by "vs", "v.", "versus", or mentioned together in a case context.

# # Return format:
# # {{
# #     "party_pairs": [
# #         {{"party1": "Name 1", "party2": "Name 2"}},
# #         {{"party1": "Name 3", "party2": "Name 4"}}
# #     ]
# # }}

# # Examples:

# # Text: "In Safari Retreat vs State of Karnataka, the court held..."
# # Output: {{"party_pairs": [{{"party1": "Safari Retreat", "party2": "State of Karnataka"}}]}}

# # Text: "Based on ABC vs XYZ and PQR vs LMN judgments..."
# # Output: {{"party_pairs": [{{"party1": "ABC", "party2": "XYZ"}}, {{"party1": "PQR", "party2": "LMN"}}]}}

# # Text: "The principle applies generally."
# # Output: {{"party_pairs": []}}

# # Now extract from the given text. Return ONLY JSON, no other text."""
        
# #         content = call_bedrock_for_extraction(prompt)
        
# #         # Clean response
# #         content = content.strip()
# #         if content.startswith("```"):
# #             lines = content.split("\n")
# #             content = "\n".join(lines[1:-1]) if len(lines) > 2 else content
# #             if content.startswith("json"):
# #                 content = content[4:].strip()
        
# #         extracted = json.loads(content.strip())
        
# #         # Convert to list of tuples
# #         pairs = []
# #         for pair_dict in extracted.get("party_pairs", []):
# #             party1 = pair_dict.get("party1")
# #             party2 = pair_dict.get("party2")
# #             if party1 and party2:
# #                 pairs.append((party1, party2))
        
# #         logger.info(f"✅ Extracted {len(pairs)} party pairs from LLM response")
# #         for p1, p2 in pairs:
# #             logger.info(f"   Pair: '{p1}' <-> '{p2}'")
        
# #         return pairs
        
# #     except Exception as e:
# #         logger.warning(f"Party pair extraction failed: {e}, falling back to regex")
# #         return regex_extract_party_pairs(llm_response)


# # def regex_extract_party_pairs(text: str) -> List[Tuple[str, str]]:
# #     """
# #     Regex fallback for extracting party pairs
    
# #     Patterns:
# #     - "Name1 vs Name2"
# #     - "Name1 v. Name2"
# #     - "Name1 versus Name2"
# #     """
    
# #     pairs = []
    
# #     # Pattern: "Name1 vs/v./versus Name2"
# #     patterns = [
# #         r'([A-Z][A-Za-z\s&.]+?)\s+vs\.?\s+([A-Z][A-Za-z\s&.]+?)(?:\s|,|\.|\()',
# #         r'([A-Z][A-Za-z\s&.]+?)\s+v\.?\s+([A-Z][A-Za-z\s&.]+?)(?:\s|,|\.|\()',
# #         r'([A-Z][A-Za-z\s&.]+?)\s+versus\s+([A-Z][A-Za-z\s&.]+?)(?:\s|,|\.|\()',
# #     ]
    
# #     for pattern in patterns:
# #         matches = re.finditer(pattern, text, re.IGNORECASE)
# #         for match in matches:
# #             party1 = match.group(1).strip()
# #             party2 = match.group(2).strip()
            
# #             # Clean up
# #             party1 = re.sub(r'\s+', ' ', party1)
# #             party2 = re.sub(r'\s+', ' ', party2)
            
# #             pairs.append((party1, party2))
    
# #     # Remove duplicates while preserving order
# #     seen = set()
# #     unique_pairs = []
# #     for pair in pairs:
# #         # Normalize for dedup (lowercase)
# #         normalized = tuple(sorted([p.lower() for p in pair]))
# #         if normalized not in seen:
# #             seen.add(normalized)
# #             unique_pairs.append(pair)
    
# #     logger.info(f"✅ Regex extracted {len(unique_pairs)} party pairs")
# #     return unique_pairs


# # def normalize_party_name_for_matching(name: str) -> str:
# #     """
# #     Normalize party name for matching (same as citation_matcher.py)
# #     """
# #     if not name:
# #         return ""
    
# #     name = name.lower()
    
# #     # Remove "& ORS." and variations
# #     name = re.sub(r'\s*[&,]\s*ors\.?', '', name, flags=re.IGNORECASE)
# #     name = re.sub(r'\s*&\s*others?', '', name, flags=re.IGNORECASE)
    
# #     # Remove titles
# #     titles = [
# #         r'\bmr\.?\b', r'\bmrs\.?\b', r'\bms\.?\b', r'\bdr\.?\b',
# #         r'\bprof\.?\b', r'\bhon\.?\b', r'\bjustice\.?\b',
# #         r'\bsri\.?\b', r'\bsmt\.?\b', r'\bm/s\.?\b',
# #     ]
    
# #     for title in titles:
# #         name = re.sub(title, '', name, flags=re.IGNORECASE)
    
# #     # Remove legal suffixes
# #     suffixes = [
# #         r'\bpvt\.?\s*ltd\.?', r'\bprivate\s+limited\b',
# #         r'\bltd\.?\b', r'\blimited\b',
# #         r'\binc\.?\b', r'\bllc\.?\b', r'\bllp\.?\b',
# #         r'\bco\.?\b', r'\bcorp\.?\b',
# #     ]
    
# #     for suffix in suffixes:
# #         name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
# #     # Clean up
# #     name = re.sub(r'[^\w\s]', ' ', name)
# #     name = re.sub(r'\s+', ' ', name).strip()
    
# #     return name


# # def find_citations_for_party_pairs(
# #     party_pairs: List[Tuple[str, str]], 
# #     all_chunks: list
# # ) -> Dict[Tuple[str, str], List[Dict]]:
# #     """
# #     Find all citations where the party pair matches (in either order)
    
# #     Args:
# #         party_pairs: List of (party1, party2) tuples
# #         all_chunks: All chunks from database
        
# #     Returns:
# #         Dict mapping party pair to list of matching citations:
# #         {
# #             ("Safari Retreat", "State of Karnataka"): [
# #                 {
# #                     "citation": "2017 Taxo.online 42",
# #                     "case_number": "WP(C). No. 34021 of 2017",
# #                     "petitioner": "Safari Retreat Pvt. Ltd.",
# #                     "respondent": "State of Karnataka",
# #                     "external_id": "J001"
# #                 },
# #                 ...
# #             ],
# #             ...
# #         }
# #     """
    
# #     results = {}
    
# #     for party1, party2 in party_pairs:
# #         # Normalize names
# #         party1_norm = normalize_party_name_for_matching(party1)
# #         party2_norm = normalize_party_name_for_matching(party2)
        
# #         if not party1_norm or not party2_norm:
# #             continue
        
# #         logger.info(f"🔍 Searching citations for pair: '{party1}' <-> '{party2}'")
# #         logger.info(f"   Normalized: '{party1_norm}' <-> '{party2_norm}'")
        
# #         matching_citations = []
# #         seen_external_ids = set()
        
# #         # Search through all judgment chunks
# #         for chunk in all_chunks:
# #             if chunk.get("chunk_type") != "judgment":
# #                 continue
            
# #             metadata = chunk.get("metadata", {})
# #             external_id = metadata.get("external_id")
            
# #             if not external_id or external_id in seen_external_ids:
# #                 continue
            
# #             db_petitioner = metadata.get("petitioner", "")
# #             db_respondent = metadata.get("respondent", "")
            
# #             # Normalize DB names
# #             db_pet_norm = normalize_party_name_for_matching(db_petitioner)
# #             db_resp_norm = normalize_party_name_for_matching(db_respondent)
            
# #             # Check if pair matches (in either order)
# #             match_forward = (party1_norm == db_pet_norm and party2_norm == db_resp_norm)
# #             match_reverse = (party1_norm == db_resp_norm and party2_norm == db_pet_norm)
            
# #             if match_forward or match_reverse:
# #                 seen_external_ids.add(external_id)
                
# #                 citation_info = {
# #                     "citation": metadata.get("citation", ""),
# #                     "case_number": metadata.get("case_number", ""),
# #                     "petitioner": db_petitioner,
# #                     "respondent": db_respondent,
# #                     "external_id": external_id,
# #                     "court": metadata.get("court", ""),
# #                     "year": metadata.get("year", ""),
# #                     "decision": metadata.get("decision", "")
# #                 }
                
# #                 matching_citations.append(citation_info)
                
# #                 logger.info(f"   ✅ Found: {citation_info['citation']} (ID: {external_id})")
        
# #         if matching_citations:
# #             results[(party1, party2)] = matching_citations
# #             logger.info(f"   Total: {len(matching_citations)} citations for this pair")
# #         else:
# #             logger.info(f"   ❌ No citations found for this pair")
    
# #     return results


# # def format_citation_attribution(
# #     party_citations: Dict[Tuple[str, str], List[Dict]]
# # ) -> str:
# #     """
# #     Format citation attributions as markdown text to append to LLM response
    
# #     Example output:
# #     '''
    
# #     ---
    
# #     **Citations Referenced:**
    
# #     **Safari Retreat vs State of Karnataka:**
# #     - 2017 Taxo.online 42 (WP(C). No. 34021 of 2017)
    
# #     **ABC Company vs XYZ Ltd:**
# #     - 2020 (5) TMI 123
# #     - 2021 (3) TMI 456
# #     '''
# #     """
    
# #     if not party_citations:
# #         return ""
    
# #     lines = ["\n\n---\n", "\n**Citations Referenced:**\n"]
    
# #     for (party1, party2), citations in party_citations.items():
# #         # Party pair header
# #         lines.append(f"\n**{party1} vs {party2}:**")
        
# #         # List all citations for this pair
# #         for cit_info in citations:
# #             citation = cit_info.get("citation", "N/A")
# #             case_number = cit_info.get("case_number", "")
            
# #             if case_number:
# #                 lines.append(f"- {citation} ({case_number})")
# #             else:
# #                 lines.append(f"- {citation}")
    
# #     return "\n".join(lines)


# # def extract_and_attribute_citations(llm_response: str, all_chunks: list) -> Tuple[str, Dict]:
# #     """
# #     Main function: Extract party pairs from LLM response, find citations, append attribution
    
# #     Args:
# #         llm_response: The generated LLM response text
# #         all_chunks: All chunks from database
        
# #     Returns:
# #         Tuple of:
# #         - Enhanced response with citation attribution appended
# #         - Dictionary of party citations for metadata
# #     """
    
# #     logger.info("=" * 80)
# #     logger.info("Starting citation extraction and attribution from LLM response")
# #     logger.info("=" * 80)
    
# #     # Step 1: Extract party pairs from response
# #     party_pairs = extract_party_pairs_from_response(llm_response)
    
# #     if not party_pairs:
# #         logger.info("No party pairs found in response - returning original response")
# #         return llm_response, {}
    
# #     # Step 2: Find citations for each pair
# #     party_citations = find_citations_for_party_pairs(party_pairs, all_chunks)
    
# #     if not party_citations:
# #         logger.info("No matching citations found - returning original response")
# #         return llm_response, {}
    
# #     # Step 3: Format citation attribution
# #     citation_text = format_citation_attribution(party_citations)
    
# #     # Step 4: Append to response
# #     enhanced_response = llm_response + citation_text
    
# #     logger.info("=" * 80)
# #     logger.info(f"Citation attribution complete: Added {len(party_citations)} party pair(s)")
# #     logger.info("=" * 80)
    
# #     return enhanced_response, party_citations

# """
# Extract party pairs and citations from LLM response - OPTIMIZED
# """

# import json
# import re
# import logging
# from typing import List, Dict, Tuple
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


# def call_bedrock_for_extraction(prompt: str) -> str:
#     """Call AWS Bedrock with Qwen model"""
#     try:
#         client = get_bedrock_client()
#         model_id = "qwen.qwen3-next-80b-a3b"
        
#         request_body = {
#             "max_tokens": 1500,  # Increased for better extraction
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
#     Extract all party name pairs from LLM response using OPTIMIZED prompt
    
#     Returns:
#         List of tuples: [(party1, party2), ...]
#     """
    
#     try:
#         # OPTIMIZED PROMPT - More specific and concise
#         prompt = f"""Extract ALL party name pairs mentioned in this legal text.

# RULES:
# 1. A party pair is two entities connected by "vs", "v.", "versus", or mentioned in case context
# 2. Extract ONLY the core names - ignore titles (Mr, Mrs, Dr, etc.)
# 3. Ignore suffixes like "Pvt Ltd", "Private Limited", "& Ors", "& Others"
# 4. Return clean names without punctuation
# 5. If no pairs found, return empty array

# Return ONLY this JSON format:
# {{
#     "pairs": [
#         ["Party 1 Name", "Party 2 Name"],
#         ["Party 3 Name", "Party 4 Name"]
#     ]
# }}

# Examples:

# Input: "In M/s. Safari Retreat Pvt. Ltd. vs State of Karnataka judgment..."
# Output: {{"pairs": [["Safari Retreat", "State of Karnataka"]]}}

# Input: "Based on Dr. ABC Company vs Mr. XYZ & Ors. and PQR Ltd. versus LMN cases..."
# Output: {{"pairs": [["ABC Company", "XYZ"], ["PQR", "LMN"]]}}

# Input: "Section 15 of the GST Act states..."
# Output: {{"pairs": []}}

# Text to analyze:
# {llm_response}

# Return ONLY JSON:"""
        
#         content = call_bedrock_for_extraction(prompt)
        
#         # Clean response
#         content = content.strip()
#         if content.startswith("```"):
#             lines = content.split("\n")
#             content = "\n".join(lines[1:-1]) if len(lines) > 2 else content
#             if content.startswith("json"):
#                 content = content[4:].strip()
#         content = content.strip()
        
#         logger.debug(f"Raw extraction response: {content}")
        
#         extracted = json.loads(content)
        
#         # Convert to list of tuples
#         pairs = []
#         for pair_list in extracted.get("pairs", []):
#             if isinstance(pair_list, list) and len(pair_list) == 2:
#                 party1 = pair_list[0]
#                 party2 = pair_list[1]
#                 if party1 and party2:
#                     # Additional cleanup
#                     party1 = clean_party_name(party1)
#                     party2 = clean_party_name(party2)
#                     if party1 and party2:
#                         pairs.append((party1, party2))
        
#         logger.info(f"✅ Extracted {len(pairs)} party pairs from LLM response")
#         for p1, p2 in pairs:
#             logger.info(f"   📌 Pair: '{p1}' <-> '{p2}'")
        
#         return pairs
        
#     except Exception as e:
#         logger.warning(f"LLM party pair extraction failed: {e}, falling back to regex")
#         return regex_extract_party_pairs(llm_response)


# def clean_party_name(name: str) -> str:
#     """
#     Clean party name extracted by LLM
#     Removes any remaining titles, suffixes, punctuation
#     """
#     if not name:
#         return ""
    
#     name = name.strip()
    
#     # Remove common prefixes that LLM might have left
#     prefixes = [r'^M/s\.?\s*', r'^Messrs\.?\s*', r'^Mr\.?\s*', r'^Mrs\.?\s*', 
#                 r'^Dr\.?\s*', r'^Ms\.?\s*', r'^Prof\.?\s*']
#     for prefix in prefixes:
#         name = re.sub(prefix, '', name, flags=re.IGNORECASE)
    
#     # Remove common suffixes
#     suffixes = [r'\s*Pvt\.?\s*Ltd\.?$', r'\s*Private\s+Limited$', 
#                 r'\s*Ltd\.?$', r'\s*Limited$',
#                 r'\s*&\s*Ors\.?$', r'\s*&\s*Others?$',
#                 r'\s*Inc\.?$', r'\s*LLC\.?$']
#     for suffix in suffixes:
#         name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
#     # Clean up extra spaces and punctuation
#     name = re.sub(r'\s+', ' ', name)
#     name = name.strip()
    
#     return name


# def regex_extract_party_pairs(text: str) -> List[Tuple[str, str]]:
#     """
#     Regex fallback for extracting party pairs
#     """
    
#     pairs = []
    
#     # Patterns for "Name1 vs/v./versus Name2"
#     patterns = [
#         r'([A-Z][A-Za-z\s&.,]+?)\s+vs\.?\s+([A-Z][A-Za-z\s&.,]+?)(?:\s|,|\.|\(|case|judgment)',
#         r'([A-Z][A-Za-z\s&.,]+?)\s+v\.?\s+([A-Z][A-Za-z\s&.,]+?)(?:\s|,|\.|\(|case|judgment)',
#         r'([A-Z][A-Za-z\s&.,]+?)\s+versus\s+([A-Z][A-Za-z\s&.,]+?)(?:\s|,|\.|\(|case|judgment)',
#     ]
    
#     for pattern in patterns:
#         matches = re.finditer(pattern, text, re.IGNORECASE)
#         for match in matches:
#             party1 = clean_party_name(match.group(1))
#             party2 = clean_party_name(match.group(2))
            
#             if party1 and party2:
#                 pairs.append((party1, party2))
    
#     # Remove duplicates
#     seen = set()
#     unique_pairs = []
#     for pair in pairs:
#         normalized = tuple(sorted([p.lower() for p in pair]))
#         if normalized not in seen:
#             seen.add(normalized)
#             unique_pairs.append(pair)
    
#     logger.info(f"✅ Regex extracted {len(unique_pairs)} party pairs")
#     return unique_pairs


# def normalize_party_name_for_matching(name: str) -> str:
#     """
#     COMPREHENSIVE normalization for matching
    
#     Removes:
#     - Titles: Mr, Mrs, Dr, Prof, Hon, Justice, Sri, Smt, M/s, Messrs
#     - Suffixes: Pvt Ltd, Private Limited, Ltd, Limited, Inc, LLC, LLP, Co, Corp
#     - & ORS., & Others, and Others, ORG
#     - All punctuation (dots, commas)
#     - Extra spaces
    
#     Examples:
#     "M/s. Safari Retreat Pvt. Ltd. & Ors." → "safari retreat"
#     "Dr. Sharma & Others" → "sharma"
#     "State of Karnataka" → "state of karnataka"
#     """
#     if not name:
#         return ""
    
#     name = name.lower()
    
#     # Remove "ORG" and "& ORS." variations (PRIORITY - do this first)
#     name = re.sub(r'\s*org\.?\s*', ' ', name, flags=re.IGNORECASE)
#     name = re.sub(r'\s*[&,]\s*ors\.?\s*', '', name, flags=re.IGNORECASE)
#     name = re.sub(r'\s*&\s*others?\s*', '', name, flags=re.IGNORECASE)
#     name = re.sub(r'\s*and\s+others?\s*', '', name, flags=re.IGNORECASE)
    
#     # Remove titles (comprehensive list)
#     titles = [
#         r'\bmr\.?\b', r'\bmrs\.?\b', r'\bms\.?\b', r'\bmiss\.?\b',
#         r'\bdr\.?\b', r'\bprof\.?\b', r'\bprofessor\.?\b',
#         r'\bhon\.?\b', r'\bhonourable\.?\b', r'\bhonorable\.?\b',
#         r'\bjustice\.?\b', r'\bj\.?\b',
#         r'\bsri\.?\b', r'\bsmt\.?\b', r'\bshri\.?\b',
#         r'\bm/s\.?\b', r'\bmessrs\.?\b', r'\bmr\b', r'\bmrs\b', r'\bdr\b',
#     ]
    
#     for title in titles:
#         name = re.sub(title, '', name, flags=re.IGNORECASE)
    
#     # Remove legal suffixes (comprehensive list)
#     suffixes = [
#         r'\bpvt\.?\s*ltd\.?\b', r'\bprivate\s+limited\b',
#         r'\bltd\.?\b', r'\blimited\b',
#         r'\binc\.?\b', r'\bincorporated\b',
#         r'\bllc\.?\b', r'\bllp\.?\b',
#         r'\bco\.?\b', r'\bcompany\b',
#         r'\bcorp\.?\b', r'\bcorporation\b',
#     ]
    
#     for suffix in suffixes:
#         name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
#     # Remove ALL punctuation (dots, commas, etc.)
#     name = re.sub(r'[^\w\s]', ' ', name)
    
#     # Remove extra spaces
#     name = re.sub(r'\s+', ' ', name)
#     name = name.strip()
    
#     return name


# def find_citations_for_party_pairs(
#     party_pairs: List[Tuple[str, str]], 
#     all_chunks: list
# ) -> Dict[Tuple[str, str], List[Dict]]:
#     """
#     Find all citations where the party pair matches (in either order)
    
#     Uses comprehensive normalization to handle all variations
#     """
    
#     results = {}
    
#     for party1, party2 in party_pairs:
#         # Normalize names using comprehensive normalization
#         party1_norm = normalize_party_name_for_matching(party1)
#         party2_norm = normalize_party_name_for_matching(party2)
        
#         if not party1_norm or not party2_norm or len(party1_norm) < 2 or len(party2_norm) < 2:
#             logger.warning(f"⚠️  Skipping invalid pair after normalization: '{party1_norm}' <-> '{party2_norm}'")
#             continue
        
#         logger.info(f"🔍 Searching citations for pair:")
#         logger.info(f"   Original: '{party1}' <-> '{party2}'")
#         logger.info(f"   Normalized: '{party1_norm}' <-> '{party2_norm}'")
        
#         matching_citations = []
#         seen_external_ids = set()
        
#         # Search through all judgment chunks
#         for chunk in all_chunks:
#             if chunk.get("chunk_type") != "judgment":
#                 continue
            
#             metadata = chunk.get("metadata", {})
#             external_id = metadata.get("external_id")
            
#             if not external_id or external_id in seen_external_ids:
#                 continue
            
#             db_petitioner = metadata.get("petitioner", "")
#             db_respondent = metadata.get("respondent", "")
            
#             # Normalize DB names using same comprehensive normalization
#             db_pet_norm = normalize_party_name_for_matching(db_petitioner)
#             db_resp_norm = normalize_party_name_for_matching(db_respondent)
            
#             # Check if pair matches (in either order)
#             match_forward = (party1_norm == db_pet_norm and party2_norm == db_resp_norm)
#             match_reverse = (party1_norm == db_resp_norm and party2_norm == db_pet_norm)
            
#             if match_forward or match_reverse:
#                 seen_external_ids.add(external_id)
                
#                 citation_info = {
#                     "citation": metadata.get("citation", ""),
#                     "case_number": metadata.get("case_number", ""),
#                     "petitioner": db_petitioner,
#                     "respondent": db_respondent,
#                     "external_id": external_id,
#                     "court": metadata.get("court", ""),
#                     "year": metadata.get("year", ""),
#                     "decision": metadata.get("decision", "")
#                 }
                
#                 matching_citations.append(citation_info)
                
#                 logger.info(f"   ✅ MATCH FOUND!")
#                 logger.info(f"      Citation: {citation_info['citation']}")
#                 logger.info(f"      DB Petitioner: '{db_petitioner}' (normalized: '{db_pet_norm}')")
#                 logger.info(f"      DB Respondent: '{db_respondent}' (normalized: '{db_resp_norm}')")
#                 logger.info(f"      Match Type: {'Forward' if match_forward else 'Reverse'}")
        
#         if matching_citations:
#             results[(party1, party2)] = matching_citations
#             logger.info(f"   📊 Total: {len(matching_citations)} citation(s) for this pair")
#         else:
#             logger.warning(f"   ❌ No citations found for this pair")
    
#     return results


# def format_citation_attribution(
#     party_citations: Dict[Tuple[str, str], List[Dict]]
# ) -> str:
#     """
#     Format citation attributions as clean markdown text
#     """
    
#     if not party_citations:
#         return ""
    
#     lines = ["\n\n---\n", "\n**Citations Referenced:**\n"]
    
#     for (party1, party2), citations in party_citations.items():
#         # Party pair header
#         lines.append(f"\n**{party1} vs {party2}:**")
        
#         # List all citations for this pair
#         for cit_info in citations:
#             citation = cit_info.get("citation", "N/A")
#             case_number = cit_info.get("case_number", "")
            
#             # Format citation line
#             if case_number:
#                 # Remove date from case number for cleaner display
#                 case_num_clean = re.sub(r'\s+dated\s+\d{1,2}\.\d{1,2}\.\d{4}.*$', '', case_number)
#                 lines.append(f"- {citation} ({case_num_clean})")
#             else:
#                 lines.append(f"- {citation}")
    
#     return "\n".join(lines)


# def extract_and_attribute_citations(llm_response: str, all_chunks: list) -> Tuple[str, Dict]:
#     """
#     Main function: Extract party pairs from LLM response, find citations, append attribution
    
#     WITH DETAILED LOGGING
#     """
    
#     logger.info("=" * 100)
#     logger.info("🚀 STARTING CITATION EXTRACTION AND ATTRIBUTION")
#     logger.info("=" * 100)
    
#     # Print original LLM response
#     logger.info("\n📝 ORIGINAL LLM RESPONSE:")
#     logger.info("-" * 100)
#     logger.info(llm_response)
#     logger.info("-" * 100)
    
#     # Step 1: Extract party pairs from response
#     logger.info("\n🔍 STEP 1: Extracting party pairs from response...")
#     party_pairs = extract_party_pairs_from_response(llm_response)
    
#     logger.info(f"\n📊 EXTRACTION RESULTS:")
#     logger.info(f"   Found {len(party_pairs)} party pair(s)")
#     for i, (p1, p2) in enumerate(party_pairs, 1):
#         logger.info(f"   {i}. '{p1}' <-> '{p2}'")
    
#     if not party_pairs:
#         logger.info("\n⚠️  No party pairs found in response - returning original response")
#         logger.info("=" * 100)
#         return llm_response, {}
    
#     # Step 2: Find citations for each pair
#     logger.info("\n🔍 STEP 2: Finding citations for each party pair...")
#     party_citations = find_citations_for_party_pairs(party_pairs, all_chunks)
    
#     logger.info(f"\n📊 CITATION MATCHING RESULTS:")
#     logger.info(f"   Matched {len(party_citations)} party pair(s) to citations")
#     for (p1, p2), citations in party_citations.items():
#         logger.info(f"   '{p1}' vs '{p2}': {len(citations)} citation(s)")
#         for cit in citations:
#             logger.info(f"      - {cit['citation']}")
    
#     if not party_citations:
#         logger.info("\n⚠️  No matching citations found - returning original response")
#         logger.info("=" * 100)
#         return llm_response, {}
    
#     # Step 3: Format citation attribution
#     logger.info("\n📝 STEP 3: Formatting citation attribution...")
#     citation_text = format_citation_attribution(party_citations)
    
#     logger.info("   Citation attribution text:")
#     logger.info(citation_text)
    
#     # Step 4: Append to response
#     enhanced_response = llm_response + citation_text
    
#     logger.info("\n✅ FINAL ENHANCED RESPONSE:")
#     logger.info("-" * 100)
#     logger.info(enhanced_response)
#     logger.info("-" * 100)
    
#     logger.info(f"\n📊 SUMMARY:")
#     logger.info(f"   Party pairs extracted: {len(party_pairs)}")
#     logger.info(f"   Citations found: {sum(len(cits) for cits in party_citations.values())}")
#     logger.info(f"   Original response length: {len(llm_response)} chars")
#     logger.info(f"   Enhanced response length: {len(enhanced_response)} chars")
#     logger.info(f"   Attribution added: {len(citation_text)} chars")
    
#     logger.info("=" * 100)
#     logger.info("✨ CITATION ATTRIBUTION COMPLETE")
#     logger.info("=" * 100)
    
#     return enhanced_response, party_citations

"""
Ultimate Citation Extractor with Edge Case Handling and LLM Re-attribution
"""

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
    Extract ALL party name pairs with better handling of abbreviations and variations
    """
    
    try:
        prompt = f"""Extract ALL party name pairs from this legal text. Be very careful with variations.

CRITICAL RULES:
1. Extract pairs connected by "vs", "v.", "v", "versus"
2. Keep full names as written (we'll normalize later)
3. Include abbreviations like "AC GST", "DGST", "State of Gujarat"
4. Don't skip incomplete-looking names
5. Return empty if no clear pairs

Return ONLY this JSON:
{{
    "pairs": [
        ["Party 1", "Party 2"],
        ["Party 3", "Party 4"]
    ]
}}

Examples:

Input: "Shree Govind Alloys Pvt. Ltd. v. AC GST (2022)"
Output: {{"pairs": [["Shree Govind Alloys Pvt. Ltd.", "AC GST"]]}}

Input: "In Commissioner of GST vs ABC Company case..."
Output: {{"pairs": [["Commissioner of GST", "ABC Company"]]}}

Input: "State of Gujarat & Ors. v. XYZ Ltd."
Output: {{"pairs": [["State of Gujarat & Ors.", "XYZ Ltd."]]}}

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
                if p1 and p2:
                    pairs.append((p1, p2))
        
        logger.info(f"✅ Extracted {len(pairs)} party pairs")
        for i, (p1, p2) in enumerate(pairs, 1):
            logger.info(f"   {i}. '{p1}' <-> '{p2}'")
        
        return pairs
        
    except Exception as e:
        logger.warning(f"LLM extraction failed: {e}")
        return regex_extract_party_pairs(llm_response)


def regex_extract_party_pairs(text: str) -> List[Tuple[str, str]]:
    """Enhanced regex extraction"""
    pairs = []
    
    # Patterns for various "vs" formats
    patterns = [
        r'([A-Z][A-Za-z\s&.,()]+?)\s+v\.?\s+([A-Z][A-Za-z\s&.,()]+?)(?:\s+\(|$|\.|,|\s+case|\s+judgment)',
        r'([A-Z][A-Za-z\s&.,()]+?)\s+vs\.?\s+([A-Z][A-Za-z\s&.,()]+?)(?:\s+\(|$|\.|,|\s+case|\s+judgment)',
        r'([A-Z][A-Za-z\s&.,()]+?)\s+versus\s+([A-Z][A-Za-z\s&.,()]+?)(?:\s+\(|$|\.|,|\s+case|\s+judgment)',
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, text)
        for match in matches:
            p1 = match.group(1).strip()
            p2 = match.group(2).strip()
            if p1 and p2:
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
    ULTIMATE normalization - handles ALL edge cases
    """
    if not name:
        return ""
    
    original = name
    name = name.lower()
    
    # STEP 1: Remove ORG, & ORS., & OTHERS first (highest priority)
    name = re.sub(r'\s*org\.?\s*', ' ', name)
    name = re.sub(r'\s*[&,]\s*ors\.?\s*', '', name)
    name = re.sub(r'\s*&\s*others?\s*', '', name)
    name = re.sub(r'\s*and\s+others?\s*', '', name)
    
    # STEP 2: Remove comprehensive title list
    titles = [
        r'\bm/s\.?\s*', r'\bmessrs\.?\s*',
        r'\bmr\.?\s*', r'\bmrs\.?\s*', r'\bms\.?\s*', r'\bmiss\.?\s*',
        r'\bdr\.?\s*', r'\bprof\.?\s*', r'\bprofessor\.?\s*',
        r'\bhon\.?\s*', r'\bhonourable\.?\s*', r'\bhon\'ble\.?\s*',
        r'\bjustice\.?\s*', r'\bj\.?\s*',
        r'\bsri\.?\s*', r'\bsmt\.?\s*', r'\bshri\.?\s*', r'\bshree\.?\s*',
        r'\bmr\b', r'\bmrs\b', r'\bdr\b', r'\bms\b',
    ]
    
    for title in titles:
        name = re.sub(title, '', name)
    
    # STEP 3: Remove legal entity suffixes
    suffixes = [
        r'\bpvt\.?\s*ltd\.?\b', r'\bprivate\s+limited\b',
        r'\bp\.?\s*ltd\.?\b', r'\bpvt\b',
        r'\bltd\.?\b', r'\blimited\b',
        r'\binc\.?\b', r'\bincorporated\b',
        r'\bllc\.?\b', r'\bllp\.?\b',
        r'\bco\.?\b', r'\bcompany\b',
        r'\bcorp\.?\b', r'\bcorporation\b',
    ]
    
    for suffix in suffixes:
        name = re.sub(suffix, '', name)
    
    # STEP 4: Handle government entity abbreviations
    # Keep these patterns but normalize them
    name = re.sub(r'\bcommissioner\s+of\s+gst\b', 'commissioner gst', name)
    name = re.sub(r'\bac\s+gst\b', 'assistant commissioner gst', name)
    name = re.sub(r'\bdgst\b', 'director general gst', name)
    name = re.sub(r'\bccgst\b', 'chief commissioner gst', name)
    name = re.sub(r'\bstate\s+of\s+', 'state ', name)
    
    # STEP 5: Remove ALL punctuation
    name = re.sub(r'[^\w\s]', ' ', name)
    
    # STEP 6: Clean up spaces
    name = re.sub(r'\s+', ' ', name).strip()
    
    logger.debug(f"Normalized: '{original}' → '{name}'")
    
    return name


def fuzzy_match_score(str1: str, str2: str) -> float:
    """
    Calculate fuzzy match score between two strings
    Returns 1.0 for exact match, 0.0-1.0 for partial matches
    """
    if str1 == str2:
        return 1.0
    
    # Check if one is substring of other
    if str1 in str2 or str2 in str1:
        # Calculate overlap ratio
        shorter = min(len(str1), len(str2))
        longer = max(len(str1), len(str2))
        return shorter / longer
    
    # Check word overlap
    words1 = set(str1.split())
    words2 = set(str2.split())
    
    if not words1 or not words2:
        return 0.0
    
    common = words1 & words2
    total = words1 | words2
    
    return len(common) / len(total) if total else 0.0


def find_citations_for_party_pairs(
    party_pairs: List[Tuple[str, str]], 
    all_chunks: list,
    fuzzy_threshold: float = 0.6
) -> Dict[Tuple[str, str], List[Dict]]:
    """
    Find citations with fuzzy matching for edge cases
    """
    
    results = {}
    
    for party1, party2 in party_pairs:
        party1_norm = normalize_party_name(party1)
        party2_norm = normalize_party_name(party2)
        
        if not party1_norm or not party2_norm:
            logger.warning(f"⚠️  Skipping invalid pair: '{party1}' <-> '{party2}'")
            continue
        
        logger.info(f"\n{'='*80}")
        logger.info(f"🔍 Searching for: '{party1}' <-> '{party2}'")
        logger.info(f"   Normalized: '{party1_norm}' <-> '{party2_norm}'")
        
        matching_citations = []
        seen_external_ids = set()
        best_matches = {}  # external_id -> (score, citation_info)
        
        for chunk in all_chunks:
            if chunk.get("chunk_type") != "judgment":
                continue
            
            metadata = chunk.get("metadata", {})
            external_id = metadata.get("external_id")
            
            if not external_id or external_id in seen_external_ids:
                continue
            
            db_petitioner = metadata.get("petitioner", "")
            db_respondent = metadata.get("respondent", "")
            
            db_pet_norm = normalize_party_name(db_petitioner)
            db_resp_norm = normalize_party_name(db_respondent)
            
            logger.debug(f"Checking {external_id}:")
            logger.debug(f"  DB Pet: '{db_petitioner}' → '{db_pet_norm}'")
            logger.debug(f"  DB Resp: '{db_respondent}' → '{db_resp_norm}'")
            
            # EXACT MATCH (both parties)
            exact_forward = (party1_norm == db_pet_norm and party2_norm == db_resp_norm)
            exact_reverse = (party1_norm == db_resp_norm and party2_norm == db_pet_norm)
            
            if exact_forward or exact_reverse:
                seen_external_ids.add(external_id)
                citation_info = build_citation_info(metadata, external_id)
                matching_citations.append(citation_info)
                
                logger.info(f"   ✅ EXACT MATCH - {external_id}")
                logger.info(f"      Pet: '{db_petitioner}'")
                logger.info(f"      Resp: '{db_respondent}'")
                logger.info(f"      Citation: {citation_info['citation']}")
                continue
            
            # FUZZY MATCH
            # Check if one party matches exactly and other fuzzy
            score1_pet = fuzzy_match_score(party1_norm, db_pet_norm)
            score1_resp = fuzzy_match_score(party1_norm, db_resp_norm)
            score2_pet = fuzzy_match_score(party2_norm, db_pet_norm)
            score2_resp = fuzzy_match_score(party2_norm, db_resp_norm)
            
            # Forward: party1→petitioner, party2→respondent
            forward_score = min(score1_pet, score2_resp)
            # Reverse: party1→respondent, party2→petitioner
            reverse_score = min(score1_resp, score2_pet)
            
            best_score = max(forward_score, reverse_score)
            
            if best_score >= fuzzy_threshold:
                citation_info = build_citation_info(metadata, external_id)
                
                if external_id not in best_matches or best_score > best_matches[external_id][0]:
                    best_matches[external_id] = (best_score, citation_info)
                    
                    logger.info(f"   🎯 FUZZY MATCH - {external_id} (score: {best_score:.2f})")
                    logger.info(f"      Pet: '{db_petitioner}' (match: {max(score1_pet, score1_resp):.2f})")
                    logger.info(f"      Resp: '{db_respondent}' (match: {max(score2_pet, score2_resp):.2f})")
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
        
        logger.info(f"{'='*80}\n")
    
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
    Use LLM to correctly insert citations into the response where party names are mentioned
    """
    
    if not party_citations:
        return original_response
    
    # Build citation mapping for LLM
    citation_mapping = []
    for (party1, party2), citations in party_citations.items():
        for cit in citations:
            citation_mapping.append({
                "party1": party1,
                "party2": party2,
                "citation": cit['citation'],
                "case_number": cit['case_number']
            })
    
    prompt = f"""You are a legal citation editor. Your task is to add citations to a legal response WHERE the cases are mentioned.

ORIGINAL RESPONSE:
{original_response}

AVAILABLE CITATIONS:
{json.dumps(citation_mapping, indent=2)}

INSTRUCTIONS:
1. Find where each party pair is mentioned in the response
2. Add the citation immediately after the first mention: "Party1 vs Party2 (Citation)"
3. Keep ALL other text EXACTLY as written
4. Do NOT change wording, explanations, or analysis
5. Do NOT add citations at the end - only inline where cases are mentioned
6. If a case is mentioned multiple times, cite it only at first mention

Return the COMPLETE response with citations added inline.

RESPONSE:"""
    
    try:
        reattributed = call_bedrock_for_extraction(prompt, max_tokens=3000)
        
        # Clean markdown artifacts if any
        reattributed = reattributed.strip()
        if reattributed.startswith("```"):
            lines = reattributed.split("\n")
            reattributed = "\n".join(lines[1:-1]) if len(lines) > 2 else reattributed
        
        logger.info("\n" + "="*100)
        logger.info("✨ LLM RE-ATTRIBUTION COMPLETE")
        logger.info("="*100)
        
        return reattributed
        
    except Exception as e:
        logger.error(f"Re-attribution failed: {e}")
        # Fallback to appending citations at end
        return original_response + format_citation_section(party_citations)


def format_citation_section(party_citations: Dict[Tuple[str, str], List[Dict]]) -> str:
    """Format citations as appendix section (fallback)"""
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
    """
    ULTIMATE citation extraction and attribution with complete logging
    """
    
    print("\n" + "="*100)
    print(" "*35 + "🚀 CITATION ATTRIBUTION SYSTEM")
    print("="*100)
    
    # STEP 1: Print original response
    print("\n📝 STEP 1: ORIGINAL LLM RESPONSE")
    print("-"*100)
    print(llm_response)
    print("-"*100)
    
    # STEP 2: Extract party pairs
    print("\n🔍 STEP 2: EXTRACTING PARTY PAIRS")
    print("-"*100)
    
    party_pairs = extract_party_pairs_from_response(llm_response)
    
    print(f"\n✅ Extracted {len(party_pairs)} party pair(s):")
    for i, (p1, p2) in enumerate(party_pairs, 1):
        print(f"   {i}. '{p1}' <-> '{p2}'")
    
    if not party_pairs:
        print("\n⚠️  No party pairs found - returning original response")
        print("="*100 + "\n")
        return llm_response, {}
    
    # STEP 3: Find citations
    print("\n🔍 STEP 3: FINDING CITATIONS IN DATABASE")
    print("-"*100)
    
    party_citations = find_citations_for_party_pairs(party_pairs, all_chunks)
    
    print(f"\n📊 FOUND {len(party_citations)} matching party pair(s):")
    for (p1, p2), citations in party_citations.items():
        print(f"\n   '{p1}' vs '{p2}':")
        for cit in citations:
            print(f"      ✅ {cit['citation']}")
            print(f"         Petitioner: {cit['petitioner']}")
            print(f"         Respondent: {cit['respondent']}")
    
    if not party_citations:
        print("\n⚠️  No citations found - returning original response")
        print("="*100 + "\n")
        return llm_response, {}
    
    # STEP 4: Re-attribute with LLM
    print("\n✨ STEP 4: RE-ATTRIBUTING CITATIONS IN RESPONSE")
    print("-"*100)
    
    enhanced_response = reattribute_citations_in_response(llm_response, party_citations)
    
    print("\n📝 FINAL ENHANCED RESPONSE:")
    print("-"*100)
    print(enhanced_response)
    print("-"*100)
    
    # STEP 5: Summary
    print("\n📊 SUMMARY:")
    print(f"   Party pairs extracted: {len(party_pairs)}")
    print(f"   Citations found: {sum(len(cits) for cits in party_citations.values())}")
    print(f"   Original length: {len(llm_response)} chars")
    print(f"   Enhanced length: {len(enhanced_response)} chars")
    
    print("\n" + "="*100)
    print(" "*30 + "✅ CITATION ATTRIBUTION COMPLETE")
    print("="*100 + "\n")
    
    return enhanced_response, party_citations