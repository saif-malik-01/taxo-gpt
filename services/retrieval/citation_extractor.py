# import json
# import re
# import logging
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
    
#     client = boto3.client(
#         'bedrock-runtime',
#         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
#         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
#         config=config
#     )
    
#     return client


# def call_bedrock_direct(prompt: str) -> str:
#     """
#     Call AWS Bedrock with Qwen model and return the text response
    
#     Args:
#         prompt: The prompt to send
        
#     Returns:
#         str: The model's response text
#     """
#     try:
#         client = get_bedrock_client()
        
#         # Using Qwen model
#         model_id = "qwen.qwen3-next-80b-a3b"
        
#         # Prepare the request body for Qwen model
#         request_body = {
#             "max_tokens": 500,
#             "messages": [
#                 {
#                     "role": "user",
#                     "content": prompt
#                 }
#             ],
#             "temperature": 0.0  # Deterministic for extraction
#         }
        
#         # Call Bedrock
#         response = client.invoke_model(
#             modelId=model_id,
#             body=json.dumps(request_body)
#         )
        
#         # Parse response
#         response_body = json.loads(response['body'].read())
        
#         # Extract text from Qwen's response (OpenAI-compatible format)
#         content = response_body['choices'][0]['message']['content']
        
#         logger.debug(f"Bedrock Qwen response: {content}")
#         return content
        
#     except Exception as e:
#         logger.error(f"Bedrock API error: {e}")
#         raise


# def extract_citation_from_query(query: str) -> dict:
#     """
#     Extract citation and case number from query using AWS Bedrock LLM, regex fallback
    
#     Returns:
#     {
#         "citation": "2023 (12) TMI 456" or None,
#         "case_number": "Civil Appeal No. 1234/2023" or None,
#         "case_name": "ABC vs XYZ" or None
#     }
#     """
    
#     # Try LLM extraction with AWS Bedrock
#     try:
#         prompt = f"""
# Extract citation and case number from this legal query.
# Return ONLY valid JSON. No explanation.

# CASE NUMBER EXTRACTION RULES (STRICT):
# - A case number may include:
#   - Case type(s): WP, W.P., W.P.(C), WRIT TAX, Civil Appeal, CM APPL., IA, etc.
#   - Multiple connected case references joined by "&", ",", or ";"
#   - Number ranges (e.g., 7425-7430)
#   - Years written as "/YYYY" or "of YYYY"
#   - Multiple application numbers within the same reference
#   - Trailing procedural descriptors in parentheses such as:
#     "(Stay)", "(IA)", "(CM)", "(Interim)", "(Misc.)"
# - The case number MUST be extracted in FULL exactly as written.
# - Do NOT remove or truncate trailing parenthetical descriptors.

# If multiple connected case references together form a single judicial matter,
# extract them as ONE combined case_number string.

# Query:
# {query}

# Return format:
# {{
#   "citation": "full citation if found, else null",
#   "case_number": "complete case number if found, else null",
#   "case_name": "case name if found, else null"
# }}

# Examples:

# Query:
# "W.P.(C) 7425-7430/2017 & 7432/2017 & CM APPL. 30649-30654/2017 & 30657/2017 (Stay)"
# Output:
# {{
#   "citation": null,
#   "case_number": "W.P.(C) 7425-7430/2017 & 7432/2017 & CM APPL. 30649-30654/2017 & 30657/2017 (Stay)",
#   "case_name": null
# }}

# Now extract from the given query.
# Return ONLY JSON, no other text.
# """

   
#         # Call Bedrock directly with Qwen
#         content = call_bedrock_direct(prompt)
        
#         # Clean content - remove markdown code blocks if present
#         content = content.strip()
#         if content.startswith("```"):
#             lines = content.split("\n")
#             content = "\n".join(lines[1:-1]) if len(lines) > 2 else content
#             if content.startswith("json"):
#                 content = content[4:].strip()
#         content = content.strip()
        
#         logger.debug(f"Cleaned LLM response: {content}")
        
#         # Parse JSON
#         extracted = json.loads(content)
        
#         # Validate extraction
#         if extracted.get("citation") or extracted.get("case_number"):
#             logger.info(f"✅ LLM extracted - Citation: '{extracted.get('citation')}', "
#                        f"Case Number: '{extracted.get('case_number')}'")
#             return extracted
#         else:
#             logger.info("LLM returned valid JSON but no citation/case_number found")
        
#     except json.JSONDecodeError as e:
#         logger.warning(f"JSON parsing failed: {e}. Content: {content[:200] if 'content' in locals() else 'None'}")
#     except Exception as e:
#         logger.warning(f"LLM extraction failed: {e}, falling back to regex")
    
#     # Fallback to regex
#     logger.info("Falling back to regex extraction")
#     return regex_fallback_extraction(query)


# def regex_fallback_extraction(query: str) -> dict:
#     """
#     Regex-based extraction for common citation and case number patterns
#     """
    
#     result = {
#         "citation": None,
#         "case_number": None,
#         "case_name": None
#     }
    
#     # ===== CITATION PATTERNS =====
    
#     citation_patterns = [
#         # TMI patterns
#         r'\d{4}\s*\(\s*\d+\s*\)\s*TMI\s*\d+',  # 2023 (12) TMI 456
#         r'\d{4}\s*TMI\s*\d+',  # 2023 TMI 456
        
#         # Taxo.online pattern - CRITICAL
#         r'\d{4}\s+Taxo\.online\s+\d+',  # 2017 Taxo.online 42
        
#         # SCC patterns (Supreme Court Cases)
#         r'\d{4}\s*\(\s*\d+\s*\)\s*SCC\s+\d+',  # 2023 (12) SCC 456
#         r'\d{4}\s+SCC\s+\d+',  # 2023 SCC 456
        
#         # SCR patterns (Supreme Court Reports)
#         r'\d{4}\s+SCR\s+\d+',
        
#         # AIR patterns (All India Reporter)
#         r'AIR\s+\d{4}\s+\w+\s+\d+',  # AIR 2023 SC 456
        
#         # Generic year + reporter + number (catch-all)
#         r'\d{4}\s+[A-Za-z.]+\s+\d+',
#     ]
    
#     for pattern in citation_patterns:
#         citation_match = re.search(pattern, query, re.IGNORECASE)
#         if citation_match:
#             result["citation"] = citation_match.group().strip()
#             logger.info(f"✅ Regex extracted citation: '{result['citation']}'")
#             break
    
#     # ===== CASE NUMBER PATTERNS =====
    
#     case_patterns = [
#         # Civil Appeal patterns
#         r'Civil\s+Appeal\s+(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
#         r'C\.?A\.?\s+(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
        
#         # Criminal Appeal patterns
#         r'Criminal\s+Appeal\s+(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
#         r'Crl\.?\s*A\.?\s+(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
        
#         # Writ Petition patterns
#         r'Writ\s+Petition\s+(?:\(C\)\s+)?(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
#         r'W\.?P\.?\s+(?:\(C\)\s+)?(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
        
#         # Special Leave Petition patterns
#         r'Special\s+Leave\s+Petition\s+(?:\(Civil\)|\(Criminal\))?\s*(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
#         r'SLP\s+(?:\(C\)|\(Crl\))?\s*(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
        
#         # Appeal patterns (generic)
#         r'Appeal\s+(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
        
#         # Petition patterns (generic)
#         r'Petition\s+(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
#     ]
    
#     for pattern in case_patterns:
#         case_match = re.search(pattern, query, re.IGNORECASE)
#         if case_match:
#             result["case_number"] = case_match.group().strip()
#             logger.info(f"✅ Regex extracted case number: '{result['case_number']}'")
#             break
    
#     return result


import json
import re
import logging
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
    
    client = boto3.client(
        'bedrock-runtime',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        config=config
    )
    
    return client


def call_bedrock_direct(prompt: str) -> str:
    """Call AWS Bedrock with Qwen model"""
    try:
        client = get_bedrock_client()
        model_id = "qwen.qwen3-next-80b-a3b"
        
        request_body = {
            "max_tokens": 500,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0
        }
        
        response = client.invoke_model(
            modelId=model_id,
            body=json.dumps(request_body)
        )
        
        response_body = json.loads(response['body'].read())
        content = response_body['choices'][0]['message']['content']
        
        logger.debug(f"Bedrock response: {content}")
        return content
        
    except Exception as e:
        logger.error(f"Bedrock API error: {e}")
        raise


def extract_citation_from_query(query: str) -> dict:
    """
    Extract citation, case number, and party names from query using LLM
    
    Returns:
    {
        "citation": "2023 (12) TMI 456" or None,
        "case_number": "Civil Appeal No. 1234/2023" or None,
        "case_name": "ABC vs XYZ" or None,
        "party_names": ["Safari Retreat", "State of Karnataka"] or []
    }
    """
    
    try:
        prompt = f"""Extract legal information from this query. Return ONLY valid JSON, no explanation.

Query: {query}

Extract:
1. Citation (e.g., "2023 (12) TMI 456", "2017 Taxo.online 42")
2. Case number (e.g., "Civil Appeal No. 1234/2023", "WP(C). No. 34021 of 2017")
3. Case name (e.g., "ABC vs XYZ")
4. Party names - any company/person names mentioned (could be petitioner or respondent)

Return format:
{{
    "citation": "full citation if found, else null",
    "case_number": "case number if found, else null",
    "case_name": "case name if found, else null",
    "party_names": ["name1", "name2"] or []
}}

Examples:

Query: "What was held in 2023 (12) TMI 456?"
Output: {{"citation": "2023 (12) TMI 456", "case_number": null, "case_name": null, "party_names": []}}

Query: "Give me the judgment of Safari Retreat"
Output: {{"citation": null, "case_number": null, "case_name": null, "party_names": ["Safari Retreat"]}}

Query: "Show me Safari Retreat vs State of Karnataka case"
Output: {{"citation": null, "case_number": null, "case_name": "Safari Retreat vs State of Karnataka", "party_names": ["Safari Retreat", "State of Karnataka"]}}

Query: "What did the court say in WP(C). No. 34021 of 2017 regarding Mr. Sharma?"
Output: {{"citation": null, "case_number": "WP(C). No. 34021 of 2017", "case_name": null, "party_names": ["Sharma"]}}

Query: "Explain the 2017 Taxo.online 42 judgment"
Output: {{"citation": "2017 Taxo.online 42", "case_number": null, "case_name": null, "party_names": []}}

Now extract from the given query. Return ONLY JSON, no other text."""
        
        content = call_bedrock_direct(prompt)
        
        # Clean response
        content = content.strip()
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(lines[1:-1]) if len(lines) > 2 else content
            if content.startswith("json"):
                content = content[4:].strip()
        content = content.strip()
        
        logger.debug(f"Cleaned LLM response: {content}")
        
        # Parse JSON
        extracted = json.loads(content)
        
        # Ensure party_names is a list
        if not isinstance(extracted.get("party_names"), list):
            extracted["party_names"] = []
        
        logger.info(f"✅ Extracted - Citation: '{extracted.get('citation')}', "
                   f"Case Number: '{extracted.get('case_number')}', "
                   f"Party Names: {extracted.get('party_names')}")
        
        return extracted
        
    except Exception as e:
        logger.warning(f"LLM extraction failed: {e}, falling back to regex")
        return regex_fallback_extraction(query)


def regex_fallback_extraction(query: str) -> dict:
    """Regex-based extraction for citations, case numbers, and party names"""
    
    result = {
        "citation": None,
        "case_number": None,
        "case_name": None,
        "party_names": []
    }
    
    # Citation patterns
    citation_patterns = [
        r'\d{4}\s*\(\s*\d+\s*\)\s*TMI\s*\d+',
        r'\d{4}\s*TMI\s*\d+',
        r'\d{4}\s+Taxo\.online\s+\d+',
        r'\d{4}\s*\(\s*\d+\s*\)\s*SCC\s+\d+',
        r'\d{4}\s+SCC\s+\d+',
        r'\d{4}\s+SCR\s+\d+',
        r'AIR\s+\d{4}\s+\w+\s+\d+',
        r'\d{4}\s+[A-Za-z.]+\s+\d+',
    ]
    
    for pattern in citation_patterns:
        citation_match = re.search(pattern, query, re.IGNORECASE)
        if citation_match:
            result["citation"] = citation_match.group().strip()
            logger.info(f"✅ Regex extracted citation: '{result['citation']}'")
            break
    
    # Case number patterns
    case_patterns = [
        r'Civil\s+Appeal\s+(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
        r'C\.?A\.?\s+(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
        r'Criminal\s+Appeal\s+(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
        r'Writ\s+Petition\s+(?:\(C\)\s+)?(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
        r'W\.?P\.?\s+(?:\(C\)\s+)?(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
        r'SLP\s+(?:\(C\)|\(Crl\))?\s*(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
    ]
    
    for pattern in case_patterns:
        case_match = re.search(pattern, query, re.IGNORECASE)
        if case_match:
            result["case_number"] = case_match.group().strip()
            logger.info(f"✅ Regex extracted case number: '{result['case_number']}'")
            break
    
    # Party name extraction (simple heuristic)
    # Look for "judgment of X", "case of X", "X vs Y" patterns
    party_patterns = [
        r'judgment\s+of\s+([A-Z][A-Za-z\s&.]+?)(?:\s+vs|\s+v\.|\s+case|$)',
        r'case\s+of\s+([A-Z][A-Za-z\s&.]+?)(?:\s+vs|\s+v\.|\s+case|$)',
        r'([A-Z][A-Za-z\s&.]+?)\s+vs?\.\s+([A-Z][A-Za-z\s&.]+)',
    ]
    
    for pattern in party_patterns:
        party_match = re.search(pattern, query, re.IGNORECASE)
        if party_match:
            # Get all captured groups
            for group in party_match.groups():
                if group:
                    result["party_names"].append(group.strip())
            if result["party_names"]:
                logger.info(f"✅ Regex extracted party names: {result['party_names']}")
                break
    
    return result