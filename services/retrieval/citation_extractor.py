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
    
    return boto3.client(
        'bedrock-runtime',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        config=config
    )


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
        return response_body['choices'][0]['message']['content']
        
    except Exception as e:
        logger.error(f"Bedrock API error: {e}")
        raise


def extract_citation_from_query(query: str) -> dict:
    """
    Extract citation, case numbers, and party names from query
    
    Returns:
    {
        "citation": "2023 (12) TMI 456" or None,
        "case_numbers": ["W.P.No. 30233 of 2017", "W.P.M.P.No. 32925 of 2017", ...],
        "full_case_number": "W.P.No. 30233 of 2017, W.P.M.P.Nos.32925 & 32926 of 2017",
        "case_name": "ABC vs XYZ" or None,
        "party_names": ["Safari Retreat", "State of Karnataka"]
    }
    """
    
    try:
        prompt = f"""Extract legal information from this query. Return ONLY valid JSON.

Query: {query}

Extract:
1. Citation (formats: "YYYY (N) TMI N", "YYYY Taxo.online N", "YYYY Taxo N")
2. Case numbers - ALL case numbers including combined ones
3. Party names

Return format:
{{
    "citation": "full citation if found, else null",
    "case_numbers": ["case number 1", "case number 2", ...] or [],
    "full_case_number": "complete case number string if multiple",
    "case_name": "case name if found, else null",
    "party_names": ["name1", "name2"] or []
}}

Examples:

Query: "2023 Taxo 42"
Output: {{"citation": "2023 Taxo.online 42", "case_numbers": [], "full_case_number": null, "case_name": null, "party_names": []}}

Query: "W.P.No. 30233 of 2017, W.P.M.P.Nos.32925 & 32926 of 2017"
Output: {{"citation": null, "case_numbers": ["W.P.No. 30233 of 2017", "W.P.M.P.No. 32925 of 2017", "W.P.M.P.No. 32926 of 2017"], "full_case_number": "W.P.No. 30233 of 2017, W.P.M.P.Nos.32925 & 32926 of 2017", "case_name": null, "party_names": []}}

Query: "Safari Retreat vs State of Karnataka"
Output: {{"citation": null, "case_numbers": [], "full_case_number": null, "case_name": "Safari Retreat vs State of Karnataka", "party_names": ["Safari Retreat", "State of Karnataka"]}}

Now extract from: {query}
Return ONLY JSON."""
        
        content = call_bedrock(prompt)
        
        # Clean response
        content = content.strip()
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(lines[1:-1]) if len(lines) > 2 else content
            if content.startswith("json"):
                content = content[4:].strip()
        
        extracted = json.loads(content.strip())
        
        # Ensure lists
        if not isinstance(extracted.get("party_names"), list):
            extracted["party_names"] = []
        if not isinstance(extracted.get("case_numbers"), list):
            extracted["case_numbers"] = []
        
        # Normalize citation format (handle "YYYY Taxo N" → "YYYY Taxo.online N")
        citation = extracted.get("citation")
        if citation:
            citation = normalize_citation_format(citation)
            extracted["citation"] = citation
        
        logger.info(f"✅ Extracted - Citation: '{extracted.get('citation')}', "
                   f"Case Numbers: {extracted.get('case_numbers')}, "
                   f"Parties: {extracted.get('party_names')}")
        
        return extracted
        
    except Exception as e:
        logger.warning(f"LLM extraction failed: {e}, falling back to regex")
        return regex_fallback_extraction(query)


def normalize_citation_format(citation: str) -> str:
    """
    Normalize citation format
    "2023 Taxo 42" → "2023 Taxo.online 42"
    """
    if not citation:
        return citation
    
    # Handle "YYYY Taxo N" → "YYYY Taxo.online N"
    taxo_pattern = r'(\d{4})\s+Taxo\s+(\d+)'
    match = re.search(taxo_pattern, citation, re.IGNORECASE)
    if match:
        year = match.group(1)
        number = match.group(2)
        return f"{year} Taxo.online {number}"
    
    return citation


def extract_multiple_case_numbers(text: str) -> list:
    """
    Extract all individual case numbers from combined case number strings
    
    Examples:
    "W.P.No. 30233 of 2017, W.P.M.P.Nos.32925 & 32926 of 2017"
    → ["W.P.No. 30233 of 2017", "W.P.M.P.No. 32925 of 2017", "W.P.M.P.No. 32926 of 2017"]
    """
    case_numbers = []
    
    # Pattern 1: Individual case numbers
    patterns = [
        r'W\.P\.(?:M\.P\.)?No\.?\s*\d+\s+of\s+\d{4}',
        r'W\.P\.(?:M\.P\.)?Nos\.?\s*\d+(?:\s*[&,]\s*\d+)*\s+of\s+\d{4}',
        r'WP\(C\)\.?\s*No\.?\s*\d+\s+of\s+\d{4}',
        r'Civil\s+Appeal\s+No\.?\s*\d+\s+of\s+\d{4}',
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            case_num = match.group(0)
            
            # If contains "Nos." or "&", expand it
            if 'Nos' in case_num or '&' in case_num:
                expanded = expand_case_numbers(case_num)
                case_numbers.extend(expanded)
            else:
                case_numbers.append(case_num)
    
    return list(set(case_numbers))  # Remove duplicates


def expand_case_numbers(case_num_str: str) -> list:
    """
    Expand combined case numbers
    "W.P.M.P.Nos.32925 & 32926 of 2017" 
    → ["W.P.M.P.No. 32925 of 2017", "W.P.M.P.No. 32926 of 2017"]
    """
    # Extract base pattern and numbers
    match = re.search(r'(W\.P\.(?:M\.P\.)?Nos?\.?)\s*([\d,&\s]+)\s+of\s+(\d{4})', 
                      case_num_str, re.IGNORECASE)
    
    if not match:
        return [case_num_str]
    
    base = match.group(1).replace('Nos', 'No')  # Convert to singular
    numbers_str = match.group(2)
    year = match.group(3)
    
    # Extract all numbers
    numbers = re.findall(r'\d+', numbers_str)
    
    # Create individual case numbers
    result = []
    for num in numbers:
        result.append(f"{base} {num} of {year}")
    
    return result


def regex_fallback_extraction(query: str) -> dict:
    """Regex-based extraction fallback"""
    
    result = {
        "citation": None,
        "case_numbers": [],
        "full_case_number": None,
        "case_name": None,
        "party_names": []
    }
    
    # Citation patterns (including "YYYY Taxo N")
    citation_patterns = [
        r'\d{4}\s*\(\s*\d+\s*\)\s*TMI\s*\d+',
        r'\d{4}\s*TMI\s*\d+',
        r'\d{4}\s+Taxo\.online\s+\d+',
        r'\d{4}\s+Taxo\s+\d+',  # Handle "YYYY Taxo N"
        r'\d{4}\s*\(\s*\d+\s*\)\s*SCC\s+\d+',
        r'\d{4}\s+SCC\s+\d+',
    ]
    
    for pattern in citation_patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            citation = match.group().strip()
            # Normalize format
            citation = normalize_citation_format(citation)
            result["citation"] = citation
            logger.info(f"✅ Regex extracted citation: '{result['citation']}'")
            break
    
    # Case numbers (handle combined)
    case_numbers = extract_multiple_case_numbers(query)
    if case_numbers:
        result["case_numbers"] = case_numbers
        result["full_case_number"] = query  # Store original for reference
        logger.info(f"✅ Regex extracted case numbers: {case_numbers}")
    
    return result