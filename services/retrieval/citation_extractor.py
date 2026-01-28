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

Query: "What did the court say in WP(C). No. 34021 of 2017?"
Output: {{"citation": null, "case_number": "WP(C). No. 34021 of 2017", "case_name": null, "party_names": []}}

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
        
        logger.info(f"✅ LLM Extracted - Citation: '{extracted.get('citation')}', "
                   f"Case Number: '{extracted.get('case_number')}', "
                   f"Party Names: {extracted.get('party_names')}")
        
        return extracted
        
    except Exception as e:
        logger.warning(f"LLM extraction failed: {e}, falling back to regex")
        return regex_fallback_extraction(query)


def regex_fallback_extraction(query: str) -> dict:
    """
    Regex-based extraction for common citation and case number patterns
    """
    
    result = {
        "citation": None,
        "case_number": None,
        "case_name": None,
        "party_names": []
    }
    
    # ===== CITATION PATTERNS =====
    
    citation_patterns = [
        r'\d{4}\s*\(\s*\d+\s*\)\s*TMI\s*\d+',  # 2023 (12) TMI 456
        r'\d{4}\s*TMI\s*\d+',  # 2023 TMI 456
        r'\d{4}\s+Taxo\.online\s+\d+',  # 2017 Taxo.online 42
        r'\d{4}\s*\(\s*\d+\s*\)\s*SCC\s+\d+',  # 2023 (12) SCC 456
        r'\d{4}\s+SCC\s+\d+',  # 2023 SCC 456
        r'\d{4}\s+SCR\s+\d+',
        r'AIR\s+\d{4}\s+\w+\s+\d+',  # AIR 2023 SC 456
    ]
    
    for pattern in citation_patterns:
        citation_match = re.search(pattern, query, re.IGNORECASE)
        if citation_match:
            result["citation"] = citation_match.group().strip()
            logger.info(f"✅ Regex extracted citation: '{result['citation']}'")
            break
    
    # ===== CASE NUMBER PATTERNS =====
    
    case_patterns = [
        r'WP\(C\)\.\s*No\.\s*\d+\s+of\s+\d{4}(?:\s*\([A-Z]\))?',  # WP(C). No. 34021 of 2017 (C)
        r'WRIT\s+TAX\s+No\.\s*-?\s*\d+\s+of\s+\d{4}',  # WRIT TAX No. 747 of 2017
        r'W\.?P\.?\s*(?:\(C\))?\s*No\.?\s*\d+(?:\s*,\s*\d+)*(?:\s*&\s*\d+)?\s+of\s+\d{4}',  # W.P No.24853, 24852 & 24842
        r'Civil\s+Appeal\s+(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
        r'Criminal\s+Appeal\s+(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
        r'Writ\s+Petition\s+(?:\(C\)\s+)?(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
        r'SLP\s+(?:\(C\)|\(Crl\))?\s*(?:No\.?|Number)?\s*\d+\s*(?:of|/)?\s*\d{4}',
    ]
    
    for pattern in case_patterns:
        case_match = re.search(pattern, query, re.IGNORECASE)
        if case_match:
            result["case_number"] = case_match.group().strip()
            logger.info(f"✅ Regex extracted case number: '{result['case_number']}'")
            break
    
    return result