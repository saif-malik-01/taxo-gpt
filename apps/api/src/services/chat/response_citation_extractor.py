import json
import re
import logging
from typing import List, Dict, Tuple, Optional
import os
import boto3
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed

from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)

def get_bedrock_client():
    config = Config(region_name=settings.AWS_REGION, signature_version='v4', retries={'max_attempts': 3, 'mode': 'standard'})
    return boto3.client('bedrock-runtime', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY, config=config)

def call_bedrock_for_extraction(prompt: str, max_tokens: int = 1500) -> str:
    try:
        client = get_bedrock_client()
        request_body = {"max_tokens": max_tokens, "messages": [{"role": "user", "content": prompt}], "temperature": 0.0}
        response = client.invoke_model(modelId="qwen.qwen3-next-80b-a3b", body=json.dumps(request_body))
        return json.loads(response['body'].read())['choices'][0]['message']['content']
    except Exception as e:
        logger.error(f"Bedrock API error: {e}"); raise

def extract_party_pairs_from_response(llm_response: str) -> List[Tuple[str, str]]:
    prompt = f"Extract party pairs (A vs B) from this text and return as JSON: {llm_response}"
    try:
        content = call_bedrock_for_extraction(prompt)
        extracted = json.loads(content.strip().strip("```json").strip("```"))
        return [(p[0], p[1]) for p in extracted.get("pairs", []) if len(p) == 2]
    except: return []

def extract_and_attribute_citations(llm_response: str, all_chunks: list):
    """
    Main entry point for citation attribution.
    Returns: (Generator of enhanced text chunks, Dict of party citations)
    """
    party_pairs = extract_party_pairs_from_response(llm_response)
    if not party_pairs:
        def stream_orig():
            for i in range(0, len(llm_response), 50): yield llm_response[i:i+50]
        return stream_orig(), {}

    # Simplified mock for matching (logic is complex, but this is the interface)
    party_citations = {} # (p1, p2) -> [cit1, cit2]
    
    def stream_enhanced():
        # Ideally calls another LLM to insert citations, here we just stream orig if mock
        for i in range(0, len(llm_response), 50): yield llm_response[i:i+50]

    return stream_enhanced(), party_citations
