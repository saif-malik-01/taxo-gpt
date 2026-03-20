"""
apps/api/src/services/llm/bedrock.py
AWS Bedrock LLM client — Qwen model via Converse API. (Refactored for RAG integration)
"""

import json
import time
import logging
from typing import Any, Dict, List, Optional, Iterator

import boto3
from botocore.exceptions import ClientError
from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)

# Legacy support for old imports if any remain
MODEL_ID = "qwen.qwen3-next-80b-a3b"

class BedrockLLMClient:
    """
    Standardised Bedrock client for both JSON extraction and Streaming generation.
    """

    def __init__(self):
        self._client = boto3.client(
            "bedrock-runtime",
            region_name=settings.AWS_REGION,
        )
        logger.info(f"BedrockLLMClient ready — model: {MODEL_ID}")

    def call(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = 4096,
        temperature: float = 0.1,
        label: str = "llm",
    ) -> Optional[str]:
        for attempt in range(1, settings.PIPELINE_MAX_RETRIES + 1):
            try:
                resp = self._client.converse(
                    modelId=MODEL_ID,
                    messages=[{"role": "user", "content": [{"text": user_message}]}],
                    system=[{"text": system_prompt}],
                    inferenceConfig={"maxTokens": max_tokens, "temperature": temperature},
                )
                text = resp["output"]["message"]["content"][0]["text"]
                logger.debug(f"[{label}] response {len(text)} chars")
                return text
            except ClientError as e:
                code = e.response["Error"]["Code"]
                logger.error(f"[{label}] ClientError [{code}] attempt {attempt}")
                if code in ("ThrottlingException", "ServiceUnavailableException"):
                    time.sleep(settings.PIPELINE_RETRY_DELAY * attempt * 2)
                elif code == "ValidationException":
                    return None
                elif attempt == settings.PIPELINE_MAX_RETRIES:
                    return None
                else:
                    time.sleep(settings.PIPELINE_RETRY_DELAY * attempt)
            except Exception as e:
                logger.error(f"[{label}] error attempt {attempt}: {e}")
                if attempt == settings.PIPELINE_MAX_RETRIES:
                    return None
                time.sleep(settings.PIPELINE_RETRY_DELAY * attempt)
        return None

    def call_stream(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = 4096,
        temperature: float = 0.1,
        label: str = "llm_stream",
    ) -> Iterator[str]:
        for attempt in range(1, settings.PIPELINE_MAX_RETRIES + 1):
            try:
                resp = self._client.converse_stream(
                    modelId=MODEL_ID,
                    messages=[{"role": "user", "content": [{"text": user_message}]}],
                    system=[{"text": system_prompt}],
                    inferenceConfig={"maxTokens": max_tokens, "temperature": temperature},
                )
                stream = resp.get("stream")
                if not stream:
                    logger.error(f"[{label}] No stream in response")
                    return
                for event in stream:
                    delta = event.get("contentBlockDelta", {}).get("delta", {})
                    text  = delta.get("text")
                    if text:
                        yield text
                return
            except Exception as e:
                logger.error(f"[{label}] stream error attempt {attempt}: {e}")
                if attempt == settings.PIPELINE_MAX_RETRIES:
                    return
                time.sleep(settings.PIPELINE_RETRY_DELAY * attempt)

    def call_json(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = 1024,
        label: str = "llm_json",
    ) -> Optional[Dict[str, Any]]:
        raw = self.call(system_prompt, user_message, max_tokens, label=label)
        if not raw:
            return None
        try:
            text = raw.strip()
            if text.startswith("```"):
                lines = text.split("\n")
                lines = lines[1:] if lines[0].startswith("```") else lines
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                text = "\n".join(lines).strip()
            return json.loads(text)
        except json.JSONDecodeError as e:
            logger.error(f"[{label}] JSON parse error: {e}\nRaw: {raw[:300]}")
            return None

# Global helper functions for legacy support
def call_bedrock(prompt: str, system_prompts: Optional[List[str]] = None, temperature: float = 0.0) -> tuple:
    client = BedrockLLMClient()
    sys_p = "\n".join(system_prompts) if system_prompts else ""
    text = client.call(sys_p, prompt, temperature=temperature)
    return text or "NONE", {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}

def call_bedrock_stream(prompt: str, system_prompts: Optional[List[str]] = None, temperature: float = 0.0) -> Iterator[dict]:
    client = BedrockLLMClient()
    sys_p = "\n".join(system_prompts) if system_prompts else ""
    for chunk in client.call_stream(sys_p, prompt, temperature=temperature):
        yield {"type": "content", "text": chunk}
