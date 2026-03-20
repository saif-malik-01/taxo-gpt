"""
apps/api/src/services/rag/retrieval/bedrock_llm.py
AWS Bedrock LLM client — Qwen model via Converse API.
"""

import json
import time
import logging
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

from apps.api.src.services.rag.config import CONFIG

logger = logging.getLogger(__name__)

MODEL_ID      = "qwen.qwen3-next-80b-a3b"
_MAX_RETRIES  = CONFIG.pipeline.max_retries
_RETRY_DELAY  = CONFIG.pipeline.retry_delay_seconds
_MAX_TOKENS   = 4096
_TEMPERATURE  = 0.1


class BedrockLLMClient:

    def __init__(self):
        self._client = boto3.client(
            "bedrock-runtime",
            region_name=CONFIG.bedrock.region_name,
        )
        logger.info(f"BedrockLLMClient ready — model: {MODEL_ID}")

    def call(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = _MAX_TOKENS,
        temperature: float = _TEMPERATURE,
        label: str = "llm",
    ) -> Optional[str]:
        for attempt in range(1, _MAX_RETRIES + 1):
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
                    time.sleep(_RETRY_DELAY * attempt * 2)
                elif code == "ValidationException":
                    return None
                elif attempt == _MAX_RETRIES:
                    return None
                else:
                    time.sleep(_RETRY_DELAY * attempt)
            except Exception as e:
                logger.error(f"[{label}] error attempt {attempt}: {e}")
                if attempt == _MAX_RETRIES:
                    return None
                time.sleep(_RETRY_DELAY * attempt)
        return None

    def call_stream(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = _MAX_TOKENS,
        temperature: float = _TEMPERATURE,
        label: str = "llm_stream",
    ):
        """
        Stream the LLM response token by token.
        Yields str chunks as they arrive from Bedrock.
        Uses converse_stream API — same model and parameters as call().
        """
        for attempt in range(1, _MAX_RETRIES + 1):
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
                return   # success
            except ClientError as e:
                code = e.response["Error"]["Code"]
                logger.error(f"[{label}] ClientError [{code}] attempt {attempt}")
                if code in ("ThrottlingException", "ServiceUnavailableException"):
                    time.sleep(_RETRY_DELAY * attempt * 2)
                elif attempt == _MAX_RETRIES:
                    return
                else:
                    time.sleep(_RETRY_DELAY * attempt)
            except Exception as e:
                logger.error(f"[{label}] stream error attempt {attempt}: {e}")
                if attempt == _MAX_RETRIES:
                    return
                time.sleep(_RETRY_DELAY * attempt)

    def call_json(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = _MAX_TOKENS,
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
