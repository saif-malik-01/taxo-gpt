"""
retrieval/bedrock_llm.py
AWS Bedrock LLM client — Qwen model via Converse API.

Root-cause fix for 5-9 minute hangs:
  boto3 by default uses botocore's retry handler with max_attempts=3 and
  read_timeout=60s.  Our own retry loop (_MAX_RETRIES=3) sits on top of that.
  When Bedrock times out, botocore retries 3 times internally (3×60s=180s)
  before raising to our loop, which then retries that 3 more times:
      3 (ours) × 3 (botocore) × 60s = 540 seconds = 9 minutes per LLM call.

  Fix: pass botocore.config.Config with:
    read_timeout   = explicit per-call timeout appropriate for the token budget
    max_attempts=1 = disable botocore internal retries entirely
                     (our retry loop in call() already handles retries with
                      correct back-off and error classification)
"""

import json
import time
from typing import Any, Dict, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from config import CONFIG
from utils.logger import get_logger

logger = get_logger("bedrock_llm")

MODEL_ID      = "qwen.qwen3-next-80b-a3b"
_MAX_RETRIES  = 3
_RETRY_DELAY  = 2.0
_MAX_TOKENS   = 8000
_TEMPERATURE  = 0.1

# ── Boto3 client config ────────────────────────────────────────────────────────
# read_timeout: how long to wait for a response from Bedrock before failing.
#   Query LLM calls (≤4096 tokens): 90s is generous.  Qwen typically responds
#   in 15-40s for a full 4096-token answer.  90s gives headroom without the
#   9-minute hang caused by botocore retrying a 60s timeout 3 times.
#
# max_attempts=1: disable botocore's own retry loop entirely.
#   Our call() / call_stream() already retry with classification-aware back-off
#   (throttle → longer sleep, validation → no retry).  Letting botocore also
#   retry multiplies the wait time:
#     our_retries(3) × botocore_retries(3) × timeout(60s) = 540s per call.
#   With max_attempts=1 botocore raises immediately on any error, and our loop
#   decides whether and how long to retry.

_BEDROCK_CONFIG = Config(
    read_timeout=180,
    retries={"max_attempts": 1, "mode": "standard"},
)


class BedrockLLMClient:

    def __init__(self):
        self._client = boto3.client(
            "bedrock-runtime",
            region_name=CONFIG.bedrock.region_name,
            config=_BEDROCK_CONFIG,
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