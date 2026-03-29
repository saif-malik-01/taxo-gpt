"""
AWS Bedrock LLM client — supports Converse + InvokeModel fallback (Qwen-safe)
"""

import json
import time
import logging
from typing import Any, Dict, List, Optional, Iterator

import boto3
from botocore.exceptions import ClientError

from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)

MODEL_ID = "qwen.qwen3-next-80b-a3b"


class BedrockLLMClient:
    def __init__(self):
        self._client = boto3.client(
            "bedrock-runtime",
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
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
                # Try Converse first
                try:
                    return self._call_converse(
                        system_prompt, user_message, max_tokens, temperature
                    )
                except ClientError as e:
                    if e.response["Error"]["Code"] == "ValidationException":
                        logger.warning(f"[{label}] Converse not supported → fallback to invoke")
                        return self._call_invoke(user_message, max_tokens, temperature)
                    raise

            except Exception as e:
                logger.error(f"[{label}] error attempt {attempt}: {e}")
                if attempt == settings.PIPELINE_MAX_RETRIES:
                    return None
                time.sleep(settings.PIPELINE_RETRY_DELAY * attempt)

        return None

    def _call_converse(self, system_prompt, user_message, max_tokens, temperature):
        system_payload = [{"text": system_prompt}] if system_prompt else []

        resp = self._client.converse(
            modelId=MODEL_ID,
            messages=[
                {
                    "role": "user",
                    "content": [{"text": user_message}],
                }
            ],
            system=system_payload,
            inferenceConfig={
                "maxTokens": max_tokens,
                "temperature": temperature,
            },
        )

        return resp["output"]["message"]["content"][0]["text"]

    def _call_invoke(self, user_message, max_tokens, temperature):
        body = {
            "prompt": user_message,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }

        resp = self._client.invoke_model(
            modelId=MODEL_ID,
            body=json.dumps(body),
        )

        result = json.loads(resp["body"].read())
        return result.get("outputText") or result.get("generation")

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
                try:
                    yield from self._stream_converse(
                        system_prompt, user_message, max_tokens, temperature
                    )
                    return
                except ClientError as e:
                    if e.response["Error"]["Code"] == "ValidationException":
                        logger.warning(f"[{label}] Converse stream unsupported → fallback")
                        yield from self._stream_invoke(user_message, max_tokens, temperature)
                        return
                    raise

            except Exception as e:
                logger.error(f"[{label}] stream error attempt {attempt}: {e}")
                if attempt == settings.PIPELINE_MAX_RETRIES:
                    return
                time.sleep(settings.PIPELINE_RETRY_DELAY * attempt)

    def _stream_converse(self, system_prompt, user_message, max_tokens, temperature):
        system_payload = [{"text": system_prompt}] if system_prompt else []

        resp = self._client.converse_stream(
            modelId=MODEL_ID,
            messages=[
                {"role": "user", "content": [{"text": user_message}]}
            ],
            system=system_payload,
            inferenceConfig={
                "maxTokens": max_tokens,
                "temperature": temperature,
            },
        )

        usage: dict = {}
        for event in resp["stream"]:
            if "contentBlockDelta" in event:
                delta = event["contentBlockDelta"]["delta"]
                text = delta.get("text")
                if text:
                    yield text
            elif "metadata" in event:
                # AWS sends token usage in the final metadata event
                usage = event["metadata"].get("usage", {})

        # Yield usage as a special sentinel so callers can capture real token counts
        if usage:
            yield f"\n\n__USAGE__{json.dumps(usage)}"

    def _stream_invoke(self, user_message, max_tokens, temperature):
        body = {
            "prompt": user_message,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }

        resp = self._client.invoke_model_with_response_stream(
            modelId=MODEL_ID,
            body=json.dumps(body),
        )

        for event in resp["body"]:
            chunk = event.get("chunk")
            if not chunk:
                continue

            data = json.loads(chunk["bytes"].decode())

            # Handle multiple formats
            text = (
                data.get("outputText")
                or data.get("generation")
                or data.get("text")
            )

            if text:
                yield text

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