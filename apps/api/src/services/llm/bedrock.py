"""
AWS Bedrock LLM client
  - AsyncBedrockLLMClient  (aioboto3) — used by RAG pipeline, extractor, responder
  - BedrockLLMClient       (boto3)    — used by doc_classifier, intent_classifier, issue_replier
"""

import re
import json
import time
import asyncio
import logging
from typing import Any, AsyncIterator, Dict, Iterator, List, Optional

import boto3
import aioboto3
from botocore.exceptions import ClientError
from botocore.config import Config

from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)

MODEL_ID     = "qwen.qwen3-next-80b-a3b"
_MAX_TOKENS  = 8000
_TEMPERATURE = 0.1


# ── Shared helper ─────────────────────────────────────────────────────────────

def _strip_thinking(text: str) -> str:
    """
    Strip Qwen3 <think>...</think> reasoning block before JSON parsing.
    Under load, this block appears before the JSON and causes:
      - 'Extra data'        → thinking text + JSON = two documents
      - 'Unterminated string' → thinking tokens exhaust the token budget
    """
    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()


def _parse_json_response(raw: str, label: str) -> Optional[Dict[str, Any]]:
    """Shared JSON parsing logic (strip thinking → strip fences → loads)."""
    try:
        text = _strip_thinking(raw.strip())
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


# ═══════════════════════════════════════════════════════════════════════════════
# ASYNC CLIENT  (aioboto3)  — hot path: RAG pipeline / extractor / responder
# ═══════════════════════════════════════════════════════════════════════════════

class AsyncBedrockLLMClient:
    """
    Async Bedrock client using aioboto3.
    One singleton per application — created at startup via setup(), closed at shutdown.
    All methods are async and release the event loop during network I/O.
    """

    def __init__(self):
        self._session = aioboto3.Session(
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION,
        )
        self._client = None
        logger.info(f"AsyncBedrockLLMClient initialised — model: {MODEL_ID}")

    async def setup(self):
        """Call once at app startup to open the async boto3 client."""
        self._client = await self._session.client(
            "bedrock-runtime",
            config=Config(
                read_timeout=180,
                connect_timeout=10,
                retries={"max_attempts": settings.PIPELINE_MAX_RETRIES},
            ),
        ).__aenter__()
        logger.info("AsyncBedrockLLMClient connection pool ready")

    async def close(self):
        """Call once at app shutdown to release the connection pool."""
        if self._client:
            try:
                await self._client.__aexit__(None, None, None)
            except Exception as e:
                logger.warning(f"AsyncBedrockLLMClient close error: {e}")

    # ── Non-streaming call ────────────────────────────────────────────────────

    async def call(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = _MAX_TOKENS,
        temperature: float = _TEMPERATURE,
        label: str = "llm",
    ) -> Optional[str]:
        for attempt in range(1, settings.PIPELINE_MAX_RETRIES + 1):
            try:
                return await self._call_converse(
                    system_prompt, user_message, max_tokens, temperature
                )
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code == "ValidationException":
                    logger.warning(f"[{label}] Converse unsupported → no async fallback for invoke")
                    return None
                if code in ("ThrottlingException", "ServiceUnavailableException"):
                    logger.warning(f"[{label}] Throttled attempt {attempt}")
                else:
                    logger.error(f"[{label}] ClientError attempt {attempt}: {e}")
            except Exception as e:
                logger.error(f"[{label}] error attempt {attempt}: {e}")

            if attempt < settings.PIPELINE_MAX_RETRIES:
                await asyncio.sleep(settings.PIPELINE_RETRY_DELAY * attempt)

        return None

    async def _call_converse(self, system_prompt, user_message, max_tokens, temperature) -> str:
        system_payload = [{"text": system_prompt}] if system_prompt else []
        resp = await self._client.converse(
            modelId=MODEL_ID,
            messages=[{"role": "user", "content": [{"text": user_message}]}],
            system=system_payload,
            inferenceConfig={"maxTokens": max_tokens, "temperature": temperature},
        )
        return resp["output"]["message"]["content"][0]["text"]

    # ── Streaming call ────────────────────────────────────────────────────────

    async def call_stream(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = _MAX_TOKENS,
        temperature: float = _TEMPERATURE,
        label: str = "llm_stream",
    ) -> AsyncIterator[str]:
        """
        Async generator — yields text chunks directly from Bedrock.
        No thread bridge. No queue. No stop_event.
        Usage sentinel __USAGE__{json} is yielded last.
        """
        for attempt in range(1, settings.PIPELINE_MAX_RETRIES + 1):
            try:
                async for chunk in self._stream_converse(
                    system_prompt, user_message, max_tokens, temperature
                ):
                    yield chunk
                return
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code in ("ThrottlingException", "ServiceUnavailableException"):
                    logger.warning(f"[{label}] stream throttled attempt {attempt}")
                else:
                    logger.error(f"[{label}] stream ClientError attempt {attempt}: {e}")
            except Exception as e:
                logger.error(f"[{label}] stream error attempt {attempt}: {e}")

            if attempt < settings.PIPELINE_MAX_RETRIES:
                await asyncio.sleep(settings.PIPELINE_RETRY_DELAY * attempt)

    async def _stream_converse(
        self, system_prompt, user_message, max_tokens, temperature
    ) -> AsyncIterator[str]:
        system_payload = [{"text": system_prompt}] if system_prompt else []
        resp = await self._client.converse_stream(
            modelId=MODEL_ID,
            messages=[{"role": "user", "content": [{"text": user_message}]}],
            system=system_payload,
            inferenceConfig={"maxTokens": max_tokens, "temperature": temperature},
        )
        usage: dict = {}
        async for event in resp["stream"]:
            if "contentBlockDelta" in event:
                text = event["contentBlockDelta"]["delta"].get("text")
                if text:
                    yield text
            elif "metadata" in event:
                usage = event["metadata"].get("usage", {})

        if usage:
            yield f"\n\n__USAGE__{json.dumps(usage)}"

    # ── JSON call ─────────────────────────────────────────────────────────────

    async def call_json(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = _MAX_TOKENS,
        label: str = "llm_json",
    ) -> Optional[Dict[str, Any]]:
        raw = await self.call(system_prompt, user_message, max_tokens, label=label)
        if not raw:
            return None
        return _parse_json_response(raw, label)


# ── Singleton management ──────────────────────────────────────────────────────

_async_client: Optional[AsyncBedrockLLMClient] = None


async def get_async_bedrock_client() -> AsyncBedrockLLMClient:
    """Return the app-wide async Bedrock client singleton."""
    global _async_client
    if _async_client is None:
        _async_client = AsyncBedrockLLMClient()
        await _async_client.setup()
    return _async_client


async def close_async_bedrock_client() -> None:
    """Close the async client at app shutdown."""
    global _async_client
    if _async_client:
        await _async_client.close()
        _async_client = None


# ═══════════════════════════════════════════════════════════════════════════════
# SYNC CLIENT  (boto3)  — document ingestion services (doc_classifier, etc.)
# ═══════════════════════════════════════════════════════════════════════════════

class BedrockLLMClient:
    """
    Synchronous Bedrock client — kept for doc_classifier, intent_classifier,
    issue_replier which run in background threads.  Do NOT use on the hot path.
    """

    def __init__(self):
        self._client = boto3.client(
            "bedrock-runtime",
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            config=Config(
                read_timeout=180,
                max_pool_connections=20,
                retries={"max_attempts": settings.PIPELINE_MAX_RETRIES},
            ),
        )
        logger.info(f"BedrockLLMClient (sync) ready — model: {MODEL_ID}")

    def call(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = _MAX_TOKENS,
        temperature: float = _TEMPERATURE,
        label: str = "llm",
    ) -> Optional[str]:
        for attempt in range(1, settings.PIPELINE_MAX_RETRIES + 1):
            try:
                try:
                    return self._call_converse(system_prompt, user_message, max_tokens, temperature)
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
            messages=[{"role": "user", "content": [{"text": user_message}]}],
            system=system_payload,
            inferenceConfig={"maxTokens": max_tokens, "temperature": temperature},
        )
        return resp["output"]["message"]["content"][0]["text"]

    def _call_invoke(self, user_message, max_tokens, temperature):
        body = {"prompt": user_message, "max_tokens": max_tokens, "temperature": temperature}
        resp = self._client.invoke_model(modelId=MODEL_ID, body=json.dumps(body))
        result = json.loads(resp["body"].read())
        return result.get("outputText") or result.get("generation")

    def call_stream(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = _MAX_TOKENS,
        temperature: float = _TEMPERATURE,
        label: str = "llm_stream",
    ) -> Iterator[str]:
        for attempt in range(1, settings.PIPELINE_MAX_RETRIES + 1):
            try:
                try:
                    yield from self._stream_converse(system_prompt, user_message, max_tokens, temperature)
                    return
                except ClientError as e:
                    if e.response["Error"]["Code"] == "ValidationException":
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
            messages=[{"role": "user", "content": [{"text": user_message}]}],
            system=system_payload,
            inferenceConfig={"maxTokens": max_tokens, "temperature": temperature},
        )
        usage: dict = {}
        for event in resp["stream"]:
            if "contentBlockDelta" in event:
                text = event["contentBlockDelta"]["delta"].get("text")
                if text:
                    yield text
            elif "metadata" in event:
                usage = event["metadata"].get("usage", {})
        if usage:
            yield f"\n\n__USAGE__{json.dumps(usage)}"

    def _stream_invoke(self, user_message, max_tokens, temperature):
        body = {"prompt": user_message, "max_tokens": max_tokens, "temperature": temperature}
        resp = self._client.invoke_model_with_response_stream(modelId=MODEL_ID, body=json.dumps(body))
        for event in resp["body"]:
            chunk = event.get("chunk")
            if not chunk:
                continue
            data = json.loads(chunk["bytes"].decode())
            text = data.get("outputText") or data.get("generation") or data.get("text")
            if text:
                yield text

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
        return _parse_json_response(raw, label)


# ── Legacy helpers (memory_updater uses call_bedrock) ─────────────────────────

_sync_singleton: Optional[BedrockLLMClient] = None
_sync_lock = __import__("threading").Lock()


def _get_sync_client() -> BedrockLLMClient:
    global _sync_singleton
    if _sync_singleton is None:
        with _sync_lock:
            if _sync_singleton is None:
                _sync_singleton = BedrockLLMClient()
    return _sync_singleton


def call_bedrock(prompt: str, system_prompts: Optional[List[str]] = None, temperature: float = 0.0) -> tuple:
    sys_p = "\n".join(system_prompts) if system_prompts else ""
    text = _get_sync_client().call(sys_p, prompt, temperature=temperature)
    return text or "NONE", {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}


def call_bedrock_stream(prompt: str, system_prompts: Optional[List[str]] = None, temperature: float = 0.0) -> Iterator[dict]:
    sys_p = "\n".join(system_prompts) if system_prompts else ""
    for chunk in _get_sync_client().call_stream(sys_p, prompt, temperature=temperature):
        yield {"type": "content", "text": chunk}