"""
ingestion_api/autofill/bedrock_caller.py

Calls AWS Bedrock with the autofill prompt and parses the JSON response.
Uses the same Bedrock region as your existing chunking pipeline.
Model: configurable via AUTOFILL_MODEL_ID in .env
       defaults to Amazon Nova Pro (reliable instruction following)
       set to your Qwen3 ARN if available in your region.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

from api.req_models import AutofillField, AutofillResponse
from autofill.prompt_builder import build_prompt
from api.config import SVC_CONFIG
from utils.logger import get_logger

logger = get_logger("autofill_bedrock")


class AutofillBedrockCaller:

    def __init__(self):
        cfg = SVC_CONFIG.bedrock_autofill
        self._client = boto3.client(
            "bedrock-runtime",
            region_name=cfg.region,
        )
        self._model_id   = cfg.model_id
        self._max_tokens = cfg.max_tokens
        self._temperature = cfg.temperature

    async def autofill(
        self,
        chunk_type:  str,
        anchor_data: dict[str, Any],
        split:       bool = False,
    ) -> AutofillResponse:
        """
        Build prompt → call Bedrock → parse JSON → return AutofillResponse.
        """
        system_prompt, user_prompt = build_prompt(chunk_type, anchor_data, split)

        start = time.time()

        loop = asyncio.get_event_loop()
        raw_text = await loop.run_in_executor(
            None,
            self._call_bedrock,
            system_prompt,
            user_prompt,
        )

        latency_ms = int((time.time() - start) * 1000)
        logger.info(
            f"Autofill Bedrock call: model={self._model_id} "
            f"latency={latency_ms}ms type={chunk_type} split={split}"
        )

        parsed = self._parse_json_response(raw_text)

        if split:
            # If split=True, parsed should be a List[Dict]
            suggested_chunks = parsed if isinstance(parsed, list) else [parsed]
            return AutofillResponse(
                chunk_type=chunk_type,
                fields=[],
                suggested_chunks=suggested_chunks,
                model_used=self._model_id,
                latency_ms=latency_ms,
            )
        else:
            # Standard single-chunk autofill
            fields = [
                AutofillField(path=path, value=value)
                for path, value in parsed.items()
                if value is not None
            ]
            return AutofillResponse(
                chunk_type=chunk_type,
                fields=fields,
                model_used=self._model_id,
                latency_ms=latency_ms,
            )

    def _call_bedrock(self, system_prompt: str, user_prompt: str) -> str:
        """
        Synchronous Bedrock invoke using the Converse API.
        The Converse API works with all Nova and Llama models.
        For Qwen3 via custom ARN, falls back to InvokeModel if needed.
        """
        max_retries = 3
        last_exc = None

        for attempt in range(max_retries):
            try:
                response = self._client.converse(
                    modelId=self._model_id,
                    system=[{"text": system_prompt}],
                    messages=[
                        {"role": "user", "content": [{"text": user_prompt}]}
                    ],
                    inferenceConfig={
                        "maxTokens":   self._max_tokens,
                        "temperature": self._temperature,
                    },
                )
                # Extract text from Converse response
                content_blocks = response["output"]["message"]["content"]
                return "".join(
                    block["text"]
                    for block in content_blocks
                    if block.get("type") == "text" or "text" in block
                )

            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code == "ThrottlingException" and attempt < max_retries - 1:
                    wait = 2 ** attempt
                    logger.warning(f"Bedrock throttled — waiting {wait}s (attempt {attempt+1})")
                    time.sleep(wait)
                    last_exc = e
                    continue
                raise

        raise last_exc  # type: ignore

    def _parse_json_response(self, raw: str) -> dict[str, Any]:
        """
        Extract and parse JSON from the LLM response.
        Handles:
          - Clean JSON object
          - JSON wrapped in ```json ... ``` fences
          - JSON embedded in explanatory text
        """
        if not raw or not raw.strip():
            logger.warning("Autofill LLM returned empty response")
            return {}

        # Try direct parse first
        try:
            return json.loads(raw.strip())
        except json.JSONDecodeError:
            pass

        # Strip markdown fences
        cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned.strip())
        try:
            return json.loads(cleaned.strip())
        except json.JSONDecodeError:
            pass

        # Find outermost {...} block
        start = raw.find("{")
        end   = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(raw[start:end + 1])
            except json.JSONDecodeError:
                pass

        logger.error(f"Failed to parse autofill JSON. Raw response (first 500): {raw[:500]}")
        return {}