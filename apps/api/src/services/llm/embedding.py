"""
apps/api/src/services/llm/embedding.py
Amazon Titan Embed Text v2 via AWS Bedrock.
Text and summary embeddings generated in parallel per chunk.
Each instance creates its own boto3 client (thread-safe).
"""

import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)


class TitanEmbeddingGenerator:

    def __init__(self):
        # Each instance gets its own client — safe for use in threads
        self._client = boto3.client(
            "bedrock-runtime",
            region_name=settings.AWS_REGION,
        )

    def _invoke(self, text: str) -> Optional[List[float]]:
        if not text or not text.strip():
            return None

        body = {
            "inputText":  text[:8000],
            "dimensions": settings.TITAN_DIMENSIONS,
            "normalize":  settings.TITAN_NORMALIZE,
        }

        for attempt in range(1, settings.PIPELINE_MAX_RETRIES + 1):
            try:
                response = self._client.invoke_model(
                    modelId     = settings.TITAN_MODEL_ID,
                    body        = json.dumps(body),
                    contentType = "application/json",
                    accept      = "application/json",
                )
                result    = json.loads(response["body"].read())
                embedding = result.get("embedding")

                if embedding and len(embedding) == settings.TITAN_DIMENSIONS:
                    return embedding

                logger.error(
                    f"Unexpected embedding size: "
                    f"got {len(embedding) if embedding else 0}"
                )
                return None

            except ClientError as e:
                code = e.response["Error"]["Code"]
                logger.error(f"Titan [{code}] attempt {attempt}")
                if code in ("ThrottlingException", "ServiceUnavailableException"):
                    time.sleep(settings.PIPELINE_RETRY_DELAY * attempt * 2)
                elif code == "ValidationException":
                    return None
                elif attempt == settings.PIPELINE_MAX_RETRIES:
                    return None
                else:
                    time.sleep(settings.PIPELINE_RETRY_DELAY)

            except Exception as e:
                logger.error(f"Titan unexpected error attempt {attempt}: {e}")
                if attempt == settings.PIPELINE_MAX_RETRIES:
                    return None
                time.sleep(settings.PIPELINE_RETRY_DELAY)

        return None

    def embed_both(
        self, text: str, summary: str
    ) -> Tuple[Optional[List[float]], Optional[List[float]]]:
        """
        Embed text and summary in parallel using two concurrent Bedrock calls.
        Returns (text_vector, summary_vector).
        If summary is empty, reuses text_vector.
        """
        if not summary or not summary.strip():
            text_vec = self._invoke(text)
            return text_vec, text_vec

        results = {}
        with ThreadPoolExecutor(max_workers=2) as ex:
            ft = ex.submit(self._invoke, text)
            fs = ex.submit(self._invoke, summary)
            results["text"]    = ft.result()
            results["summary"] = fs.result()

        text_vec    = results["text"]
        summary_vec = results["summary"] or text_vec
        return text_vec, summary_vec

    # Keep individual methods for backward compatibility
    def embed_text(self, text: str) -> Optional[List[float]]:
        return self._invoke(text)

    def embed_summary(self, summary: str) -> Optional[List[float]]:
        return self._invoke(summary)
