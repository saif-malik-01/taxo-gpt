"""
apps/api/src/services/chat/responder.py
Stage 6 — Cross-reference enrichment and LLM response generation.
"""

import json
import logging
from typing import Dict, List, Optional, Tuple

from apps.api.src.services.llm.bedrock import AsyncBedrockLLMClient
from apps.api.src.services.rag.models import (
    CitationResult, FinalResponse, IntentResult, SessionMessage, Stage2AResult, Stage2BResult,
)

logger = logging.getLogger(__name__)

class LLMResponder:
    """
    Orchestrates the LLM generation stage of the retrieval pipeline.
    """

    def __init__(self, llm: AsyncBedrockLLMClient):
        self._llm = llm

    async def generate_stream(
        self,
        query: str,
        history: List[SessionMessage],
        a2: Stage2AResult,
        b2: Stage2BResult,
        intent: IntentResult,
        top_chunks: List[Dict],
        citation_res: CitationResult,
    ):
        """
        Async generator — yields text chunks then a metadata JSON block.
        """
        system, user = self._build_prompts(query, history, a2, b2, intent, top_chunks, citation_res)

        async for chunk in self._llm.call_stream(system, user, label="stage6_stream"):
            yield chunk

        # Metadata event
        meta = {
            "intent": intent.intent,
            "confidence": intent.confidence,
            "retrieved_documents": top_chunks
        }
        yield "\n\n__META__" + json.dumps(meta)

    def _build_prompts(self, query, history, a2, b2, intent, top_chunks, citation_res):
        """Build the prompts for the LLM."""
        system = "You are an expert Indian tax law AI assistant..."
        
        chunk_texts = "\n\n".join(
            f"--- Chunk {i+1} ---\n{ch.get('text', '')}"
            for i, ch in enumerate(top_chunks)
        )
        user = f"Context:\n{chunk_texts}\n\nUser Query: {query}"
        return system, user
