"""
apps/api/src/services/chat/responder.py
Stage 6 — Cross-reference enrichment and LLM response generation.
"""

import json
import logging
from typing import Dict, List, Optional, Tuple

from apps.api.src.services.llm.bedrock import BedrockLLMClient
from apps.api.src.services.rag.models import (
    CitationResult, FinalResponse, IntentResult, SessionMessage, Stage2AResult, Stage2BResult,
)

logger = logging.getLogger(__name__)

class LLMResponder:
    """
    Orchestrates the LLM generation stage of the retrieval pipeline.
    """

    def __init__(self, llm: BedrockLLMClient):
        self._llm = llm

    def generate(
        self,
        query: str,
        history: List[SessionMessage],
        a2: Stage2AResult,
        b2: Stage2BResult,
        intent: IntentResult,
        top_chunks: List[Dict],
        citation_res: CitationResult,
    ) -> FinalResponse:
        """
        1. Formulate complex prompt including retrieved chunks and intent.
        2. Non-streaming LLM call.
        3. Parse the result.
        """
        system, user = self._build_prompts(query, history, a2, b2, intent, top_chunks, citation_res)
        answer = self._llm.call(system, user, label="stage6")
        
        return FinalResponse(
            answer=answer or "I couldn't generate a response based on the retrieved information.",
            retrieved_documents=top_chunks,
            intent=intent.intent,
            confidence=intent.confidence,
        )

    def generate_stream(
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
        Yields text chunks and then follows with metadata JSON block.
        """
        system, user = self._build_prompts(query, history, a2, b2, intent, top_chunks, citation_res)
        
        for chunk in self._llm.call_stream(system, user, label="stage6_stream"):
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
