import asyncio
from services.retrieval.hybrid import retrieve
from services.retrieval.citation_matcher import get_index
from services.llm.bedrock_client import call_bedrock, call_bedrock_stream
from services.chat.prompt_builder import build_structured_prompt
from starlette.concurrency import run_in_threadpool


def classify_query_intent(query: str) -> str:
    q = query.lower()

    if "judgment" in q or "case law" in q or "court" in q:
        return "judgment"

    if "define" in q or "what is section" in q or "meaning of" in q:
        return "definition"

    if "rcm" in q or "reverse charge" in q:
        return "rcm"

    if "rate" in q or "gst rate" in q:
        return "rate"

    if "procedure" in q or "how to" in q:
        return "procedure"

    if "difference" in q or "vs" in q:
        return "comparison"

    return "general"


def split_primary_and_supporting(chunks, intent):
    """
    Split chunks into primary and supporting based on intent
    
    HANDLES COMPLETE JUDGMENT CHUNKS:
    - Complete judgment chunks (with _is_complete_judgment=True) are always primary
    - They already have metadata prepended in their text field
    """
    primary = []
    supporting = []

    for ch in chunks:
        ctype = ch.get("chunk_type")
        
        # ✅ Complete judgment chunks are ALWAYS primary
        if ch.get("_is_complete_judgment"):
            primary.append(ch)
            continue

        # Regular chunk classification
        if intent == "judgment" and ctype == "judgment":
            primary.append(ch)

        elif intent == "definition" and ctype in ("definition", "operative", "act"):
            primary.append(ch)

        elif intent == "procedure" and ctype == "rule":
            primary.append(ch)

        else:
            supporting.append(ch)

    # Fallback if no primary
    if not primary:
        primary = chunks[:3]
        supporting = chunks[3:]

    return primary, supporting


def get_full_judgments(retrieved_chunks, all_chunks):
    """
    Extract complete judgments metadata for citation display
    
    HANDLES BOTH:
    1. Complete judgment chunks (already assembled with metadata)
    2. Regular judgment chunks (need assembly)
    
    Returns judgment metadata for display/citation purposes
    """
    full_judgments = {}
    
    for chunk in retrieved_chunks:
        # Skip non-judgment chunks
        if chunk.get("chunk_type") != "judgment":
            continue
        
        # Case 1: Complete judgment chunk (already assembled)
        if chunk.get("_is_complete_judgment"):
            external_id = chunk.get("_external_id")
            
            if external_id and external_id not in full_judgments:
                metadata = chunk.get("metadata", {})
                
                full_judgments[external_id] = {
                    "citation": metadata.get("citation", ""),
                    "title": metadata.get("title", ""),
                    "case_number": metadata.get("case_number", ""),
                    "court": metadata.get("court", ""),
                    "state": metadata.get("state", ""),
                    "year": metadata.get("year", ""),
                    "judge": metadata.get("judge", ""),
                    "petitioner": metadata.get("petitioner", ""),
                    "respondent": metadata.get("respondent", ""),
                    "decision": metadata.get("decision", ""),
                    "current_status": metadata.get("current_status", ""),
                    "law": metadata.get("law", ""),
                    "act_name": metadata.get("act_name", ""),
                    "section_number": metadata.get("section_number", ""),
                    "rule_name": metadata.get("rule_name", ""),
                    "rule_number": metadata.get("rule_number", ""),
                    "notification_number": metadata.get("notification_number", ""),
                    "case_note": metadata.get("case_note", ""),
                    "full_text": chunk["text"],  # Already has metadata prepended
                    "external_id": external_id,
                    "_is_complete": True
                }
        
        # Case 2: Regular judgment chunk (need to assemble)
        else:
            external_id = chunk.get("metadata", {}).get("external_id")
            
            if not external_id or external_id in full_judgments:
                continue
            
            # OPTIMIZED: Use index lookup instead of linear scan over 450MB
            index = get_index(all_chunks)
            related_chunks = index.judgment_by_external_id.get(external_id, [])
            
            if related_chunks:
                # Combine original chunks (no metadata headers)
                full_text = "\n\n".join(c["text"] for c in related_chunks)
                
                metadata = related_chunks[0].get("metadata", {})
                
                full_judgments[external_id] = {
                    "citation": metadata.get("citation", ""),
                    "title": metadata.get("title", ""),
                    "case_number": metadata.get("case_number", ""),
                    "court": metadata.get("court", ""),
                    "state": metadata.get("state", ""),
                    "year": metadata.get("year", ""),
                    "judge": metadata.get("judge", ""),
                    "petitioner": metadata.get("petitioner", ""),
                    "respondent": metadata.get("respondent", ""),
                    "decision": metadata.get("decision", ""),
                    "current_status": metadata.get("current_status", ""),
                    "law": metadata.get("law", ""),
                    "act_name": metadata.get("act_name", ""),
                    "section_number": metadata.get("section_number", ""),
                    "rule_name": metadata.get("rule_name", ""),
                    "rule_number": metadata.get("rule_number", ""),
                    "notification_number": metadata.get("notification_number", ""),
                    "case_note": metadata.get("case_note", ""),
                    "full_text": full_text,  # Clean original text
                    "external_id": external_id,
                    "_is_complete": False
                }
    
    return full_judgments


async def chat(query, store, all_chunks, history=[], profile_summary=None):
    # Offload retrieval to thread pool
    retrieved = await run_in_threadpool(
        retrieve,
        query=query,
        vector_store=store,
        all_chunks=all_chunks,
        k=25
    )

    # Step 2: Classify and split
    intent = classify_query_intent(query)
    primary, supporting = split_primary_and_supporting(retrieved, intent)
    
    # Step 3: Build prompt
    # The prompt builder will receive complete judgment chunks in primary/supporting
    # and will render them using chunk['text'] which already has metadata prepended
    prompt = build_structured_prompt(
        query=query,
        primary=primary,
        supporting=supporting,
        history=history,
        profile_summary=profile_summary
    )

    # Offload Bedrock call
    answer = await run_in_threadpool(call_bedrock, prompt)
    
    # Offload reassembly to thread pool
    full_judgments = await run_in_threadpool(get_full_judgments, retrieved, all_chunks)

    return answer, retrieved, full_judgments


async def chat_stream(query, store, all_chunks, history=[], profile_summary=None):
    # Offload retrieval
    retrieved = await run_in_threadpool(
        retrieve,
        query=query,
        vector_store=store,
        all_chunks=all_chunks,
        k=25
    )

    # Step 2: Classify and split
    intent = classify_query_intent(query)
    primary, supporting = split_primary_and_supporting(retrieved, intent)
    
    # Step 3: Build prompt
    prompt = build_structured_prompt(
        query=query,
        primary=primary,
        supporting=supporting,
        history=history,
        profile_summary=profile_summary
    )

    # Offload reassembly
    full_judgments = await run_in_threadpool(get_full_judgments, retrieved, all_chunks)

    # Yield retrieval info first
    yield {
        "type": "retrieval",
        "sources": retrieved,
        "full_judgments": full_judgments
    }

    # Step 5: Stream LLM response
    async for chunk in call_bedrock_stream(prompt):
        await asyncio.sleep(0.01)
        yield {
            "type": "content",
            "delta": chunk
        }