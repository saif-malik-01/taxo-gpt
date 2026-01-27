import asyncio
from services.retrieval.hybrid import retrieve
from services.llm.bedrock_client import call_bedrock, call_bedrock_stream
from services.chat.prompt_builder import build_structured_prompt


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
    primary = []
    supporting = []

    for ch in chunks:
        ctype = ch.get("chunk_type")

        if intent == "judgment" and ctype == "judgment":
            primary.append(ch)

        elif intent == "definition" and ctype in ("definition", "operative", "act"):
            primary.append(ch)

        elif intent == "procedure" and ctype == "rule":
            primary.append(ch)

        else:
            supporting.append(ch)

    if not primary:
        primary = chunks[:3]
        supporting = chunks[3:]

    return primary, supporting


def get_full_judgments(retrieved_chunks, all_chunks):
    """
    Extract complete judgments by reassembling all chunks with same external_id
    Returns: dict mapping external_id -> complete judgment data
    """
    full_judgments = {}
    judgment_chunks_used = [c for c in retrieved_chunks if c.get("chunk_type") == "judgment"]
    
    for jchunk in judgment_chunks_used:
        # Use external_id to group all chunks of same judgment
        external_id = jchunk.get("metadata", {}).get("external_id")
        
        if not external_id:
            continue
            
        if external_id not in full_judgments:
            # Find ALL chunks belonging to this judgment (same external_id)
            related_chunks = [
                c for c in all_chunks 
                if c.get("chunk_type") == "judgment" and 
                   c.get("metadata", {}).get("external_id") == external_id
            ]
            
            if related_chunks:
                # Combine all chunks into complete judgment text
                full_text = "\n\n".join(c["text"] for c in related_chunks)
                
                # Get metadata from first chunk
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
                    "full_text": full_text,
                    "external_id": external_id
                }
    
    return full_judgments


async def chat(query, store, all_chunks, history=[], profile_summary=None):
    retrieved = retrieve(
        query=query,
        vector_store=store,
        all_chunks=all_chunks,
        k=25
    )

    intent = classify_query_intent(query)
    primary, supporting = split_primary_and_supporting(retrieved, intent)
    
    # Build prompt (without judgment_citations parameter)
    prompt = build_structured_prompt(
        query=query,
        primary=primary,
        supporting=supporting,
        history=history,
        profile_summary=profile_summary
    )

    answer = call_bedrock(prompt)
    
    # Get complete judgments (reassembled from all chunks)
    full_judgments = get_full_judgments(retrieved, all_chunks)

    return answer, retrieved, full_judgments


async def chat_stream(query, store, all_chunks, history=[], profile_summary=None):
    retrieved = retrieve(
        query=query,
        vector_store=store,
        all_chunks=all_chunks,
        k=25
    )

    intent = classify_query_intent(query)
    primary, supporting = split_primary_and_supporting(retrieved, intent)
    
    prompt = build_structured_prompt(
        query=query,
        primary=primary,
        supporting=supporting,
        history=history,
        profile_summary=profile_summary
    )

    # Get complete judgments
    full_judgments = get_full_judgments(retrieved, all_chunks)

    # Yield retrieved info first
    yield {
        "type": "retrieval",
        "sources": retrieved,
        "full_judgments": full_judgments
    }

    # Stream chunks
    for chunk in call_bedrock_stream(prompt):
        await asyncio.sleep(0.01)  # Tiny delay to ensure it doesn't batch too much
        yield {
            "type": "content",
            "delta": chunk
        }