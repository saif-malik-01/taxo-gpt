import asyncio
from services.retrieval.hybrid import retrieve
from services.llm.bedrock_client import call_bedrock, call_bedrock_stream
from services.chat.prompt_builder import build_structured_prompt
from services.chat.response_citation_extractor import extract_and_attribute_citations
import logging

logger = logging.getLogger(__name__)


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
    """
    primary = []
    supporting = []

    for ch in chunks:
        ctype = ch.get("chunk_type")
        
        # Complete judgment chunks are ALWAYS primary
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
    """
    full_judgments = {}
    
    for chunk in retrieved_chunks:
        if chunk.get("chunk_type") != "judgment":
            continue
        
        # Case 1: Complete judgment chunk
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
                    "full_text": chunk["text"],
                    "external_id": external_id,
                    "_is_complete": True
                }
        
        # Case 2: Regular judgment chunk
        else:
            external_id = chunk.get("metadata", {}).get("external_id")
            
            if not external_id or external_id in full_judgments:
                continue
            
            related_chunks = [
                c for c in all_chunks
                if c.get("chunk_type") == "judgment" and 
                   c.get("metadata", {}).get("external_id") == external_id
            ]
            
            if related_chunks:
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
                    "full_text": full_text,
                    "external_id": external_id,
                    "_is_complete": False
                }
    
    return full_judgments


async def chat(query, store, all_chunks, history=[], profile_summary=None):
    """
    Enhanced chat with automatic citation attribution
    """
    
    # Step 1: Retrieve
    retrieved = retrieve(
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

    # Step 4: Call LLM
    raw_answer = call_bedrock(prompt)
    
    logger.info("=" * 80)
    logger.info("RAW LLM RESPONSE (before citation attribution):")
    logger.info(raw_answer[:500] + "..." if len(raw_answer) > 500 else raw_answer)
    logger.info("=" * 80)
    
    # Step 5: Extract party pairs and find citations
    enhanced_answer, party_citations = extract_and_attribute_citations(raw_answer, all_chunks)
    
    logger.info("=" * 80)
    logger.info("ENHANCED RESPONSE (with citation attribution):")
    logger.info(enhanced_answer[:500] + "..." if len(enhanced_answer) > 500 else enhanced_answer)
    logger.info(f"Party citations found: {len(party_citations)}")
    logger.info("=" * 80)
    
    # Step 6: Get complete judgments
    full_judgments = get_full_judgments(retrieved, all_chunks)

    # Return enhanced answer (with citations appended) and party_citations metadata
    return enhanced_answer, retrieved, full_judgments, party_citations


async def chat_stream(query, store, all_chunks, history=[], profile_summary=None):
    """
    Streaming chat with citation attribution at the end
    """
    
    # Step 1: Retrieve
    retrieved = retrieve(
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

    # Step 4: Get complete judgments
    full_judgments = get_full_judgments(retrieved, all_chunks)

    # Yield retrieval info first
    yield {
        "type": "retrieval",
        "sources": retrieved,
        "full_judgments": full_judgments
    }

    # Step 5: Stream LLM response and collect full answer
    full_answer = ""
    for chunk in call_bedrock_stream(prompt):
        full_answer += chunk
        await asyncio.sleep(0.01)
        yield {
            "type": "content",
            "delta": chunk
        }
    
    logger.info("=" * 80)
    logger.info("STREAMING: Full answer collected, extracting citations...")
    logger.info("=" * 80)
    
    # Step 6: Extract citations from full answer
    _, party_citations = extract_and_attribute_citations(full_answer, all_chunks)
    
    # Step 7: If citations found, stream the attribution section
    if party_citations:
        from services.chat.response_citation_extractor import format_citation_attribution
        citation_text = format_citation_attribution(party_citations)
        
        logger.info(f"Streaming citation attribution ({len(citation_text)} chars)")
        
        # Stream the citation section
        yield {
            "type": "content",
            "delta": citation_text
        }
        
        # Also send metadata about citations
        yield {
            "type": "citations",
            "party_citations": party_citations
        }