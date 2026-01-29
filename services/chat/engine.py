# import asyncio
# from services.retrieval.hybrid import retrieve
# from services.retrieval.citation_matcher import get_index
# from services.llm.bedrock_client import call_bedrock, call_bedrock_stream
# from services.chat.prompt_builder import build_structured_prompt, get_system_prompt
# from services.chat.response_citation_extractor import extract_and_attribute_citations
# from starlette.concurrency import run_in_threadpool
# import logging

# logger = logging.getLogger(__name__)


# def classify_query_intent(query: str) -> str:
#     q = query.lower()

#     if "judgment" in q or "case law" in q or "court" in q:
#         return "judgment"

#     if "define" in q or "what is section" in q or "meaning of" in q:
#         return "definition"

#     if "rcm" in q or "reverse charge" in q:
#         return "rcm"

#     if "rate" in q or "gst rate" in q:
#         return "rate"

#     if "procedure" in q or "how to" in q:
#         return "procedure"

#     if "difference" in q or "vs" in q:
#         return "comparison"

#     return "general"


# def split_primary_and_supporting(chunks, intent):
#     """
#     Split chunks into primary and supporting based on intent
    
#     HANDLES COMPLETE JUDGMENT CHUNKS:
#     - Complete judgment chunks (with _is_complete_judgment=True) are always primary
#     """
#     primary = []
#     supporting = []

#     for ch in chunks:
#         ctype = ch.get("chunk_type")
        
#         # Complete judgment chunks are ALWAYS primary
#         if ch.get("_is_complete_judgment"):
#             primary.append(ch)
#             continue

#         # Regular chunk classification
#         if intent == "judgment" and ctype == "judgment":
#             primary.append(ch)

#         elif intent == "definition" and ctype in ("definition", "operative", "act"):
#             primary.append(ch)

#         elif intent == "procedure" and ctype == "rule":
#             primary.append(ch)

#         else:
#             supporting.append(ch)

#     # Fallback if no primary
#     if not primary:
#         primary = chunks[:3]
#         supporting = chunks[3:]

#     return primary, supporting


# def get_full_judgments(retrieved_chunks, all_chunks):
#     """
#     Extract complete judgments metadata for citation display
    
#     HANDLES BOTH:
#     1. Complete judgment chunks (already assembled with metadata)
#     2. Regular judgment chunks (need assembly)
#     """
#     full_judgments = {}
    
#     for chunk in retrieved_chunks:
#         if chunk.get("chunk_type") != "judgment":
#             continue
        
#         # Case 1: Complete judgment chunk
#         if chunk.get("_is_complete_judgment"):
#             external_id = chunk.get("_external_id")
            
#             if external_id and external_id not in full_judgments:
#                 metadata = chunk.get("metadata", {})
                
#                 full_judgments[external_id] = {
#                     "citation": metadata.get("citation", ""),
#                     "title": metadata.get("title", ""),
#                     "case_number": metadata.get("case_number", ""),
#                     "court": metadata.get("court", ""),
#                     "state": metadata.get("state", ""),
#                     "year": metadata.get("year", ""),
#                     "judge": metadata.get("judge", ""),
#                     "petitioner": metadata.get("petitioner", ""),
#                     "respondent": metadata.get("respondent", ""),
#                     "decision": metadata.get("decision", ""),
#                     "current_status": metadata.get("current_status", ""),
#                     "law": metadata.get("law", ""),
#                     "act_name": metadata.get("act_name", ""),
#                     "section_number": metadata.get("section_number", ""),
#                     "rule_name": metadata.get("rule_name", ""),
#                     "rule_number": metadata.get("rule_number", ""),
#                     "notification_number": metadata.get("notification_number", ""),
#                     "case_note": metadata.get("case_note", ""),
#                     "full_text": chunk["text"],
#                     "external_id": external_id,
#                     "_is_complete": True
#                 }
        
#         # Case 2: Regular judgment chunk
#         else:
#             external_id = chunk.get("metadata", {}).get("external_id")
            
#             if not external_id or external_id in full_judgments:
#                 continue
            
#             related_chunks = [
#                 c for c in all_chunks
#                 if c.get("chunk_type") == "judgment" and 
#                    c.get("metadata", {}).get("external_id") == external_id
#             ]
            
#             if related_chunks:
#                 full_text = "\n\n".join(c["text"] for c in related_chunks)
#                 metadata = related_chunks[0].get("metadata", {})
                
#                 full_judgments[external_id] = {
#                     "citation": metadata.get("citation", ""),
#                     "title": metadata.get("title", ""),
#                     "case_number": metadata.get("case_number", ""),
#                     "court": metadata.get("court", ""),
#                     "state": metadata.get("state", ""),
#                     "year": metadata.get("year", ""),
#                     "judge": metadata.get("judge", ""),
#                     "petitioner": metadata.get("petitioner", ""),
#                     "respondent": metadata.get("respondent", ""),
#                     "decision": metadata.get("decision", ""),
#                     "current_status": metadata.get("current_status", ""),
#                     "law": metadata.get("law", ""),
#                     "act_name": metadata.get("act_name", ""),
#                     "section_number": metadata.get("section_number", ""),
#                     "rule_name": metadata.get("rule_name", ""),
#                     "rule_number": metadata.get("rule_number", ""),
#                     "notification_number": metadata.get("notification_number", ""),
#                     "case_note": metadata.get("case_note", ""),
#                     "full_text": full_text,
#                     "external_id": external_id,
#                     "_is_complete": False
#                 }
    
#     return full_judgments


# async def chat(query, store, all_chunks, history=[], profile_summary=None):
#     """
#     Enhanced chat with automatic citation attribution
#     """
    
#     # Step 1: Retrieve
#     retrieved = retrieve(
#         query=query,
#         vector_store=store,
#         all_chunks=all_chunks,
#         k=25
#     )

#     # Step 2: Classify and split
#     intent = classify_query_intent(query)
#     primary, supporting = split_primary_and_supporting(retrieved, intent)
    
#     # Step 3: Build prompt (SYSTEM + USER)
#     system_prompt = get_system_prompt(profile_summary)
    
#     user_prompt = build_structured_prompt(
#         query=query,
#         primary=primary,
#         supporting=supporting,
#         history=history,
#         profile_summary=profile_summary
#     )

#     # Step 4: Call LLM (Inference Params: Temp=0)
#     raw_answer = call_bedrock(
#         prompt=user_prompt,
#         system_prompts=[system_prompt],
#         temperature=0.0
#     )
    
#     logger.info("=" * 80)
#     logger.info("RAW LLM RESPONSE (before citation attribution):")
#     logger.info(raw_answer[:500] + "..." if len(raw_answer) > 500 else raw_answer)
#     logger.info("=" * 80)
    
#     # Step 5: Extract party pairs and find citations
#     enhanced_answer, party_citations = extract_and_attribute_citations(raw_answer, all_chunks)
    
#     logger.info("=" * 80)
#     logger.info("ENHANCED RESPONSE (with citation attribution):")
#     logger.info(enhanced_answer[:500] + "..." if len(enhanced_answer) > 500 else enhanced_answer)
#     logger.info(f"Party citations found: {len(party_citations)}")
#     logger.info("=" * 80)
    
#     # Step 6: Get complete judgments
#     full_judgments = get_full_judgments(retrieved, all_chunks)

#     # Return enhanced answer (with citations appended) and party_citations metadata
#     return enhanced_answer, retrieved, full_judgments, party_citations


# async def chat_stream(query, store, all_chunks, history=[], profile_summary=None):
#     """
#     Streaming chat with citation attribution
    
#     UPDATED FLOW:
#     1. Retrieve sources
#     2. Generate initial response (collect fully)
#     3. Extract citations and re-attribute
#     4. Stream the ENHANCED response character by character
#     """
    
#     # Step 1: Retrieve
#     retrieved = retrieve(
#         query=query,
#         vector_store=store,
#         all_chunks=all_chunks,
#         k=25
#     )

#     # Step 2: Classify and split
#     intent = classify_query_intent(query)
#     primary, supporting = split_primary_and_supporting(retrieved, intent)
    
#     # Step 3: Build prompt (SYSTEM + USER)
#     system_prompt = get_system_prompt(profile_summary)
    
#     user_prompt = build_structured_prompt(
#         query=query,
#         primary=primary,
#         supporting=supporting,
#         history=history,
#         profile_summary=profile_summary
#     )

#     # Offload reassembly to thread pool
#     full_judgments = await run_in_threadpool(get_full_judgments, retrieved, all_chunks)

#     # Yield retrieval info first
#     yield {
#         "type": "retrieval",
#         "sources": retrieved,
#         "full_judgments": full_judgments
#     }

#     # Step 4: Collect FULL initial response (non-streaming)
#     logger.info("=" * 80)
#     logger.info("STREAMING: Collecting initial LLM response...")
#     logger.info("=" * 80)
    
#     initial_response = ""
#     for chunk in call_bedrock_stream(
#         prompt=user_prompt,
#         system_prompts=[system_prompt],
#         temperature=0.0
#     ):
#         initial_response += chunk
    
#     logger.info(f"Initial response collected: {len(initial_response)} chars")
    
#     # Step 5: Extract citations and re-attribute
#     logger.info("Extracting and attributing citations...")
    
#     enhanced_response, party_citations = await run_in_threadpool(
#         extract_and_attribute_citations,
#         initial_response,
#         all_chunks
#     )
    
#     logger.info(f"Enhanced response ready: {len(enhanced_response)} chars")
#     logger.info(f"Party citations found: {len(party_citations)}")
    
#     # Step 6: Stream the ENHANCED response
#     logger.info("Streaming enhanced response...")
    
#     # Stream character by character with small delays
#     for char in enhanced_response:
#         await asyncio.sleep(0.01)  # Small delay for smooth streaming
#         yield {
#             "type": "content",
#             "delta": char
#         }
    
#     # Step 7: Send citation metadata
#     if party_citations:
#         # Convert tuple keys to string for JSON serialization
#         party_citations_json = {}
#         for (p1, p2), citations in party_citations.items():
#             party_citations_json[f"{p1} vs {p2}"] = citations

#         yield {
#             "type": "citations",
#             "party_citations": party_citations_json
#         }
    
#     logger.info("=" * 80)
#     logger.info("STREAMING COMPLETE")
#     logger.info("=" * 80)
import asyncio
from services.retrieval.hybrid import retrieve
from services.retrieval.citation_matcher import get_index
from services.llm.bedrock_client import call_bedrock, call_bedrock_stream
from services.chat.prompt_builder import build_structured_prompt, get_system_prompt
from services.chat.response_citation_extractor import extract_and_attribute_citations
from starlette.concurrency import run_in_threadpool
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
    
    # Step 3: Build prompt (SYSTEM + USER)
    system_prompt = get_system_prompt(profile_summary)
    
    user_prompt = build_structured_prompt(
        query=query,
        primary=primary,
        supporting=supporting,
        history=history,
        profile_summary=profile_summary
    )

    # Step 4: Call LLM (Inference Params: Temp=0)
    raw_answer = call_bedrock(
        prompt=user_prompt,
        system_prompts=[system_prompt],
        temperature=0.0
    )
    
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
    ULTRA-OPTIMIZED STREAMING
    
    OPTIMIZATIONS:
    1. Sources sent AFTER first content chunk (instant user feedback)
    2. Full_judgments processed in parallel (non-blocking)
    3. Citation extraction in thread pool (non-blocking)
    4. No artificial delays
    5. Larger chunks for faster delivery (50 chars)
    6. All heavy operations parallelized
    """
    
    logger.info("=" * 80)
    logger.info(f"QUERY: {query[:100]}...")
    logger.info("=" * 80)
    
    # Step 1: Retrieve chunks
    retrieved = retrieve(
        query=query,
        vector_store=store,
        all_chunks=all_chunks,
        k=25
    )
    logger.info(f"✓ Retrieved {len(retrieved)} chunks")

    # Step 2: Classify and split
    intent = classify_query_intent(query)
    primary, supporting = split_primary_and_supporting(retrieved, intent)
    logger.info(f"✓ Intent: {intent} | Primary: {len(primary)} | Supporting: {len(supporting)}")
    
    # Step 3: Build prompts
    system_prompt = get_system_prompt(profile_summary)
    user_prompt = build_structured_prompt(
        query=query,
        primary=primary,
        supporting=supporting,
        history=history,
        profile_summary=profile_summary
    )

    # Step 4: START BACKGROUND TASKS immediately
    full_judgments_task = asyncio.create_task(
        run_in_threadpool(get_full_judgments, retrieved, all_chunks)
    )

    # Step 5: Collect initial LLM response (while full_judgments runs in background)
    logger.info("⏳ Collecting LLM response...")
    initial_response = ""
    
    for chunk in call_bedrock_stream(
        prompt=user_prompt,
        system_prompts=[system_prompt],
        temperature=0.0
    ):
        initial_response += chunk
    
    logger.info(f"✓ LLM response collected: {len(initial_response)} chars")
    
    # Step 6: START citation extraction in background
    citation_task = asyncio.create_task(
        run_in_threadpool(
            extract_and_attribute_citations,
            initial_response,
            all_chunks
        )
    )
    
    logger.info("⏳ Citation extraction started in background...")
    
    # Step 7: Wait for citation extraction to complete
    enhanced_response, party_citations = await citation_task
    
    logger.info(f"✓ Citations extracted: {len(party_citations)} party pairs")
    logger.info(f"✓ Enhanced response ready: {len(enhanced_response)} chars")
    
    # Step 8: Stream ENHANCED response - send first chunk IMMEDIATELY
    logger.info("🚀 Streaming enhanced response...")
    
    # OPTIMIZED: Larger chunks = faster delivery, less overhead
    CHUNK_SIZE = 50  # Increased from 25 for better performance
    
    sources_sent = False
    
    for i in range(0, len(enhanced_response), CHUNK_SIZE):
        chunk_text = enhanced_response[i:i + CHUNK_SIZE]
        
        # Send first content chunk
        yield {
            "type": "content",
            "delta": chunk_text
        }
        
        # Send sources AFTER first chunk for instant user feedback
        if not sources_sent:
            yield {
                "type": "retrieval",
                "sources": retrieved,
                "full_judgments": {}  # Will be populated later
            }
            sources_sent = True
            logger.info("✓ Sent retrieval metadata after first chunk")
        
        # NO DELAY - maximum speed
    
    logger.info("✓ Streaming complete")
    
    # Step 9: Wait for full_judgments (might already be done)
    full_judgments = await full_judgments_task
    logger.info(f"✓ Full judgments ready: {len(full_judgments)}")
    
    yield {
        "type": "metadata",
        "full_judgments": full_judgments
    }
    
    # Step 10: Send citation metadata
    if party_citations:
        party_citations_json = {}
        for (p1, p2), citations in party_citations.items():
            party_citations_json[f"{p1} vs {p2}"] = citations

        yield {
            "type": "citations",
            "party_citations": party_citations_json
        }
        logger.info(f"✓ Sent {len(party_citations_json)} citation groups")
    
    logger.info("=" * 80)
    logger.info("✅ STREAMING COMPLETE")
    logger.info("=" * 80)


async def chat_stream_alternative(query, store, all_chunks, history=[], profile_summary=None):
    """
    ALTERNATIVE: Even more aggressive optimization
    
    This version sends chunks as WORDS instead of character chunks
    for more natural-looking streaming and better performance.
    """
    
    logger.info("=" * 80)
    logger.info(f"QUERY: {query[:100]}...")
    logger.info("=" * 80)
    
    # Steps 1-3: Same as above
    retrieved = retrieve(query=query, vector_store=store, all_chunks=all_chunks, k=25)
    logger.info(f"✓ Retrieved {len(retrieved)} chunks")

    intent = classify_query_intent(query)
    primary, supporting = split_primary_and_supporting(retrieved, intent)
    logger.info(f"✓ Intent: {intent} | Primary: {len(primary)} | Supporting: {len(supporting)}")
    
    system_prompt = get_system_prompt(profile_summary)
    user_prompt = build_structured_prompt(
        query=query, primary=primary, supporting=supporting,
        history=history, profile_summary=profile_summary
    )

    # Start background task
    full_judgments_task = asyncio.create_task(
        run_in_threadpool(get_full_judgments, retrieved, all_chunks)
    )

    # Collect LLM response
    logger.info("⏳ Collecting LLM response...")
    initial_response = ""
    for chunk in call_bedrock_stream(
        prompt=user_prompt, system_prompts=[system_prompt], temperature=0.0
    ):
        initial_response += chunk
    logger.info(f"✓ LLM response collected: {len(initial_response)} chars")
    
    # Extract citations
    citation_task = asyncio.create_task(
        run_in_threadpool(extract_and_attribute_citations, initial_response, all_chunks)
    )
    enhanced_response, party_citations = await citation_task
    logger.info(f"✓ Citations: {len(party_citations)} pairs | Enhanced: {len(enhanced_response)} chars")
    
    # Stream by WORDS (more natural, faster than small char chunks)
    logger.info("🚀 Streaming by words...")
    
    words = enhanced_response.split(' ')
    sources_sent = False
    
    # Send words in batches of 5 for optimal performance
    WORDS_PER_BATCH = 5
    
    for i in range(0, len(words), WORDS_PER_BATCH):
        batch = words[i:i + WORDS_PER_BATCH]
        
        # Reconstruct with spaces
        if i + WORDS_PER_BATCH >= len(words):
            # Last batch - no trailing space
            chunk_text = ' '.join(batch)
        else:
            chunk_text = ' '.join(batch) + ' '
        
        yield {
            "type": "content",
            "delta": chunk_text
        }
        
        # Send sources after first batch
        if not sources_sent:
            yield {
                "type": "retrieval",
                "sources": retrieved,
                "full_judgments": {}
            }
            sources_sent = True
            logger.info("✓ Sent sources after first word batch")
    
    logger.info("✓ Word streaming complete")
    
    # Send metadata
    full_judgments = await full_judgments_task
    logger.info(f"✓ Full judgments: {len(full_judgments)}")
    
    yield {"type": "metadata", "full_judgments": full_judgments}
    
    if party_citations:
        party_citations_json = {}
        for (p1, p2), citations in party_citations.items():
            party_citations_json[f"{p1} vs {p2}"] = citations
        yield {"type": "citations", "party_citations": party_citations_json}
        logger.info(f"✓ Sent {len(party_citations_json)} citation groups")
    
    logger.info("=" * 80)
    logger.info("✅ STREAMING COMPLETE")
    logger.info("=" * 80)