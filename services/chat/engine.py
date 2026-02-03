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


async def chat(query, store, all_chunks, history=[], profile_summary=None, document_context=None):
    """
    Enhanced chat with automatic citation attribution
    
    Supports optional document_context for analyzing uploaded documents.
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
    
    # Step 3: Build prompt (SYSTEM + USER) with optional document context
    system_prompt = get_system_prompt(profile_summary)
    
    user_prompt = build_structured_prompt(
        query=query,
        primary=primary,
        supporting=supporting,
        history=history,
        profile_summary=profile_summary,
        document_context=document_context  # Pass document context
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
    
    # Step 5: Extract party pairs and find citations (returns generator)
    enhanced_stream, party_citations = extract_and_attribute_citations(raw_answer, all_chunks)
    
    # Collect the streamed response
    enhanced_answer = "".join(enhanced_stream)
    
    logger.info("=" * 80)
    logger.info("ENHANCED RESPONSE (with citation attribution):")
    logger.info(enhanced_answer[:500] + "..." if len(enhanced_answer) > 500 else enhanced_answer)
    logger.info(f"Party citations found: {len(party_citations)}")
    logger.info("=" * 80)
    
    # Step 6: Get complete judgments
    full_judgments = get_full_judgments(retrieved, all_chunks)

    # Return enhanced answer (with citations appended) and party_citations metadata
    return enhanced_answer, retrieved, full_judgments, party_citations


async def chat_stream(query, store, all_chunks, history=[], profile_summary=None, document_context=None):
    """
    ✅ FULLY OPTIMIZED STREAMING WITH REAL-TIME ENHANCED RESPONSE
    
    KEY IMPROVEMENTS:
    1. Collect LLM output silently
    2. Process citations in background (parallel processing, top-5, etc.)
    3. STREAM enhanced response AS LLM GENERATES IT (real streaming!)
    4. Send sources and metadata after streaming
    
    DOCUMENT SUPPORT:
    - Optional document_context parameter for analyzing uploaded documents
    - If provided, documents are integrated into the prompt for context-aware analysis
    - Works seamlessly with existing chat flow (backward compatible)
    """
    
    import time
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info(f"QUERY: {query[:100]}...")
    if document_context:
        logger.info(f"📄 Document context provided: {len(document_context)} chars")
    logger.info("=" * 80)
    
    # Step 1: Retrieve chunks (optionally skip if document_context is comprehensive)
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
    
    # Step 3: Build prompts (incorporate document context if provided)
    system_prompt = get_system_prompt(profile_summary)
    user_prompt = build_structured_prompt(
        query=query,
        primary=primary,
        supporting=supporting,
        history=history,
        profile_summary=profile_summary,
        document_context=document_context  # Pass document context to prompt builder
    )

    # Step 4: START BACKGROUND TASK for full_judgments
    full_judgments_task = asyncio.create_task(
        run_in_threadpool(get_full_judgments, retrieved, all_chunks)
    )

    # Step 5: ✅ COLLECT LLM OUTPUT SILENTLY
    logger.info("🔄 Collecting LLM response...")
     
    collected_response = ""
    collection_start = time.time()
    
    for chunk in call_bedrock_stream(
        prompt=user_prompt,
        system_prompts=[system_prompt],
        temperature=0.0
    ):
        collected_response += chunk
    
    collection_time = time.time() - collection_start
    logger.info(f"✓ LLM response collected in {collection_time:.2f}s ({len(collected_response)} chars)")
    
    # Step 6: ✅ START CITATION PROCESSING (returns generator and party_citations)
    logger.info("⏳ Processing citations with optimized parallel matching...")
    
    enhanced_stream = None
    party_citations = {}
    
    try:
        # This returns a generator and party_citations dict
        enhanced_stream, party_citations = await asyncio.wait_for(
            run_in_threadpool(
                extract_and_attribute_citations,
                collected_response,
                all_chunks
            ),
            timeout=120.0
        )
        
        total_citations = sum(len(cits) for cits in party_citations.values())
        logger.info(f"✓ Citations processed: {len(party_citations)} party pairs, {total_citations} total citations")
        logger.info("✓ Enhanced response generator ready")
        
    except asyncio.TimeoutError:
        logger.warning("⚠️ Citation extraction timeout - using original response")
        party_citations = {}
        # Fallback: create generator from original response
        def fallback_generator():
            chunk_size = 50
            for i in range(0, len(collected_response), chunk_size):
                yield collected_response[i:i+chunk_size]
        enhanced_stream = fallback_generator()
        
    except Exception as e:
        logger.error(f"❌ Citation extraction failed: {e}")
        party_citations = {}
        # Fallback: create generator from original response
        def fallback_generator():
            chunk_size = 50
            for i in range(0, len(collected_response), chunk_size):
                yield collected_response[i:i+chunk_size]
        enhanced_stream = fallback_generator()
    
    citation_processing_time = time.time() - start_time - collection_time
    logger.info(f"✓ Citation processing setup took {citation_processing_time:.2f}s")
    
    # Step 7: ✅ STREAM THE ENHANCED RESPONSE IN REAL-TIME
    logger.info("🚀 Starting REAL-TIME streaming of enhanced response...")
    
    first_chunk_sent = False
    first_chunk_time = 0
    
    # Stream directly from the generator (as re-attribution LLM generates)
    for chunk in enhanced_stream:
        yield {
            "type": "content",
            "delta": chunk
        }
        
        # Track timing of first chunk
        if not first_chunk_sent:
            first_chunk_time = time.time() - start_time
            logger.info(f"⚡ First chunk of enhanced response sent in {first_chunk_time:.2f}s")
            first_chunk_sent = True
    
    stream_complete_time = time.time() - start_time
    logger.info(f"✓ Enhanced response streaming complete in {stream_complete_time:.2f}s")
    
    # Step 8: ✅ SEND SOURCES
    logger.info("📤 Sending retrieval sources...")
    yield {
        "type": "retrieval",
        "sources": retrieved,
        "full_judgments": {}
    }
    logger.info("✓ Sent retrieval metadata")
    
    # Step 9: Wait for full_judgments and send
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
        
        total_cits = sum(len(cits) for cits in party_citations.values())
        logger.info(f"✓ Sent {len(party_citations_json)} citation groups ({total_cits} total citations)")
    
    total_time = time.time() - start_time
    print("=" * 80)
    print(f"✅ STREAMING COMPLETE - Total time: {total_time:.2f}s")
    print(f"   - LLM collection: {collection_time:.2f}s")
    print(f"   - Citation processing setup: {citation_processing_time:.2f}s")
    print(f"   - First enhanced chunk: {first_chunk_time:.2f}s")
    print(f"   - Complete streaming: {stream_complete_time:.2f}s")
    print("=" * 80)