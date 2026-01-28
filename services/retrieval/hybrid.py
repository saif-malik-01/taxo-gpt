from sentence_transformers import SentenceTransformer
from services.retrieval.exact_match import exact_match
from services.retrieval.citation_extractor import extract_citation_from_query
from services.retrieval.citation_matcher import find_matching_judgments
import logging
import numpy as np

logger = logging.getLogger(__name__)

MODEL = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


def create_complete_judgment_chunk(match_info, all_chunks):
    """
    Create a SINGLE chunk containing the complete judgment with metadata prepended
    
    This chunk will be treated like any other chunk in retrieval results
    
    Args:
        match_info: Dict from find_matching_judgments
        all_chunks: All chunks from database
    
    Returns:
        Single chunk dict with complete judgment text
    """
    external_id = match_info["external_id"]
    
    # Get ALL chunks for this judgment from original all_chunks
    judgment_chunks = [
        c for c in all_chunks
        if c.get("chunk_type") == "judgment" and 
           c.get("metadata", {}).get("external_id") == external_id
    ]
    
    if not judgment_chunks:
        logger.warning(f"No chunks found for judgment {external_id}")
        return None
    
    # Build complete judgment text
    judgment_texts = [chunk.get('text', '') for chunk in judgment_chunks]
    complete_judgment_text = "\n\n".join(judgment_texts)
    
    # Build metadata header
    citation = match_info.get('citation', '')
    case_number = match_info.get('case_number', '')
    petitioner = match_info.get('petitioner', '')
    respondent = match_info.get('respondent', '')
    
    header_parts = []
    if citation:
        header_parts.append(f"Citation: {citation}")
    if case_number:
        header_parts.append(f"Case Number: {case_number}")
    if petitioner:
        header_parts.append(f"Petitioner: {petitioner}")
    if respondent:
        header_parts.append(f"Respondent: {respondent}")
    
    header = "\n".join(header_parts) + "\n\n" if header_parts else ""
    
    # Create the complete chunk with prepended metadata
    complete_text = header + complete_judgment_text
    
    # Use the first chunk as a template
    first_chunk = judgment_chunks[0]
    
    complete_chunk = {
        "id": f"{external_id}_complete",  # Unique ID
        "text": complete_text,  # COMPLETE JUDGMENT WITH METADATA
        "chunk_type": "judgment",
        "metadata": first_chunk.get("metadata", {}),
        "_is_complete_judgment": True,
        "_match_score": match_info.get("score", 1.0),
        "_matched_field": match_info.get("matched_field", ""),
        "_matched_value": match_info.get("matched_value", ""),
        "_external_id": external_id
    }
    
    logger.info(f"✅ Created complete judgment chunk for {external_id}")
    logger.info(f"   Citation: {citation}")
    logger.info(f"   Case Number: {case_number}")
    logger.info(f"   Petitioner: {petitioner}")
    logger.info(f"   Respondent: {respondent}")
    logger.info(f"   Complete text length: {len(complete_text)} chars")
    logger.info(f"   Header preview:\n{header}")
    
    return complete_chunk


def retrieve(query, vector_store, all_chunks, k=25):
    """
    Enhanced hybrid retrieval with exact judgment matching
    """
    
    scored_results = {}  # chunk_id -> (chunk, final_score)
    seen_external_ids = set()
    
    # ========== STEP 1: Extract All Fields ==========
    extracted = extract_citation_from_query(query)
    logger.info(f"🔍 Extracted - Citation: '{extracted.get('citation')}', "
               f"Case#: '{extracted.get('case_number')}', "
               f"Parties: {extracted.get('party_names')}")
    
    # ========== STEP 2: Find Exact/Partial Matches in Metadata ==========
    citation_matches = {"exact_matches": [], "partial_matches": [], "substring_matches": []}
    
    if (extracted.get("citation") or extracted.get("case_number") or 
        extracted.get("party_names")):
        citation_matches = find_matching_judgments(extracted, all_chunks)
    
    # ========== STEP 2a: EXACT Matches (score 1.0) ==========
    # Create complete judgment chunks for each exact match
    
    for match in citation_matches["exact_matches"]:
        external_id = match["external_id"]
        seen_external_ids.add(external_id)
        
        # Create ONE complete judgment chunk
        complete_chunk = create_complete_judgment_chunk(match, all_chunks)
        
        if complete_chunk:
            chunk_id = complete_chunk["id"]
            scored_results[chunk_id] = (complete_chunk, 1.0)
            
            logger.info(f"✅ Added EXACT match judgment {external_id} as COMPLETE chunk - "
                       f"Field: {match['matched_field']}, Value: '{match['matched_value']}'")
    
    # ========== STEP 2b: PARTIAL Matches (score 0.65) ==========
    # Also create complete judgment chunks for partial matches
    
    for match in citation_matches["partial_matches"]:
        external_id = match["external_id"]
        
        if external_id in seen_external_ids:
            continue
        
        seen_external_ids.add(external_id)
        
        # Create complete judgment chunk
        complete_chunk = create_complete_judgment_chunk(match, all_chunks)
        
        if complete_chunk:
            chunk_id = complete_chunk["id"]
            scored_results[chunk_id] = (complete_chunk, 0.65)
            
            logger.info(f"⚠️  Added PARTIAL match judgment {external_id} as COMPLETE chunk - "
                       f"Field: {match['matched_field']}, Value: '{match['matched_value']}'")
    
    # ========== STEP 2c: Substring Weights (score 0.1) ==========
    substring_weights = {}
    for match in citation_matches["substring_matches"]:
        chunk = match["chunk"]
        external_id = chunk.get("metadata", {}).get("external_id")
        
        if external_id not in seen_external_ids:
            substring_weights[chunk["id"]] = 0.1
    
    # ========== STEP 3: Regular Retrieval ==========
    
    # STEP 3a: Exact Statutory Matches
    exact = exact_match(query, all_chunks)
    if exact:
        for chunk in exact:
            chunk_id = chunk["id"]
            external_id = chunk.get("metadata", {}).get("external_id")
            
            if external_id in seen_external_ids:
                continue
            
            if chunk_id not in scored_results:
                base_score = 0.95
                boost = substring_weights.get(chunk_id, 0)
                scored_results[chunk_id] = (chunk, base_score + boost)
        
        logger.info(f"Added {len(exact)} exact statutory matches")
    
    # STEP 3b: Vector Search
    embedding = MODEL.encode(query, normalize_embeddings=True)
    vector_hits = vector_store.search(embedding, top_k=50)
    
    vector_count = 0
    for chunk in vector_hits:
        chunk_id = chunk["id"]
        external_id = chunk.get("metadata", {}).get("external_id")
        
        # Skip if this judgment already matched exactly/partially
        if external_id in seen_external_ids:
            continue
        
        if chunk_id not in scored_results:
            chunk_embedding = MODEL.encode(chunk["text"], normalize_embeddings=True)
            base_score = float(np.dot(embedding, chunk_embedding))
            base_score = max(0.0, min(1.0, base_score))
            
            boost = substring_weights.get(chunk_id, 0)
            scored_results[chunk_id] = (chunk, base_score + boost)
            vector_count += 1
    
    logger.info(f"Added {vector_count} vector search results")
    
    # ========== STEP 4: Add Semantic Score to Exact/Partial Complete Judgments ==========
    # Calculate semantic similarity for complete judgment chunks
    
    for chunk_id, (chunk, current_score) in list(scored_results.items()):
        # If this is a complete judgment chunk (exact or partial match)
        if chunk.get("_is_complete_judgment"):
            # Get the text for embedding (use the complete text)
            text_for_embedding = chunk["text"]
            
            # Calculate semantic similarity
            chunk_embedding = MODEL.encode(text_for_embedding, normalize_embeddings=True)
            semantic_score = float(np.dot(embedding, chunk_embedding))
            semantic_score = max(0.3, min(1.0, semantic_score))
            
            # Final score = semantic + match_boost
            # For exact matches: 1.0 + semantic
            # For partial matches: 0.65 + semantic
            match_boost = chunk.get("_match_score", current_score)
            final_score = semantic_score + match_boost
            
            scored_results[chunk_id] = (chunk, final_score)
            
            logger.debug(f"Complete judgment {chunk_id}: semantic={semantic_score:.3f}, "
                        f"boost={match_boost}, final={final_score:.3f}")
    
    # ========== STEP 5: Sort by Score ==========
    sorted_results = sorted(scored_results.values(), key=lambda x: x[1], reverse=True)
    
    logger.info(f"Total chunks scored: {len(sorted_results)}")
    if sorted_results:
        logger.info(f"Score range: {sorted_results[0][1]:.3f} to {sorted_results[-1][1]:.3f}")
        
        # Log top 5 chunks
        logger.info("Top 5 chunks:")
        for i, (chunk, score) in enumerate(sorted_results[:5]):
            is_complete = chunk.get("_is_complete_judgment", False)
            chunk_type = chunk.get("chunk_type", "unknown")
            preview = chunk["text"][:100].replace("\n", " ")
            logger.info(f"  {i+1}. Score={score:.3f}, Type={chunk_type}, "
                       f"Complete={is_complete}, Preview: {preview}...")
    
    # ========== STEP 6: Apply Legal Hierarchy ==========
    final_results = apply_legal_hierarchy(sorted_results)
    
    logger.info(f"Returning top {min(k, len(final_results))} out of {len(final_results)} chunks")
    
    return final_results[:k]


def apply_legal_hierarchy(sorted_results):
    """
    Apply legal priority ordering to chunks with similar scores
    Groups chunks with scores within 0.15 of each other
    """
    
    priority_order = [
        "judgment",
        "definition",
        "operative",
        "rule",
        "notification",
        "circular",
        "analytical_review"
    ]
    
    if len(sorted_results) <= 1:
        return [chunk for chunk, score in sorted_results]
    
    result = []
    i = 0
    
    while i < len(sorted_results):
        current_chunk, current_score = sorted_results[i]
        
        # Find all chunks with similar scores (within 0.15)
        similar_group = [current_chunk]
        j = i + 1
        
        while j < len(sorted_results):
            next_chunk, next_score = sorted_results[j]
            if abs(current_score - next_score) <= 0.15:
                similar_group.append(next_chunk)
                j += 1
            else:
                break
        
        # Sort similar group by legal priority
        similar_group.sort(
            key=lambda c: priority_order.index(c.get("chunk_type", "analytical_review"))
            if c.get("chunk_type") in priority_order else 99
        )
        
        result.extend(similar_group)
        i = j
    
    return result