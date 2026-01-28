from sentence_transformers import SentenceTransformer
from services.retrieval.exact_match import exact_match
from services.retrieval.citation_extractor import extract_citation_from_query
from services.retrieval.citation_matcher import find_matching_judgments
import logging
import numpy as np
import copy

logger = logging.getLogger(__name__)

MODEL = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


def create_enriched_chunks_from_judgment(match_info):
    """
    Create enriched chunks with metadata prepended to each chunk
    
    Args:
        match_info: Dict containing matched_field, matched_value, citation, 
                    case_number, petitioner, respondent, chunks
    
    Returns:
        List of enriched chunks with prepended metadata
    """
    external_id = match_info["external_id"]
    original_chunks = match_info["chunks"]
    matched_field = match_info.get("matched_field", "")
    matched_value = match_info.get("matched_value", "")
    
    # Build metadata header
    header_parts = []
    
    # Show what matched first
    if matched_field == "citation":
        header_parts.append(f"Matched Citation: {matched_value}")
    elif matched_field == "case_number":
        header_parts.append(f"Matched Case Number: {matched_value}")
    elif matched_field == "petitioner":
        header_parts.append(f"Matched Petitioner: {matched_value}")
    elif matched_field == "respondent":
        header_parts.append(f"Matched Respondent: {matched_value}")
    elif matched_field == "both_parties":
        header_parts.append(f"Matched Both Parties: {matched_value}")
    
    # Add all available metadata (avoid duplication)
    citation = match_info.get('citation', '')
    case_number = match_info.get('case_number', '')
    petitioner = match_info.get('petitioner', '')
    respondent = match_info.get('respondent', '')
    
    if citation and matched_field != "citation":
        header_parts.append(f"Citation: {citation}")
    if case_number and matched_field != "case_number":
        header_parts.append(f"Case Number: {case_number}")
    if petitioner and matched_field not in ["petitioner", "both_parties"]:
        header_parts.append(f"Petitioner: {petitioner}")
    if respondent and matched_field not in ["respondent", "both_parties"]:
        header_parts.append(f"Respondent: {respondent}")
    
    header = "\n".join(header_parts) + "\n\n" if header_parts else ""
    
    logger.info(f"📋 Creating enriched chunks for {external_id}")
    logger.info(f"Metadata header:\n{header}")
    
    # Create enriched chunks
    enriched_chunks = []
    
    for i, chunk in enumerate(original_chunks):
        enriched_chunk = copy.deepcopy(chunk)
        
        # Prepend header to chunk text
        enriched_chunk["text"] = header + chunk["text"]
        enriched_chunk["_enriched_for_llm"] = True
        enriched_chunk["_original_text"] = chunk["text"]
        enriched_chunk["_match_score"] = match_info.get("score", 1.0)
        
        # Log first chunk preview
        if i == 0:
            preview = enriched_chunk["text"][:400]
            logger.info(f"✅ First enriched chunk preview:\n{preview}...")
        
        enriched_chunks.append(enriched_chunk)
    
    logger.info(f"Created {len(enriched_chunks)} enriched chunks for judgment {external_id}")
    
    return enriched_chunks


def retrieve(query, vector_store, all_chunks, k=25):
    """
    Enhanced hybrid retrieval with multi-field matching in metadata
    (citation, case_number, petitioner, respondent)
    """
    
    scored_results = {}  # chunk_id -> (chunk, final_score)
    seen_external_ids = set()
    
    # ========== STEP 1: Extract All Fields ==========
    extracted = extract_citation_from_query(query)
    logger.info(f"🔍 Extracted - Citation: '{extracted.get('citation')}', "
               f"Case#: '{extracted.get('case_number')}', "
               f"Parties: {extracted.get('party_names')}")
    
    # ========== STEP 2: Find Matches in Metadata ==========
    citation_matches = {"exact_matches": [], "partial_matches": [], "substring_matches": []}
    
    if (extracted.get("citation") or extracted.get("case_number") or 
        extracted.get("party_names")):
        citation_matches = find_matching_judgments(extracted, all_chunks)
    
    # ========== STEP 2a: EXACT Matches (score 1.0) ==========
    for match in citation_matches["exact_matches"]:
        external_id = match["external_id"]
        seen_external_ids.add(external_id)
        
        # Create enriched chunks with metadata prepended
        enriched_chunks = create_enriched_chunks_from_judgment(match)
        
        for chunk in enriched_chunks:
            chunk_id = chunk["id"]
            scored_results[chunk_id] = (chunk, 1.0)
        
        logger.info(f"✅ Added EXACT match judgment {external_id} ({len(enriched_chunks)} chunks) - "
                   f"Field: {match['matched_field']}, Value: '{match['matched_value']}'")
    
    # ========== STEP 2b: PARTIAL Matches (score 0.5) ==========
    for match in citation_matches["partial_matches"]:
        external_id = match["external_id"]
        
        if external_id in seen_external_ids:
            continue
        
        seen_external_ids.add(external_id)
        
        # Create enriched chunks
        enriched_chunks = create_enriched_chunks_from_judgment(match)
        
        for chunk in enriched_chunks:
            chunk_id = chunk["id"]
            scored_results[chunk_id] = (chunk, 0.5)
        
        logger.info(f"⚠️  Added PARTIAL match judgment {external_id} ({len(enriched_chunks)} chunks) - "
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
    vector_hits = vector_store.search(embedding, top_k=50) # Now returns (chunk, score)
    
    vector_count = 0
    for chunk, dist in vector_hits:
        chunk_id = chunk["id"]
        external_id = chunk.get("metadata", {}).get("external_id")
        
        if external_id in seen_external_ids:
            continue
        
        if chunk_id not in scored_results:
            # L2 distance conversion to similarity score (approximate)
            # If using IndexFlatIP, dist is already similarity
            # If using IndexFlatL2, we might want inverse or 1/(1+dist)
            # Assuming IndexFlatL2 as per store.py:
            base_score = 1 / (1 + dist) 
            
            boost = substring_weights.get(chunk_id, 0)
            scored_results[chunk_id] = (chunk, base_score + boost)
            vector_count += 1
    
    logger.info(f"Added {vector_count} vector search results")
    
    # ========== STEP 4: Scoring Adjustments ==========
    # (Removed redundant re-encoding of matching chunks to save CPU/Event Loop)
    pass
    
    # ========== STEP 5: Sort by Score ==========
    sorted_results = sorted(scored_results.values(), key=lambda x: x[1], reverse=True)
    
    logger.info(f"Total chunks scored: {len(sorted_results)}")
    if sorted_results:
        logger.info(f"Score range: {sorted_results[0][1]:.3f} to {sorted_results[-1][1]:.3f}")
    
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