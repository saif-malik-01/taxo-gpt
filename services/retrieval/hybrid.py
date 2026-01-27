# from sentence_transformers import SentenceTransformer
# from services.retrieval.exact_match import exact_match

# MODEL = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


# def retrieve(query, vector_store, all_chunks, k=25):
#     """
#     Legal-grade hybrid retrieval:
#     1. Exact statutory anchoring
#     2. Vector expansion across ALL documents
#     3. Legal-priority ordering
#     """

#     results = []

#     # 1️⃣ EXACT MATCH
#     exact = exact_match(query, all_chunks)
#     if exact:
#         results.extend(exact)

#     # 2️⃣ VECTOR SEARCH
#     embedding = MODEL.encode(query, normalize_embeddings=True)
#     vector_hits = vector_store.search(embedding, top_k=50)

#     seen = {c["id"] for c in results}

#     for ch in vector_hits:
#         if ch["id"] not in seen:
#             results.append(ch)
#             seen.add(ch["id"])

#     # 3️⃣ LEGAL PRIORITY ORDER
#     priority = (
#         "definition",
#         "operative",
#         "rule",
#         "notification",
#         "circular",
#         "judgment",
#         "analytical_review",
#     )

#     ordered = []
#     for p in priority:
#         ordered.extend([c for c in results if c.get("chunk_type") == p])

#     return ordered[:k]


from sentence_transformers import SentenceTransformer
from services.retrieval.exact_match import exact_match
from services.retrieval.citation_extractor import extract_citation_from_query
from services.retrieval.citation_matcher import find_matching_judgments
import logging
import numpy as np

logger = logging.getLogger(__name__)

MODEL = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


def retrieve(query, vector_store, all_chunks, k=25):
    """
    Enhanced hybrid retrieval with citation and case number matching
    
    Flow:
    1. Extract citation and case number from query
    2. Find exact matches (citation OR case_number) → extract complete judgment
    3. Find substring matches → create weight map (+0.3)
    4. Regular retrieval (exact statutory + vector search)
    5. Apply citation weights to scores
    6. Re-rank and return top K
    """
    
    scored_results = {}  # chunk_id -> (chunk, final_score)
    seen_external_ids = set()  # Track judgments already included via exact match
    
    # ========== STEP 1: Extract Citation & Case Number ==========
    extracted = extract_citation_from_query(query)
    logger.info(f"Extracted from query - Citation: {extracted.get('citation')}, "
               f"Case Number: {extracted.get('case_number')}")
    
    # ========== STEP 2: Find Citation/Case Number Matches ==========
    citation_matches = {"exact_matches": [], "substring_matches": []}
    
    if extracted.get("citation") or extracted.get("case_number"):
        citation_matches = find_matching_judgments(extracted, all_chunks)
    
    # ========== STEP 2a: Add EXACT Matched Complete Judgments ==========
    for match in citation_matches["exact_matches"]:
        external_id = match["external_id"]
        seen_external_ids.add(external_id)
        
        # Add ALL chunks of this judgment with exact match boost
        for chunk in match["chunks"]:
            chunk_id = chunk["id"]
            # Exact match boost = 1.0 (will add base score later)
            scored_results[chunk_id] = (chunk, 1.0)
        
        logger.info(f"Added complete judgment {external_id} "
                   f"({len(match['chunks'])} chunks) via exact {match['matched_field']} match")
    
    # ========== STEP 2b: Create Substring Weight Map ==========
    substring_weights = {}
    for match in citation_matches["substring_matches"]:
        chunk = match["chunk"]
        external_id = chunk.get("metadata", {}).get("external_id")
        
        # Only add substring weight if not already exact matched
        if external_id not in seen_external_ids:
            substring_weights[chunk["id"]] = 0.3
            logger.debug(f"Substring match weight 0.3 for chunk {chunk['id']}")
    
    # ========== STEP 3: Regular Retrieval ==========
    
    # STEP 3a: Exact Statutory Matches (Section/Rule/etc)
    exact = exact_match(query, all_chunks)
    if exact:
        for chunk in exact:
            chunk_id = chunk["id"]
            external_id = chunk.get("metadata", {}).get("external_id")
            
            # Skip if part of exact citation match
            if external_id in seen_external_ids:
                continue
            
            if chunk_id not in scored_results:
                base_score = 0.95  # High score for exact statutory
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
        
        # Skip if part of exact citation match
        if external_id in seen_external_ids:
            continue
        
        if chunk_id not in scored_results:
            # Get similarity score (assuming vector_store returns it)
            # If not available, compute it
            chunk_embedding = MODEL.encode(chunk["text"], normalize_embeddings=True)
            base_score = float(np.dot(embedding, chunk_embedding))
            base_score = max(0.0, min(1.0, base_score))  # Clamp to [0, 1]
            
            boost = substring_weights.get(chunk_id, 0)
            scored_results[chunk_id] = (chunk, base_score + boost)
            vector_count += 1
    
    logger.info(f"Added {vector_count} vector search results")
    
    # ========== STEP 4: Add Base Semantic Score to Exact Citation Matches ==========
    # For chunks added via exact citation match (boost=1.0), calculate their semantic score
    for chunk_id, (chunk, current_score) in list(scored_results.items()):
        external_id = chunk.get("metadata", {}).get("external_id")
        
        # If this was exact citation match (score=1.0 and in seen_external_ids)
        if external_id in seen_external_ids and current_score == 1.0:
            # Calculate semantic similarity
            chunk_embedding = MODEL.encode(chunk["text"], normalize_embeddings=True)
            semantic_score = float(np.dot(embedding, chunk_embedding))
            semantic_score = max(0.3, min(1.0, semantic_score))  # Clamp to [0.3, 1.0]
            
            # Final score = semantic_score + exact_match_boost(1.0)
            scored_results[chunk_id] = (chunk, semantic_score + 1.0)
    
    # ========== STEP 5: Sort by Final Score ==========
    sorted_results = sorted(scored_results.values(), key=lambda x: x[1], reverse=True)
    
    logger.info(f"Total chunks scored: {len(sorted_results)}")
    if sorted_results:
        logger.info(f"Top score: {sorted_results[0][1]:.3f}, "
                   f"Bottom score: {sorted_results[-1][1]:.3f}")
    
    # ========== STEP 6: Apply Legal Hierarchy ==========
    # Within similar score ranges (±0.15), apply legal priority
    final_results = apply_legal_hierarchy(sorted_results)
    
    logger.info(f"Returning top {min(k, len(final_results))} out of {len(final_results)} chunks")
    
    return final_results[:k]


def apply_legal_hierarchy(sorted_results):
    """
    Apply legal priority ordering to chunks with similar scores
    Groups chunks with scores within 0.15 of each other and sorts by legal priority
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