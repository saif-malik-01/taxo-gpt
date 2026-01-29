import logging
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from services.retrieval.citation_matcher import normalize_citation, MetadataIndex, find_matching_judgments

# Mock logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_normalization():
    print("--- Testing Normalization ---")
    c1 = "2025 Taxo.online 455"
    c2 = "2025 taxo.online 455"
    n1 = normalize_citation(c1)
    n2 = normalize_citation(c2)
    print(f"'{c1}' -> '{n1}'")
    print(f"'{c2}' -> '{n2}'")
    assert n1 == n2 == "2025taxoonline455"
    print("Normalization passed.\n")

def test_indexing():
    print("--- Testing Indexing ---")
    mock_chunk = {
        "id": "test-chunk-1",
        "chunk_type": "judgment",
        "metadata": {
            "external_id": "ext-123",
            "citation": "2025 Taxo.online 455",
            "case_number": "W.P.No. 123/2025",
            "petitioner": "ABC Corp",
            "respondent": "State of XYZ"
        },
        "text": "Judgment text..."
    }
    
    chunks = [mock_chunk]
    
    print("Building index...")
    index = MetadataIndex(chunks)
    
    print(f"Index by citation keys: {index.by_citation.keys()}")
    
    target_cit = "2025 Taxo.online 455"
    norm_cit = normalize_citation(target_cit)
    
    matches = index.by_citation.get(norm_cit)
    print(f"Lookup for '{norm_cit}': {matches}")
    
    if matches:
        print("Success: Found chunk in index.")
    else:
        print("FAILURE: Chunk not found in index.")

def test_matching():
    print("\n--- Testing find_matching_judgments ---")
    mock_chunk = {
        "id": "test-chunk-1",
        "chunk_type": "judgment",
        "metadata": {
            "external_id": "ext-123",
            "citation": "2025 Taxo.online 455",
            "case_number": "W.P.No. 123/2025",
            "petitioner": "ABC Corp",
            "respondent": "State of XYZ"
        },
        "text": "Judgment text..."
    }
    chunks = [mock_chunk]
    
    # Needs to patch get_index to return our local index for this test, 
    # OR we rely on the fact that get_index calculates it fresh if not cached, 
    # BUT get_index is a singleton in the module.
    # We should reset the cache or mock it.
    
    import services.retrieval.citation_matcher as cm
    cm._INDEX_CACHE = None # Reset singleton
    
    extracted = {
        "citation": "2025 taxo.online 455",
        "case_numbers": [],
        "party_names": []
    }
    
    results = find_matching_judgments(extracted, chunks)
    print("Results:", results)
    
    exact = results.get("exact_matches", [])
    if exact and exact[0]['external_id'] == 'ext-123':
         print("Success: find_matching_judgments returned exact match.")
    else:
         print("FAILURE: find_matching_judgments did not return match.")

if __name__ == "__main__":
    test_normalization()
    test_indexing()
    test_matching()
