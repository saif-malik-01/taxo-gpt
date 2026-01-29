import logging
import sys
import os
import json
import time

# Add project root to path
sys.path.append(os.getcwd())

from services.retrieval.citation_matcher import get_index, find_matching_judgments, normalize_citation
from services.retrieval.citation_extractor import extract_citation_from_query

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_chunks():
    print("Loading chunks (this might take a moment)...")
    # Load first N chunks or search for specific one to save time?
    # Ideally load all to match prod environment, but 400MB is large for quick script.
    # Let's load all to be sure.
    start = time.time()
    with open('data/processed/all_chunks.json', 'r', encoding='utf-8') as f:
        chunks = json.load(f)
    print(f"Loaded {len(chunks)} chunks in {time.time() - start:.2f}s")
    return chunks

def debug_full_flow():
    query = "explain me the 2025 taxo.online 455 judgment"
    
    # 1. Load Data
    chunks = load_chunks()
    
    # 2. Build Index
    print("Building Index...")
    start = time.time()
    index = get_index(chunks)
    print(f"Index built in {time.time() - start:.2f}s")
    
    # Debug: Check if citation exists in index
    target = "2025 Taxo.online 455"
    norm = normalize_citation(target)
    print(f"Checking index for '{target}' (norm: '{norm}')...")
    
    match_list = index.by_citation.get(norm)
    if match_list:
        print(f"✅ Found {len(match_list)} chunks in index for this citation.")
        print(f"   First match external_id: {match_list[0].get('metadata', {}).get('external_id')}")
    else:
        print("❌ NOT FOUND in index by_citation!")
        
        # fallback search to see why
        print("Searching manually in chunks...")
        found = False
        for c in chunks:
            if c.get("chunk_type") == "judgment":
                cit = c.get("metadata", {}).get("citation", "")
                if cit and normalize_citation(cit) == norm:
                    print(f"   Found manual match! ID: {c.get('id')}, Cit: {cit}")
                    found = True
                    break
        if not found:
            print("   Also not found manually in chunks with matching normalization.")

    # 3. Extract
    print("\nExtracting from query...")
    extracted = extract_citation_from_query(query)
    print(f"Extracted: {extracted}")
    
    # 4. Match
    print("\nFinding Matching Judgments...")
    results = find_matching_judgments(extracted, chunks)
    print(f"Results keys: {results.keys()}")
    
    if results["exact_matches"]:
        print(f"✅ EXACT MATCH FOUND: {len(results['exact_matches'])}")
        print(results["exact_matches"][0])
    else:
        print("❌ NO EXACT MATCH FOUND")

if __name__ == "__main__":
    debug_full_flow()
