import logging
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

# Mock Bedrock client to avoid AWS calls if possible, or use real one
# For now, let's see if regex fallback works or if we can use the real one.
# The user env has AWS credentials likely set up based on previous context.

from services.retrieval.citation_extractor import extract_citation_from_query

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_extraction():
    query = "explain me the 2025 taxo.online 455 judgment"
    print(f"Testing extraction for query: '{query}'")
    
    try:
        extracted = extract_citation_from_query(query)
        print("Extracted:", extracted)
        
        cit = extracted.get("citation")
        if cit and "2025" in cit and "455" in cit:
             print("Success: Extraction found the citation.")
        else:
             print("FAILURE: Extraction MISSED the citation.")
             
    except Exception as e:
        print(f"Error during extraction: {e}")

if __name__ == "__main__":
    test_extraction()
