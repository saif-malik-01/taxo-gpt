import logging
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from services.chat.prompt_builder import build_structured_prompt

def test_prompt_generation():
    print("Testing Prompt Generation...")
    
    # Mock a complete judgment chunk as produced by create_complete_judgment_chunk
    mock_complete_chunk = {
        "id": "ext-123_complete",
        "text": "Citation: 2025 Taxo.online 455\nCase Number: W.P.No. 123/2025\nPetitioner: ABC Corp\n\nFull text of the judgment starts here...",
        "chunk_type": "judgment",
        "metadata": {
            "external_id": "ext-123",
            "citation": "2025 Taxo.online 455"
        },
        "_is_complete_judgment": True
    }
    
    query = "explain me the 2025 taxo.online 455 judgment"
    
    prompt = build_structured_prompt(
        query=query,
        primary=[mock_complete_chunk],
        supporting=[]
    )
    
    print("\n--- GENERATED PROMPT PREVIEW ---")
    print(prompt)
    
    if "Citation: 2025 Taxo.online 455" in prompt:
        print("\nSUCCESS: Citation found in prompt.")
    else:
        print("\nFAILURE: Citation NOT found in prompt.")

if __name__ == "__main__":
    test_prompt_generation()
