import httpx
import json
import asyncio
import sys

# Replace with your local server URL
URL = "http://localhost:8000/chat/ask/stream"

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhZG1pbkBnc3QuY29tIiwicm9sZSI6InVzZXIiLCJleHAiOjE3Njk1MzUxOTd9.vc6Ly221o4MmLZ9bUnxm5JF7aZxqkHa08lTWxIh1Uzk"
}

PAYLOAD = {
    "question": "What is GST? Explain briefly.",
    "session_id": "test-session-123"
}

async def test_stream():
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            print(f"Connecting to {URL}...")
            async with client.stream("POST", URL, json=PAYLOAD, headers=HEADERS) as response:
                response.raise_for_status()
                
                print("--- STREAMING RESPONSE START ---")
                async for line in response.aiter_lines():
                    if line:
                        try:
                            chunk = json.loads(line)
                            if chunk["type"] == "retrieval":
                                print(f"\n[RETRIEVAL] Found {len(chunk['sources'])} sources.")
                            elif chunk["type"] == "content":
                                print(chunk["delta"], end="", flush=True)
                        except json.JSONDecodeError:
                            print(f"\n[RAW] {line}")
                print("\n--- STREAMING RESPONSE FINISHED ---")
    except Exception as e:
        print(f"\nError: {e}")

if __name__ == "__main__":
    asyncio.run(test_stream())
