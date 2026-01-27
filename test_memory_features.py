import requests
import uuid

BASE_URL = "http://localhost:8000"

def test_memory():
    # 1. Setup - Create User and Login
    email = f"test_{uuid.uuid4().hex[:6]}@gst.com"
    password = "password123"
    print(f"Creating test user: {email}")
    
    reg_res = requests.post(f"{BASE_URL}/auth/register", json={"email": email, "password": password})
    token = reg_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    
    # 2. Test Short-term (Session) Memory
    session_id = str(uuid.uuid4())
    print("\n--- Testing Short-term Memory ---")
    
    # Message 1
    print("User: I am asking about GST rates for stationary.")
    res1 = requests.post(f"{BASE_URL}/chat/ask", headers=headers, json={
        "question": "I am asking about GST rates for stationary.",
        "session_id": session_id
    })
    print(f"AI: {res1.json()['answer'][:100]}...")
    
    # Message 2 - Context Dependent
    print("\nUser: What about books?")
    res2 = requests.post(f"{BASE_URL}/chat/ask", headers=headers, json={
        "question": "What about books?",
        "session_id": session_id
    })
    print(f"AI: {res2.json()['answer'][:100]}...")
    # Verification: If AI talks about GST rates for books, session memory is working.

    # 3. Test Long-term (Profile) Memory
    print("\n--- Testing Long-term Memory ---")
    
    # Set Profile
    print("Setting Profile: User is a business consultant from Kerala.")
    requests.put(f"{BASE_URL}/auth/profile", headers=headers, json={
        "dynamic_summary": "User is a business consultant from Kerala interested in export rules."
    })
    
    # Start NEW session
    new_session_id = str(uuid.uuid4())
    print("\nUser (New Session): What state-specific rules apply to me?")
    res3 = requests.post(f"{BASE_URL}/chat/ask", headers=headers, json={
        "question": "What state-specific rules apply to me?",
        "session_id": new_session_id
    })
    print(f"AI: {res3.json()['answer'][:200]}...")
    # Verification: If AI mentions Kerala or export rules, profile memory is working.

if __name__ == "__main__":
    test_memory()
