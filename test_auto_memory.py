import requests
import uuid
import time

BASE_URL = "http://localhost:8000"

def test_auto_memory():
    # 1. Setup - Create User and Login
    email = f"auto_test_{uuid.uuid4().hex[:6]}@gst.com"
    password = "password123"
    print(f"Creating test user: {email}")
    
    reg_res = requests.post(f"{BASE_URL}/auth/register", json={"email": email, "password": password})
    token = reg_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    
    # 2. Step 1: Tell the AI a fact naturally
    session_id_1 = str(uuid.uuid4())
    print("\n[Session 1] User: Hi, I'm a pharmaceutical distributor from Ahmedabad.")
    res1 = requests.post(f"{BASE_URL}/chat/ask", headers=headers, json={
        "question": "Hi, I'm a pharmaceutical distributor from Ahmedabad. What GST rate applies to insulin?",
        "session_id": session_id_1
    })
    print(f"AI: {res1.json()['answer'][:100]}...")
    
    print("\nWaiting 2 seconds for background memory extraction...")
    time.sleep(2) # Give background task time to finish

    # 3. Step 2: Start a NEW session and ask a vague question
    session_id_2 = str(uuid.uuid4())
    print("\n[Session 2 - NEW] User: Based on my business, what are my main compliance tasks?")
    res2 = requests.post(f"{BASE_URL}/chat/ask", headers=headers, json={
        "question": "Based on my business and location, what are my main compliance tasks?",
        "session_id": session_id_2
    })
    
    answer = res2.json()['answer']
    print(f"AI: {answer[:250]}...")
    
    # Verification
    if "Ahmedabad" in answer or "pharmaceutical" in answer or "medicine" in answer:
        print("\n✅ SUCCESS: Automatic Long-term Memory working! AI remembered your business and location across sessions.")
    else:
        print("\n❌ FAILURE: AI did not pick up the facts automatically. Check background tasks.")

if __name__ == "__main__":
    test_auto_memory()
