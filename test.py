import json
from services.vector.store import VectorStore
from services.chat.engine import chat

print("\n🟢 GST CHATBOT (CMD MODE)")
print("Type 'exit' to quit\n")

# ---------- LOAD DATA ----------
with open("data/processed/all_chunks.json", "r", encoding="utf-8") as f:
    ALL_CHUNKS = json.load(f)

store = VectorStore(
    "data/vector_store/faiss.index",
    "data/processed/all_chunks.json"
)

# ---------- CLI LOOP ----------
while True:
    query = input("GST > ").strip()
    if query.lower() in ("exit", "quit"):
        break

    answer, sources = chat(query, store, ALL_CHUNKS)

    print("\n📘 ANSWER:\n")
    print(answer.strip())

    print("\n" + "-" * 60)
