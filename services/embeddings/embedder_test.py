from sentence_transformers import SentenceTransformer
import numpy as np
import faiss
import json

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

def build_index():
    with open("data/processed/notification_chunks.json", "r", encoding="utf-8") as f:
        chunks = json.load(f)

    texts = [c["text"] for c in chunks]

    model = SentenceTransformer(MODEL_NAME)
    embeddings = model.encode(texts, show_progress_bar=True)

    dim = embeddings.shape[1]
    index = faiss.IndexFlatL2(dim)
    index.add(np.array(embeddings).astype("float32"))

    faiss.write_index(index, "data/test/faiss.index")
    print(f"✅ FAISS index built for {len(texts)} chunks")

if __name__ == "__main__":
    build_index()
