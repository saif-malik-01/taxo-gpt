import faiss
import json
import numpy as np

class FAISSVectorStore:
    def __init__(self, index_path, chunks_path):
        self.index = faiss.read_index(index_path)
        with open(chunks_path, "r", encoding="utf-8") as f:
            self.chunks = json.load(f)

    def search(self, embedding, top_k=10):
        D, I = self.index.search(
            np.array([embedding]).astype("float32"),
            top_k
        )
        # Return list of (chunk, score) tuples
        results = []
        for j, i in enumerate(I[0]):
            if i != -1: # FAISS returns -1 if not enough neighbors
                results.append((self.chunks[i], float(D[0][j])))
        return results
