import faiss
import json
import numpy as np


class VectorStore:
    def __init__(self, index_path, chunks_path):
        self.index = faiss.read_index(index_path)
        with open(chunks_path, "r", encoding="utf-8") as f:
            self.chunks = json.load(f)

    def search(self, embedding, top_k=10):
        D, I = self.index.search(
            np.array([embedding]).astype("float32"),
            top_k
        )
        return [self.chunks[i] for i in I[0]]
