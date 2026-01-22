from sentence_transformers import SentenceTransformer
from services.retrieval.exact_match import exact_match

MODEL = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


def retrieve(query, vector_store, all_chunks, k=25):
    """
    Legal-grade hybrid retrieval:
    1. Exact statutory anchoring
    2. Vector expansion across ALL documents
    3. Legal-priority ordering
    """

    results = []

    # 1️⃣ EXACT MATCH
    exact = exact_match(query, all_chunks)
    if exact:
        results.extend(exact)

    # 2️⃣ VECTOR SEARCH
    embedding = MODEL.encode(query, normalize_embeddings=True)
    vector_hits = vector_store.search(embedding, top_k=50)

    seen = {c["id"] for c in results}

    for ch in vector_hits:
        if ch["id"] not in seen:
            results.append(ch)
            seen.add(ch["id"])

    # 3️⃣ LEGAL PRIORITY ORDER
    priority = (
        "definition",
        "operative",
        "rule",
        "notification",
        "circular",
        "judgment",
        "analytical_review",
    )

    ordered = []
    for p in priority:
        ordered.extend([c for c in results if c.get("chunk_type") == p])

    return ordered[:k]
