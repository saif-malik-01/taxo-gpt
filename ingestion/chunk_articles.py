import uuid

def chunk_article(article, max_chars=900):
    """
    Chunk GST articles for semantic search & explanation.
    Articles are non-statutory and should never override Acts/Rules.
    """

    text = article["text"]

    paragraphs = [
        p.strip() for p in text.split("\n")
        if p.strip()
    ]

    chunks = []
    buffer = ""

    for para in paragraphs:
        # Start new chunk if adding paragraph exceeds limit
        if len(buffer) + len(para) + 1 > max_chars:
            chunks.append({
                "id": str(uuid.uuid4()),
                "chunk_type": "article",
                "content_type": "article",
                "is_statutory": False,
                "text": buffer.strip(),
                "metadata": {
                    **article["metadata"],
                    "title": article["title"],
                    "external_id": article["external_id"]
                }
            })
            buffer = para
        else:
            buffer += " " + para if buffer else para

    if buffer:
        chunks.append({
            "id": str(uuid.uuid4()),
            "chunk_type": "article",
            "content_type": "article",
            "is_statutory": False,
            "text": buffer.strip(),
            "metadata": {
                **article["metadata"],
                "title": article["title"],
                "external_id": article["external_id"]
            }
        })

    return chunks
