import uuid
import re

# Common judgment structure cues
SECTION_BREAK_PATTERN = re.compile(
    r"^(facts?|background|issues?|arguments?|submissions?|findings?|analysis|held|decision|conclusion)",
    re.IGNORECASE
)

def chunk_judgment(judgment, max_chars=1100):
    """
    Chunk judgments safely for legal reasoning.
    Judgments are non-statutory and must not override law.
    """

    text = judgment["text"]

    paragraphs = [
        p.strip() for p in text.split("\n")
        if p.strip()
    ]

    chunks = []
    buffer = ""

    for para in paragraphs:
        # Force break on logical section changes
        if (
            SECTION_BREAK_PATTERN.match(para)
            and buffer
        ):
            chunks.append({
                "id": str(uuid.uuid4()),
                "chunk_type": "judgment",
                "content_type": "judgment",
                "is_statutory": False,
                "text": buffer.strip(),
                "metadata": {
                    **judgment["metadata"],
                    "title": judgment["title"],
                    "external_id": judgment["external_id"]
                }
            })
            buffer = para
            continue

        # Size-based chunking fallback
        if len(buffer) + len(para) + 1 > max_chars:
            chunks.append({
                "id": str(uuid.uuid4()),
                "chunk_type": "judgment",
                "content_type": "judgment",
                "is_statutory": False,
                "text": buffer.strip(),
                "metadata": {
                    **judgment["metadata"],
                    "title": judgment["title"],
                    "external_id": judgment["external_id"]
                }
            })
            buffer = para
        else:
            buffer += " " + para if buffer else para

    if buffer:
        chunks.append({
            "id": str(uuid.uuid4()),
            "chunk_type": "judgment",
            "content_type": "judgment",
            "is_statutory": False,
            "text": buffer.strip(),
            "metadata": {
                **judgment["metadata"],
                "title": judgment["title"],
                "external_id": judgment["external_id"]
            }
        })

    return chunks
