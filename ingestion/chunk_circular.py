import uuid
import re

# ---------- PATTERNS ----------
CLARIFICATION_TRIGGER = re.compile(
    r"(it\s+is\s+hereby\s+clarified\s+that|it\s+is\s+clarified\s+that|in\s+view\s+of\s+the\s+above)",
    re.IGNORECASE
)

QUESTION_LINE = re.compile(
    r"^\s*\d+\.\s*Is\s+", re.IGNORECASE
)

SIGNATURE_BLOCK = re.compile(
    r"(yours\s+faithfully|sd/|-sd-)",
    re.IGNORECASE
)

POINT_PREFIX = re.compile(r"^\(?[a-z0-9]\)", re.IGNORECASE)


# ---------- CLEANER ----------
def normalize_text(text: str) -> str:
    replacements = {
        " - ": "-",
        "  ": " ",
        "bean s": "beans",
        "j aggery": "jaggery",
        "de - husked": "dehusked",
        "inter –state": "inter-state",
        "noti fication": "notification",
        "person s": "persons"
    }

    for k, v in replacements.items():
        text = text.replace(k, v)

    return " ".join(text.split())


# ---------- CHUNKER ----------
def chunk_circular(circular: dict):
    lines = [l.strip() for l in circular["text"].split("\n") if l.strip()]

    chunks = []
    buffer = []
    collecting = False

    def flush():
        nonlocal buffer
        if not buffer:
            return

        text = normalize_text(" ".join(buffer))

        # Skip tiny / junk chunks
        if len(text) < 120:
            buffer = []
            return

        chunks.append({
            "id": str(uuid.uuid4()),
            "chunk_type": "circular",
            "content_type": "circular",
            "is_statutory": True,
            "text": text,
            "metadata": {
                "source": "GST Circular",
                "circular_no": circular.get("circular_no"),
                "date": circular.get("date"),
                "subject": circular.get("subject"),
                "category": circular.get("category")
            }
        })

        buffer = []

    for line in lines:
        # Stop at signature
        if SIGNATURE_BLOCK.search(line):
            flush()
            break

        # Ignore question headings
        if QUESTION_LINE.match(line):
            flush()
            collecting = False
            continue

        # Start of clarification
        if CLARIFICATION_TRIGGER.search(line):
            flush()
            collecting = True
            buffer = [line]
            continue

        # Continue clarification block
        if collecting:
            # Skip pure numbering
            if POINT_PREFIX.match(line):
                buffer.append(line)
                continue

            buffer.append(line)

    flush()
    return chunks
