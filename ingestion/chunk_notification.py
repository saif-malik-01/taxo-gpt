import uuid
import re

OPERATIVE_TRIGGER = re.compile(
    r"(hereby\s+notifies|hereby\s+exempts|hereby\s+specifies|hereby\s+amends)",
    re.IGNORECASE
)

SIGNATURE_BLOCK = re.compile(
    r"(yours\s+faithfully|by\s+order|sd/|-sd-)",
    re.IGNORECASE
)

CLAUSE_PREFIX = re.compile(r"^\(?\d+\)?")

def normalize_text(text: str) -> str:
    replacements = {
        " - ": "-",
        "  ": " ",
        "inter –state": "inter-state",
        "noti fication": "notification",
        "taxa ble": "taxable",
        "exemp tion": "exemption"
    }

    for k, v in replacements.items():
        text = text.replace(k, v)

    return " ".join(text.split())


def chunk_notification(notification: dict):
    lines = [l.strip() for l in notification["text"].split("\n") if l.strip()]

    chunks = []
    buffer = []
    collecting = False

    def flush():
        nonlocal buffer
        if not buffer:
            return

        text = normalize_text(" ".join(buffer))

        if len(text) < 120:
            buffer = []
            return

        chunks.append({
            "id": str(uuid.uuid4()),
            "chunk_type": "notification",
            "content_type": "notification",
            "is_statutory": True,
            "text": text,
            "metadata": {
                "source": "GST Notification",
                "notification_no": notification.get("notification_no"),
                "date": notification.get("date"),
                "subject": notification.get("subject"),
                "category": notification.get("category")
            }
        })

        buffer = []

    for line in lines:
        if SIGNATURE_BLOCK.search(line):
            flush()
            break

        if OPERATIVE_TRIGGER.search(line):
            flush()
            collecting = True
            buffer = [line]
            continue

        if collecting:
            if CLAUSE_PREFIX.match(line):
                buffer.append(line)
                continue
            buffer.append(line)

    flush()
    return chunks
