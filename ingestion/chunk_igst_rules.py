import re
import uuid

SUBRULE_PATTERN = re.compile(r"^\((\d+[A-Z]?)\)")
CLAUSE_PATTERN = re.compile(r"^\(([a-z]+)\)")
ILLUSTRATION_PATTERN = re.compile(r"^Illustration", re.IGNORECASE)
PROVISO_PATTERN = re.compile(r"^Provided\s+that", re.IGNORECASE)

def chunk_igst_rule(rule):
    """
    Chunk IGST Rules safely:
    - Sub-rules split (1), (2), (3A)
    - Clauses + Illustrations stay inside sub-rule
    """

    lines = rule["text"].split("\n")

    chunks = []
    buffer = []
    subrule = None

    def flush():
        nonlocal buffer, subrule

        if not buffer:
            return

        text = "\n".join(buffer).strip()
        if not text:
            buffer = []
            return

        chunks.append({
            "id": str(uuid.uuid4()),
            "chunk_type": "operative",
            "act": "IGST",
            "rule_number": rule["rule_number"],
            "rule_title": rule["rule_title"],
            "subrule": subrule,
            "text": text,
            "metadata": {
                "source": "IGST Rules, 2017",
                "doc_type": "Rules"
            }
        })

        buffer = []
        subrule = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # -------- SUB-RULE --------
        sub_match = SUBRULE_PATTERN.match(line)
        if sub_match:
            flush()
            subrule = sub_match.group(1)
            buffer = [line]
            continue

        # -------- CLAUSE / ILLUSTRATION / PROVISO --------
        if (
            CLAUSE_PATTERN.match(line)
            or ILLUSTRATION_PATTERN.match(line)
            or PROVISO_PATTERN.match(line)
        ):
            buffer.append(line)
            continue

        buffer.append(line)

    flush()
    return chunks
