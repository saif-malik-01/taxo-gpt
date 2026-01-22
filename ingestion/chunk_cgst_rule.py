import re
import uuid

# Sub-rule like (1), (2), (3A)
SUBRULE_PATTERN = re.compile(r"^\((\d+[A-Z]?)\)")

# Definition clause like (a), (b)
DEF_CLAUSE_PATTERN = re.compile(r"^\(([a-z])\)\s*“([^”]+)”", re.IGNORECASE)

CLAUSE_PATTERN = re.compile(r"^\(([a-z]+)\)")
PROVISO_PATTERN = re.compile(r"^Provided\s+that", re.IGNORECASE)
EXPLANATION_PATTERN = re.compile(r"^Explanation", re.IGNORECASE)

INTRO_LINE_PATTERN = re.compile(
    r"unless the context otherwise requires", re.IGNORECASE
)

def chunk_rule(rule):
    lines = rule["text"].split("\n")

    chunks = []
    buffer = []
    subrule = None
    definition_term = None

    is_definition_rule = rule.get("rule_number") == "2"

    def flush(chunk_type):
        nonlocal buffer, subrule, definition_term

        if not buffer:
            return

        text = "\n".join(buffer).strip()
        if not text:
            return

        # Skip Rule 2 opening line
        if INTRO_LINE_PATTERN.search(text):
            buffer = []
            return

        chunks.append({
            "id": str(uuid.uuid4()),
            "chunk_type": chunk_type,
            "act": "CGST",
            "rule_number": rule["rule_number"],
            "rule_title": rule["rule_title"],
            "subrule": subrule,
            "definition_term": definition_term,
            "text": text,
            "metadata": {
                "source": "CGST Rules, 2017",
                "doc_type": "Rules"
            }
        })

        buffer = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # -------- RULE 2 DEFINITIONS --------
        if is_definition_rule:
            def_match = DEF_CLAUSE_PATTERN.match(line)
            if def_match:
                flush("definition")
                subrule = def_match.group(1)
                definition_term = def_match.group(2).lower()
                buffer = [line]
                continue

        # -------- SUB-RULE (incl. 3A) --------
        sub_match = SUBRULE_PATTERN.match(line)
        if sub_match:
            flush("definition" if is_definition_rule else "operative")
            subrule = sub_match.group(1)
            definition_term = None
            buffer = [line]
            continue

        # -------- CLAUSE / PROVISO / EXPLANATION --------
        if (
            CLAUSE_PATTERN.match(line)
            or PROVISO_PATTERN.match(line)
            or EXPLANATION_PATTERN.match(line)
        ):
            buffer.append(line)
            continue

        buffer.append(line)

    flush("definition" if is_definition_rule else "operative")
    return chunks
