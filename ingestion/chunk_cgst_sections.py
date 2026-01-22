import re
import uuid

SUBSECTION_PATTERN = re.compile(r"^\((\d+[A-Z]?)\)")
CLAUSE_PATTERN = re.compile(r"^\(([a-z]+)\)")
EXPLANATION_PATTERN = re.compile(r"^Explanation", re.IGNORECASE)
PROVISO_PATTERN = re.compile(r"^Provided\s+that", re.IGNORECASE)

DEFINITION_PATTERN = re.compile(
    r'^\((\d+)\)\s*“([^”]+)”\s*means', re.IGNORECASE
)

def chunk_section(section):
    lines = section["text"].split("\n")

    chunks = []
    buffer = []
    subsection = None
    definition_term = None

    is_definitions_section = section["section_number"] == "2"

    def flush(chunk_type="operative"):
        if not buffer:
            return

        text = "\n".join(buffer).strip()
        if not text:
            return

        chunks.append({
            "id": str(uuid.uuid4()),
            "chunk_type": chunk_type,
            "act": "CGST",
            "section_number": section["section_number"],
            "section_title": section.get("section_title"),
            "chapter": section.get("chapter"),
            "subsection": subsection,
            "definition_term": definition_term,
            "text": text,
            "metadata": {
                "source": "CGST Act, 2017",
                "notified_date": section.get("notified_date"),
                "doc_type": "Act"
            }
        })

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # -------- SECTION 2 DEFINITIONS --------
        if is_definitions_section:
            def_match = DEFINITION_PATTERN.match(line)
            if def_match:
                flush(chunk_type="definition")
                subsection = def_match.group(1)
                definition_term = def_match.group(2).lower()
                buffer = [line]
                continue

        # -------- SUBSECTION --------
        sub_match = SUBSECTION_PATTERN.match(line)
        if sub_match:
            flush()
            subsection = sub_match.group(1)
            definition_term = None
            buffer = [line]
            continue

        # -------- CLAUSE / PROVISO / EXPLANATION --------
        if (
            CLAUSE_PATTERN.match(line)
            or EXPLANATION_PATTERN.match(line)
            or PROVISO_PATTERN.match(line)
        ):
            buffer.append(line)
            continue

        buffer.append(line)

    flush("definition" if is_definitions_section else "operative")
    return chunks
