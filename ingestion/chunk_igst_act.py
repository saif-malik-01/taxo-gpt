import re
import uuid

SUBSECTION_PATTERN = re.compile(r"^\((\d+[A-Z]?)\)")

DEFINITION_PATTERN = re.compile(
    r'^\((\d+)\)\s*“([^”]+)”\s*means',
    re.IGNORECASE
)

INTRO_LINE_PATTERN = re.compile(
    r"unless the context otherwise requires",
    re.IGNORECASE
)

def chunk_igst_section(section):
    lines = section["text"].split("\n")

    chunks = []
    buffer = []
    subsection = None
    definition_term = None

    is_definition_section = section["section_number"] == "2"

    def flush(chunk_type):
        nonlocal buffer, subsection, definition_term

        if not buffer:
            return

        text = "\n".join(buffer).strip()
        if not text:
            buffer = []
            return

        # 🚫 Skip definition intro line
        if INTRO_LINE_PATTERN.search(text):
            buffer = []
            return

        chunks.append({
            "id": str(uuid.uuid4()),
            "chunk_type": chunk_type,
            "act": "IGST",
            "section_number": section["section_number"],
            "section_title": section.get("section_title"),
            "chapter": section.get("chapter"),
            "subsection": subsection,
            "definition_term": definition_term,
            "text": text,
            "metadata": {
                "source": "IGST Act, 2017",
                "notified_date": section.get("notified_date"),
                "doc_type": "Act"
            }
        })

        buffer = []
        subsection = None
        definition_term = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # -------- DEFINITIONS (Section 2) --------
        if is_definition_section:
            def_match = DEFINITION_PATTERN.match(line)
            if def_match:
                flush("definition")
                subsection = def_match.group(1)
                definition_term = def_match.group(2).lower()
                buffer = [line]
                continue

        # -------- SUBSECTION --------
        sub_match = SUBSECTION_PATTERN.match(line)
        if sub_match:
            flush("definition" if is_definition_section else "operative")
            subsection = sub_match.group(1)
            buffer = [line]
            continue

        buffer.append(line)

    flush("definition" if is_definition_section else "operative")
    return chunks
