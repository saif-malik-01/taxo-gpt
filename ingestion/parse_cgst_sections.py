import uuid
import re
from docx import Document

CHAPTER_PATTERN = re.compile(r"^CHAPTER\s+[IVXLC]+", re.IGNORECASE)
NOTIFIED_PATTERN = re.compile(r"Notified date of Section:\s*(.*)", re.IGNORECASE)

SECTION_HEADER_1 = re.compile(
    r"^Section\s+(\d{1,3}[A-Z]?)\.\s+(.+?)[–-]"
)
SECTION_HEADER_2 = re.compile(
    r"^(\d{1,3}[A-Z])\.\s+(.+?)[–-]"
)

AMENDMENT_PATTERN = re.compile(r"^\[\d+\]|\bInserted by\b|\bSubstituted by\b|\bOmitted by\b", re.IGNORECASE)

def parse_cgst_act(docx_path):
    doc = Document(docx_path)
    paragraphs = [p.text.strip() for p in doc.paragraphs if p.text.strip()]

    sections = []
    current_section = None
    current_text = []
    current_chapter = "PRELIMINARY"
    notified_date = None
    seen_sections = set()

    def flush_section():
        if not current_section:
            return

        sections.append({
            "id": str(uuid.uuid4()),
            "section_number": current_section["number"],
            "section_title": current_section["title"],
            "chapter": current_section["chapter"],
            "notified_date": current_section["notified_date"],
            "text": "\n".join(current_text).strip(),
            "metadata": {
                "source": "CGST Act, 2017",
                "doc_type": "Act",
                "file_name": docx_path
            }
        })

    for line in paragraphs:

        # -------- CHAPTER --------
        if CHAPTER_PATTERN.match(line):
            current_chapter = line
            continue

        # -------- NOTIFIED DATE --------
        nd = NOTIFIED_PATTERN.search(line)
        if nd:
            notified_date = nd.group(1).strip()
            continue

        # -------- SECTION HEADER --------
        match = SECTION_HEADER_1.match(line) or SECTION_HEADER_2.match(line)
        if match:
            flush_section()

            section_no = match.group(1)
            section_title = match.group(2).strip()

            if section_no in seen_sections:
                current_section = None
                current_text = []
                continue

            seen_sections.add(section_no)

            current_section = {
                "number": section_no,
                "title": section_title,
                "chapter": current_chapter,
                "notified_date": notified_date
            }

            current_text = []
            notified_date = None
            continue

        # -------- SKIP AMENDMENT FOOTNOTES --------
        if AMENDMENT_PATTERN.match(line):
            continue

        # -------- NORMAL CONTENT --------
        if current_section:
            current_text.append(line)

    flush_section()
    return sections
