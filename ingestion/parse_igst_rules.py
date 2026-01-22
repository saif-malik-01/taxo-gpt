import re
import uuid
from docx import Document

RULE_HEADER = re.compile(
    r"^\[?Rule\s+(\d+[A-Z]?)\.\s*(.+?)\]?$",
    re.IGNORECASE
)

AMENDMENT_BLOCK = re.compile(
    r"^Reference to Amendments", re.IGNORECASE
)

def parse_igst_rules(docx_path):
    doc = Document(docx_path)

    rules = []
    current_rule = None
    buffer = []
    skip_amendments = False

    def flush():
        if not current_rule:
            return

        rules.append({
            "id": str(uuid.uuid4()),
            "act": "IGST",
            "rule_number": current_rule["rule_number"],
            "rule_title": current_rule["rule_title"],
            "text": "\n".join(buffer).strip(),
            "metadata": {
                "source": "IGST Rules, 2017",
                "doc_type": "Rules",
                "file_name": docx_path
            }
        })

    for para in doc.paragraphs:
        text = para.text.strip()
        if not text:
            continue

        # -------- START AMENDMENT BLOCK --------
        if AMENDMENT_BLOCK.match(text):
            skip_amendments = True
            continue

        # -------- SKIP AMENDMENT CONTENT --------
        if skip_amendments:
            if RULE_HEADER.match(text):
                skip_amendments = False
            else:
                continue

        # -------- RULE HEADER --------
        match = RULE_HEADER.match(text)
        if match:
            flush()

            rule_no, title = match.groups()
            current_rule = {
                "rule_number": rule_no,
                "rule_title": title.strip()
            }
            buffer = []
            continue

        # -------- NORMAL RULE CONTENT --------
        if current_rule:
            buffer.append(text)

    flush()
    return rules
