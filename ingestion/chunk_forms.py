import os
import re
import json
import uuid
import pdfplumber
import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import io
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer

nltk.download('punkt')

# ==========================
# CONFIG
# ==========================

PDF_FOLDER = "data/raw/pdf/forms"
OUTPUT_FILE = "data/processed/forms.json"

HIERARCHY_LEVEL = 4

# ==========================
# TEXT EXTRACTION
# ==========================

def extract_text_digital(pdf_path):
    pages = []
    try:
        doc = fitz.open(pdf_path)
        for i, page in enumerate(doc):
            text = page.get_text()
            if text:
                pages.append({
                    "page": i + 1,
                    "text": text
                })
    except Exception as e:
        print(f"   [ERROR] Digital extraction failed for {os.path.basename(pdf_path)}: {e}")
    return pages

# ==========================
# OCR FALLBACK
# ==========================

def extract_text_ocr(pdf_path):
    pages = []
    try:
        doc = fitz.open(pdf_path)

        for i, page in enumerate(doc):
            pix = page.get_pixmap()
            img_data = pix.tobytes("png")
            img = Image.open(io.BytesIO(img_data))
            text = pytesseract.image_to_string(img)

            pages.append({
                "page": i + 1,
                "text": text
            })
    except Exception as e:
        print(f"   [WARN] OCR failed for {os.path.basename(pdf_path)}: {e}")
    return pages

# ==========================
# TABLE EXTRACTION
# ==========================

def extract_tables(pdf_path):

    tables_data = []

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            tables = page.extract_tables()
            for table in tables:
                if table:
                    tables_data.append({
                        "page": i + 1,
                        "table": table
                    })

    return tables_data

# ==========================
# DETECT DOCUMENT TYPE
# ==========================

def detect_doc_type(text):

    if re.search(r'GSTAT\s*FORM', text, re.IGNORECASE):
        return "Form"

    if re.search(r'GSTAT[-\s]?CDR', text, re.IGNORECASE):
        return "Register"

    return "Form"

# ==========================
# EXTRACT METADATA
# ==========================

def extract_metadata(text):

    # FORM detection
    form_match = re.search(r"GSTAT[-\s]*(FORM|CDR)[-\s]*(\d+)", text, re.IGNORECASE)
    number = form_match.group(2) if form_match else "UNKNOWN"

    title_match = re.search(r'–\s*(.*?)\n', text)
    title = title_match.group(1).strip() if title_match else "Untitled"

    rule_match = re.search(r'\[See\s*rule\s*([\d\(\)a-zA-Z]+)\]', text, re.IGNORECASE)
    related_rule = rule_match.group(1) if rule_match else None

    return number, title, related_rule

# ==========================
# SPLIT INTO SECTIONS
# ==========================

def split_sections(text):

    sections = {}

    patterns = {
        "Declaration": r'Declaration',
        "Instructions": r'Instructions',
        "Schedule": r'SCHEDULE OF FEES'
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            sections[key] = text[match.start():]

    return sections

# ==========================
# KEYWORD EXTRACTION
# ==========================

def extract_keywords(text, top_n=8):

    try:
        vectorizer = TfidfVectorizer(
            stop_words='english',
            max_features=200,
            ngram_range=(1,2)
        )

        tfidf_matrix = vectorizer.fit_transform([text])
        scores = zip(vectorizer.get_feature_names_out(), tfidf_matrix.toarray()[0])
        sorted_keywords = sorted(scores, key=lambda x: x[1], reverse=True)

        return [word for word, score in sorted_keywords[:top_n]]

    except:
        return []

# ==========================
# PROCESS SINGLE PDF
# ==========================

def process_pdf(pdf_path):

    print(f"Processing {os.path.basename(pdf_path)}")

    pages = extract_text_digital(pdf_path)

    if not pages:
        print("   No digital text found → Running OCR")
        pages = extract_text_ocr(pdf_path)

    full_text = " ".join([p["text"] for p in pages])

    doc_type = detect_doc_type(full_text)
    number, title, related_rule = extract_metadata(full_text)

    tables = extract_tables(pdf_path)
    sections = split_sections(full_text)

    base_chunk = {
        "id": str(uuid.uuid4()),
        "doc_type": doc_type,
        "chunk_type": "gstat_register" if doc_type == "Register" else "gstat_form",
        "parent_doc": "GSTAT Forms",
        "hierarchy_level": HIERARCHY_LEVEL,

        "structure": {
            "number": number,
            "title": title,
            "related_rule": related_rule
        },

        "text": full_text,
        "summary": f"{doc_type} {number} relates to {title}.",
        "keywords": extract_keywords(full_text),

        "metadata": {
            "source": "GSTAT Forms",
            "source_file": os.path.basename(pdf_path),
            "total_pages": len(pages)
        }
    }

    chunks = [base_chunk]

    # Add table chunks separately
    for idx, table_data in enumerate(tables):
        chunk = {
            "id": str(uuid.uuid4()),
            "doc_type": f"{doc_type} Table",
            "chunk_type": "gstat_form",
            "parent_doc": "GSTAT Forms",
            "hierarchy_level": HIERARCHY_LEVEL,

            "structure": {
                "related_document": number,
                "table_index": idx + 1,
                "page": table_data["page"]
            },

            "text": str(table_data["table"]),
            "summary": f"Table {idx+1} from {doc_type} {number}.",
            "keywords": [],
            "metadata": {
                "source": "GSTAT Forms",
                "source_file": os.path.basename(pdf_path)
            }
        }

        chunks.append(chunk)

    # Add section chunks
    for section_name, section_text in sections.items():

        chunk = {
            "id": str(uuid.uuid4()),
            "doc_type": f"{doc_type} Section",
            "chunk_type": "gstat_form",
            "parent_doc": "GSTAT Forms",
            "hierarchy_level": HIERARCHY_LEVEL,

            "structure": {
                "related_document": number,
                "section": section_name
            },

            "text": section_text,
            "summary": f"{section_name} section of {doc_type} {number}.",
            "keywords": [],
            "metadata": {
                "source_file": os.path.basename(pdf_path)
            }
        }

        chunks.append(chunk)

    return chunks

# ==========================
# MAIN PIPELINE
# ==========================

def main():

    all_chunks = []

    for filename in os.listdir(PDF_FOLDER):

        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(PDF_FOLDER, filename)
            file_chunks = process_pdf(pdf_path)
            all_chunks.extend(file_chunks)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_chunks, f, indent=4, ensure_ascii=False)

    print(f"\nCreated {len(all_chunks)} form chunks.")

if __name__ == "__main__":
    main()
