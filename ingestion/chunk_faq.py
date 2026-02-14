import os
import re
import json
import uuid
import pdfplumber
import fitz
import pytesseract
from PIL import Image
import io
from docx import Document
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk

nltk.download('punkt')

# ==========================
# CONFIG
# ==========================

INPUT_FOLDER = "data/raw/docx/faqs"
OUTPUT_FILE = "data/processed/faqs.json"

DOC_TYPE = "FAQ"
HIERARCHY_LEVEL = 4

# ==========================
# LOAD DOCX
# ==========================

def load_docx(file_path):
    doc = Document(file_path)
    return "\n".join([p.text.strip() for p in doc.paragraphs if p.text.strip()])

# ==========================
# LOAD PDF (DIGITAL)
# ==========================

def load_pdf_digital(file_path):

    text = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text.append(page_text)

    return "\n".join(text)

# ==========================
# OCR FALLBACK
# ==========================

def load_pdf_ocr(file_path):

    text = []
    doc = fitz.open(file_path)

    for page in doc:
        pix = page.get_pixmap()
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        page_text = pytesseract.image_to_string(img)
        text.append(page_text)

    return "\n".join(text)

# ==========================
# SPLIT Q&A BLOCKS
# ==========================

def split_qa(text):

    qa_pattern = r'(Q\.?\s*\d+.*?Ans\.?.*?)(?=Q\.?\s*\d+|$)'
    matches = re.findall(qa_pattern, text, re.DOTALL | re.IGNORECASE)

    qa_blocks = []

    for block in matches:

        q_match = re.search(r'Q\.?\s*(\d+)', block)
        question_number = q_match.group(1) if q_match else None

        question_text_match = re.search(r'Q\.?\s*\d+[:\.\s]*(.*?)\n', block)
        question_text = question_text_match.group(1).strip() if question_text_match else "Unknown"

        qa_blocks.append((question_number, question_text, block.strip()))

    return qa_blocks

# ==========================
# SUMMARY
# ==========================

def generate_summary(text):
    sentences = nltk.sent_tokenize(text)
    return sentences[0] if sentences else text[:200]

# ==========================
# KEYWORDS
# ==========================

def extract_keywords(text, top_n=6):

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
# CROSS REFERENCES
# ==========================

def extract_cross_references(text):

    sections = re.findall(r'Section\s+(\d+[A-Za-z\(\)]*)', text)
    rules = re.findall(r'Rule\s+(\d+[A-Za-z\(\)]*)', text)
    notifications = re.findall(r'Notification\s*No\.?\s*([\d/]+)', text)

    return {
        "sections": list(set(sections)),
        "rules": list(set(rules)),
        "notifications": list(set(notifications))
    }

# ==========================
# PROCESS FILE
# ==========================

def process_file(file_path):

    print(f"Processing {os.path.basename(file_path)}")

    if file_path.lower().endswith(".docx"):
        text = load_docx(file_path)

    elif file_path.lower().endswith(".pdf"):
        text = load_pdf_digital(file_path)

        if not text.strip():
            print("Running OCR...")
            text = load_pdf_ocr(file_path)

    else:
        return []

    qa_blocks = split_qa(text)
    parent_doc = os.path.splitext(os.path.basename(file_path))[0]

    chunks = []

    for question_number, question_text, block_text in qa_blocks:

        chunk = {
            "id": str(uuid.uuid4()),
            "doc_type": DOC_TYPE,
            "parent_doc": parent_doc,
            "hierarchy_level": HIERARCHY_LEVEL,

            "structure": {
                "question_number": question_number,
                "question": question_text
            },

            "text": block_text,
            "summary": generate_summary(block_text),
            "keywords": extract_keywords(block_text),
            "cross_references": extract_cross_references(block_text),

            "metadata": {
                "source": "GST FAQs",
                "source_file": os.path.basename(file_path)
            }
        }

        chunks.append(chunk)

    return chunks

# ==========================
# MAIN
# ==========================

def main():

    all_chunks = []

    for filename in os.listdir(INPUT_FOLDER):

        if filename.lower().endswith((".pdf", ".docx")):
            file_path = os.path.join(INPUT_FOLDER, filename)
            file_chunks = process_file(file_path)
            all_chunks.extend(file_chunks)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_chunks, f, indent=4, ensure_ascii=False)

    print(f"\nCreated {len(all_chunks)} FAQ chunks.")

if __name__ == "__main__":
    main()
