import os
import re
import json
import uuid
import pdfplumber
from collections import defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk

nltk.download('punkt')

# ==========================
# CONFIG
# ==========================

PDF_FOLDER = "data/raw/pdf/gst_council_meetings"
OUTPUT_FILE = "data/processed/gst_council_meetings.json"

DOC_TYPE = "Council Minutes"
HIERARCHY_LEVEL = 2

# ==========================
# PDF TEXT EXTRACTION
# ==========================

def extract_text_by_page(pdf_path):

    pages = []

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if text:
                pages.append({
                    "page": i + 1,
                    "text": text
                })

    return pages

# ==========================
# CLEAN TEXT
# ==========================

def clean_text(text):

    text = re.sub(r'\n+', '\n', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'Page\s+\d+', '', text)
    text = re.sub(r'GST Council Secretariat', '', text)

    return text.strip()

# ==========================
# EXTRACT MEETING METADATA
# ==========================

def extract_meeting_metadata(full_text):

    meeting_match = re.search(r'(\d+)(st|nd|rd|th)\s+Meeting', full_text, re.IGNORECASE)
    date_match = re.search(r'\d{1,2}\s+\w+\s+\d{4}', full_text)

    meeting_number = meeting_match.group(1) if meeting_match else "UNKNOWN"
    meeting_date = date_match.group(0) if date_match else "UNKNOWN"

    return meeting_number, meeting_date

# ==========================
# SPLIT BY AGENDA
# ==========================

def split_by_agenda(pages):

    combined_text = ""
    page_map = {}

    for p in pages:
        cleaned = clean_text(p["text"])
        page_map[p["page"]] = cleaned
        combined_text += f"\n[PAGE_{p['page']}]\n{cleaned}"

    agenda_pattern = r'Agenda Item\s+\d+'
    splits = re.split(f'({agenda_pattern})', combined_text)

    agenda_chunks = []

    for i in range(1, len(splits), 2):

        agenda_heading = splits[i]
        agenda_body = splits[i + 1]

        agenda_number_match = re.search(r'Agenda Item\s+(\d+)', agenda_heading)
        agenda_number = agenda_number_match.group(1) if agenda_number_match else "UNKNOWN"

        # Detect page range
        pages_found = re.findall(r'\[PAGE_(\d+)\]', agenda_body)
        page_numbers = sorted(list(set([int(p) for p in pages_found])))

        cleaned_body = re.sub(r'\[PAGE_\d+\]', '', agenda_body)

        agenda_chunks.append({
            "agenda_number": agenda_number,
            "text": cleaned_body.strip(),
            "page_range": page_numbers
        })

    return agenda_chunks

# ==========================
# EXTRACT DECISION SENTENCES
# ==========================

def extract_decisions(text):

    sentences = nltk.sent_tokenize(text)

    decision_keywords = [
        "decided",
        "approved",
        "recommended",
        "agreed",
        "resolved"
    ]

    decisions = []

    for sentence in sentences:
        for keyword in decision_keywords:
            if keyword.lower() in sentence.lower():
                decisions.append(sentence.strip())
                break

    return decisions

# ==========================
# EXTRACT CROSS REFERENCES
# ==========================

def extract_cross_references(text):

    sections = re.findall(r'Section\s+(\d+[A-Za-z\(\)]*)', text)
    rules = re.findall(r'Rule\s+(\d+[A-Za-z\(\)]*)', text)
    notifications = re.findall(r'Notification\s+No\.?\s*([\d\/-]+)', text)

    return {
        "sections": list(set(sections)),
        "rules": list(set(rules)),
        "notifications": list(set(notifications))
    }

# ==========================
# KEYWORD EXTRACTION
# ==========================

def extract_keywords(text, top_n=10):

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

    pages = extract_text_by_page(pdf_path)

    if not pages:
        print(f"⚠ No text extracted from {pdf_path}")
        return []

    full_text = " ".join([p["text"] for p in pages])
    meeting_number, meeting_date = extract_meeting_metadata(full_text)

    agenda_chunks = split_by_agenda(pages)

    final_chunks = []

    for agenda in agenda_chunks:

        decisions = extract_decisions(agenda["text"])
        cross_refs = extract_cross_references(agenda["text"])
        keywords = extract_keywords(agenda["text"])

        chunk = {
            "id": str(uuid.uuid4()),
            "doc_type": DOC_TYPE,
            "chunk_type": "council_decision",
            "parent_doc": f"{meeting_number}th GST Council Meeting",
            "hierarchy_level": HIERARCHY_LEVEL,

            "structure": {
                "meeting_number": meeting_number,
                "meeting_date": meeting_date,
                "agenda_item": agenda["agenda_number"]
            },

            "text": agenda["text"],
            "decision_sentences": decisions,
            "summary": decisions[0] if decisions else f"Agenda Item {agenda['agenda_number']} discussion.",
            "keywords": keywords,
            "cross_references": cross_refs,

            "metadata": {
                "source": "GST Council Meetings",
                "source_file": os.path.basename(pdf_path),
                "page_range": agenda["page_range"]
            }
        }

        final_chunks.append(chunk)

    return final_chunks

# ==========================
# MAIN PIPELINE
# ==========================

def main():

    all_chunks = []

    for filename in os.listdir(PDF_FOLDER):

        if filename.lower().endswith(".pdf"):

            pdf_path = os.path.join(PDF_FOLDER, filename)
            print(f"Processing {filename}...")

            chunks = process_pdf(pdf_path)
            all_chunks.extend(chunks)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_chunks, f, indent=4, ensure_ascii=False)

    print(f"\nCreated {len(all_chunks)} Council agenda chunks.")

if __name__ == "__main__":
    main()
