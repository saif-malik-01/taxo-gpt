import os
import re
import json
import uuid
from docx import Document
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk

nltk.download('punkt')

# ==========================
# CONFIG
# ==========================

INPUT_FILE = "data/raw/docx/section_analytical_review.docx"
OUTPUT_FILE = "data/processed/section_analytical_review.json"

DOC_TYPE = "Analytical Review"
HIERARCHY_LEVEL = 3

# ==========================
# LOAD DOCX
# ==========================

def load_docx(file_path):
    doc = Document(file_path)
    return "\n".join([p.text.strip() for p in doc.paragraphs if p.text.strip()])

# ==========================
# SPLIT BY SECTION
# ==========================

def split_by_section(text):

    pattern = r'(Section\s+\d+.*?)(?=Section\s+\d+|$)'
    return re.findall(pattern, text, re.DOTALL | re.IGNORECASE)

# ==========================
# EXTRACT SECTION METADATA
# ==========================

def extract_section_metadata(section_text):

    match = re.search(r'Section\s+(\d+)\s*-\s*(.*?)\n', section_text)

    if match:
        return match.group(1), match.group(2).strip()

    return "UNKNOWN", "Untitled"

# ==========================
# SPLIT BY TOPIC HEADINGS
# ==========================

def split_by_headings(section_text):

    # Headings like 2.1, 2.2.1, etc.
    pattern = r'(\d+\.\d+.*?)(?=\n\d+\.\d+|\n\d+\.\d+\.\d+|$)'
    matches = re.findall(pattern, section_text, re.DOTALL)

    if matches:
        return matches

    # fallback: large block
    return [section_text]

# ==========================
# SUMMARY
# ==========================

def generate_summary(text):
    sentences = nltk.sent_tokenize(text)
    return sentences[0] if sentences else text[:200]

# ==========================
# KEYWORDS
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
# CROSS REFERENCES
# ==========================

def extract_cross_references(text):

    sections = re.findall(r'Section\s+(\d+[A-Za-z\(\)]*)', text)
    rules = re.findall(r'Rule\s+(\d+[A-Za-z\(\)]*)', text)
    circulars = re.findall(r'Circular\s+No\.?\s*([\d/.-]+)', text)
    case_laws = re.findall(r'Vs\.\s*([A-Za-z\s\.]+)', text)

    return {
        "sections": list(set(sections)),
        "rules": list(set(rules)),
        "circulars": list(set(circulars)),
        "case_laws": list(set(case_laws))
    }

# ==========================
# MAIN PROCESS
# ==========================

def main():

    print("Loading document...")
    full_text = load_docx(INPUT_FILE)

    print("Splitting by section...")
    sections = split_by_section(full_text)

    all_chunks = []

    for section_block in sections:

        section_number, section_title = extract_section_metadata(section_block)
        topic_blocks = split_by_headings(section_block)

        for topic in topic_blocks:

            chunk = {
                "id": str(uuid.uuid4()),
                "doc_type": DOC_TYPE,
                "parent_doc": "CGST Section Analytical Review",
                "hierarchy_level": HIERARCHY_LEVEL,

                "structure": {
                    "section_number": section_number,
                    "section_title": section_title
                },

                "text": topic.strip(),
                "summary": generate_summary(topic),
                "keywords": extract_keywords(topic),
                "cross_references": extract_cross_references(topic),

                "metadata": {
                    "source": "CGST Section Analytical Review",
                    "source_file": os.path.basename(INPUT_FILE)
                }
            }

            all_chunks.append(chunk)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_chunks, f, indent=4, ensure_ascii=False)

    print(f"\nCreated {len(all_chunks)} analytical review chunks.")

if __name__ == "__main__":
    main()
