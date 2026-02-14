import os
import re
import json
import uuid
from docx import Document
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.corpus import stopwords
import nltk

nltk.download('stopwords')

# ==========================
# CONFIG
# ==========================

INPUT_FILE = "data/raw/docx/gstat_rules.docx"
OUTPUT_FILE = "data/processed/gstat_rules.json"

DOC_TYPE = "Rule"
PARENT_DOC = "GSTAT Rules 2025"
HIERARCHY_LEVEL = 3

GENERIC_WORDS = {
    "rule", "rules", "2025", "appellate", "tribunal",
    "goods", "services", "tax", "gstat"
}

STOP_WORDS = set(stopwords.words('english'))

# ==========================
# LOAD DOCX
# ==========================

def load_docx_text(file_path):
    doc = Document(file_path)
    text = []
    for para in doc.paragraphs:
        if para.text.strip():
            text.append(para.text.strip())
    return "\n".join(text)

# ==========================
# SPLIT RULES
# ==========================

def split_into_rules(text):
    pattern = r"(Rule\s+\d+\..*?)(?=Rule\s+\d+\.|$)"
    return re.findall(pattern, text, flags=re.DOTALL)

# ==========================
# EXTRACT METADATA
# ==========================

def extract_rule_metadata(rule_text):
    header_match = re.match(r"Rule\s+(\d+)\.\s*(.*?)\s*–", rule_text)
    if header_match:
        return header_match.group(1), header_match.group(2).strip()
    return None, "Untitled"

# ==========================
# CLEAN TEXT
# ==========================

def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip()

# ==========================
# IMPROVED SUMMARY
# ==========================

def generate_summary(rule_number, rule_title, text):
    # Remove header
    body = re.sub(r"Rule\s+\d+\.\s*.*?–", "", text, count=1).strip()
    sentences = re.split(r'(?<=[.!?]) +', body)

    if sentences and len(sentences[0]) > 30:
        return sentences[0][:300]

    return f"Rule {rule_number} deals with {rule_title}."

# ==========================
# SMART KEYWORD EXTRACTION
# ==========================

def extract_keywords(text, top_n=8):

    vectorizer = TfidfVectorizer(
        stop_words='english',
        max_features=100,
        ngram_range=(1,2)
    )

    tfidf_matrix = vectorizer.fit_transform([text])
    scores = zip(vectorizer.get_feature_names_out(), tfidf_matrix.toarray()[0])
    sorted_keywords = sorted(scores, key=lambda x: x[1], reverse=True)

    keywords = []
    for word, score in sorted_keywords:
        if word.lower() not in GENERIC_WORDS:
            keywords.append(word)
        if len(keywords) >= top_n:
            break

    return keywords

# ==========================
# CROSS REFERENCES
# ==========================

def detect_cross_references(text):

    rules_ref = re.findall(r"Rule\s+(\d+)", text)
    sections_ref = re.findall(r"section\s+(\d+)", text, flags=re.IGNORECASE)
    forms_ref = re.findall(r"GSTAT[-\s]?FORM[-\s]?\d+", text, flags=re.IGNORECASE)

    return {
        "rules": list(set(rules_ref)),
        "sections": list(set(sections_ref)),
        "forms": list(set(forms_ref))
    }

# ==========================
# MAIN
# ==========================

def main():

    print("Loading document...")
    full_text = load_docx_text(INPUT_FILE)

    print("Splitting rules...")
    rules = split_into_rules(full_text)

    final_chunks = []

    for rule_text in rules:

        rule_number, rule_title = extract_rule_metadata(rule_text)
        cleaned_text = clean_text(rule_text)

        summary = generate_summary(rule_number, rule_title, cleaned_text)
        keywords = extract_keywords(cleaned_text)
        cross_refs = detect_cross_references(cleaned_text)

        chunk = {
            "id": str(uuid.uuid4()),
            "doc_type": DOC_TYPE,
            "chunk_type": "gstat_rule",  # For legal hierarchy prioritization
            "parent_doc": PARENT_DOC,
            "hierarchy_level": HIERARCHY_LEVEL,

            "structure": {
                "rule_number": rule_number,
                "rule_title": rule_title,
                "chapter": None,
                "subrule": None
            },

            "text": cleaned_text,
            "summary": summary,
            "keywords": keywords,
            "cross_references": cross_refs,

            "metadata": {
                "source": PARENT_DOC,
                "notified_date": None
            }
        }

        final_chunks.append(chunk)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_chunks, f, indent=4, ensure_ascii=False)

    print(f"Created {len(final_chunks)} structured rule chunks.")

if __name__ == "__main__":
    main()
