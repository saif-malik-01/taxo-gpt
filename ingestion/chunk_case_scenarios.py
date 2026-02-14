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

INPUT_FILE = "data/raw/docx/case_scenarios.docx"
OUTPUT_FILE = "data/processed/case_scenarios.json"

DOC_TYPE = "Case Scenario"
HIERARCHY_LEVEL = 5

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
# EXTRACT SECTION NUMBER
# ==========================

def extract_section_number(section_text):

    match = re.search(r'Section\s+(\d+)', section_text)
    return match.group(1) if match else "UNKNOWN"

# ==========================
# SPLIT ILLUSTRATIONS
# ==========================

def split_illustrations(section_text):

    pattern = r'(Illustration\s+\d+.*?Solution:.*?)(?=Illustration\s+\d+|$)'
    return re.findall(pattern, section_text, re.DOTALL | re.IGNORECASE)

# ==========================
# SPLIT PROBLEM & SOLUTION
# ==========================

def split_problem_solution(illustration_text):

    parts = re.split(r'Solution:', illustration_text, flags=re.IGNORECASE)

    problem = parts[0].strip()
    solution = parts[1].strip() if len(parts) > 1 else ""

    return problem, solution

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
    notifications = re.findall(r'Notification\s+No\.?\s*([\d/.-]+)', text)

    return {
        "sections": list(set(sections)),
        "rules": list(set(rules)),
        "notifications": list(set(notifications))
    }

# ==========================
# MAIN
# ==========================

def main():

    print("Loading document...")
    full_text = load_docx(INPUT_FILE)

    print("Splitting sections...")
    sections = split_by_section(full_text)

    all_chunks = []

    for section_block in sections:

        section_number = extract_section_number(section_block)
        illustrations = split_illustrations(section_block)

        for illustration in illustrations:

            illustration_num_match = re.search(r'Illustration\s+(\d+)', illustration)
            illustration_number = illustration_num_match.group(1) if illustration_num_match else "UNKNOWN"

            problem, solution = split_problem_solution(illustration)

            chunk = {
                "id": str(uuid.uuid4()),
                "doc_type": DOC_TYPE,
                "parent_doc": "GST Case Scenarios",
                "hierarchy_level": HIERARCHY_LEVEL,

                "structure": {
                    "section_number": section_number,
                    "illustration_number": illustration_number
                },

                "problem": problem,
                "solution": solution,
                "text": illustration.strip(),

                "summary": generate_summary(solution if solution else problem),
                "keywords": extract_keywords(illustration),
                "cross_references": extract_cross_references(illustration),

                "metadata": {
                    "source": "Case Scenarios",
                    "source_file": os.path.basename(INPUT_FILE)
                }
            }

            all_chunks.append(chunk)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_chunks, f, indent=4, ensure_ascii=False)

    print(f"\nCreated {len(all_chunks)} case scenario chunks.")

if __name__ == "__main__":
    main()
