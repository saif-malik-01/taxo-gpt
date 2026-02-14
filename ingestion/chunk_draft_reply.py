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

INPUT_FOLDER = "data/raw/docx/draft_replies"
OUTPUT_FILE = "data/processed/draft_replies.json"

DOC_TYPE = "Draft Reply"
HIERARCHY_LEVEL = 5

# ==========================
# LOAD DOCX
# ==========================

def load_docx(file_path):

    doc = Document(file_path)
    paragraphs = [p.text.strip() for p in doc.paragraphs if p.text.strip()]

    return paragraphs

# ==========================
# SECTION SPLITTER
# ==========================

def split_sections(text):

    sections = []

    # Facts
    facts_match = re.search(r'Facts.*?(?=NOTICEE|Submissions|Issue|Conclusion)', text, re.DOTALL | re.IGNORECASE)
    if facts_match:
        sections.append(("Facts", None, facts_match.group().strip()))

    # Submissions
    submissions_match = re.search(r'(NOTICEE.?S SUBMISSIONS|Submissions).*?(?=Issue|Conclusion)', text, re.DOTALL | re.IGNORECASE)
    if submissions_match:
        sections.append(("Submissions", None, submissions_match.group().strip()))

    # Legal Provisions
    legal_pattern = r'(Section\s+\d+.*?)(?=Section\s+\d+|Rule\s+\d+|Issue|Conclusion|$)'
    legal_matches = re.findall(legal_pattern, text, re.DOTALL | re.IGNORECASE)

    for lm in legal_matches:
        sections.append(("Legal Provision", None, lm.strip()))

    # Issue-wise Split
    issue_pattern = r'(Issue[-\s]*\d+.*?)(?=Issue[-\s]*\d+|Conclusion|$)'
    issue_matches = re.findall(issue_pattern, text, re.DOTALL | re.IGNORECASE)

    for issue_block in issue_matches:
        issue_num_match = re.search(r'Issue[-\s]*(\d+)', issue_block)
        issue_number = issue_num_match.group(1) if issue_num_match else None

        sections.append(("Issue", issue_number, issue_block.strip()))

    # Conclusion
    conclusion_match = re.search(r'(Conclusion.*)', text, re.DOTALL | re.IGNORECASE)
    if conclusion_match:
        sections.append(("Conclusion", None, conclusion_match.group().strip()))

    return sections

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
    rules = re.findall(r'Rule[-\s]*(\d+[A-Za-z\(\)]*)', text)
    case_laws = re.findall(r'Vs\.\s*([A-Za-z\s\.]+)', text)

    return {
        "sections": list(set(sections)),
        "rules": list(set(rules)),
        "case_laws": list(set(case_laws))
    }

# ==========================
# PROCESS FILE
# ==========================

def process_draft_reply(file_path):

    print(f"Processing {os.path.basename(file_path)}")

    paragraphs = load_docx(file_path)
    full_text = "\n".join(paragraphs)

    sections = split_sections(full_text)
    case_name = os.path.splitext(os.path.basename(file_path))[0]

    chunks = []

    for section_type, issue_number, text in sections:

        chunk = {
            "id": str(uuid.uuid4()),
            "doc_type": DOC_TYPE,
            "parent_doc": case_name,
            "hierarchy_level": HIERARCHY_LEVEL,

            "structure": {
                "section_type": section_type,
                "issue_number": issue_number
            },

            "text": text,
            "summary": generate_summary(text),
            "keywords": extract_keywords(text),
            "cross_references": extract_cross_references(text),

            "metadata": {
                "source": "GST Draft Replies",
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

        if filename.lower().endswith(".docx"):
            file_path = os.path.join(INPUT_FOLDER, filename)
            chunks = process_draft_reply(file_path)
            all_chunks.extend(chunks)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_chunks, f, indent=4, ensure_ascii=False)

    print(f"\nCreated {len(all_chunks)} draft reply chunks.")

if __name__ == "__main__":
    main()
