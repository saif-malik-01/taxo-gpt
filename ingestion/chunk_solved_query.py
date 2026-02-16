import os
import re
import json
import uuid
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk

nltk.download('punkt', quiet=True)

# ==========================
# CONFIG
# ==========================

INPUT_CSV = "data/raw/csv/export_solved_query.csv"
OUTPUT_FILE = "data/processed/solved_query_chunks.json"

DOC_TYPE = "QA_PAIR"
HIERARCHY_LEVEL = 4

MAX_CHUNK_SIZE = 2000
CHUNK_OVERLAP = 200

CSV_COLUMNS = {
    "id": "ID",
    "query": "Query"
}

# ==========================
# SAFE CSV READER
# ==========================

def safe_read_csv(csv_path):
    """
    Robust CSV reader that:
    - Tries multiple encodings
    - Uses python engine (more tolerant)
    - Logs bad lines instead of crashing
    """

    encodings = ['utf-8', 'latin-1', 'cp1252']

    for enc in encodings:
        try:
            print(f"Trying encoding: {enc}")

            df = pd.read_csv(
                csv_path,
                encoding=enc,
                engine='python',
                on_bad_lines='warn'   # Warn instead of crash
            )

            print(f"Successfully loaded with encoding: {enc}")
            return df

        except Exception as e:
            print(f"Failed with encoding {enc}: {e}")

    raise Exception("❌ Failed to read CSV with all attempted encodings.")

# ==========================
# TEXT CLEANING
# ==========================

def clean_text(text):
    if not text:
        return text

    text = str(text).strip()
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\s+([.,!?;:])', r'\1', text)
    text = re.sub(r'([.,!?;:])([^\s\d])', r'\1 \2', text)
    text = re.sub(r'\s*\(\s*', ' (', text)
    text = re.sub(r'\s*\)\s*', ') ', text)

    return text.strip()

# ==========================
# SPLIT Q&A
# ==========================

def split_question_answer(query_text):

    if not query_text or pd.isna(query_text):
        return "", ""

    text = str(query_text).strip()

    separators = [
        (r'(?i)\bAns\.?\s*:?\s*', 'ans_keyword'),
        (r'[-—]{4,}', 'dashes')
    ]

    for pattern, _ in separators:
        parts = re.split(pattern, text, maxsplit=1, flags=re.DOTALL)
        if len(parts) == 2:
            question = clean_text(parts[0])
            answer = clean_text(parts[1])
            if question and answer:
                return question, answer

    if '?' in text:
        parts = text.split('?', 1)
        if len(parts) == 2:
            question = clean_text(parts[0]) + '?'
            answer = clean_text(parts[1])
            if answer:
                return question, answer

    return clean_text(text), ""

# ==========================
# REMOVE QUESTION NUMBER
# ==========================

def remove_question_number(text):
    if not text:
        return text

    patterns = [
        r'^(?:Q\.?|Ques\.?|Question)\s*[-:\.]?\s*\d+\s*[\.:-]?\s*',
        r'^\d+\s*[\.:-]?\s*',
        r'^(?:Ques|QUES)\s*[:\.]\s*',
        r'^(?:Q|q)\s*[:\.]\s*',
    ]

    original = text

    for pattern in patterns:
        cleaned = re.sub(pattern, '', original, count=1, flags=re.IGNORECASE)
        if cleaned != original:
            return clean_text(cleaned)

    return clean_text(original)

# ==========================
# CROSS REFERENCES
# ==========================

def extract_cross_references(text):

    if not text:
        return {
            "sections": [],
            "rules": [],
            "notifications": [],
            "forms": [],
            "hsn_codes": [],
            "sac_codes": []
        }

    sections = []
    section_matches = re.findall(
        r'(?:Section|Sec\.?|section|sec\.?)\s+(\d+)\s*(?:\((\d+[A-Za-z]*)\))?',
        text, re.IGNORECASE
    )

    for match in section_matches:
        if match[1]:
            sections.append(f"{match[0]}({match[1]})")
        else:
            sections.append(match[0])

    under_section = re.findall(r'(?:u/s|under\s+section)\s+(\d+)', text, re.IGNORECASE)
    sections.extend(under_section)

    rules = []
    rule_matches = re.findall(
        r'(?:Rule|rule)\s+(\d+[A-Za-z]*)\s*(?:\((\d+[A-Za-z]*)\))?',
        text, re.IGNORECASE
    )

    for match in rule_matches:
        if match[1]:
            rules.append(f"{match[0]}({match[1]})")
        else:
            rules.append(match[0])

    notifications = re.findall(
        r'Notification\s*(?:No\.?)?\s*([\d/\-]+(?:/\d{4})?)',
        text, re.IGNORECASE
    )

    forms = re.findall(
        r'(?:Form|GSTAT\s+Form|form)\s+([\d]+[A-Z]*(?:\s+[A-Z]+-\d+)?)',
        text, re.IGNORECASE
    )

    hsn_codes = re.findall(r'HSN\s+(?:Code\s+)?(\d{4,8})', text, re.IGNORECASE)
    sac_codes = re.findall(r'SAC\s+(?:Code\s+)?(\d{4,6})', text, re.IGNORECASE)

    return {
        "sections": list(set(sections)),
        "rules": list(set(rules)),
        "notifications": list(set(notifications)),
        "forms": list(set(forms)),
        "hsn_codes": list(set(hsn_codes)),
        "sac_codes": list(set(sac_codes))
    }

# ==========================
# KEYWORDS
# ==========================

def extract_keywords(text, top_n=6):

    if not text or len(text.strip()) < 10:
        return []

    keywords = []

    gst_terms = {
        'ITC', 'RCM', 'IGST', 'CGST', 'SGST', 'UTGST', 'GST',
        'Input Tax Credit', 'Reverse Charge', 'Place of Supply',
        'refund', 'assessment', 'penalty', 'interest'
    }

    text_lower = text.lower()
    for term in gst_terms:
        if term.lower() in text_lower:
            keywords.append(term)

    try:
        vectorizer = TfidfVectorizer(
            stop_words='english',
            max_features=200,
            ngram_range=(1, 2)
        )

        tfidf_matrix = vectorizer.fit_transform([text])
        scores = zip(vectorizer.get_feature_names_out(), tfidf_matrix.toarray()[0])
        sorted_keywords = sorted(scores, key=lambda x: x[1], reverse=True)

        tfidf_keywords = [word for word, score in sorted_keywords[:top_n]]
        keywords.extend(tfidf_keywords)

    except:
        pass

    seen = set()
    unique_keywords = []

    for kw in keywords:
        if kw.lower() not in seen:
            seen.add(kw.lower())
            unique_keywords.append(kw)

    return unique_keywords[:10]

# ==========================
# CHUNKING
# ==========================

def chunk_large_text(question, answer, max_size=MAX_CHUNK_SIZE, overlap=CHUNK_OVERLAP):

    combined_text = f"Q: {question}\n\nA: {answer}"

    if len(combined_text) <= max_size:
        return [(question, answer)]

    chunks = []

    try:
        sentences = nltk.sent_tokenize(answer)
    except:
        sentences = [s.strip() + '.' for s in answer.split('.') if s.strip()]

    current_chunk = ""

    for sentence in sentences:

        test_chunk = current_chunk + " " + sentence if current_chunk else sentence
        question_prefix_size = len(question) + 10

        if len(test_chunk) + question_prefix_size > max_size:

            if current_chunk:
                chunks.append((question, current_chunk.strip()))
                overlap_text = current_chunk[-overlap:] if len(current_chunk) > overlap else current_chunk
                current_chunk = overlap_text + " " + sentence
            else:
                chunks.append((question, sentence.strip()))
                current_chunk = ""
        else:
            current_chunk = test_chunk

    if current_chunk:
        chunks.append((question, current_chunk.strip()))

    return chunks

# ==========================
# PROCESS CSV
# ==========================

def process_csv(csv_path):

    print(f"Processing {os.path.basename(csv_path)}")

    df = safe_read_csv(csv_path)

    required_cols = {CSV_COLUMNS["id"], CSV_COLUMNS["query"]}
    available_cols = set(df.columns)

    if not required_cols.issubset(available_cols):
        missing = required_cols - available_cols
        print(f"❌ Missing columns: {missing}")
        return []

    parent_doc = os.path.splitext(os.path.basename(csv_path))[0]

    all_chunks = []
    skipped_rows = 0

    for idx, row in df.iterrows():

        row_id = str(row[CSV_COLUMNS["id"]])
        query = str(row[CSV_COLUMNS["query"]]) if not pd.isna(row[CSV_COLUMNS["query"]]) else ""

        if not query.strip():
            skipped_rows += 1
            continue

        question, answer = split_question_answer(query)
        question = remove_question_number(question)

        if len(question.strip()) < 5:
            skipped_rows += 1
            continue

        qa_chunks = chunk_large_text(question, answer)

        for q_text, a_text in qa_chunks:

            chunk_text = f"Q: {q_text}\n\nA: {a_text}" if a_text else f"Q: {q_text}"

            chunk = {
                "id": str(uuid.uuid4()),
                "doc_type": DOC_TYPE,
                "parent_doc": parent_doc,
                "hierarchy_level": HIERARCHY_LEVEL,
                "structure": {
                    "csv_row_id": row_id,
                    "question": q_text,
                    "answer": a_text
                },
                "text": chunk_text,
                "keywords": extract_keywords(question + " " + answer),
                "cross_references": extract_cross_references(question + " " + answer),
                "metadata": {
                    "source": "CSV Q&A Dataset",
                    "source_file": parent_doc,
                    "row_id": row_id
                }
            }

            all_chunks.append(chunk)

    print(f"✅ Created {len(all_chunks)} chunks")
    print(f"Skipped rows: {skipped_rows}")

    return all_chunks

# ==========================
# MAIN
# ==========================

def main():

    print("=" * 60)
    print("CSV Q&A CHUNKER")
    print("=" * 60)

    if not os.path.exists(INPUT_CSV):
        print(f"❌ Input file not found: {INPUT_CSV}")
        return

    chunks = process_csv(INPUT_CSV)

    if not chunks:
        print("❌ No chunks created.")
        return

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(chunks, f, indent=2, ensure_ascii=False)

    print(f"✅ Output saved to: {OUTPUT_FILE}")
    print("DONE.")

if __name__ == "__main__":
    main()
