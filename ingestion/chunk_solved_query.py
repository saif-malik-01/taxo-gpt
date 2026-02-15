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

INPUT_CSV = "data/raw/csv/Export_solved_query.csv"
OUTPUT_FILE = "data/processed/solved_query_chunks.json"

DOC_TYPE = "QA_PAIR"
HIERARCHY_LEVEL = 4

# Chunking config for large Q&A pairs
MAX_CHUNK_SIZE = 2000  # characters (~400-500 words, safe for all-MiniLM-L6-v2)
CHUNK_OVERLAP = 200    # character overlap between chunks

# CSV column mapping
# ID: Required for data management/tracking only, NOT used for retrieval
#     Users are not aware of these IDs - retrieval is purely content-based
# Query: Contains "<Question> <Separator> <Answer>" where separator is ONE of:
#        - "Ans." or "ans:" (case insensitive)
#        - "----" (4+ dashes)
CSV_COLUMNS = {
    "id": "ID",      # For tracking/debugging, NOT retrieval
    "query": "Query"  # Contains: Question + Separator + Answer
}

# ==========================
# TEXT CLEANING
# ==========================

def clean_text(text):
    """
    Clean text by removing extra whitespace and normalizing spacing
    Handles spacing issues in CSV data
    """
    if not text:
        return text
    
    # Convert to string and strip
    text = str(text).strip()
    
    # Replace multiple spaces with single space
    text = re.sub(r'\s+', ' ', text)
    
    # Remove spaces before punctuation
    text = re.sub(r'\s+([.,!?;:])', r'\1', text)
    
    # Ensure space after punctuation (except at end)
    text = re.sub(r'([.,!?;:])([^\s\d])', r'\1 \2', text)
    
    # Clean up spaces around parentheses
    text = re.sub(r'\s*\(\s*', ' (', text)
    text = re.sub(r'\s*\)\s*', ') ', text)
    
    return text.strip()

# ==========================
# SPLIT Q&A FROM QUERY CELL
# ==========================

def split_question_answer(query_text):
    """
    Split question and answer from same cell
    
    Format in Query cell:
    <Question (can be multiple lines)>
    <Separator>
    <Answer (can be multiple lines)>
    
    Answer always begins from a new line after separator.
    ONE of these separators will be present:
    - "Ans." or "ans:" (case insensitive)
    - "----" (4 or more dashes)
    
    Example:
    "What is Section 80C 
    and its benefits?
    Ans. 
    The deduction limit is Rs. 1.5 lakh.
    It covers investments like PPF."
    """
    
    if not query_text or pd.isna(query_text):
        return "", ""
    
    # Initial cleanup - preserve newlines but normalize spacing
    text = str(query_text).strip()
    
    # Try separators in priority order (ONE will be present)
    # Use DOTALL flag to handle multiline text
    separators = [
        (r'(?i)\bAns\.?\s*:?\s*', 'ans_keyword'),     # Ans. ans: ANS.
        (r'[-—]{4,}', 'dashes')                        # ---- or ———— (4+ regular or EM dashes)
    ]
    
    for pattern, sep_type in separators:
        # DOTALL allows . to match newlines
        parts = re.split(pattern, text, maxsplit=1, flags=re.DOTALL)
        
        if len(parts) == 2:
            question = clean_text(parts[0])
            answer = clean_text(parts[1])
            
            if question and answer:  # Both must be non-empty
                return question, answer
    
    # Fallback: No separator found - try question mark split
    if '?' in text:
        parts = text.split('?', 1)
        if len(parts) == 2:
            question = clean_text(parts[0]) + '?'
            answer = clean_text(parts[1])
            if answer:  # Must have answer text
                return question, answer
    
    # Last resort: treat entire text as question with no answer
    return clean_text(text), ""

# ==========================
# REMOVE QUESTION NUMBER
# ==========================

def remove_question_number(text):
    """
    Remove question number prefix from start of text
    Patterns: 
    - "55", "Q. 55", "Ques: 55", "Q-55", "Q-55.", "Question 55"
    - "Ques:" (without number)
    - "Q:" (without number)
    Also cleans up any resulting spacing issues
    """
    
    if not text:
        return text
    
    # Remove from START only (not from middle of text)
    patterns = [
        # With question number
        r'^(?:Q\.?|Ques\.?|Question)\s*[-:\.]?\s*\d+\s*[\.:-]?\s*',  # Q. 55, Ques: 55, Q-55., etc.
        r'^\d+\s*[\.:-]?\s*',                                          # Just number: 55. or 55:
        # Without question number (just labels)
        r'^(?:Ques|QUES)\s*[:\.]\s*',                                  # Ques: or Ques.
        r'^(?:Q|q)\s*[:\.]\s*',                                        # Q: or Q.
    ]
    
    cleaned = text
    for pattern in patterns:
        cleaned = re.sub(pattern, '', cleaned, count=1, flags=re.IGNORECASE)
        # If something was removed, don't try other patterns
        if cleaned != text:
            break
    
    # Clean up any extra spacing after removal
    return clean_text(cleaned)

# ==========================
# EXTRACT CROSS REFERENCES
# ==========================

def extract_cross_references(text):
    """
    Extract legal references: sections, rules, notifications, forms, HSN, SAC
    Handles various formats including subsections in parentheses
    """
    
    if not text:
        return {
            "sections": [],
            "rules": [],
            "notifications": [],
            "forms": [],
            "hsn_codes": [],
            "sac_codes": []
        }
    
    # Section patterns (with or without subsection)
    sections = []
    # Pattern 1: Section 15(1), Section 15, Sec 15(1)
    section_matches = re.findall(r'(?:Section|Sec\.?|section|sec\.?)\s+(\d+)\s*(?:\((\d+[A-Za-z]*)\))?', text, re.IGNORECASE)
    for match in section_matches:
        if match[1]:  # Has subsection
            sections.append(f"{match[0]}({match[1]})")
        else:
            sections.append(match[0])
    
    # Pattern 2: Standalone section references like "u/s 74" or "under section 74"
    under_section = re.findall(r'(?:u/s|under\s+section)\s+(\d+)', text, re.IGNORECASE)
    sections.extend(under_section)
    
    # Rules - enhanced to capture rules with subsections
    rules = []
    # Pattern 1: Rule 42, Rule 96(10)
    rule_matches = re.findall(r'(?:Rule|rule)\s+(\d+[A-Za-z]*)\s*(?:\((\d+[A-Za-z]*)\))?', text, re.IGNORECASE)
    for match in rule_matches:
        if match[1]:  # Has subsection like Rule 96(10)
            rules.append(f"{match[0]}({match[1]})")
        else:
            rules.append(match[0])
    
    # Notifications - multiple formats
    notifications = []
    # Pattern 1: Notification No. 12/2020
    notif1 = re.findall(r'Notification\s*(?:No\.?)?\s*([\d/\-]+(?:/\d{4})?)', text, re.IGNORECASE)
    notifications.extend(notif1)
    
    # Forms - multiple formats
    forms = []
    # Pattern 1: Form 16, GSTAT Form 01, Form GST REG-01
    form_matches = re.findall(r'(?:Form|GSTAT\s+Form|form)\s+([\d]+[A-Z]*(?:\s+[A-Z]+-\d+)?)', text, re.IGNORECASE)
    forms.extend(form_matches)
    
    # HSN Codes
    hsn_codes = re.findall(r'HSN\s+(?:Code\s+)?(\d{4,8})', text, re.IGNORECASE)
    
    # SAC Codes
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
# EXTRACT KEYWORDS
# ==========================

def extract_keywords(text, top_n=6):
    """
    Extract keywords using TF-IDF + GST-specific domain terms
    """
    
    if not text or len(text.strip()) < 10:
        return []
    
    keywords = []
    
    # Extract GST-specific domain terms (high priority)
    gst_terms = {
        'ITC', 'RCM', 'IGST', 'CGST', 'SGST', 'UTGST', 'GST',
        'Input Tax Credit', 'Reverse Charge', 'Place of Supply', 'PoS',
        'GSTR', 'advance ruling', 'SCN', 'show cause notice',
        'export', 'import', 'customs', 'SEZ', 'advance licence',
        'refund', 'assessment', 'demand', 'penalty', 'interest'
    }
    
    text_lower = text.lower()
    for term in gst_terms:
        if term.lower() in text_lower:
            keywords.append(term)
    
    # TF-IDF keywords
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
        # Fallback: extract common legal/GST terms if TF-IDF fails
        common_terms = ['section', 'rule', 'notification', 'gst', 'tax', 'deduction', 
                       'exemption', 'liable', 'assessment', 'appeal', 'credit', 'supply']
        found_terms = [term for term in common_terms if term in text.lower()]
        keywords.extend(found_terms[:top_n])
    
    # Remove duplicates while preserving order, limit to reasonable number
    seen = set()
    unique_keywords = []
    for kw in keywords:
        kw_lower = kw.lower()
        if kw_lower not in seen:
            seen.add(kw_lower)
            unique_keywords.append(kw)
    
    return unique_keywords[:10]  # Return top 10 keywords max

# ==========================
# CHUNK LARGE TEXT
# ==========================

def chunk_large_text(question, answer, max_size=MAX_CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    """
    Split large Q&A pairs into multiple chunks
    Each chunk contains the question + part of the answer
    
    Returns list of (question, answer_chunk) tuples
    """
    
    combined_text = f"Q: {question}\n\nA: {answer}"
    
    # If total size is under limit, return as single chunk
    if len(combined_text) <= max_size:
        return [(question, answer)]
    
    # Answer is too long - need to split
    chunks = []
    
    # Split answer into sentences for better boundaries
    try:
        sentences = nltk.sent_tokenize(answer)
    except:
        # Fallback: split by periods if nltk fails
        sentences = [s.strip() + '.' for s in answer.split('.') if s.strip()]
    
    current_chunk = ""
    
    for sentence in sentences:
        # Check if adding this sentence exceeds limit
        test_chunk = current_chunk + " " + sentence if current_chunk else sentence
        
        # Reserve space for question prefix
        question_prefix_size = len(question) + 10  # "Q: ...\n\nA: "
        
        if len(test_chunk) + question_prefix_size > max_size:
            # Save current chunk if not empty
            if current_chunk:
                chunks.append((question, current_chunk.strip()))
                
                # Start new chunk with overlap (last few sentences)
                overlap_text = current_chunk[-overlap:] if len(current_chunk) > overlap else current_chunk
                current_chunk = overlap_text + " " + sentence
            else:
                # Single sentence exceeds limit - force include it
                chunks.append((question, sentence.strip()))
                current_chunk = ""
        else:
            current_chunk = test_chunk
    
    # Add remaining text
    if current_chunk:
        chunks.append((question, current_chunk.strip()))
    
    return chunks if chunks else [(question, answer)]

# ==========================
# CREATE CHUNKS
# ==========================

def create_chunks_from_qa(row_id, question, answer, parent_doc):
    """
    Create one or more chunks from a Q&A pair
    Large answers are split into multiple chunks
    
    Note: row_id is for tracking only, NOT used for retrieval
    Users don't know question numbers - retrieval is purely semantic + legal metadata
    """
    
    if not question and not answer:
        return []
    
    # Get sub-chunks if text is large
    qa_chunks = chunk_large_text(question, answer)
    
    chunks = []
    
    for idx, (q_text, a_text) in enumerate(qa_chunks):
        
        # Format text
        if q_text and a_text:
            chunk_text = f"Q: {q_text}\n\nA: {a_text}"
        elif q_text:
            chunk_text = f"Q: {q_text}"
        else:
            chunk_text = a_text
        
        # Extract metadata from full Q&A (not just this chunk)
        full_text = f"{question} {answer}"
        keywords = extract_keywords(full_text)
        cross_refs = extract_cross_references(full_text)
        
        # Create unique ID
        if len(qa_chunks) == 1:
            chunk_id = str(uuid.uuid4())
        else:
            # Multiple chunks: add suffix
            chunk_id = f"{uuid.uuid4()}_part{idx+1}"
        
        chunk = {
            "id": chunk_id,
            "doc_type": DOC_TYPE,
            "parent_doc": parent_doc,
            "hierarchy_level": HIERARCHY_LEVEL,
            
            "structure": {
                "csv_row_id": row_id,  # For debugging/tracking ONLY - NOT used in retrieval
                "question": q_text,
                "answer": a_text,
                "is_multi_chunk": len(qa_chunks) > 1,
                "chunk_index": idx + 1,
                "total_chunks": len(qa_chunks)
            },
            
            "text": chunk_text,
            "keywords": keywords,
            "cross_references": cross_refs,
            
            "metadata": {
                "source": "CSV Q&A Dataset",
                "source_file": parent_doc,
                "row_id": row_id
            }
        }
        
        # Add section/subsection to metadata for exact_match compatibility
        if cross_refs["sections"]:
            # Parse first section for exact_match
            first_section = cross_refs["sections"][0]
            match = re.match(r'(\d+)(?:\((\d+[A-Za-z]*)\))?', first_section)
            if match:
                chunk["metadata"]["section_number"] = match.group(1)
                if match.group(2):  # Has subsection
                    chunk["metadata"]["subsection_number"] = match.group(2)
        
        # Add rule to metadata (handle rules with subsections like 96(10))
        if cross_refs["rules"]:
            first_rule = cross_refs["rules"][0]
            # Extract base rule number (before parentheses if any)
            rule_match = re.match(r'(\d+[A-Za-z]*)(?:\(.*\))?', first_rule)
            if rule_match:
                chunk["metadata"]["rule_number"] = rule_match.group(1)
                # Store full rule with subsection for reference
                chunk["metadata"]["rule_full"] = first_rule
        
        # Add HSN/SAC codes to metadata
        if cross_refs["hsn_codes"]:
            chunk["metadata"]["hsn_code"] = cross_refs["hsn_codes"][0]
        
        if cross_refs["sac_codes"]:
            chunk["metadata"]["sac_code"] = cross_refs["sac_codes"][0]
        
        # Add form number to metadata
        if cross_refs["forms"]:
            chunk["metadata"]["form_number"] = cross_refs["forms"][0]
        
        # Add notification to metadata
        if cross_refs["notifications"]:
            chunk["metadata"]["notification_number"] = cross_refs["notifications"][0]
        
        chunks.append(chunk)
    
    return chunks

# ==========================
# PROCESS CSV
# ==========================

def process_csv(csv_path):
    """
    Process CSV file and create chunks
    
    Required columns:
    - ID: For row tracking/debugging (NOT used in retrieval)
    - Query: Contains question + separator + answer
    """
    
    print(f"Processing {os.path.basename(csv_path)}")
    
    # Read CSV
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding='latin-1')
    
    # Validate required columns (only ID and Query needed)
    required_cols = {CSV_COLUMNS["id"], CSV_COLUMNS["query"]}
    available_cols = set(df.columns)
    
    if not required_cols.issubset(available_cols):
        missing = required_cols - available_cols
        print(f"❌ ERROR: Missing columns: {missing}")
        print(f"   Available columns: {list(df.columns)}")
        print(f"   Note: ID is required for tracking only, not for retrieval")
        return []
    
    parent_doc = os.path.splitext(os.path.basename(csv_path))[0]
    
    all_chunks = []
    skipped_rows = 0
    multi_chunk_count = 0
    
    print(f"\nProcessing {len(df)} rows...")
    
    for idx, row in df.iterrows():
        
        # Get values (only ID and Query are used)
        row_id = str(row[CSV_COLUMNS["id"]])
        query = str(row[CSV_COLUMNS["query"]]) if not pd.isna(row[CSV_COLUMNS["query"]]) else ""
        
        if not query.strip():
            skipped_rows += 1
            continue
        
        # Split Q&A
        question, answer = split_question_answer(query)
        
        # Remove question number from question (if any)
        question = remove_question_number(question)
        
        # Validate minimum length
        if len(question.strip()) < 5:
            skipped_rows += 1
            continue
        
        # Create chunks (title not needed)
        chunks = create_chunks_from_qa(row_id, question, answer, parent_doc)
        
        if len(chunks) > 1:
            multi_chunk_count += 1
        
        all_chunks.extend(chunks)
        
        # Progress indicator
        if (idx + 1) % 50 == 0:
            print(f"  Processed {idx + 1} rows...")
    
    print(f"\n✅ Created {len(all_chunks)} chunks from {len(df) - skipped_rows} valid Q&A pairs")
    print(f"   Multi-chunk Q&As: {multi_chunk_count}")
    print(f"   Skipped rows: {skipped_rows}")
    
    return all_chunks

# ==========================
# VALIDATION
# ==========================

def validate_chunks(chunks):
    """
    Validate chunk structure and quality
    """
    
    print("\n🔍 Validating chunks...")
    
    required_keys = {"id", "text", "doc_type", "structure", "keywords", "cross_references", "metadata"}
    
    invalid_count = 0
    warnings = []
    
    for i, chunk in enumerate(chunks):
        # Check required keys
        if not required_keys.issubset(chunk.keys()):
            invalid_count += 1
            continue
        
        # Check text length
        text = chunk.get("text", "")
        if len(text) < 10:
            warnings.append(f"Chunk {i}: Text too short ({len(text)} chars)")
        
        if len(text) > MAX_CHUNK_SIZE + 500:  # Allow some buffer
            warnings.append(f"Chunk {i}: Text too long ({len(text)} chars) - may affect retrieval")
        
        # Check for legal references in tax/legal content
        cross_refs = chunk.get("cross_references", {})
        has_legal_ref = any([
            cross_refs.get("sections"),
            cross_refs.get("rules"),
            cross_refs.get("notifications")
        ])
        
        keywords = chunk.get("keywords", [])
        
        # If no legal refs and no keywords, might be low quality
        if not has_legal_ref and len(keywords) < 2:
            warnings.append(f"Chunk {chunk['id'][:8]}...: Low metadata quality")
    
    print(f"   Valid chunks: {len(chunks) - invalid_count}")
    print(f"   Invalid chunks: {invalid_count}")
    print(f"   Warnings: {len(warnings)}")
    
    if warnings[:5]:  # Show first 5 warnings
        print("\n   Sample warnings:")
        for w in warnings[:5]:
            print(f"   - {w}")
    
    return invalid_count == 0

# ==========================
# MAIN
# ==========================

def main():
    
    print("=" * 70)
    print("CSV Q&A CHUNKER")
    print("=" * 70)
    
    # Check input file exists
    if not os.path.exists(INPUT_CSV):
        print(f"\n❌ ERROR: Input file not found: {INPUT_CSV}")
        print("\nPlease ensure your CSV file is at:")
        print(f"  {INPUT_CSV}")
        return
    
    # Process CSV
    chunks = process_csv(INPUT_CSV)
    
    if not chunks:
        print("\n❌ No chunks created!")
        return
    
    # Validate
    if not validate_chunks(chunks):
        print("\n⚠️  WARNING: Some chunks failed validation")
    
    # Save output
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(chunks, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Output saved to: {OUTPUT_FILE}")
    
    # Show sample chunk
    if chunks:
        print("\n" + "=" * 70)
        print("SAMPLE CHUNK")
        print("=" * 70)
        sample = chunks[0]
        print(json.dumps(sample, indent=2, ensure_ascii=False)[:1000] + "...")
    
    print("\n" + "=" * 70)
    print("DONE!")
    print("=" * 70)
    print("\nNext steps:")
    print("1. Review the output file")
    print("2. Run merge script to combine with other chunks")
    print("3. Rebuild vector store")

if __name__ == "__main__":
    main()