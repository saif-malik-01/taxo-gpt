import re
import logging

logger = logging.getLogger(__name__)


def normalize_citation(text: str) -> str:
    """Normalize citation for matching"""
    if not text:
        return ""
    
    text = text.lower()
    noise_words = ['no', 'number', 'of']
    for word in noise_words:
        text = re.sub(r'\b' + word + r'\b', '', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '', text)
    
    return text


def extract_core_case_number(text: str) -> str:
    """Extract core case number by removing date and normalizing"""
    if not text:
        return ""
    
    date_patterns = [
        r'\s+dated\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',
        r'\s+dt\.?\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',
        r'\s+on\s+\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}.*$',
    ]
    
    text_without_date = text
    for pattern in date_patterns:
        text_without_date = re.sub(pattern, '', text_without_date, flags=re.IGNORECASE)
    
    return normalize_citation(text_without_date)


def normalize_party_name(name: str) -> str:
    """
    Normalize party name for matching
    Removes titles, legal suffixes, extra whitespace
    """
    if not name:
        return ""
    
    name = name.lower()
    
    # Remove titles
    titles = [
        r'\bmr\.?\b', r'\bmrs\.?\b', r'\bms\.?\b', r'\bmiss\.?\b',
        r'\bdr\.?\b', r'\bprof\.?\b', r'\bhon\.?\b',
        r'\bjustice\.?\b', r'\bj\.?\b',
        r'\bsri\.?\b', r'\bsmt\.?\b', r'\bshri\.?\b',
        r'\bm/s\.?\b', r'\bmessrs\.?\b',
    ]
    
    for title in titles:
        name = re.sub(title, '', name, flags=re.IGNORECASE)
    
    # Remove legal suffixes
    suffixes = [
        r'\bpvt\.?\s*ltd\.?\b', r'\bprivate\s+limited\b',
        r'\bltd\.?\b', r'\blimited\b',
        r'\binc\.?\b', r'\bllc\.?\b', r'\bllp\.?\b',
        r'\bco\.?\b', r'\bcorp\.?\b',
    ]
    
    for suffix in suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
    # Clean up
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.strip()
    
    return name


# --- GLOBAL METADATA INDEX (Build once at startup/import) ---
_metadata_index = None

class MetadataIndex:
    def __init__(self, all_chunks):
        self.judgment_by_external_id = {} # external_id -> [chunks]
        self.by_citation = {} # normalized_citation -> external_id
        self.by_case_num = {} # normalized_case_num -> external_id
        
        # Statutory Indexing
        self.by_section = {} # (section, subsection) -> [chunks]
        self.by_rule = {}    # rule_number -> [chunks]
        self.by_hsn = {}     # hsn_code -> [chunks]
        self.by_sac = {}     # sac_code -> [chunks]
        
        logger.info("⚡ Building Metadata Index for Retrieval...")
        for chunk in all_chunks:
            # 1. Statutory/HSN indexing (for all chunk types)
            s_num = chunk.get("section_number")
            ss_num = str(chunk.get("subsection")) if chunk.get("subsection") else None
            r_num = chunk.get("rule_number")
            meta = chunk.get("metadata", {})
            hsn = meta.get("hsn_code")
            sac = meta.get("sac_code")
            
            if s_num:
                key = (s_num, ss_num)
                if key not in self.by_section: self.by_section[key] = []
                self.by_section[key].append(chunk)
            
            if r_num:
                if r_num not in self.by_rule: self.by_rule[r_num] = []
                self.by_rule[r_num].append(chunk)

            if hsn:
                if hsn not in self.by_hsn: self.by_hsn[hsn] = []
                self.by_hsn[hsn].append(chunk)

            if sac:
                if sac not in self.by_sac: self.by_sac[sac] = []
                self.by_sac[sac].append(chunk)

            # 2. Judgment indexing
            if chunk.get("chunk_type") == "judgment":
                external_id = meta.get("external_id")
                if not external_id:
                    continue
                    
                if external_id not in self.judgment_by_external_id:
                    self.judgment_by_external_id[external_id] = []
                self.judgment_by_external_id[external_id].append(chunk)
                
                if external_id not in self.by_citation:
                    raw_cit = meta.get("citation", "")
                    if raw_cit: self.by_citation[normalize_citation(raw_cit)] = external_id
                
                if external_id not in self.by_case_num:
                    raw_cn = meta.get("case_number", "")
                    if raw_cn: self.by_case_num[extract_core_case_number(raw_cn)] = external_id
        
        logger.info(f"✅ Index built: {len(self.judgment_by_external_id)} judgments, "
                    f"{len(self.by_section)} sections, {len(self.by_rule)} rules.")

def get_index(all_chunks):
    global _metadata_index
    if _metadata_index is None:
        _metadata_index = MetadataIndex(all_chunks)
    return _metadata_index

def find_matching_judgments(extracted: dict, all_chunks: list) -> dict:
    """
    Optimized judgment matching using MetadataIndex
    """
    index = get_index(all_chunks)
    
    exact_matches = {}
    partial_matches = {}
    
    extracted_citation = extracted.get("citation", "")
    extracted_case_num = extracted.get("case_number", "")
    party_names = extracted.get("party_names", [])
    
    citation_norm = normalize_citation(extracted_citation) if extracted_citation else None
    case_num_core = extract_core_case_number(extracted_case_num) if extracted_case_num else None
    party_names_norm = [normalize_party_name(name) for name in party_names if name]
    
    # === 1. CITATION LOOKUP (O(1)) ===
    if citation_norm and citation_norm in index.by_citation:
        external_id = index.by_citation[citation_norm]
        chunks = index.judgment_by_external_id[external_id]
        meta = chunks[0].get("metadata", {})
        exact_matches[external_id] = {
            "external_id": external_id,
            "matched_field": "citation",
            "matched_value": meta.get("citation", ""),
            "score": 1.0,
            "chunks": chunks,
            **meta
        }

    # === 2. CASE NUMBER LOOKUP (O(1)) ===
    if case_num_core and case_num_core in index.by_case_num:
        external_id = index.by_case_num[case_num_core]
        if external_id not in exact_matches:
            chunks = index.judgment_by_external_id[external_id]
            meta = chunks[0].get("metadata", {})
            exact_matches[external_id] = {
                "external_id": external_id,
                "matched_field": "case_number",
                "matched_value": meta.get("case_number", ""),
                "score": 1.0,
                "chunks": chunks,
                **meta
            }

    # === 3. PARTY NAMES (Linear scan over unique judgments only, not all chunks) ===
    if party_names_norm:
        for external_id, chunks in index.judgment_by_external_id.items():
            if external_id in exact_matches: continue
            
            meta = chunks[0].get("metadata", {})
            petitioner_norm = normalize_party_name(meta.get("petitioner", ""))
            respondent_norm = normalize_party_name(meta.get("respondent", ""))
            
            # Exact matches (1.0)
            matched = False
            for p_norm in party_names_norm:
                if len(p_norm) < 3: continue
                if p_norm == petitioner_norm:
                    exact_matches[external_id] = {"external_id": external_id, "matched_field": "petitioner", "matched_value": meta.get("petitioner"), "score": 1.0, "chunks": chunks, **meta}
                    matched = True; break
                if p_norm == respondent_norm:
                    exact_matches[external_id] = {"external_id": external_id, "matched_field": "respondent", "matched_value": meta.get("respondent"), "score": 1.0, "chunks": chunks, **meta}
                    matched = True; break
            
            if matched: continue

            # Partial matches (0.5)
            for p_norm in party_names_norm:
                if len(p_norm) < 3: continue
                if p_norm in petitioner_norm:
                    partial_matches[external_id] = {"external_id": external_id, "matched_field": "petitioner", "matched_value": meta.get("petitioner"), "score": 0.5, "chunks": chunks, **meta}
                    break
                if p_norm in respondent_norm:
                    partial_matches[external_id] = {"external_id": external_id, "matched_field": "respondent", "matched_value": meta.get("respondent"), "score": 0.5, "chunks": chunks, **meta}
                    break
    
    # === 4. BUILD FINAL RESULTS (O(Matches)) ===
    exact_match_list = list(exact_matches.values())
    partial_match_list = list(partial_matches.values())
    
    if not exact_match_list and not partial_match_list:
        logger.info("❌ NO MATCHES in metadata index")
    else:
        logger.info(f"✅ Indexed match found: {len(exact_match_list)} exact, {len(partial_match_list)} partial")
    
    return {
        "exact_matches": exact_match_list,
        "partial_matches": partial_match_list,
        "substring_matches": [] # Deprecated
    }
