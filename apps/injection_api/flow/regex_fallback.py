"""
pipeline/regex_fallback.py
Regex-based fallback extractor for sections, rules, notifications, circulars.
Used when LLM extraction fails or returns invalid JSON.
Only extracts these 4 fields — everything else requires LLM.
"""

import re
from typing import Dict, List

from utils.logger import get_logger

logger = get_logger("regex_fallback")

# ── Compiled patterns ─────────────────────────────────────────────────────────

_SEC_PAT = re.compile(
    r'(?:'
    r'sub[-\s]?section\s*\(([^)]+)\)\s*of\s*section\s*(\d+[A-Za-z]*)'   # sub-section (X) of section Y
    r'|clause\s*\([^)]+\)\s*of\s*section\s*(\d+[A-Za-z]*)'               # clause (X) of section Y
    r'|u\s*/\s*s\s*(\d+[A-Za-z]*)(?:\s*\(([^)]+)\))?'                    # u/s X or u/s X(Y)
    r'|sec(?:tion|t|\.?)?\s*\.?\s*(\d+[A-Za-z]*)(?:\s*\(([^)]+)\))?'    # section/sec/sect X or X(Y)
    r')',
    re.IGNORECASE,
)

_RULE_PAT = re.compile(
    r'(?:sub[-\s]?rule\s*\(([^)]+)\)\s*of\s*rule\s*(\d+[A-Za-z]*)'  # subrule (2) of Rule 90
    r'|\brule\s*\.?\s*(\d+[A-Za-z]*)(?:\s*\(([^)]+)\))?)',           # Rule 89 / Rule 89(2)
    re.IGNORECASE,
)

_NOTIF_PAT = re.compile(
    r'(?:notification|notif)\s*(?:no\.?)?\s*'
    r'(\d+\s*[/\-]\s*\d+)'
    r'(?:\s*[-–]\s*([A-Za-z][A-Za-z\s]{1,30}?))?'
    r'(?=\s+dated|\s+w\.e\.f|\s*[,\.\[\(]|$)',
    re.IGNORECASE,
)

_CIRC_PAT = re.compile(
    r'circular\s*(?:no\.?)?\s*(\d+[/\-]\d+(?:[/\-]\d+)?)',
    re.IGNORECASE,
)

# ── Token builders ────────────────────────────────────────────────────────────

def _clean(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', s.lower())


def _is_valid_subclause(s: str) -> bool:
    """
    Returns True only if s looks like a valid legal sub-clause reference.
    Valid: (a), (1), (1A), (ba), (xiii), (2b) — short, no spaces, starts with digit/letter
    Invalid: (pg.S), (pg.II), (pg.ll), (as a supplier of...), (construction of...)
    """
    if not s or len(s) > 8 or ' ' in s:
        return False
    # Must start with digit or letter — not "pg", punctuation etc.
    if not re.match(r'^[0-9a-zA-Z]', s):
        return False
    # Reject page-reference prefix
    if re.match(r'^pg', s, re.IGNORECASE):
        return False
    return True


def _section_from_groups(groups) -> str | None:
    sub, sec_sub, sec_clause, sec_us, sub_us, sec_plain, sub_plain = groups

    if sec_sub and sub:
        return f"section_{_clean(sec_sub)}_{_clean(sub)}"

    if sec_clause:
        return f"section_{_clean(sec_clause)}"

    if sec_us:
        base = f"section_{_clean(sec_us)}"
        return f"{base}_{_clean(sub_us)}" if sub_us else base

    if sec_plain:
        # Extract ONLY number + max 2-letter suffix — stops at title text
        # "9compositionlevy" → "9"  |  "14A" → "14a"  |  "11AC" → "11ac"
        import re as _re
        num_m = _re.match(r"(\d+[A-Za-z]{0,2})(?=[^A-Za-z]|$)", sec_plain)
        if not num_m:
            return None
        sec_clean = _clean(num_m.group(1))
        if sub_plain:
            sub_stripped = sub_plain.strip()
            # Valid sub-clauses: short (≤8), no spaces, start with digit/letter
            # Rejects: (pg.S), (pg.II), (pg.ll), title phrases
            if _is_valid_subclause(sub_stripped):
                return f"section_{sec_clean}_{_clean(sub_stripped)}"
        return f"section_{sec_clean}"

    return None


def _rule_from_match(m: re.Match) -> str | None:
    """
    Groups from _RULE_PAT:
      0: subrule sub-number (if "subrule (X) of rule Y" form)
      1: rule number        (if "subrule (X) of rule Y" form)
      2: rule number        (if plain "Rule X" form)
      3: sub-bracket        (if plain "Rule X(Y)" form)
    """
    sub_of, rule_of, num_plain, sub_plain = m.groups()

    if rule_of:
        # subrule (2) of Rule 90 → rule_90_2
        num_str = rule_of
        sub_str = sub_of or ""
    else:
        num_str = num_plain or ""
        sub_str = sub_plain or ""

    if not num_str:
        return None

    # Guard: Indian GST rules max ~164, IT Rules max ~134
    # Numbers > 200 are almost certainly false positives
    digits = re.match(r"(\d+)", num_str)
    if digits and int(digits.group(1)) > 200:
        return None

    # Rule 89(2) → rule_89_2  (underscore separator — not rule_892)
    if sub_str:
        sub_stripped = sub_str.strip()
        if _is_valid_subclause(sub_stripped):
            return f"rule_{_clean(num_str)}_{_clean(sub_stripped)}"
        # Title or page ref in brackets — return just the rule number
        return f"rule_{_clean(num_str)}"
    return f"rule_{_clean(num_str)}"


def _notif_from_match(m: re.Match) -> str:
    num = re.sub(r'\s*', '', m.group(1))
    num = re.sub(r'[/\-]', '_', num)
    typ = m.group(2)
    if typ:
        typ = re.sub(r'[^a-z0-9]', '_', typ.strip().lower())
        typ = re.sub(r'_+', '_', typ).strip('_')
        return f"notification_{num}_{typ}"
    return f"notification_{num}"


def _circ_from_match(m: re.Match) -> str:
    num = re.sub(r'[^a-z0-9_]', '_', m.group(1).lower())
    num = re.sub(r'_+', '_', num).strip('_')
    return f"circular_{num}"


# ── Public extractor ──────────────────────────────────────────────────────────

def extract_fallback(text: str) -> Dict[str, List[str]]:
    """
    Full fallback when LLM fails.

    Two things happen:
      1. Entire raw text is whitespace-split — every token added to BM25.
         This ensures authority names, legal terms, amounts, dates (raw form)
         all get indexed even without LLM normalisation.

      2. Regex patterns run on top to extract and NORMALISE:
         sections, rules, notifications, circulars
         These replace their raw whitespace-split forms with canonical tokens.

    Result: maximum coverage from raw text + normalised legal refs.
    """
    if not text or not text.strip():
        return _empty()

    # ── Step 1: Whitespace split entire text ─────────────────────────
    # Clean and split — everything goes into BM25 as raw tokens
    # Strip markdown, footnote markers, special chars but keep meaningful content
    cleaned = re.sub(r'[\[\]_*#]', ' ', text)          # remove markdown/footnote markers
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    raw_tokens_raw = cleaned.split()
    raw_tokens = []
    for t in raw_tokens_raw:
        # Strip common punctuation from edges
        t = t.strip('.,;:()"\'\-_[]{}!?')
        # Remove trailing/leading brackets that got glued to words: "2(appeal" → "2"
        # If token contains ( and the part before ( is meaningful, keep only that
        if '(' in t and not t.startswith('('):
            t = t[:t.index('(')].strip('.,;:')
        if ')' in t and not t.endswith(')'):
            t = t[t.rindex(')')+1:].strip('.,;:') or t[:t.rindex(')')].strip('.,;:')
        t = t.strip('.,;:()"\'_')
        if t and len(t) > 1:
            raw_tokens.append(t)

    # ── Step 2: Regex extract normalised legal refs ───────────────────
    sections      = []
    rules         = []
    notifications = []
    circulars     = []
    seen          = set()

    # Track positions of normalised matches so we can
    # exclude their raw tokens from whitespace split output
    # (normalised form replaces raw form — no duplication)
    normalised_raw_words = set()

    for m in _SEC_PAT.finditer(text):
        tok = _section_from_groups(m.groups())
        if tok and tok not in seen:
            seen.add(tok)
            sections.append(tok)
            # Mark raw words covered by this match
            for w in m.group(0).lower().split():
                normalised_raw_words.add(w.strip('.,;:()\'"'))

    for m in _RULE_PAT.finditer(text):
        tok = _rule_from_match(m)
        if tok and tok not in seen:          # tok can be None from >200 guard
            seen.add(tok)
            rules.append(tok)
            for w in m.group(0).lower().split():
                normalised_raw_words.add(w.strip('.,;:()\'"'))

    for m in _NOTIF_PAT.finditer(text):
        tok = _notif_from_match(m)
        if tok not in seen:
            seen.add(tok)
            notifications.append(tok)
            for w in m.group(0).lower().split():
                normalised_raw_words.add(w.strip('.,;:()\'"'))

    for m in _CIRC_PAT.finditer(text):
        tok = _circ_from_match(m)
        if tok not in seen:
            seen.add(tok)
            circulars.append(tok)
            for w in m.group(0).lower().split():
                normalised_raw_words.add(w.strip('.,;:()\'"'))

    # ── Step 3: Build final raw token list ───────────────────────────
    # Exclude tokens already covered by normalised legal refs
    # to avoid having both "section" "14a" AND section_14a in the index
    STOPWORDS = {
        # Articles / prepositions / conjunctions
        'the','of','and','in','to','a','an','for','or','is','are',
        'was','be','by','on','at','as','it','its','with','from',
        'that','this','which','not','also','such','shall','may',
        'any','all','where','has','have','been','no','if','but',
        'who','whom','their','he','she','they','we','i','you',
        # Legal boilerplate
        'vide','wef','viz','ie','eg','ibid','supra','infra',
        'w.e.f','viz.','i.e','i.e.',
        # High-frequency low-value words (from corpus analysis)
        'quite','see','why','once','take','read','upon','another',
        'wherein','itself','one','being','had','were','before',
        'there','only','through','should','without','present',
        'view','against','held','facts','fact','matter','time',
        'whether','however','further','therefore','thus','hence',
        'whereas','whereby','therein','thereafter','thereto',
        'herein','hereof','hereto','herewith','thereof',
        'said','same','above','below','following','respective',
        'mentioned','stated','referred','noted','aforesaid',
    }

    final_raw = []
    for tok in raw_tokens:
        tl = tok.lower()
        if tl in STOPWORDS:
            continue
        if len(tl) <= 1:
            continue
        if tl in normalised_raw_words:
            continue        # already represented by normalised token
        final_raw.append(tl)

    total_normalised = len(sections) + len(rules) + len(notifications) + len(circulars)
    logger.debug(
        f"  Regex fallback: {len(final_raw)} raw tokens + "
        f"{total_normalised} normalised "
        f"(sec:{len(sections)} rule:{len(rules)} "
        f"notif:{len(notifications)} circ:{len(circulars)})"
    )

    result = _empty()
    result["sections"]      = sections
    result["rules"]         = rules
    result["notifications"] = notifications
    result["circulars"]     = circulars
    # Raw whitespace tokens go into topics — merger handles them as L3 tokens
    # weight 1x — lower than L1 metadata but still indexed
    result["topics"]        = final_raw
    return result


def _empty() -> Dict[str, List[str]]:
    return {
        "sections": [], "rules": [], "notifications": [], "circulars": [],
        "acts": [], "case_citations": [], "authorities": [], "dates": [],
        "assessment_years": [], "financial_years": [], "financial_amounts": [],
        "legal_concepts": [], "topics": [], "parties": [], "person_names": [],
        "hypothetical_queries": [],
    }