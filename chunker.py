"""
GST Judgments RAG Chunking Pipeline — AWS Bedrock
=================================================
Titan-optimized hierarchical chunking for GST judgments.

Reads judgments.csv (21-column format) and produces UKC chunks.

FIXES INCLUDED
--------------
- Handles very large CSV fields (full judgments)
- Robust multiline quoted CSV parsing
- Better encoding fallback
- Titan-optimized chunking
- Overview + multiple order child chunks

WHY THIS VERSION IS BETTER
--------------------------
The previous approach stored the full judgment order as one giant chunk.
That is suboptimal for embedding retrieval (especially Titan embeddings),
because large chunks dilute semantic similarity.

This version uses:

CHUNK STRATEGY: TITAN-OPTIMIZED HIERARCHICAL CHUNKING
-----------------------------------------------------
  Chunk 1 — OVERVIEW (1 chunk per judgment)
    Text = Case Note + metadata
    Purpose = high-value retrieval anchor

  Chunk 2+ — ORDER CHILD CHUNKS (N chunks per judgment)
    Text = paragraph-aware splits of Judgement Description
    Target size = ~300 tokens
    Overlap = ~40 tokens
    Purpose = precise semantic retrieval with Titan embeddings
"""

import argparse
import csv
import json
import logging
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# IMPORTANT: allow huge CSV fields (full legal judgments)
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2147483647)

_missing = []
try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
except ImportError:
    _missing.append("boto3")
try:
    from dotenv import load_dotenv
except ImportError:
    _missing.append("python-dotenv")

if _missing:
    print(f"ERROR: Missing: {', '.join(_missing)}\n  pip install {' '.join(_missing)}")
    sys.exit(1)

load_dotenv()
logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)-7s %(message)s")
logger = logging.getLogger("judgment_pipeline")


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_MODEL  = "qwen.qwen3-next-80b-a3b"
DEFAULT_REGION = "us-east-1"

MODEL_PRICING = {
    "amazon.nova-pro-v1:0":                     {"input": 0.80, "output": 3.20, "name": "Nova Pro"},
    "amazon.nova-lite-v1:0":                    {"input": 0.06, "output": 0.24, "name": "Nova Lite"},
    "anthropic.claude-3-5-haiku-20241022-v1:0": {"input": 0.80, "output": 4.00, "name": "Claude Haiku"},
    "qwen.qwen3-next-80b-a3b":                  {"input": 0.72, "output": 0.72, "name": "Qwen3-80B"},
    "meta.llama3-3-70b-instruct-v1:0":          {"input": 0.72, "output": 0.72, "name": "Llama3.3-70B"},
}

# Titan embedding optimized chunking
TARGET_CHUNK_TOKENS = 300
CHUNK_OVERLAP_TOKENS = 40

# Approx char mapping for English legal text
MAX_CHUNK_CHARS = 1400
OVERLAP_CHARS = 180
MIN_CHUNK_CHARS = 500


REQUIRED_COLUMNS = [
    "ID",
    "Title",
    "Citation",
    "Case Number",
    "Petitioner/Appellant Title",
    "Respondent Title",
    "Year of Judgement",
    "Act Name",
    "Section Number",
    "Rule Name",
    "Rule Number",
    "Notification / Circular Number",
    "Judge Name",
    "Decision",
    "Court",
    "State",
    "Case Note",
    "Judgement Description",
    "Current Status",
]


# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a GST legal data extraction engine specializing in court judgments.
Output ONLY a valid JSON object (not an array) with exactly two fields:
  "summary"  : string  — 2-4 sentences written for a GST practitioner
  "keywords" : array   — 10-15 terms

No markdown fences. No commentary. Pure JSON only.
Start with {  End with }

For summary: cover the legal issue decided, the court's holding, sections involved, and practical impact.
For keywords: include taxpayer type, sections, legal principle, decision outcome, GST topic, court name."""


# ─────────────────────────────────────────────────────────────────────────────
# PROMPT TEMPLATES
# ─────────────────────────────────────────────────────────────────────────────

OVERVIEW_PROMPT = """\
Generate summary and keywords for this GST judgment overview.

Case: {title}
Citation: {citation}
Court: {court}, {state}
Judge: {judge}
Decision: {decision}
Petitioner: {petitioner}
Respondent: {respondent}
Sections: {sections}
Rules: {rules}
Notifications: {notifications}
Case Summary:
{case_note}

Return JSON with "summary" (2-3 sentences) and "keywords" (10-15 terms)."""


ORDER_PROMPT = """\
Generate summary and keywords for this GST court order/judgment excerpt.

Case: {title}  ({citation})
Court: {court}, {state}  |  Decision: {decision}
Sections involved: {sections}

Order Excerpt:
{order_text}

Return JSON with "summary" (2-4 sentences covering: legal issue, court's reasoning, holding, practical impact) and "keywords" (10-15 terms)."""


# ─────────────────────────────────────────────────────────────────────────────
# CSV PARSING HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_sections(s: str) -> list:
    if not s.strip():
        return []
    cleaned = re.sub(r'\bSection[s]?\b', '', s, flags=re.I).strip()
    parts   = re.split(r'[\s,&/]+', cleaned)
    return [p.strip() for p in parts
            if p.strip() and re.match(r'^\d+[A-Z]?(\(\d+\))?$', p.strip())]


def _parse_rules(rule_name: str, rule_num: str) -> list:
    if not rule_num.strip():
        return []
    cleaned = re.sub(r'\bRule[s]?\b', '', rule_num, flags=re.I).strip()
    parts   = re.split(r'[\s,&/]+', cleaned)
    return [p.strip() for p in parts
            if p.strip() and re.match(r'^\d+[A-Z]?$', p.strip())]


def _parse_notifications(s: str) -> list:
    if not s.strip():
        return []
    parts = re.split(r'\s*[;&]\s*', s.strip())
    return [p.strip() for p in parts if p.strip()]


def _decision_to_query_categories(decision: str) -> list:
    d = decision.lower()
    if any(w in d for w in ['interim', 'stay', 'injunction']):
        return ['notice_defence', 'appeal_procedure']
    if any(w in d for w in ['favour of assessee', 'allowed', 'quashed', 'set aside']):
        return ['notice_defence', 'applicability', 'historical']
    if any(w in d for w in ['remand', 'fresh']):
        return ['appeal_procedure', 'compliance_procedure']
    if any(w in d for w in ['favour of revenue', 'dismissed', 'upheld']):
        return ['compliance_procedure', 'applicability']
    return ['general_information', 'applicability']


def _extract_date_from_case_number(case_num: str) -> str | None:
    m = re.search(r'dated\s+(\d{2}[./-]\d{2}[./-]\d{4})', case_num, re.I)
    if m:
        d = m.group(1).replace('/', '-').replace('.', '-')
        parts = d.split('-')
        if len(parts) == 3:
            return f"{parts[0]}-{parts[1]}-{parts[2]}"
    return None


def _boost_score(decision: str, is_order: bool) -> float:
    d = decision.lower()
    base = 0.90 if is_order else 0.87
    if any(w in d for w in ['favour of assessee', 'quashed', 'set aside', 'allowed']):
        return min(base + 0.05, 0.98)
    if any(w in d for w in ['remand', 'interim', 'stay']):
        return min(base + 0.03, 0.98)
    if any(w in d for w in ['favour of revenue', 'dismissed']):
        return min(base + 0.02, 0.98)
    return base


def _infer_tax_type(act_name: str, sections: list) -> str:
    a = act_name.lower()
    if 'integrated' in a or 'igst' in a:
        return 'IGST'
    if 'union territory' in a or 'utgst' in a:
        return 'UTGST'
    if 'state' in a or 'sgst' in a:
        return 'SGST'
    return 'CGST'


def _extract_judgment_date(title: str, case_num: str, year: str) -> str | None:
    m = re.search(r'(\d{2})[./-](\d{2})[./-](\d{4})', title)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return _extract_date_from_case_number(case_num)


def split_order_text(text: str,
                     max_chars: int = MAX_CHUNK_CHARS,
                     overlap_chars: int = OVERLAP_CHARS,
                     min_chunk_chars: int = MIN_CHUNK_CHARS) -> list[str]:
    """
    Titan-optimized paragraph-aware chunking for legal judgments.
    """
    if not text or not text.strip():
        return []

    text = re.sub(r'\r\n?', '\n', text).strip()

    paras = [p.strip() for p in re.split(r'\n\s*\n+', text) if p.strip()]
    if not paras:
        return [text]

    chunks = []
    current = []

    def current_len(parts):
        return sum(len(p) for p in parts) + max(0, len(parts) - 1) * 2

    def add_chunk(parts):
        chunk = "\n\n".join(parts).strip()
        if chunk:
            chunks.append(chunk)

    for para in paras:
        if len(para) > max_chars:
            sentences = re.split(r'(?<=[.!?])\s+', para)
            for sent in sentences:
                sent = sent.strip()
                if not sent:
                    continue
                if current and current_len(current + [sent]) > max_chars:
                    add_chunk(current)
                    prev = chunks[-1] if chunks else ""
                    overlap = prev[-overlap_chars:] if overlap_chars and prev else ""
                    current = [overlap, sent] if overlap else [sent]
                else:
                    current.append(sent)
            continue

        if current and current_len(current + [para]) > max_chars:
            add_chunk(current)
            prev = chunks[-1] if chunks else ""
            overlap = prev[-overlap_chars:] if overlap_chars and prev else ""
            current = [overlap, para] if overlap else [para]
        else:
            current.append(para)

    if current:
        add_chunk(current)

    merged = []
    for ch in chunks:
        if merged and len(ch) < min_chunk_chars:
            merged[-1] += "\n\n" + ch
        else:
            merged.append(ch)

    return [c.strip() for c in merged if c.strip()]


# ─────────────────────────────────────────────────────────────────────────────
# CSV LOADER (FIXED)
# ─────────────────────────────────────────────────────────────────────────────

def load_csv(path: str) -> list:
    """
    Robust CSV loader for very large GST judgment files.
    Handles:
      - huge fields
      - multiline quoted cells
      - encoding fallback
      - malformed blank rows
    """
    p = Path(path)

    if not p.exists():
        print(f"ERROR: Input file not found: {path}")
        return []

    if p.stat().st_size == 0:
        print(f"ERROR: Input file is empty: {path}")
        return []

    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]

    for enc in encodings:
        try:
            with open(path, mode="r", encoding=enc, newline="") as f:
                reader = csv.DictReader(
                    f,
                    delimiter=",",
                    quotechar='"',
                    doublequote=True,
                    skipinitialspace=False
                )

                rows = []
                for i, row in enumerate(reader, start=2):
                    if not row:
                        continue

                    cleaned = {}
                    for k, v in row.items():
                        if k is None:
                            continue
                        key = str(k).strip()
                        val = str(v).strip() if v is not None else ""
                        cleaned[key] = val

                    if not any(cleaned.values()):
                        continue

                    rows.append(cleaned)

                if rows:
                    print(f"Loaded CSV using encoding={enc} | rows={len(rows)}")
                    print(f"Detected columns: {list(rows[0].keys())}")
                    return rows

        except Exception as e:
            print(f"[WARN] Failed reading with encoding={enc}: {e}")
            continue

    print(f"ERROR: Could not parse CSV file: {path}")
    return []


def validate_csv_columns(rows: list):
    if not rows:
        raise ValueError("CSV has no rows")

    cols = set(rows[0].keys())
    missing = [c for c in REQUIRED_COLUMNS if c not in cols]

    if missing:
        raise ValueError(
            f"CSV header mismatch. Missing columns: {missing}\n"
            f"Found columns: {list(rows[0].keys())}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# CSV ROW → STRUCTURED METADATA
# ─────────────────────────────────────────────────────────────────────────────

def row_to_metadata(row: dict) -> dict:
    case_id      = row.get('ID', '').strip()
    title        = row.get('Title', '').strip()
    citation     = row.get('Citation', '').strip()
    case_num     = row.get('Case Number', '').strip()
    petitioner   = row.get('Petitioner/Appellant Title', '').strip()
    respondent   = row.get('Respondent Title', '').strip()
    year         = row.get('Year of Judgement', '').strip()
    act_name     = row.get('Act Name', '').strip()
    section_raw  = row.get('Section Number', '').strip()
    rule_name    = row.get('Rule Name', '').strip()
    rule_num     = row.get('Rule Number', '').strip()
    notif_raw    = row.get('Notification / Circular Number', '').strip()
    judge        = row.get('Judge Name', '').strip()
    decision     = row.get('Decision', '').strip()
    court        = row.get('Court', '').strip()
    state        = row.get('State', '').strip()
    case_note    = row.get('Case Note', '').strip()
    order_text   = row.get('Judgement Description', '').strip()
    status       = row.get('Current Status', '').strip()

    sections      = _parse_sections(section_raw)
    rules         = _parse_rules(rule_name, rule_num)
    notifications = _parse_notifications(notif_raw)
    jud_date      = _extract_judgment_date(title, case_num, year)
    tax_type      = _infer_tax_type(act_name, sections)

    safe_title = re.sub(r'[^\w\-]', '-', title.lower())[:40].strip('-')
    id_pfx     = f"judg-{case_id}-{safe_title}" if case_id else f"judg-{safe_title}"

    return {
        "case_id":      case_id,
        "title":        title,
        "citation":     citation,
        "case_num":     case_num,
        "petitioner":   petitioner,
        "respondent":   respondent,
        "year":         year,
        "act_name":     act_name,
        "section_raw":  section_raw,
        "rule_name":    rule_name,
        "rule_num":     rule_num,
        "notif_raw":    notif_raw,
        "judge":        judge,
        "decision":     decision,
        "court":        court,
        "state":        state,
        "case_note":    case_note,
        "order_text":   order_text,
        "status":       status,
        "sections":     sections,
        "rules":        rules,
        "notifications": notifications,
        "jud_date":     jud_date,
        "tax_type":     tax_type,
        "id_pfx":       id_pfx,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CHUNK BUILDERS
# ─────────────────────────────────────────────────────────────────────────────

def build_overview_chunk(meta: dict, summary: str, keywords: list,
                         today: str, source_file: str) -> dict:
    m = meta

    text_parts = [
        f"JUDGMENT: {m['title']}",
        f"Citation: {m['citation']}",
        f"Case Number: {m['case_num']}",
        f"Court: {m['court']}, {m['state']}",
        f"Judge: {m['judge']}",
        f"Year: {m['year']}",
        f"Petitioner: {m['petitioner']}",
        f"Respondent: {m['respondent']}",
        f"Decision: {m['decision']}",
    ]
    if m['section_raw']:
        text_parts.append(f"Sections: {m['section_raw']}")
    if m['rule_name'] or m['rule_num']:
        text_parts.append(f"Rule: {m['rule_name']} {m['rule_num']}".strip())
    if m['notif_raw']:
        text_parts.append(f"Notifications/Circulars: {m['notif_raw']}")
    if m['case_note']:
        text_parts.append(f"\nSUMMARY:\n{m['case_note']}")

    return {
        "id":           f"{m['id_pfx']}-overview",
        "chunk_type":   "judgment",
        "parent_doc":   m['title'],
        "chunk_index":  1,
        "total_chunks": 1,
        "text":         "\n".join(text_parts),
        "summary":      summary,
        "keywords":     keywords,
        "authority": {
            "level":         5,
            "label":         "Judicial Interpretations",
            "is_statutory":  False,
            "is_binding":    False,
            "can_be_cited":  True,
        },
        "temporal": {
            "effective_date":  m['jud_date'],
            "superseded_date": None,
            "is_current":      True,
            "financial_year":  None,
        },
        "legal_status": {
            "is_disputed":    False,
            "dispute_note":   None,
            "current_status": m['status'] if m['status'] else "active",
            "overruled_by":   None,
        },
        "cross_references": {
            "sections":      m['sections'],
            "rules":         m['rules'],
            "notifications": m['notifications'],
            "circulars":     [],
            "forms":         [],
            "hsn_codes":     [],
            "sac_codes":     [],
            "judgment_ids":  [],
            "parent_chunk_id": None,
        },
        "retrieval": {
            "primary_topics":  [m['decision'], m['court']] + m['sections'][:3],
            "tax_type":        m['tax_type'],
            "applicable_to":   "both",
            "query_categories": _decision_to_query_categories(m['decision']),
            "boost_score":     _boost_score(m['decision'], is_order=False),
        },
        "provenance": {
            "source_file":    source_file,
            "page_range":     None,
            "ingestion_date": today,
            "version":        "2.1",
        },
        "ext": {
            "case_id":          m['case_id'],
            "citation":         m['citation'],
            "case_number":      m['case_num'],
            "petitioner":       m['petitioner'],
            "respondent":       m['respondent'],
            "year_of_judgment": m['year'],
            "judgment_date":    m['jud_date'],
            "judge_name":       m['judge'],
            "court":            m['court'],
            "state":            m['state'],
            "decision":         m['decision'],
            "act_name":         m['act_name'],
            "section_raw":      m['section_raw'],
            "rule_name":        m['rule_name'],
            "rule_number":      m['rule_num'],
            "notification_circular": m['notif_raw'],
            "hierarchy_level":  5,
            "chunk_subtype":    "judgment_overview",
            "has_full_order":   bool(m['order_text']),
            "full_order_chunk_prefix": f"{m['id_pfx']}-order-",
        },
    }


def build_order_chunk(meta: dict, summary: str, keywords: list,
                      order_text: str, chunk_index: int, total_chunks: int,
                      today: str, source_file: str, parent_chunk_id: str | None = None) -> dict:
    m = meta

    header = (
        f"FULL ORDER: {m['title']}\n"
        f"Citation: {m['citation']}  |  Court: {m['court']}, {m['state']}  |  "
        f"Decision: {m['decision']}\n\n"
    )

    return {
        "id":           f"{m['id_pfx']}-order-{chunk_index}",
        "chunk_type":   "judgment",
        "parent_doc":   m['title'],
        "chunk_index":  chunk_index,
        "total_chunks": total_chunks,
        "text":         header + order_text,
        "summary":      summary,
        "keywords":     keywords,
        "authority": {
            "level":         5,
            "label":         "Judicial Interpretations",
            "is_statutory":  False,
            "is_binding":    False,
            "can_be_cited":  True,
        },
        "temporal": {
            "effective_date":  m['jud_date'],
            "superseded_date": None,
            "is_current":      True,
            "financial_year":  None,
        },
        "legal_status": {
            "is_disputed":    False,
            "dispute_note":   None,
            "current_status": m['status'] if m['status'] else "active",
            "overruled_by":   None,
        },
        "cross_references": {
            "sections":      m['sections'],
            "rules":         m['rules'],
            "notifications": m['notifications'],
            "circulars":     [],
            "forms":         [],
            "hsn_codes":     [],
            "sac_codes":     [],
            "judgment_ids":  [],
            "parent_chunk_id": parent_chunk_id or f"{m['id_pfx']}-overview",
        },
        "retrieval": {
            "primary_topics":  [m['decision'], m['court']] + m['sections'][:3],
            "tax_type":        m['tax_type'],
            "applicable_to":   "both",
            "query_categories": _decision_to_query_categories(m['decision']),
            "boost_score":     _boost_score(m['decision'], is_order=True),
        },
        "provenance": {
            "source_file":    source_file,
            "page_range":     None,
            "ingestion_date": today,
            "version":        "2.1",
        },
        "ext": {
            "case_id":          m['case_id'],
            "citation":         m['citation'],
            "case_number":      m['case_num'],
            "petitioner":       m['petitioner'],
            "respondent":       m['respondent'],
            "year_of_judgment": m['year'],
            "judgment_date":    m['jud_date'],
            "judge_name":       m['judge'],
            "court":            m['court'],
            "state":            m['state'],
            "decision":         m['decision'],
            "act_name":         m['act_name'],
            "section_raw":      m['section_raw'],
            "rule_name":        m['rule_name'],
            "rule_number":      m['rule_num'],
            "notification_circular": m['notif_raw'],
            "hierarchy_level":  5,
            "chunk_subtype":    "judgment_order_part",
            "order_text_len":   len(order_text),
            "overview_chunk_id": f"{m['id_pfx']}-overview",
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# JSON EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_summary_keywords(raw: str) -> tuple:
    text  = raw.strip()
    text2 = re.sub(r"```(?:json)?\s*", "", text).strip()

    for t in (text, text2):
        try:
            obj = json.loads(t)
            if isinstance(obj, dict):
                summary  = obj.get("summary", "")
                keywords = obj.get("keywords", [])
                if isinstance(keywords, str):
                    keywords = [k.strip() for k in keywords.split(",")]
                return str(summary), list(keywords)
        except Exception:
            pass

    s_m = re.search(r'"summary"\s*:\s*"([^"]{20,})"', text, re.DOTALL)
    k_m = re.search(r'"keywords"\s*:\s*\[([^\]]+)\]', text)
    summary  = s_m.group(1).strip() if s_m else ""
    keywords = []
    if k_m:
        keywords = [k.strip().strip('"') for k in k_m.group(1).split(",") if k.strip()]

    logger.warning(f"extract_summary_keywords fallback. Raw[:100]: {raw[:100]}")
    return summary, keywords


# ─────────────────────────────────────────────────────────────────────────────
# BEDROCK
# ─────────────────────────────────────────────────────────────────────────────

class ThrottlingError(Exception):
    def __init__(self, msg, retry_after=30):
        super().__init__(msg)
        self.retry_after = retry_after

class BedrockAPIError(Exception):
    pass


def _make_client(region, timeout):
    cfg = Config(
        region_name=region,
        read_timeout=timeout,
        connect_timeout=30,
        retries={"max_attempts": 3, "mode": "adaptive"}
    )
    return boto3.client(service_name="bedrock-runtime", config=cfg)


def _converse(client, model_id, max_tokens, user_content):
    try:
        resp = client.converse(
            modelId=model_id,
            system=[{"text": SYSTEM_PROMPT}],
            messages=[{"role": "user", "content": [{"text": user_content}]}],
            inferenceConfig={"maxTokens": max_tokens, "temperature": 0.1, "topP": 0.9},
        )
    except ClientError as e:
        code, msg = e.response["Error"]["Code"], e.response["Error"]["Message"]
        if code in ("ThrottlingException", "TooManyRequestsException"):
            raise ThrottlingError(f"{code}: {msg}", retry_after=30)
        if code == "ModelNotReadyException":
            raise ThrottlingError(msg, retry_after=60)
        if code in ("ValidationException", "AccessDeniedException",
                    "ResourceNotFoundException", "UnrecognizedClientException"):
            raise BedrockAPIError(f"{code}: {msg}")
        raise BedrockAPIError(f"ClientError [{code}]: {msg}")

    usage = resp.get("usage", {})
    return resp["output"]["message"]["content"][0]["text"].strip(), {
        "prompt_tokens":     usage.get("inputTokens", 0),
        "completion_tokens": usage.get("outputTokens", 0),
        "total_tokens":      usage.get("totalTokens", 0),
    }


def _call_with_retry(client, model_id, max_tokens, prompt, retries, label, wid):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            raw, usage    = _converse(client, model_id, max_tokens, prompt)
            summary, kws  = extract_summary_keywords(raw)
            return summary, kws, usage
        except ThrottlingError as e:
            last_err = e
            print(f"  [W{wid}] THROTTLED {e.retry_after}s [{label} {attempt}/{retries}]")
            time.sleep(e.retry_after)
        except BedrockAPIError:
            raise
        except Exception as e:
            last_err = e
            if attempt < retries:
                wait = 2 ** (attempt - 1)
                print(f"  [W{wid}] RETRY {attempt}/{retries} [{label}]: {e} ({wait}s)")
                time.sleep(wait)
    raise last_err


def ping_bedrock(region, model_id, timeout):
    try:
        c = _make_client(region, timeout)
        c.converse(
            modelId=model_id,
            messages=[{"role": "user", "content": [{"text": "hi"}]}],
            inferenceConfig={"maxTokens": 1, "temperature": 0.0}
        )
        return True, ""
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("ValidationException", "ResourceNotFoundException",
                    "ModelNotReadyException", "ServiceQuotaExceededException",
                    "ThrottlingException"):
            return True, ""
        return False, f"{code}: {e.response['Error']['Message']}"
    except (NoCredentialsError, PartialCredentialsError) as e:
        return False, f"No credentials: {e}"
    except Exception:
        return True, ""


# ─────────────────────────────────────────────────────────────────────────────
# PROCESS ONE JUDGMENT
# ─────────────────────────────────────────────────────────────────────────────

def process_judgment(row: dict, index: int, today: str, source_file: str,
                     model_id: str, region: str, max_tokens: int,
                     retries: int, timeout: int, worker_id: int) -> tuple:
    client = _make_client(region, timeout)
    stats  = {"api_calls": 0, "input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    chunks = []

    meta = row_to_metadata(row)

    def _call(prompt, label):
        summary, kws, usage = _call_with_retry(
            client, model_id, max_tokens, prompt, retries, label, worker_id)
        stats["api_calls"]     += 1
        stats["input_tokens"]  += usage.get("prompt_tokens", 0)
        stats["output_tokens"] += usage.get("completion_tokens", 0)
        stats["total_tokens"]  += usage.get("total_tokens", 0)
        return summary, kws

    ov_prompt = OVERVIEW_PROMPT.format(
        title         = meta["title"],
        citation      = meta["citation"],
        court         = meta["court"],
        state         = meta["state"],
        judge         = meta["judge"],
        decision      = meta["decision"],
        petitioner    = meta["petitioner"],
        respondent    = meta["respondent"],
        sections      = meta["section_raw"] or "N/A",
        rules         = f"{meta['rule_name']} {meta['rule_num']}".strip() or "N/A",
        notifications = meta["notif_raw"] or "N/A",
        case_note     = meta["case_note"][:1500] if meta["case_note"] else "(no summary available)",
    )
    ov_summary, ov_kws = _call(ov_prompt, f"judg[{index}]-ov")

    ov_chunk = build_overview_chunk(meta, ov_summary, ov_kws, today, source_file)
    chunks.append(ov_chunk)

    has_order = bool(meta["order_text"].strip())

    if has_order:
        order_parts = split_order_text(meta["order_text"])
        total = 1 + len(order_parts)

        ov_chunk["total_chunks"] = total
        chunks[0] = ov_chunk

        for i, part in enumerate(order_parts, start=2):
            prompt_text = part[:4000]

            ord_prompt = ORDER_PROMPT.format(
                title      = meta["title"],
                citation   = meta["citation"],
                court      = meta["court"],
                state      = meta["state"],
                decision   = meta["decision"],
                sections   = meta["section_raw"] or "N/A",
                order_text = prompt_text,
            )

            ord_summary, ord_kws = _call(ord_prompt, f"judg[{index}]-ord-{i}")

            ord_chunk = build_order_chunk(
                meta=meta,
                summary=ord_summary,
                keywords=ord_kws,
                order_text=part,
                chunk_index=i,
                total_chunks=total,
                today=today,
                source_file=source_file,
                parent_chunk_id=f"{meta['id_pfx']}-overview"
            )

            chunks.append(ord_chunk)
    else:
        ov_chunk["total_chunks"] = 1
        chunks[0] = ov_chunk

    return chunks, stats


# ─────────────────────────────────────────────────────────────────────────────
# SHARED STATE
# ─────────────────────────────────────────────────────────────────────────────

class SharedState:
    def __init__(self, progress_path, master_path, chunks_dir):
        self._lock        = threading.Lock()
        self._prog_path   = Path(progress_path)
        self._master_path = Path(master_path)
        self._chunks_dir  = Path(chunks_dir)
        self._completed = set()
        self._failed = {}
        self._errors = []
        self._master = []
        self._in_tok = 0
        self._out_tok = 0
        self._api_calls = 0
        self._processed = 0
        self._failed_cnt = 0
        self._load()

    def _load(self):
        if self._prog_path.exists():
            try:
                d = json.loads(self._prog_path.read_text("utf-8"))
                self._completed = set(d.get("completed", []))
                self._failed    = {int(k): v for k, v in d.get("failed", {}).items()}
            except Exception:
                pass
        if self._master_path.exists():
            try:
                self._master = json.loads(self._master_path.read_text("utf-8"))
            except Exception:
                pass

    def is_done(self, idx):
        with self._lock:
            return idx in self._completed

    def save(self, idx, case_id, chunks, stats):
        with self._lock:
            safe_case = re.sub(r'[^\w\-]', '_', str(case_id))
            f = self._chunks_dir / f"judg_{idx:05d}_{safe_case}.json"
            f.write_text(json.dumps(chunks, indent=2, ensure_ascii=False), "utf-8")

            ids = {c.get("id") for c in chunks if isinstance(c, dict)}
            self._master = [c for c in self._master
                            if not (isinstance(c, dict) and c.get("id") in ids)]
            self._master.extend(chunks)
            self._master_path.write_text(
                json.dumps(self._master, indent=2, ensure_ascii=False), "utf-8")

            self._completed.add(idx)
            self._failed.pop(idx, None)
            self._save_progress()

            self._in_tok    += stats.get("input_tokens", 0)
            self._out_tok   += stats.get("output_tokens", 0)
            self._api_calls += stats.get("api_calls", 0)
            self._processed += 1
            return f.name

    def error(self, idx, case_id, err):
        with self._lock:
            self._failed[idx] = err
            self._errors.append({
                "index": idx,
                "case_id": case_id,
                "error": err,
                "ts": datetime.now().isoformat()
            })
            self._failed_cnt += 1
            self._save_progress()

    def _save_progress(self):
        self._prog_path.write_text(json.dumps({
            "completed": sorted(self._completed),
            "failed": {str(k): v for k, v in self._failed.items()},
        }, indent=2), "utf-8")

    def flush_errors(self, path):
        with self._lock:
            if self._errors:
                Path(path).write_text(
                    json.dumps(self._errors, indent=2, ensure_ascii=False), "utf-8")

    def reset(self):
        with self._lock:
            self._completed.clear()
            self._failed.clear()
            self._errors.clear()
            self._master = []
            self._in_tok = self._out_tok = self._api_calls = 0
            self._processed = self._failed_cnt = 0
            self._save_progress()
            self._master_path.write_text("[]", "utf-8")

    def snapshot(self):
        with self._lock:
            return {
                "processed": self._processed,
                "failed": self._failed_cnt,
                "total_chunks": len(self._master),
                "api_calls": self._api_calls,
                "in_tok": self._in_tok,
                "out_tok": self._out_tok,
                "errors": list(self._errors),
                "completed": set(self._completed)
            }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(args):
    today       = datetime.now().strftime("%Y-%m-%d")
    source_file = Path(args.input).name
    output_dir  = Path(args.output)
    chunks_dir  = output_dir / "chunks"
    output_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)
    master_path   = output_dir / "master_chunks.json"
    progress_path = output_dir / "progress.json"
    errors_path   = output_dir / "errors.json"

    pricing = MODEL_PRICING.get(args.model, {"input": 0.72, "output": 0.72, "name": args.model})

    print(f"\n{'='*72}")
    print("  GST JUDGMENTS CHUNKING PIPELINE (TITAN-OPTIMIZED)")
    print(f"{'='*72}")
    print(f"  Model             : {args.model}  ({pricing['name']})")
    print(f"  Max tokens/call   : {args.max_tokens}")
    print(f"  Workers           : {args.workers}")
    print(f"  Input             : {args.input}")
    print(f"  Strategy          : 1 overview + N order child chunks")
    print(f"  Order chunk size  : ~{TARGET_CHUNK_TOKENS} tokens")
    print(f"  Overlap           : ~{CHUNK_OVERLAP_TOKENS} tokens")
    print(f"{'='*72}\n")

    rows = load_csv(args.input)
    if not rows:
        print("ERROR: No rows loaded from CSV.")
        sys.exit(1)

    try:
        validate_csv_columns(rows)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    total = len(rows)
    start = max(0, args.start)
    end   = total - 1 if args.end == -1 else min(args.end, total - 1)
    print(f"Loaded {total} judgments  |  Processing [{start}..{end}]\n")

    if args.cost_check:
        n = end - start + 1
        avg_order_chunks = 4
        calls = n * (1 + avg_order_chunks)
        avg_in = 0.003
        avg_out = 0.0003
        cost = calls * (avg_in * pricing["input"] + avg_out * pricing["output"])
        print(f"  Judgments      : {n}")
        print(f"  API calls      : ~{calls}")
        print(f"  Est. cost      : ~${cost:.3f} USD\n")
        sys.exit(0)

    print("Verifying AWS credentials...")
    ok, err = ping_bedrock(args.region, args.model, args.timeout)
    if not ok:
        print(f"ERROR: {err}")
        sys.exit(1)
    print("Credentials OK\n")

    state = SharedState(progress_path, master_path, chunks_dir)
    if not args.resume:
        state.reset()
    else:
        snap = state.snapshot()
        done = len([i for i in range(start, end + 1) if i in snap["completed"]])
        if done:
            print(f"Resuming: {done}/{end - start + 1} already done.\n")

    work = [i for i in range(start, end + 1) if not (args.resume and state.is_done(i))]
    print(f"Launching {args.workers} workers for {len(work)} judgments...\n{'─'*72}")

    def worker_fn(idx, wid):
        row     = rows[idx]
        case_id = row.get("ID", f"idx_{idx}").strip()
        try:
            t0 = time.time()
            chunks, stats = process_judgment(
                row, idx, today, source_file,
                args.model, args.region, args.max_tokens,
                args.retries, args.timeout, wid
            )
            state.save(idx, case_id, chunks, stats)
            return {
                "ok": True,
                "idx": idx,
                "case_id": case_id,
                "chunks": len(chunks),
                "stats": stats,
                "elapsed": time.time() - t0
            }
        except BedrockAPIError as e:
            state.error(idx, case_id, str(e))
            return {
                "ok": False,
                "idx": idx,
                "case_id": case_id,
                "error": str(e),
                "fatal": "AccessDenied" in str(e)
            }
        except Exception as e:
            state.error(idx, case_id, str(e))
            return {
                "ok": False,
                "idx": idx,
                "case_id": case_id,
                "error": str(e),
                "fatal": False
            }

    futures = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for i, idx in enumerate(work):
            futures[pool.submit(worker_fn, idx, (i % args.workers) + 1)] = idx

        for future in as_completed(futures):
            r = future.result()
            if r["ok"]:
                snap = state.snapshot()
                cost = (snap["in_tok"] / 1e6 * pricing["input"]) + \
                       (snap["out_tok"] / 1e6 * pricing["output"])
                title = rows[r["idx"]].get("Title", "?")[:35]
                print(f"  OK  [{r['idx']:>5}]  {title:<35}  "
                      f"{r['chunks']} chunks | "
                      f"{r['stats']['api_calls']} calls | "
                      f"{r['elapsed']:.1f}s | ${cost:.4f}")
            else:
                print(f"  ERR [{r['idx']:>5}]  {r['case_id']}  {r['error'][:65]}")
                if r.get("fatal"):
                    print("FATAL: AccessDenied — check IAM permissions")
                    for f in futures:
                        f.cancel()
                    break

    state.flush_errors(errors_path)
    snap = state.snapshot()
    cost = (snap["in_tok"] / 1e6 * pricing["input"]) + \
           (snap["out_tok"] / 1e6 * pricing["output"])
    print(f"\n{'='*72}\n  DONE  |  {snap['processed']} ok  |  "
          f"{snap['failed']} failed  |  {snap['total_chunks']} chunks  |  "
          f"{snap['api_calls']} calls  |  ${cost:.4f}\n{'='*72}\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="GST Judgments CSV → Titan-Optimized RAG Chunks")
    p.add_argument("--input",      default="judgments.csv")
    p.add_argument("--output",     default="judgment_chunks")
    p.add_argument("--region",     default=DEFAULT_REGION)
    p.add_argument("--model",      default=DEFAULT_MODEL)
    p.add_argument("--workers",    type=int, default=8)
    p.add_argument("--start",      type=int, default=0)
    p.add_argument("--end",        type=int, default=-1)
    p.add_argument("--max-tokens", type=int, default=2048)
    p.add_argument("--retries",    type=int, default=3)
    p.add_argument("--timeout",    type=int, default=120)
    p.add_argument("--cost-check", action="store_true")
    p.add_argument("--no-resume",  dest="resume", action="store_false")
    p.set_defaults(resume=True)
    run_pipeline(p.parse_args())