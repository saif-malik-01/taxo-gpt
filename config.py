"""
config.py
All configuration loaded from .env file.
"""

import os
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class BedrockConfig:
    region_name:       str   = os.getenv("AWS_REGION", "us-east-1")
    titan_model_id:    str   = "amazon.titan-embed-text-v2:0"
    titan_dimensions:  int   = 1024
    titan_normalize:   bool  = True


@dataclass
class QdrantConfig:
    host:                str           = os.getenv("QDRANT_HOST", "localhost")
    port:                int           = int(os.getenv("QDRANT_PORT", "6333"))
    api_key:             Optional[str] = os.getenv("QDRANT_API_KEY") or None
    collection_name:     str           = "tax_chunks"
    text_vector_name:    str           = "text_vector"
    summary_vector_name: str           = "summary_vector"
    sparse_vector_name:  str           = "bm25_sparse"
    vector_size:         int           = 1024
    timeout:             int           = 60


@dataclass
class BM25Config:
    k1:        float = 1.5
    b:         float = 0.75
    l1_weight: int   = 3
    l3_weight: int   = 1


@dataclass
class PathConfig:
    chunks_dir:        str = os.getenv("CHUNKS_DIR", "./chunks")
    tracker_file:      str = os.getenv("TRACKER_FILE", "./ingestion_tracker.json")
    debug_tokens_dir:  str = os.getenv("DEBUG_DIR", "./debug_tokens")
    corpus_stats_file: str = "./corpus_stats.json"


@dataclass
class PipelineConfig:
    max_retries:         int   = 3
    retry_delay_seconds: float = 2.0
    write_debug_tokens:  bool  = True
    grounding_check:     bool  = True


@dataclass
class Config:
    bedrock:  BedrockConfig  = field(default_factory=BedrockConfig)
    qdrant:   QdrantConfig   = field(default_factory=QdrantConfig)
    bm25:     BM25Config     = field(default_factory=BM25Config)
    paths:    PathConfig     = field(default_factory=PathConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)


CONFIG = Config()


# ── Normalisation constants ───────────────────────────────────────────────────

PRESERVED_UPPERCASE = {
    # Tax authorities
    "CBDT", "CBIC", "ITAT", "CESTAT", "GSTAT", "NAA", "DGGI",
    # Courts
    "SC", "HC", "AAR",
    # Tax officers
    "AO", "CIT", "PCIT", "ITO", "TRO", "DCIT", "ACIT", "JCIT",
    "CCT", "SCT", "ACCT", "CIT_A",
    # Other bodies
    "ED", "CBI", "SFIO", "NCLT", "DRT", "MOF",
    # Tax types and codes
    "TDS", "TCS", "GST", "IGST", "CGST", "SGST", "UTGST",
    "AY", "FY", "ITC", "HSN", "SAC",
}

AUTHORITY_MAP = {
    # ── CBIC ──────────────────────────────────────────────────────────
    "central board of indirect taxes and customs": "CBIC",
    "central board of excise and customs":         "CBIC",
    "cbec":                                        "CBIC",

    # ── CBDT ──────────────────────────────────────────────────────────
    "central board of direct taxes":               "CBDT",

    # ── GST Council ───────────────────────────────────────────────────
    "gst council":                                 "GST_council",
    "goods and services tax council":              "GST_council",

    # ── Tribunals ─────────────────────────────────────────────────────
    "income tax appellate tribunal":               "ITAT",
    "customs excise and service tax appellate tribunal": "CESTAT",
    "cestat":                                      "CESTAT",
    "goods and services tax appellate tribunal":   "GSTAT",
    "national anti profiteering authority":        "NAA",
    "naa":                                         "NAA",

    # ── Courts ────────────────────────────────────────────────────────
    "supreme court":                               "SC",
    "supreme court of india":                      "SC",
    "high court":                                  "HC",
    "authority for advance ruling":                "AAR",
    "advance ruling authority":                    "AAR",

    # ── Income Tax Officers ───────────────────────────────────────────
    "assessing officer":                           "AO",
    "commissioner of income tax":                  "CIT",
    "principal commissioner of income tax":        "PCIT",
    "commissioner of income tax appeals":          "CIT_A",
    "cit(a)":                                      "CIT_A",
    "income tax officer":                          "ITO",
    "tax recovery officer":                        "TRO",
    "deputy commissioner of income tax":           "DCIT",
    "assistant commissioner of income tax":        "ACIT",
    "joint commissioner of income tax":            "JCIT",

    # ── GST Officers ──────────────────────────────────────────────────
    "commissioner of central tax":                 "CCT",
    "superintendent of central tax":               "SCT",
    "additional commissioner of central tax":      "ACCT",

    # ── Intelligence and Investigation ────────────────────────────────
    "directorate general of gst intelligence":     "DGGI",
    "dggi":                                        "DGGI",
    "enforcement directorate":                     "ED",
    "central bureau of investigation":             "CBI",
    "serious fraud investigation office":          "SFIO",

    # ── Other Tribunals and Bodies ────────────────────────────────────
    "national company law tribunal":               "NCLT",
    "debt recovery tribunal":                      "DRT",
    "ministry of finance":                         "MOF",
}


# ── Chunk type groups ─────────────────────────────────────────────────────────
# All chunk_type values present in the corpus.
# Imported directly from this module in retrieval code — never use CONFIG.X here.
# Usage: from config import SECTION_CHUNK_TYPES, RULE_CHUNK_TYPES etc.

SECTION_CHUNK_TYPES = [
    "cgst_section",
    "igst_section",
]

RULE_CHUNK_TYPES = [
    "cgst_rule",
    "igst_rule",
    "gstat_rule",
]

FORM_CHUNK_TYPES = [
    "gst_form",
    "gstat_form",
]

JUDGMENT_CHUNK_TYPES = [
    "judgment",
]

RATE_CHUNK_TYPES = [
    "hsn_code",
    "sac_code",
    "notification",
    "circular",
]

DEFINITION_CHUNK_TYPES = SECTION_CHUNK_TYPES + RULE_CHUNK_TYPES

# Maps response hierarchy labels to their chunk_types.
# Position 9 "others" captures everything not in positions 1–8.
HIERARCHY_CHUNK_MAP = {
    "act":                        SECTION_CHUNK_TYPES,
    "rules":                      RULE_CHUNK_TYPES,
    "notification_circular_faq":  ["notification", "circular", "faq"],
    "case_scenario_illustration": ["case_scenario", "case_study"],
    "judgment":                   JUDGMENT_CHUNK_TYPES,
    "analytical_review":          ["analytical_review"],
    "summary":                    [],
    "others": [
        "article",
        "contemporary_issue",
        "draft_reply",
        "council_decision",
        "solved_query",
    ],
}

# Complete list of all known chunk types in the corpus
ALL_CHUNK_TYPES = [
    "article",
    "case_scenario",
    "case_study",
    "cgst_rule",
    "cgst_section",
    "circular",
    "contemporary_issue",
    "draft_reply",
    "faq",
    "gst_form",
    "council_decision",
    "gstat_form",
    "gstat_rule",
    "hsn_code",
    "igst_rule",
    "igst_section",
    "judgment",
    "notification",
    "sac_code",
    "analytical_review",
    "solved_query",
]