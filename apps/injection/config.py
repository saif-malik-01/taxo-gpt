"""
config.py
All configuration loaded from .env file for the Injection App.
"""

import os
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class BedrockConfig:
    region_name:       str   = os.getenv("AWS_REGION", "us-east-1")
    titan_model_id:    str   = os.getenv("TITAN_MODEL_ID", "amazon.titan-embed-text-v2:0")
    titan_dimensions:  int   = int(os.getenv("TITAN_DIMENSIONS", "1024"))
    titan_normalize:   bool  = os.getenv("TITAN_NORMALIZE", "True").lower() == "true"


@dataclass
class QdrantConfig:
    host:                str           = os.getenv("QDRANT_HOST", "localhost")
    port:                int           = int(os.getenv("QDRANT_PORT", "6333"))
    api_key:             Optional[str] = os.getenv("QDRANT_API_KEY") or None
    collection_name:     str           = os.getenv("QDRANT_COLLECTION", "gst_chunks")
    text_vector_name:    str           = "text_vector"
    summary_vector_name: str           = "summary_vector"
    sparse_vector_name:  str           = "sparse_vector"
    vector_size:         int           = 1024
    timeout:             int           = 60


@dataclass
class BM25Config:
    k1:        float = float(os.getenv("BM25_K1", "1.5"))
    b:         float = float(os.getenv("BM25_B", "0.75"))
    l1_weight: int   = int(os.getenv("BM25_L1_WEIGHT", "3"))
    l3_weight: int   = int(os.getenv("BM25_L3_WEIGHT", "1"))


@dataclass
class PathConfig:
    chunks_dir:        str = os.getenv("CHUNKS_DIR", "./data/processed")
    tracker_file:      str = os.getenv("TRACKER_FILE", "./ingestion_tracker.json")
    debug_tokens_dir:  str = os.getenv("DEBUG_DIR", "./debug_tokens")
    corpus_stats_file: str = os.getenv("CORPUS_STATS_FILE", "./corpus_stats.json")


@dataclass
class PipelineConfig:
    max_retries:         int   = int(os.getenv("PIPELINE_MAX_RETRIES", "3"))
    retry_delay_seconds: float = float(os.getenv("PIPELINE_RETRY_DELAY", "2.0"))
    write_debug_tokens:  bool  = os.getenv("WRITE_DEBUG_TOKENS", "True").lower() == "true"
    grounding_check:     bool  = True


@dataclass
class Config:
    bedrock:  BedrockConfig  = field(default_factory=BedrockConfig)
    qdrant:   QdrantConfig   = field(default_factory=QdrantConfig)
    bm25:     BM25Config     = field(default_factory=BM25Config)
    paths:    PathConfig     = field(default_factory=PathConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)


CONFIG = Config()


# --- Constants needed by Pipeline Logic ---

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