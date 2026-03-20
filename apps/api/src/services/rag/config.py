"""
apps/api/src/services/rag/config.py
Redirected version of the root config.py for internal RAG use.
"""

from apps.api.src.core.config import settings

class ConfigObject:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

class ConfigBridge:
    def __init__(self):
        # Bedrock Section
        self.bedrock = ConfigObject(
            region_name=settings.AWS_REGION,
            titan_model_id=settings.TITAN_MODEL_ID,
            titan_dimensions=settings.TITAN_DIMENSIONS,
            titan_normalize=settings.TITAN_NORMALIZE
        )

        # Qdrant Section
        self.qdrant = ConfigObject(
            host=settings.QDRANT_HOST,
            port=settings.QDRANT_PORT,
            api_key=settings.QDRANT_API_KEY,
            collection_name=settings.QDRANT_COLLECTION,
            text_vector_name=settings.QDRANT_TEXT_VECTOR,
            summary_vector_name="summary_vector",
            sparse_vector_name=settings.QDRANT_SPARSE_VECTOR,
            vector_size=settings.TITAN_DIMENSIONS,
            timeout=settings.QDRANT_TIMEOUT
        )

        # BM25 Section
        self.bm25 = ConfigObject(
            k1=settings.BM25_K1,
            b=settings.BM25_B,
            l1_weight=settings.BM25_L1_WEIGHT,
            l3_weight=settings.BM25_L3_WEIGHT
        )

        # Path Section
        self.paths = ConfigObject(
            chunks_dir="data/processed/chunks",
            tracker_file="data/processed/ingestion_tracker.json",
            debug_tokens_dir="data/debug/tokens",
            corpus_stats_file=settings.CORPUS_STATS_FILE
        )

        # Pipeline Section
        self.pipeline = ConfigObject(
            max_retries=settings.PIPELINE_MAX_RETRIES,
            retry_delay_seconds=settings.PIPELINE_RETRY_DELAY,
            write_debug_tokens=True,
            grounding_check=True
        )

CONFIG = ConfigBridge()

# ── Normalisation constants (Copied from new/retrieval/config.py) ──────────────

PRESERVED_UPPERCASE = settings.PRESERVED_UPPERCASE # Or use the one from settings
AUTHORITY_MAP = settings.AUTHORITY_MAP

SECTION_CHUNK_TYPES = ["cgst_section", "igst_section"]
RULE_CHUNK_TYPES = ["cgst_rule", "igst_rule", "gstat_rule"]
FORM_CHUNK_TYPES = ["gst_form", "gstat_form"]
JUDGMENT_CHUNK_TYPES = ["judgment"]
RATE_CHUNK_TYPES = ["hsn_code", "sac_code", "notification", "circular"]

DEFINITION_CHUNK_TYPES = SECTION_CHUNK_TYPES + RULE_CHUNK_TYPES

HIERARCHY_CHUNK_MAP = {
    "act":                        SECTION_CHUNK_TYPES,
    "rules":                      RULE_CHUNK_TYPES,
    "notification_circular_faq":  ["notification", "circular", "faq"],
    "case_scenario_illustration": ["case_scenario", "case_study"],
    "judgment":                   JUDGMENT_CHUNK_TYPES,
    "analytical_review":          ["analytical_review"],
    "summary":                    [],
    "others": [
        "article", "contemporary_issue", "draft_reply", "council_decision", "solved_query"
    ],
}

ALL_CHUNK_TYPES = [
    "article", "case_scenario", "case_study", "cgst_rule", "cgst_section", 
    "circular", "contemporary_issue", "draft_reply", "faq", "gst_form", 
    "council_decision", "gstat_form", "gstat_rule", "hsn_code", "igst_rule", 
    "igst_section", "judgment", "notification", "sac_code", "analytical_review", "solved_query"
]
