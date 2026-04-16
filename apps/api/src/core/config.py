from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Postgres cred
    DATABASE_URL: str = "postgresql+asyncpg://postgres:admin@localhost:5432/taxogpt"

    # Redis cred
    REDIS_URL: str = "redis://localhost:6379/0"

    # Project cred
    PROJECT_NAME: str = "Taxobuddy"
    API_V1_STR: str = "/api/v1"

    # JWT
    JWT_SECRET_KEY: str = "supersecretkey"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15      # Short-lived
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7         # Long-lived in Cookies
    CSRF_SECRET_KEY: str = "csrf-super-secret" # Used for Header-based CSRF
    ENVIRONMENT: str = "dev"                   # Set to "prod" in production
    COOKIE_DOMAIN: str | None = None           # e.g., ".taxobuddy.ai"
    
    # Email Configuration for Feedback Reports
    SMTP_SERVER: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USERNAME: str = ""
    SMTP_PASSWORD: str = ""
    FEEDBACK_RECIPIENT_EMAIL: str = "atul@gmail.com"

    # OAuth
    GOOGLE_CLIENT_ID: str = ""
    FACEBOOK_APP_ID: str = ""
    FACEBOOK_APP_SECRET: str = ""
    
    # Razorpay Configuration
    RAZORPAY_KEY_ID: str = ""
    RAZORPAY_KEY_SECRET: str = ""

    QDRANT_HOST: str = "localhost"
    QDRANT_PORT: int = 6333
    QDRANT_API_KEY: str = ""
    QDRANT_TIMEOUT: int = 60
    QDRANT_COLLECTION: str = "tax_chunks"
    QDRANT_HTTPS: bool = False
    QDRANT_TEXT_VECTOR: str = "text_vector"    # Dense vector name
    QDRANT_SPARSE_VECTOR: str = "sparse_vector" # Sparse vector name (BM25)

    # --- Path for BM25 Stats ---
    CORPUS_STATS_FILE: str = "corpus_stats.json"

    # --- Legal Draft Settings ---
    MAX_CONCURRENT_PAGES: int = 25      # How many pages to process at once across all users
    PAGE_SEMAPHORE_BACKEND: str = "redis" # Use 'redis' for production, 'asyncio' for local dev
    NOVA_LITE_DPI: int = 150            # Optimized DPI for speed/cost balance (keep at 150)
    
    # AWS Configuration
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_REGION: str = "us-east-1"
    AWS_S3_BUCKET: str = "taxobuddy-docs"
    WORKER_URL: str = "http://localhost:8001"
    
    # Frontend Configuration
    FRONTEND_URL: str = ""

    # TaxoCredit API Configuration
    TAXO_API_KEY: str = ""

    # FUP (Fair Usage Policy) Configurations
    GLOBAL_MONTHLY_TOKEN_LIMIT: int = 1000000
    SESSION_TOKEN_LIMIT_DRAFT: int = 60000   
    SESSION_TOKEN_LIMIT_SIMPLE: int = 100000

    # Trial / Welcome Package Defaults (Fallbacks)
    DEFAULT_SIMPLE_CREDITS: int = 1000000
    DEFAULT_DRAFT_CREDITS: int = 3
    DEFAULT_VALIDITY_DAYS: int = 365

    # Logging Configuration
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/app.log"

    # BM25 Configuration
    BM25_K1: float = 1.5
    BM25_B: float = 0.75
    BM25_L1_WEIGHT: int = 3
    BM25_L3_WEIGHT: int = 1

    # Bedrock Configuration for new logic
    TITAN_MODEL_ID: str = "amazon.titan-embed-text-v2:0"
    TITAN_DIMENSIONS: int = 1024
    TITAN_NORMALIZE: bool = True
    PIPELINE_MAX_RETRIES: int = 3
    PIPELINE_RETRY_DELAY: float = 2.0
    GLOBAL_EXECUTOR_WORKERS: int = 10   # 1 vCPU: 4 workers is optimal for I/O-bound Qdrant/Bedrock tasks

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

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
