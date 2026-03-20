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
    API_KEY_SECRET: str = "taxogpt-internal-api-key"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
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
    QDRANT_TIMEOUT: int = 20
    QDRANT_COLLECTION: str = "gst_chunks"
    QDRANT_TEXT_VECTOR: str = "text_vector"    # Dense vector name
    QDRANT_SPARSE_VECTOR: str = "sparse_vector" # Sparse vector name (BM25)

    # --- Path for BM25 Stats ---
    CORPUS_STATS_FILE: str = "data/processed/corpus_stats.json"
    
    # AWS Configuration
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_REGION: str = "us-east-1"
    AWS_S3_BUCKET: str = "taxobuddy-docs"
    WORKER_URL: str = "http://localhost:8001"
    
    # Frontend Configuration
    FRONTEND_URL: str = ""

    # FUP (Fair Usage Policy) Configurations
    GLOBAL_MONTHLY_TOKEN_LIMIT: int = 1000000
    SESSION_TOKEN_LIMIT_DRAFT: int = 60000   
    SESSION_TOKEN_LIMIT_SIMPLE: int = 100000

    # Logging Configuration
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/app.log"

    # --- New Retrieval Constants ---
    PRESERVED_UPPERCASE: set = {
        "SC", "HC", "ITAT", "AAR", "CBIC", "CBDT", "DGFT", "GSTN", "ITC", "RCM", "HSN", "SAC",
        "GSTR", "IGST", "CGST", "SGST", "UTGST", "GST", "VAT", "CENVAT", "MODVAT"
    }

    AUTHORITY_MAP: dict = {
        "central board of indirect taxes": "CBIC",
        "cbic": "CBIC",
        "central board of direct taxes": "CBDT",
        "cbdt": "CBDT",
        "supreme court": "SC",
        "high court": "HC",
        "authority for advance ruling": "AAR",
        "aar": "AAR",
        "goods and services tax council": "GST_COUNCIL",
        "gst council": "GST_COUNCIL",
        "directorate general of foreign trade": "DGFT",
        "dgft": "DGFT"
    }

    SECTION_CHUNK_TYPES: set = {"cgst_section", "igst_section", "utgst_section", "sgst_section"}
    RULE_CHUNK_TYPES: set = {"cgst_rule", "igst_rule", "utgst_rule", "sgst_rule", "gstat_rule"}
    FORM_CHUNK_TYPES: set = {"gst_form", "gstat_form", "gstat_register"}

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

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
