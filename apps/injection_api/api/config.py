"""
ingestion_api/config.py

Ingestion-service specific config.
Imports and re-exports the base pipeline CONFIG so every module
can do: from ingestion_api.config import SVC_CONFIG
"""

import os
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass
class JWTConfig:
    secret_key:        str   = os.getenv("JWT_SECRET_KEY", "change-me-in-production")
    algorithm:         str   = "HS256"
    # Access token valid for 8 hours — this is an internal admin tool
    access_token_expire_minutes: int = int(os.getenv("JWT_EXPIRE_MINUTES", "480"))


@dataclass
class AdminConfig:
    # Single admin user for now — extend to DB-backed users later
    username: str = os.getenv("ADMIN_USERNAME", "admin")
    # Store bcrypt hash in .env — generate with:
    #   python -c "import bcrypt; print(bcrypt.hashpw(b'yourpassword', bcrypt.gensalt()).decode())"
    password_hash: str = os.getenv(
        "ADMIN_PASSWORD_HASH",
        "$2b$12$placeholder_hash_replace_this"
    )


@dataclass
class RedisConfig:
    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    db:   int = int(os.getenv("REDIS_DB", "0"))

    @property
    def url(self) -> str:
        return f"redis://{self.host}:{self.port}/{self.db}"


@dataclass
class BedrockAutofillConfig:
    # Qwen3-235B — best instruction following for structured JSON output
    model_id:   str = os.getenv(
        "AWS_MODEL_ID") or os.getenv("AUTOFILL_MODEL_ID", "us.amazon.nova-pro-v1:0")
    # Override to Qwen3 once confirmed available:
    # "us.meta.llama3-3-70b-instruct-v1:0" or the Qwen3 ARN you use in chunking
    max_tokens: int = int(os.getenv("AUTOFILL_MAX_TOKENS", "4096"))
    temperature: float = float(os.getenv("AUTOFILL_TEMPERATURE", "0.1"))
    region:     str = os.getenv("AWS_REGION", "us-east-1")


@dataclass
class ServiceConfig:
    jwt:              JWTConfig            = field(default_factory=JWTConfig)
    admin:            AdminConfig          = field(default_factory=AdminConfig)
    redis:            RedisConfig          = field(default_factory=RedisConfig)
    bedrock_autofill: BedrockAutofillConfig = field(default_factory=BedrockAutofillConfig)

    # How long to keep job results in Redis before expiry (seconds)
    job_ttl_seconds: int = int(os.getenv("JOB_TTL_SECONDS", "86400"))   # 24 hours


SVC_CONFIG = ServiceConfig()