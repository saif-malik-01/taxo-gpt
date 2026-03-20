from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://postgres:admin@localhost:5432/taxogpt"
    REDIS_URL: str = "redis://localhost:6379/0"
    SECRET_KEY: str = "supersecretkey"
    ALGORITH: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # Email Configuration for Feedback Reports
    SMTP_SERVER: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USERNAME: str = ""
    SMTP_PASSWORD: str = ""
    FEEDBACK_RECIPIENT_EMAIL: str = "atul@gmail.com"
    GOOGLE_CLIENT_ID: str = ""
    FACEBOOK_APP_ID: str = ""
    FACEBOOK_APP_SECRET: str = ""
    
    # Razorpay Configuration
    RAZORPAY_KEY_ID: str = ""
    RAZORPAY_KEY_SECRET: str = ""
    
    # Frontend Configuration
    FRONTEND_URL: str = "http://localhost:3000"

    # FUP (Fair Usage Policy) Configurations
    GLOBAL_MONTHLY_TOKEN_LIMIT: int = 1000000 # 1 Million tokens / month
    SESSION_TOKEN_LIMIT_DRAFT: int = 30000    # 30k tokens / draft chat
    SESSION_TOKEN_LIMIT_SIMPLE: int = 50000   # 50k tokens / simple chat

    # Logging Configuration
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/app.log"

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
