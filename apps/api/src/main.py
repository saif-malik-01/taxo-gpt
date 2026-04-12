import os
import asyncio
import logging
import time
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from apps.api.src.core.config import settings
from apps.api.src.api.v1.auth import router as auth_router
from apps.api.src.api.v1.chat import router as chat_router
from apps.api.src.api.v1.payments import router as payments_router
from apps.api.src.api.v1.admin import router as admin_router
from apps.api.src.services.jobs.scheduler import start_scheduler, stop_scheduler
from sqlalchemy import text
from apps.api.src.db.session import engine, get_redis
from apps.api.src.services.chat.engine import get_pipeline
from apps.api.src.services.document.issue_replier import set_pipeline
from apps.api.src.services.llm.bedrock import close_async_bedrock_client
from starlette.concurrency import run_in_threadpool

# Environment Settings for Transformers Cache (from main.py)
os.environ['HF_HUB_DISABLE_SYMLINKS'] = '1'
os.environ['HF_HOME'] = os.path.join(os.path.dirname(__file__), '..', '.hf_cache')

# Initialize FastAPI App
app = FastAPI(
    title=settings.PROJECT_NAME,
    version="3.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

@app.exception_handler(RequestValidationError)
@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: Exception):
    """
    Safely handle validation errors that might contain binary data (bytes) 
    that cause UnicodeDecodeErrors in the default FastAPI encoder.
    """
    def _scrub(item):
        if isinstance(item, bytes):
            try:
                # Try to decode, or return placeholder if it's true binary
                return item.decode("utf-8")
            except:
                return f"<binary data: {len(item)} bytes>"
        if isinstance(item, list):
            return [_scrub(x) for x in item]
        if isinstance(item, dict):
            return {k: _scrub(v) for k, v in item.items()}
        if isinstance(item, tuple):
            return tuple(_scrub(x) for x in item)
        return item

    # Get errors list from either RequestValidationError or pydantic.ValidationError
    errors = getattr(exc, "errors", lambda: [])()
    if callable(errors):
        errors = errors()
        
    logger.error(f"Validation error occurred: {type(exc).__name__}")
    
    # We scrub these to remove raw bytes before encoding
    safe_errors = jsonable_encoder(_scrub(errors))
    return JSONResponse(
        status_code=422, 
        content={"detail": safe_errors, "error_type": "validation_error"}
    )

# Logging Setup
os.makedirs(os.path.dirname(settings.LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(settings.LOG_FILE, encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# Request Logging Middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    logger.info(f"REQ - {request.method} {request.url.path}")
    response = await call_next(request)
    process_time = time.time() - start_time
    logger.info(f"RES - {response.status_code} ({process_time:.4f}s)")
    return response

# CORS Configuration
origins = [settings.FRONTEND_URL] if settings.FRONTEND_URL else ["http://localhost:3000"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_headers=["*"],
    allow_methods=["*"],
)

# Startup Events (Scheduler/Database Initializers)
@app.on_event("startup")
async def startup_event():
    logger.info(f"Starting {settings.PROJECT_NAME}...")

    # Warm up pipeline at startup — never cold-init on a real request
    try:
        p = await get_pipeline()
        set_pipeline(p) # Inject into issue_replier
        logger.info("RAG pipeline ready")
    except Exception as e:
        logger.error(f"Pipeline warmup failed: {e}")

    try:
        start_scheduler()
        logger.info("Scheduler started")
    except ImportError as e:
        logger.warning(f"Scheduler import error: {e}, skipping...")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info(f"Shutting down {settings.PROJECT_NAME}...")
    try:
        stop_scheduler()
    except ImportError:
        pass
    await close_async_bedrock_client()
    logger.info("Async Bedrock client closed")

app.include_router(auth_router, prefix="/api/v1")
app.include_router(chat_router, prefix="/api/v1")
app.include_router(payments_router, prefix="/api/v1")
app.include_router(admin_router, prefix="/api/v1")

@app.get("/api/v1/health")
async def health_check():
    """Shallow health check for load balancers."""
    return {"status": "ok"}

@app.get("/api/v1/health/deep")
async def deep_health_check():
    """Deep health check that verifies all database connections."""
    health_status = {
        "status": "ok",
        "version": "v1.1.0",
        "database": "unknown",
        "redis": "unknown",
        "qdrant": "unknown"
    }
    
    # 1. Check Postgres
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        health_status["database"] = "connected"
    except Exception as e:
        health_status["database"] = f"error: {str(e)}"
        health_status["status"] = "error"

    # 2. Check Redis
    try:
        redis_client = await get_redis()
        await redis_client.ping() # type: ignore
        health_status["redis"] = "connected"
    except Exception as e:
        health_status["redis"] = f"error: {str(e)}"
        health_status["status"] = "error"

    # 3. Check Qdrant (via Pipeline)
    try:
        pipeline = await get_pipeline()
        await pipeline._qdrant.get_collections()
        health_status["qdrant"] = "connected"
    except Exception as e:
        health_status["qdrant"] = f"error: {str(e)}"
        health_status["status"] = "error"

    return health_status
