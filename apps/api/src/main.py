import os
import logging
import time
from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from apps.api.src.core.config import settings
from apps.api.src.db.session import engine, Base
from apps.api.src.api.v1.auth import router as auth_router
from apps.api.src.api.v1.chat import router as chat_router
from apps.api.src.api.v1.payments import router as payments_router
from apps.api.src.api.v1.admin import router as admin_router

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
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Startup Events (Scheduler/Database Initializers)
@app.on_event("startup")
async def startup_event():
    logger.info(f"Starting {settings.PROJECT_NAME}...")
    # Background jobs will be migrated in a later step
    try:
        from apps.api.src.services.jobs.scheduler import start_scheduler
        start_scheduler()
        logger.info("Scheduler started")
    except ImportError as e:
        logger.warning(f"Scheduler import error: {e}, skipping...")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info(f"Shutting down {settings.PROJECT_NAME}...")
    try:
        from apps.api.src.services.jobs.scheduler import stop_scheduler
        stop_scheduler()
    except ImportError: pass

# Include Versioned Routers
app.include_router(auth_router, prefix="/api/v1")
app.include_router(chat_router, prefix="/api/v1")
app.include_router(payments_router, prefix="/api/v1")
app.include_router(admin_router, prefix="/api/v1")

@app.get("/health")
@app.get("/api/v1/health")
def health_check():
    return {"status": "ok", "version": "v1", "app": settings.PROJECT_NAME}
