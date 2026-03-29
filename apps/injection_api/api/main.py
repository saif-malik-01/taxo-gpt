"""
ingestion_api/api/main.py

FastAPI application factory.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import auth, chunks, jobs


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup: verify Qdrant and Redis are reachable.
    Shutdown: nothing to clean up (connections are per-request).
    """
    # ── Startup ──────────────────────────────────────────────────────
    from core_models.qdrant_manager import QdrantManager
    qdrant = QdrantManager()
    qdrant.ensure_collection()   # creates collection if it doesn't exist

    from worker.celery_app import celery_app
    # Ping the broker to fail fast if Redis is down
    try:
        celery_app.control.ping(timeout=2)
    except Exception:
        # Non-fatal at startup — worker may not be running yet
        pass

    yield
    # ── Shutdown ─────────────────────────────────────────────────────
    # Nothing to clean up


def create_app() -> FastAPI:
    app = FastAPI(
        title="GST Ingestion Service",
        description=(
            "Internal admin API for ingesting GST legal data into Qdrant. "
            "Supports 21 chunk types, LLM autofill via AWS Bedrock, "
            "supersession detection, and async job tracking."
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # ── CORS — restrict to your frontend origin in production ────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3001", "http://localhost:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Routes ────────────────────────────────────────────────────────
    app.include_router(auth.router)
    app.include_router(chunks.router)
    app.include_router(jobs.router)

    @app.get("/health", tags=["health"])
    def health():
        return {"status": "ok"}

    return app


app = create_app()