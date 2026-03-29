"""
ingestion_api/worker/celery_app.py

Celery application instance.
Import this anywhere you need to queue or inspect tasks.
"""

import os
import sys

# Ensure the root directory is in sys.path for Celery worker
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from celery import Celery

from api.config import SVC_CONFIG

celery_app = Celery(
    "ingestion",
    broker=SVC_CONFIG.redis.url,
    backend=SVC_CONFIG.redis.url,
    include=["worker.tasks"],
)

celery_app.conf.update(
    # Serialization
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # Result storage
    result_expires=SVC_CONFIG.job_ttl_seconds,

    # Retry behaviour
    task_acks_late=True,          # re-queue on worker crash
    task_reject_on_worker_lost=True,

    # Concurrency — one worker process handles one I/O-heavy task at a time
    # Bedrock embedding is the bottleneck; worker count set in docker-compose
    worker_prefetch_multiplier=1,

    # Timezone
    timezone="UTC",
    enable_utc=True,
)