import logging
from concurrent.futures import ThreadPoolExecutor
from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)

# Singleton Global Executor Pool for all RAG and LLM parallel tasks.
# This ensures that the 1 vCPU / 2GB RAM container never blooms 
# hundreds of threads across multiple requests.
rag_executor = ThreadPoolExecutor(
    max_workers=settings.GLOBAL_EXECUTOR_WORKERS,
    thread_name_prefix="rag_task"
)

logger.info(f"Global RAG Executor initialized with max_workers={settings.GLOBAL_EXECUTOR_WORKERS}")
