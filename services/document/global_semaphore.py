"""
services/document/global_semaphore.py

Global limit on concurrent Amazon Nova Lite page extraction calls.

Architecture:
  Single worker (current: --workers 1):
    asyncio.Semaphore at module level. Each acquired slot = one Nova Lite call
    running concurrently. Released as soon as the call returns.

  Multi-worker upgrade path (when you scale to multiple uvicorn workers):
    Replace the asyncio.Semaphore with the RedisPageSemaphore below.
    Each worker then uses Redis for cross-process counting.
    Enable by setting PAGE_SEMAPHORE_BACKEND=redis in environment.

Limit = 30:
  Empirically chosen. Nova Lite processes one page image per call (~3-8s).
  30 concurrent = good throughput without hitting Bedrock rate limits.
  Adjust MAX_CONCURRENT_PAGES via environment variable.
"""

import asyncio
import logging
import os
import time
import threading
from typing import Optional

logger = logging.getLogger(__name__)

MAX_CONCURRENT_PAGES = int(os.getenv("MAX_CONCURRENT_PAGES", "25"))
# 25: doubles throughput vs 12 while staying within default Bedrock Nova Lite
# quota (~50–100 RPM). Set MAX_CONCURRENT_PAGES env var to tune.
# For production with quota increase to 500 RPM: safely push to 50.
SEMAPHORE_BACKEND    = os.getenv("PAGE_SEMAPHORE_BACKEND", "asyncio")  # "asyncio" | "redis"

# ── asyncio backend (default, single-worker) ─────────────────────────────────
# Lazily created on first use to avoid "no running event loop" errors at import.
_ASYNCIO_SEM: Optional[asyncio.Semaphore] = None
_ASYNCIO_SEM_LOCK = threading.Lock()


def _get_asyncio_semaphore() -> asyncio.Semaphore:
    """Return (or create) the module-level asyncio Semaphore."""
    global _ASYNCIO_SEM
    if _ASYNCIO_SEM is None:
        with _ASYNCIO_SEM_LOCK:
            if _ASYNCIO_SEM is None:
                _ASYNCIO_SEM = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
                logger.info(
                    f"Page semaphore initialised — max concurrent pages: {MAX_CONCURRENT_PAGES}"
                )
    return _ASYNCIO_SEM


def get_page_semaphore() -> asyncio.Semaphore:
    """
    Return the active page semaphore.
    Currently always returns the asyncio.Semaphore (single-worker mode).
    Swap to RedisPageSemaphore when scaling to multiple workers.
    """
    return _get_asyncio_semaphore()


# ── Redis backend (multi-worker, production scale) ───────────────────────────
# Uncomment and use when scaling to multiple uvicorn workers.
# Requires: pip install redis
#
# Usage:
#   1. Set PAGE_SEMAPHORE_BACKEND=redis in .env
#   2. Replace get_page_semaphore() to return RedisPageSemaphore() instance
#
# class RedisPageSemaphore:
#     """
#     Distributed counting semaphore using Redis atomic Lua scripts.
#     Works across multiple process workers.
#     
#     Acquire: atomically increment counter if below max.
#     Release: atomically decrement counter (floor 0).
#     TTL: 5 min expiry on the counter key prevents permanent leak if
#          a worker crashes while holding slots.
#     """
#     KEY     = "doc:page_sem_count"
#     TTL_SEC = 300
#
#     def __init__(self):
#         import redis as sync_redis
#         from api.config import settings
#         self._r = sync_redis.from_url(
#             settings.REDIS_URL,
#             decode_responses=True,
#             socket_connect_timeout=2,
#             socket_timeout=2,
#         )
#         self._max = MAX_CONCURRENT_PAGES
#
#     # Lua script: atomic check-and-increment (returns 1=acquired, 0=full)
#     _ACQUIRE_SCRIPT = """
#         local cur = tonumber(redis.call('get', KEYS[1]) or '0')
#         if cur < tonumber(ARGV[1]) then
#             redis.call('incr', KEYS[1])
#             redis.call('expire', KEYS[1], ARGV[2])
#             return 1
#         end
#         return 0
#     """
#
#     # Lua script: atomic decrement (floor 0)
#     _RELEASE_SCRIPT = """
#         local cur = tonumber(redis.call('get', KEYS[1]) or '0')
#         if cur > 0 then redis.call('decr', KEYS[1]) end
#         return 1
#     """
#
#     def _try_acquire(self) -> bool:
#         result = self._r.eval(self._ACQUIRE_SCRIPT, 1, self.KEY, self._max, self.TTL_SEC)
#         return int(result) == 1
#
#     def _release(self):
#         self._r.eval(self._RELEASE_SCRIPT, 1, self.KEY)
#
#     # Context manager interface — call from async code via run_in_threadpool
#     # or wrap in asyncio task. NOT a native async context manager on purpose
#     # (sync Redis client runs in threadpool).
#     def __enter__(self):
#         deadline = time.monotonic() + 120  # 2-minute max wait
#         while time.monotonic() < deadline:
#             if self._try_acquire():
#                 return self
#             time.sleep(0.1)
#         raise TimeoutError("Page semaphore acquire timeout (2 min)")
#
#     def __exit__(self, *_):
#         try:
#             self._release()
#         except Exception as e:
#             logger.warning(f"Page semaphore release error: {e}")