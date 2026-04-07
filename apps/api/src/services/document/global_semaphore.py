"""
services/document/global_semaphore.py

Production-grade distributed semaphore for Nova Lite page extraction.

Single-worker (dev/staging):
    asyncio.Semaphore — module-level, zero overhead, zero dependencies.

Multi-worker production (PAGE_SEMAPHORE_BACKEND=redis):
    Redis atomic Lua semaphore — enforces the global limit across ALL
    uvicorn workers on ALL machines sharing the same Redis instance.

    Design:
      Key   : "doc:page_sem:{MAX}"
      Value : current in-flight count (integer)
      TTL   : 300s safety expiry — prevents permanent lock on worker crash.

      Acquire: Lua INCR-if-below-max (atomic). Spin-waits 50ms between
               retries. Times out after PAGE_SEM_ACQUIRE_TIMEOUT seconds
               and fails-open (request proceeds) to avoid starvation.
      Release: Lua DECR-floor-0 (atomic). Never goes negative.

Environment variables:
    MAX_CONCURRENT_PAGES      default 25
    PAGE_SEMAPHORE_BACKEND    "asyncio" (default) | "redis"
    PAGE_SEM_ACQUIRE_TIMEOUT  seconds before timeout (default 120)
"""

import asyncio
import logging
import os
import time
import threading
from typing import Optional

logger = logging.getLogger(__name__)

MAX_CONCURRENT_PAGES = int(os.getenv("MAX_CONCURRENT_PAGES", "25"))
SEMAPHORE_BACKEND    = os.getenv("PAGE_SEMAPHORE_BACKEND", "asyncio")
ACQUIRE_TIMEOUT_S    = int(os.getenv("PAGE_SEM_ACQUIRE_TIMEOUT", "120"))


# ── asyncio backend (single-worker / dev) ─────────────────────────────────────

_ASYNCIO_SEM: Optional[asyncio.Semaphore] = None
_ASYNCIO_SEM_LOCK = threading.Lock()


def _get_asyncio_semaphore() -> asyncio.Semaphore:
    global _ASYNCIO_SEM
    if _ASYNCIO_SEM is None:
        with _ASYNCIO_SEM_LOCK:
            if _ASYNCIO_SEM is None:
                _ASYNCIO_SEM = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
                logger.info(
                    f"asyncio page semaphore initialised: "
                    f"max={MAX_CONCURRENT_PAGES} concurrent pages (per-worker)"
                )
    return _ASYNCIO_SEM


# ── Redis backend (multi-worker / production) ─────────────────────────────────

class RedisPageSemaphore:
    """
    Distributed counting semaphore backed by Redis atomic Lua scripts.
    Enforces MAX_CONCURRENT_PAGES globally across all uvicorn workers.

    Implements the async context manager protocol so it can be used with
    `async with semaphore:` exactly like asyncio.Semaphore.
    """

    # Atomic INCR-if-below-cap. Returns 1 = acquired, 0 = full.
    _ACQUIRE_LUA = """
local key = KEYS[1]
local cap = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local cur = tonumber(redis.call('GET', key) or '0')
if cur < cap then
    redis.call('INCR', key)
    redis.call('EXPIRE', key, ttl)
    return 1
end
return 0
"""

    # Atomic DECR floor-0. Never goes negative.
    _RELEASE_LUA = """
local key = KEYS[1]
local cur = tonumber(redis.call('GET', key) or '0')
if cur > 0 then redis.call('DECR', key) end
return 1
"""

    def __init__(self):
        self._max     = MAX_CONCURRENT_PAGES
        self._key     = f"doc:page_sem:{self._max}"
        self._ttl     = 300        # safety expiry seconds
        self._timeout = ACQUIRE_TIMEOUT_S
        self._sleep   = 0.05       # 50 ms spin interval
        self._r       = self._make_client()

    @staticmethod
    def _make_client():
        import redis as sync_redis
        from apps.api.src.core.config import settings
        return sync_redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
            retry_on_timeout=True,
        )

    def _try_acquire(self) -> bool:
        try:
            result = self._r.eval(
                self._ACQUIRE_LUA, 1, self._key, self._max, self._ttl
            )
            return int(result) == 1
        except Exception as e:
            logger.warning(f"Redis semaphore acquire error (fail-open): {e}")
            return True   # fail-open: don't block requests if Redis is down

    def _release(self):
        try:
            self._r.eval(self._RELEASE_LUA, 1, self._key)
        except Exception as e:
            logger.warning(f"Redis semaphore release error: {e}")

    def _blocking_acquire(self):
        """Spin-wait in a thread until slot available or timeout."""
        deadline = time.monotonic() + self._timeout
        while time.monotonic() < deadline:
            if self._try_acquire():
                return
            time.sleep(self._sleep)
        logger.error(
            f"Redis page semaphore timeout after {self._timeout}s "
            f"(max={self._max}). Proceeding without slot (fail-open)."
        )

    async def __aenter__(self):
        from starlette.concurrency import run_in_threadpool
        await run_in_threadpool(self._blocking_acquire)
        return self

    async def __aexit__(self, *_):
        from starlette.concurrency import run_in_threadpool
        await run_in_threadpool(self._release)

    def current_count(self) -> int:
        try:
            val = self._r.get(self._key)
            return int(val) if val else 0
        except Exception:
            return -1


# ── Singleton ─────────────────────────────────────────────────────────────────

_REDIS_SEM: Optional[RedisPageSemaphore] = None
_REDIS_SEM_LOCK = threading.Lock()


def _get_redis_semaphore() -> RedisPageSemaphore:
    global _REDIS_SEM
    if _REDIS_SEM is None:
        with _REDIS_SEM_LOCK:
            if _REDIS_SEM is None:
                _REDIS_SEM = RedisPageSemaphore()
                logger.info(
                    f"Redis page semaphore initialised: "
                    f"max={MAX_CONCURRENT_PAGES} concurrent pages "
                    f"(global across all workers)"
                )
    return _REDIS_SEM


def get_page_semaphore():
    """
    Return the active page semaphore.

    Set PAGE_SEMAPHORE_BACKEND=redis for production multi-worker deployment.
    Default is asyncio (single-worker, zero-dependency).
    """
    if SEMAPHORE_BACKEND == "redis":
        return _get_redis_semaphore()
    return _get_asyncio_semaphore()