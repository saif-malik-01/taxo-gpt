from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
import redis.asyncio as redis
from api.config import settings

# ── Postgres ──────────────────────────────────────────────────────────────────
# pool_size        : persistent connections kept open (per process worker)
# max_overflow     : extra connections allowed above pool_size under burst load
# pool_pre_ping    : sends a lightweight SELECT 1 before lending a connection —
#                    kills stale connections after Postgres restarts silently
# pool_recycle     : recycle connections older than 30 min (prevents timeout errors
#                    from cloud DB proxies that close idle connections)
# pool_timeout     : seconds to wait for a connection before raising QueuePool error
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_timeout=30,
)

AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)

Base = declarative_base()


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


# ── Redis ─────────────────────────────────────────────────────────────────────
# max_connections : cap the pool so we don't exhaust Redis under burst load
# socket_keepalive: detect dead connections faster
# socket_connect_timeout / socket_timeout: fail fast instead of hanging
redis_client = redis.from_url(
    settings.REDIS_URL,
    decode_responses=True,
    max_connections=50,
    socket_keepalive=True,
    socket_connect_timeout=5,
    socket_timeout=5,
)


async def get_redis():
    return redis_client