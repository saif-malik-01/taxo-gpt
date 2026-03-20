import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from apps.api.src.core.config import settings

async def verify_all():
    engine = create_async_engine(settings.DATABASE_URL)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE users SET is_verified = TRUE"))
    await engine.dispose()
if __name__ == "__main__": asyncio.run(verify_all())
