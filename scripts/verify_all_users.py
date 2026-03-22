import asyncio
import sys
import os

# Ensure project root is on path regardless of where script is run from
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from api.config import settings

async def verify_all():
    print(f"Connecting to database: {settings.DATABASE_URL}")
    engine = create_async_engine(settings.DATABASE_URL)

    async with engine.begin() as conn:
        print("Marking all existing users as verified...")
        try:
            result = await conn.execute(text("UPDATE users SET is_verified = TRUE"))
            print(f"Successfully updated {result.rowcount} users.")
        except Exception as e:
            print(f"Error during update: {e}")

    await engine.dispose()
    print("Verification script finished.")

if __name__ == "__main__":
    asyncio.run(verify_all())