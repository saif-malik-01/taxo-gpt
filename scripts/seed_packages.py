import asyncio
from apps.api.src.db.session import AsyncSessionLocal
from apps.api.src.db.models.base import CreditPackage
from sqlalchemy import select

async def seed_packages():
    async with AsyncSessionLocal() as db:
        packages = [
            {"name": "draft-1", "title": "Solo Draft Reply", "description": "1 reply", "amount": 79900, "credits_added": 1},
            {"name": "draft-20", "title": "Draft Pro", "description": "20 replies", "amount": 99900, "credits_added": 20}
        ]
        for pkg_data in packages:
            res = await db.execute(select(CreditPackage).where(CreditPackage.name == pkg_data["name"]))
            if not res.scalars().first():
                db.add(CreditPackage(**pkg_data))
        await db.commit()
if __name__ == "__main__": asyncio.run(seed_packages())
