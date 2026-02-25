import asyncio
from services.database import AsyncSessionLocal
from services.models import CreditPackage
from sqlalchemy import select

async def seed_packages():
    async with AsyncSessionLocal() as db:
        packages = [
            {
                "name": "draft-1",
                "title": "Solo Draft Reply",
                "description": "Perfect for a single detailed notice reply.",
                "amount": 79900, # In paise (799 INR)
                "credits_added": 1
            },
            {
                "name": "draft-20",
                "title": "Draft Pro Package",
                "description": "Best for power users. 20 draft replies with document context.",
                "amount": 99900, # In paise (999 INR)
                "credits_added": 20
            }
        ]
        
        for pkg_data in packages:
            # Check if exists
            res = await db.execute(select(CreditPackage).where(CreditPackage.name == pkg_data["name"]))
            if not res.scalars().first():
                pkg = CreditPackage(**pkg_data)
                db.add(pkg)
                print(f"Adding package: {pkg_data['name']}")
            else:
                print(f"Package {pkg_data['name']} already exists.")
        
        await db.commit()
        print("Seeding completed.")

if __name__ == "__main__":
    asyncio.run(seed_packages())
