import asyncio
import sys
import os

# Add the project root to sys.path to allow imports from apps/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from apps.api.src.db.session import AsyncSessionLocal
from apps.api.src.db.models.base import User, UserProfile
from apps.api.src.services.auth.utils import get_password_hash
from apps.api.src.services.payments import initialize_user_credits
from sqlalchemy.future import select

async def create_admin(email, password, full_name="Admin User"):
    async with AsyncSessionLocal() as db:
        # Check if user already exists
        result = await db.execute(select(User).where(User.email == email.lower()))
        existing_user = result.scalars().first()
        
        if existing_user:
            print(f"Error: User with email {email} already exists.")
            return

        print(f"Creating admin user: {email}...")

        # 1. Create User
        new_user = User(
            email=email.lower(),
            password_hash=get_password_hash(password),
            full_name=full_name,
            role="admin",
            is_verified=True,
            onboarding_step=2 # Jump to fully onboarded
        )
        db.add(new_user)
        await db.commit()
        await db.refresh(new_user)

        # 2. Create Profile
        db.add(UserProfile(user_id=new_user.id))
        
        # 3. Initialize Credits (Applies default welcome package if available)
        await initialize_user_credits(new_user.id, db, use_welcome_package=True)
        
        await db.commit()
        print(f"✅ Successfully created admin user: {email}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scripts/create_admin.py <email> <password> [full_name]")
        sys.exit(1)
    
    email = sys.argv[1]
    password = sys.argv[2]
    full_name = sys.argv[3] if len(sys.argv) > 3 else "System Admin"
    
    asyncio.run(create_admin(email, password, full_name))
