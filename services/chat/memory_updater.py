from services.llm.bedrock_client import call_bedrock
from services.database import AsyncSessionLocal
from services.models import UserProfile
from sqlalchemy import select
import json

async def auto_update_profile(user_id: int, query: str, response: str):
    """
    Analyzes the latest interaction to extract permanent user facts 
    and updates the long-term Profile Memory.
    """
    async with AsyncSessionLocal() as db:
        # 1. Fetch current profile
        result = await db.execute(select(UserProfile).where(UserProfile.user_id == user_id))
        profile = result.scalars().first()
        
        if not profile:
            return

        current_summary = profile.dynamic_summary or "No facts known yet."

        # 2. Ask LLM to extract facts
        extraction_prompt = f"""
You are a 'Memory Manager'. Your job is to update a long-term user profile summary based on a new chat interaction.

CURRENT PROFILE SUMMARY:
{current_summary}

NEW INTERACTION:
User: {query}
Assistant: {response}

TASK:
- Identify any NEW permanent facts about the user (e.g., their business type, location, tax status, professional role, or specific preferences).
- Merge them into the CURRENT PROFILE SUMMARY.
- If the new info contradicts old info, update it.
- Keep the summary professional, concise, and focused on GST/Tax relevance.
- If NO new facts were shared, return the CURRENT PROFILE SUMMARY exactly as is.

REVISED PROFILE SUMMARY:
"""
        
        updated_summary = call_bedrock(extraction_prompt)
        
        # 3. Save if changed
        if updated_summary and updated_summary.strip() != current_summary.strip():
            profile.dynamic_summary = updated_summary.strip()
            await db.commit()
            return True
            
    return False
