from services.llm.bedrock_client import call_bedrock
from services.database import AsyncSessionLocal
from services.models import UserProfile
from sqlalchemy import select
from starlette.concurrency import run_in_threadpool
import json
import logging

logger = logging.getLogger(__name__)

async def auto_update_profile(user_id: int, query: str, response: str):
    """
    Analyzes the latest interaction to extract permanent user facts 
    and updates the long-term Profile Memory using a non-blocking thread pool.
    """
    try:
        async with AsyncSessionLocal() as db:
            # 1. Fetch current profile
            result = await db.execute(select(UserProfile).where(UserProfile.user_id == user_id))
            profile = result.scalars().first()
            
            if not profile:
                return

            current_summary = profile.dynamic_summary or "No facts known yet."

            # 2. Extract facts via LLM (Offloaded to thread pool to avoid blocking)
            extraction_prompt = f"""
You are a background system that extracts LONG-TERM USER MEMORY.

Rules:
- Only extract facts about the user
- Ignore greetings, filler, questions
- Be concise
- If nothing important exists, output "NONE"

Conversation:
User: {query}
Assistant: {response}

Return JSON only:
{{
  "user_preferences": [],
  "facts": [],
  "ongoing_goals": []
}}
"""
            
            # Using run_in_threadpool because call_bedrock is synchronous (boto3)
            raw_output = await run_in_threadpool(call_bedrock, extraction_prompt)
            
            if not raw_output or "NONE" in raw_output.upper():
                return False

            # 3. Parse and Merge
            try:
                # Clean up potential markdown code blocks
                json_str = raw_output.strip()
                if json_str.startswith("```json"):
                    json_str = json_str.replace("```json", "", 1).replace("```", "", 1).strip()
                elif json_str.startswith("```"):
                    json_str = json_str.replace("```", "", 1).replace("```", "", 1).strip()
                    
                new_data = json.loads(json_str)
                
                # 1. Update Preferences (JSON)
                # Ensure existing preferences are preserved
                current_preferences = profile.preferences or {}
                new_prefs_list = new_data.get("user_preferences", [])
                
                # Simple heuristic: if the LLM returns a list of strings, 
                # we store them in a "highlights" or similar key, or just merge them.
                # To keep it flexible, let's keep a 'tags' or 'extracted' list in preferences.
                if new_prefs_list:
                    if "extracted_tags" not in current_preferences:
                        current_preferences["extracted_tags"] = []
                    
                    for pref in new_prefs_list:
                        if pref not in current_preferences["extracted_tags"]:
                            current_preferences["extracted_tags"].append(pref)
                    
                    profile.preferences = current_preferences

                # 2. Update Dynamic Summary (Text)
                # Combine facts and ongoing goals for the human-readable summary
                new_summary_items = []
                for item in new_data.get("facts", []):
                    new_summary_items.append(f"- {item}")
                for item in new_data.get("ongoing_goals", []):
                    new_summary_items.append(f"- Goal: {item}")
                
                if new_summary_items:
                    new_summary_text = "\n".join(new_summary_items)
                    
                    if current_summary == "No facts known yet." or not current_summary:
                        updated_summary = new_summary_text
                    else:
                        # Append new unique lines
                        final_lines = current_summary.split("\n")
                        for line in new_summary_items:
                            if line not in final_lines:
                                final_lines.append(line)
                        updated_summary = "\n".join(final_lines)
                    
                    profile.dynamic_summary = updated_summary.strip()

                # 3. Save if anything changed
                # SQLAlchemy tracks changes to the object, but for JSON we might need to flag it.
                from sqlalchemy.orm.attributes import flag_modified
                flag_modified(profile, "preferences")
                
                await db.commit()
                logger.info(f"Updated profile for user {user_id} (Summary & Preferences)")
                return True
            except json.JSONDecodeError:
                logger.error(f"Failed to parse Bedrock JSON output: {raw_output}")
                return False
                
    except Exception as e:
        logger.error(f"Error in auto_update_profile: {str(e)}")
        return False
        
    return False
