from apps.api.src.services.llm.bedrock import get_async_bedrock_client, _strip_thinking
from apps.api.src.db.session import AsyncSessionLocal
from apps.api.src.db.models.base import UserProfile
from sqlalchemy import select
import json
import logging

logger = logging.getLogger(__name__)

async def auto_update_profile(user_id: int, query: str, response: str):
    """
    Analyzes ONLY the user's message to extract permanent user attributes.
    Response is intentionally ignored to prevent case names / legal topics
    from being stored as user facts.
    """
    try:
        async with AsyncSessionLocal() as db:
            # 1. Fetch current profile
            result = await db.execute(select(UserProfile).where(UserProfile.user_id == user_id))
            profile = result.scalars().first()

            if not profile:
                return

            current_summary = profile.dynamic_summary or "No facts known yet."

            # 2. Extract ONLY explicit user attributes from the user's own message
            extraction_prompt = f"""You are a memory editor for a user persona. Your job is to take the OLD profile, analyze the NEW message, and output a properly reconciled profile by extracting PERMANENT, LONG-TERM user attributes.

STRICT RULES:
1. ONLY extract information the user EXPLICITLY stated about themselves.
2. RECONCILE outdated facts. If the new message contradicts the old profile (e.g., from "Unregistered" to "Registered"), REPLACE the old fact. Do not keep contradictions.
3. NEVER extract temporary facts, specific questions, case names, or opinions that might change.
4. If the new message contains NO new permanent facts, output the facts exactly as they were in the OLD profile.
5. "IF IN DOUBT, LEAVE IT OUT": If a fact seems temporary, do not include it.

OLD User Profile (Current Facts):
{current_summary}

NEW User Message:
{query}

Output Format (JSON only):
{{
  "user_preferences": ["Style/language preferences. Example: 'Prefers Hindi responses'"],
  "facts": ["Current self-identity facts only. Example: 'Registered GST taxpayer'"],
  "ongoing_goals": ["Enduring career/business goals."]
}}"""

            llm = await get_async_bedrock_client()
            raw_output = await llm.call(
                system_prompt="",
                user_message=extraction_prompt,
                max_tokens=1024,
                temperature=0.0,
                label="memory_update",
            )

            if not raw_output or "NONE" in raw_output.upper():
                return False

            # strip Qwen3 thinking block before JSON parsing
            raw_output = _strip_thinking(raw_output)

            # 3. Parse and Merge
            try:
                json_str = raw_output.strip()
                if json_str.startswith("```json"):
                    json_str = json_str.replace("```json", "", 1).replace("```", "", 1).strip()
                elif json_str.startswith("```"):
                    json_str = json_str.replace("```", "", 1).replace("```", "", 1).strip()

                new_data = json.loads(json_str)

                # Update Preferences (JSON) - REPLACEMENT logic
                current_preferences = profile.preferences or {}
                new_prefs_list = new_data.get("user_preferences", [])
                
                # Replace tags entirely with reconciled list from LLM
                current_preferences["extracted_tags"] = new_prefs_list
                profile.preferences = current_preferences

                # Update Dynamic Summary (Text) - REPLACEMENT logic
                new_summary_items = []
                for item in new_data.get("facts", []):
                    new_summary_items.append(f"- {item}")
                for item in new_data.get("ongoing_goals", []):
                    new_summary_items.append(f"- Goal: {item}")

                updated_summary = "\n".join(new_summary_items).strip()
                if updated_summary:
                    profile.dynamic_summary = updated_summary
                elif current_summary != "No facts known yet.":
                    pass # Don't overwrite with empty if nothing returned

                from sqlalchemy.orm.attributes import flag_modified
                flag_modified(profile, "preferences")

                await db.commit()
                logger.info(f"Updated profile for user {user_id}")
                return True

            except json.JSONDecodeError:
                logger.error(f"Failed to parse Bedrock JSON output: {raw_output}")
                return False

    except Exception as e:
        logger.error(f"Error in auto_update_profile: {str(e)}")
        return False

    return False
