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
            extraction_prompt = f"""You are a background system that extracts PERMANENT, LONG-TERM user attributes from a message. 

Your goal is to extract ONLY facts that will still be true and useful 6 months from now.

STRICT EXTRACTION RULES:
1. ONLY extract information the user EXPLICITLY stated about themselves.
2. ONLY extract: 
   - Profession/Designation (e.g. "CA", "Advocate", "Business Owner")
   - Industry/Sector (e.g. "Real Estate", "Exports", "E-commerce")
   - Technical Profile (e.g. "Composition taxpayer", "SEZ unit")
   - Permanent Preferences (e.g. "always give me sections first", "respond in Hindi")
3. NEVER extract:
   - Case names, party names, or legal citations.
   - Specific questions, problems, or transient issues ("I have a notice today").
   - Names of clients or temporary project details.
   - Opinions or feelings that might change.
4. "IF IN DOUBT, LEAVE IT OUT": If a fact seems temporary, specific to a single interaction, or ambiguous, do not extract it.
5. If NO permanent long-term facts are explicitly stated, output exactly "NONE".

User Message to Analyze:
{query}

Output Format (JSON only):
{{
  "user_preferences": ["Style/language preferences. Example: 'Prefers Hindi responses'"],
  "facts": ["Self-identity facts. Example: 'CA by profession', 'Works in Exports'"],
  "ongoing_goals": ["Enduring career/business goals. Example: 'Wants to specialize in SEZ consulting'"]
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

                # Update Preferences (JSON)
                current_preferences = profile.preferences or {}
                new_prefs_list = new_data.get("user_preferences", [])

                if new_prefs_list:
                    if "extracted_tags" not in current_preferences:
                        current_preferences["extracted_tags"] = []
                    for pref in new_prefs_list:
                        if pref not in current_preferences["extracted_tags"]:
                            current_preferences["extracted_tags"].append(pref)
                    profile.preferences = current_preferences

                # Update Dynamic Summary (Text)
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
                        final_lines = current_summary.split("\n")
                        for line in new_summary_items:
                            if line not in final_lines:
                                final_lines.append(line)
                        updated_summary = "\n".join(final_lines)

                    profile.dynamic_summary = updated_summary.strip()

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
