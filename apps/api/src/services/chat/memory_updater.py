from apps.api.src.services.llm.bedrock import call_bedrock
from apps.api.src.db.session import AsyncSessionLocal
from apps.api.src.db.models.base import UserProfile
from sqlalchemy import select
from starlette.concurrency import run_in_threadpool
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
            extraction_prompt = f"""You are a background system that extracts PERMANENT USER ATTRIBUTES from a user's message.

STRICT RULES:
- Extract ONLY what the user explicitly stated about themselves in their own message
- ONLY extract: profession, designation, industry, GST registration type, preferred response style, preferred language, ongoing matters they explicitly stated they are working on
- DO NOT extract: case names, judgment names, legal topics, questions they asked, legal issues they mentioned
- DO NOT infer anything — only extract directly stated facts
- DO NOT extract anything from context or implications
- If the user did not explicitly state anything about themselves, output "NONE"

User Message:
{query}

Return JSON only:
{{
  "user_preferences": [],
  "facts": [],
  "ongoing_goals": []
}}"""

            raw_output, usage = await run_in_threadpool(call_bedrock, extraction_prompt)

            if not raw_output or "NONE" in raw_output.upper():
                return False

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
