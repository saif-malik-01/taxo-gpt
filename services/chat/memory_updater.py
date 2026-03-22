"""
services/chat/memory_updater.py
Analyses the user's message to extract permanent user attributes.
Response is intentionally ignored to prevent case names / legal topics
from being stored as user facts.

Changes from original:
  1. Uses retrieval.bedrock_llm.BedrockLLMClient (old call_bedrock removed).
  2. Added _is_trivial_message() guard — skips the Bedrock call for short
     confirmations ("yes", "ok", "Option 1", "defence" etc.) that cannot
     contain useful user profile facts. This avoids a wasted LLM round-trip
     on every confirmation message in the document feature.
  3. Thread-safe lazy init for the LLM client.
"""

import json
import logging
import re
import threading

from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified
from starlette.concurrency import run_in_threadpool

from services.database import AsyncSessionLocal
from services.models import UserProfile

logger = logging.getLogger(__name__)

# ── Lazy LLM client — thread-safe ─────────────────────────────────────────────
_llm_client = None
_llm_lock   = threading.Lock()


def _get_llm():
    global _llm_client
    if _llm_client is None:
        with _llm_lock:
            if _llm_client is None:
                from retrieval.bedrock_llm import BedrockLLMClient
                _llm_client = BedrockLLMClient()
    return _llm_client


# ── Trivial-message patterns — skip Bedrock call for these ────────────────────
# These patterns match confirmations, mode selections, and one-word replies
# that cannot possibly contain user profile facts worth extracting.
_TRIVIAL_MAX_WORDS = 8

_TRIVIAL_PATTERNS = re.compile(
    r"""^(
        yes|no|ok|okay|sure|proceed|confirm|continue|go\s+ahead|
        option\s*[12]|[12]|
        (yes\s+)?(defence|defensive|protect|assessee|taxpayer)|
        (yes\s+)?in\s+favou?r|
        (yes\s+)?proceed|
        done|thanks|thank\s+you|perfect|got\s+it|understood|
        next|more|show\s+me|tell\s+me|
        hi|hello|hey|
        update|change|correct|remove|merge|add
    )$""",
    re.IGNORECASE | re.VERBOSE,
)


def _is_trivial_message(query: str) -> bool:
    """
    Return True if the message is too short or too generic to contain
    extractable user profile facts.  Skips the Bedrock call entirely.
    """
    stripped = query.strip()
    if not stripped:
        return True
    # Too short to contain meaningful facts
    if len(stripped.split()) <= _TRIVIAL_MAX_WORDS:
        if _TRIVIAL_PATTERNS.match(stripped):
            return True
    return False


# ── Extraction prompt ─────────────────────────────────────────────────────────
_EXTRACTION_SYSTEM = """You are a background system that extracts PERMANENT USER ATTRIBUTES from a user's message.

STRICT RULES:
- Extract ONLY what the user explicitly stated about themselves in their own message.
- ONLY extract: profession, designation, industry, GST registration type,
  preferred response style, preferred language, ongoing matters they explicitly stated.
- DO NOT extract: case names, judgment names, legal topics, questions asked, legal issues mentioned.
- DO NOT infer anything — only directly stated facts.
- If the user did not explicitly state anything about themselves, output "NONE".

Return JSON only:
{
  "user_preferences": [],
  "facts": [],
  "ongoing_goals": []
}"""


async def auto_update_profile(user_id: int, query: str, response: str):
    """
    Analyses ONLY the user's message to extract permanent user attributes.
    Response is intentionally ignored.

    Skips the Bedrock call entirely for trivial/confirmation messages to
    avoid wasting LLM calls on messages like "yes", "ok", "Option 1" etc.
    """
    # Fast-path: skip trivial messages without hitting Bedrock
    if _is_trivial_message(query):
        logger.debug(f"auto_update_profile: skipping trivial message for user {user_id}")
        return False

    try:
        async with AsyncSessionLocal() as db:
            result  = await db.execute(
                select(UserProfile).where(UserProfile.user_id == user_id)
            )
            profile = result.scalars().first()
            if not profile:
                return False

            current_summary = profile.dynamic_summary or "No facts known yet."

            llm        = _get_llm()
            raw_output = await run_in_threadpool(
                llm.call,
                _EXTRACTION_SYSTEM,
                f"User Message:\n{query}",
                512,
                0.1,
                "memory_updater",
            )

            if not raw_output or "NONE" in raw_output.upper():
                return False

            try:
                json_str = raw_output.strip()
                if json_str.startswith("```json"):
                    json_str = json_str.replace("```json", "", 1).replace("```", "", 1).strip()
                elif json_str.startswith("```"):
                    json_str = json_str.replace("```", "", 1).replace("```", "", 1).strip()
                new_data = json.loads(json_str)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse memory_updater JSON: {raw_output}")
                return False

            current_preferences = profile.preferences or {}
            new_prefs_list      = new_data.get("user_preferences", [])
            if new_prefs_list:
                if "extracted_tags" not in current_preferences:
                    current_preferences["extracted_tags"] = []
                for pref in new_prefs_list:
                    if pref not in current_preferences["extracted_tags"]:
                        current_preferences["extracted_tags"].append(pref)
                profile.preferences = current_preferences

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

            flag_modified(profile, "preferences")
            await db.commit()
            logger.info(f"Updated profile for user {user_id}")
            return True

    except Exception as e:
        logger.error(f"Error in auto_update_profile: {str(e)}", exc_info=True)
        return False