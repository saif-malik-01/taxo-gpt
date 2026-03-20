import json
import re
import logging
from services.llm.bedrock_client import call_bedrock

logger = logging.getLogger(__name__)


def classify_document(
    extracted_text: str,
    filename: str,
    user_question: str = None,
    existing_case: dict = None,
) -> dict:
    """
    Classify a single document.

    Returns:
        document_type        : notice | show_cause_notice | order | demand_order |
                               previous_reply | reference_material | other
        is_primary           : True if doc has issues/allegations needing a reply
        parties              : {sender, recipient} or nulls
        same_case            : True / False / None
        same_matter          : True / False / None
        needs_clarification  : True only when genuinely impossible to determine
    """
    preview = (extracted_text or "")[:1500]

    existing_block = ""
    if existing_case:
        p       = existing_case.get("parties", {})
        summary = (existing_case.get("summary") or "")[:300]
        existing_block = (
            f"\nEXISTING CASE:\n"
            f"Sender: {p.get('sender', 'Unknown')}\n"
            f"Recipient: {p.get('recipient', 'Unknown')}\n"
            f"Summary: {summary}\n"
        )

    prompt = f"""Analyze this legal document. Return ONLY valid JSON — no explanation.

FILENAME: {filename}
{"USER MESSAGE: " + user_question if user_question else ""}
{existing_block}
DOCUMENT TEXT (first 1500 chars):
{preview}

1. document_type  — one of: notice, show_cause_notice, order, demand_order, previous_reply, reference_material, other
2. is_primary     — true if the document contains issues/allegations/charges/orders requiring a formal legal reply
3. parties        — sender and recipient if explicitly present; null otherwise
4. same_case      — true/false/null — belongs to same case as existing? null if no existing case
5. same_matter    — true/false/null — same subject, tax period, dispute? null if no existing case
6. needs_clarification — true ONLY if genuinely impossible to determine same_case from text alone

Return ONLY this JSON:
{{
    "document_type": "...",
    "is_primary": true,
    "parties": {{"sender": null, "recipient": null}},
    "same_case": null,
    "same_matter": null,
    "needs_clarification": false
}}"""

    try:
        result, _ = call_bedrock(prompt, temperature=0.0)
        result = result.strip()
        m = re.search(r'\{.*\}', result, re.DOTALL)
        if m:
            result = m.group()
        parsed = json.loads(result)
        logger.info(
            f"Doc classified → type={parsed.get('document_type')}, "
            f"primary={parsed.get('is_primary')}, same_case={parsed.get('same_case')}"
        )
        return parsed
    except Exception as e:
        logger.error(f"Document classification error: {e}")
        return {
            "document_type": "other",
            "is_primary": True,
            "parties": {"sender": None, "recipient": None},
            "same_case": None,
            "same_matter": None,
            "needs_clarification": False,
        }


def determine_routing(classification: dict, has_existing_case: bool) -> str:
    """
    Returns:
        "add_to_existing"
        "new_case"
        "ask_user"
    """
    if not has_existing_case:
        return "add_to_existing"

    if classification.get("needs_clarification"):
        return "ask_user"

    same_case = classification.get("same_case")
    if same_case is True:
        return "add_to_existing"
    if same_case is False:
        return "new_case"
    return "ask_user"