"""
services/document/intent_classifier.py
Classifies user intent for the document draft reply feature.
Uses new BedrockLLMClient instead of old call_bedrock.
"""

import json
import re
import logging

logger = logging.getLogger(__name__)

_llm = None

def _get_llm():
    global _llm
    if _llm is None:
        from retrieval.bedrock_llm import BedrockLLMClient
        _llm = BedrockLLMClient()
    return _llm


def classify_intent(
    message: str,
    active_case: dict = None,
    has_files: bool = False,
) -> dict:
    """
    Classify user intent for document feature.
    Returns: intent, issue_numbers, mode, case_id, details
    """
    case_state     = None
    issues_preview = ""
    has_issues     = False
    has_pending    = False
    current_mode   = None

    if active_case:
        case_state   = active_case.get("state")
        current_mode = active_case.get("mode")
        issues       = active_case.get("issues", [])
        has_issues   = bool(issues)
        has_pending  = any(not i.get("reply") for i in issues)
        if issues:
            issue_lines    = ["  " + str(i["id"]) + ". " + i["text"][:80] for i in issues[:6]]
            issues_preview = "\n".join(issue_lines)

    issues_block = ("Current issues:\n" + issues_preview) if issues_preview else "No issues yet."

    system = "You are an intent classifier for a GST legal document assistant. Return ONLY valid JSON."

    prompt = (
        'Classify user intent.\n\n'
        'USER MESSAGE: "' + message + '"\n\n'
        'SESSION STATE:\n'
        '- Files uploaded with this message: ' + str(has_files) + '\n'
        '- Case state: ' + str(case_state or "none") + '\n'
        '- Issues extracted: ' + str(has_issues) + '\n'
        '- Pending (unreplied) issues: ' + str(has_pending) + '\n'
        '- Mode already set: ' + str(current_mode or "not set") + '\n'
        + issues_block + '\n\n'
        'INTENT OPTIONS (pick exactly one):\n'
        '  summarize       — show/update summary and issues\n'
        '  draft_all       — generate replies for ALL pending issues\n'
        '  draft_specific  — generate reply for specific issue numbers only\n'
        '  confirm_mode    — user confirming mode (defensive/in_favour) in any phrasing\n'
        '                    DEFENSIVE: "yes defence", "go defensive", "protect taxpayer", "defend",\n'
        '                               "assessee side", "yes go ahead" (when awaiting_mode state),\n'
        '                               "yes", "ok", "proceed" when state=awaiting_mode and no mode set\n'
        '                    IN FAVOUR: "in favour", "support notice", "department side",\n'
        '                               "revenue is right", "in favor", "favour of revenue"\n'
        '  update_issues   — user wants to merge/split/add/correct/remove issues OR says issues are missed\n'
        '  update_reply    — user wants to change reply for a specific issue\n'
        '  query_document  — question answerable from the uploaded document\n'
        '  query_mixed     — needs both document content and GST knowledge base\n'
        '  query_general   — pure GST question, no document context needed\n'
        '  switch_case     — user wants to work on a previously archived case\n'
        '  new_case        — user explicitly starting fresh for a different case\n\n'
        'Return ONLY this JSON:\n'
        '{\n'
        '    "intent": "...",\n'
        '    "issue_numbers": [],\n'
        '    "mode": null,\n'
        '    "case_id": null,\n'
        '    "details": "one-line reason"\n'
        '}'
    )

    try:
        result = _get_llm().call(
            system_prompt=system,
            user_message=prompt,
            max_tokens=512,
            temperature=0.0,
            label="intent_classifier",
        )
        if not result:
            raise ValueError("Empty LLM response")
        result = result.strip()
        m = re.search(r'\{.*\}', result, re.DOTALL)
        if m:
            result = m.group()
        parsed = json.loads(result)
        logger.info(
            "Intent → %s | mode=%s | issues=%s",
            parsed.get("intent"), parsed.get("mode"), parsed.get("issue_numbers")
        )
        return parsed
    except Exception as e:
        logger.error("Intent classification error: %s", e)
        if has_files:
            return {"intent": "summarize", "issue_numbers": [], "mode": None, "case_id": None, "details": "fallback"}
        return {"intent": "query_general", "issue_numbers": [], "mode": None, "case_id": None, "details": "fallback"}


def parse_issue_update(message: str, current_issues: list) -> dict:
    """
    Parse a user's instruction about changing the issues list.
    Returns: action, issue_ids, new_text, merge_text
    """
    issue_lines = [str(i["id"]) + ". " + i["text"][:100] for i in current_issues]
    issues_text = "\n".join(issue_lines)

    system = "You are a parser for legal issue list instructions. Return ONLY valid JSON."

    prompt = (
        'Parse this instruction about a legal issues list.\n\n'
        'USER INSTRUCTION: "' + message + '"\n\n'
        'CURRENT ISSUES:\n'
        + issues_text + '\n\n'
        'Determine:\n'
        '  action     : merge | add | correct | remove | reextract\n'
        '               Use "reextract" when user says issues are missed but does NOT provide specific text\n'
        '               Use "add" when user provides specific missing issue text\n'
        '  issue_ids  : list of issue IDs involved (integers); empty for reextract/add-new\n'
        '  new_text   : verbatim text from user for add or correct; null otherwise\n'
        '  merge_text : combined text for merged issue; null otherwise\n\n'
        'Return ONLY this JSON:\n'
        '{\n'
        '    "action": "...",\n'
        '    "issue_ids": [],\n'
        '    "new_text": null,\n'
        '    "merge_text": null\n'
        '}'
    )

    try:
        result = _get_llm().call(
            system_prompt=system,
            user_message=prompt,
            max_tokens=256,
            temperature=0.0,
            label="parse_issue_update",
        )
        if not result:
            raise ValueError("Empty response")
        result = result.strip()
        m = re.search(r'\{.*\}', result, re.DOTALL)
        if m:
            result = m.group()
        return json.loads(result)
    except Exception as e:
        logger.error("Issue update parse error: %s", e)
        return {"action": "reextract", "issue_ids": [], "new_text": None, "merge_text": None}