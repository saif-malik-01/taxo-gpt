import json
import re
import logging
from services.llm.bedrock_client import call_bedrock

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Keywords that strongly indicate a knowledge-base / case-law query
# These should NEVER be routed to 'summarize' (which is for uploaded docs)
# ---------------------------------------------------------------------------
_JUDGMENT_SIGNALS = [
    "judgment", "judgement", "case law", "case", "ruling", "court", "order",
    "gist", "held", "decided", "decision", "verdict", "bench", "petitioner",
    "respondent", "high court", "supreme court", "tribunal", "writ",
    "taxo.online", "tmi", "section ", "rule ",
]


def _is_knowledge_query(message: str, has_files: bool, active_case) -> bool:
    """
    Return True when the message is clearly asking about the legal knowledge base
    (judgments, act sections, etc.) rather than an uploaded document.

    Fires when:
    - No files are uploaded in this request, AND
    - No active case document is in context, AND
    - The message contains a judgment/knowledge keyword.
    """
    if has_files or active_case:
        return False
    q = message.lower()
    return any(kw in q for kw in _JUDGMENT_SIGNALS)


def classify_intent(
    message: str,
    active_case: dict = None,
    has_files: bool = False,
) -> dict:
    """
    Classify user intent.

    Returns:
        intent        : string (see options below)
        issue_numbers : list of int
        mode          : "defensive" | "in_favour" | null
        case_id       : int | null
        details       : brief reason
    """
    # ―― Fast path: judgment / knowledge query with no document context ――
    # Prevents "gist of sunscraft judgment" being routed to 'summarize'
    if _is_knowledge_query(message, has_files, active_case):
        logger.info("Intent → query_general (fast-path: knowledge query, no doc context)")
        return {"intent": "query_general", "issue_numbers": [], "mode": None, "case_id": None, "details": "fast-path: judgment keyword, no uploaded doc"}

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

    prompt = (
        'Classify user intent for a GST legal document assistant.\n'
        'Return ONLY valid JSON — no explanation.\n\n'
        'USER MESSAGE: "' + message + '"\n\n'
        'SESSION STATE:\n'
        '- Files uploaded with this message: ' + str(has_files) + '\n'
        '- Case state: ' + str(case_state or "none") + '\n'
        '- Issues extracted: ' + str(has_issues) + '\n'
        '- Pending (unreplied) issues: ' + str(has_pending) + '\n'
        '- Mode already set: ' + str(current_mode or "not set") + '\n'
        + issues_block + '\n\n'
        'INTENT OPTIONS (pick exactly one):\n'
        '  summarize       — show/update summary and issues FOR AN UPLOADED DOCUMENT\n'
        '                    USE ONLY WHEN: files were uploaded in this request, OR user explicitly\n'
        '                    asks to summarise/show issues from a document they already uploaded.\n'
        '                    DO NOT USE for questions about court judgments, case law, or GST law\n'
        '                    when no document has been uploaded.\n'
        '  draft_all       — generate replies for ALL pending issues\n'
        '  draft_specific  — generate reply for specific issue numbers only\n'
        '  confirm_mode    — user confirming mode in any phrasing\n'
        '                    DEFENSIVE: "yes defence", "go defensive", "protect taxpayer", "defend",\n'
        '                               "assessee side", "yes go ahead" (when awaiting_mode state)\n'
        '                    IN FAVOUR: "in favour of revenue", "support the notice", "department side",\n'
        '                               "revenue is right", "yes in favour"\n'
        '  update_issues   — user wants to merge/split/add/correct/remove issues OR says issues are missed\n'
        '  update_reply    — user wants to change reply for a specific issue\n'
        '  query_document  — question answerable from the uploaded document\n'
        '  query_mixed     — needs both document content and GST knowledge base\n'
        '  query_general   — pure GST question, no document context needed\n'
        '                    USE FOR: questions about judgments, case law, sections, rules, rates, etc.\n'
        '                    when the user has NOT uploaded a document in this session.\n'
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
        result, _ = call_bedrock(prompt, temperature=0.0)
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

    Returns:
        action     : merge | add | correct | remove | reextract
        issue_ids  : list of int
        new_text   : replacement text (for add/correct)
        merge_text : combined text (for merge)
    """
    issue_lines = [str(i["id"]) + ". " + i["text"][:100] for i in current_issues]
    issues_text = "\n".join(issue_lines)

    prompt = (
        'Parse this instruction about a legal issues list.\n'
        'Return ONLY valid JSON — no explanation.\n\n'
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
        result, _ = call_bedrock(prompt, temperature=0.0)
        result = result.strip()
        m = re.search(r'\{.*\}', result, re.DOTALL)
        if m:
            result = m.group()
        return json.loads(result)
    except Exception as e:
        logger.error("Issue update parse error: %s", e)
        return {"action": "reextract", "issue_ids": [], "new_text": None, "merge_text": None}