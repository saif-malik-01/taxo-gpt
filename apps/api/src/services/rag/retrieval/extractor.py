"""
apps/api/src/services/rag/retrieval/extractor.py
Stage 1  -  Query clarification
Stage 2A  -  Regex extraction (mirrors indexing L3)
Stage 2B  -  LLM field extraction (mirrors indexing L1)
Stage 2C  -  Intent + score_weights + response_hierarchy
2A, 2B, 2C run in parallel via asyncio.gather.
"""

import re
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from apps.api.src.services.llm.bedrock import AsyncBedrockLLMClient
from apps.api.src.services.rag.models import IntentResult, SessionMessage, Stage2AResult, Stage2BResult
from apps.api.src.services.rag.retrieval.regex_fallback import extract_fallback
from apps.api.src.core.normalizer import universal_normalise, whitespace_split_normalise
import logging

logger = logging.getLogger(__name__)

# -- Stage 1 -------------------------------------------------------------------

_S1_SYSTEM = """You are a query classifier for an Indian tax law chatbot.
Given conversation history and the current query, classify:
- SELF_CONTAINED: complete standalone question
- DEPENDENT: references prior context ("that", "this", "the above", "same")
- FOLLOW_UP: only makes sense with prior context

If DEPENDENT or FOLLOW_UP, rewrite into one complete standalone question.
Return ONLY valid JSON:
{"type": "SELF_CONTAINED"|"DEPENDENT"|"FOLLOW_UP", "rewritten_query": null or "..."}"""


class Stage1Clarifier:
    def __init__(self, llm: AsyncBedrockLLMClient):
        self._llm = llm

    async def clarify(self, query: str, history: List[SessionMessage]) -> str:
        if not history:
            return query
        hist_text = "\n".join(
            f"[{i+1}] User: {m.user_query}\n    Assistant: {m.llm_response[:300]}"
            for i, m in enumerate(history[-2:])
        )
        result = await self._llm.call_json(
            system_prompt=_S1_SYSTEM,
            user_message=f"History (last 2 turns):\n{hist_text}\n\nCurrent query: {query}",
            max_tokens=512,
            label="stage1",
        )
        if not result:
            return query
        if result.get("type") in ("DEPENDENT", "FOLLOW_UP") and result.get("rewritten_query"):
            rewritten = result["rewritten_query"]
            logger.info(f"Stage 1 rewrite: {rewritten[:100]}")
            return rewritten
        return query


# -- Stage 2A ------------------------------------------------------------------

def _extract_citation(text: str) -> Optional[str]:
    """Extract and normalise any taxo.online or MANU citation from text."""
    m = re.search(r"MANU/[A-Z]+/\d+/\d+", text, re.IGNORECASE)
    if m:
        return m.group(0).upper()

    m = re.search(r"(\d{4})\s+SCC\s+Online\s+(\d+)", text, re.IGNORECASE)
    if m:
        return f"{m.group(1)} SCC Online {m.group(2)}"

    m = re.search(r"(\d{2,4})\s+taxo[\s.\-]?online\s+(\d+)", text, re.IGNORECASE)
    if m:
        year_raw = m.group(1)
        number   = m.group(2)
        year = year_raw if len(year_raw) == 4 else (
            f"20{year_raw}" if int(year_raw) < 50 else f"19{year_raw}"
        )
        return f"{year} Taxo.online {number}"

    m = re.search(r"taxo[\s.\-]?(?:online\s+)?(\d+)(?!\d)", text, re.IGNORECASE)
    if m:
        return f"Taxo.online {m.group(1)}"

    return None


class Stage2ARegex:
    def extract(self, query: str) -> Stage2AResult:
        try:
            result   = extract_fallback(query)
            norm     = []
            for field in ("sections", "rules", "notifications", "circulars"):
                norm.extend(result.get(field, []))
            raw      = result.get("topics", [])
            citation = _extract_citation(query)
            logger.debug(f"2A: {len(norm)} norm, {len(raw)} raw, citation={citation}")
            return Stage2AResult(
                normalised_tokens=norm,
                raw_tokens=raw,
                citation=citation,
            )
        except Exception as e:
            logger.error(f"Stage 2A failed: {e}")
            return Stage2AResult([], [], None)


# -- Stage 2B ------------------------------------------------------------------

_S2B_SYSTEM = """You are a precise field extractor for an Indian tax law chatbot.
Extract fields from the user query. Return ONLY valid JSON.

RULES:
1. parties / person_names: extract ANY name  -  full or partial, one word is fine.
   Extract EXACTLY as written  -  do not expand or correct.
2. case_number: extract as-is, preserve all punctuation exactly.
3. citation: recognise ALL these taxo.online variants:
   "2017 taxo.online 42", "2017 taxo online 42", "taxo online 42",
   "taxo 42", "17 taxo online 42", "taxoonline 42", "Taxo.Online 42",
   "2017 SCC Online 234", "MANU/SC/0872/2021"
   Normalise to: "YYYY Taxo.online N" or "MANU/..." as appropriate.
4. HSN/SAC  -  CRITICAL:
   - HSN (8-digit): GOODS/PRODUCTS only. Exact code only  -  never guess.
   - SAC (6-digit): SERVICES only. Exact code only  -  never guess.
   - Product described but no code  ->  hsn_code: null
   - Service described but no code  ->  sac_code: null
5. acts: extract even if implied ("GST"  ->  "CGST Act" / "IGST Act",
   "income tax"  ->  "Income Tax Act 1961")
6. Return null for fields not mentioned. Return [] for empty lists.

Return:
{
  "sections": [], "rules": [], "notifications": [], "circulars": [],
  "acts": [], "keywords": [], "topics": [],
  "form_name": null, "form_number": null,
  "case_name": null, "parties": [], "person_names": [],
  "case_number": null, "court": null, "court_level": null,
  "citation": null, "decision_type": null,
  "hsn_code": null, "sac_code": null, "issued_by": null
}

court_level: "HC"|"SC"|"ITAT"|"CESTAT"|"GSTAT"|"AAR"|"Other"|null
decision_type: "in_favour_of_assessee"|"in_favour_of_revenue"|"remanded"|"dismissed"|null"""


class Stage2BLLM:
    def __init__(self, llm: AsyncBedrockLLMClient):
        self._llm = llm

    async def extract(self, query: str) -> Stage2BResult:
        raw = await self._llm.call_json(
            system_prompt=_S2B_SYSTEM,
            user_message=f"Query: {query}",
            max_tokens=4096,  # Qwen3 thinking tokens consume budget; 4096 prevents truncation
            label="stage2b",
        )
        if not raw:
            return Stage2BResult()
        try:
            return Stage2BResult(
                sections      = _lst(raw.get("sections")),
                rules         = _lst(raw.get("rules")),
                notifications = _lst(raw.get("notifications")),
                circulars     = _lst(raw.get("circulars")),
                acts          = _lst(raw.get("acts")),
                keywords      = _lst(raw.get("keywords")),
                topics        = _lst(raw.get("topics")),
                form_name     = _opt(raw.get("form_name")),
                form_number   = _opt(raw.get("form_number")),
                case_name     = _opt(raw.get("case_name")),
                parties       = _lst(raw.get("parties")),
                person_names  = _lst(raw.get("person_names")),
                case_number   = _opt(raw.get("case_number")),
                court         = _opt(raw.get("court")),
                court_level   = _opt(raw.get("court_level")),
                citation      = _normalise_citation(_opt(raw.get("citation"))),
                decision_type = _opt(raw.get("decision_type")),
                hsn_code      = _opt(raw.get("hsn_code")),
                sac_code      = _opt(raw.get("sac_code")),
                issued_by     = _opt(raw.get("issued_by")),
            )
        except Exception as e:
            logger.error(f"Stage 2B build failed: {e}")
            return Stage2BResult()


class SyncStage2BLLM:
    def __init__(self, llm):
        self._llm = llm

    def extract(self, query: str) -> Stage2BResult:
        raw = self._llm.call_json(
            system_prompt=_S2B_SYSTEM,
            user_message=f"Query: {query}",
            max_tokens=4096,  # Qwen3 thinking tokens consume budget; 4096 prevents truncation
            label="stage2b",
        )
        if not raw:
            return Stage2BResult()
        try:
            return Stage2BResult(
                sections      = _lst(raw.get("sections")),
                rules         = _lst(raw.get("rules")),
                notifications = _lst(raw.get("notifications")),
                circulars     = _lst(raw.get("circulars")),
                acts          = _lst(raw.get("acts")),
                keywords      = _lst(raw.get("keywords")),
                topics        = _lst(raw.get("topics")),
                form_name     = _opt(raw.get("form_name")),
                form_number   = _opt(raw.get("form_number")),
                case_name     = _opt(raw.get("case_name")),
                parties       = _lst(raw.get("parties")),
                person_names  = _lst(raw.get("person_names")),
                case_number   = _opt(raw.get("case_number")),
                court         = _opt(raw.get("court")),
                court_level   = _opt(raw.get("court_level")),
                citation      = _normalise_citation(_opt(raw.get("citation"))),
                decision_type = _opt(raw.get("decision_type")),
                hsn_code      = _opt(raw.get("hsn_code")),
                sac_code      = _opt(raw.get("sac_code")),
                issued_by     = _opt(raw.get("issued_by")),
            )
        except Exception as e:
            logger.error(f"Stage 2B (sync) build failed: {e}")
            return Stage2BResult()


# -- Stage 2C ------------------------------------------------------------------

_S2C_SYSTEM = """You are an intent classifier for an Indian tax law chatbot.

Analyse the query and return:

1. INTENT  -  choose ONE:
   JUDGMENT: user asks for court cases, judgements, rulings, orders, held, precedents.
             Signals: "judgement", "case", "ruling", "held", "HC", "SC", "ITAT",
             party names, citation numbers, "writ", "appeal", "order".

   RATE: user asks specifically for the GST/tax RATE PERCENTAGE on a product or service.
         Signals: "rate", "GST on", "tax on", "how much GST", "what is the rate",
         "GST rate", "%", HSN/SAC codes, product/service names WITH rate intent.
         NOT RATE if user asks "what is section X" or "explain" or "define"  - 
         those are GENERAL even if the section is about rates.

   FORM: user asks about a specific GST form, filing procedure, or how to file.
         Signals: "form", "GSTR-", "REG-", "how to file", "format", "filing".

   GENERAL: definitions, explanations, provisions, concepts, procedures,
            compliance, "what is", "explain", "define", "section X", "rule X",
            "meaning of", "applicability". DEFAULT to GENERAL when in doubt.

   CHIT_CHAT: user just says hi, hello, thank you, or asks your name/identity.
              WARNING: If the query has ANY GST/Tax question alongside a greeting (e.g., "Hi, what is the rate of GST on..."), DO NOT use CHIT_CHAT. Classify as the appropriate tax intent.

   OUT_OF_SCOPE: completely unrelated topics (cooking, medical, coding, non-tax laws) OR malicious/harmful prompts.

2. CONFIDENCE: 0-100.
   Be conservative  -  default to GENERAL if unsure. Set confidence < 70 if unclear.

3. SCORE_WEIGHTS: additive score boosts per chunk_type.
   Things user EXPLICITLY asks for  ->  0.10
   Things DIRECTLY RELATED to primary ask  ->  0.05
   Everything else  ->  0.00

   chunk_type values: judgment, cgst_section, igst_section, cgst_rule,
   igst_rule, gstat_rule, notification, circular, faq, gst_form,
   hsn_code, sac_code, case_scenario, illustration, analytical_review,
   article, contemporary_issue, draft_reply, council_decision, solved_query,
   financial_budget

4. RESPONSE_HIERARCHY: order for LLM answer.
   STANDARD (use unless user explicitly asks for specific type):
   ["act", "rules", "notification_circular_faq",
    "case_scenario_illustration", "judgment",
    "analytical_review", "summary"]

   Reorder ONLY if user explicitly requests:
   "give me budget"      ->  act (financial_budget) to position 1
   "give me judgement"  ->  judgment to position 1
   "give me rate"       ->  notification_circular_faq to position 1
   "show me form"       ->  rules to position 1

   analytical_review always second to last. summary always last.

Return ONLY valid JSON:
{
  "intent": "...",
  "confidence": 0-100,
  "score_weights": {"chunk_type": weight, ...},
  "response_hierarchy": [...]
}"""


_STANDARD_HIERARCHY = [
    "act", "rules", "notification_circular_faq",
    "case_scenario_illustration", "judgment",
    "analytical_review", "summary",
]


class Stage2CIntent:
    def __init__(self, llm: AsyncBedrockLLMClient):
        self._llm = llm

    async def classify(self, query: str) -> IntentResult:
        raw = await self._llm.call_json(
            system_prompt=_S2C_SYSTEM,
            user_message=f"Query: {query}",
            max_tokens=512,
            label="stage2c",
        )
        if not raw:
            return _default_intent()
        try:
            intent     = str(raw.get("intent", "GENERAL")).upper()
            confidence = max(0, min(100, int(raw.get("confidence", 50))))
            weights    = {k: float(v) for k, v in (raw.get("score_weights") or {}).items()}
            hierarchy  = _lst(raw.get("response_hierarchy")) or _STANDARD_HIERARCHY[:]

            for item in ["analytical_review", "summary"]:
                if item in hierarchy:
                    hierarchy.remove(item)
            hierarchy += ["analytical_review", "summary"]

            result = IntentResult(
                intent=intent,
                confidence=confidence,
                score_weights=weights,
                response_hierarchy=hierarchy,
            )
            logger.info(
                f"2C: intent={intent} conf={confidence} "
                f"weights={weights} hierarchy={hierarchy[:3]}..."
            )
            return result
        except Exception as e:
            logger.error(f"Stage 2C failed: {e}")
            return _default_intent()


# -- Combined extractor --------------------------------------------------------

class CombinedExtractor:
    def __init__(self, llm: AsyncBedrockLLMClient):
        self._regex  = Stage2ARegex()
        self._llm2b  = Stage2BLLM(llm)
        self._intent = Stage2CIntent(llm)

    async def extract(
        self, query: str
    ) -> Tuple[Stage2AResult, Stage2BResult, IntentResult]:
        """
        Runs all three stages in parallel:
          - Stage2A (regex) in a thread executor (CPU-bound)
          - Stage2B (LLM extraction) as native async I/O
          - Stage2C (intent) as native async I/O
        Zero blocking on the event loop.
        """
        loop = asyncio.get_running_loop()
        regex_res, llm2b_res, intent_res = await asyncio.gather(
            loop.run_in_executor(None, self._regex.extract, query),
            self._llm2b.extract(query),
            self._intent.classify(query),
            return_exceptions=True,
        )
        if isinstance(regex_res, Exception):
            logger.error(f"Stage 2A failed: {regex_res}")
            regex_res = Stage2AResult([], [], None)
        if isinstance(llm2b_res, Exception):
            logger.error(f"Stage 2B failed: {llm2b_res}")
            llm2b_res = Stage2BResult()
        if isinstance(intent_res, Exception):
            logger.error(f"Stage 2C failed: {intent_res}")
            intent_res = _default_intent()
        return regex_res, llm2b_res, intent_res


# -- BM25 query expansion dictionary ------------------------------------------

_BM25_EXPANSIONS: Dict[str, List[str]] = {
    "itc":   ["input_tax_credit"],
    "rcm":   ["reverse_charge_mechanism"],
    "tds":   ["tax_deducted_at_source"],
    "tcs":   ["tax_collected_at_source"],
    "igst":  ["integrated_goods_services_tax"],
    "cgst":  ["central_goods_services_tax"],
    "sgst":  ["state_goods_services_tax"],
    "utgst": ["union_territory_goods_services_tax"],
    "gstr":  ["gst_return"],
    "gstin": ["gst_identification_number"],
    "sez":   ["special_economic_zone"],
    "dtaa":  ["double_taxation_avoidance_agreement"],
    "ay":    ["assessment_year"],
    "fy":    ["financial_year"],
}


def _expand_tokens(tokens: List[str]) -> List[str]:
    expanded = list(tokens)
    for token in tokens:
        t_lower = token.lower()
        if t_lower in _BM25_EXPANSIONS:
            expanded += _BM25_EXPANSIONS[t_lower]
    return expanded


def build_bm25_keyword_document(a: Stage2AResult, b: Stage2BResult) -> str:
    parts: List[str] = []
    parts += a.normalised_tokens
    parts += a.raw_tokens

    l1: List[str] = []
    for v in b.sections:
        t = universal_normalise(v)
        if t:
            l1.append(t)
    for v in b.rules:
        t = universal_normalise(v)
        if t:
            l1.append(t)
    for v in b.notifications:
        t = universal_normalise(v)
        if t:
            l1.append(t)
    for v in b.circulars:
        t = universal_normalise(v)
        if t:
            l1.append(t)
    for vals in (b.acts, b.keywords, b.topics):
        for v in vals:
            l1 += whitespace_split_normalise(v)
    for v in (b.form_name, b.form_number, b.case_name, b.court,
              b.citation, b.issued_by):
        if v:
            l1 += whitespace_split_normalise(v)
    for v in (b.hsn_code, b.sac_code):
        if v:
            l1.append(v)
    if b.court_level:
        l1.append(b.court_level)
    for name in b.parties + b.person_names:
        l1 += whitespace_split_normalise(name)

    l1    = _expand_tokens(l1)
    parts = _expand_tokens(parts)
    parts += l1 * 3
    doc = " ".join(t for t in parts if t)
    logger.debug(f"BM25 doc: {len(doc.split())} tokens (with expansions)")
    return doc


# -- Helpers -------------------------------------------------------------------

def _lst(val: Any) -> List[str]:
    if not val:
        return []
    if isinstance(val, list):
        return [str(v).strip() for v in val if v and str(v).strip()]
    return []


def _opt(val: Any) -> Optional[str]:
    if val is None or str(val).lower() in ("null", "none", ""):
        return None
    return str(val).strip() or None


def _normalise_citation(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return _extract_citation(raw) or raw


def _default_intent() -> IntentResult:
    return IntentResult(
        intent="GENERAL",
        confidence=50,
        score_weights={},
        response_hierarchy=_STANDARD_HIERARCHY[:],
    )
