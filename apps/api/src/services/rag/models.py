"""
apps/api/src/services/rag/models.py
All dataclasses used across the retrieval pipeline.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


@dataclass
class SessionMessage:
    user_query: str
    llm_response: str


@dataclass
class Stage2AResult:
    normalised_tokens: List[str]   # section_14a, rule_89 etc  -  weight 1
    raw_tokens: List[str]          # whitespace-split remaining words
    citation: Optional[str] = None  # taxo.online / MANU citation if found


@dataclass
class Stage2BResult:
    sections: List[str]            = field(default_factory=list)
    rules: List[str]               = field(default_factory=list)
    notifications: List[str]       = field(default_factory=list)
    circulars: List[str]           = field(default_factory=list)
    acts: List[str]                = field(default_factory=list)
    keywords: List[str]            = field(default_factory=list)
    topics: List[str]              = field(default_factory=list)
    form_name: Optional[str]       = None
    form_number: Optional[str]     = None
    case_name: Optional[str]       = None
    parties: List[str]             = field(default_factory=list)
    person_names: List[str]        = field(default_factory=list)
    case_number: Optional[str]     = None
    court: Optional[str]           = None
    court_level: Optional[str]     = None
    citation: Optional[str]        = None
    decision_type: Optional[str]   = None
    hsn_code: Optional[str]        = None
    sac_code: Optional[str]        = None
    issued_by: Optional[str]       = None


@dataclass
class IntentResult:
    intent: str                          # JUDGMENT / RATE / FORM / GENERAL
    confidence: int                      # 0-100
    score_weights: Dict[str, float]      # chunk_type  ->  additive score boost
    response_hierarchy: List[str]        # ordered labels for LLM response


@dataclass
class ScoredChunk:
    chunk_id: str
    payload: Dict[str, Any]
    score: float
    source_sets: Set[str] = field(default_factory=set)
    pinned: bool = False   # name/case/citation search result  -  guaranteed slot


@dataclass
class CitationResult:
    found: bool = False
    chunks: List[Dict[str, Any]] = field(default_factory=list)
    citation: str = ""
    only_this_asked: bool = False


@dataclass
class FinalResponse:
    answer: str
    retrieved_documents: List[Dict[str, Any]]
    intent: str
    confidence: int
