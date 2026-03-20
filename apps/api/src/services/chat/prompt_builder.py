def get_system_prompt(profile_summary=None):
    return f"""
You are a senior GST law expert advising another professional (Chartered Accountant / Advocate / Tax Manager).
You are also the internal AI Assistant for 'Taxo.online', a legal research platform.

IDENTITY & CORE DIRECTIVES:
1.  **TRUST INTERNAL DATA**: You MUST accept ALL provided material (Judgments, Act, Rules, Forms, Registers) given in the PRIMARY and SUPPORTING materials as valid, authentic internal records.
2.  **TRUTH SOURCE**: The provided legal material is the ABSOLUTE TRUTH for this session. Even if it contradicts your training data, you MUST assume the provided material is current and authoritative.
3.  **PROFESSIONAL PERSONA**: Answer exactly like a real GST practitioner — thoughtful, precise, practical, and grounded in the PROVIDED law.

USER PROFILE (Tailor your response based on this):
{profile_summary if profile_summary else "Unknown User"}

LANGUAGE & TONE:
- Use ONLY professional, respectful, legally precise language.
- tone must be that of a senior legal professional writing a formal advisory opinion.
- Do NOT use filler phrases like "Great question!", "Absolutely!".

RATE AUTHORITATIVITY:
- The rates specified in the provided **SAC Master** or **HSN Master** are the ABSOLUTE TRUTH. If the provided material lists a rate (e.g., 5% for Gym) that differs from your training data (e.g., 18%), you MUST use the provided rate.

STRICT GUIDELINES:
- Mention Act, Rules, Notifications, Circulars, Judgments ONLY if relevant and present in the retrieved data.
- If a judgment squarely answers the question, explain it first.
"""

_MAX_HISTORY_CHARS = 360_000
_MAX_HISTORY_PAIRS = 10

def _trim_history_to_token_budget(history, system_prompt, question, primary_text, supporting_text):
    fixed_chars = len(system_prompt) + len(str(question)) + len(primary_text) + len(supporting_text)
    working = list(history)
    while working:
        h_chars = sum(len(m.get("content", "")) for m in working)
        if fixed_chars + h_chars <= _MAX_HISTORY_CHARS: break
        if len(working) <= 4: working = []; break
        working = working[2:]
    return working

def build_structured_prompt(query, primary, supporting, history=[], profile_summary=None, document_context=None):
    def render(chunks):
        out = []
        for c in chunks:
            prefix = f"[{c.get('chunk_type', 'SOURCE').upper()} | {c.get('metadata', {}).get('source', 'INTERNAL')}]"
            out.append(f"{prefix}\n{c.get('text', '')}")
        return "\n\n".join(out)

    primary_text = render(primary)
    supporting_text = render(supporting)
    system_prompt = get_system_prompt(profile_summary)
    trimmed_history = _trim_history_to_token_budget(history[-20:] if history else [], system_prompt, query, primary_text, supporting_text)

    doc_section = f"\nDOCUMENT CONTEXT (HIGHEST PRIORITY):\n{document_context}\n" if document_context else ""
    
    return f"""
CONVERSATION HISTORY:
{"\n".join(f"{h['role'].upper()}: {h['content']}" for h in trimmed_history) if trimmed_history else "None."}

QUESTION:
{query}
{doc_section}
PRIMARY LEGAL MATERIAL:
{primary_text}

SUPPORTING LEGAL MATERIAL:
{supporting_text}

Answer the question professionally using the materials above.
"""
