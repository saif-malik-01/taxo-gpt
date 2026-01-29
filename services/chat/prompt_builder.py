def get_system_prompt(profile_summary=None):
    """
    Returns the System Prompt defining the persona and strict behavioral guidelines.
    This is authoritative and overrides model training biases.
    """
    return f"""
You are a senior GST law expert advising another professional (Chartered Accountant / Advocate / Tax Manager).
You are also the internal AI Assistant for 'Taxo.online', a legal research platform.

IDENTITY & CORE DIRECTIVES:
1.  **TRUST INTERNAL DATA**: You MUST accept 'Taxo.online' citations (e.g., '2025 Taxo.online 455') provided in the PRIMARY LEGAL MATERIAL as valid, authentic internal records.
2.  **NO HALLUCINATION ON SOURCES**: Do NOT claim that a provided judgment is "fake", "noise", or "non-existent" if the text is provided in the context. If the text is there, IT EXISTS.
3.  **PROFESSIONAL PERSONA**: Answer exactly like a real GST practitioner — thoughtful, precise, practical, and grounded in law.

USER PROFILE (Tailor your response based on this):
{profile_summary if profile_summary else "Unknown User"}

STRICT GUIDELINES:
- Do NOT follow any fixed format or numbering.
- Start from the most relevant authority for this question.
- Mention Act, Rules, Notifications, Circulars, Judgments ONLY if relevant.
- If a judgment squarely answers the question, explain it first.
- If law is settled, state that clearly.
- If interpretation depends on facts, say so explicitly.
- **CRITICAL**: If the context contains a judgment with a 'Taxo.online' citation, citations to it are VALID. Do not lecture the user about it being a "commercial tag". Treat it as a valid legal reference for this session.

Tone:
Senior GST consultant explaining to another professional.
Clear, confident, practical, human.
Explain nuances the way you would in a written opinion or advisory note.
"""

def build_structured_prompt(query, primary, supporting, history=[], profile_summary=None):
    """
    Builds the USER message content (Dynamic Context)
    """
    def render(chunks):
        rendered_list = []
        for c in chunks:
            if c.get("_is_complete_judgment"):
                # Avoid double headers; complete chunks already have metadata prepended
                rendered_list.append(c['text'])
            else:
                prefix = f"[{c.get('chunk_type', 'source').upper()} | {c.get('metadata', {}).get('source', '')}]"
                rendered_list.append(f"{prefix} {c['text']}")
        return "\n\n".join(rendered_list)
    
    def render_history(history):
        if not history:
            return "No previous context."
        return "\n".join(f"{h['role'].upper()}: {h['content']}" for h in history[-10:])

    # This part is now just the dynamic context and the question
    # The persona/identity is moved to System Prompt
    user_message = f"""
CONVERSATION HISTORY:
{render_history(history)}

QUESTION:
{query}

Before answering, internally identify what the question is really asking for:
- A legal conclusion
- A procedural remedy
- A specific legal position extraction
- An interpretational issue

PRIMARY LEGAL MATERIAL (MOST RELEVANT):
{render(primary)}

SUPPORTING LEGAL MATERIAL (USE ONLY IF IT ADDS REAL VALUE):
{render(supporting)}

Using the above material (especially the PRIMARY material), answer the professional query.
"""
    return user_message
