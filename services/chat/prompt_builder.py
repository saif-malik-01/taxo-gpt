def get_system_prompt(profile_summary=None):
    """
    Returns the System Prompt defining the persona and strict behavioral guidelines.
    This is authoritative and overrides model training biases.
    """
    return f"""
You are a senior GST law expert advising another professional (Chartered Accountant / Advocate / Tax Manager).
You are also the internal AI Assistant for 'Taxo.online', a legal research platform.

IDENTITY & CORE DIRECTIVES:
1.  **TRUST INTERNAL DATA**: You MUST accept ALL provided material (Judgments, Act, Rules, Forms, Registers) given in the PRIMARY and SUPPORTING materials as valid, authentic internal records. This includes GSTAT Forms, GSTAT Rules (2025), and CDR registers.
2.  **TRUTH SOURCE**: The provided legal material is the ABSOLUTE TRUTH for this session. Even if it contradicts your training data (e.g., citations to "Rules 29 and 49" which you think don't exist in the 2021 Rules), you MUST assume the provided material is current and authoritative. Do NOT claim a provided source is "fake", "noise", or "non-existent".
3.  **PROFESSIONAL PERSONA**: Answer exactly like a real GST practitioner — thoughtful, precise, practical, and grounded in the PROVIDED law.

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
- **TERMINOLOGY FLEXIBILITY**: Users often use terms like "Judgment", "Case Law", or "Ruling" loosely to refer to any Court Order, Interim Order, or Decision. **Do NOT pedantically correct them.** If a user asks for a "judgment" and the provided text is an "Interim Order", simply explain the Order.
    - BAD: "There is no judgment, this is an interim order."
    - GOOD: "In this Interim Order (cited as 2025 Taxo.online 455)..."

Tone:
Senior GST consultant explaining to another professional.
Clear, confident, practical, human.
Explain nuances the way you would in a written opinion or advisory note.

"""

def build_structured_prompt(query, primary, supporting, history=[], profile_summary=None, document_context=None):
    """
    Builds the USER message content (Dynamic Context)
    
    Supports optional document_context for analyzing uploaded documents.
    If document_context is provided, it's integrated as the highest priority context.
    """
    def render(chunks):
        rendered_list = []
        for c in chunks:
            if c.get("_is_complete_judgment"):
                # Avoid double headers; complete chunks already have metadata prepended
                rendered_list.append(c['text'])
                continue

            doc_type = c.get('doc_type', '')
            struct = c.get('structure', {})
            meta = c.get('metadata', {})
            source = meta.get('source') or meta.get('source_file') or c.get('parent_doc', 'source')
            
            if doc_type == "Case Scenario":
                section = struct.get('section_number', 'UNKNOWN')
                illus = struct.get('illustration_number', 'UNKNOWN')
                prefix = f"[CASE SCENARIO | {source} | Sec {section} | Illus {illus}]"
                
                problem = c.get('problem', 'No problem description provided.')
                solution = c.get('solution', 'No solution provided.')
                
                content = f"PROBLEM: {problem}\nSOLUTION: {solution}"
                rendered_list.append(f"{prefix}\n{content}")

            elif doc_type == "Analytical Review":
                section = struct.get('section_number', 'UNKNOWN')
                title = struct.get('section_title', 'Untitled')
                prefix = f"[ANALYTICAL REVIEW | {source} | Sec {section} | {title}]"
                rendered_list.append(f"{prefix}\n{c['text']}")

            elif doc_type == "FAQ":
                q_num = struct.get('question_number', 'UNKNOWN')
                prefix = f"[FAQ | {source} | Q {q_num}]"
                rendered_list.append(f"{prefix}\n{c['text']}")

            elif doc_type == "Draft Reply":
                sec_type = struct.get('section_type', 'Content')
                prefix = f"[DRAFT REPLY | {source} | {sec_type}]"
                rendered_list.append(f"{prefix}\n{c['text']}")

            elif doc_type in ["Case Study", "Case Study Table"]:
                sec_type = struct.get('section_type', 'Content')
                table_idx = struct.get('table_index')
                if table_idx:
                    prefix = f"[CASE STUDY TABLE {table_idx} | {source}]"
                else:
                    prefix = f"[CASE STUDY | {source} | {sec_type}]"
                rendered_list.append(f"{prefix}\n{c['text']}")

            elif "hsn_code" in c.get('metadata', {}):
                hsn = c['metadata']['hsn_code']
                prefix = f"[HSN CODE {hsn} | {source}]"
                rendered_list.append(f"{prefix} {c['text']}")
                
            elif "sac_code" in c.get('metadata', {}):
                sac = c['metadata']['sac_code']
                prefix = f"[SAC CODE {sac} | {source}]"
                rendered_list.append(f"{prefix} {c['text']}")

            elif doc_type == "Council Minutes" or c.get("chunk_type") == "council_decision":
                meeting = struct.get('meeting_number', 'UNKNOWN')
                prefix = f"[GST COUNCIL {meeting}th MEETING | {source}]"
                rendered_list.append(f"{prefix}\n{c['text']}")

            else:
                chunk_type = c.get('chunk_type', 'source').upper()
                prefix = f"[{chunk_type} | {source}]"
                rendered_list.append(f"{prefix} {c['text']}")
        return "\n\n".join(rendered_list)
    
    def render_history(history):
        if not history:
            return "No previous context."
        return "\n".join(f"{h['role'].upper()}: {h['content']}" for h in history[-10:])

    # Build document context section if provided
    document_section = ""
    if document_context:
        document_section = f"""
DOCUMENT CONTEXT (UPLOADED BY USER - HIGHEST PRIORITY):
{document_context}

"""

    # This part is now just the dynamic context and the question
    # The persona/identity is moved to System Prompt
    user_message = f"""
CONVERSATION HISTORY:
{render_history(history)}

QUESTION:
{query}

{document_section}Before answering, internally identify what the question is really asking for:
- A legal conclusion
- A procedural remedy
- A specific legal position extraction
- An interpretational issue
- Document analysis or specific questions about provided documents

PRIMARY LEGAL MATERIAL (MOST RELEVANT):
{render(primary)}

SUPPORTING LEGAL MATERIAL (USE ONLY IF IT ADDS REAL VALUE):
{render(supporting)}

Using the above material (especially the PRIMARY material{', and the document context if provided,' if document_context else ''}), answer the professional query.
"""
    return user_message
