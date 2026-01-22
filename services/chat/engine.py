from services.retrieval.hybrid import retrieve
from services.llm.bedrock_client import call_bedrock
from services.chat.prompt_builder import build_structured_prompt


def classify_query_intent(query: str) -> str:
    q = query.lower()

    if "judgment" in q or "case law" in q or "court" in q:
        return "judgment"

    if "define" in q or "what is section" in q or "meaning of" in q:
        return "definition"

    if "rcm" in q or "reverse charge" in q:
        return "rcm"

    if "rate" in q or "gst rate" in q:
        return "rate"

    if "procedure" in q or "how to" in q:
        return "procedure"

    if "difference" in q or "vs" in q:
        return "comparison"

    return "general"


def split_primary_and_supporting(chunks, intent):
    primary = []
    supporting = []

    for ch in chunks:
        ctype = ch.get("chunk_type")

        if intent == "judgment" and ctype == "judgment":
            primary.append(ch)

        elif intent == "definition" and ctype in ("definition", "operative", "act"):
            primary.append(ch)

        elif intent == "procedure" and ctype == "rule":
            primary.append(ch)

        else:
            supporting.append(ch)

    # Fallback: if nothing classified as primary, use top authoritative chunks
    if not primary:
        primary = chunks[:3]
        supporting = chunks[3:]

    return primary, supporting


def chat(query, store, all_chunks):
    retrieved = retrieve(
        query=query,
        vector_store=store,
        all_chunks=all_chunks,
        k=25
    )

    intent = classify_query_intent(query)

    primary, supporting = split_primary_and_supporting(retrieved, intent)

    prompt = build_structured_prompt(
        query=query,
        primary=primary,
        supporting=supporting
    )

    answer = call_bedrock(prompt)

    return answer, retrieved
