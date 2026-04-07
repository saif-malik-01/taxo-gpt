"""
services/document/session_doc_store.py

Handles write-once storage and retrieval of full extracted document text.
Text is stored in DB at upload time and read only when needed
(re-extraction, reply generation for reference docs).

Redis stores NONE of this text — only summary, issues, parties, mode, state.
"""

import logging
from typing import List, Optional
from sqlalchemy import Column, Integer, String, Text, DateTime, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func as sa_func

from apps.api.src.db.session import Base, AsyncSessionLocal

logger = logging.getLogger(__name__)


# ─── Model ────────────────────────────────────────────────────────────────────

class SessionDocumentText(Base):
    __tablename__ = "session_document_texts"

    id            = Column(Integer, primary_key=True, index=True)
    session_id    = Column(String, index=True, nullable=False)
    case_id       = Column(Integer, nullable=False)
    filename      = Column(String, nullable=False)
    doc_type      = Column(String, nullable=False)   # "primary" | "reference"
    extracted_text = Column(Text, nullable=False)
    created_at    = Column(DateTime(timezone=True), server_default=sa_func.now())


# ─── Write ────────────────────────────────────────────────────────────────────

async def save_document_text(
    session_id: str,
    case_id: int,
    filename: str,
    doc_type: str,
    extracted_text: str,
):
    """Save extracted text once at upload time."""
    async with AsyncSessionLocal() as db:
        record = SessionDocumentText(
            session_id=session_id,
            case_id=case_id,
            filename=filename,
            doc_type=doc_type,
            extracted_text=extracted_text,
        )
        db.add(record)
        await db.commit()
        logger.info(f"Saved {doc_type} doc text for session={session_id} case={case_id} file={filename}")


# ─── Read ─────────────────────────────────────────────────────────────────────

async def get_primary_texts(session_id: str, case_id: int) -> str:
    """
    Return consolidated text of all primary documents for this case.
    Used only for re-extraction (when user says issues are missed).
    """
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(SessionDocumentText)
            .where(
                SessionDocumentText.session_id == session_id,
                SessionDocumentText.case_id    == case_id,
                SessionDocumentText.doc_type   == "primary",
            )
            .order_by(SessionDocumentText.created_at)
        )
        rows = result.scalars().all()

    if not rows:
        return ""

    parts = []
    for row in rows:
        text = (row.extracted_text or "").strip()
        if text:
            if len(rows) > 1:
                parts.append(f"[DOCUMENT: {row.filename}]\n{text}")
            else:
                parts.append(text)

    sep = "\n\n" + "=" * 60 + "\n\n"
    return sep.join(parts)


async def get_reference_texts(session_id: str, case_id: int) -> str:
    """
    Return consolidated text of all reference documents for this case.
    Used when building issue reply prompts.
    """
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(SessionDocumentText)
            .where(
                SessionDocumentText.session_id == session_id,
                SessionDocumentText.case_id    == case_id,
                SessionDocumentText.doc_type   == "reference",
            )
            .order_by(SessionDocumentText.created_at)
        )
        rows = result.scalars().all()

    if not rows:
        return ""

    parts = []
    for row in rows:
        text = (row.extracted_text or "").strip()
        if text:
            parts.append(f"[REFERENCE: {row.filename}]\n{text}")

    return "\n\n".join(parts)


async def delete_session_documents(session_id: str):
    """Clean up all document texts when a session is deleted."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(SessionDocumentText).where(SessionDocumentText.session_id == session_id)
        )
        rows = result.scalars().all()
        for row in rows:
            await db.delete(row)
        await db.commit()
        logger.info(f"Deleted {len(rows)} document text records for session={session_id}")


async def get_text_by_filename(session_id: str, case_id: int, filename: str) -> str:
    """
    Return extracted text for one specific file in a session/case.
    Used by Step 6 issue extraction to get per-doc text when multiple
    primary docs exist in the same case.
    """
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(SessionDocumentText)
            .where(
                SessionDocumentText.session_id == session_id,
                SessionDocumentText.case_id    == case_id,
                SessionDocumentText.filename   == filename,
            )
        )
        row = result.scalars().first()
    return (row.extracted_text or "").strip() if row else ""


async def get_reply_reference_texts(session_id: str, case_id: int) -> str:
    """
    Return consolidated text of previous_reply and user_draft_reply documents.
    Used in issue draft prompt to maintain consistency with prior positions.
    """
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(SessionDocumentText)
            .where(
                SessionDocumentText.session_id == session_id,
                SessionDocumentText.case_id    == case_id,
                SessionDocumentText.doc_type   == "reply_reference",
            )
            .order_by(SessionDocumentText.created_at)
        )
        rows = result.scalars().all()

    if not rows:
        return ""

    parts = []
    for row in rows:
        text = (row.extracted_text or "").strip()
        if text:
            parts.append(f"[REPLY REFERENCE: {row.filename}]\n{text}")

    return "\n\n".join(parts)