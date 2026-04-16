"""
services/document/processor.py

Page extraction via AWS Bedrock Nova Lite.

Design:
  - Convert each uploaded file to per-page PNG images (pdf2image / python-docx / Pillow)
  - Send each page image to Nova Lite concurrently, throttled by the global semaphore
  - ThreadPoolExecutor wraps boto3 calls (boto3 is synchronous)
  - DPI=150: same output quality as 300 DPI for Nova Lite (thumbnails to 2048px anyway)
    saves ~25% memory and 20-30% extraction time vs DPI=300

Semaphore sizing:
  MAX_CONCURRENT_PAGES = floor(Bedrock_RPM / (60 / avg_page_latency_s))
  At 100 RPM, 15s avg: MAX = 25
  Set via env MAX_CONCURRENT_PAGES (default 25)

Thread pool:
  Sized to MAX_CONCURRENT_PAGES + 5 (slight headroom for retry bursts)
"""

import asyncio
import base64
import concurrent.futures
import io
import json
import logging
import os
import threading
import time
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

_DPI = int(os.getenv("NOVA_LITE_DPI", "150"))
_MAX_PAGES_PER_DOC = 200

_PAGE_EXTRACTION_PROMPT = (
    "Extract ALL text from this document page exactly as it appears. "
    "Rules:\n"
    "1. Preserve the original reading order (top to bottom, left to right).\n"
    "2. Reproduce tables as structured text blocks with | separators.\n"
    "3. Mark stamps as [STAMP: <text>], signatures as [SIGNATURE], blank pages as [BLANK PAGE].\n"
    "4. Do NOT interpret, summarise, or add any text that is not on the page.\n"
    "5. Include ALL numbers, dates, section references, GSTINs, amounts exactly as printed.\n"
    "Output the extracted text only — no preamble, no commentary."
)

_MAX_RETRIES = 5
_RETRY_BASE  = 2.0   # seconds — exponential backoff


# ─────────────────────────────────────────────────────────────────────────────
# boto3 client singleton
# ─────────────────────────────────────────────────────────────────────────────

_boto_lock   = threading.Lock()
_nova_client = None
_executor:   Optional[concurrent.futures.ThreadPoolExecutor] = None


def _get_nova_client():
    global _nova_client
    if _nova_client is None:
        with _boto_lock:
            if _nova_client is None:
                import boto3
                from botocore.config import Config
                from apps.api.src.services.document.global_semaphore import MAX_CONCURRENT_PAGES
                cfg = Config(
                    max_pool_connections=MAX_CONCURRENT_PAGES + 10,
                    retries={"max_attempts": 0, "mode": "standard"},  # we handle retries manually
                )
                from apps.api.src.core.config import settings
                _nova_client = boto3.client(
                    "bedrock-runtime",
                    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                    region_name=settings.AWS_REGION or os.getenv("AWS_REGION", "us-east-1"),
                    config=cfg,
                )
                logger.info(
                    f"Nova Lite boto3 client initialised — "
                    f"pool={MAX_CONCURRENT_PAGES + 10}, DPI={_DPI}"
                )
    return _nova_client


def _get_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _executor
    if _executor is None:
        with _boto_lock:
            if _executor is None:
                from apps.api.src.services.document.global_semaphore import MAX_CONCURRENT_PAGES
                workers = MAX_CONCURRENT_PAGES + 5
                _executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=workers, thread_name_prefix="nova_page"
                )
                logger.info(f"ThreadPoolExecutor: {workers} workers for Nova Lite")
    return _executor


# ─────────────────────────────────────────────────────────────────────────────
# Single-page extraction (runs in ThreadPoolExecutor thread)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_page_sync(page_image_bytes: bytes) -> str:
    """
    Send one page image to Nova Lite. Returns extracted text string.
    Retries up to _MAX_RETRIES times on throttling (429) or transient errors.
    """
    client = _get_nova_client()
    image_b64 = base64.b64encode(page_image_bytes).decode("utf-8")

    body = {
        "messages": [{
            "role": "user",
            "content": [
                {
                    "image": {
                        "format": "png",
                        "source": {"bytes": image_b64},
                    }
                },
                {"text": _PAGE_EXTRACTION_PROMPT},
            ],
        }],
        "inferenceConfig": {
            "maxTokens": 4096,
            "temperature": 0.0,
        },
    }

    for attempt in range(_MAX_RETRIES):
        try:
            resp = client.invoke_model(
                modelId="amazon.nova-lite-v1:0",
                body=json.dumps(body),
                contentType="application/json",
                accept="application/json",
            )
            result = json.loads(resp["body"].read())
            content = result.get("output", {}).get("message", {}).get("content", [])
            for block in content:
                if isinstance(block, dict) and block.get("text"):
                    return block["text"].strip()
            return ""
        except Exception as exc:
            err_str = str(exc)
            is_throttle = any(k in err_str.lower() for k in ("throttling", "rate", "429", "toomanyrequests"))
            if attempt < _MAX_RETRIES - 1:
                wait = _RETRY_BASE ** attempt + (0.1 * os.urandom(1)[0] / 255)
                if is_throttle:
                    logger.warning(f"Nova Lite throttled (attempt {attempt+1}), retry in {wait:.1f}s")
                else:
                    logger.warning(f"Nova Lite error (attempt {attempt+1}): {exc}, retry in {wait:.1f}s")
                time.sleep(wait)
            else:
                logger.error(f"Nova Lite failed after {_MAX_RETRIES} attempts: {exc}")
                raise


# ─────────────────────────────────────────────────────────────────────────────
# File → page images (synchronous, runs in threadpool from caller)
# ─────────────────────────────────────────────────────────────────────────────

def file_to_page_images(file_path: str) -> List[bytes]:
    """
    Convert an uploaded file to a list of PNG images (one per page).
    Returns list of PNG bytes. Raises on unsupported formats.
    """
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".pdf":
        return _pdf_to_images(file_path)
    elif ext in (".png", ".jpg", ".jpeg", ".tiff", ".bmp"):
        return _image_file_to_images(file_path)
    elif ext == ".docx":
        return _docx_to_images(file_path)
    elif ext in (".pptx", ".xlsx", ".html"):
        return _office_to_images(file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def _pdf_to_images(file_path: str) -> List[bytes]:
    try:
        from pdf2image import convert_from_path
        pages = convert_from_path(file_path, dpi=_DPI, fmt="png")
        result = []
        for page in pages[:_MAX_PAGES_PER_DOC]:
            buf = io.BytesIO()
            page.save(buf, format="PNG")
            result.append(buf.getvalue())
        return result
    except ImportError:
        # Fallback: try pypdf text extraction directly (no images)
        return _pdf_text_fallback(file_path)


def _pdf_text_fallback(file_path: str) -> List[bytes]:
    """Last-resort: convert PDF pages to plain-text rendered as PNG via Pillow."""
    from pypdf import PdfReader
    reader = PdfReader(file_path)
    images = []
    for page in list(reader.pages)[:_MAX_PAGES_PER_DOC]:
        text = page.extract_text() or ""
        if text.strip():
            images.append(_text_to_png(text))
    return images


def _text_to_png(text: str) -> bytes:
    """Render text as a simple white PNG — used when pdf2image unavailable."""
    try:
        from PIL import Image, ImageDraw, ImageFont
        img = Image.new("RGB", (1200, 1600), color="white")
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 14)
        except Exception:
            font = ImageFont.load_default()
        y = 20
        for line in text[:3000].split("\n"):
            draw.text((20, y), line[:180], fill="black", font=font)
            y += 18
            if y > 1560:
                break
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except Exception:
        # Absolute fallback: return a 1x1 white PNG
        return b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82'


def _image_file_to_images(file_path: str) -> List[bytes]:
    from PIL import Image
    img = Image.open(file_path).convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return [buf.getvalue()]


def _docx_to_images(file_path: str) -> List[bytes]:
    """Convert DOCX to PDF then to images. Falls back to text extraction."""
    try:
        # LibreOffice conversion
        import subprocess, tempfile, os
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                ["libreoffice", "--headless", "--convert-to", "pdf",
                 "--outdir", tmp, file_path],
                capture_output=True, timeout=60,
            )
            pdf_files = [f for f in os.listdir(tmp) if f.endswith(".pdf")]
            if pdf_files:
                return _pdf_to_images(os.path.join(tmp, pdf_files[0]))
    except Exception as e:
        logger.warning(f"LibreOffice conversion failed: {e}")

    # Fallback: extract text from DOCX directly
    try:
        from docx import Document
        doc = Document(file_path)
        text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
        if text.strip():
            return [_text_to_png(text[:3000])]
    except Exception as e:
        logger.warning(f"DOCX text extraction failed: {e}")
    return []


def _office_to_images(file_path: str) -> List[bytes]:
    """Generic office file conversion via LibreOffice → PDF → images."""
    try:
        import subprocess, tempfile, os
        with tempfile.TemporaryDirectory() as tmp:
            subprocess.run(
                ["libreoffice", "--headless", "--convert-to", "pdf",
                 "--outdir", tmp, file_path],
                capture_output=True, timeout=60,
            )
            pdf_files = [f for f in os.listdir(tmp) if f.endswith(".pdf")]
            if pdf_files:
                return _pdf_to_images(os.path.join(tmp, pdf_files[0]))
    except Exception as e:
        logger.warning(f"Office conversion failed: {e}")
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Async extraction — entry point
# ─────────────────────────────────────────────────────────────────────────────

async def extract_document_pages(
    file_path: str,
    filename: str,
) -> Tuple[str, int, str]:
    """
    Full document extraction:
      1. Convert file to page images (threadpool — no event loop blocking)
      2. Extract each page via Nova Lite (concurrent, global semaphore)

    Returns: (full_text, page_count, error_message_or_empty)
    """
    from starlette.concurrency import run_in_threadpool
    from apps.api.src.services.document.global_semaphore import get_page_semaphore

    # Step 1: file → images (CPU-bound, in threadpool)
    try:
        page_images = await run_in_threadpool(file_to_page_images, file_path)
    except Exception as e:
        return "", 0, f"Could not open '{filename}': {e}"

    if not page_images:
        return "", 0, (
            f"'{filename}' appears to be empty or could not be converted. "
            "Please upload a text-searchable PDF or Word document."
        )

    page_count = len(page_images)
    if page_count > _MAX_PAGES_PER_DOC:
        return "", 0, (
            f"'{filename}' has {page_count} pages. Maximum is {_MAX_PAGES_PER_DOC} pages per document."
        )

    logger.info(f"Extracting '{filename}': {page_count} page(s) via Nova Lite at DPI={_DPI}")
    semaphore = get_page_semaphore()
    executor  = _get_executor()
    loop      = asyncio.get_event_loop()

    async def _extract_one(page_idx: int, page_bytes: bytes) -> Tuple[int, str]:
        async with semaphore:
            t0 = time.monotonic()
            text = await loop.run_in_executor(executor, _extract_page_sync, page_bytes)
            logger.debug(
                f"  '{filename}' p{page_idx+1}/{page_count} "
                f"→ {len(text)} chars ({time.monotonic()-t0:.1f}s)"
            )
            return page_idx, text

    tasks   = [_extract_one(i, img) for i, img in enumerate(page_images)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Reassemble in page order
    page_texts: List[Tuple[int, str]] = []
    for r in results:
        if isinstance(r, Exception):
            logger.warning(f"Page extraction error in '{filename}': {r}")
        else:
            idx, text = r
            if text and text.strip():
                page_texts.append((idx, text))

    page_texts.sort(key=lambda x: x[0])
    blocks = []
    for idx, text in page_texts:
        block = f"[PAGE {idx+1}]\n{text}" if page_count > 1 else text
        blocks.append(block)

    full_text = "\n\n".join(blocks)

    if len(full_text.strip()) < 50 and page_count >= 1:
        return "", 0, (
            f"'{filename}' appears to be scanned or image-only — very little text extracted. "
            "Please upload a text-searchable PDF."
        )

    logger.info(f"Extraction done: '{filename}' — {page_count}p, {len(full_text)} chars")
    return full_text, page_count, ""