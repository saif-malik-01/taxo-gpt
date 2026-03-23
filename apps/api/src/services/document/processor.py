import os
import json
import base64
import boto3
import re
import tempfile
import shutil
import threading
from botocore.config import Config
from io import BytesIO
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from PIL import Image, ImageOps
from dotenv import load_dotenv

load_dotenv()

# ── Boto3 config for vision calls (AmazonNovaClient) ──────────────────────────
# Nova Lite processes full-page images and returns verbatim page text.
# With max_new_tokens=8192 a dense page can take 30-90s.
# read_timeout=150s gives enough headroom.
# max_attempts=1: disables botocore internal retries — we handle errors via
# the ThreadPoolExecutor result (failed pages return an error string, not crash).
_NOVA_CONFIG = Config(
    read_timeout=150,
    retries={"max_attempts": 1, "mode": "standard"},
)

# ── Boto3 config for document analysis (DocumentAnalyzer / Qwen) ──────────────
# _run_analysis requests up to 8192 output tokens for a large notice.
# Qwen typically takes 30-120s for a full 8192-token response.
# read_timeout=180s covers the slowest responses.
# max_attempts=1: disables botocore retries — errors surface immediately to
# _run_analysis's try/except which raises cleanly to analyze().
# Previously: botocore default was 3 retries x 60s timeout = 180s per attempt,
# causing the 5-minute hang seen in production logs.
_ANALYZER_CONFIG = Config(
    read_timeout=180,
    retries={"max_attempts": 1, "mode": "standard"},
)


# ============================================================================
# VISION CLIENT
# ============================================================================

class AmazonNovaClient:
    def __init__(self, model_id: str = "amazon.nova-lite-v1:0", region: str = None):
        self.model_id = model_id
        if region is None:
            region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.client = boto3.client(
            'bedrock-runtime',
            region_name=region,
            config=_NOVA_CONFIG,
        )

    def describe_image(self, pil_image: Image.Image, prompt: str = None) -> str:
        max_size = 2048
        if max(pil_image.size) > max_size:
            pil_image = pil_image.copy()
            pil_image.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)

        buffered = BytesIO()
        pil_image.save(buffered, format="PNG")
        img_base64 = base64.b64encode(buffered.getvalue()).decode("utf-8")

        if prompt is None:
            prompt = """Extract all information from this image/diagram/flowchart.
Include: all visible text, visual elements, flow, meaning, labels, annotations."""

        request_body = {
            "messages": [{
                "role": "user",
                "content": [
                    {"image": {"format": "png", "source": {"bytes": img_base64}}},
                    {"text": prompt}
                ]
            }],
            "inferenceConfig": {"max_new_tokens": 8192, "temperature": 0.1, "top_p": 0.9}
        }

        try:
            response = self.client.invoke_model(
                modelId=self.model_id,
                body=json.dumps(request_body),
                contentType="application/json",
                accept="application/json"
            )
            response_body = json.loads(response['body'].read())
            if 'output' in response_body and 'message' in response_body['output']:
                content = response_body['output']['message'].get('content', [])
                if content and len(content) > 0:
                    return content[0].get('text', '')
            return str(response_body.get('completion', response_body))
        except Exception as e:
            return f"[Vision Error: {str(e)}]"


# ============================================================================
# PAGE EXTRACTION PROMPT
# ============================================================================

PAGE_EXTRACTION_PROMPT = """You are a precise document extraction engine. Your ONLY job is to reproduce every piece of content from this page exactly as it appears — in the same order, with the same wording, structure, and layout intent.

GOLDEN RULES:
- NEVER paraphrase, summarize, infer, or add anything not visible on the page.
- Extract text VERBATIM — character for character, including capitalisation, punctuation, abbreviations, and spacing.
- Preserve the reading order: top-to-bottom, left-to-right (or column-by-column for multi-column layouts).
- Read the ENTIRE page: from the absolute top pixel (headers, document numbers, dates) to the absolute bottom pixel (footers, page numbers, signatures, stamps).

---

SECTION-BY-SECTION RULES:

1. PLAIN TEXT (body, paragraphs, headings, labels)
   - Copy every word exactly as written.
   - Separate paragraphs with a single blank line.
   - Use markdown heading levels matching the visual hierarchy.
   - Do NOT rephrase, simplify, or omit any sentence.

2. TABLES
   [TABLE: <exact table title if present, else "Table">]
   | Column Header 1 | Column Header 2 | ... |
   |-----------------|-----------------|-----|
   | row1_val1       | row1_val2       | ... |
   Notes: <any footnote text>
   [/TABLE]

3. FLOWCHARTS, PROCESS DIAGRAMS
   [FLOWCHART: <title>]
   START → [Step] → <Decision?> YES → [Step] NO → [Step] → END
   [/FLOWCHART]

4. FORMS AND FIELDS
   Label: Value (or [blank] if empty)

5. LISTS
   Preserve all bullet points and numbered lists exactly.

6. STAMPS, SEALS, WATERMARKS, SIGNATURES
   [STAMP: <text>] [SEAL: <text>] [SIGNATURE: <n>] [WATERMARK: <text>]

7. BLANK PAGE
   [BLANK PAGE]

OUTPUT RULES:
- Start directly with extracted content. No preamble.
- Maintain exact top-to-bottom order.
"""


# ============================================================================
# DOCUMENT TO IMAGES CONVERTER
# ============================================================================

def _add_padding(img: Image.Image, padding_px: int = 30) -> Image.Image:
    return ImageOps.expand(img, border=padding_px, fill=(255, 255, 255))


def _convert_to_images(file_path: str, dpi: int = 300) -> List[Image.Image]:
    ext = os.path.splitext(file_path)[1].lower()

    if ext in ('.png', '.jpg', '.jpeg', '.tiff', '.tif', '.bmp', '.gif', '.webp'):
        img = Image.open(file_path).convert("RGB")
        return [_add_padding(img)]

    if ext == '.pdf':
        try:
            import fitz
            doc    = fitz.open(file_path)
            images = []
            mat    = fitz.Matrix(dpi / 72, dpi / 72)
            for page in doc:
                pix = page.get_pixmap(matrix=mat, alpha=False, clip=None)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                images.append(_add_padding(img))
            doc.close()
            return images
        except ImportError:
            raise ImportError("pymupdf required. Install: pip install pymupdf")

    if ext in ('.docx', '.doc', '.pptx', '.ppt', '.xlsx', '.xls', '.odt', '.odp', '.ods'):
        try:
            import subprocess, fitz
            tmp_dir = tempfile.mkdtemp()
            lo_profile_dir = tempfile.mkdtemp(prefix="lo_profile_")
            try:
                result = subprocess.run(
                    [
                        'libreoffice',
                        f'-env:UserInstallation=file://{lo_profile_dir}',
                        '--headless',
                        '--convert-to', 'pdf',
                        '--outdir', tmp_dir,
                        file_path,
                    ],
                    capture_output=True, text=True, timeout=120
                )
                if result.returncode != 0:
                    raise RuntimeError(f"LibreOffice failed: {result.stderr}")
                base_name = os.path.splitext(os.path.basename(file_path))[0]
                pdf_path  = os.path.join(tmp_dir, base_name + '.pdf')
                if not os.path.exists(pdf_path):
                    pdfs = [f for f in os.listdir(tmp_dir) if f.endswith('.pdf')]
                    if not pdfs:
                        raise FileNotFoundError("LibreOffice produced no PDF")
                    pdf_path = os.path.join(tmp_dir, pdfs[0])
                doc    = fitz.open(pdf_path)
                images = []
                mat    = fitz.Matrix(dpi / 72, dpi / 72)
                for page in doc:
                    pix = page.get_pixmap(matrix=mat, alpha=False, clip=None)
                    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                    images.append(_add_padding(img))
                doc.close()
                return images
            finally:
                shutil.rmtree(tmp_dir,        ignore_errors=True)
                shutil.rmtree(lo_profile_dir, ignore_errors=True)
        except ImportError:
            raise ImportError("pymupdf required. Install: pip install pymupdf")

    if ext in ('.html', '.htm'):
        try:
            import weasyprint, fitz
            tmp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
            tmp_pdf.close()
            doc = None
            try:
                weasyprint.HTML(filename=file_path).write_pdf(tmp_pdf.name)
                doc    = fitz.open(tmp_pdf.name)
                images = []
                mat    = fitz.Matrix(dpi / 72, dpi / 72)
                for page in doc:
                    pix = page.get_pixmap(matrix=mat, alpha=False, clip=None)
                    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                    images.append(_add_padding(img))
                return images
            finally:
                if doc is not None:
                    doc.close()
                if os.path.exists(tmp_pdf.name):
                    os.unlink(tmp_pdf.name)
        except ImportError:
            raise ImportError("weasyprint required. Install: pip install weasyprint")

    raise ValueError(f"Unsupported file format: {ext}")


# ============================================================================
# DOCUMENT PROCESSOR  (module-level singleton, thread-safe)
# ============================================================================

class DocumentProcessor:
    """Vision-based document processor with parallel page processing."""

    def __init__(
        self,
        bedrock_model: str = "amazon.nova-lite-v1:0",
        bedrock_region: str = None,
        dpi: int = 300,
        max_workers: int = 10
    ):
        if bedrock_region is None:
            bedrock_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.vision_client = AmazonNovaClient(bedrock_model, bedrock_region)
        self.dpi        = dpi
        self.max_workers = max_workers

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        text  = text.replace('\t', ' ')
        text  = re.sub(r' {3,}', ' ', text)
        lines = [line.rstrip() for line in text.split('\n')]
        result     = []
        prev_blank = False
        for line in lines:
            if line.strip():
                result.append(line)
                prev_blank = False
            else:
                if not prev_blank:
                    result.append('')
                prev_blank = True
        while result and not result[0].strip():
            result.pop(0)
        while result and not result[-1].strip():
            result.pop()
        return '\n'.join(result)

    def _extract_page_text(self, page_index: int, pil_image: Image.Image) -> tuple:
        try:
            text    = self.vision_client.describe_image(pil_image, prompt=PAGE_EXTRACTION_PROMPT)
            cleaned = self._clean_text(text)
            if cleaned.strip() == "[BLANK PAGE]" or not cleaned.strip():
                return page_index, ""
            return page_index, cleaned
        except Exception as e:
            return page_index, f"[Page {page_index + 1} extraction error: {str(e)}]"

    def extract_text(self, file_path: str) -> str:
        page_images  = _convert_to_images(file_path, dpi=self.dpi)
        if not page_images:
            return ""
        total_pages  = len(page_images)
        page_results = {}
        with ThreadPoolExecutor(max_workers=min(self.max_workers, total_pages)) as executor:
            futures = {
                executor.submit(self._extract_page_text, idx, img): idx
                for idx, img in enumerate(page_images)
            }
            for future in as_completed(futures):
                page_idx, page_text = future.result()
                page_results[page_idx] = page_text

        content_blocks = []
        for i in range(total_pages):
            page_text = page_results.get(i, "")
            if page_text.strip():
                if total_pages > 1:
                    content_blocks.append(f"[PAGE {i + 1}]\n{page_text}")
                else:
                    content_blocks.append(page_text)

        return self._clean_text('\n\n'.join(content_blocks))

    def extract_text_from_multiple_files(self, file_paths: List[str], filenames: List[str]) -> str:
        combined_text_parts = []
        for idx, (file_path, filename) in enumerate(zip(file_paths, filenames), 1):
            separator = f"\n\n{'=' * 80}\nDOCUMENT {idx}: {filename}\n{'=' * 80}\n\n"
            try:
                extracted_text = self.extract_text(file_path)
                combined_text_parts.append(
                    separator + (extracted_text if extracted_text.strip() else f"[No text from {filename}]")
                )
            except Exception as e:
                combined_text_parts.append(separator + f"[Error: {filename}: {str(e)}]")
        return '\n\n'.join(combined_text_parts)


_processor_instance: Optional[DocumentProcessor] = None
_processor_lock = threading.Lock()


def get_document_processor() -> DocumentProcessor:
    """Return the module-level singleton DocumentProcessor."""
    global _processor_instance
    if _processor_instance is None:
        with _processor_lock:
            if _processor_instance is None:
                _processor_instance = DocumentProcessor()
    return _processor_instance


# ============================================================================
# DOCUMENT ANALYZER — Enhanced issue extraction
# ============================================================================

_TWO_PASS_THRESHOLD = 80_000
_SPLIT_OVERLAP      = 2_000

ISSUE_EXTRACTION_INSTRUCTION = """
4. ISSUES EXTRACTION — VERBATIM WITH ALL ENTITIES:
Extract every issue, allegation, discrepancy, observation, ground, or charge mentioned in the document.

CRITICAL RULES:
- Extract each issue EXACTLY as it appears — verbatim, word for word.
- Each issue MUST be self-contained — it must include ALL specific details mentioned in the same context:
    * Exact amounts (e.g., "Rs. 5,00,000", "tax of Rs. 12,456")
    * Tax periods (e.g., "April 2022 to March 2023", "FY 2021-22")
    * Section/Rule/Notification numbers (e.g., "Section 74(1) of CGST Act")
    * GSTIN, PAN, invoice numbers, or any other identifier
    * Percentages, rates, quantities
    * Any other quantitative or identifying detail that is part of the allegation
- Do NOT paraphrase, summarise, or condense.
- Do NOT filter — extract ALL issues including minor observations.
- If an issue spans multiple sentences, include all of them.
- Someone reading only that issue must have all the facts needed to respond to it.
- If there are NO issues at all → set issues to null.
"""


class DocumentAnalyzer:
    """Analyzes legal documents using Qwen3-Next-80B-A3B."""

    def __init__(self, model_id: str = "qwen.qwen3-next-80b-a3b", region: str = None):
        self.model_id = model_id
        if region is None:
            region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.client = boto3.client(
            'bedrock-runtime',
            region_name=region,
            config=_ANALYZER_CONFIG,
        )

    def _run_analysis(self, extracted_text: str, user_question: Optional[str] = None) -> dict:
        prompt = f"""You are analyzing a legal document. Extract information ONLY from the document text provided.

DOCUMENT CONTENT:
{extracted_text}

INSTRUCTIONS:

1. PARTY IDENTIFICATION
Extract sender and recipient ONLY if explicitly mentioned.
Sender: who issued/sent this document (From, Issued by, signature block with name).
Recipient: to whom addressed (To, Addressed to, populated GSTIN/Legal Name field).
Rules: null if not explicitly present. DO NOT infer.

2. COMPREHENSIVE SUMMARY:
Create a detailed summary capturing ALL key information:
purpose, background, key details, actions required, deadlines, amounts, references,
periods, GSTINs, transaction types. Write 4-7 sentences covering entire document.

3. USER QUESTION RESPONSE (if user asked a question):
If answer exists in document → provide it.
If not → respond: "It would we better to answer your query using my knowledge?"
{ISSUE_EXTRACTION_INSTRUCTION}
{"USER QUESTION: " + repr(user_question) if user_question else ""}

OUTPUT FORMAT (JSON):
{{
    "sender": "exact name" or null,
    "recipient": "exact name/GSTIN" or null,
    "summary": "comprehensive 4-7 sentence summary",
    "user_question_response": "answer" or "Should I resolve your query using my knowledge?" or null,
    "issues": ["verbatim issue with all entities 1", "verbatim issue 2", ...] or null,
    "issues_prompt": "Should I prepare the reply or guide for these issues?" or null
}}"""

        request_body = {
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "inferenceConfig": {"maxTokens": 8192, "temperature": 0.1, "topP": 0.9}
        }

        try:
            response   = self.client.converse(
                modelId=self.model_id,
                messages=request_body["messages"],
                inferenceConfig=request_body["inferenceConfig"]
            )
            content    = response['output']['message']['content'][0]['text']
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            return {"summary": content}
        except Exception as e:
            raise Exception(f"LLM Analysis failed: {str(e)}")

    def _run_reextraction(self, extracted_text: str, existing_issues: list) -> list:
        existing_preview = "\n".join(
            f"{i}. {iss['text'][:120]}" for i, iss in enumerate(existing_issues, 1)
        )

        prompt = f"""You are re-analyzing a legal document to find any issues that were missed in a previous extraction.

DOCUMENT CONTENT:
{extracted_text}

ALREADY EXTRACTED ISSUES (do NOT repeat these):
{existing_preview or "None extracted yet."}

YOUR TASK:
Read the ENTIRE document carefully.
Find any issues, allegations, charges, observations, or discrepancies that are present in the document
but are NOT already in the list above.

{ISSUE_EXTRACTION_INSTRUCTION}

ADDITIONAL RULE:
- Only return issues that are genuinely different from the already-extracted ones above.
- If nothing new is found → return empty list.

OUTPUT FORMAT (JSON only):
{{
    "new_issues": ["verbatim new issue with all entities", ...] or []
}}"""

        request_body = {
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "inferenceConfig": {"maxTokens": 4096, "temperature": 0.1, "topP": 0.9}
        }

        try:
            response   = self.client.converse(
                modelId=self.model_id,
                messages=request_body["messages"],
                inferenceConfig=request_body["inferenceConfig"]
            )
            content    = response['output']['message']['content'][0]['text']
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group())
                return parsed.get("new_issues") or []
            return []
        except Exception as e:
            raise Exception(f"Re-extraction failed: {str(e)}")

    def _deduplicate_issues(self, issues: list) -> list:
        if not issues:
            return issues
        seen   = []
        unique = []
        for issue in issues:
            normalised = re.sub(r'\s+', ' ', issue.lower().strip())
            is_dup = False
            for s in seen:
                shorter = min(len(normalised), len(s))
                if shorter == 0:
                    continue
                if normalised in s or s in normalised:
                    is_dup = True
                    break
                common = sum(1 for a, b in zip(normalised, s) if a == b)
                if common / shorter > 0.85:
                    is_dup = True
                    break
            if not is_dup:
                seen.append(normalised)
                unique.append(issue)
        return unique

    def analyze(self, extracted_text: str, user_question: Optional[str] = None) -> dict:
        """Analyze document. Two-pass for long documents."""
        if len(extracted_text) <= _TWO_PASS_THRESHOLD:
            analysis = self._run_analysis(extracted_text, user_question)
            return self._build_structured_response(analysis)

        mid = len(extracted_text) // 2
        nl  = extracted_text.rfind('\n', mid - 500, mid + 500)
        if nl != -1:
            mid = nl

        first_half  = extracted_text[:mid + _SPLIT_OVERLAP]
        second_half = extracted_text[mid - _SPLIT_OVERLAP:]

        analysis_first  = self._run_analysis(first_half,  user_question)
        analysis_second = self._run_analysis(second_half, user_question)

        issues_first  = analysis_first.get("issues")  or []
        issues_second = analysis_second.get("issues") or []
        all_issues    = issues_first + issues_second

        merged = dict(analysis_first)
        if all_issues:
            merged["issues"]        = self._deduplicate_issues(all_issues)
            merged["issues_prompt"] = "Should I prepare the reply or guide for these issues?"
        else:
            merged["issues"]        = None
            merged["issues_prompt"] = None

        return self._build_structured_response(merged)

    def reextract_missed_issues(self, full_text: str, existing_issues: list) -> list:
        if len(full_text) <= _TWO_PASS_THRESHOLD:
            return self._run_reextraction(full_text, existing_issues)

        mid = len(full_text) // 2
        nl  = full_text.rfind('\n', mid - 500, mid + 500)
        if nl != -1:
            mid = nl

        first_half  = full_text[:mid + _SPLIT_OVERLAP]
        second_half = full_text[mid - _SPLIT_OVERLAP:]

        new_first  = self._run_reextraction(first_half,  existing_issues)
        new_second = self._run_reextraction(second_half, existing_issues)
        return self._deduplicate_issues(new_first + new_second)

    def _build_structured_response(self, analysis: dict) -> dict:
        structured = {}
        if analysis.get("sender"):
            structured["sender"] = analysis["sender"]
        if analysis.get("recipient"):
            structured["recipient"] = analysis["recipient"]
        structured["summary"] = analysis.get("summary", "")
        if analysis.get("user_question_response"):
            structured["user_question_response"] = analysis["user_question_response"]
        if analysis.get("issues"):
            structured["issues"]        = analysis["issues"]
            structured["issues_prompt"] = analysis.get("issues_prompt", "")
        return structured

    def format_response_for_frontend(self, analysis: dict) -> str:
        parts = []
        if analysis.get("sender"):
            parts.append(f"From: {analysis['sender']}")
        if analysis.get("recipient"):
            parts.append(f"To: {analysis['recipient']}")
        if analysis.get("sender") or analysis.get("recipient"):
            parts.append("")
        parts.append(analysis.get("summary", ""))
        if analysis.get("user_question_response"):
            parts.append("")
            parts.append(f"User Query reply: {analysis['user_question_response']}")
        if analysis.get("issues"):
            parts.append("")
            parts.append("Issues:")
            for i, issue in enumerate(analysis["issues"], 1):
                parts.append(f"{i}. {issue}")
            if analysis.get("issues_prompt"):
                parts.append("")
                parts.append(analysis["issues_prompt"])
        return "\n".join(parts).strip()