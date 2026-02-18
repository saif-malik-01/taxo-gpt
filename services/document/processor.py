import os
import json
import base64
import boto3
import re
import tempfile
import shutil
from io import BytesIO
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from PIL import Image, ImageOps
from dotenv import load_dotenv

load_dotenv()


# ============================================================================
# VISION CLIENT
# ============================================================================


class AmazonNovaClient:
    """AWS Bedrock client for vision analysis"""

    def __init__(self, model_id: str = "amazon.nova-lite-v1:0", region: str = None):
        self.model_id = model_id
        if region is None:
            region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.client = boto3.client('bedrock-runtime', region_name=region)

    def describe_image(self, pil_image: Image.Image, prompt: str = None) -> str:
        """Describe image using Amazon Nova with optimized settings"""
        # Keep full resolution up to Nova's 2048px limit
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
            "inferenceConfig": {"max_new_tokens": 4096, "temperature": 0.1, "top_p": 0.9}
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

PAGE_EXTRACTION_PROMPT = """You are a precise document extraction engine. Extract EVERY piece of content from this document page with complete fidelity.

IMPORTANT: This page contains content at the very top edge (headers, document numbers, dates, reference codes) AND at the very bottom edge (footers, page numbers, signatures, stamps, disclaimers). You MUST read the ENTIRE page — from the absolute top pixel to the absolute bottom pixel — without skipping anything at the margins.

FOLLOW THESE EXTRACTION RULES EXACTLY:

1. HEADERS & TOP-OF-PAGE (read this first — do not miss anything at the top):
   Extract document title, reference/notice/case numbers, dates, letterhead name and address, any text printed near the top margin or header area.

2. BODY TEXT:
   Extract every word in natural reading order (top-to-bottom, left-to-right).
   Separate paragraphs with a blank line.
   Use markdown heading levels for headings: # for main, ## for sub, ### for minor.

3. TABLES — use this descriptive format (do NOT output raw grid cells or JSON):
   Wrap the entire table description in [TABLE] ... [/TABLE] tags.
   Inside the tags write:
     - Title: <table title or purpose if visible, else "Untitled Table">
     - Columns: <comma-separated list of all column header names>
     - Then for every data row write exactly: Row: <col1 name> is <value>, <col2 name> is <value>, <col3 name> is <value> ...
     - For total/summary rows write: Summary Row: <col name> is <value>, ...
     - For any footnotes or annotations below the table write: Note: <text>
   Every single data cell must appear. Do not skip any row or column.
   Example:
     [TABLE]
     Title: Tax Invoice Summary
     Columns: Sr No, Description, HSN Code, Qty, Unit, Rate (Rs), Amount (Rs)
     Row: Sr No is 1, Description is Consulting Services, HSN Code is 998314, Qty is 10, Unit is Hours, Rate (Rs) is 5000, Amount (Rs) is 50000
     Row: Sr No is 2, Description is Software License, HSN Code is 997331, Qty is 1, Unit is License, Rate (Rs) is 20000, Amount (Rs) is 20000
     Summary Row: Amount (Rs) Total is 70000
     Note: All amounts are exclusive of GST
     [/TABLE]

4. LISTS:
   Preserve all bullet points and numbered lists exactly, including indentation.

5. FORMS AND FIELDS:
   Extract as "Label: Value" for every field.
   If a field is empty write "Label: [blank]".
   Capture all fields: GSTIN, PAN, address, date, ARN, acknowledgement number, etc.

6. IMAGES, DIAGRAMS, FLOWCHARTS:
   Describe fully inside [IMAGE/FLOWCHART] ... [/IMAGE/FLOWCHART] tags.
   Include: all text labels, arrows, flow direction, box contents, relationships.

7. STAMPS, SEALS, SIGNATURES, WATERMARKS:
   [STAMP] ... [/STAMP] for rubber/digital stamps with their text.
   [SIGNATURE: description] for signatures.
   [WATERMARK: text] for watermarks.

8. MULTI-COLUMN LAYOUTS:
   Process left column fully then right column, separated by a blank line.

9. FOOTERS & BOTTOM-OF-PAGE (read this last — do not miss anything at the bottom):
   Extract page numbers, footer disclaimers, URLs, authorization text, sign-off lines, any text near the bottom margin.

10. BLANK PAGE:
    If the page has no content at all output only: [BLANK PAGE]

Start your output directly with the extracted content. No preamble. No "Here is the text:". Just the content."""


# ============================================================================
# DOCUMENT TO IMAGES CONVERTER
# ============================================================================

def _add_padding(img: Image.Image, padding_px: int = 30) -> Image.Image:
    """
    Add white padding around the image.

    Why: PDF renderers and rasterizers sometimes clip 1-3mm at page edges.
    Adding padding ensures the vision model sees a small safe margin around
    all four sides, so no header/footer text is cut off at the extreme edges.
    """
    return ImageOps.expand(img, border=padding_px, fill=(255, 255, 255))


def _convert_to_images(file_path: str, dpi: int = 300) -> List[Image.Image]:
    """
    Convert any supported document to a list of PIL Images (one per page).

    DPI=300:
      - 72 DPI (screen): blurry, fine print lost
      - 150 DPI: acceptable but small fonts can fail
      - 300 DPI: standard for OCR/vision — captures fine print, footnotes,
                 small stamps, and boundary text reliably
      - 400+ DPI: diminishing returns, large image size

    Each page gets 30px white padding so boundary text is never clipped.

    Supports: PDF, DOCX, PPTX, XLSX, images (PNG, JPG, TIFF, BMP), HTML.
    """
    ext = os.path.splitext(file_path)[1].lower()

    # --- Standalone image files ---
    if ext in ('.png', '.jpg', '.jpeg', '.tiff', '.tif', '.bmp', '.gif', '.webp'):
        img = Image.open(file_path).convert("RGB")
        return [_add_padding(img)]

    # --- PDF ---
    if ext == '.pdf':
        try:
            import fitz  # pymupdf
            doc = fitz.open(file_path)
            images = []
            mat = fitz.Matrix(dpi / 72, dpi / 72)
            for page in doc:
                # clip=None ensures full page including margins is rendered
                pix = page.get_pixmap(matrix=mat, alpha=False, clip=None)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                images.append(_add_padding(img))
            doc.close()
            return images
        except ImportError:
            raise ImportError(
                "pymupdf is required for PDF processing. "
                "Install with: pip install pymupdf"
            )

    # --- DOCX / PPTX / XLSX: convert to PDF via LibreOffice, then rasterize ---
    if ext in ('.docx', '.doc', '.pptx', '.ppt', '.xlsx', '.xls', '.odt', '.odp', '.ods'):
        try:
            import subprocess
            import fitz

            tmp_dir = tempfile.mkdtemp()
            try:
                result = subprocess.run(
                    [
                        'libreoffice', '--headless', '--convert-to', 'pdf',
                        '--outdir', tmp_dir, file_path
                    ],
                    capture_output=True, text=True, timeout=120
                )
                if result.returncode != 0:
                    raise RuntimeError(f"LibreOffice conversion failed: {result.stderr}")

                base_name = os.path.splitext(os.path.basename(file_path))[0]
                pdf_path = os.path.join(tmp_dir, base_name + '.pdf')
                if not os.path.exists(pdf_path):
                    pdfs = [f for f in os.listdir(tmp_dir) if f.endswith('.pdf')]
                    if not pdfs:
                        raise FileNotFoundError("LibreOffice did not produce a PDF output")
                    pdf_path = os.path.join(tmp_dir, pdfs[0])

                doc = fitz.open(pdf_path)
                images = []
                mat = fitz.Matrix(dpi / 72, dpi / 72)
                for page in doc:
                    pix = page.get_pixmap(matrix=mat, alpha=False, clip=None)
                    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                    images.append(_add_padding(img))
                doc.close()
                return images
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)

        except ImportError:
            raise ImportError(
                "pymupdf is required. Install with: pip install pymupdf"
            )

    # --- HTML ---
    if ext in ('.html', '.htm'):
        try:
            import weasyprint
            import fitz

            tmp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
            tmp_pdf.close()
            try:
                weasyprint.HTML(filename=file_path).write_pdf(tmp_pdf.name)
                doc = fitz.open(tmp_pdf.name)
                images = []
                mat = fitz.Matrix(dpi / 72, dpi / 72)
                for page in doc:
                    pix = page.get_pixmap(matrix=mat, alpha=False, clip=None)
                    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                    images.append(_add_padding(img))
                doc.close()
                return images
            finally:
                if os.path.exists(tmp_pdf.name):
                    os.unlink(tmp_pdf.name)
        except ImportError:
            raise ImportError(
                "weasyprint is required for HTML processing. "
                "Install with: pip install weasyprint"
            )

    raise ValueError(f"Unsupported file format: {ext}")


# ============================================================================
# DOCUMENT PROCESSOR - VISION-BASED WITH PARALLEL PAGE PROCESSING
# ============================================================================


class DocumentProcessor:
    """
    Vision-based document processor.

    For each document:
      1. Render all pages to 300 DPI images with 30px white padding
      2. Send ALL pages to Bedrock Amazon Nova IN PARALLEL (ThreadPoolExecutor)
      3. Collect results keyed by page index
      4. Combine in correct page order into a single clean string

    Tables are extracted as structured prose (not JSON) inside [TABLE]...[/TABLE] tags,
    preserving all column names, row values, totals, and notes while remaining
    readable by the downstream DocumentAnalyzer LLM prompt.
    """

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
        self.dpi = dpi
        self.max_workers = max_workers

    def _clean_text(self, text: str) -> str:
        """Clean and normalize extracted text while preserving meaningful structure"""
        if not text:
            return ""

        text = text.replace('\t', ' ')

        # Collapse runs of 3+ spaces to a single space
        # (2 spaces can be intentional indentation)
        text = re.sub(r' {3,}', ' ', text)

        lines = [line.rstrip() for line in text.split('\n')]

        # Max 1 consecutive blank line
        result = []
        prev_blank = False
        for line in lines:
            if line.strip():
                result.append(line)
                prev_blank = False
            else:
                if not prev_blank:
                    result.append('')
                prev_blank = True

        # Strip leading/trailing blank lines
        while result and not result[0].strip():
            result.pop(0)
        while result and not result[-1].strip():
            result.pop()

        return '\n'.join(result)

    def _extract_page_text(self, page_index: int, pil_image: Image.Image) -> tuple:
        """
        Extract all text from a single page image via Bedrock vision model.
        Returns (page_index, text) — page_index is the sort key for ordering.
        """
        try:
            text = self.vision_client.describe_image(pil_image, prompt=PAGE_EXTRACTION_PROMPT)
            cleaned = self._clean_text(text)

            if cleaned.strip() == "[BLANK PAGE]" or not cleaned.strip():
                return page_index, ""

            return page_index, cleaned
        except Exception as e:
            return page_index, f"[Page {page_index + 1} extraction error: {str(e)}]"

    def extract_text(self, file_path: str) -> str:
        """
        Extract all text from a document.

        All pages are processed in parallel; results are combined in correct page order.
        """
        # Step 1: Render document pages to high-res images
        page_images = _convert_to_images(file_path, dpi=self.dpi)

        if not page_images:
            return ""

        total_pages = len(page_images)

        # Step 2: Parallel extraction — all pages hit Bedrock simultaneously
        page_results = {}

        with ThreadPoolExecutor(max_workers=min(self.max_workers, total_pages)) as executor:
            futures = {
                executor.submit(self._extract_page_text, idx, img): idx
                for idx, img in enumerate(page_images)
            }
            for future in as_completed(futures):
                page_idx, page_text = future.result()
                page_results[page_idx] = page_text

        # Step 3: Assemble in page order
        content_blocks = []
        for i in range(total_pages):
            page_text = page_results.get(i, "")
            if page_text.strip():
                content_blocks.append(page_text)

        full_text = '\n\n'.join(content_blocks)
        return self._clean_text(full_text)

    def extract_text_from_multiple_files(self, file_paths: List[str], filenames: List[str]) -> str:
        """Extract and combine text from multiple documents"""
        combined_text_parts = []

        for idx, (file_path, filename) in enumerate(zip(file_paths, filenames), 1):
            separator = f"\n\n{'=' * 80}\nDOCUMENT {idx}: {filename}\n{'=' * 80}\n\n"

            try:
                extracted_text = self.extract_text(file_path)
                if extracted_text.strip():
                    combined_text_parts.append(separator + extracted_text)
                else:
                    combined_text_parts.append(separator + f"[No text could be extracted from {filename}]")
            except Exception as e:
                combined_text_parts.append(separator + f"[Error extracting text from {filename}: {str(e)}]")

        return '\n\n'.join(combined_text_parts)


# ============================================================================
# LLM ANALYZER - OPTIMIZED FOR LEGAL DOCUMENTS
# ============================================================================


class DocumentAnalyzer:
    """Analyzes legal documents using Qwen3-Next-80B-A3B with optimized prompts"""

    def __init__(self, model_id: str = "qwen.qwen3-next-80b-a3b", region: str = None):
        self.model_id = model_id
        if region is None:
            region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.client = boto3.client('bedrock-runtime', region_name=region)

    def analyze(self, extracted_text: str, user_question: Optional[str] = None) -> dict:
        """Analyze legal document with optimized issue detection and comprehensive summary"""

        MAX_TEXT_LENGTH = 100000
        if len(extracted_text) > MAX_TEXT_LENGTH:
            extracted_text = extracted_text[:MAX_TEXT_LENGTH] + "\n\n[...Document truncated due to length...]"

        prompt = f"""You are analyzing a legal document. Extract information ONLY from the document text provided. Do not infer or generate information.

DOCUMENT CONTENT:
{extracted_text}

INSTRUCTIONS:
1. PARTY IDENTIFICATION  
(Extract ONLY if explicitly and unambiguously mentioned in the document text)

A party MUST satisfy BOTH conditions:
• A legal role is clearly indicated
• A concrete identifying value (name / company / authority / ID) is present

DO NOT guess, infer, or reconstruct missing information.

---

a) SENDER / FROM  
Who issued or sent this document.

Extract ONLY if the document explicitly contains:
• "From:", "Issued by", "Issued under the authority of"
• Name of department / authority / organization issuing the document
• Sender name in signature block (with actual name, not just title)

Rules:
• Extract the exact text as written in the document
• Preserve original wording and capitalization
• If the sender is implied but not named → return null
• If only a designation/title is present without a name → return null

Examples:
• "From: Income Tax Department, Mumbai" → "Income Tax Department, Mumbai"
• "Issued by the Registrar of Companies, Delhi" → "Registrar of Companies, Delhi"
• "Authorised Signatory" → null

---

b) RECIPIENT / TO  
To whom the document is addressed.

Extract ONLY if the document explicitly contains:
• "To:", "Addressed to", "Notice to"
• Recipient name/company mentioned as the addressee
• A populated identification field (e.g., Legal Name, GSTIN) WITH an actual value

Rules:
• Extract the exact value as written
• DO NOT treat field labels, headings, or empty placeholders as values
• Labels such as "GSTIN:", "Legal Name:", "PAN:" WITHOUT a filled value → null
• If the document is general or system-generated without an addressee → null

Examples:
• "To: ABC Pvt Ltd (GSTIN: 27XXXXX)" → "ABC Pvt Ltd (GSTIN: 27XXXXX)"
• "Legal Name:" → null
• "GSTIN :" → null
• "GSTIN : Legal Name :" → null

---

ABSOLUTE CONSTRAINT:
If there is ANY ambiguity about whether a real party name is present,
you MUST return null.

2. COMPREHENSIVE SUMMARY:
   - Create a detailed summary that captures ALL key information from the document
   - Someone reading ONLY the summary should understand the complete document without reading it
   - Include: purpose, background, key details, actions required, deadlines, amounts, references
   - Write 4-7 sentences covering the entire document comprehensively
   - Use simple, clear language
   - Extract all critical information from the document text

3. USER QUESTION RESPONSE (if user asked a question):
   - If the answer exists in the document text → provide the answer
   - If the answer does NOT exist in document text → respond: "It would we better to answer your query using my knowledge?"

4. ISSUES/ALLEGATIONS DETECTION (STRICT CRITERIA):
   
   **Include in "issues" ONLY when the document contains:**
   - Formal allegations against the recipient
   - Legal violations or non-compliance accusations
   - Show cause notices or charges
   - Disputes, complaints, or legal cases filed
   - Penalties, fines, or recovery actions initiated
   - Statutory violations or regulatory breaches mentioned
   
   **DO NOT include as "issues":**
   - Administrative discrepancies (like tax return differences that are being notified for clarification)
   - Informational notices without formal allegations
   - Routine compliance requests
   - Procedural notifications
   - Requests for clarification or explanation
   - System-generated intimations of differences (unless they explicitly state violations/penalties)
   
   **If genuine issues/allegations are found:**
   - List each issue clearly and separately
   - Extract verbatim or close paraphrase from document
   - Add issues_prompt: "Should I prepare the reply or guide for these issues?"
   
   **If NO genuine issues/allegations:**
   - Set issues: null
   - Set issues_prompt: null

"""

        if user_question:
            prompt += f"""
USER QUESTION:
"{user_question}"
"""

        prompt += """
OUTPUT FORMAT (JSON):
{{
    "sender": "exact name from document" or null,
    "recipient": "exact name/GSTIN from document" or null,
    "summary": "comprehensive 4-7 sentence summary covering all key information",
    "user_question_response": "answer from document" or "Should I resolve your query using my knowledge?" or null,
    "issues": ["issue 1", "issue 2", ...] or null,
    "issues_prompt": "Should I prepare the reply or guide for these issues?" or null
}}

CRITICAL RULES:
- sender/recipient: null if not explicitly mentioned (no guessing)
- summary: comprehensive coverage of entire document (4-7 sentences minimum)
- user_question_response: only if user asked a question
- issues: ONLY for formal allegations/violations/cases (null for routine notices/discrepancies)
- issues_prompt: only when issues exist
- Use document text only, no external knowledge
"""

        request_body = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"text": prompt}]
                }
            ],
            "inferenceConfig": {
                "maxTokens": 32000,
                "temperature": 0.1,
                "topP": 0.9
            }
        }

        try:
            response = self.client.converse(
                modelId=self.model_id,
                messages=request_body["messages"],
                inferenceConfig=request_body["inferenceConfig"]
            )

            content = response['output']['message']['content'][0]['text']

            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                analysis = json.loads(json_match.group())

                structured_response = {}

                if analysis.get("sender"):
                    structured_response["sender"] = analysis["sender"]

                if analysis.get("recipient"):
                    structured_response["recipient"] = analysis["recipient"]

                structured_response["summary"] = analysis.get("summary", "")

                if analysis.get("user_question_response"):
                    structured_response["user_question_response"] = analysis["user_question_response"]

                if analysis.get("issues"):
                    structured_response["issues"] = analysis["issues"]
                    if analysis.get("issues_prompt"):
                        structured_response["issues_prompt"] = analysis["issues_prompt"]

                return structured_response
            else:
                return {"summary": content}

        except Exception as e:
            raise Exception(f"LLM Analysis failed: {str(e)}")

    def format_response_for_frontend(self, analysis: dict) -> str:
        """Format the analysis into a single clean text response for frontend display"""

        response_parts = []

        if analysis.get("sender"):
            response_parts.append(f"From: {analysis['sender']}")

        if analysis.get("recipient"):
            response_parts.append(f"To: {analysis['recipient']}")

        if analysis.get("sender") or analysis.get("recipient"):
            response_parts.append("")

        response_parts.append(analysis.get("summary", ""))

        if analysis.get("user_question_response"):
            response_parts.append("")
            response_parts.append(f"User Query reply: {analysis['user_question_response']}")

        if analysis.get("issues"):
            response_parts.append("")
            response_parts.append("Issues:")
            for i, issue in enumerate(analysis["issues"], 1):
                response_parts.append(f"{i}. {issue}")

            if analysis.get("issues_prompt"):
                response_parts.append("")
                response_parts.append(analysis["issues_prompt"])

        return "\n".join(response_parts).strip()