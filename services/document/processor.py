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
            # Output tokens updated from 4096 to 8192
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
   - Use markdown heading levels matching the visual hierarchy:
       # for the largest heading on the page
       ## for sub-headings
       ### for minor headings
   - Do NOT rephrase, simplify, or omit any sentence.

2. TABLES
   Use this format — it preserves both structure and meaning for downstream LLM processing:

   [TABLE: <exact table title if present, else "Table">]
   | Column Header 1 | Column Header 2 | Column Header 3 | ... |
   |-----------------|-----------------|-----------------|-----|
   | row1_val1       | row1_val2       | row1_val3       | ... |
   | row2_val1       | row2_val2       | row2_val3       | ... |
   | **Total/Summary label** | **val** | **val** | ... |
   Notes: <any footnote, annotation, or disclaimer text below the table, verbatim>
   [/TABLE]

   RULES for tables:
   - Include EVERY row and EVERY column — no skipping.
   - For merged/spanning cells, repeat the merged value in each affected column.
   - For blank cells, use an empty cell: | |
   - Bold totals/summary rows using **text**.
   - If a table has no visible title, use a descriptive label based on its first row.
   - If multiple tables appear on the page, extract each separately with its own [TABLE]...[/TABLE] block.
   - Preserve numeric formatting exactly: "1,23,456.78" stays "1,23,456.78", not "123456.78".

3. FLOWCHARTS, PROCESS DIAGRAMS, DECISION TREES
   Use this format to preserve flow, logic, and all labels:

   [FLOWCHART: <title or "Process Flow">]
   START
   → [Box/Step label: exact text inside the shape]
   → <Decision: exact question text?>
       YES → [Step label: text]
             → [Next step: text]
       NO  → [Step label: text]
             → [Next step: text]
   → [Final step or END]
   END

   Additional elements (use as needed):
   PARALLEL: [Branch A: text] | [Branch B: text]   ← for parallel tracks
   LOOP: repeat from <step name> if <condition>      ← for loops
   ARROW LABEL: "<label text>"                       ← for named arrows
   NOTE: <any annotation or legend text>
   [/FLOWCHART]

   RULES for flowcharts:
   - Capture every shape, every arrow, every label — verbatim.
   - Show branching using indented YES/NO (or True/False / other condition labels).
   - If a flowchart and a table appear on the same page, extract both in the order they appear.

4. FORMS AND FIELDS
   Extract as "Label: Value" for every field.
   If a field is blank, write: "Label: [blank]"
   Examples: GSTIN: 27AABCU9603R1ZX, Date: [blank], PAN: AABCU9603R

5. LISTS
   Preserve all bullet points and numbered lists exactly, including nesting and indentation.

6. IMAGES, PHOTOGRAPHS, CHARTS (non-flowchart)
   [IMAGE: <brief factual description of what is depicted — people, objects, logo, chart type>]
   Text visible in image: <verbatim text>
   [/IMAGE]

7. STAMPS, SEALS, WATERMARKS, SIGNATURES
   [STAMP: <exact text on the stamp>]
   [SEAL: <text or description>]
   [SIGNATURE: <name or "illegible">]
   [WATERMARK: <text>]

8. MULTI-COLUMN LAYOUTS
   Process the left column fully first, then the right column.
   Separate columns with: --- (column break) ---

9. HEADERS AND FOOTERS
   Extract verbatim at the start (header) and end (footer) of each page's output.
   [HEADER: <text>]
   ...body content...
   [FOOTER: <text>]

10. BLANK PAGE
    If the page has absolutely no content: [BLANK PAGE]

---

OUTPUT RULES:
- Start directly with the extracted content. No preamble. No "Here is the extracted text:".
- Maintain the exact top-to-bottom order in which content appears on the page.
- Do not merge content from different visual sections without a blank line separator.
- Do not add headings, labels, or commentary that are not present on the page.
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
            doc = fitz.open(file_path)
            images = []
            mat = fitz.Matrix(dpi / 72, dpi / 72)
            for page in doc:
                pix = page.get_pixmap(matrix=mat, alpha=False, clip=None)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                images.append(_add_padding(img))
            doc.close()
            return images
        except ImportError:
            raise ImportError("pymupdf is required for PDF processing. Install with: pip install pymupdf")

    if ext in ('.docx', '.doc', '.pptx', '.ppt', '.xlsx', '.xls', '.odt', '.odp', '.ods'):
        try:
            import subprocess
            import fitz

            tmp_dir = tempfile.mkdtemp()
            try:
                result = subprocess.run(
                    ['libreoffice', '--headless', '--convert-to', 'pdf', '--outdir', tmp_dir, file_path],
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
            raise ImportError("pymupdf is required. Install with: pip install pymupdf")

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
            raise ImportError("weasyprint is required for HTML processing. Install with: pip install weasyprint")

    raise ValueError(f"Unsupported file format: {ext}")


# ============================================================================
# DOCUMENT PROCESSOR
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
        self.dpi = dpi
        self.max_workers = max_workers

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""

        text = text.replace('\t', ' ')
        text = re.sub(r' {3,}', ' ', text)

        lines = [line.rstrip() for line in text.split('\n')]

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

        while result and not result[0].strip():
            result.pop(0)
        while result and not result[-1].strip():
            result.pop()

        return '\n'.join(result)

    def _extract_page_text(self, page_index: int, pil_image: Image.Image) -> tuple:
        try:
            text = self.vision_client.describe_image(pil_image, prompt=PAGE_EXTRACTION_PROMPT)
            cleaned = self._clean_text(text)

            if cleaned.strip() == "[BLANK PAGE]" or not cleaned.strip():
                return page_index, ""

            return page_index, cleaned
        except Exception as e:
            return page_index, f"[Page {page_index + 1} extraction error: {str(e)}]"

    def extract_text(self, file_path: str) -> str:
        page_images = _convert_to_images(file_path, dpi=self.dpi)

        if not page_images:
            return ""

        total_pages = len(page_images)
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

        full_text = '\n\n'.join(content_blocks)
        return self._clean_text(full_text)

    def extract_text_from_multiple_files(self, file_paths: List[str], filenames: List[str]) -> str:
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
# DOCUMENT ANALYZER
# ============================================================================

# Two-pass threshold: documents longer than this are split for analysis
_TWO_PASS_THRESHOLD = 80_000
# Overlap at split point to avoid cutting mid-issue
_SPLIT_OVERLAP = 2_000


class DocumentAnalyzer:
    """Analyzes legal documents using Qwen3-Next-80B-A3B."""

    def __init__(self, model_id: str = "qwen.qwen3-next-80b-a3b", region: str = None):
        self.model_id = model_id
        if region is None:
            region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.client = boto3.client('bedrock-runtime', region_name=region)

    def _run_analysis(self, extracted_text: str, user_question: Optional[str] = None) -> dict:
        """
        Single-pass analysis on a text segment.
        Issues are extracted verbatim — no paraphrasing, no filtering.
        Output tokens corrected from 32000 to 8192.
        """

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

a) SENDER / FROM
Who issued or sent this document.
Extract ONLY if the document explicitly contains:
• "From:", "Issued by", "Issued under the authority of"
• Name of department / authority / organization issuing the document
• Sender name in signature block (with actual name, not just title)
Rules:
• Extract the exact text as written in the document
• If the sender is implied but not named → return null
• If only a designation/title is present without a name → return null

b) RECIPIENT / TO
To whom the document is addressed.
Extract ONLY if the document explicitly contains:
• "To:", "Addressed to", "Notice to"
• Recipient name/company mentioned as the addressee
• A populated identification field (e.g., Legal Name, GSTIN) WITH an actual value
Rules:
• DO NOT treat field labels, headings, or empty placeholders as values
• Labels such as "GSTIN:", "Legal Name:", "PAN:" WITHOUT a filled value → null

2. COMPREHENSIVE SUMMARY:
- Create a detailed summary that captures ALL key information from the document
- Include: purpose, background, key details, actions required, deadlines, amounts, references, periods, GSTINs, transaction types
- Write 4-7 sentences covering the entire document comprehensively
- Use simple, clear language

3. USER QUESTION RESPONSE (if user asked a question):
- If the answer exists in the document text → provide the answer
- If the answer does NOT exist in document text → respond: "It would we better to answer your query using my knowledge?"

4. ISSUES EXTRACTION — VERBATIM:
Extract every issue, allegation, discrepancy, observation, ground, or charge mentioned in the document.
Extract each one EXACTLY as it appears in the document — verbatim, word for word.
Do NOT paraphrase, summarise, condense, or reword.
Do NOT filter or judge whether an issue is significant — extract all of them.
If an issue spans multiple sentences, include all sentences.
If there are no issues, allegations, discrepancies, or observations at all → set issues to null.

"""

        if user_question:
            prompt += f"""
USER QUESTION:
"{user_question}"
"""

        prompt += """
OUTPUT FORMAT (JSON):
{
    "sender": "exact name from document" or null,
    "recipient": "exact name/GSTIN from document" or null,
    "summary": "comprehensive 4-7 sentence summary covering all key information",
    "user_question_response": "answer from document" or "Should I resolve your query using my knowledge?" or null,
    "issues": ["verbatim issue 1", "verbatim issue 2", ...] or null,
    "issues_prompt": "Should I prepare the reply or guide for these issues?" or null
}

CRITICAL RULES:
- sender/recipient: null if not explicitly mentioned
- summary: comprehensive coverage of entire document
- issues: extract ALL verbatim — every allegation, discrepancy, observation, ground, or charge
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
                "maxTokens": 8192,  # corrected from 32000
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
                return json.loads(json_match.group())
            else:
                return {"summary": content}

        except Exception as e:
            raise Exception(f"LLM Analysis failed: {str(e)}")

    def _deduplicate_issues(self, issues: list) -> list:
        """
        Deduplicate issues from two-pass extraction.
        Uses simple normalised string comparison to detect near-duplicates.
        """
        if not issues:
            return issues

        seen = []
        unique = []

        for issue in issues:
            # Normalise for comparison: lowercase, collapse whitespace
            normalised = re.sub(r'\s+', ' ', issue.lower().strip())

            # Check against already-seen issues
            is_duplicate = False
            for s in seen:
                # If normalised strings share >80% of characters, treat as duplicate
                shorter = min(len(normalised), len(s))
                if shorter == 0:
                    continue
                # Simple overlap check: if one string contains the other or they are very similar
                if normalised in s or s in normalised:
                    is_duplicate = True
                    break
                # Check common prefix length
                common = sum(1 for a, b in zip(normalised, s) if a == b)
                if common / shorter > 0.85:
                    is_duplicate = True
                    break

            if not is_duplicate:
                seen.append(normalised)
                unique.append(issue)

        return unique

    def analyze(self, extracted_text: str, user_question: Optional[str] = None) -> dict:
        """
        Analyze legal document with verbatim issue extraction.

        For documents exceeding _TWO_PASS_THRESHOLD characters:
          - Split into two halves with _SPLIT_OVERLAP overlap
          - Run _run_analysis on each half independently
          - Merge issues lists (deduplicated)
          - Summary and parties taken from first half (contains document header)

        For shorter documents: single pass as before.
        """

        if len(extracted_text) <= _TWO_PASS_THRESHOLD:
            # Single pass
            analysis = self._run_analysis(extracted_text, user_question)
            return self._build_structured_response(analysis)

        # --- Two-pass for long documents ---
        mid = len(extracted_text) // 2
        # Adjust mid to nearest newline to avoid splitting mid-sentence
        newline_near_mid = extracted_text.rfind('\n', mid - 500, mid + 500)
        if newline_near_mid != -1:
            mid = newline_near_mid

        first_half  = extracted_text[:mid + _SPLIT_OVERLAP]
        second_half = extracted_text[mid - _SPLIT_OVERLAP:]

        analysis_first  = self._run_analysis(first_half,  user_question)
        analysis_second = self._run_analysis(second_half, user_question)

        # Merge: use first half for summary/parties/user_question_response
        # Merge issues from both passes
        issues_first  = analysis_first.get("issues")  or []
        issues_second = analysis_second.get("issues") or []
        all_issues    = issues_first + issues_second

        merged = dict(analysis_first)  # start with first pass as base
        if all_issues:
            merged["issues"] = self._deduplicate_issues(all_issues)
            merged["issues_prompt"] = "Should I prepare the reply or guide for these issues?"
        else:
            merged["issues"] = None
            merged["issues_prompt"] = None

        return self._build_structured_response(merged)

    def _build_structured_response(self, analysis: dict) -> dict:
        """Build the clean structured response dict from raw analysis output."""
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

    def format_response_for_frontend(self, analysis: dict) -> str:
        """Format the analysis into a single clean text response for frontend display."""

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