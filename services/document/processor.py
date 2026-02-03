import os
# Configure Hugging Face cache for cross-platform compatibility
os.environ['HF_HUB_DISABLE_SYMLINKS'] = '1'
os.environ['HF_HOME'] = os.path.join(os.path.dirname(__file__), '..', '..', '.hf_cache')

import json
import base64
import boto3
import re
from io import BytesIO
from typing import Optional, List

from PIL import Image
from dotenv import load_dotenv
from docling.document_converter import DocumentConverter
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.datamodel.base_models import InputFormat
from docling.document_converter import PdfFormatOption

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
# DOCUMENT PROCESSOR - WITH JSON TABLE EXTRACTION (OPTIMIZED)
# ============================================================================


class DocumentProcessor:
    """Document processor with improved text extraction and JSON table support"""

    def __init__(self, bedrock_model: str = "amazon.nova-lite-v1:0", bedrock_region: str = None):
        if bedrock_region is None:
            bedrock_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

        self.vision_client = AmazonNovaClient(bedrock_model, bedrock_region)

        # Optimized pipeline settings
        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = True
        pipeline_options.generate_picture_images = True
        pipeline_options.images_scale = 2.0

        self.converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
            }
        )

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text for better readability"""
        if not text:
            return ""

        # Replace tabs with spaces
        text = text.replace('\t', ' ')

        # Normalize multiple spaces to single space
        text = re.sub(r' {2,}', ' ', text)

        # Split into lines and process
        lines = [line.strip() for line in text.split('\n')]

        # Remove excessive blank lines (keep max 1 consecutive blank line)
        result = []
        prev_blank = False
        for line in lines:
            if line:
                result.append(line)
                prev_blank = False
            else:
                if not prev_blank:
                    result.append(line)
                prev_blank = True

        return '\n'.join(result)

    def extract_text(self, file_path: str) -> str:
        """Extract text from document - returns clean text with JSON tables"""
        result = self.converter.convert(file_path)
        self.current_document = result.document

        content_blocks = []

        for item, _ in result.document.iterate_items():
            element_type = type(item).__name__

            if element_type == "TableItem":
                # Extract table as JSON
                table_json = self._format_table_as_json(item)
                if table_json:
                    content_blocks.append(table_json)

            elif element_type == "PictureItem":
                # Process images/diagrams
                image_desc = self._process_image(item)
                if image_desc:
                    content_blocks.append(image_desc)

            else:
                # Regular text content
                if hasattr(item, 'text') and item.text.strip():
                    cleaned_text = self._clean_text(item.text.strip())
                    if cleaned_text:
                        content_blocks.append(cleaned_text)

        # Join all content with double newlines for readability
        full_text = '\n\n'.join(content_blocks)

        # Final cleanup pass
        return self._clean_text(full_text)

    def extract_text_from_multiple_files(self, file_paths: List[str], filenames: List[str]) -> str:
        """Extract and combine text from multiple documents"""
        combined_text_parts = []

        for idx, (file_path, filename) in enumerate(zip(file_paths, filenames), 1):
            # Add document separator with filename
            separator = f"\n\n{'=' * 80}\nDOCUMENT {idx}: {filename}\n{'=' * 80}\n\n"

            # Extract text from this document
            try:
                extracted_text = self.extract_text(file_path)
                if extracted_text.strip():
                    combined_text_parts.append(separator + extracted_text)
                else:
                    combined_text_parts.append(separator + f"[No text could be extracted from {filename}]")
            except Exception as e:
                combined_text_parts.append(separator + f"[Error extracting text from {filename}: {str(e)}]")

        # Combine all documents
        return '\n\n'.join(combined_text_parts)

    def _format_table_as_json(self, table_item) -> str:
        """Convert table to clean row-wise JSON format"""
        try:
            # Export table to markdown first
            md = table_item.export_to_markdown(doc=self.current_document)

            # Parse markdown table to JSON
            lines = [line.strip() for line in md.strip().split('\n') if line.strip()]

            if len(lines) < 2:
                # Fallback to markdown if too short
                return f"\n[TABLE]\n{md}\n[/TABLE]\n"

            # Extract headers from first line
            headers = [h.strip() for h in lines[0].split('|') if h.strip()]

            # Skip separator line (line 1)
            # Process data rows (from line 2 onwards)
            rows_json = []
            for line in lines[2:]:
                cells = [c.strip() for c in line.split('|') if c.strip()]
                if cells:
                    row_dict = {}
                    for i, header in enumerate(headers):
                        if i < len(cells):
                            # Clean cell content
                            cell_value = cells[i].strip()
                            row_dict[header] = cell_value
                        else:
                            row_dict[header] = ""
                    rows_json.append(row_dict)

            # Format as clean JSON
            json_output = json.dumps(rows_json, indent=2, ensure_ascii=False)
            return f"\n[TABLE_JSON]\n{json_output}\n[/TABLE_JSON]\n"

        except Exception as e:
            # Fallback to markdown if JSON conversion fails
            try:
                md = table_item.export_to_markdown(doc=self.current_document)
                return f"\n[TABLE]\n{md}\n[/TABLE]\n"
            except:
                return f"[TABLE Error: {str(e)}]"

    def _process_image(self, picture_item) -> str:
        """Process images and flowcharts"""
        if picture_item.image and picture_item.image.pil_image:
            try:
                desc = self.vision_client.describe_image(picture_item.image.pil_image)
                cleaned_desc = self._clean_text(desc)
                return f"\n[IMAGE/FLOWCHART]\n{cleaned_desc}\n[/IMAGE/FLOWCHART]\n"
            except Exception as e:
                return f"[IMAGE Error: {str(e)}]"
        return ""


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

        # Truncate text if too long (leave room for prompt + response)
        # 32k tokens ≈ 24k words ≈ 120k chars
        MAX_TEXT_LENGTH = 100000  # Conservative limit
        if len(extracted_text) > MAX_TEXT_LENGTH:
            extracted_text = extracted_text[:MAX_TEXT_LENGTH] + "\n\n[...Document truncated due to length...]"

        # Build optimized analysis prompt
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

        # Call Qwen3-Next via Bedrock Converse API with increased max tokens
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

            # Extract response from Converse API
            content = response['output']['message']['content'][0]['text']

            # Extract JSON from response
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                analysis = json.loads(json_match.group())

                # Build clean structured response (no empty fields)
                structured_response = {}

                # Add sender if present
                if analysis.get("sender"):
                    structured_response["sender"] = analysis["sender"]

                # Add recipient if present
                if analysis.get("recipient"):
                    structured_response["recipient"] = analysis["recipient"]

                # Summary is always present
                structured_response["summary"] = analysis.get("summary", "")

                # Add user question response if present
                if analysis.get("user_question_response"):
                    structured_response["user_question_response"] = analysis["user_question_response"]

                # Add issues if present
                if analysis.get("issues"):
                    structured_response["issues"] = analysis["issues"]
                    if analysis.get("issues_prompt"):
                        structured_response["issues_prompt"] = analysis["issues_prompt"]

                return structured_response
            else:
                return {
                    "summary": content
                }

        except Exception as e:
            raise Exception(f"LLM Analysis failed: {str(e)}")

    def format_response_for_frontend(self, analysis: dict) -> str:
        """Format the analysis into a single clean text response for frontend display"""

        response_parts = []

        # 1. Sender (if present)
        if analysis.get("sender"):
            response_parts.append(f"From: {analysis['sender']}")

        # 2. Recipient (if present)
        if analysis.get("recipient"):
            response_parts.append(f"To: {analysis['recipient']}")

        # Add blank line after party info if present
        if analysis.get("sender") or analysis.get("recipient"):
            response_parts.append("")

        # 3. Summary (always present)
        response_parts.append(analysis.get("summary", ""))

        # 4. User Question Response (if present)
        if analysis.get("user_question_response"):
            response_parts.append("")
            response_parts.append(f"User Query reply: {analysis['user_question_response']}")

        # 5. Issues (if present)
        if analysis.get("issues"):
            response_parts.append("")
            response_parts.append("Issues:")
            for i, issue in enumerate(analysis["issues"], 1):
                response_parts.append(f"{i}. {issue}")

            # Add issues prompt
            if analysis.get("issues_prompt"):
                response_parts.append("")
                response_parts.append(analysis["issues_prompt"])

        # Combine all parts
        return "\n".join(response_parts).strip()