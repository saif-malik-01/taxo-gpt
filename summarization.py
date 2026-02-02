import json
import base64
import boto3
import os
from io import BytesIO
from docling.document_converter import DocumentConverter
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.datamodel.base_models import InputFormat
from docling.document_converter import PdfFormatOption
from typing import Optional
from PIL import Image
from dotenv import load_dotenv
import re
import tempfile
import shutil

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

load_dotenv()

# ============================================================================
# DOCUMENT PROCESSOR - WITH JSON TABLE EXTRACTION
# ============================================================================

class AmazonNovaClient:
    """AWS Bedrock client"""
    
    def __init__(self, model_id: str = "amazon.nova-lite-v1:0", region: str = None):
        self.model_id = model_id
        if region is None:
            region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.client = boto3.client('bedrock-runtime', region_name=region)
    
    def describe_image(self, pil_image: Image.Image, prompt: str = None) -> str:
        max_size = 2048
        if max(pil_image.size) > max_size:
            pil_image.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
        
        buffered = BytesIO()
        pil_image.save(buffered, format="PNG")
        img_base64 = base64.b64encode(buffered.getvalue()).decode("utf-8")
        
        if prompt is None:
            prompt = """Extract information from this image/diagram/flowchart.
Extract: all visible text, visual elements, flow, meaning, labels."""

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


class DocumentProcessor:
    """Document processor for extraction with JSON table support"""
    
    def __init__(self, bedrock_model: str = "amazon.nova-lite-v1:0", bedrock_region: str = None):
        if bedrock_region is None:
            bedrock_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        
        self.vision_client = AmazonNovaClient(bedrock_model, bedrock_region)
        
        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = True
        pipeline_options.generate_picture_images = True
        pipeline_options.images_scale = 2.0
        
        self.converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
            }
        )
    
    def _normalize_text(self, text: str) -> str:
        text = text.replace('\t', ' ')
        text = re.sub(r' {2,}', ' ', text)
        lines = [line.strip() for line in text.split('\n')]
        
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
        """Extract text from document - returns plain text with JSON tables"""
        result = self.converter.convert(file_path)
        self.current_document = result.document
        
        all_content = []
        
        for item, _ in result.document.iterate_items():
            element_type = type(item).__name__
            
            if element_type == "TableItem":
                all_content.append(self._format_table_as_json(item))
            elif element_type == "PictureItem":
                all_content.append(self._process_image(item))
            else:
                if hasattr(item, 'text') and item.text.strip():
                    all_content.append(item.text.strip())
        
        return '\n\n'.join(all_content)
    
    def _format_table_as_json(self, table_item) -> str:
        """Convert table to row-wise JSON format"""
        try:
            # Export table to markdown first to get structured data
            md = table_item.export_to_markdown(doc=self.current_document)
            
            # Parse markdown table to JSON
            lines = [line.strip() for line in md.strip().split('\n') if line.strip()]
            
            if len(lines) < 2:
                return f"\n[TABLE]\n{md}\n[/TABLE]\n"
            
            # Extract headers
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
                            row_dict[header] = cells[i]
                        else:
                            row_dict[header] = ""
                    rows_json.append(row_dict)
            
            # Format as JSON
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
        if picture_item.image and picture_item.image.pil_image:
            try:
                desc = self.vision_client.describe_image(picture_item.image.pil_image)
                return f"\n[IMAGE/FLOWCHART]\n{self._normalize_text(desc)}\n[/IMAGE/FLOWCHART]\n"
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
   - If the answer does NOT exist in document text → respond: "Should I resolve your query using my knowledge?"

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
{
    "sender": "exact name from document" or null,
    "recipient": "exact name/GSTIN from document" or null,
    "summary": "comprehensive 4-7 sentence summary covering all key information",
    "user_question_response": "answer from document" or "Should I resolve your query using my knowledge?" or null,
    "issues": ["issue 1", "issue 2", ...] or null,
    "issues_prompt": "Should I prepare the reply or guide for these issues?" or null
}

CRITICAL RULES:
- sender/recipient: null if not explicitly mentioned (no guessing)
- summary: comprehensive coverage of entire document (4-7 sentences minimum)
- user_question_response: only if user asked a question
- issues: ONLY for formal allegations/violations/cases (null for routine notices/discrepancies)
- issues_prompt: only when issues exist
- Use document text only, no external knowledge
"""

        # Call Qwen3-Next via Bedrock Converse API
        request_body = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"text": prompt}]
                }
            ],
            "inferenceConfig": {
                "maxTokens": 4096,
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
            response_parts.append(f"Answer: {analysis['user_question_response']}")
        
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


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

app = FastAPI(
    title="Legal Document Analysis API - Optimized",
    description="Analyze legal documents with optimized issue detection and comprehensive summaries",
    version="5.0.0"
)

# Initialize processors (singleton)
doc_processor = DocumentProcessor()
doc_analyzer = DocumentAnalyzer()


class AnalysisResponse(BaseModel):
    """Clean response model"""
    success: bool
    extracted_text: str
    structured_analysis: dict
    formatted_response: str
    metadata: dict


@app.post("/analyze-document", response_model=AnalysisResponse)
async def analyze_document(
    file: UploadFile = File(..., description="Legal document (PDF, DOCX, PPTX, XLSX, HTML, Images)"),
    user_question: Optional[str] = Form(None, description="Optional: Question about the document")
):
    """
    Analyze legal documents with optimized structure
    
    **Features:**
    - Comprehensive document summary (entire document in 4-7 sentences)
    - Smart issue detection (only genuine allegations/violations, not routine notices)
    - Conditional party extraction (only if explicitly present)
    - Row-wise JSON table extraction
    - Single formatted response for frontend display
    
    **Output Structure:**
    1. extracted_text: Full document text with JSON tables
    2. structured_analysis: JSON object with individual fields
    3. formatted_response: Single formatted text combining all non-empty fields (for frontend display)
    4. metadata: File and processing information
    
    **formatted_response structure:**
    - PARTY IDENTIFICATION (if sender/recipient present)
    - DOCUMENT SUMMARY (always present)
    - YOUR QUESTION (if user asked question)
    - ISSUES/ALLEGATIONS (only if genuine issues found)
    
    **Issue Detection:**
    Issues included ONLY for:
    - Formal allegations/violations
    - Legal cases/disputes
    - Show cause notices
    - Penalties/fines/recovery actions
    
    NOT included for:
    - Routine administrative notices
    - Informational discrepancies
    - Requests for clarification
    """
    
    temp_file = None
    
    try:
        # Validate file extension
        filename = file.filename.lower()
        supported = ['.pdf', '.docx', '.pptx', '.xlsx', '.html', '.png', '.jpg', '.jpeg', '.tiff', '.bmp']
        
        if not any(filename.endswith(ext) for ext in supported):
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file format. Supported: {', '.join(supported)}"
            )
        
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as tmp:
            temp_file = tmp.name
            shutil.copyfileobj(file.file, tmp)
        
        # Step 1: Extract text from document
        print(f"Extracting text from {filename}...")
        extracted_text = doc_processor.extract_text(temp_file)
        
        if not extracted_text.strip():
            raise HTTPException(
                status_code=422,
                detail="No text could be extracted from the document"
            )
        
        # Step 2: Analyze with Qwen3-Next-80B-A3B
        print(f"Analyzing document...")
        structured_analysis = doc_analyzer.analyze(extracted_text, user_question)
        
        # Step 3: Format response for frontend
        formatted_response = doc_analyzer.format_response_for_frontend(structured_analysis)
        
        # Step 4: Return complete response
        return AnalysisResponse(
            success=True,
            extracted_text=extracted_text,
            structured_analysis=structured_analysis,
            formatted_response=formatted_response,
            metadata={
                "filename": file.filename,
                "content_type": file.content_type,
                "size_bytes": os.path.getsize(temp_file),
                "llm_model": "qwen.qwen3-next-80b-a3b"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")
    
    finally:
        # Cleanup temp file
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except:
                pass


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Legal Document Analysis API - Optimized",
        "version": "5.0.0",
        "llm_model": "Qwen3-Next-80B-A3B"
    }


@app.get("/supported-formats")
async def supported_formats():
    """List supported document formats"""
    return {
        "supported_formats": [
            "PDF (.pdf)",
            "Word (.docx)",
            "PowerPoint (.pptx)",
            "Excel (.xlsx)",
            "HTML (.html)",
            "Images (.png, .jpg, .jpeg, .tiff, .bmp)"
        ]
    }


@app.get("/issue-detection-criteria")
async def issue_detection_criteria():
    """Explain issue detection criteria"""
    return {
        "issues_included": [
            "Formal allegations or charges against recipient",
            "Legal violations or statutory non-compliance",
            "Show cause notices",
            "Penalties, fines, or recovery actions",
            "Legal disputes or cases filed",
            "Regulatory breaches"
        ],
        "issues_excluded": [
            "Administrative discrepancies",
            "Routine compliance notifications",
            "Informational notices",
            "Requests for clarification/explanation",
            "System-generated difference intimations (without formal allegations)",
            "Procedural notifications"
        ],
        "example": {
            "included_as_issue": "Notice u/s 74 for tax evasion with penalty of Rs 1,00,000",
            "not_included_as_issue": "Intimation of difference in GSTR-1 vs GSTR-3B (requesting clarification)"
        }
    }


@app.get("/response-structure")
async def response_structure():
    """Explain the response structure"""
    return {
        "response_fields": {
            "extracted_text": "Full document text with [TABLE_JSON] sections",
            "structured_analysis": "JSON object with individual fields (clean, no empty fields)",
            "formatted_response": "Single clean text merging all non-empty fields (ready for frontend display)",
            "metadata": "File and processing information"
        },
        "structured_analysis_fields": {
            "sender": "Present only if explicitly mentioned in document",
            "recipient": "Present only if explicitly mentioned in document",
            "summary": "Always present - comprehensive 4-7 sentence summary of entire document",
            "user_question_response": "Present only if user asked a question",
            "issues": "Present only if genuine allegations/violations found",
            "issues_prompt": "Present only when issues are present"
        },
        "formatted_response_structure": {
            "description": "Simple clean text merging all non-empty fields",
            "format": [
                "From: ... (if sender present)",
                "To: ... (if recipient present)",
                "",
                "Summary text...",
                "",
                "Answer: ... (if user asked question)",
                "",
                "Issues: (if issues present)",
                "1. Issue one",
                "2. Issue two",
                "",
                "Should I prepare the reply or guide for these issues?"
            ],
            "example_full": """From: Income Tax Department, Mumbai
To: ABC Pvt Ltd (GSTIN: 27XXXXX)

This is a system-generated notice under Rule 88C of GST law...

Answer: The deadline for response is 7 days from notice date.

Issues:
1. Tax evasion allegation under Section 74
2. Penalty of Rs 5,00,000 proposed

Should I prepare the reply or guide for these issues?""",
            "example_simple": """This is a system-generated notice under Rule 88C of GST law, intimating the taxpayer of differential tax liability..."""
        },
        "usage": {
            "for_json_processing": "Use structured_analysis field",
            "for_frontend_display": "Use formatted_response field (clean merged text, ready to display)"
        }
    }


if __name__ == "__main__":
    import uvicorn
    
    # Check environment
    if not all([os.getenv('AWS_ACCESS_KEY_ID'), 
                os.getenv('AWS_SECRET_ACCESS_KEY'),
                os.getenv('AWS_DEFAULT_REGION')]):
        print("ERROR: AWS credentials not configured!")
        exit(1)
    
    print("\n" + "="*70)
    print("LEGAL DOCUMENT ANALYSIS API - OPTIMIZED VERSION")
    print("="*70)
    print("Version: 5.0.0")
    print("LLM Model: Qwen3-Next-80B-A3B-Instruct")
    print("\nOptimizations:")
    print("  ✓ Comprehensive summaries (4-7 sentences covering entire document)")
    print("  ✓ Smart issue detection (only genuine allegations/violations)")
    print("  ✓ Clean response structure (no empty fields)")
    print("  ✓ Row-wise JSON table extraction")
    print("  ✓ Conditional party identification")
    print("\nIssue Detection:")
    print("  • Includes: Allegations, violations, penalties, legal cases")
    print("  • Excludes: Routine notices, administrative discrepancies")
    print("="*70)
    print("Endpoints:")
    print("  POST /analyze-document")
    print("  GET  /health")
    print("  GET  /supported-formats")
    print("  GET  /issue-detection-criteria")
    print("  GET  /response-structure")
    print("  GET  /docs")
    print("="*70 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)
