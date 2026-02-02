import json
import base64
import boto3
import os
from io import BytesIO
from docling.document_converter import DocumentConverter
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.datamodel.base_models import InputFormat
from docling.document_converter import PdfFormatOption
from typing import Optional, List
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
# DOCUMENT PROCESSOR - WITH JSON TABLE EXTRACTION (OPTIMIZED)
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
            separator = f"\n\n{'='*80}\nDOCUMENT {idx}: {filename}\n{'='*80}\n\n"
            
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

        # Call Qwen3-Next via Bedrock Converse API with increased max tokens
        request_body = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"text": prompt}]
                }
            ],
            "inferenceConfig": {
                "maxTokens": 32000,  # Increased from 4096 to 32000
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
    title="Legal Document Analysis API - Optimized with Multi-File Support",
    description="Analyze single or multiple legal documents with improved text extraction and 32k token support",
    version="5.2.0"
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
    files: List[UploadFile] = File(..., description="Legal document(s) - single or multiple files (PDF, DOCX, PPTX, XLSX, HTML, Images)"),
    user_question: Optional[str] = Form(None, description="Optional: Question about the document(s)")
):
    """
    Analyze single or multiple legal documents with optimized structure and improved text extraction
    
    **Features:**
    - **Multi-file support:** Upload one or more documents - they will be combined for analysis
    - Clean, normalized text extraction
    - Comprehensive document summary (entire document in 4-7 sentences)
    - Smart issue detection (only genuine allegations/violations, not routine notices)
    - Conditional party extraction (only if explicitly present)
    - Row-wise JSON table extraction
    - Single formatted response for frontend display
    - 32k max token support for analysis
    
    **Multi-File Processing:**
    - Each document is extracted separately
    - Documents are combined with clear separators (Document 1, Document 2, etc.)
    - Combined text is passed to the analyzer for comprehensive analysis
    - Summary covers all documents together
    
    **Output Structure:**
    1. extracted_text: Full combined text from all documents with JSON tables (cleaned and normalized)
    2. structured_analysis: JSON object with individual fields
    3. formatted_response: Single formatted text combining all non-empty fields (for frontend display)
    4. metadata: File and processing information for all documents
    
    **formatted_response structure:**
    - PARTY IDENTIFICATION (if sender/recipient present)
    - DOCUMENT SUMMARY (always present - covers all documents)
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
    
    temp_files = []
    
    try:
        # Supported file extensions
        supported = ['.pdf', '.docx', '.pptx', '.xlsx', '.html', '.png', '.jpg', '.jpeg', '.tiff', '.bmp']
        
        # Validate and save all uploaded files
        file_paths = []
        filenames = []
        total_size = 0
        
        for file in files:
            filename = file.filename.lower()
            
            # Validate file extension
            if not any(filename.endswith(ext) for ext in supported):
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported file format for '{file.filename}'. Supported: {', '.join(supported)}"
                )
            
            # Save uploaded file temporarily
            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp:
                temp_file = tmp.name
                temp_files.append(temp_file)
                shutil.copyfileobj(file.file, tmp)
                
                file_paths.append(temp_file)
                filenames.append(file.filename)
                total_size += os.path.getsize(temp_file)
        
        # Step 1: Extract text from all documents and combine them
        print(f"Extracting text from {len(files)} document(s)...")
        if len(file_paths) == 1:
            # Single file - extract normally
            extracted_text = doc_processor.extract_text(file_paths[0])
        else:
            # Multiple files - extract and combine
            extracted_text = doc_processor.extract_text_from_multiple_files(file_paths, filenames)
        
        if not extracted_text.strip():
            raise HTTPException(
                status_code=422,
                detail="No text could be extracted from the document(s)"
            )
        
        # Step 2: Analyze with Qwen3-Next-80B-A3B (with 32k max tokens)
        print(f"Analyzing document(s)...")
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
                "num_files": len(files),
                "filenames": [f.filename for f in files],
                "content_types": [f.content_type for f in files],
                "total_size_bytes": total_size,
                "llm_model": "qwen.qwen3-next-80b-a3b",
                "max_tokens": 32000
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")
    
    finally:
        # Cleanup all temp files
        for temp_file in temp_files:
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
        "service": "Legal Document Analysis API - Optimized with Multi-File Support",
        "version": "5.2.0",
        "llm_model": "Qwen3-Next-80B-A3B",
        "max_tokens": 32000,
        "features": ["single_file", "multi_file", "32k_tokens", "improved_text_extraction"]
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
        ],
        "multi_file_support": True,
        "note": "You can upload multiple files at once. They will be combined and analyzed together."
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
            "extracted_text": "Full combined text from all documents with [TABLE_JSON] sections (cleaned and normalized)",
            "structured_analysis": "JSON object with individual fields (clean, no empty fields)",
            "formatted_response": "Single clean text merging all non-empty fields (ready for frontend display)",
            "metadata": "File and processing information for all documents"
        },
        "structured_analysis_fields": {
            "sender": "Present only if explicitly mentioned in document(s)",
            "recipient": "Present only if explicitly mentioned in document(s)",
            "summary": "Always present - comprehensive 4-7 sentence summary of entire document(s)",
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
                "Summary text covering all documents...",
                "",
                "Answer: ... (if user asked question)",
                "",
                "Issues: (if issues present)",
                "1. Issue one",
                "2. Issue two",
                "",
                "Should I prepare the reply or guide for these issues?"
            ]
        },
        "multi_file_processing": {
            "description": "When multiple files are uploaded",
            "process": [
                "1. Each document is extracted separately",
                "2. Documents are combined with clear separators",
                "3. Combined text shows: DOCUMENT 1: filename.pdf, DOCUMENT 2: filename2.pdf, etc.",
                "4. Analyzer processes all documents together",
                "5. Summary and analysis cover all documents comprehensively"
            ],
            "example_separator": "================================================================================\nDOCUMENT 1: notice.pdf\n================================================================================\n\n[extracted text from notice.pdf]\n\n\n================================================================================\nDOCUMENT 2: reply.pdf\n================================================================================\n\n[extracted text from reply.pdf]"
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
    print("LEGAL DOCUMENT ANALYSIS API - OPTIMIZED WITH MULTI-FILE SUPPORT")
    print("="*70)
    print("Version: 5.2.0")
    print("LLM Model: Qwen3-Next-80B-A3B-Instruct")
    print("\nFeatures:")
    print("  ✓ Multi-file upload support (analyze multiple documents together)")
    print("  ✓ Improved text extraction with better normalization")
    print("  ✓ Increased max tokens to 32,000 (from 4,096)")
    print("  ✓ Comprehensive summaries (4-7 sentences covering entire document)")
    print("  ✓ Smart issue detection (only genuine allegations/violations)")
    print("  ✓ Clean response structure (no empty fields)")
    print("  ✓ Row-wise JSON table extraction")
    print("  ✓ Conditional party identification")
    print("\nMulti-File Processing:")
    print("  • Upload 1 or more documents at once")
    print("  • Each document is extracted separately")
    print("  • Documents are combined with clear separators")
    print("  • Analysis covers all documents comprehensively")
    print("\nIssue Detection:")
    print("  • Includes: Allegations, violations, penalties, legal cases")
    print("  • Excludes: Routine notices, administrative discrepancies")
    print("="*70)
    print("Endpoints:")
    print("  POST /analyze-document  (accepts single or multiple files)")
    print("  GET  /health")
    print("  GET  /supported-formats")
    print("  GET  /issue-detection-criteria")
    print("  GET  /response-structure")
    print("  GET  /docs")
    print("="*70 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)