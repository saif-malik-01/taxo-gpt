import os
import json
import base64
import boto3
import re
import logging
import tempfile
from io import BytesIO
from typing import List, Optional
from PIL import Image, ImageOps
from concurrent.futures import ThreadPoolExecutor, as_completed

# We assume the worker will have its own env vars for AWS
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
MODEL_ID = "amazon.nova-lite-v1:0"

logger = logging.getLogger(__name__)

class AmazonNovaClient:
    def __init__(self, model_id: str = MODEL_ID):
        self.model_id = model_id
        self.client = boto3.client('bedrock-runtime', region_name=AWS_REGION)

    def describe_image(self, pil_image: Image.Image, prompt: str = None) -> str:
        max_size = 2048
        if max(pil_image.size) > max_size:
            pil_image = pil_image.copy()
            pil_image.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)

        buffered = BytesIO()
        pil_image.save(buffered, format="PNG")
        img_base64 = base64.b64encode(buffered.getvalue()).decode("utf-8")

        if prompt is None:
            prompt = "Extract all text VERBATIM from this document page. Preserve reading order."

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
            return ""
        except Exception as e:
            logger.error(f"Nova error: {e}")
            return f"[Error: {str(e)}]"

def _convert_to_images(file_path: str, dpi: int = 300) -> List[Image.Image]:
    ext = os.path.splitext(file_path)[1].lower()
    if ext in ('.png', '.jpg', '.jpeg', '.tiff', '.tif', '.bmp', '.gif', '.webp'):
        return [Image.open(file_path).convert("RGB")]
    
    if ext == '.pdf':
        import fitz
        doc = fitz.open(file_path)
        images = []
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        for page in doc:
            pix = page.get_pixmap(matrix=mat, alpha=False)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            images.append(img)
        doc.close()
        return images
    raise ValueError(f"Unsupported format: {ext}")

class WorkerProcessor:
    def __init__(self, max_workers: int = 5):
        self.vision = AmazonNovaClient()
        self.max_workers = max_workers

    def process_file(self, local_path: str) -> str:
        images = _convert_to_images(local_path)
        page_results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.vision.describe_image, img): idx for idx, img in enumerate(images)}
            for future in as_completed(futures):
                idx = futures[future]
                page_results[idx] = future.result()
        
        content = [page_results.get(i, "") for i in range(len(images))]
        return "\n\n".join(content)
