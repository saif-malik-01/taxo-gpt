"""
services/document/s3_storage.py

Unified AWS S3 storage service for Feature 2/3.
Handles permanent document persistence, partitioned by user and session.
"""

import boto3
import logging
from botocore.config import Config
from typing import Optional
from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)

# --- S3 Client Singleton ---
_s3_client = None

def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        cfg = Config(
            region_name=settings.AWS_REGION,
            retries={"max_attempts": 3, "mode": "standard"},
            max_pool_connections=50
        )
        _s3_client = boto3.client(
            "s3",
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            config=cfg
        )
    return _s3_client


import uuid

async def upload_document_to_s3(
    file_obj, 
    user_id: int, 
    session_id: str, 
    filename: str
) -> str:
    """
    Streams a file directly to the permanent AWS S3 storage.
    Returns the 's3_key' (path) to be stored in the session context.
    Each file is given a unique prefix to prevent overwrites within a session.
    """
    s3 = _get_s3_client()
    unique_prefix = uuid.uuid4().hex[:8]
    s3_key = f"docs/{user_id}/{session_id}/{unique_prefix}_{filename}"
    
    try:
        # We use upload_fileobj for efficient streaming
        s3.upload_fileobj(
            file_obj,
            settings.AWS_S3_BUCKET,
            s3_key,
            ExtraArgs={"ContentType": "application/pdf"} if filename.lower().endswith(".pdf") else {}
        )
        logger.info(f"Successfully uploaded {filename} to S3 bucket {settings.AWS_S3_BUCKET}")
        return s3_key
    except Exception as e:
        logger.error(f"S3 Upload Error: {e}")
        raise e


def generate_presigned_view_url(s3_key: str, expiration: int = 3600) -> Optional[str]:
    """
    Generates a secure, time-limited link for the frontend to view/download a notice.
    Default expiration is 1 hour.
    """
    s3 = _get_s3_client()
    try:
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": settings.AWS_S3_BUCKET, "Key": s3_key},
            ExpiresIn=expiration
        )
        return url
    except Exception as e:
        logger.error(f"Failed to generate S3 Presigned URL: {e}")
        return None


async def delete_document_from_s3(s3_key: str) -> bool:
    """Removes a file from S3 (e.g. if a user deletes a session)."""
    s3 = _get_s3_client()
    try:
        s3.delete_object(Bucket=settings.AWS_S3_BUCKET, Key=s3_key)
        return True
    except Exception as e:
        logger.error(f"Failed to delete {s3_key} from S3: {e}")
        return False
