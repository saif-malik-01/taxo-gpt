import logging
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import sessionmaker
from apps.api.src.db.models.base import SessionDocumentText
from apps.worker.src.processor import WorkerProcessor
from apps.api.src.services.document.s3_storage import s3_storage
import tempfile
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/taxobuddy")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

logger = logging.getLogger(__name__)
processor = WorkerProcessor()

def process_doc_worker(doc_id: int):
    """Actual worker task to process a document."""
    db = SessionLocal()
    try:
        # 1. Fetch record
        doc = db.get(SessionDocumentText, doc_id)
        if not doc:
            logger.error(f"Doc {doc_id} not found")
            return

        doc.status = "processing"
        db.commit()

        # 2. Download from S3
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(doc.filename)[1]) as tmp:
            tmp_path = tmp.name
        
        if not s3_storage.download_file(doc.s3_key, tmp_path):
            raise Exception("S3 Download failed")

        # 3. Process
        logger.info(f"Processing doc {doc_id} from {tmp_path}")
        text = processor.process_file(tmp_path)
        os.unlink(tmp_path)

        # 4. Save results
        doc.extracted_text = text
        doc.status = "completed"
        db.commit()
        logger.info(f"Doc {doc_id} processed successfully")

    except Exception as e:
        logger.error(f"Worker process failed for doc {doc_id}: {e}")
        if doc:
            doc.status = "error"
            db.commit()
    finally:
        db.close()
