import os
import logging
from fastapi import FastAPI, BackgroundTasks, HTTPException
from apps.worker.src.worker_service import process_doc_worker

app = FastAPI(title="Taxobuddy-Worker")
logger = logging.getLogger(__name__)

@app.get("/health")
def health(): return {"status": "worker alive"}

@app.post("/process/{doc_id}")
async def trigger_processing(doc_id: int, background_tasks: BackgroundTasks):
    """
    Trigger document processing. This endpoint is called by the Chat API.
    """
    logger.info(f"Worker received request for doc_id: {doc_id}")
    background_tasks.add_task(process_doc_worker, doc_id)
    return {"status": "accepted", "doc_id": doc_id}

@app.get("/status/{doc_id}")
async def get_status(doc_id: int):
    # This might not be needed if API polls the shared Postgres DB directly, 
    # but good for direct worker health/status checks.
    return {"status": "check postgres for status"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
