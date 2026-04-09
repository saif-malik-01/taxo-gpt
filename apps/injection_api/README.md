# Taxo GPT — Injection API & Ingestion Worker

The **Injection API** is a FastAPI-based ingestion service that manages the live addition, autofilling, and processing of legal documents (GST chunks) into Qdrant. It comes paired with a highly concurrent **Celery Worker** that handles the computationally expensive pipeline (L1/L3 extraction, BM25 computation, Titan embedding, and Supersession).

## 🏗 Architecture

1. **FastAPI (Frontend Gateway):** 
   - Provides chunk-schema definitions and dynamic form rendering support.
   - Dedup checking (live checking of citations, section numbers, etc.).
   - AI Autofill endpoint (using AWS Bedrock `Amazon Nova Pro`) to populate complex metadata before submission.
2. **Celery Worker (Asynchronous Pipeline):**
   - **BM25 Sparse Vectorization:** Computes and saves corpus statistics across all ingested chunks.
   - **Layer 1 & 3 Extraction:** Standardizes metadata, extracts entities, and creates the sparse/keyword representation.
   - **Titan Embedding:** Uses AWS Bedrock (`amazon.titan-embed-text-v2:0`) to generate dense dense representations.
   - **Supersession Engine:** Checks if the incoming chunk overrides passing case law, amends a section, or drops an old notification, mutating old Qdrant chunks dynamically.
   - **Qdrant Upsert:** Commits to the vector DB.

## 🚀 Setup & Installation

### 1. Requirements

- Python 3.10+
- Redis (Broker for Celery)
- Qdrant (Local or Cloud)
- AWS Credentials with Bedrock access (Nova Pro & Titan v2)

### 2. Install Dependencies

```bash
cd apps/injection_api
python -m venv venv
venv\Scripts\activate   # (Windows)
python -m pip install -r requirements.txt
```

### 3. Environment Variables

Copy the `.env.example` file to create your local `.env`:

```bash
cp .env.example .env
```

You must update the following in `.env`:
- `JWT_SECRET_KEY` & `ADMIN_PASSWORD_HASH`
- `AWS_ACCESS_KEY_ID` & `AWS_SECRET_ACCESS_KEY`
- `QDRANT_HOST` / `QDRANT_PORT`

To generate a new secure password hash for the `admin` user:
```bash
venv\Scripts\python.exe -c "import bcrypt; print(bcrypt.hashpw(b'YOUR_NEW_PASSWORD', bcrypt.gensalt()).decode())"
```

## 🛠 Running the System

You must run **both** the API and the Worker simultaneously in two separate terminals.

### Terminal 1: FastAPI API

```bash
venv\Scripts\uvicorn.exe main:app --host 0.0.0.0 --port 8000 --reload
```
API Documentation will be securely available at: `http://localhost:8000/docs`

### Terminal 2: Celery Worker
> **NOTE FOR WINDOWS USERS:** Windows does not support Celery's default `prefork` process pool (you will get a `[WinError 5] Access is denied` error). You must use the `--pool=solo` flag locally.

```bash
# Windows
venv\Scripts\celery.exe -A worker.celery_app worker --loglevel=info --pool=solo

# Linux / Mac / Production Docker
celery -A worker.celery_app worker --loglevel=info --concurrency=2
```

## 🌐 Core API Endpoints

All endpoints require JWT authorization (`Bearer <token>`).

### Auth
- **`POST /auth/login`**: Authenticate using `ADMIN_USERNAME` and password. Returns a JWT token.

### Schema & UI Forms
- **`GET /chunks/schema/{chunk_type}`**: Returns the schema definition (Anchor fields, Autofill fields) needed to render the form for a specific document type (e.g. `judgment`, `cgst_section`).

### Pre-computation
- **`GET /chunks/dedup/{chunk_type}?key_value=...`**: Live duplication check. Scopes the search strictly to the current `chunk_type` and returns duplicate warnings or supersession alerts.
- **`POST /chunks/autofill`**: Uses Amazon Nova Pro to parse the provided text and anchor fields, suggesting values for all remaining autofill fields.

### Execution
- **`POST /chunks/submit`**: Validates the schema, runs a hard dedup check, adds provenance, and queues the chunk into Celery. Returns a `job_id`.
- **`GET /jobs/{job_id}`**: Poll the status of the ingestion pipeline (`queued`, `processing`, `success`, `failed`).

## 🧠 Supersession Logic

The worker includes an active patching engine to manage the "Current" status of law. Examples:
- **Judgments:** If a Supreme Court case overrules a High Court case, ingesting the SC case with `ext.overrules_citation` automatically locates the HC chunk in Qdrant and sets `is_current: false`, `current_status: overruled`, and drops it from the primary RAG search index.
- **Statutory Law:** Amending `cgst_section` chunks drops the legacy sections down to historical status.
