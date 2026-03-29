# Ingestion Service — File Structure

```
ingestion_service/
├── api/
│   ├── main.py               ← FastAPI app factory + lifespan
│   ├── auth.py               ← JWT issue + verify
│   ├── deps.py               ← FastAPI dependency injectors
│   ├── models.py             ← Pydantic request/response models
│   └── routes/
│       ├── auth.py           ← POST /auth/login
│       ├── chunks.py         ← POST /chunks/submit, /chunks/autofill, GET /chunks/schema/{type}
│       └── jobs.py           ← GET /jobs/{job_id}
├── worker/
│   ├── celery_app.py         ← Celery instance
│   ├── tasks.py              ← ingest_chunk Celery task
│   └── supersession.py       ← SupersessionEngine
├── schemas/
│   └── chunk_type_specs.py   ← All 21 type specs (authority, fields, dedup, supersession)
├── autofill/
│   ├── prompt_builder.py     ← Builds per-type Bedrock prompt
│   └── bedrock_caller.py     ← Calls Bedrock, parses JSON, retries
├── tests/
│   └── test_cgst_section.py  ← End-to-end test for cgst_section
├── config.py                 ← Ingestion-service config (extends pipeline config)
├── .env.example
├── docker-compose.yml
└── requirements.txt
```

Shared pipeline files (from your existing codebase — mounted read-only):
  pipeline/bm25_vectorizer.py
  pipeline/keyword_merger.py
  pipeline/layer1_extractor.py
  pipeline/layer3_qwen.py
  pipeline/regex_fallback.py
  pipeline/file_tracker.py
  models/embedding_generator.py
  models/qdrant_manager.py      ← +update_payload() +search_by_payload() added
  config.py                     ← base pipeline config
  utils/