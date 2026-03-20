```markdown:docs/project_structure.md
## 📂 High-Level Overview

```text
root/
├── .github/              # CI/CD Workflows (GitHub Actions)
├── apps/                 # Application codebases
│   ├── api/              # Backend (FastAPI/Python)
│   └── worker/           # Background tasks (Ingestion/Processing)
├── infra/                # Infrastructure as Code (Terraform/K8s/Docker)
├── scripts/              # Dev scripts (seeding DB, migration triggers)
├── tests/                # Global E2E and Integration tests
└── docker-compose.yml    # Local orchestration
```

---

## 🐍 Backend Structure (`apps/api`)
Focuses on the **Service-Repository Pattern**.

```text
apps/api/
├── src/
│   ├── api/              # API Layer (Routes & Definitions)
│   │   ├── v1/           # Versioned routes
│   │   │   ├── chat.py   # Chat & streaming endpoints
│   │   │   └── docs.py   # Document upload/management
│   │   └── middleware/   # Auth, CORS, Logging
│   ├── core/             # Framework-level config
│   │   ├── config.py     # Pydantic settings / Env vars
│   │   ├── security.py   # JWT, hashing
│   │   └── constants.py  # Shared enums (e.g., Model names)
│   ├── db/               # Persistence Layer
│   │   ├── models/       # SQL Alchemy / Motor models
│   │   ├── repository/   # Generic CRUD operations
│   │   └── migrations/   # Alembic (SQL) or custom (NoSQL)
│   ├── services/         # CORE BUSINESS LOGIC
│   │   ├── rag/          # RAG Engine
│   │   │   ├── ingestion/ # Parsing, Chunking, Embedding
│   │   │   ├── retrieval/ # Search, Hybrid-search, Re-ranking
│   │   │   └── store/     # ChromaDB, Qdrant, or Pinecone wrappers
│   │   ├── llm/          # Model Wrappers
│   │   │   ├── openai.py
│   │   │   ├── prompts/   # .yaml or .jinja2 prompt templates
│   │   │   └── memory.py  # Chat history management
│   │   └── auth_service.py
│   ├── schemas/          # Pydantic DTOs (Request/Response validation)
│   └── main.py           # App entry point
├── tests/                # Unit & Integration tests
├── .env.example
├── pyproject.toml        # Dependency management (Poetry/Pipenv)
└── Dockerfile
```

---

## 🏗️ The RAG Ingestion Pipeline (`apps/worker`)
Decoupling document processing ensures the API stays responsive.

```text
apps/worker/
├── tasks/
│   ├── processing.py     # Chunking & Embedding logic
│   └── cleanup.py        # Expired document removal
├── loaders/              # Specialized parsers
│   ├── pdf_loader.py     # OCR / Layout analysis
│   └── markdown_loader.py
└── main.py               # Worker entry point (Celery/RQ)
```

---

## 📋 Naming Conventions

### 1. General Rules
- **Folders**: `snake_case` (Python).
- **Python Files**: `snake_case.py` (e.g., `user_service.py`).
- **Environment Variables**: `UPPER_SNAKE_CASE` (e.g., `OPENAI_API_KEY`).

### 2. File Suffixes
- **Interfaces/Schemas**: `*.schema.py` or `*.types.ts`.
- **Services**: `*.service.py` (contains complex business logic).
- **Repositories**: `*.repo.py` (abstracts database interactions).
- **Tests**: `test_*.py` or `*.test.tsx`.

---

## 🚀 Deployment & DevOps

| Directory | Purpose |
| :--- | :--- |
| `/infra/docker/` | Dockerfiles for dev, staging, and prod environment. |
| `/infra/terraform/` | Cloud provisioning (AWS/Azure/GCP). |
| `/.github/workflows/` | `ci.yml` (Lint/Test) and `cd.yml` (Build/Deploy). |
| `/scripts/` | `seed_vector_db.py`, `backup_db.sh`, `migrate.py`. |

---

## 🛡️ Best Practices for RAG Applications
1.  **Prompt Versioning**: Store prompts in a dedicated `prompts/` folder, never hardcoded in services. Use `.yaml` files with version tags.
2.  **Streaming**: Ensure `api/v1/chat.py` supports Server-Sent Events (SSE) for "typing" effects.
3.  **Evaluation**: Create a `tests/eval/` directory to store golden datasets and RAGAS metrics to track retrieval quality.
4.  **Audit Logs**: Implement middleware to log the `prompt` + `context` + `response` for every query to monitor hallucinations.
```