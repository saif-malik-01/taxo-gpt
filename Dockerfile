# Use Python 3.11 slim as base image
FROM python:3.11-slim-bullseye

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Create necessary directories for logs and cache
RUN mkdir -p logs apps/api/src/.hf_cache

# Expose the API port
EXPOSE 8000

# Health check using the health endpoint we built
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://127.0.0.1:8000/api/v1/health || exit 1

# Start the application
# 1. Create DB if not exists (scripts/db_manager.py)
# 2. Run migrations (alembic upgrade head) 
# 3. Start uvicorn
CMD ["sh", "-c", "python scripts/db_manager.py && alembic upgrade head && uvicorn apps.api.src.main:app --host 0.0.0.0 --port 8000"]