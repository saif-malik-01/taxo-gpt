import os
import sys
import logging
import asyncio
from sqlalchemy import create_engine, text
from sqlalchemy.engine import url

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("db_manager")

def get_base_url(original_url: str):
    """Returns the database URL pointing to the 'postgres' database."""
    # Convert async driver to sync for management tasks if needed
    sync_url = original_url.replace("+asyncpg", "").replace("+aiopg", "")
    parsed = url.make_url(sync_url)
    target_db = parsed.database
    
    # Create base URL pointing to default 'postgres' database
    base_url = parsed.set(database="postgres")
    return str(base_url), target_db

def create_db_if_not_exists():
    db_url = os.getenv("DATABASE_URL")
    
    # Debug: log the url with masked password
    if db_url:
        masked_url = db_url.split('@')[-1] if '@' in db_url else db_url
        logger.info(f"Received DATABASE_URL: ...@{masked_url}")
    else:
        logger.warning("DATABASE_URL is not set!")

    if not db_url or db_url.strip() == "":
        logger.error("DATABASE_URL environment variable is not set or empty!")
        return

    try:
        base_url, target_db = get_base_url(db_url)
        logger.info(f"Connecting to Postgres to check/create database: {target_db}")
        
        # Connect to 'postgres' database
        engine = create_engine(base_url, isolation_level="AUTOCOMMIT")
        
        with engine.connect() as conn:
            # Check if target database exists
            query = text(f"SELECT 1 FROM pg_database WHERE datname = '{target_db}'")
            result = conn.execute(query).fetchone()
            
            if not result:
                logger.info(f"Database '{target_db}' not found. Creating it...")
                conn.execute(text(f"CREATE DATABASE {target_db}"))
                logger.info(f"Database '{target_db}' created successfully.")
            else:
                logger.info(f"Database '{target_db}' already exists.")
        
        engine.dispose()
    except Exception as e:
        logger.error(f"Error during database creation check: {e}")
        # We don't exit(1) here because maybe the user already created it 
        # or permissions don't allow CREATE DATABASE (RDS master user needed).
        # We let the migrations try to run anyway.

if __name__ == "__main__":
    create_db_if_not_exists()
