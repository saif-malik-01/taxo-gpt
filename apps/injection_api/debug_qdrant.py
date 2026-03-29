import os
import sys
from qdrant_client import QdrantClient
from flow.config import CONFIG

client = QdrantClient(
    host=CONFIG.qdrant.host,
    port=CONFIG.qdrant.port,
    api_key=CONFIG.qdrant.api_key,
    https=CONFIG.qdrant.https,
)

collection_name = CONFIG.qdrant.collection_name
print(f"DEBUG: Checking collection: {collection_name}")

try:
    collection_info = client.get_collection(collection_name)
    print(f"DEBUG: Collection info: {collection_info}")
    
    config = collection_info.config
    print(f"DEBUG: Vectors config: {config.params.vectors}")
    print(f"DEBUG: Sparse vectors config: {config.params.sparse_vectors}")
    
except Exception as e:
    print(f"DEBUG: Error getting collection: {e}")
