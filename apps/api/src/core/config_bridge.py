from apps.api.src.core.config import settings

class ConfigObject:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

class ConfigBridge:
    def __init__(self):
        # Qdrant Section
        self.qdrant = ConfigObject(
            host=settings.QDRANT_HOST,
            port=settings.QDRANT_PORT,
            api_key=settings.QDRANT_API_KEY,
            timeout=settings.QDRANT_TIMEOUT,
            collection_name=settings.QDRANT_COLLECTION,
            text_vector_name=settings.QDRANT_TEXT_VECTOR,
            sparse_vector_name=settings.QDRANT_SPARSE_VECTOR
        )

        # Bedrock Section
        self.bedrock = ConfigObject(
            region_name=settings.AWS_REGION,
            titan_model_id=settings.TITAN_MODEL_ID,
            titan_dimensions=settings.TITAN_DIMENSIONS,
            titan_normalize=settings.TITAN_NORMALIZE
        )

        # Pipeline Section
        self.pipeline = ConfigObject(
            max_retries=settings.PIPELINE_MAX_RETRIES,
            retry_delay_seconds=settings.PIPELINE_RETRY_DELAY,
            write_debug_tokens=False
        )

        # BM25 Section
        self.bm25 = ConfigObject(
            k1=settings.BM25_K1,
            b=settings.BM25_B,
            l1_weight=settings.BM25_L1_WEIGHT,
            l3_weight=settings.BM25_L3_WEIGHT
        )

        # Path Section
        self.paths = ConfigObject(
            corpus_stats_file=settings.CORPUS_STATS_FILE,
            chunks_dir="data/processed/chunks",
            tracker_file="data/processed/file_tracker.json",
            debug_tokens_dir="data/debug/tokens"
        )

CONFIG = ConfigBridge()
