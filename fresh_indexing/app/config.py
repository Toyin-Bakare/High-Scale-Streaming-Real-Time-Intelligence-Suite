import os
from pydantic import BaseModel

class Settings(BaseModel):
    DB_PATH: str = os.getenv("DB_PATH", "fresh_indexing.sqlite3")

    # Freshness SLO (seconds) for demo. In prod you'd do per-dataset SLOs.
    FRESHNESS_SLO_SECONDS: int = int(os.getenv("FRESHNESS_SLO_SECONDS", "300"))  # 5 minutes

    # Indexer batch size
    CONSUMER_BATCH_SIZE: int = int(os.getenv("CONSUMER_BATCH_SIZE", "200"))

settings = Settings()
