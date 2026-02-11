import os
from pydantic import BaseModel

class Settings(BaseModel):
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql+psycopg2://cdc:cdc@localhost:5432/cdc")
    KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    TOPIC_ORDERS: str = os.getenv("TOPIC_ORDERS", "cdc.orders")
    WAREHOUSE_DIR: str = os.getenv("WAREHOUSE_DIR", "warehouse")
    DUCKDB_PATH: str = os.getenv("DUCKDB_PATH", "warehouse/warehouse.duckdb")

settings = Settings()
