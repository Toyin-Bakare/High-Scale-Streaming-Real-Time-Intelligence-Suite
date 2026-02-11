from fastapi import FastAPI
from app.db import init_db
from app.monitor import get_dataset_metrics

app = FastAPI(title="Freshness Monitor API")

@app.on_event("startup")
def _startup():
    init_db()

@app.get("/health")
def health():
    datasets = ["cases", "kb"]
    metrics = [get_dataset_metrics(d) for d in datasets]
    ok = all(m["ok"] for m in metrics)
    return {"ok": ok, "datasets": metrics}

@app.get("/metrics")
def metrics():
    datasets = ["cases", "kb"]
    return {"datasets": [get_dataset_metrics(d) for d in datasets]}
