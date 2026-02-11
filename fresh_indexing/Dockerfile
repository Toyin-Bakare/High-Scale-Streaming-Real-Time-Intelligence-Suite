FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY tests ./tests
COPY README.md .

EXPOSE 8000

# Start indexer in background, then run API in foreground
CMD ["bash", "-lc", "python -m app.seed && (python -m app.indexer &) && uvicorn app.api:app --host 0.0.0.0 --port 8000"]
