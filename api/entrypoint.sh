#!/bin/bash
set -e

echo "Running database migrations..."
python -m src.migrate

echo "Starting API server..."
exec uvicorn src.main:app --host 0.0.0.0 --port 8000 --log-config /app/src/log_config.json
