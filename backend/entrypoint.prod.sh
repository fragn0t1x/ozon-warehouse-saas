#!/bin/sh
set -eu

echo "Running Alembic migrations..."
alembic upgrade heads

echo "Starting backend..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
