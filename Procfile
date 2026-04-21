web: uvicorn api.server:app --host 0.0.0.0 --port ${PORT:-8001}
worker: python -m api.worker
social-worker: python -m api.social_worker
