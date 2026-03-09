.PHONY: setup-backend setup-frontend run-api run-frontend qa qa-backend qa-frontend

setup-backend:
	python3 -m venv venv
	. venv/bin/activate && pip install -r requirements.txt

setup-frontend:
	npm --prefix frontend ci

run-api:
	. venv/bin/activate && python -m uvicorn api.server:app --reload --port 8001

run-frontend:
	npm --prefix frontend run dev

qa-backend:
	python3 -m compileall api buffer scraper processor ingester

qa-frontend:
	npm --prefix frontend run build

qa: qa-backend qa-frontend
