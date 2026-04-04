.PHONY: setup-backend setup-frontend run-api run-frontend lint-backend test-backend test-frontend build-frontend smoke-check qa qa-backend qa-frontend

setup-backend:
	python3 -m venv venv
	. venv/bin/activate && pip install -r requirements.txt -r requirements-dev.txt

setup-frontend:
	npm --prefix frontend ci

run-api:
	. venv/bin/activate && python -m uvicorn api.server:app --reload --port 8001

run-frontend:
	npm --prefix frontend run dev

lint-backend:
	venv/bin/ruff check --select F,E9 \
		config.py \
		api/server.py \
		api/runtime_coordinator.py \
		api/scraper_scheduler.py \
		api/worker.py \
		tests/test_ai_helper.py \
		tests/test_analytics_auth.py \
		tests/test_operator_auth.py \
		tests/test_runtime_hardening.py \
		scripts/check_secret_hygiene.py \
		scripts/run_smoke_checks.py

test-backend:
	venv/bin/pytest \
		--cov=api \
		--cov=buffer \
		--cov=scraper \
		--cov=processor \
		--cov=ingester \
		--cov-report=term-missing \
		--cov-fail-under=25

qa-backend: lint-backend test-backend

test-frontend:
	npm --prefix frontend run test

build-frontend:
	npm --prefix frontend run build

qa-frontend:
	$(MAKE) test-frontend
	$(MAKE) build-frontend

smoke-check:
	venv/bin/python scripts/run_smoke_checks.py --wait-ready

qa: qa-backend qa-frontend
