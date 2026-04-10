.PHONY: setup-backend setup-frontend run-api run-frontend lint-backend test-backend build-frontend smoke-check qa qa-backend qa-frontend

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
	venv/bin/python -m compileall api buffer scraper processor ingester scripts tests
	venv/bin/ruff check --select F,E9 \
		config.py \
		api/server.py \
		api/aggregator.py \
		api/runtime_executors.py \
		api/queries/comparative.py \
		buffer/supabase_writer.py \
		tests/test_analytics_auth.py \
		tests/test_dashboard_persisted_cache.py \
		tests/test_dashboard_refresh_wait.py \
		tests/test_runtime_persistence.py \
		tests/test_runtime_stability.py \
		tests/test_server_runtime_roles.py \
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
		--cov-fail-under=20

qa-backend: lint-backend test-backend

build-frontend:
	npm --prefix frontend run build

qa-frontend: build-frontend

smoke-check:
	venv/bin/python scripts/run_smoke_checks.py --wait-ready

qa: qa-backend qa-frontend
