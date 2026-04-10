# Release A Reconciliation Log

## Baseline
- Clean worktree: `/tmp/telegram-main-clean`
- Starting stability commit: `98185fe`
- Mandatory included bugfix: `c717f87`

## Preserved from the Clean Baseline
- Single-service production posture with `APP_ROLE=all`
- Historical dashboard fast path for uncached historical ranges
- Persisted dashboard snapshot behavior
- Request/background executor split via `api/runtime_executors.py`
- Existing scheduler and runtime stability test coverage
- Existing materializer gating posture, with recurring card materializers remaining off
- Existing mixed-load probe and schema utility scripts:
  - `scripts/probe_mixed_load.py`
  - `scripts/ensure_neo4j_schema.py`

## Imported from the Dirty Workspace
- CI hardening in `.github/workflows/ci.yml`:
  - dependency install from `requirements-dev.txt`
  - secret hygiene
  - focused linting
  - backend tests with coverage
  - aggregate `quality-gate`
- Reusable smoke tooling:
  - `scripts/run_smoke_checks.py`
  - `.github/workflows/deployment-smoke.yml`
  - `.github/workflows/post-deploy-warmup.yml`
- Local QA parity in `Makefile`
- Dev tooling manifests:
  - `requirements-dev.txt`
  - `pytest.ini`
- Documentation updates aligned to current Release A posture:
  - `README.md`
  - `docs/production_runbook.md`

## Explicitly Rejected for Release A
- Runtime coordinator / Redis boot blocking
- `api.worker`
- Operator/admin auth enforcement changes
- Frontend Supabase admin auth
- Locked-environment startup validation
- Any documentation that describes `web + worker + Redis` as already-live production reality
- Any change that removes runtime executor separation
- Any change that starts recurring materializers unconditionally
- Frozen feature commits outside the stabilization line:
  - `8c55b22`
  - `9a6d810`

## Release A Verification Snapshot
- Secret hygiene: passed
- Focused lint: passed
- Backend tests: `47 passed`
- Backend coverage: `28.17%`
- Frontend build: passed
- Thread-anchor production fix from `c717f87`: included and resolved onto the `98185fe` baseline
