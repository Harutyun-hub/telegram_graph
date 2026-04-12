# Staging Runbook

## Purpose
- `staging` is the pre-production integration branch.
- It deploys to a separate Railway staging project and uses non-production data.
- Production stays on `main` after main realignment is complete.
- Current status: staging is operational, but it is **not yet trustworthy as pre-production** until it is isolated from production-grade data/services.
- Current temporary decision:
  - staging backend is still pointed at the same Supabase host and Neo4j cluster/database as production
  - isolation is deferred for now, so staging must be treated as provisional and non-destructive

## Current Branch Model
- `feature/*` -> `staging` -> `main`
- `hotfix/*` branches start from the current production branch until `main` is realigned.
- After realignment:
  - `main` is the only production branch
  - `staging` is the only pre-production integration branch

## Required GitHub Setup
- Create GitHub environments:
  - `staging`
  - `production`
- Protect branches:
  - `staging` requires `quality-gate`
  - `main` requires `quality-gate`
- Use the PR template in `.github/pull_request_template.md` for every merge into `staging` or `main`.

## Required Railway Setup
- Create a separate Railway staging project with:
  - `telegram_graph_staging`
  - `poetic_sparkle_staging`
- Keep the initial staging shape aligned with Release A:
  - one backend service
  - one frontend service
  - no worker
  - no Redis

## Day 1 Blocker
- Move staging off production-grade data/services before treating staging as a valid QA environment.
- Required isolation targets:
  - isolated staging Supabase/Postgres
  - isolated staging Neo4j
- Until this is complete:
  - do not trust staging writes
  - do not use staging as release evidence for `main`
  - do not start Release B validation
  - current exception: staging is temporarily left shared by decision, but that does not change the risk posture above

## Staging Secrets
- Repository owner is the default owner for all staging secrets until an ops owner exists.
- Required backend secrets:
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY`
  - `NEO4J_URI`
  - `NEO4J_USERNAME`
  - `NEO4J_PASSWORD`
  - `NEO4J_DATABASE`
  - `OPENAI_API_KEY` or `OpenAI_API`
  - `ANALYTICS_API_KEY_FRONTEND`
  - `ADMIN_API_KEY`
  - `SENTRY_DSN`
  - `SENTRY_ENVIRONMENT=staging`
- Telegram secrets are optional for staging only when scraper runtime is disabled.
- Required frontend secrets:
  - `BACKEND_URL`
  - `BACKEND_ANALYTICS_API_KEY_FRONTEND`
  - `VITE_SUPABASE_URL`
  - `VITE_SUPABASE_ANON_KEY`
- Frontend deployment note:
  - `BACKEND_URL` must be the Railway internal backend URL, not the public `up.railway.app` URL
  - `BACKEND_ANALYTICS_API_KEY_FRONTEND` must match backend `ANALYTICS_API_KEY_FRONTEND`
  - keep browser-facing API traffic on `/api` through Caddy reverse proxy

## Staging Data Policy
- Use separate staging Supabase and Neo4j targets.
- Do not reuse production Telegram sessions.
- Do not enable live scraping by default.
- Do not point staging services at production write targets.
- Default safe staging boot:
  - `APP_ROLE=web`
  - `RUN_STARTUP_WARMERS=false`
  - `ENABLE_SCRAPER_SCHEDULER=false`
  - `REQUIRE_TELEGRAM_CREDENTIALS=false`

## Merge Order
- Social merges into `staging` first.
- Graph merges into `staging` only after Social staging smoke passes.
- Do not merge Social and Graph together in one integration step.

## Observability
- Confirm Railway logs are accessible for both staging services.
- Backend Sentry should report with `SENTRY_ENVIRONMENT=staging`.
- Frontend Sentry should report with `VITE_SENTRY_ENVIRONMENT=staging`.
- Use `Deployment Smoke Checks` and `Staging Post-Deploy Warmup` after every staging deploy.

## Smoke Checklist
- `/readyz`
- `/api/dashboard`
- `/api/topics?page=0&size=100`
- `/api/freshness?force=true`
- Frontend loads and authenticates correctly

## Backup and Restore Ownership
- Repository owner owns restore checkpoints until responsibilities are split.
- Supabase backup:
  - PITR or scheduled backup must exist before schema-changing releases.
- Neo4j backup:
  - snapshot or provider backup must exist before schema-changing releases.
- Schema rollback is restore-based, not down-migration-based.

## Hotfix Path
- Before `main` realignment:
  - create `hotfix/*` from `codex/release-a-reconcile-20260328`
  - deploy urgent fix to production
  - merge hotfix into `staging`
  - after realignment, merge into `main`
- After `main` realignment:
  - create `hotfix/*` from `main`
  - merge back into `staging` after production release

## Main Realignment Trigger
- Realign `main` only after:
  - staging backend and frontend deploy with `SUCCESS`
  - staging smoke checks pass
  - staging observability is confirmed working
  - staging isolation from production-grade data/services is complete
