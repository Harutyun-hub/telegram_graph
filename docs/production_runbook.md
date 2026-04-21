# Production Runbook

## Runtime Shape
- `frontend`: existing Railway static/proxy deployment
- `web`: `uvicorn api.server:app --host 0.0.0.0 --port $PORT`
- `worker`: `python -m api.worker`
- `social-worker`: `python -m api.social_worker`
- `redis`: managed Redis required in staging and production

## Canonical Stage 1 Environment Split
Use the split below as the source of truth for the hardened Stage 1 deployment.

### Web service
Required:
- `APP_ROLE=web`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `NEO4J_URI`
- `NEO4J_USERNAME`
- `NEO4J_PASSWORD`
- `NEO4J_DATABASE`
- `REDIS_URL`
- `ADMIN_API_KEY`
- analytics auth and frontend-facing API keys required by the web app

Must not be present on `web`:
- any `TELEGRAM_*` runtime credentials
- worker-only scraper throughput variables
- worker-only background feature toggles
- `ALLOW_STAGING_SOCIAL_WORKER`

Recommended web-only hardening:
- `RUN_STARTUP_WARMERS=false`

### Worker service
Required:
- `APP_ROLE=worker`
- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `TELEGRAM_SESSION_STRING`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `NEO4J_URI`
- `NEO4J_USERNAME`
- `NEO4J_PASSWORD`
- `NEO4J_DATABASE`
- `REDIS_URL`

Canonical worker flags and limits:
- `FEATURE_SOURCE_RESOLUTION_QUEUE=true`
- `FEATURE_SOURCE_RESOLUTION_WORKER=true`
- `FEATURE_SOURCE_PEER_REF_LOOKUP=true`
- `RUN_STARTUP_WARMERS=false`
- `SCRAPE_SKIP_WHEN_BACKLOG=true`
- `SCRAPER_CONTROL_POLL_SECONDS=5`
- `AI_NORMAL_COMMENT_LIMIT=120`
- `AI_NORMAL_POST_LIMIT=50`
- `AI_NORMAL_SYNC_LIMIT=160`
- `AI_CATCHUP_COMMENT_LIMIT=220`
- `AI_CATCHUP_POST_LIMIT=120`
- `AI_CATCHUP_SYNC_LIMIT=320`
- `AI_PROCESS_STAGE_MAX_SECONDS=1800`
- `AI_SYNC_STAGE_MAX_SECONDS=1800`
- `OPENAI_CIRCUIT_BREAKER_ENABLED=true`
- `NEO4J_CONNECTION_ACQUISITION_TIMEOUT_SECONDS=15`
- `NEO4J_MAX_TRANSACTION_RETRY_TIME_SECONDS=30`

Operational rule:
- only the worker may touch Telegram
- the web service stays Telegram-blind and passive

### Social worker service
Required:
- `APP_ROLE=social-worker`
- `SOCIAL_RUNTIME_ENABLED=true`
- `SOCIAL_SUPABASE_URL`
- `SOCIAL_SUPABASE_SERVICE_ROLE_KEY`
- `SOCIAL_DATABASE_URL` if direct Postgres leasing is enabled
- `SOCIAL_NEO4J_URI`
- `SOCIAL_NEO4J_USERNAME`
- `SOCIAL_NEO4J_PASSWORD`
- `SOCIAL_NEO4J_DATABASE`
- `OPENAI_API_KEY`
- `SCRAPECREATORS_API_KEY`
- `REDIS_URL`

Canonical social-worker flags and limits:
- `RUN_STARTUP_WARMERS=false`
- `SOCIAL_CONTROL_POLL_SECONDS=5`
- `ALLOW_STAGING_SOCIAL_WORKER=true` only during approved staging validation windows

Must not be present on `social-worker`:
- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `TELEGRAM_PHONE`
- `TELEGRAM_SESSION_STRING`
- Telegram-only throughput or login flags

Operational rule:
- only the `social-worker` may run social collect, analysis, replay, retry, and graph sync in production-shaped deployments
- `web` serves social status and queues control commands through `social_runtime_settings`
- social data must continue to flow only through the social operational store and the separate social graph

## GitHub Environments and Required Checks
- Create GitHub environments named `staging` and `production`.
- Store these environment secrets in both where applicable:
  - `DEPLOY_BASE_URL`
  - `ANALYTICS_API_KEY_FRONTEND`
  - `ADMIN_API_KEY`
- Protect `main` with the `quality-gate` status check from [ci.yml](/Users/harutnahapetyan/Documents/Gemini/Telegram/.github/workflows/ci.yml).
- Use [deployment-smoke.yml](/Users/harutnahapetyan/Documents/Gemini/Telegram/.github/workflows/deployment-smoke.yml) for manual staging or production smoke verification.

## Locked Environment Rules
- `ANALYTICS_API_REQUIRE_AUTH=true`
- `CORS_ALLOW_ORIGINS` must not include `*`
- `REDIS_URL` must be configured
- `ADMIN_API_KEY` must be configured
- AI helper admin binding must be configured before boot
- `ENABLE_DEBUG_ENDPOINTS=false` unless an explicit incident/debug window is approved

## Staging Data Policy
- Do not connect staging to live Telegram scraping
- Do not reuse the production Telegram session in staging
- Seed staging from a sanitized production-derived export
- Remove or hash user identifiers and any sensitive content before loading staging data
- Keep the staging worker manual-only for this milestone
- Keep the staging social-worker disabled by default; enable it only for explicit validation windows with `ALLOW_STAGING_SOCIAL_WORKER=true`

## Release Flow
1. Merge only after CI is green.
2. Deploy staging.
3. Run staging smoke checks:
   - `/readyz`
   - `/api/dashboard`
   - `/api/topics`
   - `/api/freshness`
   - `/api/scraper/scheduler` with `ADMIN_API_KEY`
   - `/api/social/runtime/status` with `ADMIN_API_KEY`
4. Run a short worker soak in staging and confirm:
   - no duplicate cycle execution
   - no escalating error rate
   - no freshness regression
5. Run a short social-worker soak in staging and confirm:
   - `web` stays passive for social runtime
   - `social-worker` persists a shared runtime snapshot
   - `run-once` issued through web is consumed by `social-worker`
   - no duplicate activity rows or graph writes on rerun
6. Approve production deployment.
7. Deploy production.
8. Run post-deploy smoke/warmup workflow.
9. Tag the release and record the rollback target.

Local/manual smoke command:

```bash
DEPLOY_BASE_URL=https://your-app.example.com \
ANALYTICS_API_KEY_FRONTEND=... \
ADMIN_API_KEY=... \
python scripts/run_smoke_checks.py --wait-ready --label staging
```

## Migration Policy
- `supabase/migrations/` is the executable source of truth for database changes.
- Migration policy is fix-forward only.
- Do not use down migrations in staging or production.
- If staging migration fails:
  - stop rollout immediately
  - write a corrective forward migration
  - restore from backup only if data integrity is at risk
- Production is blocked until the exact migration set succeeds on staging.

## Backup and Restore
- Supabase PITR or scheduled backups must be enabled.
- Neo4j automated backups or snapshots must be enabled.
- Retention policy must be documented before calling the system production ready.
- Restore drill requirement:
  - restore Supabase to staging
  - restore Neo4j to staging
  - rerun smoke checks
  - record timing, gaps, and operator notes

## Rollback
- Record for every production deploy:
  - release git tag
  - previous git tag
  - Railway deploy identifier for `frontend`, `web`, `worker`, and `social-worker`
  - any env var changes made during the release
- Rollback steps:
  1. Re-deploy the previous Railway release for each service.
  2. Revert any env var changes made in the failed release.
  3. If a migration caused data corruption, restore from the latest safe backup and re-run smoke checks.
  4. Validate `/readyz`, `/api/dashboard`, `/api/freshness`, `/api/social/runtime/status`, and one operator route before reopening access.
