# Railway Live Staging Inventory

This branch is a clean source snapshot for the currently working Radar staging app.
It is intended to be used as the canonical source for cloning the app into a
separate Railway project, such as a Finance demo.

Do not commit live secrets, database dumps, exported customer data, Telegram
session files, or Redis/Supabase/Neo4j contents to this branch.

## Source Assembly

The branch was assembled from the Railway live deployment inventory captured
from the `soothing-vitality` project, `staging` environment, on 2026-04-30.

| Area | Railway service | Source used |
| --- | --- | --- |
| Backend API | `loyal-presence` | `Harutyun-hub/telegram_graph`, `codex/prod-backend-staging-copy`, commit `3edc624ca7b96a92731d86dee64ba8ab0c4af2e8`, deployed 2026-04-30 13:26 UTC |
| Frontend | `romantic-acceptance` | `Harutyun-hub/telegram_graph`, `codex/prod-frontend-staging-copy`, commit `b10cf654adb4097751e6a4728c0e22c2702e50d1`, root `/frontend`, Dockerfile `/frontend/Dockerfile`, deployed 2026-04-30 15:04 UTC |
| Telegram worker | `telegram-worker-staging` | `Harutyun-hub/telegram_graph`, `main`, commit `a38a67886dcece019d2a710f88901c1db1d0c803`, start `python -m api.worker`, deployed 2026-04-30 13:06 UTC |
| Main worker | `loyal-presence-worker` | CLI-deployed service running `python -m api.worker` with `APP_ROLE=worker`; current deployment `6e6aea59-30da-47cd-a6cd-397e2b41badb` |
| Social worker | `social-worker` | CLI-deployed service running `python -m api.social_worker` with `APP_ROLE=social-worker`; current deployment `09b79648-68df-408b-8bc0-a3ba5ef43b5f` |
| Redis | `Redis` | Railway Redis image `redis:8.2.1` |

This branch uses the live backend API commit as the base because it contains
the worker and social-worker entrypoints. The live frontend `frontend/`
directory was overlaid from the live frontend commit.

## Railway Service Commands

The repository `Procfile` defines the runtime roles:

```procfile
web: uvicorn api.server:app --host 0.0.0.0 --port ${PORT:-8001}
worker: python -m api.worker
social-worker: python -m api.social_worker
```

For Railway:

| New service role | Railway source | Start command |
| --- | --- | --- |
| API | repository root | `uvicorn api.server:app --host 0.0.0.0 --port ${PORT:-8001}` |
| Worker | repository root | `python -m api.worker` |
| Social worker | repository root | `python -m api.social_worker` |
| Frontend | `/frontend` | Dockerfile `/frontend/Dockerfile` |
| Redis | Railway plugin/image | `redis:8.2.1` |

## Sanitized Env Names

These are variable names only. Values must be copied or regenerated in Railway.
For Finance, replace every data/runtime connection with Finance-owned services.

Railway-generated `RAILWAY_*` variables are intentionally omitted.

### Backend API: `loyal-presence`

```dotenv
APP_ROLE=web
ADMIN_API_KEY=
AI_CATCHUP_COMMENT_LIMIT=
AI_CATCHUP_POST_LIMIT=
AI_CATCHUP_SYNC_LIMIT=
AI_HELPER_ADMIN_SUPABASE_USER_ID=
AI_NORMAL_COMMENT_LIMIT=
AI_NORMAL_POST_LIMIT=
AI_NORMAL_SYNC_LIMIT=
ANALYTICS_API_KEY_FRONTEND=
ANALYTICS_API_KEY_OPENCLAW=
ANALYTICS_API_REQUIRE_AUTH=
ANALYTICS_RATE_LIMIT_ENABLED=
ANALYTICS_RATE_LIMIT_MAX_REQUESTS=
ANALYTICS_RATE_LIMIT_WINDOW_SECONDS=
CORS_ALLOW_ORIGINS=
DASH_V2_API_ENABLED=
DASH_V2_COMPARE_ENABLED=
DASH_V2_COMPARE_OLD_PATH_TIMEOUT_SECONDS=
DASH_V2_FACTS_ENABLED=
DASH_V2_FACT_LOOKBACK_DAYS=
DASH_V2_FRONTEND_READ_ENABLED=
DASH_V2_JOB_OWNER=
DETAIL_CACHE_TTL_SECONDS=
DETAIL_MAX_STALE_SECONDS=
DETAIL_REFRESH_TIMEOUT_SECONDS=
ENABLE_BEHAVIORAL_CARD_MATERIALIZER=
ENABLE_CARD_MATERIALIZERS=
ENABLE_OPPORTUNITY_CARD_MATERIALIZER=
ENABLE_QUESTION_CARD_MATERIALIZER=
ENABLE_SCRAPER_SCHEDULER=
GEMINI_API_KEY=
KB_STORAGE_PATH=
NEO4J_BACKGROUND_POOL_SIZE=
NEO4J_CONNECTION_ACQUISITION_TIMEOUT_SECONDS=
NEO4J_CONNECTION_TIMEOUT_SECONDS=
NEO4J_CONNECTION_WRITE_TIMEOUT_SECONDS=
NEO4J_DATABASE=
NEO4J_DEBUG_WATCH=
NEO4J_DRIVER_RESET_COOLDOWN_SECONDS=
NEO4J_LIVENESS_CHECK_TIMEOUT_SECONDS=
NEO4J_MAX_CONNECTION_LIFETIME_SECONDS=
NEO4J_MAX_TRANSACTION_RETRY_TIME_SECONDS=
NEO4J_PASSWORD=
NEO4J_REQUEST_POOL_SIZE=
NEO4J_URI=
NEO4J_USERNAME=
OPENCLAW_BRIDGE_AGENT_ID=
OPENCLAW_BRIDGE_BASE_URL=
OPENCLAW_BRIDGE_TOKEN=
OPENCLAW_GATEWAY_TRANSPORT=
OPENCLAW_HELPER_CONNECT_TIMEOUT_SECONDS=
OPENCLAW_HELPER_HTTP_MAX_BODY_BYTES=
OPENCLAW_HELPER_READ_TIMEOUT_SECONDS=
OPENCLAW_HELPER_REPLAY_MAX_CHARS=
OPENCLAW_HELPER_REPLAY_MAX_MESSAGES=
OPENCLAW_HELPER_RETRY_ATTEMPTS=
OPENCLAW_HELPER_TIMEOUT_SECONDS=
OPENCLAW_KB_SESSION_KEY=
OPENCLAW_WEB_SESSION_KEY=
OpenAI_API=
PIPELINE_DATABASE_URL=
PIPELINE_QUEUE_ENABLED=
REDIS_URL=
REQUIRE_TELEGRAM_CREDENTIALS=
RUN_STARTUP_WARMERS=
SCRAPECREATORS_API_KEY=
SIMPLE_AUTH_PASSWORD=
SIMPLE_AUTH_USERNAME=
SOCIAL_DATABASE_URL=
SOCIAL_NEO4J_DATABASE=
SOCIAL_NEO4J_PASSWORD=
SOCIAL_NEO4J_URI=
SOCIAL_NEO4J_USERNAME=
SOCIAL_SUPABASE_ANON_ROLE_KEY=
SOCIAL_SUPABASE_SERVICE_ROLE_KEY=
SOCIAL_SUPABASE_URL=
SUPABASE_SERVICE_ROLE_KEY=
SUPABASE_URL=
TOPIC_QUERY_V2=
```

### Worker: `loyal-presence-worker`

```dotenv
APP_ROLE=worker
NIXPACKS_START_CMD=python -m api.worker
STAGING_ENABLE_BACKGROUND_JOBS=
DASH_DEFAULT_ARTIFACT_SEEDER_ENABLED=
DASH_DEFAULT_ARTIFACT_SEED_ON_STARTUP=
SCRAPER_CONTROL_POLL_SECONDS=
```

Use the same backend data/runtime variables as the API service, with service
specific worker tuning where needed.

### Telegram Worker: `telegram-worker-staging`

```dotenv
APP_ROLE=worker
STAGING_ENABLE_BACKGROUND_JOBS=
TELEGRAM_API_HASH=
TELEGRAM_API_ID=
TELEGRAM_PHONE=
TELEGRAM_SESSION_STRING=
FEATURE_SOURCE_PEER_REF_LOOKUP=
FEATURE_SOURCE_RESOLUTION_QUEUE=
FEATURE_SOURCE_RESOLUTION_WORKER=
FEATURE_BEHAVIORAL_BRIEFS_AI=
FEATURE_OPPORTUNITY_BRIEFS_AI=
FEATURE_QUESTION_BRIEFS_AI=
FEATURE_TOPIC_OVERVIEWS_AI=
GROUP_MAX_MESSAGES_PER_SOURCE_PER_CYCLE=
GROUP_MAX_THREAD_ANCHORS_PER_SOURCE_PER_CYCLE=
NEO4J_MAX_CONNECTION_POOL_SIZE=
OPENAI_MODEL=
SCRAPE_MAX_COMMENT_POSTS_PER_SOURCE_PER_CYCLE=
SCRAPE_MAX_POSTS_PER_SOURCE_PER_CYCLE=
SCRAPE_SKIP_WHEN_BACKLOG=
SOURCE_RESOLUTION_INTERVAL_MINUTES=
SOURCE_RESOLUTION_MAX_JOBS_PER_RUN=
SOURCE_RESOLUTION_MIN_INTERVAL_SECONDS=
AI_COMMENT_WORKERS=
AI_MAX_INFLIGHT_REQUESTS=
AI_POST_WORKERS=
AI_PROCESS_STAGE_MAX_SECONDS=
AI_REQUEST_MAX_RETRIES=
AI_REQUEST_TIMEOUT_SECONDS=
AI_SYNC_STAGE_MAX_SECONDS=
AI_TRANSIENT_RECOVERY_ENABLED=
```

Use the same backend data/runtime variables as the API service.

### Social Worker: `social-worker`

```dotenv
ALLOW_STAGING_SOCIAL_WORKER=
APP_ROLE=social-worker
NIXPACKS_START_CMD=python -m api.social_worker
RAILPACK_START_CMD=python -m api.social_worker
OPENAI_API_KEY=
REDIS_URL=
RUN_STARTUP_WARMERS=
SCRAPECREATORS_API_KEY=
SOCIAL_CONTROL_POLL_SECONDS=
SOCIAL_DATABASE_URL=
SOCIAL_NEO4J_DATABASE=
SOCIAL_NEO4J_PASSWORD=
SOCIAL_NEO4J_URI=
SOCIAL_NEO4J_USERNAME=
SOCIAL_RUNTIME_ENABLED=
SOCIAL_SUPABASE_SERVICE_ROLE_KEY=
SOCIAL_SUPABASE_URL=
```

### Frontend: `romantic-acceptance`

```dotenv
BACKEND_ANALYTICS_API_KEY_FRONTEND=
BACKEND_URL=
NPM_CONFIG_PRODUCTION=
VITE_ENABLE_SIMPLE_AUTH=
VITE_SIMPLE_AUTH_PASSWORD=
VITE_SIMPLE_AUTH_USERNAME=
VITE_USE_DASHBOARD_V2=
```

## Finance Clone Rule

Finance should be deployed to a separate Railway project. Reuse this branch as
the source code, but use Finance-owned Supabase, Neo4j, Redis, domains, auth
secrets, scraping credentials, and Telegram/social session credentials.

Never point Finance services at Radar Supabase, Radar Neo4j, or Radar Redis.

## Verification Notes

On 2026-04-30, the non-Railway-generated env variable names in this document
were checked against the live Railway services:

| Service | Non-Railway env names checked | Missing from this document |
| --- | ---: | ---: |
| `loyal-presence` | 80 | 0 |
| `loyal-presence-worker` | 82 | 0 |
| `social-worker` | 17 | 0 |
| `telegram-worker-staging` | 62 | 0 |
| `romantic-acceptance` | 7 | 0 |

The CLI-deployed worker services do not expose Git commit metadata in Railway.
They are reproducible from this branch by configuring the same Railway service
commands and env names:

| Service | Runtime role | Command |
| --- | --- | --- |
| `loyal-presence-worker` | `APP_ROLE=worker` | `python -m api.worker` |
| `social-worker` | `APP_ROLE=social-worker` | `python -m api.social_worker` |
| `telegram-worker-staging` | `APP_ROLE=worker` | `python -m api.worker` |

The branch contains and compiles both required worker entrypoints:
`api/worker.py` and `api/social_worker.py`.
