# Original Dashboard Mirror Lab

This branch is the isolated performance lane for the original dashboard path only:

- frontend uses `/api/dashboard`
- backend serves the original route in `api/server.py`
- aggregation stays in `api/aggregator.py`
- original query modules stay in `api/queries/`

Out of scope:

- `/api/dashboard-v2`
- `dashboard_v2*`
- V2 warmers, compare flows, fact pipelines, or rollout state
- widget logic or payload-shape changes

## Railway Mirror Services

Create a fully separate Railway environment with new services:

- `original-dashboard-lab-frontend`
- `original-dashboard-lab-web`
- `original-dashboard-lab-redis`

Do not reuse:

- `romantic-acceptance`
- `loyal-presence`
- current staging Redis
- any staging V2 frontend flag or route switch

## Mirror Data Plane

The lab must use cloned read/test stores only:

- cloned Supabase/Postgres project or restored database
- cloned Neo4j instance or database
- separate Redis instance

Treat the mirror data as lab-only. Do not read from or write to the active staging data plane during optimization.

## Mirror Runtime Shape

Recommended web runtime:

- `APP_ROLE=web`
- `RUN_STARTUP_WARMERS=false`
- `DASH_REQUEST_PROFILE_ENABLED=false`

Manual warmup and profiling now use the admin route added in this branch:

- `POST /api/admin/dashboard/warm`

Body:

```json
{
  "from_date": "2026-04-01",
  "to_date": "2026-04-30",
  "wait": true,
  "force_refresh": false,
  "profile": true
}
```

This route is operator-protected and is intended for the mirror lab only.

## Baseline Commands

Warm a single range:

```bash
python scripts/original_dashboard_lab.py warm \
  --base-url https://your-mirror-web.example.com \
  --admin-token "$ADMIN_API_KEY"
```

Record default, explicit 7-day, and explicit 30-day baselines:

```bash
python scripts/original_dashboard_lab.py baseline \
  --base-url https://your-mirror-web.example.com \
  --analytics-token "$ANALYTICS_API_KEY_FRONTEND" \
  --admin-token "$ADMIN_API_KEY"
```

Compare normalized `/api/dashboard` payloads between two environments:

```bash
python scripts/original_dashboard_lab.py compare \
  --left-base-url https://before.example.com \
  --right-base-url https://after.example.com \
  --left-analytics-token "$ANALYTICS_API_KEY_FRONTEND" \
  --right-analytics-token "$ANALYTICS_API_KEY_FRONTEND" \
  --from-date 2026-04-01 \
  --to-date 2026-04-30
```

The compare command ignores volatile metadata only. Any remaining widget data diff should be treated as a regression.

## Current Optimization Hooks

This branch adds:

- opt-in per-request profiling for original dashboard builds
- slow-query summaries for Neo4j and Supabase/PostgREST reads
- a manual admin warm endpoint for mirror QA and cold-build measurement
- safer reuse of shared Supabase clients on original query paths
- a consolidated vitality query to remove repeated Neo4j round trips

## QA Checklist

- verify mirror frontend network requests hit `/api/dashboard`
- capture `cacheStatus`, `cacheSource`, `tierTimes`, response bytes, and warm timings
- confirm no V2 path is used anywhere in the mirror deployment
- compare normalized payloads before and after each optimization batch
