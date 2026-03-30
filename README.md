# Radar Obshchiny Monorepo

[![Documentation Status](https://img.shields.io/badge/docs-current--state-green)](./PROFESSIONAL_DOCUMENTATION.md)
[![API Docs](https://img.shields.io/badge/api-docs-blue)](./docs/api/)
[![Architecture](https://img.shields.io/badge/architecture-documented-orange)](./docs/architecture/)

Production-oriented Telegram intelligence platform for community monitoring, AI enrichment, graph analytics, and dashboard delivery.

## What This Repository Ships

- FastAPI backend for dashboard and graph APIs
- Telegram scraping/orchestration with Telethon
- Supabase-backed operational storage and runtime state
- Neo4j analytics graph for strategic and network queries
- React + Vite dashboard frontend
- AI-powered enrichment and brief generation

## Current Dashboard Semantics

- A topic mention means one directly tagged message: one post or one comment.
- Strategic graph analytics use a clean 15-day Neo4j retention window by default.
- Topic Landscape and Conversation Trends operate on direct message mentions only.
- Topic Lifecycle is currently a short-window momentum classifier inside the 15-day window, not a full multi-stage lifecycle model.
- Service Gap Detector is AI-only. It shows AI-grounded service-gap bars when valid service cards exist, and a soft `No service gap detected.` state when they do not.

## Repository Layout

```text
.
├── api/                    # FastAPI server, aggregation, query tiers, AI briefs
├── scraper/                # Telegram extraction orchestration
├── processor/              # LLM extraction / enrichment
├── ingester/               # Neo4j write path
├── buffer/                 # Supabase/Postgres read-write layer
├── frontend/               # React dashboard application
├── scripts/                # Maintenance, validation, and rebuild utilities
├── tests/                  # Python regression coverage
├── config.py               # Central env/config loader
└── requirements.txt        # Backend dependencies
```

## Architecture Overview

```mermaid
graph LR
    A[Telegram Channels] -->|Telethon| B[Scraper]
    B -->|Raw content| C[Supabase]
    C -->|Analysis jobs| D[AI enrichment]
    D -->|Structured outputs| C
    C -->|Sync| E[Neo4j]
    E -->|Cypher queries| F[FastAPI]
    F -->|JSON| G[React Dashboard]
```

## Local Quick Start

### 1. Prerequisites

- Python 3.10+
- Node.js 20+
- npm 10+
- Telegram API credentials
- Supabase project
- Neo4j database
- OpenAI API key

### 2. Configure environment

```bash
cp .env.example .env
cp frontend/.env.example frontend/.env
```

Fill `.env` with real secrets before running the stack.

### 3. Install dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cd frontend
npm ci
cd ..
```

### 4. Run backend

```bash
source venv/bin/activate
python -m uvicorn api.server:app --reload --port 8001
```

### 5. Run frontend

```bash
cd frontend
npm run dev
```

The frontend defaults to `/api`. Set `VITE_API_BASE_URL` in `frontend/.env` if you want an explicit backend URL.

## Important Environment Variables

Backend:

- `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_PHONE`
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`
- `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE`
- `OPENAI_API_KEY`
- `APP_ROLE` optional, default `all` for the current single-service deployment; use `web` only when a separate worker service is running background jobs, and `worker` on that dedicated worker
- `ANALYTICS_API_REQUIRE_AUTH` default `false`
- `ANALYTICS_API_KEY_FRONTEND` for the frontend proxy
- `ANALYTICS_API_KEY_OPENCLAW` for OpenClaw/server-to-server calls
- `ANALYTICS_RATE_LIMIT_ENABLED` default `true`
- `ANALYTICS_RATE_LIMIT_WINDOW_SECONDS` default `60`
- `ANALYTICS_RATE_LIMIT_MAX_REQUESTS` default `120`
- `OPENAI_MODEL` default `gpt-5.4-mini`
- `QUESTION_BRIEFS_MODEL` default `gpt-5.4-mini`
- `QUESTION_BRIEFS_TRIAGE_MODEL` default `gpt-5.4-mini`
- `QUESTION_BRIEFS_SYNTHESIS_MODEL` default `gpt-5.4-mini`
- `GRAPH_ANALYTICS_RETENTION_DAYS` default `15`
- `BEHAVIORAL_BRIEFS_MODEL` default `gpt-5.4-mini`
- `BEHAVIORAL_BRIEFS_PROMPT_VERSION` default `behavior-v2`

Frontend:

- `VITE_API_BASE_URL` default `/api`
- `BACKEND_ANALYTICS_API_KEY_FRONTEND` runtime secret for Caddy proxy injection
- `BACKEND_URL` runtime backend target for Caddy reverse proxy

## AI Systems

- The main extraction pipeline defaults to `gpt-5.4-mini` through `OPENAI_MODEL`.
- Question briefs default to `gpt-5.4-mini` for primary, triage, and synthesis passes.
- Behavioral briefs default to `gpt-5.4-mini`.
- Service-gap cards are generated only from grounded service/help evidence in posts and related comments. There is no production fallback that turns generic topic dissatisfaction into service-gap bars.

## Railway Compatibility

Release A remains intentionally compatible with the current Railway single-service backend shape:

- no change to the frontend Caddy reverse proxy contract
- no new Railway manifest or service split
- no new dependency requirements beyond the existing backend/frontend stacks
- background jobs still run in the default single-service deploy with `APP_ROLE=all`
- recurring card materializers remain intentionally disabled in production during the stabilization window

Operational note:

- if Railway does not explicitly set the AI model variables, the new defaults apply automatically
- if Railway already pins any of these values, update them manually for parity with this release:
  `OPENAI_MODEL`, `QUESTION_BRIEFS_MODEL`, `QUESTION_BRIEFS_TRIAGE_MODEL`, `QUESTION_BRIEFS_SYNTHESIS_MODEL`, `BEHAVIORAL_BRIEFS_MODEL`, `BEHAVIORAL_BRIEFS_PROMPT_VERSION`

The frontend still expects `/api/*` to be reverse-proxied to the backend through `BACKEND_URL` in Railway.
- In Railway staging/production, `BACKEND_URL` should use the internal Railway backend URL, not the public `up.railway.app` URL.

Analytics auth rollout:

- deploy backend code with auth support while `ANALYTICS_API_REQUIRE_AUTH=false`
- set backend + frontend + OpenClaw env vars
- deploy frontend proxy change
- verify website and OpenClaw both still work
- flip `ANALYTICS_API_REQUIRE_AUTH=true`
- run smoke tests again

Immediate rollback:

- if there is an outage, set `ANALYTICS_API_REQUIRE_AUTH=false` and redeploy
- if a Release A deployment regresses, redeploy the recorded `pre-release-a-stable` artifacts and revert any env changes introduced with that release

Future architecture note:

- `web + worker + Redis` is a planned Release B/C target, not the current live production posture
- do not flip production to `APP_ROLE=web` unless a separate worker service exists and has passed staging validation

## Validation Commands

Backend QA:

```bash
make qa-backend
```

Frontend build QA:

```bash
make qa-frontend
```

Smoke checks against a deployed environment:

```bash
DEPLOY_BASE_URL=https://your-app.example.com \
ANALYTICS_API_KEY_FRONTEND=... \
make smoke-check
```

GitHub release gate recommendation:

- protect `main` with the `quality-gate` status check from `.github/workflows/ci.yml`
- use `.github/workflows/deployment-smoke.yml` or `.github/workflows/post-deploy-warmup.yml` after deploys instead of relying on ad hoc curls

## Operational Scripts

Relevant maintenance scripts for the current analytics stack:

- `scripts/reset_topic_analytics_window.py` — resets and rebuilds the clean analytics window
- `scripts/validate_topic_mentions.py` — validates direct-message mention counts
- `scripts/remove_redundant_general_topic_links.py` — removes redundant `General` taxonomy links when a stronger category exists
- `scripts/probe_mixed_load.py` — runs the Phase 1 mixed-load validation probe
- `scripts/run_smoke_checks.py` — reusable post-deploy smoke validation for readiness, dashboard, topics, and freshness

## Documentation

- [`PROFESSIONAL_DOCUMENTATION.md`](./PROFESSIONAL_DOCUMENTATION.md) — current-state system and product documentation
- [`docs/api/`](./docs/api/) — API references
- [`docs/architecture/`](./docs/architecture/) — architecture references
- [`frontend/README.md`](./frontend/README.md) — frontend runtime notes

## Security Notes

- Never commit `.env`, Telegram sessions, or service credentials
- Use least-privilege credentials in production
- Restrict admin and scheduler endpoints in deployed environments

## Status

- Deployment target: Railway-compatible mainline
- Graph analytics window: 15 days
- Service-gap mode: AI-only
- Documentation status: current-state and release-aligned
