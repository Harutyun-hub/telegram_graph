# Radar Obshchiny

Radar Obshchiny is a Telegram intelligence platform with a React frontend, a FastAPI web service, dedicated Telegram and social worker runtimes, Supabase/Postgres for operational data, Neo4j for analytics, and protected operator/social surfaces.

## Canonical Documentation

Start here for the current system architecture and runtime model:

- [PROFESSIONAL_DOCUMENTATION.md](/Users/harutnahapetyan/Documents/Gemini/Telegram/PROFESSIONAL_DOCUMENTATION.md)

Use this for deployment, staging/production rules, smoke checks, and rollback:

- [docs/production_runbook.md](/Users/harutnahapetyan/Documents/Gemini/Telegram/docs/production_runbook.md)

If another high-level document conflicts with the canonical document, prefer `PROFESSIONAL_DOCUMENTATION.md`.

## Current Stack

- Backend: FastAPI, Python, APScheduler
- Background runtimes:
  - Telegram/runtime worker: `python -m api.worker`
  - Social/runtime worker: `python -m api.social_worker`
- Operational store: Supabase/Postgres
- Analytics graph: Neo4j
- Coordination/runtime support: Redis
- Frontend: React 18, TypeScript, Vite
- AI providers: OpenAI, optional OpenClaw-backed helper integrations

## Local Quick Start

### Prerequisites

- Python 3.10+
- Node.js 20+
- npm 10+
- Supabase credentials
- Neo4j credentials
- OpenAI API key

Telegram credentials are required only for worker-connected scraping flows.

### Backend

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
venv/bin/python -m uvicorn api.server:app --reload --port 8001
```

### Frontend

```bash
cd frontend
npm ci
npm run dev
```

The frontend uses `/api` as its default API base in development and production-compatible setups.

## Repository Layout

```text
.
├── api/           FastAPI app, endpoint orchestration, aggregation, helper APIs
├── scraper/       Telegram scraping and source preparation
├── processor/     AI enrichment logic
├── ingester/      Neo4j synchronization
├── buffer/        Operational persistence helpers
├── social/        Social intelligence runtime and storage helpers
├── frontend/      React application
├── scripts/       Maintenance, validation, smoke, and repair tooling
├── tests/         Backend regression coverage
├── config.py      Central configuration loader
└── docs/          Supporting runbooks and handover/reference docs
```

## Useful Entry Points

- Backend app: `api/server.py`
- Worker runtime: `api/worker.py`
- Social worker runtime: `api/social_worker.py`
- Frontend router: `frontend/src/app/routes.tsx`
- Runtime configuration: `config.py`

## Development Notes

- The preferred deployment shape is `frontend` + `web` + `worker` + `social-worker`
- `web` is passive for social runtime and reads shared social runtime state from the social operational store
- `worker` owns Telegram/background jobs only
- `social-worker` owns social collect, analysis, and graph sync only
- Use the runbook for environment split, rollout, and rollback guidance
- Use the canonical document for architecture, ownership, and source-of-truth rules
