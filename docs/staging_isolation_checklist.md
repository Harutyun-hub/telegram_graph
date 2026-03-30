# Staging Isolation Checklist

## Goal
- Turn staging into a trustworthy pre-production environment by removing production-grade data/service coupling.

## Required Changes
- Replace staging `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` with isolated staging values.
- Replace staging `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, and `NEO4J_DATABASE` with isolated staging values.
- Keep `ENABLE_SCRAPER_SCHEDULER=false`.
- Keep `REQUIRE_TELEGRAM_CREDENTIALS=false` unless a controlled Telegram test is explicitly planned.
- Keep `APP_ROLE=web` during initial staging hardening.

## Acceptance
- Staging writes do not touch production-grade stores.
- Backend and frontend still deploy with `SUCCESS`.
- Smoke checks pass against the isolated staging environment.
- Logs and error reporting still work after isolation.

## Blockers
- Do not realign `main` until this checklist is complete.
- Do not start Release B validation until this checklist is complete.
