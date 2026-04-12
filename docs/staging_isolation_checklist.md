# Staging Isolation Checklist

## Goal
- Turn staging into a trustworthy pre-production environment by removing production-grade data/service coupling.

## Current Decision
- Isolation is deferred for now by explicit decision.
- Staging is temporarily allowed to share Supabase and Neo4j with production.
- Because of that, staging must be treated as provisional and non-destructive only.

## Required Changes
- Replace staging `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` with isolated staging values.
- Replace staging `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, and `NEO4J_DATABASE` with isolated staging values.
- Keep `ENABLE_SCRAPER_SCHEDULER=false`.
- Keep `REQUIRE_TELEGRAM_CREDENTIALS=false` unless a controlled Telegram test is explicitly planned.
- Keep `APP_ROLE=web` during initial staging hardening.

## Current Live Risk
- Staging currently points at the same Supabase host and Neo4j cluster/database as production.
- This remains a real risk and should be treated as deferred technical/operational debt.

## Acceptance
- Staging writes do not touch production-grade stores.
- Backend and frontend still deploy with `SUCCESS`.
- Smoke checks pass against the isolated staging environment.
- Logs and error reporting still work after isolation.

## Blockers
- Realigning `main` and starting Release B should still wait for a safer staging posture, even if isolation is temporarily deferred.
