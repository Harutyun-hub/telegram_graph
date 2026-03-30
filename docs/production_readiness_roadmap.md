# Production Readiness Roadmap

## Current State
- Release A created the current stable baseline:
  - single backend service
  - `APP_ROLE=all` in production
  - scraper enabled in production
  - recurring card materializers disabled
  - request/background executor split active
- Major Release A corrections are complete:
  - dashboard fallback behavior corrected
  - freshness hot-path behavior corrected
  - topics path materially improved
  - Topic Overview restored to materialized backend generation
  - frontend/backend contract regressions corrected
- Staging now exists and is deployable, but is still provisional until isolated from production-grade data/services.

## Current Decisions
- Release A status: `HOLD`, not `PASS`
- Release B: staging-only, not production-ready yet
- Release C: blocked until Release B staging validation succeeds

## Execution Order
1. Close Release A with written evidence.
2. Isolate staging from production-grade data/services.
3. Confirm staging smoke and observability.
4. Realign `main` to the Release A truth.
5. Merge Social into staging first.
6. Merge Graph into staging after Social is green.
7. Run Release B in staging only.
8. Prepare Release C cutover runbook during Release B validation.

## Scale Architecture
- Frontend Web
- API Web
- Worker
- Redis
- Supabase/Postgres
- Neo4j
- OpenAI as asynchronous enrichment only

## Runtime Contract
- Request path:
  - memory cache
  - persisted snapshots
  - materialized read models
  - bounded degraded fast paths
- Background path:
  - scraping orchestration
  - enrichment
  - materialization and refresh jobs
- AI must never be a required request-path dependency.
