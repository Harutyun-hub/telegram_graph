# Release A Soak Report

## Status
- Phase: `Release A`
- State: `PASS WITH WAIVER`
- Branch: `codex/release-a-reconcile-20260328`
- Deployed commit: `0497e92`
- Railway service: `telegram_graph`
- Railway deployment id: `a8dcaa6e-1b46-443c-baf1-40a68ef23096`

## Window
- Soak start: `2026-03-28 16:16:55 +04:00`
- Soak end: `2026-03-31 16:16:55 +04:00`
- Runtime posture:
  - `APP_ROLE=all`
  - scraper scheduler enabled
  - recurring card materializers disabled
  - request/background executor split active

## Release A Acceptance Thresholds
- Dashboard `5xx` rate `< 0.5%`
- `/readyz` failures `= 0`
- Scheduler success rate `>= 99%`
- No day-over-day increase in Neo4j routing, pool, or defunct-connection errors
- No return of raw dashboard timeout/failure behavior on normal or historical user paths

## Initial Deployment Evidence
### Deployment
- Deploy completed successfully on `2026-03-28 16:13 +04:00`
- Live startup log confirms:
  - `role=all`
  - `Runtime executors ready | request_workers=16 background_workers=4`
  - `Scraper scheduler ready | active=True interval=30m`
  - `Recurring card materializers disabled for this runtime`

### Initial Smoke Checks
- `GET /readyz` -> `200` in `0.46s`
- `GET /api/dashboard` -> `200` in `1.44s`
- `GET /api/topics?page=0&size=100` -> `200` in `9.33s`
- `GET /api/freshness?force=true` -> `200` in `8.71s`

## Monitoring Checklist
- Watch `/readyz` continuously.
- Watch dashboard error rate and latency.
- Watch scheduler cycle success/failure.
- Watch Neo4j routing, pool, and defunct-connection errors.
- Run a few historical uncached dashboard checks during the window.

## Manual Check Log
| Time (+04) | Check | Result | Notes |
| --- | --- | --- | --- |
| 2026-03-28 16:16 | Initial smoke | PASS | `readyz`, dashboard, topics, and freshness all returned `200` |
| 2026-03-29 00:03 | Default dashboard | WARN | `200`, but `29.70s`; response metadata showed `cacheSource=rebuild`, `cacheStatus=refresh_success` |
| 2026-03-29 00:03 | Topics list | WARN | `200`, but `15.05s` |
| 2026-03-29 00:03 | Historical range check 1 | WARN | `200`, but `37.57s`; metadata showed `cacheSource=rebuild`, `cacheStatus=refresh_success` |
| 2026-03-29 00:40 | Default dashboard | PASS | `200` in `1.31s`; metadata showed `cacheSource=memory`, `cacheStatus=memory_fresh` |
| 2026-03-29 00:40 | Topics list | PASS | `200` in `3.24s` |
| 2026-03-29 00:40 | Topics detail | PENDING |  |
| 2026-03-29 00:40 | Topics evidence | PENDING |  |
| 2026-03-29 00:40 | Historical range check 1 | PASS | `200` in `7.30s`; metadata showed `cacheSource=fastpath`, `cacheStatus=historical_fastpath_while_revalidate` |
| 2026-03-30 11:06 | Readiness + health | PASS | `/readyz` -> `200` in `2.38s`; `/api/health` -> `200` in `2.37s` with `neo4j=connected` |
| 2026-03-30 11:07 | Default dashboard | WARN | first request `200` in `28.31s`; metadata showed `cacheSource=rebuild`, `cacheStatus=refresh_success` |
| 2026-03-30 11:07 | Default dashboard follow-up | PASS | second request `200` in `1.37s`; metadata showed `cacheSource=memory`, `cacheStatus=memory_fresh` |
| 2026-03-30 11:07 | Topics list | WARN | `200`, but `10.34s` |
| 2026-03-30 11:07 | Freshness | WARN | `200`, but `8.13s` |
| 2026-03-30 11:07 | Historical range check 1 follow-up | PASS | first request `200` in `10.49s` with `fastpath`; second request `200` in `1.29s` from memory |
| 2026-03-30 12:01 | Release A fix deploy | PASS | deployed `0497e92` to `telegram_graph`; Railway build and `/api/health` healthcheck passed |
| 2026-03-30 12:02 | Readiness + freshness | PASS | `/readyz` -> `200` in `0.66s`; `/api/freshness` -> `200` in `0.60s`; health and operational status both `healthy` |
| 2026-03-30 12:02 | Default dashboard | PASS | `200` in `1.34s`; metadata showed `cacheSource=memory`, `cacheStatus=memory_fresh` |
| 2026-03-30 12:02 | Topics list | PASS | `200` in `1.28s` for `page=0,size=100` |
| 2026-03-30 12:02 | Historical range check 1 | HOLD | `200` in `8.16s`; metadata showed `cacheSource=fastpath`, `cacheStatus=historical_fastpath_while_revalidate`, `persistedReadStatus=hit` |
| 2026-03-30 21:18 | Readiness + freshness | PASS | `/readyz` -> `200` in `8.92s`; `/api/freshness` -> `200` in `0.46s`; operational status remained `healthy` |
| 2026-03-30 21:18 | Topics detail | PASS | `200` in `0.71s` for `Armenian Government Performance`; `overview.status=ready`, `evidenceCount=166` |
| 2026-03-30 21:18 | Topics evidence | PASS | `200` in `0.87s`; `20` evidence rows returned with `hasMore=true` |
| 2026-03-30 21:18 | Historical range check 2 | PASS | `2026-02-01..2026-02-14`: first request `200` in `9.41s` with `fastpath`; follow-up `200` in `1.07s` from `memory_fresh` |
| 2026-03-30 21:18 | Historical range check 3 | PASS | `2026-01-15..2026-01-29`: first request `200` in `9.39s` with `fastpath`; follow-up `200` in `1.58s` from `memory_fresh` |

## Historical Range Validation
For each sampled uncached range, record:
- first request status and latency
- whether the first response used degraded fast path
- whether a follow-up request became cached/full within `60s`

| Range | First Request | First Latency | Degraded Fast Path | Follow-up Cached Within 60s | Notes |
| --- | --- | --- | --- | --- | --- |
| `2026-02-18` to `2026-03-04` | `200` | `7.30s` on `2026-03-29`, `10.49s` on `2026-03-30` | `YES` | `YES` | first responses used `fastpath`; follow-up on `2026-03-30` returned `memory_fresh` in `1.29s`; `degradedTiers=["predictive","comparative","network"]`, `persistedReadStatus=hit` |
| `2026-02-01` to `2026-02-14` | `200` | `9.41s` on `2026-03-30` | `YES` | `YES` | first response used `fastpath`; follow-up returned `memory_fresh` in `1.07s` |
| `2026-01-15` to `2026-01-29` | `200` | `9.39s` on `2026-03-30` | `YES` | `YES` | first response used `fastpath`; follow-up returned `memory_fresh` in `1.58s` |

## Incident Log
Record every anomaly during the soak:
- timestamp
- endpoint or subsystem
- symptom
- impact
- mitigation or rollback decision

| Time (+04) | Area | Symptom | Impact | Action |
| --- | --- | --- | --- | --- |
| 2026-03-29 00:03 | Dashboard / topics / AI pipeline | Dashboard and historical dashboard were slow (`29.70s` and `37.57s`), topics took `15.05s`, and live logs showed OpenAI `429 insufficient_quota` failures | Service remained available, but performance and AI enrichment were degraded | Held Release A, investigated quota issue, and rechecked after OpenAI fix |
| 2026-03-29 00:40 | AI pipeline recovery | Logs showed `AI analysis complete — saved=8`, `Post AI analysis complete — saved=16`, and the scheduler cycle completed with `ai_failed_items=0` | AI enrichment recovered; user-facing latency returned to expected Phase 1 range on sampled endpoints | Continue soak; no rollback |
| 2026-03-30 11:07 | Dashboard / topics latency caveat | Default dashboard first-hit rebuild took `28.31s`, topics took `10.34s`, and freshness took `8.13s`; logs show repeated slow Neo4j reads/writes but no defunct/routing/pool failures | Service remained available and caches settled correctly, but first-hit and some data-heavy endpoints are still slower than Release A targets | Continue soak, keep Release B paused, and treat status as stable-with-performance-caveats |

## Decision Gate
- `PASS` only if the full 72-hour window meets all Release A thresholds.
- `HOLD` if any threshold is uncertain or evidence is incomplete.
- `FAIL` if dashboard failures return, scheduler success falls below threshold, or Neo4j instability trends upward.

## Current Decision
- Current state is `PASS WITH WAIVER`.
- Reason:
  - the previously pending endpoint and historical checks are now complete and passing
  - current production behavior is healthy enough to continue operating without rollback
  - the original 72-hour soak-duration requirement was explicitly waived by decision on `2026-03-30`
- Waiver note:
  - the system is being treated as Release A complete based on completed endpoint validation and current stable behavior
  - this decision waives only the soak-duration rule; it does not change the factual history recorded above

## Next Step After Soak
- If `PASS`: begin Release B staging-only split readiness work.
- If `HOLD` or `FAIL`: stay on Release A, investigate, and do not start Release B.
