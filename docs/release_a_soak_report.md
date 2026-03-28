# Release A Soak Report

## Status
- Phase: `Release A`
- State: `IN_PROGRESS`
- Branch: `codex/release-a-reconcile-20260328`
- Deployed commit: `745563e`
- Railway service: `telegram_graph`
- Railway deployment id: `db676846-51d0-4621-852c-d913cabbae49`

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
|  | Historical range check 2 | PENDING |  |
|  | Historical range check 3 | PENDING |  |

## Historical Range Validation
For each sampled uncached range, record:
- first request status and latency
- whether the first response used degraded fast path
- whether a follow-up request became cached/full within `60s`

| Range | First Request | First Latency | Degraded Fast Path | Follow-up Cached Within 60s | Notes |
| --- | --- | --- | --- | --- | --- |
| `2026-02-18` to `2026-03-04` | `200` | `7.30s` | `YES` | PENDING | `degradedTiers=["predictive","comparative","network"]`, `cacheSource=fastpath`, `persistedReadStatus=hit` |
| TBD | PENDING | PENDING | PENDING | PENDING |  |
| TBD | PENDING | PENDING | PENDING | PENDING |  |

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

## Decision Gate
- `PASS` only if the full 72-hour window meets all Release A thresholds.
- `HOLD` if any threshold is uncertain or evidence is incomplete.
- `FAIL` if dashboard failures return, scheduler success falls below threshold, or Neo4j instability trends upward.

## Next Step After Soak
- If `PASS`: begin Release B staging-only split readiness work.
- If `HOLD` or `FAIL`: stay on Release A, investigate, and do not start Release B.
