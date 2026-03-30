# Production Runbook

## Release A Posture
- Source-of-truth stabilization line: `codex/release-a-reconcile-20260328`
- Release A status: `HOLD`
- Current production runtime remains single-service:
  - one backend service
  - `APP_ROLE=all`
  - scraper scheduler enabled
  - recurring card materializers disabled
- Release B and Release C changes are planned later and are not part of the current live posture.

## Current Runtime Safety Rules
- Preserve the request/background executor split from `api/runtime_executors.py`.
- Preserve the historical dashboard fast path for uncached ranges.
- Preserve persisted dashboard snapshot behavior across restarts.
- Keep recurring materializers disabled until the Phase 1 soak window is clean.
- Do not switch production to `APP_ROLE=web` until a separate worker service exists and passes staging validation.

## GitHub Release Gates
- Protect `main` with the `quality-gate` status check from `.github/workflows/ci.yml`.
- Backend gate covers:
  - dependency install from `requirements.txt` + `requirements-dev.txt`
  - syntax/import compile checks
  - secret hygiene
  - focused linting
  - backend tests with a transitional coverage floor
- Frontend gate currently covers the production build only.
- Use `.github/workflows/deployment-smoke.yml` for manual environment smoke checks.
- Use `.github/workflows/post-deploy-warmup.yml` after production deploys.

## Release A Verification
- Mixed-load probe target envelope:
  - `/api/dashboard`: p95 `<= 4.5s`, `0` non-200, `0` timeouts
  - `/api/topics`: p95 `<= 1.5s`, `0` non-200, `0` timeouts
  - `/api/topics/detail`: p95 `<= 1.0s`, `0` non-200, `0` timeouts
  - `/api/topics/evidence`: p95 `<= 1.0s`, `0` non-200, at most `1` timeout in the probe window
- Historical range validation:
  - first uncached historical request returns `200`
  - first response may be degraded fast path
  - first response completes `<= 8s`
  - full cached follow-up completes `<= 2s` within `60s`
- Soak window: `72h`
- Soak thresholds:
  - dashboard `5xx` rate `< 0.5%`
  - `/readyz` failures `= 0`
  - scheduler success `>= 99%`
  - no day-over-day increase in Neo4j routing, pool, or defunct-connection errors
  - no return of the raw dashboard timeout/failure path on normal or historical user traffic

## Release A Exit Gate
- `PASS` only when:
  - all required historical range checks are complete
  - `/api/topics/detail` and `/api/topics/evidence` validation is complete
  - the final soak conclusion is written into `docs/release_a_soak_report.md`
  - no new instability trend appears during the closeout window
- `HOLD` when:
  - the runtime is stable enough to stay live
  - evidence is still incomplete
  - no fresh regression requires rollback
- `FAIL` only when:
  - dashboard failures or readiness failures return
  - scheduler success falls below threshold
  - Neo4j instability trends upward again

## Smoke Commands
Manual smoke:

```bash
DEPLOY_BASE_URL=https://your-app.example.com \
ANALYTICS_API_KEY_FRONTEND=... \
python scripts/run_smoke_checks.py --wait-ready --label production
```

Backend QA:

```bash
make qa-backend
```

Frontend QA:

```bash
make qa-frontend
```

## Release A Rollback
- Before deployment, record:
  - current live git revision as `pre-release-a-stable`
  - Railway deploy identifiers for live services
  - any environment changes made for the release
- If Release A regresses:
  1. redeploy the recorded `pre-release-a-stable` artifacts
  2. revert any environment changes introduced by the release
  3. run smoke checks before reopening access
- Release A contains no schema change, so database restore is not part of Release A rollback.

## Deferred Work
- Release B will introduce staging-only `web + worker + Redis` validation and locked-environment gates.
- Release C will introduce the production split, controlled materializer rollout, and the backup/restore exit gate.
- Do not treat those future controls as already-live production behavior in Release A documentation or deployment instructions.

## Next Approved Sequence
1. Close Release A with a written `PASS` or `HOLD` decision.
2. Move staging off production-grade data/services before trusting it as pre-production.
3. Realign `main` only after staging isolation and staging smoke are green.
4. Merge Social into staging first; merge Graph only after Social staging smoke passes.
5. Start Release B in staging only.
6. Prepare the Release C cutover runbook during Release B staging validation, before any production split rollout.
