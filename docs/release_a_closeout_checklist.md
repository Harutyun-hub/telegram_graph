# Release A Closeout Checklist

## Current Decision
- Default status: `HOLD`
- Do not change this to `PASS` until every item below is complete and documented.

## Required Evidence
- Historical range validation is complete for all planned sample windows.
- `topics/detail` validation is complete.
- `topics/evidence` validation is complete.
- The final Release A manual check table in `docs/release_a_soak_report.md` is up to date.
- The final incident log is up to date.

## Runtime Conditions
- `/readyz` remains stable.
- Default dashboard path is behaving correctly:
  - rebuild only when necessary
  - cached follow-up path confirmed
- Historical dashboard path is behaving correctly:
  - fast path on first uncached request is acceptable
  - cached follow-up path confirmed
- No new Neo4j routing, pool, or defunct-connection pattern is visible.
- Scheduler success remains at or above the Release A threshold.

## Decision Rules
- Mark `PASS` only when both evidence and runtime conditions are complete.
- Keep `HOLD` if the runtime is acceptable but documentation or evidence is incomplete.
- Mark `FAIL` only if a real regression returns and production safety is in doubt.

## Exit Sequence After Release A
1. Isolate staging from production-grade data/services.
2. Confirm staging smoke and observability.
3. Realign `main` to the Release A truth.
4. Merge Social into staging first.
5. Merge Graph into staging after Social staging smoke passes.
6. Start Release B in staging only.
