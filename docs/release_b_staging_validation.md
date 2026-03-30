# Release B Staging Validation

## Goal
- Validate the `web + worker + Redis` architecture in staging before any production split rollout.

## Scope
- Introduce:
  - `web`
  - `worker`
  - `Redis`
- Keep production unchanged.

## Acceptance
- Web stays responsive under load.
- Worker owns background execution.
- Request paths do not depend on live AI generation.
- Read-model refreshes still behave correctly.

## Critical Validation: Exact-Once Execution
- Run forced refresh/materialization cycles in staging.
- Verify each job executes exactly once.
- Verify no duplicate writes to snapshots/read models.
- Verify no overlap between web and worker responsibilities.

## Redis Degradation Posture
- If Redis is unavailable:
  - web stays up
  - read paths continue from existing cache/snapshots where possible
  - background jobs pause or fail closed
  - no fallback is allowed that causes duplicate execution across services

## Required Evidence
- Staging smoke results
- Logs showing web and worker role separation
- Validation notes for exact-once execution
- Validation notes for Redis-unavailable behavior
