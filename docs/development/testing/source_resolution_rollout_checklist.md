# Source Resolution Rollout Checklist

This checklist is for testing the Release-A-safe Telegram source-resolution queue before enabling it in production.

## Scope

- Keep the current single-service runtime shape.
- Validate additive schema first.
- Turn feature flags on in phases.
- Do not enable peer-ref scrape mode until active resolved sources have peer refs.

## Prerequisites

1. Apply `supabase/migrations/20260404_source_resolution_queue.sql`.
2. Confirm all three flags are `false`:
   - `FEATURE_SOURCE_RESOLUTION_QUEUE`
   - `FEATURE_SOURCE_RESOLUTION_WORKER`
   - `FEATURE_SOURCE_PEER_REF_LOOKUP`
3. Leave rollout defaults conservative:
   - `SOURCE_RESOLUTION_INTERVAL_MINUTES=1`
   - `SOURCE_RESOLUTION_MIN_INTERVAL_SECONDS=5`
   - `SOURCE_RESOLUTION_MAX_JOBS_PER_RUN=10`
4. Restart the service after applying the migration and env values.

## Phase 0: Dormant Schema

1. Open `/readyz` and `/api/scraper/scheduler`.
2. Confirm the app is healthy and the scheduler still reports normal status.
3. Open `/api/sources/resolution`.
4. Expected:
   - endpoint returns a `resolution` block
   - `enabled=false`
   - no errors from missing tables

## Phase 1: Queue On, Worker Off

Set:

- `FEATURE_SOURCE_RESOLUTION_QUEUE=true`
- `FEATURE_SOURCE_RESOLUTION_WORKER=false`
- `FEATURE_SOURCE_PEER_REF_LOOKUP=false`

Checks:

1. Create a new source with `POST /api/sources/channels`.
2. Reactivate an unresolved existing source with the same endpoint.
3. Patch an unresolved inactive source to `is_active=true`.
4. Open `/api/sources/resolution`.
5. Open `/api/freshness?force=true`.

Expected:

- source writes return quickly
- created or reactivated sources land in `resolution_status=pending`
- no inline Telegram resolution is attempted in the request path
- due jobs appear in the resolution snapshot
- freshness includes:
  - `resolution_due_jobs`
  - `resolution_leased_jobs`
  - `resolution_dead_letter_jobs`
  - `resolution_cooldown_slots`
  - `active_pending_sources`

Rollback:

- turn `FEATURE_SOURCE_RESOLUTION_QUEUE=false`

## Phase 2: Resolution Worker On

Set:

- `FEATURE_SOURCE_RESOLUTION_QUEUE=true`
- `FEATURE_SOURCE_RESOLUTION_WORKER=true`
- `FEATURE_SOURCE_PEER_REF_LOOKUP=false`

Checks:

1. Open `/api/sources/resolution` and verify `enabled=true`.
2. Trigger `POST /api/sources/resolution/run-once`.
3. Watch `/api/sources/resolution` for:
   - `jobs_processed`
   - `jobs_resolved`
   - `jobs_requeued`
   - `jobs_dead_lettered`
4. Watch `/api/freshness?force=true`.

Expected:

- pending jobs drain gradually
- flood-wait jobs stay `pending`
- flood-wait jobs get `resolution_error_code=flood_wait`
- `resolution_retry_after_at` is populated for retryable jobs
- `cooldown_slots` increases only when Telegram actually returns flood wait
- no scraper/process/sync regression

Rollback:

- turn `FEATURE_SOURCE_RESOLUTION_WORKER=false`

## Phase 3: Peer-Ref Backfill

Keep:

- `FEATURE_SOURCE_RESOLUTION_QUEUE=true`
- `FEATURE_SOURCE_RESOLUTION_WORKER=true`
- `FEATURE_SOURCE_PEER_REF_LOOKUP=false`

Checks:

1. Open `/api/sources/resolution` and note `active_missing_peer_refs`.
2. Trigger `POST /api/sources/resolution/backfill-peer-refs`.
3. Run `POST /api/sources/resolution/run-once` until the active missing-peer-ref count reaches zero.

Expected:

- backfill endpoint reports `queued > 0` when active resolved sources still lack peer refs
- `active_missing_peer_refs` trends to `0`
- no source should flip to `error` just because peer-ref backfill is running

## Phase 4: Peer-Ref Scrape Mode

Set:

- `FEATURE_SOURCE_RESOLUTION_QUEUE=true`
- `FEATURE_SOURCE_RESOLUTION_WORKER=true`
- `FEATURE_SOURCE_PEER_REF_LOOKUP=true`

Checks:

1. Run one scrape cycle.
2. Verify known resolved sources with peer refs scrape normally.
3. Verify a source missing a peer ref is queued and skipped instead of forcing username resolution in the scrape loop.
4. Verify a stale peer ref is reset to `pending` and requeued.

Expected:

- scrape continues for resolved sources with peer refs
- missing peer refs do not cause repeated `ResolveUsernameRequest` calls in scrape prep
- stale peer refs trigger re-resolution, not permanent failure

Rollback:

- turn `FEATURE_SOURCE_PEER_REF_LOOKUP=false`

## Acceptance Criteria

- source request handlers no longer produce Telegram flood-wait errors
- pending sources resolve asynchronously through the queue
- freshness exposes resolution backlog and cooldown state
- active resolved sources can be backfilled to cached peer refs
- scrape mode can switch to peer refs without breaking source processing

## Focus Areas During Manual Validation

- repeated create/reactivate calls should not create duplicate resolution jobs
- flood-wait should delay work, not dead-letter valid sources
- invalid usernames should land in `error` and stop retrying
- scheduler status should expose both scrape and resolution runtime state
- rollback must be possible with flags only
