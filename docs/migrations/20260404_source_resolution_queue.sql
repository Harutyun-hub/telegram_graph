-- Source resolution queue and peer-reference support.
--
-- Release-A safe rollout:
-- 1. All additions are additive and dormant behind feature flags.
-- 2. Queue + peer-ref tables can be deployed before any runtime behavior changes.
-- 3. Existing source flows continue to work with FEATURE_SOURCE_RESOLUTION_* flags disabled.

ALTER TABLE public.telegram_channels
  ADD COLUMN IF NOT EXISTS resolution_error_code TEXT NULL,
  ADD COLUMN IF NOT EXISTS resolution_last_attempt_at TIMESTAMPTZ NULL,
  ADD COLUMN IF NOT EXISTS resolution_attempt_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS resolution_retry_after_at TIMESTAMPTZ NULL;

CREATE TABLE IF NOT EXISTS public.telegram_session_slots (
  slot_key TEXT PRIMARY KEY,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  priority INTEGER NOT NULL DEFAULT 100,
  cooldown_until TIMESTAMPTZ NULL,
  last_flood_wait_seconds INTEGER NULL,
  min_resolve_interval_seconds INTEGER NOT NULL DEFAULT 5,
  max_concurrent_resolves INTEGER NOT NULL DEFAULT 1,
  last_dispatch_at TIMESTAMPTZ NULL,
  last_success_at TIMESTAMPTZ NULL,
  last_error_at TIMESTAMPTZ NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
);

CREATE TABLE IF NOT EXISTS public.telegram_source_resolution_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  channel_id UUID NOT NULL REFERENCES public.telegram_channels(id) ON DELETE CASCADE,
  job_kind TEXT NOT NULL DEFAULT 'resolve_metadata',
  priority INTEGER NOT NULL DEFAULT 30,
  status TEXT NOT NULL DEFAULT 'pending',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  lease_token TEXT NULL,
  lease_expires_at TIMESTAMPTZ NULL,
  last_error_code TEXT NULL,
  last_error_message TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  finished_at TIMESTAMPTZ NULL,
  CONSTRAINT telegram_source_resolution_jobs_status_check
    CHECK (status IN ('pending', 'leased', 'done', 'dead_letter'))
);

CREATE UNIQUE INDEX IF NOT EXISTS telegram_source_resolution_jobs_channel_kind_idx
  ON public.telegram_source_resolution_jobs (channel_id, job_kind);

CREATE INDEX IF NOT EXISTS telegram_source_resolution_jobs_due_idx
  ON public.telegram_source_resolution_jobs (status, next_attempt_at, priority);

CREATE INDEX IF NOT EXISTS telegram_source_resolution_jobs_lease_idx
  ON public.telegram_source_resolution_jobs (status, lease_expires_at);

CREATE TABLE IF NOT EXISTS public.telegram_channel_peer_refs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  channel_id UUID NOT NULL REFERENCES public.telegram_channels(id) ON DELETE CASCADE,
  session_slot TEXT NOT NULL REFERENCES public.telegram_session_slots(slot_key) ON DELETE CASCADE,
  peer_id BIGINT NOT NULL,
  access_hash BIGINT NOT NULL,
  resolved_username TEXT NULL,
  resolved_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  last_verified_at TIMESTAMPTZ NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
);

CREATE UNIQUE INDEX IF NOT EXISTS telegram_channel_peer_refs_channel_slot_idx
  ON public.telegram_channel_peer_refs (channel_id, session_slot);

CREATE INDEX IF NOT EXISTS telegram_channel_peer_refs_slot_verified_idx
  ON public.telegram_channel_peer_refs (session_slot, last_verified_at);

INSERT INTO public.telegram_session_slots (
  slot_key,
  is_active,
  priority,
  min_resolve_interval_seconds,
  max_concurrent_resolves
)
VALUES ('primary', TRUE, 100, 5, 1)
ON CONFLICT (slot_key) DO NOTHING;
