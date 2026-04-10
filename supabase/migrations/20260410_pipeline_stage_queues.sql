-- Pipeline stage queue tables
--
-- Staging-first rollout:
-- 1. Apply this schema migration to staging first.
-- 2. Run the standalone backfill SQL in supabase/backfills/ on staging.
-- 3. Validate claim/ack worker behavior on staging before any production apply.
--
-- Assumptions to verify against the live schema before production apply:
-- - public.telegram_posts.id is UUID
-- - public.telegram_comments.post_id is UUID referencing public.telegram_posts(id)
-- - public.telegram_comments.channel_id is UUID referencing public.telegram_channels(id)
-- - public.telegram_posts currently uses neo4j_synced (not is_synced_to_neo4j)

CREATE TABLE IF NOT EXISTS public.ai_post_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  post_id UUID NOT NULL REFERENCES public.telegram_posts(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'pending',
  lease_owner TEXT NULL,
  lease_token UUID NULL,
  lease_expires_at TIMESTAMPTZ NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  last_error TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  CONSTRAINT ai_post_jobs_status_check
    CHECK (status IN ('pending', 'leased', 'done', 'failed', 'dead_lettered')),
  CONSTRAINT ai_post_jobs_attempt_count_check
    CHECK (attempt_count >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS ai_post_jobs_post_id_idx
  ON public.ai_post_jobs (post_id);

CREATE INDEX IF NOT EXISTS ai_post_jobs_claimable_idx
  ON public.ai_post_jobs (status, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS ai_post_jobs_lease_expiry_idx
  ON public.ai_post_jobs (status, lease_expires_at);

CREATE INDEX IF NOT EXISTS ai_post_jobs_lease_token_idx
  ON public.ai_post_jobs (lease_token);

CREATE TABLE IF NOT EXISTS public.ai_comment_group_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  scope_key TEXT NOT NULL,
  telegram_user_id BIGINT NULL,
  channel_id UUID NOT NULL REFERENCES public.telegram_channels(id) ON DELETE CASCADE,
  post_id UUID NOT NULL REFERENCES public.telegram_posts(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'pending',
  lease_owner TEXT NULL,
  lease_token UUID NULL,
  lease_expires_at TIMESTAMPTZ NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  last_error TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  CONSTRAINT ai_comment_group_jobs_scope_key_check
    CHECK (length(btrim(scope_key)) > 0),
  CONSTRAINT ai_comment_group_jobs_status_check
    CHECK (status IN ('pending', 'leased', 'done', 'failed', 'dead_lettered')),
  CONSTRAINT ai_comment_group_jobs_attempt_count_check
    CHECK (attempt_count >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS ai_comment_group_jobs_scope_key_idx
  ON public.ai_comment_group_jobs (scope_key);

CREATE INDEX IF NOT EXISTS ai_comment_group_jobs_lookup_idx
  ON public.ai_comment_group_jobs (channel_id, post_id, telegram_user_id);

CREATE INDEX IF NOT EXISTS ai_comment_group_jobs_claimable_idx
  ON public.ai_comment_group_jobs (status, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS ai_comment_group_jobs_lease_expiry_idx
  ON public.ai_comment_group_jobs (status, lease_expires_at);

CREATE INDEX IF NOT EXISTS ai_comment_group_jobs_lease_token_idx
  ON public.ai_comment_group_jobs (lease_token);

CREATE TABLE IF NOT EXISTS public.neo4j_sync_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  post_id UUID NOT NULL REFERENCES public.telegram_posts(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'pending',
  lease_owner TEXT NULL,
  lease_token UUID NULL,
  lease_expires_at TIMESTAMPTZ NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  last_error TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
  CONSTRAINT neo4j_sync_jobs_status_check
    CHECK (status IN ('pending', 'leased', 'done', 'failed', 'dead_lettered')),
  CONSTRAINT neo4j_sync_jobs_attempt_count_check
    CHECK (attempt_count >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS neo4j_sync_jobs_post_id_idx
  ON public.neo4j_sync_jobs (post_id);

CREATE INDEX IF NOT EXISTS neo4j_sync_jobs_claimable_idx
  ON public.neo4j_sync_jobs (status, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS neo4j_sync_jobs_lease_expiry_idx
  ON public.neo4j_sync_jobs (status, lease_expires_at);

CREATE INDEX IF NOT EXISTS neo4j_sync_jobs_lease_token_idx
  ON public.neo4j_sync_jobs (lease_token);
