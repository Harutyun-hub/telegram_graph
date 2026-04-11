-- AI processing failure registry for retry backoff + dead-letter handling.

CREATE TABLE IF NOT EXISTS public.ai_processing_failures (
  id BIGSERIAL PRIMARY KEY,
  scope_type TEXT NOT NULL CHECK (scope_type IN ('comment_group', 'post')),
  scope_key TEXT NOT NULL,
  channel_id UUID NULL,
  post_id UUID NULL,
  telegram_user_id BIGINT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 1,
  last_error TEXT NULL,
  first_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  is_dead_letter BOOLEAN NOT NULL DEFAULT FALSE,
  resolved_at TIMESTAMPTZ NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ai_processing_failures_uq_scope
ON public.ai_processing_failures (scope_type, scope_key);

CREATE INDEX IF NOT EXISTS ai_processing_failures_idx_retry
ON public.ai_processing_failures (scope_type, is_dead_letter, next_retry_at);

CREATE INDEX IF NOT EXISTS ai_processing_failures_idx_post
ON public.ai_processing_failures (post_id)
WHERE post_id IS NOT NULL;
