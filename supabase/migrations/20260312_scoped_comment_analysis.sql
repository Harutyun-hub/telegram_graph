-- Scoped comment-analysis safety indexes
-- Applies to rows used for per-user-per-post scope:
--   content_type = 'batch' AND content_id IS NOT NULL

-- 1) Enforce one logical scoped row per (channel, user, post)
CREATE UNIQUE INDEX IF NOT EXISTS ai_analysis_uq_batch_scoped_comment
ON public.ai_analysis (channel_id, telegram_user_id, content_id)
WHERE content_type = 'batch'
  AND content_id IS NOT NULL
  AND telegram_user_id IS NOT NULL;

-- 2) Speed up post-bundle scoped lookups
CREATE INDEX IF NOT EXISTS ai_analysis_idx_batch_scoped_lookup
ON public.ai_analysis (channel_id, content_id, telegram_user_id, created_at DESC)
WHERE content_type = 'batch'
  AND content_id IS NOT NULL;

-- 3) Speed up legacy fallback lookups while migration is in progress
CREATE INDEX IF NOT EXISTS ai_analysis_idx_batch_legacy_fallback
ON public.ai_analysis (channel_id, telegram_user_id, created_at DESC)
WHERE content_type = 'batch'
  AND content_id IS NULL;
