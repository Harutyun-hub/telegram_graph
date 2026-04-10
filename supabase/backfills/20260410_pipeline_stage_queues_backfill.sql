-- Pipeline stage queue backfill
--
-- Apply to staging first after supabase/migrations/20260410_pipeline_stage_queues.sql.
-- This file is intentionally standalone and should not be embedded in a schema migration.
--
-- Assumptions to verify before production apply:
-- - public.telegram_posts.neo4j_synced is the live graph-sync flag
-- - AI-processing candidate posts/comments should continue to follow current runtime semantics:
--   * posts: is_processed = false AND text IS NOT NULL
--   * comment groups: source rows from telegram_comments where is_processed = false AND text IS NOT NULL

INSERT INTO public.ai_post_jobs (
  post_id,
  status,
  next_attempt_at,
  created_at,
  updated_at
)
SELECT
  tp.id,
  'pending',
  timezone('utc', now()),
  timezone('utc', now()),
  timezone('utc', now())
FROM public.telegram_posts AS tp
WHERE tp.is_processed = FALSE
  AND tp.text IS NOT NULL
ON CONFLICT (post_id) DO NOTHING;

INSERT INTO public.ai_comment_group_jobs (
  scope_key,
  telegram_user_id,
  channel_id,
  post_id,
  status,
  next_attempt_at,
  created_at,
  updated_at
)
SELECT
  CONCAT(
    COALESCE(tc.telegram_user_id::text, 'anonymous'),
    ':',
    COALESCE(tc.channel_id::text, 'unknown'),
    ':',
    COALESCE(tc.post_id::text, 'unknown')
  ) AS scope_key,
  tc.telegram_user_id,
  tc.channel_id,
  tc.post_id,
  'pending',
  timezone('utc', now()),
  timezone('utc', now()),
  timezone('utc', now())
FROM public.telegram_comments AS tc
WHERE tc.is_processed = FALSE
  AND tc.text IS NOT NULL
  AND tc.channel_id IS NOT NULL
  AND tc.post_id IS NOT NULL
GROUP BY
  tc.telegram_user_id,
  tc.channel_id,
  tc.post_id
ON CONFLICT (scope_key) DO NOTHING;

INSERT INTO public.neo4j_sync_jobs (
  post_id,
  status,
  next_attempt_at,
  created_at,
  updated_at
)
SELECT
  tp.id,
  'pending',
  timezone('utc', now()),
  timezone('utc', now()),
  timezone('utc', now())
FROM public.telegram_posts AS tp
WHERE tp.is_processed = TRUE
  AND tp.neo4j_synced = FALSE
ON CONFLICT (post_id) DO NOTHING;
