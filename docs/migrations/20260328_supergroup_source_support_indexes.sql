-- Optional post-deploy follow-up for performance tuning.
--
-- Run this only after the primary schema migration is deployed successfully.
-- These statements must be executed outside a transaction because they use
-- CREATE INDEX CONCURRENTLY to avoid write-blocking on production tables.

CREATE INDEX CONCURRENTLY IF NOT EXISTS telegram_channels_idx_resolution
ON public.telegram_channels (is_active, resolution_status, source_type);

CREATE INDEX CONCURRENTLY IF NOT EXISTS telegram_posts_idx_entry_kind_posted
ON public.telegram_posts (entry_kind, posted_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS telegram_comments_idx_thread_top
ON public.telegram_comments (thread_top_message_id, posted_at DESC)
WHERE thread_top_message_id IS NOT NULL;
