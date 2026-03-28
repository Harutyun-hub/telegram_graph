-- Supergroup source typing + thread-aware storage support.
--
-- Safer production rollout notes:
-- 1. This migration only adds columns/defaults and NOT VALID check constraints.
-- 2. It intentionally avoids table-wide backfills and non-concurrent index builds.
-- 3. Existing rows may remain NULL in the new columns until touched by the app, which is safe:
--    the runtime uses COALESCE/default handling for all new fields.
-- 4. Optional indexes can be added later in a low-risk follow-up migration using
--    CREATE INDEX CONCURRENTLY outside a transaction.

ALTER TABLE public.telegram_channels
  ADD COLUMN IF NOT EXISTS source_type TEXT DEFAULT 'channel',
  ADD COLUMN IF NOT EXISTS resolution_status TEXT DEFAULT 'resolved',
  ADD COLUMN IF NOT EXISTS last_resolution_error TEXT NULL,
  ADD COLUMN IF NOT EXISTS telegram_peer_flags JSONB DEFAULT '{}'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'telegram_channels_source_type_check'
  ) THEN
    ALTER TABLE public.telegram_channels
      ADD CONSTRAINT telegram_channels_source_type_check
      CHECK (source_type IN ('channel', 'supergroup', 'pending'))
      NOT VALID;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'telegram_channels_resolution_status_check'
  ) THEN
    ALTER TABLE public.telegram_channels
      ADD CONSTRAINT telegram_channels_resolution_status_check
      CHECK (resolution_status IN ('pending', 'resolved', 'error'))
      NOT VALID;
  END IF;
END
$$;

ALTER TABLE public.telegram_posts
  ADD COLUMN IF NOT EXISTS entry_kind TEXT DEFAULT 'broadcast_post',
  ADD COLUMN IF NOT EXISTS thread_message_count INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS thread_participant_count INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'telegram_posts_entry_kind_check'
  ) THEN
    ALTER TABLE public.telegram_posts
      ADD CONSTRAINT telegram_posts_entry_kind_check
      CHECK (entry_kind IN ('broadcast_post', 'thread_anchor'))
      NOT VALID;
  END IF;
END
$$;

ALTER TABLE public.telegram_comments
  ADD COLUMN IF NOT EXISTS message_kind TEXT DEFAULT 'discussion_comment',
  ADD COLUMN IF NOT EXISTS is_thread_root BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS thread_top_message_id BIGINT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'telegram_comments_message_kind_check'
  ) THEN
    ALTER TABLE public.telegram_comments
      ADD CONSTRAINT telegram_comments_message_kind_check
      CHECK (message_kind IN ('discussion_comment', 'group_message'))
      NOT VALID;
  END IF;
END
$$;
