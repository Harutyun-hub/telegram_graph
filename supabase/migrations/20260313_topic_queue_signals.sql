-- Topic queue signal hardening for emerging-topic visibility gates.

  ALTER TABLE public.topic_review_queue
    ADD COLUMN IF NOT EXISTS last_scope_key TEXT NULL,
    ADD COLUMN IF NOT EXISTS seen_scope_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS seen_content_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS seen_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS seen_user_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS distinct_scope_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS distinct_content_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS distinct_channel_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS distinct_user_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS visibility_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS visibility_state TEXT NOT NULL DEFAULT 'candidate';

  DO $$
  BEGIN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conname = 'topic_review_queue_visibility_state_check'
    ) THEN
      ALTER TABLE public.topic_review_queue
        ADD CONSTRAINT topic_review_queue_visibility_state_check
        CHECK (visibility_state IN ('candidate', 'emerging_visible', 'approved', 'rejected'));
    END IF;
  END
  $$;

  CREATE INDEX IF NOT EXISTS topic_review_queue_idx_visibility_last_seen
  ON public.topic_review_queue (status, visibility_state, last_seen_at DESC);
