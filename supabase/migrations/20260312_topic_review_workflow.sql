-- Topic proposal review workflow and runtime promotion aliases

CREATE TABLE IF NOT EXISTS public.topic_review_queue (
  topic_name TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
  proposed_count INTEGER NOT NULL DEFAULT 1,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  latest_evidence TEXT NULL,
  closest_category TEXT NULL,
  domain TEXT NULL,
  latest_analysis_id UUID NULL,
  latest_channel_id UUID NULL,
  latest_content_type TEXT NULL,
  approved_topic TEXT NULL,
  review_notes TEXT NULL,
  reviewed_by TEXT NULL,
  reviewed_at TIMESTAMPTZ NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS topic_review_queue_idx_status_last_seen
ON public.topic_review_queue (status, last_seen_at DESC);

CREATE TABLE IF NOT EXISTS public.topic_taxonomy_promotions (
  alias_name TEXT PRIMARY KEY,
  canonical_topic TEXT NOT NULL,
  source_topic TEXT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  notes TEXT NULL,
  promoted_by TEXT NULL,
  promoted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS topic_taxonomy_promotions_idx_active
ON public.topic_taxonomy_promotions (is_active, updated_at DESC);
