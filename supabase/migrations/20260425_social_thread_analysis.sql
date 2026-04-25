ALTER TABLE public.social_activities
  ADD COLUMN IF NOT EXISTS parent_activity_uid TEXT NULL;

CREATE INDEX IF NOT EXISTS social_activities_idx_parent_activity_uid
ON public.social_activities (parent_activity_uid)
WHERE parent_activity_uid IS NOT NULL;

CREATE INDEX IF NOT EXISTS social_activities_idx_analysis_parent_queue
ON public.social_activities (analysis_status, ingest_status, source_kind, parent_activity_uid, last_seen_at DESC);

UPDATE public.social_activities
SET parent_activity_uid = CONCAT(platform, ':post:', provider_payload ->> '__parent_post_id')
WHERE source_kind = 'comment'
  AND parent_activity_uid IS NULL
  AND NULLIF(BTRIM(provider_payload ->> '__parent_post_id'), '') IS NOT NULL;

UPDATE public.social_activities
SET
  analysis_status = 'not_needed',
  graph_status = 'not_ready',
  analysis_claimed_at = NULL,
  analysis_claimed_by = NULL,
  graph_claimed_at = NULL,
  graph_claimed_by = NULL,
  updated_at = NOW()
WHERE source_kind = 'comment'
  AND analysis_status <> 'not_needed';
