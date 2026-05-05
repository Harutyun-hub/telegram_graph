-- Website promotion monitor support.
-- Uses existing social_entities.website as the source of truth.

ALTER TABLE public.social_activities
  DROP CONSTRAINT IF EXISTS social_activities_platform_check;

ALTER TABLE public.social_activities
  ADD CONSTRAINT social_activities_platform_check
  CHECK (platform IN ('facebook', 'instagram', 'google', 'tiktok', 'website'));

ALTER TABLE public.social_ingest_runs
  DROP CONSTRAINT IF EXISTS social_ingest_runs_kind_check;

ALTER TABLE public.social_ingest_runs
  ADD CONSTRAINT social_ingest_runs_kind_check
  CHECK (run_kind IN ('seed', 'collect', 'website_research', 'analysis', 'graph', 'runtime'));

ALTER TABLE public.social_processing_failures
  DROP CONSTRAINT IF EXISTS social_processing_failures_stage_check;

ALTER TABLE public.social_processing_failures
  ADD CONSTRAINT social_processing_failures_stage_check
  CHECK (stage IN ('ingest', 'website_research', 'analysis', 'graph'));

CREATE INDEX IF NOT EXISTS social_activities_idx_website_promotions
ON public.social_activities (entity_id, content_format, last_seen_at DESC)
WHERE platform = 'website';
