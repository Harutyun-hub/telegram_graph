-- Social Media Activities v1
--
-- Design notes:
-- 1. Keeps legacy n8n-owned tables untouched.
-- 2. Derives social_entities from companies via idempotent trigger.
-- 3. Seeds social_entity_accounts from verified company columns that are
--    already used by the legacy social workflow.

CREATE TABLE IF NOT EXISTS public.social_entities (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  legacy_company_id UUID NOT NULL UNIQUE REFERENCES public.companies(id),
  company_key TEXT NULL,
  name TEXT NOT NULL,
  industry TEXT NULL,
  website TEXT NULL,
  logo_url TEXT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  last_company_sync_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS social_entities_idx_active
ON public.social_entities (is_active, name);

CREATE TABLE IF NOT EXISTS public.social_entity_accounts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id UUID NOT NULL REFERENCES public.social_entities(id) ON DELETE CASCADE,
  platform TEXT NOT NULL,
  account_handle TEXT NULL,
  account_external_id TEXT NULL,
  domain TEXT NULL,
  import_source TEXT NOT NULL DEFAULT 'manual',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT social_entity_accounts_platform_check
    CHECK (platform IN ('facebook', 'instagram', 'google', 'tiktok'))
);

CREATE UNIQUE INDEX IF NOT EXISTS social_entity_accounts_uq_entity_platform
ON public.social_entity_accounts (entity_id, platform);

CREATE INDEX IF NOT EXISTS social_entity_accounts_idx_lookup
ON public.social_entity_accounts (platform, is_active);

CREATE TABLE IF NOT EXISTS public.social_activities (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id UUID NOT NULL REFERENCES public.social_entities(id) ON DELETE CASCADE,
  account_id UUID NULL REFERENCES public.social_entity_accounts(id) ON DELETE SET NULL,
  activity_uid TEXT NOT NULL UNIQUE,
  platform TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  provider_item_id TEXT NULL,
  source_url TEXT NOT NULL,
  text_content TEXT NULL,
  published_at TIMESTAMPTZ NULL,
  author_handle TEXT NULL,
  cta_type TEXT NULL,
  content_format TEXT NULL,
  region_name TEXT NULL,
  engagement_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
  assets JSONB NOT NULL DEFAULT '[]'::jsonb,
  provider_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  normalization_version TEXT NOT NULL DEFAULT 'social-v1',
  ingest_status TEXT NOT NULL DEFAULT 'collected',
  analysis_status TEXT NOT NULL DEFAULT 'pending',
  graph_status TEXT NOT NULL DEFAULT 'not_ready',
  analysis_version TEXT NULL,
  graph_projection_version TEXT NULL,
  last_error TEXT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT social_activities_platform_check
    CHECK (platform IN ('facebook', 'instagram', 'google', 'tiktok')),
  CONSTRAINT social_activities_ingest_status_check
    CHECK (ingest_status IN ('collected', 'normalized', 'failed', 'dead_letter')),
  CONSTRAINT social_activities_analysis_status_check
    CHECK (analysis_status IN ('not_needed', 'pending', 'analyzed', 'failed', 'dead_letter')),
  CONSTRAINT social_activities_graph_status_check
    CHECK (graph_status IN ('not_ready', 'pending', 'synced', 'failed', 'dead_letter'))
);

CREATE INDEX IF NOT EXISTS social_activities_idx_entity_platform_date
ON public.social_activities (entity_id, platform, published_at DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS social_activities_idx_analysis_queue
ON public.social_activities (analysis_status, ingest_status, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS social_activities_idx_graph_queue
ON public.social_activities (graph_status, analysis_status, last_seen_at DESC);

CREATE TABLE IF NOT EXISTS public.social_activity_analysis (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  activity_id UUID NOT NULL UNIQUE REFERENCES public.social_activities(id) ON DELETE CASCADE,
  entity_id UUID NOT NULL REFERENCES public.social_entities(id) ON DELETE CASCADE,
  platform TEXT NOT NULL,
  analysis_version TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  model TEXT NOT NULL,
  summary TEXT NULL,
  marketing_intent TEXT NULL,
  sentiment TEXT NULL,
  sentiment_score DOUBLE PRECISION NULL,
  analysis_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_model_output JSONB NOT NULL DEFAULT '{}'::jsonb,
  analyzed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS social_activity_analysis_idx_entity_platform
ON public.social_activity_analysis (entity_id, platform, analyzed_at DESC);

CREATE TABLE IF NOT EXISTS public.social_ingest_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id UUID NULL REFERENCES public.social_entities(id) ON DELETE SET NULL,
  platform TEXT NULL,
  run_kind TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'running',
  metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
  error TEXT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT social_ingest_runs_kind_check
    CHECK (run_kind IN ('seed', 'collect', 'analysis', 'graph', 'runtime')),
  CONSTRAINT social_ingest_runs_status_check
    CHECK (status IN ('running', 'succeeded', 'failed'))
);

CREATE INDEX IF NOT EXISTS social_ingest_runs_idx_recent
ON public.social_ingest_runs (started_at DESC);

CREATE TABLE IF NOT EXISTS public.social_processing_failures (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  activity_id UUID NULL REFERENCES public.social_activities(id) ON DELETE CASCADE,
  entity_id UUID NULL REFERENCES public.social_entities(id) ON DELETE CASCADE,
  platform TEXT NULL,
  stage TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 1,
  last_error TEXT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  is_dead_letter BOOLEAN NOT NULL DEFAULT FALSE,
  resolved_at TIMESTAMPTZ NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT social_processing_failures_stage_check
    CHECK (stage IN ('ingest', 'analysis', 'graph'))
);

CREATE UNIQUE INDEX IF NOT EXISTS social_processing_failures_uq_scope
ON public.social_processing_failures (stage, scope_key);

CREATE INDEX IF NOT EXISTS social_processing_failures_idx_retry
ON public.social_processing_failures (stage, is_dead_letter, next_retry_at);

CREATE TABLE IF NOT EXISTS public.social_runtime_settings (
  key TEXT PRIMARY KEY,
  value JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION public.set_social_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_social_entities_updated_at ON public.social_entities;
CREATE TRIGGER trg_social_entities_updated_at
BEFORE UPDATE ON public.social_entities
FOR EACH ROW
EXECUTE FUNCTION public.set_social_updated_at();

DROP TRIGGER IF EXISTS trg_social_entity_accounts_updated_at ON public.social_entity_accounts;
CREATE TRIGGER trg_social_entity_accounts_updated_at
BEFORE UPDATE ON public.social_entity_accounts
FOR EACH ROW
EXECUTE FUNCTION public.set_social_updated_at();

DROP TRIGGER IF EXISTS trg_social_activities_updated_at ON public.social_activities;
CREATE TRIGGER trg_social_activities_updated_at
BEFORE UPDATE ON public.social_activities
FOR EACH ROW
EXECUTE FUNCTION public.set_social_updated_at();

DROP TRIGGER IF EXISTS trg_social_activity_analysis_updated_at ON public.social_activity_analysis;
CREATE TRIGGER trg_social_activity_analysis_updated_at
BEFORE UPDATE ON public.social_activity_analysis
FOR EACH ROW
EXECUTE FUNCTION public.set_social_updated_at();

DROP TRIGGER IF EXISTS trg_social_ingest_runs_updated_at ON public.social_ingest_runs;
CREATE TRIGGER trg_social_ingest_runs_updated_at
BEFORE UPDATE ON public.social_ingest_runs
FOR EACH ROW
EXECUTE FUNCTION public.set_social_updated_at();

DROP TRIGGER IF EXISTS trg_social_processing_failures_updated_at ON public.social_processing_failures;
CREATE TRIGGER trg_social_processing_failures_updated_at
BEFORE UPDATE ON public.social_processing_failures
FOR EACH ROW
EXECUTE FUNCTION public.set_social_updated_at();

CREATE OR REPLACE FUNCTION public.sync_social_entity_from_company()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO public.social_entities (
    legacy_company_id,
    company_key,
    name,
    industry,
    website,
    logo_url,
    metadata,
    is_active,
    last_company_sync_at
  )
  VALUES (
    NEW.id,
    NEW.company_key,
    NEW.name,
    NEW.industry,
    NEW.website,
    NEW.logo_url,
    COALESCE(NEW.metadata, '{}'::jsonb),
    COALESCE(NEW.is_active, TRUE),
    NOW()
  )
  ON CONFLICT (legacy_company_id) DO UPDATE
  SET
    company_key = EXCLUDED.company_key,
    name = EXCLUDED.name,
    industry = EXCLUDED.industry,
    website = EXCLUDED.website,
    logo_url = EXCLUDED.logo_url,
    metadata = EXCLUDED.metadata,
    is_active = EXCLUDED.is_active,
    last_company_sync_at = NOW();

  RETURN NEW;
END;
$$;

INSERT INTO public.social_entities (
  legacy_company_id,
  company_key,
  name,
  industry,
  website,
  logo_url,
  metadata,
  is_active,
  last_company_sync_at
)
SELECT
  c.id,
  c.company_key,
  c.name,
  c.industry,
  c.website,
  c.logo_url,
  COALESCE(c.metadata, '{}'::jsonb),
  COALESCE(c.is_active, TRUE),
  NOW()
FROM public.companies AS c
ON CONFLICT (legacy_company_id) DO UPDATE
SET
  company_key = EXCLUDED.company_key,
  name = EXCLUDED.name,
  industry = EXCLUDED.industry,
  website = EXCLUDED.website,
  logo_url = EXCLUDED.logo_url,
  metadata = EXCLUDED.metadata,
  is_active = EXCLUDED.is_active,
  last_company_sync_at = NOW();

INSERT INTO public.social_entity_accounts (
  entity_id,
  platform,
  account_external_id,
  account_handle,
  domain,
  import_source,
  is_active,
  metadata
)
SELECT
  se.id,
  'facebook',
  NULLIF(BTRIM(c.facebook_page_id), ''),
  NULL,
  NULL,
  'companies_seed',
  COALESCE(c.is_active, TRUE),
  jsonb_build_object('seeded_from', 'companies.facebook_page_id')
FROM public.companies AS c
JOIN public.social_entities AS se
  ON se.legacy_company_id = c.id
WHERE NULLIF(BTRIM(c.facebook_page_id), '') IS NOT NULL
ON CONFLICT (entity_id, platform) DO UPDATE
SET
  account_external_id = EXCLUDED.account_external_id,
  import_source = EXCLUDED.import_source,
  is_active = EXCLUDED.is_active,
  metadata = EXCLUDED.metadata;

INSERT INTO public.social_entity_accounts (
  entity_id,
  platform,
  account_external_id,
  account_handle,
  domain,
  import_source,
  is_active,
  metadata
)
SELECT
  se.id,
  'instagram',
  NULL,
  NULLIF(BTRIM(c.instagram_username), ''),
  NULL,
  'companies_seed',
  COALESCE(c.is_active, TRUE),
  jsonb_build_object('seeded_from', 'companies.instagram_username')
FROM public.companies AS c
JOIN public.social_entities AS se
  ON se.legacy_company_id = c.id
WHERE NULLIF(BTRIM(c.instagram_username), '') IS NOT NULL
ON CONFLICT (entity_id, platform) DO UPDATE
SET
  account_handle = EXCLUDED.account_handle,
  import_source = EXCLUDED.import_source,
  is_active = EXCLUDED.is_active,
  metadata = EXCLUDED.metadata;

INSERT INTO public.social_entity_accounts (
  entity_id,
  platform,
  account_external_id,
  account_handle,
  domain,
  import_source,
  is_active,
  metadata
)
SELECT
  se.id,
  'google',
  NULL,
  NULL,
  NULLIF(BTRIM(c.google_ads_domain), ''),
  'companies_seed',
  COALESCE(c.is_active, TRUE),
  jsonb_build_object('seeded_from', 'companies.google_ads_domain')
FROM public.companies AS c
JOIN public.social_entities AS se
  ON se.legacy_company_id = c.id
WHERE NULLIF(BTRIM(c.google_ads_domain), '') IS NOT NULL
ON CONFLICT (entity_id, platform) DO UPDATE
SET
  domain = EXCLUDED.domain,
  import_source = EXCLUDED.import_source,
  is_active = EXCLUDED.is_active,
  metadata = EXCLUDED.metadata;

INSERT INTO public.social_runtime_settings (key, value)
VALUES
  (
    'scheduler',
    jsonb_build_object(
      'is_active', false,
      'interval_minutes', 360,
      'updated_at', NOW()
    )
  ),
  (
    'scrapecreators',
    jsonb_build_object(
      'max_pages', 3,
      'page_size', 50,
      'tiktok_enabled', false,
      'updated_at', NOW()
    )
  )
ON CONFLICT (key) DO NOTHING;

DROP TRIGGER IF EXISTS trg_companies_sync_social_entities ON public.companies;
CREATE TRIGGER trg_companies_sync_social_entities
AFTER INSERT OR UPDATE ON public.companies
FOR EACH ROW
EXECUTE FUNCTION public.sync_social_entity_from_company();
