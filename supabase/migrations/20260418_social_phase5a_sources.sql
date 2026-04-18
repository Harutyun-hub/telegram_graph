-- Social Phase 5A source registry, canonical provenance, and producer metrics.
--
-- This migration is intentionally additive:
-- 1. `social_entity_accounts` remains the table name while it evolves into the
--    source registry.
-- 2. Existing `(entity_id, platform)` uniqueness stays in place for backward
--    compatibility until every write path is cut over to `source_key`.
-- 3. Legacy activity UIDs are preserved; new canonical identity fields are
--    added alongside them.

ALTER TABLE public.social_entity_accounts
  ADD COLUMN IF NOT EXISTS provider_key TEXT NOT NULL DEFAULT 'scrapecreators',
  ADD COLUMN IF NOT EXISTS source_key TEXT NULL,
  ADD COLUMN IF NOT EXISTS target_type TEXT NULL,
  ADD COLUMN IF NOT EXISTS content_types JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS cadence_minutes INTEGER NULL,
  ADD COLUMN IF NOT EXISTS next_collect_after TIMESTAMPTZ NULL,
  ADD COLUMN IF NOT EXISTS provider_metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'social_entity_accounts_provider_key_check'
  ) THEN
    ALTER TABLE public.social_entity_accounts
      ADD CONSTRAINT social_entity_accounts_provider_key_check
      CHECK (provider_key IN ('scrapecreators'));
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'social_entity_accounts_target_type_check'
  ) THEN
    ALTER TABLE public.social_entity_accounts
      ADD CONSTRAINT social_entity_accounts_target_type_check
      CHECK (target_type IN ('page_id', 'handle', 'domain'));
  END IF;
END;
$$;

UPDATE public.social_entity_accounts
SET
  provider_key = COALESCE(NULLIF(BTRIM(provider_key), ''), 'scrapecreators'),
  target_type = COALESCE(
    NULLIF(BTRIM(target_type), ''),
    CASE
      WHEN platform = 'facebook' THEN 'page_id'
      WHEN platform = 'google' THEN 'domain'
      ELSE 'handle'
    END
  ),
  content_types = CASE
    WHEN jsonb_typeof(content_types) = 'array' AND jsonb_array_length(content_types) > 0 THEN content_types
    ELSE CASE
      WHEN platform = 'facebook' THEN '["ad"]'::jsonb
      WHEN platform = 'google' THEN '["ad"]'::jsonb
      WHEN platform = 'instagram' THEN '["post"]'::jsonb
      WHEN platform = 'tiktok' THEN '["video"]'::jsonb
      ELSE '[]'::jsonb
    END
  END,
  provider_metadata = COALESCE(provider_metadata, '{}'::jsonb),
  next_collect_after = COALESCE(next_collect_after, last_collected_at, NOW())
WHERE TRUE;

UPDATE public.social_entity_accounts
SET source_key = CONCAT(
    provider_key,
    ':',
    platform,
    ':',
    target_type,
    ':',
    CASE
      WHEN target_type = 'page_id' THEN NULLIF(BTRIM(account_external_id), '')
      WHEN target_type = 'handle' THEN LOWER(NULLIF(BTRIM(account_handle), ''))
      WHEN target_type = 'domain' THEN LOWER(REGEXP_REPLACE(NULLIF(BTRIM(domain), ''), '^https?://', ''))
      ELSE NULL
    END
  )
WHERE source_key IS NULL
  AND CASE
    WHEN target_type = 'page_id' THEN NULLIF(BTRIM(account_external_id), '')
    WHEN target_type = 'handle' THEN LOWER(NULLIF(BTRIM(account_handle), ''))
    WHEN target_type = 'domain' THEN LOWER(REGEXP_REPLACE(NULLIF(BTRIM(domain), ''), '^https?://', ''))
    ELSE NULL
  END IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS social_entity_accounts_uq_source_key
ON public.social_entity_accounts (source_key)
WHERE source_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS social_entity_accounts_idx_due_source
ON public.social_entity_accounts (provider_key, platform, is_active, next_collect_after);

ALTER TABLE public.social_activities
  ADD COLUMN IF NOT EXISTS provider_key TEXT NOT NULL DEFAULT 'scrapecreators',
  ADD COLUMN IF NOT EXISTS source_key TEXT NULL,
  ADD COLUMN IF NOT EXISTS provider_context JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'social_activities_provider_key_check'
  ) THEN
    ALTER TABLE public.social_activities
      ADD CONSTRAINT social_activities_provider_key_check
      CHECK (provider_key IN ('scrapecreators'));
  END IF;
END;
$$;

UPDATE public.social_activities AS activity
SET
  provider_key = COALESCE(NULLIF(BTRIM(activity.provider_key), ''), 'scrapecreators'),
  source_key = COALESCE(activity.source_key, source.source_key),
  provider_context = COALESCE(activity.provider_context, '{}'::jsonb)
FROM public.social_entity_accounts AS source
WHERE activity.account_id = source.id;

CREATE INDEX IF NOT EXISTS social_activities_idx_source_key
ON public.social_activities (source_key, published_at DESC NULLS LAST);

CREATE UNIQUE INDEX IF NOT EXISTS social_activities_uq_canonical_identity
ON public.social_activities (provider_key, platform, source_key, provider_item_id, source_kind)
WHERE source_key IS NOT NULL
  AND provider_item_id IS NOT NULL;

ALTER TABLE public.social_ingest_runs
  ADD COLUMN IF NOT EXISTS account_id UUID NULL REFERENCES public.social_entity_accounts(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS provider_key TEXT NULL;

ALTER TABLE public.social_processing_failures
  ADD COLUMN IF NOT EXISTS account_id UUID NULL REFERENCES public.social_entity_accounts(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS provider_key TEXT NULL;

CREATE INDEX IF NOT EXISTS social_processing_failures_idx_source_scope
ON public.social_processing_failures (stage, scope_key, account_id);
