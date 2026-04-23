ALTER TABLE public.companies
  ADD COLUMN IF NOT EXISTS facebook_url TEXT;

UPDATE public.companies AS c
SET facebook_url = source_values.facebook_url
FROM (
  SELECT
    se.legacy_company_id,
    COALESCE(
      NULLIF(BTRIM(sea.metadata ->> 'page_url'), ''),
      NULLIF(BTRIM(sea.metadata ->> 'source_url'), '')
    ) AS facebook_url
  FROM public.social_entity_accounts AS sea
  JOIN public.social_entities AS se
    ON se.id = sea.entity_id
  WHERE sea.platform = 'facebook'
) AS source_values
WHERE c.id = source_values.legacy_company_id
  AND NULLIF(BTRIM(c.facebook_url), '') IS NULL
  AND source_values.facebook_url IS NOT NULL;

ALTER TABLE public.social_entity_accounts
  ADD COLUMN IF NOT EXISTS source_kind TEXT;

UPDATE public.social_entity_accounts
SET source_kind = CASE platform
  WHEN 'facebook' THEN 'meta_ads'
  WHEN 'instagram' THEN 'instagram_profile'
  WHEN 'google' THEN 'google_domain'
  WHEN 'tiktok' THEN 'tiktok_profile'
  ELSE 'meta_ads'
END
WHERE NULLIF(BTRIM(source_kind), '') IS NULL;

ALTER TABLE public.social_entity_accounts
  ALTER COLUMN source_kind SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'social_entity_accounts_source_kind_check'
  ) THEN
    ALTER TABLE public.social_entity_accounts
      ADD CONSTRAINT social_entity_accounts_source_kind_check
      CHECK (
        (platform = 'facebook' AND source_kind IN ('facebook_page', 'meta_ads'))
        OR (platform = 'instagram' AND source_kind = 'instagram_profile')
        OR (platform = 'google' AND source_kind = 'google_domain')
        OR (platform = 'tiktok' AND source_kind = 'tiktok_profile')
      );
  END IF;
END $$;

DROP INDEX IF EXISTS social_entity_accounts_uq_entity_platform;

CREATE UNIQUE INDEX IF NOT EXISTS social_entity_accounts_uq_entity_platform_kind
ON public.social_entity_accounts (entity_id, platform, source_kind);

CREATE INDEX IF NOT EXISTS social_entity_accounts_idx_platform_kind_active
ON public.social_entity_accounts (platform, source_kind, is_active);
