WITH remap AS (
  SELECT
    failure.id,
    CONCAT(failure.entity_id::text, ':', failure.platform, ':', account.source_kind) AS new_scope_key
  FROM public.social_processing_failures AS failure
  JOIN public.social_entity_accounts AS account
    ON account.entity_id = failure.entity_id
   AND account.platform = failure.platform
   AND account.source_kind = CASE failure.platform
     WHEN 'facebook' THEN 'meta_ads'
     WHEN 'instagram' THEN 'instagram_profile'
     WHEN 'google' THEN 'google_domain'
     WHEN 'tiktok' THEN 'tiktok_profile'
     ELSE ''
   END
  WHERE failure.stage = 'ingest'
    AND failure.resolved_at IS NULL
    AND failure.entity_id IS NOT NULL
    AND failure.platform IS NOT NULL
    AND failure.scope_key = CONCAT(failure.entity_id::text, ':', failure.platform)
),
updated AS (
  UPDATE public.social_processing_failures AS failure
  SET scope_key = remap.new_scope_key
  FROM remap
  WHERE failure.id = remap.id
    AND NOT EXISTS (
      SELECT 1
      FROM public.social_processing_failures AS existing
      WHERE existing.stage = failure.stage
        AND existing.scope_key = remap.new_scope_key
        AND existing.id <> failure.id
    )
  RETURNING failure.id
)
UPDATE public.social_processing_failures AS failure
SET
  resolved_at = COALESCE(failure.resolved_at, NOW()),
  metadata = COALESCE(failure.metadata, '{}'::jsonb) || jsonb_build_object('superseded_by_source_kind_scope', TRUE)
FROM remap
WHERE failure.id = remap.id
  AND failure.id NOT IN (SELECT id FROM updated);
