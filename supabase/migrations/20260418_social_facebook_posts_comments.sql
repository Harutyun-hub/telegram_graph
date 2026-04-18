-- Social Phase 5A follow-up:
-- - make Facebook page sources default to canonical post collection
-- - persist parent linkage for comment activities

UPDATE public.social_entity_accounts
SET content_types = '["post"]'::jsonb,
    updated_at = NOW()
WHERE provider_key = 'scrapecreators'
  AND platform = 'facebook'
  AND target_type = 'page_id'
  AND (
    content_types = '["ad"]'::jsonb
    OR content_types = '[]'::jsonb
  );

ALTER TABLE public.social_activities
  ADD COLUMN IF NOT EXISTS parent_provider_item_id TEXT NULL,
  ADD COLUMN IF NOT EXISTS parent_activity_uid TEXT NULL;

CREATE INDEX IF NOT EXISTS social_activities_idx_parent_activity_uid
ON public.social_activities (parent_activity_uid);
