-- Social runtime hardening
--
-- Adds operator-facing account health fields and lease columns so the social
-- worker can claim collection/analysis/graph work through Postgres.

ALTER TABLE public.social_entity_accounts
  ADD COLUMN IF NOT EXISTS health_status TEXT NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS last_health_error TEXT NULL,
  ADD COLUMN IF NOT EXISTS last_health_checked_at TIMESTAMPTZ NULL,
  ADD COLUMN IF NOT EXISTS last_collected_at TIMESTAMPTZ NULL,
  ADD COLUMN IF NOT EXISTS collect_claimed_at TIMESTAMPTZ NULL,
  ADD COLUMN IF NOT EXISTS collect_claimed_by TEXT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'social_entity_accounts_health_status_check'
  ) THEN
    ALTER TABLE public.social_entity_accounts
      ADD CONSTRAINT social_entity_accounts_health_status_check
      CHECK (
        health_status IN (
          'unknown',
          'healthy',
          'invalid_identifier',
          'provider_404',
          'rate_limited',
          'auth_error',
          'network_error'
        )
      );
  END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS social_entity_accounts_idx_health
ON public.social_entity_accounts (platform, health_status, is_active);

CREATE INDEX IF NOT EXISTS social_entity_accounts_idx_collect_claim
ON public.social_entity_accounts (collect_claimed_at, platform, is_active);

ALTER TABLE public.social_activities
  ADD COLUMN IF NOT EXISTS analysis_claimed_at TIMESTAMPTZ NULL,
  ADD COLUMN IF NOT EXISTS analysis_claimed_by TEXT NULL,
  ADD COLUMN IF NOT EXISTS graph_claimed_at TIMESTAMPTZ NULL,
  ADD COLUMN IF NOT EXISTS graph_claimed_by TEXT NULL;

CREATE INDEX IF NOT EXISTS social_activities_idx_analysis_claim
ON public.social_activities (analysis_claimed_at, analysis_status, ingest_status);

CREATE INDEX IF NOT EXISTS social_activities_idx_graph_claim
ON public.social_activities (graph_claimed_at, graph_status, analysis_status);
