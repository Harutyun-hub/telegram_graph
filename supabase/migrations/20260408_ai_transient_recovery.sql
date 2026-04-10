-- Harden AI failure tracking so transient provider outages can recover automatically
-- instead of deadlocking the scrape/process/sync pipeline.

ALTER TABLE public.ai_processing_failures
  ADD COLUMN IF NOT EXISTS error_code TEXT,
  ADD COLUMN IF NOT EXISTS failure_class TEXT,
  ADD COLUMN IF NOT EXISTS recovery_after_at TIMESTAMPTZ NULL,
  ADD COLUMN IF NOT EXISTS auto_recovery_attempts INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_recovery_attempt_at TIMESTAMPTZ NULL;

UPDATE public.ai_processing_failures
SET
  error_code = CASE
    WHEN lower(coalesce(last_error, '')) LIKE '%insufficient_quota%' THEN 'openai_insufficient_quota'
    WHEN lower(coalesce(last_error, '')) LIKE '%rate limit%' OR lower(coalesce(last_error, '')) LIKE '%429%' THEN 'provider_rate_limit'
    WHEN lower(coalesce(last_error, '')) LIKE '%connectionterminated%' OR lower(coalesce(last_error, '')) LIKE '%connection terminated%' THEN 'transport_connection_terminated'
    WHEN lower(coalesce(last_error, '')) LIKE '%resource temporarily unavailable%' THEN 'runtime_resource_temporarily_unavailable'
    WHEN lower(coalesce(last_error, '')) LIKE '%timeout%' OR lower(coalesce(last_error, '')) LIKE '%timed out%' THEN 'transport_timeout'
    WHEN lower(coalesce(last_error, '')) LIKE '%service unavailable%' THEN 'upstream_service_unavailable'
    WHEN lower(coalesce(last_error, '')) LIKE '%bad gateway%' THEN 'upstream_bad_gateway'
    WHEN lower(coalesce(last_error, '')) LIKE '%gateway timeout%' THEN 'upstream_gateway_timeout'
    WHEN lower(coalesce(last_error, '')) LIKE '%missing parsed payload%' THEN 'missing_parsed_payload'
    WHEN lower(coalesce(last_error, '')) LIKE '%jsondecodeerror%' OR lower(coalesce(last_error, '')) LIKE '%invalid json%' THEN 'invalid_json_response'
    ELSE coalesce(error_code, 'transient_unknown')
  END,
  failure_class = CASE
    WHEN lower(coalesce(last_error, '')) LIKE '%missing parsed payload%' THEN 'permanent'
    WHEN lower(coalesce(last_error, '')) LIKE '%jsondecodeerror%' THEN 'permanent'
    WHEN lower(coalesce(last_error, '')) LIKE '%invalid json%' THEN 'permanent'
    ELSE coalesce(failure_class, 'transient')
  END
WHERE error_code IS NULL OR failure_class IS NULL;

UPDATE public.ai_processing_failures
SET recovery_after_at = NOW() + interval '60 minutes'
WHERE is_dead_letter = TRUE
  AND coalesce(failure_class, 'transient') = 'transient'
  AND recovery_after_at IS NULL;

ALTER TABLE public.ai_processing_failures
  ALTER COLUMN error_code SET DEFAULT 'transient_unknown',
  ALTER COLUMN failure_class SET DEFAULT 'transient';

UPDATE public.ai_processing_failures
SET error_code = 'transient_unknown'
WHERE error_code IS NULL;

UPDATE public.ai_processing_failures
SET failure_class = 'transient'
WHERE failure_class IS NULL;

ALTER TABLE public.ai_processing_failures
  ALTER COLUMN error_code SET NOT NULL,
  ALTER COLUMN failure_class SET NOT NULL;

ALTER TABLE public.ai_processing_failures
  DROP CONSTRAINT IF EXISTS ai_processing_failures_failure_class_check;

ALTER TABLE public.ai_processing_failures
  ADD CONSTRAINT ai_processing_failures_failure_class_check
  CHECK (failure_class IN ('transient', 'permanent'));

CREATE INDEX IF NOT EXISTS ai_processing_failures_idx_recovery
ON public.ai_processing_failures (scope_type, failure_class, is_dead_letter, recovery_after_at);

CREATE INDEX IF NOT EXISTS ai_processing_failures_idx_failure_class
ON public.ai_processing_failures (failure_class, updated_at DESC);
