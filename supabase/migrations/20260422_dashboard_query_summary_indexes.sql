CREATE INDEX IF NOT EXISTS ai_analysis_idx_dashboard_content_type_created_at
ON public.ai_analysis (content_type, created_at DESC);

CREATE INDEX IF NOT EXISTS ai_analysis_idx_dashboard_batch_created_at_user
ON public.ai_analysis (created_at DESC, telegram_user_id)
WHERE content_type = 'batch'
  AND telegram_user_id IS NOT NULL;
