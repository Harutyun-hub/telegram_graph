DROP FUNCTION IF EXISTS public.dashboard_ai_analysis_window_summary(timestamptz, timestamptz);
DROP FUNCTION IF EXISTS public.dashboard_batch_signal_summary(timestamptz, timestamptz, timestamptz, timestamptz);

CREATE OR REPLACE FUNCTION public.dashboard_ai_analysis_window_summary(
  p_start timestamptz,
  p_end timestamptz
)
RETURNS TABLE (
  analysis_units integer,
  positive_rows integer,
  negative_rows integer,
  neutral_rows integer,
  unique_users integer,
  posts_analyzed integer,
  comment_scopes_analyzed integer
)
LANGUAGE sql
STABLE
AS $$
WITH base AS (
  SELECT
    lower(trim(coalesce(primary_intent, ''))) AS intent_text,
    coalesce(sentiment_score, 0.0) AS sentiment_score,
    lower(trim(coalesce(content_type, ''))) AS content_type,
    nullif(trim(coalesce(content_id, '')), '') AS content_id_text,
    nullif(trim(coalesce(channel_id, '')), '') AS channel_id_text,
    nullif(trim(coalesce(telegram_user_id::text, '')), '') AS user_id_text
  FROM public.ai_analysis
  WHERE created_at >= p_start
    AND created_at < p_end
),
classified AS (
  SELECT
    *,
    CASE
      WHEN sentiment_score >= 0.2 THEN 'positive'
      WHEN sentiment_score <= -0.2 THEN 'negative'
      WHEN EXISTS (
        SELECT 1
        FROM unnest(ARRAY[
          'vent', 'critique', 'critic', 'complaint', 'condemn', 'sarcasm',
          'protest', 'hostile', 'mock', 'conflict'
        ]) AS hint
        WHERE intent_text LIKE '%' || hint || '%'
      ) THEN 'negative'
      WHEN EXISTS (
        SELECT 1
        FROM unnest(ARRAY[
          'inform', 'information', 'support', 'help', 'job', 'question',
          'clarification', 'analysis', 'analyze', 'observation', 'report',
          'discuss', 'gratitude', 'solution'
        ]) AS hint
        WHERE intent_text LIKE '%' || hint || '%'
      ) THEN 'positive'
      ELSE 'neutral'
    END AS intent_bucket
  FROM base
)
SELECT
  count(*)::integer AS analysis_units,
  count(*) FILTER (WHERE intent_bucket = 'positive')::integer AS positive_rows,
  count(*) FILTER (WHERE intent_bucket = 'negative')::integer AS negative_rows,
  count(*) FILTER (WHERE intent_bucket = 'neutral')::integer AS neutral_rows,
  count(DISTINCT user_id_text)::integer AS unique_users,
  (
    count(DISTINCT CASE
      WHEN content_type = 'post' AND content_id_text IS NOT NULL THEN content_id_text
      ELSE NULL
    END)
    + count(*) FILTER (
      WHERE content_type = 'post' AND content_id_text IS NULL
    )
  )::integer AS posts_analyzed,
  (
    count(DISTINCT CASE
      WHEN content_type = 'batch'
       AND channel_id_text IS NOT NULL
       AND user_id_text IS NOT NULL
       AND content_id_text IS NOT NULL
      THEN channel_id_text || ':' || user_id_text || ':' || content_id_text
      ELSE NULL
    END)
    + count(*) FILTER (
      WHERE content_type = 'batch'
        AND (
          channel_id_text IS NULL
          OR user_id_text IS NULL
          OR content_id_text IS NULL
        )
    )
  )::integer AS comment_scopes_analyzed
FROM classified;
$$;

CREATE OR REPLACE FUNCTION public.dashboard_batch_signal_summary(
  p_previous_start timestamptz,
  p_previous_end timestamptz,
  p_start timestamptz,
  p_end timestamptz
)
RETURNS TABLE (
  window_key text,
  user_id text,
  signal_type text,
  signal_count integer
)
LANGUAGE sql
STABLE
AS $$
WITH base AS (
  SELECT
    CASE
      WHEN created_at >= p_start AND created_at < p_end THEN 'current'
      WHEN created_at >= p_previous_start AND created_at < p_previous_end THEN 'previous'
      ELSE NULL
    END AS window_key,
    nullif(trim(coalesce(telegram_user_id::text, '')), '') AS user_id,
    nullif(trim(coalesce(raw_llm_response -> 'business_opportunity' ->> 'opportunity_type', '')), '') AS signal_type,
    created_at
  FROM public.ai_analysis
  WHERE content_type = 'batch'
    AND telegram_user_id IS NOT NULL
    AND created_at >= p_previous_start
    AND created_at < p_end
),
filtered AS (
  SELECT *
  FROM base
  WHERE window_key IS NOT NULL
    AND user_id IS NOT NULL
    AND signal_type IN ('Job_Seeking', 'Hiring', 'Partnership_Request')
),
aggregated AS (
  SELECT
    window_key,
    user_id,
    signal_type,
    count(*)::integer AS signal_count,
    max(created_at) AS latest_at
  FROM filtered
  GROUP BY 1, 2, 3
),
ranked AS (
  SELECT
    window_key,
    user_id,
    signal_type,
    signal_count,
    row_number() OVER (
      PARTITION BY window_key, user_id
      ORDER BY
        signal_count DESC,
        latest_at DESC,
        CASE signal_type
          WHEN 'Job_Seeking' THEN 3
          WHEN 'Hiring' THEN 2
          WHEN 'Partnership_Request' THEN 1
          ELSE 0
        END DESC,
        signal_type ASC
    ) AS rn
  FROM aggregated
)
SELECT
  window_key,
  user_id,
  signal_type,
  signal_count
FROM ranked
WHERE rn = 1;
$$;
