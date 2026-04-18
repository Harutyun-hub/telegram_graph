-- Dashboard V2 fact-backed read-model foundation
-- Additive only. Old dashboard paths remain intact.

create table if not exists public.dashboard_fact_runs (
  run_id uuid primary key,
  fact_family text not null,
  fact_version integer not null,
  coverage_start date not null,
  coverage_end date not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  status text not null default 'running',
  error text,
  meta_json jsonb not null default '{}'::jsonb
);

create index if not exists dashboard_fact_runs_family_materialized_idx
  on public.dashboard_fact_runs (fact_family, materialized_at desc);
create index if not exists dashboard_fact_runs_status_idx
  on public.dashboard_fact_runs (status, materialized_at desc);

create table if not exists public.dashboard_range_artifacts_v2 (
  cache_key text primary key,
  from_date date not null,
  to_date date not null,
  range_mode text not null,
  summary_granularity text,
  fact_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  built_at timestamptz not null default timezone('utc', now()),
  artifact_version integer not null default 1,
  payload_json jsonb not null default '{}'::jsonb,
  dependency_watermarks jsonb not null default '{}'::jsonb,
  stale_fact_families text[] not null default '{}'::text[],
  stale_reason text,
  is_stale boolean not null default false
);

create index if not exists dashboard_range_artifacts_v2_dates_idx
  on public.dashboard_range_artifacts_v2 (from_date, to_date);
create index if not exists dashboard_range_artifacts_v2_materialized_idx
  on public.dashboard_range_artifacts_v2 (materialized_at desc);
create index if not exists dashboard_range_artifacts_v2_stale_idx
  on public.dashboard_range_artifacts_v2 (is_stale, materialized_at desc);

create table if not exists public.dashboard_compare_runs (
  compare_id uuid primary key,
  cache_key text not null,
  from_date date not null,
  to_date date not null,
  old_path_meta jsonb not null default '{}'::jsonb,
  v2_meta jsonb not null default '{}'::jsonb,
  direct_truth_meta jsonb not null default '{}'::jsonb,
  diff_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now())
);

create index if not exists dashboard_compare_runs_dates_idx
  on public.dashboard_compare_runs (from_date, to_date, created_at desc);

create table if not exists public.dashboard_fact_daily_content (
  fact_date date not null,
  row_key text not null,
  run_id uuid not null references public.dashboard_fact_runs(run_id) on delete cascade,
  fact_version integer not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  source_event_at timestamptz,
  topic_key text,
  channel_id text,
  user_id text,
  content_type text,
  payload_json jsonb not null default '{}'::jsonb,
  primary key (fact_date, row_key)
) partition by range (fact_date);

create table if not exists public.dashboard_fact_daily_content_default
  partition of public.dashboard_fact_daily_content default;

create index if not exists dashboard_fact_daily_content_fact_topic_idx
  on public.dashboard_fact_daily_content (fact_date, topic_key);
create index if not exists dashboard_fact_daily_content_fact_channel_idx
  on public.dashboard_fact_daily_content (fact_date, channel_id);
create index if not exists dashboard_fact_daily_content_fact_user_idx
  on public.dashboard_fact_daily_content (fact_date, user_id);
create index if not exists dashboard_fact_daily_content_fact_type_idx
  on public.dashboard_fact_daily_content (fact_date, content_type);
create index if not exists dashboard_fact_daily_content_source_event_idx
  on public.dashboard_fact_daily_content (fact_date, source_event_at);
create index if not exists dashboard_fact_daily_content_run_idx
  on public.dashboard_fact_daily_content (run_id);
create index if not exists dashboard_fact_daily_content_watermark_idx
  on public.dashboard_fact_daily_content (fact_date, source_watermark);

create table if not exists public.dashboard_fact_daily_topics (
  fact_date date not null,
  row_key text not null,
  run_id uuid not null references public.dashboard_fact_runs(run_id) on delete cascade,
  fact_version integer not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  topic_key text,
  payload_json jsonb not null default '{}'::jsonb,
  primary key (fact_date, row_key)
) partition by range (fact_date);

create table if not exists public.dashboard_fact_daily_topics_default
  partition of public.dashboard_fact_daily_topics default;

create index if not exists dashboard_fact_daily_topics_fact_topic_idx
  on public.dashboard_fact_daily_topics (fact_date, topic_key);
create index if not exists dashboard_fact_daily_topics_topic_fact_idx
  on public.dashboard_fact_daily_topics (topic_key, fact_date desc);
create index if not exists dashboard_fact_daily_topics_run_idx
  on public.dashboard_fact_daily_topics (run_id);
create index if not exists dashboard_fact_daily_topics_watermark_idx
  on public.dashboard_fact_daily_topics (fact_date, source_watermark);

create table if not exists public.dashboard_fact_daily_channels (
  fact_date date not null,
  row_key text not null,
  run_id uuid not null references public.dashboard_fact_runs(run_id) on delete cascade,
  fact_version integer not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  channel_id text,
  payload_json jsonb not null default '{}'::jsonb,
  primary key (fact_date, row_key)
) partition by range (fact_date);

create table if not exists public.dashboard_fact_daily_channels_default
  partition of public.dashboard_fact_daily_channels default;

create index if not exists dashboard_fact_daily_channels_fact_channel_idx
  on public.dashboard_fact_daily_channels (fact_date, channel_id);
create index if not exists dashboard_fact_daily_channels_run_idx
  on public.dashboard_fact_daily_channels (run_id);
create index if not exists dashboard_fact_daily_channels_watermark_idx
  on public.dashboard_fact_daily_channels (fact_date, source_watermark);

create table if not exists public.dashboard_fact_daily_users (
  fact_date date not null,
  row_key text not null,
  run_id uuid not null references public.dashboard_fact_runs(run_id) on delete cascade,
  fact_version integer not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  user_id text,
  cohort_key text,
  payload_json jsonb not null default '{}'::jsonb,
  primary key (fact_date, row_key)
) partition by range (fact_date);

create table if not exists public.dashboard_fact_daily_users_default
  partition of public.dashboard_fact_daily_users default;

create index if not exists dashboard_fact_daily_users_fact_user_idx
  on public.dashboard_fact_daily_users (fact_date, user_id);
create index if not exists dashboard_fact_daily_users_cohort_idx
  on public.dashboard_fact_daily_users (cohort_key, fact_date);
create index if not exists dashboard_fact_daily_users_run_idx
  on public.dashboard_fact_daily_users (run_id);
create index if not exists dashboard_fact_daily_users_watermark_idx
  on public.dashboard_fact_daily_users (fact_date, source_watermark);

create table if not exists public.dashboard_fact_daily_behavioral (
  fact_date date not null,
  row_key text not null,
  run_id uuid not null references public.dashboard_fact_runs(run_id) on delete cascade,
  fact_version integer not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  payload_json jsonb not null default '{}'::jsonb,
  primary key (fact_date, row_key)
) partition by range (fact_date);

create table if not exists public.dashboard_fact_daily_behavioral_default
  partition of public.dashboard_fact_daily_behavioral default;

create index if not exists dashboard_fact_daily_behavioral_run_idx
  on public.dashboard_fact_daily_behavioral (run_id);
create index if not exists dashboard_fact_daily_behavioral_watermark_idx
  on public.dashboard_fact_daily_behavioral (fact_date, source_watermark);

create table if not exists public.dashboard_fact_daily_predictive (
  fact_date date not null,
  row_key text not null,
  run_id uuid not null references public.dashboard_fact_runs(run_id) on delete cascade,
  fact_version integer not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  payload_json jsonb not null default '{}'::jsonb,
  primary key (fact_date, row_key)
) partition by range (fact_date);

create table if not exists public.dashboard_fact_daily_predictive_default
  partition of public.dashboard_fact_daily_predictive default;

create index if not exists dashboard_fact_daily_predictive_run_idx
  on public.dashboard_fact_daily_predictive (run_id);
create index if not exists dashboard_fact_daily_predictive_watermark_idx
  on public.dashboard_fact_daily_predictive (fact_date, source_watermark);

create table if not exists public.dashboard_fact_daily_actionable (
  fact_date date not null,
  row_key text not null,
  run_id uuid not null references public.dashboard_fact_runs(run_id) on delete cascade,
  fact_version integer not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  payload_json jsonb not null default '{}'::jsonb,
  primary key (fact_date, row_key)
) partition by range (fact_date);

create table if not exists public.dashboard_fact_daily_actionable_default
  partition of public.dashboard_fact_daily_actionable default;

create index if not exists dashboard_fact_daily_actionable_run_idx
  on public.dashboard_fact_daily_actionable (run_id);
create index if not exists dashboard_fact_daily_actionable_watermark_idx
  on public.dashboard_fact_daily_actionable (fact_date, source_watermark);

create table if not exists public.dashboard_fact_daily_comparative (
  fact_date date not null,
  row_key text not null,
  run_id uuid not null references public.dashboard_fact_runs(run_id) on delete cascade,
  fact_version integer not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  payload_json jsonb not null default '{}'::jsonb,
  primary key (fact_date, row_key)
) partition by range (fact_date);

create table if not exists public.dashboard_fact_daily_comparative_default
  partition of public.dashboard_fact_daily_comparative default;

create index if not exists dashboard_fact_daily_comparative_run_idx
  on public.dashboard_fact_daily_comparative (run_id);
create index if not exists dashboard_fact_daily_comparative_watermark_idx
  on public.dashboard_fact_daily_comparative (fact_date, source_watermark);

create table if not exists public.dashboard_ai_question_briefs (
  widget_id text not null,
  window_start date not null,
  window_end date not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  status text not null default 'ready',
  payload_json jsonb not null default '{}'::jsonb,
  meta_json jsonb not null default '{}'::jsonb,
  primary key (widget_id, window_start, window_end)
);

create table if not exists public.dashboard_ai_behavioral_briefs (
  widget_id text not null,
  window_start date not null,
  window_end date not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  status text not null default 'ready',
  payload_json jsonb not null default '{}'::jsonb,
  meta_json jsonb not null default '{}'::jsonb,
  primary key (widget_id, window_start, window_end)
);

create table if not exists public.dashboard_ai_recommendation_briefs (
  widget_id text not null,
  window_start date not null,
  window_end date not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  status text not null default 'ready',
  payload_json jsonb not null default '{}'::jsonb,
  meta_json jsonb not null default '{}'::jsonb,
  primary key (widget_id, window_start, window_end)
);

create table if not exists public.dashboard_ai_opportunity_briefs (
  widget_id text not null,
  window_start date not null,
  window_end date not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  status text not null default 'ready',
  payload_json jsonb not null default '{}'::jsonb,
  meta_json jsonb not null default '{}'::jsonb,
  primary key (widget_id, window_start, window_end)
);

create table if not exists public.dashboard_persona_clusters (
  widget_id text not null,
  window_start date not null,
  window_end date not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  status text not null default 'ready',
  payload_json jsonb not null default '{}'::jsonb,
  meta_json jsonb not null default '{}'::jsonb,
  primary key (widget_id, window_start, window_end)
);

create table if not exists public.dashboard_topic_overviews_v2 (
  widget_id text not null,
  window_start date not null,
  window_end date not null,
  source_watermark timestamptz,
  materialized_at timestamptz not null default timezone('utc', now()),
  status text not null default 'ready',
  payload_json jsonb not null default '{}'::jsonb,
  meta_json jsonb not null default '{}'::jsonb,
  primary key (widget_id, window_start, window_end)
);
