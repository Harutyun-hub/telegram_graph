-- Dashboard V2 chunked materialization queue
-- Additive only. Web enqueues/resumes jobs; worker claims and executes slices.

create table if not exists public.dashboard_materialize_jobs_v2 (
  job_id uuid primary key,
  mode text not null,
  requested_start date not null,
  requested_end date not null,
  fact_version integer not null,
  status text not null default 'queued',
  requested_by_role text,
  requested_by_actor text,
  job_owner text not null default 'worker',
  created_at timestamptz not null default timezone('utc', now()),
  started_at timestamptz,
  finished_at timestamptz,
  updated_at timestamptz not null default timezone('utc', now()),
  active_worker_id text,
  last_heartbeat_at timestamptz,
  last_error text,
  total_slices integer not null default 0,
  completed_slices integer not null default 0,
  failed_slices integer not null default 0,
  total_days integer not null default 0,
  completed_days integer not null default 0,
  constraint dashboard_materialize_jobs_v2_mode_check
    check (mode in ('backfill', 'reconciliation')),
  constraint dashboard_materialize_jobs_v2_status_check
    check (status in ('queued', 'running', 'paused', 'completed', 'failed', 'cancelled'))
);

create index if not exists dashboard_materialize_jobs_v2_status_created_idx
  on public.dashboard_materialize_jobs_v2 (status, created_at asc);
create index if not exists dashboard_materialize_jobs_v2_owner_status_idx
  on public.dashboard_materialize_jobs_v2 (job_owner, status, created_at asc);
create index if not exists dashboard_materialize_jobs_v2_requested_window_idx
  on public.dashboard_materialize_jobs_v2 (mode, requested_start, requested_end, fact_version, created_at desc);

create table if not exists public.dashboard_materialize_slices_v2 (
  slice_id uuid primary key,
  job_id uuid not null references public.dashboard_materialize_jobs_v2(job_id) on delete cascade,
  fact_family text not null,
  slice_order integer not null,
  slice_start date not null,
  slice_end date not null,
  status text not null default 'pending',
  attempt_count integer not null default 0,
  lease_owner text,
  lease_expires_at timestamptz,
  started_at timestamptz,
  finished_at timestamptz,
  rows_inserted integer not null default 0,
  days_processed integer not null default 0,
  degraded_days text[] not null default '{}'::text[],
  failed_widgets text[] not null default '{}'::text[],
  fact_run_id uuid references public.dashboard_fact_runs(run_id) on delete set null,
  last_error text,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  constraint dashboard_materialize_slices_v2_status_check
    check (status in ('pending', 'running', 'completed', 'failed'))
);

create unique index if not exists dashboard_materialize_slices_v2_job_order_idx
  on public.dashboard_materialize_slices_v2 (job_id, slice_order);
create index if not exists dashboard_materialize_slices_v2_status_lease_idx
  on public.dashboard_materialize_slices_v2 (status, lease_expires_at asc);
create index if not exists dashboard_materialize_slices_v2_job_status_idx
  on public.dashboard_materialize_slices_v2 (job_id, status, slice_order asc);
