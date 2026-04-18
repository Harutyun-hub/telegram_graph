from __future__ import annotations


VALID_DASHBOARD_V2_JOB_OWNERS = {"worker", "web", "all"}


def normalize_dashboard_v2_job_owner(value: str | None) -> str:
    owner = str(value or "").strip().lower()
    if owner in VALID_DASHBOARD_V2_JOB_OWNERS:
        return owner
    return "worker"


def should_run_dashboard_v2_jobs(*, app_role: str, job_owner: str) -> bool:
    owner = normalize_dashboard_v2_job_owner(job_owner)
    role = str(app_role or "").strip().lower() or "all"
    if owner == "all":
        return role in {"worker", "all", "web"}
    if owner == "web":
        return role == "web"
    return role in {"worker", "all"}
