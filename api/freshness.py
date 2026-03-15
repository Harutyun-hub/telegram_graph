"""
freshness.py — Pipeline freshness and data-drift snapshot for trust monitoring.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from api import db

_CACHE: dict | None = None
_CACHE_TS: Optional[datetime] = None
_CACHE_TTL_SECONDS = 30


def _parse_iso(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _age_minutes(value: Any, now: datetime) -> Optional[int]:
    ts = _parse_iso(value)
    if not ts:
        return None
    delta = now - ts
    return max(0, int(delta.total_seconds() // 60))


def _status_from_age(age_minutes: Optional[int], warn_after: int, stale_after: int) -> str:
    if age_minutes is None:
        return "unknown"
    if age_minutes >= stale_after:
        return "stale"
    if age_minutes >= warn_after:
        return "warning"
    return "healthy"


def _worst_status(statuses: list[str]) -> str:
    if any(s == "stale" for s in statuses):
        return "stale"
    if any(s == "warning" for s in statuses):
        return "warning"
    if any(s == "unknown" for s in statuses):
        return "unknown"
    return "healthy"


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _neo4j_snapshot() -> dict:
    posts_row = db.run_single(
        """
        MATCH (p:Post)
        RETURN count(p) AS postCount,
               toString(max(p.posted_at)) AS lastPostAt
        """
    ) or {}

    channels_row = db.run_single("MATCH (c:Channel) RETURN count(c) AS channelCount") or {}
    topics_row = db.run_single("MATCH (t:Topic) RETURN count(t) AS topicCount") or {}

    return {
        "post_count": _to_int(posts_row.get("postCount"), 0),
        "last_post_at": posts_row.get("lastPostAt"),
        "channel_count": _to_int(channels_row.get("channelCount"), 0),
        "topic_count": _to_int(topics_row.get("topicCount"), 0),
    }


def _build_notes(snapshot: dict) -> list[str]:
    notes: list[str] = []
    backlog = snapshot.get("backlog", {})
    drift = snapshot.get("drift", {})
    pipeline = snapshot.get("pipeline", {})

    if _to_int(backlog.get("unsynced_posts"), 0) > 0:
        notes.append(
            f"{backlog.get('unsynced_posts')} posts are waiting for Neo4j sync."
        )
    if _to_int(backlog.get("unprocessed_comments"), 0) > 0:
        notes.append(
            f"{backlog.get('unprocessed_comments')} comments are waiting for AI processing."
        )
    if _to_int(backlog.get("dead_letter_scopes"), 0) > 0:
        notes.append(
            f"{backlog.get('dead_letter_scopes')} AI scopes are in dead-letter state and need operator retry."
        )
    if _to_int(backlog.get("retry_blocked_scopes"), 0) > 0:
        notes.append(
            f"{backlog.get('retry_blocked_scopes')} AI scopes are temporarily backoff-blocked."
        )
    if drift.get("latest_post_delta_minutes") is not None and _to_int(drift.get("latest_post_delta_minutes"), 0) > 120:
        notes.append("Latest post timestamp differs by more than 2 hours between Supabase and Neo4j.")
    if pipeline.get("scrape", {}).get("status") == "stale":
        notes.append("Scraper appears stale relative to configured interval.")
    if pipeline.get("sync", {}).get("status") == "stale":
        notes.append("Graph sync signals are stale.")
    return notes


def _compute_health_score(snapshot: dict) -> int:
    status_penalty = {
        "healthy": 0,
        "unknown": 10,
        "warning": 20,
        "stale": 35,
    }

    score = 100
    pipeline = snapshot.get("pipeline", {})
    for key in ("scrape", "process", "sync"):
        score -= status_penalty.get(pipeline.get(key, {}).get("status"), 0)

    backlog = snapshot.get("backlog", {})
    score -= min(20, _to_int(backlog.get("unsynced_posts"), 0) // 25)
    score -= min(15, _to_int(backlog.get("unprocessed_comments"), 0) // 40)
    score -= min(10, _to_int(backlog.get("unprocessed_posts"), 0) // 20)

    drift = snapshot.get("drift", {})
    score -= min(15, _to_int(drift.get("latest_post_delta_minutes"), 0) // 60)

    return max(0, min(100, score))


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _avg_rate_per_hour(run_history: list[dict], key: str) -> float:
    rates: list[float] = []
    for item in run_history:
        duration = _safe_float(item.get("duration_minutes"))
        if not duration or duration <= 0:
            continue
        value = _to_int(item.get(key), 0)
        if value <= 0:
            continue
        rates.append((value / duration) * 60.0)
    if not rates:
        return 0.0
    return round(sum(rates) / len(rates), 2)


def _avg_ai_failure_rate(run_history: list[dict]) -> float:
    ratios: list[float] = []
    for item in run_history:
        processed = _to_int(item.get("ai_processed_items"), 0)
        failed = _to_int(item.get("ai_failed_items"), 0)
        total = processed + failed
        if total <= 0:
            continue
        ratios.append(failed / total)
    if not ratios:
        return 0.0
    return round((sum(ratios) / len(ratios)) * 100.0, 2)


def _eta_minutes(backlog_count: int, rate_per_hour: float) -> int | None:
    if backlog_count <= 0:
        return 0
    if rate_per_hour <= 0:
        return None
    return max(1, int(round((backlog_count / rate_per_hour) * 60.0)))


def _eta_confidence(run_history: list[dict], running_now: bool) -> str:
    usable = [item for item in run_history if _safe_float(item.get("duration_minutes"))]
    if len(usable) >= 5:
        return "high"
    if len(usable) >= 2:
        return "medium"
    return "medium" if running_now else "low"


def get_freshness_snapshot(
    supabase_writer,
    *,
    scheduler_status: Optional[dict] = None,
    force_refresh: bool = False,
) -> dict:
    global _CACHE, _CACHE_TS

    now = datetime.now(timezone.utc)
    if not force_refresh and _CACHE and _CACHE_TS:
        cache_age = (now - _CACHE_TS).total_seconds()
        if cache_age < _CACHE_TTL_SECONDS:
            return _CACHE

    scheduler = scheduler_status or {}
    interval_minutes = max(1, _to_int(scheduler.get("interval_minutes"), 15))
    supa = supabase_writer.get_pipeline_freshness_snapshot()
    neo = _neo4j_snapshot()

    last_scrape_at = supa.get("last_scrape_at")
    last_process_at = supa.get("last_process_at")
    sync_source = "supabase_synced_content_timestamp"
    sync_estimated = True
    last_graph_sync_at = supa.get("last_graph_sync_at")
    if not last_graph_sync_at:
        last_graph_sync_at = neo.get("last_post_at")
        sync_source = "neo4j_max_posted_at"

    scrape_age = _age_minutes(last_scrape_at, now)
    process_age = _age_minutes(last_process_at, now)
    sync_age = _age_minutes(last_graph_sync_at, now)
    supa_latest_post_age = _age_minutes(supa.get("last_post_at"), now)
    neo_latest_post_age = _age_minutes(neo.get("last_post_at"), now)

    supa_last_post_dt = _parse_iso(supa.get("last_post_at"))
    neo_last_post_dt = _parse_iso(neo.get("last_post_at"))
    latest_post_delta_minutes: Optional[int] = None
    if supa_last_post_dt and neo_last_post_dt:
        latest_post_delta_minutes = abs(int((supa_last_post_dt - neo_last_post_dt).total_seconds() // 60))

    scrape_status = _status_from_age(scrape_age, warn_after=interval_minutes * 2, stale_after=interval_minutes * 4)
    process_status = _status_from_age(process_age, warn_after=120, stale_after=360)
    sync_status = _status_from_age(sync_age, warn_after=120, stale_after=360)

    run_history = scheduler.get("run_history") or []
    recent_history = run_history[-6:] if isinstance(run_history, list) else []

    ai_items_last_run = 0
    ai_failed_last_run = 0
    ai_blocked_last_run = 0
    ai_deferred_last_run = 0
    neo4j_synced_last_run = 0
    scraped_items_last_run = 0
    if recent_history:
        latest = recent_history[-1]
        ai_items_last_run = _to_int(latest.get("ai_processed_items"), 0)
        ai_failed_last_run = _to_int(latest.get("ai_failed_items"), 0)
        ai_blocked_last_run = _to_int(latest.get("ai_blocked_items"), 0)
        ai_deferred_last_run = _to_int(latest.get("ai_deferred_items"), 0)
        neo4j_synced_last_run = _to_int(latest.get("neo4j_synced_posts"), 0)
        scraped_items_last_run = _to_int(latest.get("scraped_items"), 0)

    ai_rate_per_hour = _avg_rate_per_hour(recent_history, "ai_processed_items")
    ai_failure_rate = _avg_ai_failure_rate(recent_history)
    sync_rate_per_hour = _avg_rate_per_hour(recent_history, "neo4j_synced_posts")
    scrape_rate_per_hour = _avg_rate_per_hour(recent_history, "scraped_items")

    ai_queue = _to_int(supa.get("unprocessed_comments"), 0) + _to_int(supa.get("unprocessed_posts"), 0)
    graph_queue = _to_int(supa.get("unsynced_posts"), 0)

    eta_ai = _eta_minutes(ai_queue, ai_rate_per_hour)
    eta_graph = _eta_minutes(graph_queue, sync_rate_per_hour)
    eta_total: int | None
    if ai_queue > 0 and eta_ai is None:
        eta_total = None
    elif graph_queue > 0 and eta_graph is None:
        eta_total = None
    elif eta_ai is None and eta_graph is None:
        eta_total = None
    else:
        eta_total = max(eta_ai or 0, eta_graph or 0)

    snapshot = {
        "generated_at": now.isoformat(),
        "scheduler": {
            "is_active": bool(scheduler.get("is_active", False)),
            "interval_minutes": interval_minutes,
            "running_now": bool(scheduler.get("running_now", False)),
            "last_success_at": scheduler.get("last_success_at"),
            "next_run_at": scheduler.get("next_run_at"),
            "last_error": scheduler.get("last_error"),
        },
        "pipeline": {
            "scrape": {
                "status": scrape_status,
                "last_scrape_at": last_scrape_at,
                "age_minutes": scrape_age,
                "active_channels": _to_int(supa.get("active_channels"), 0),
                "active_channels_never_scraped": _to_int(supa.get("active_channels_never_scraped"), 0),
            },
            "process": {
                "status": process_status,
                "last_process_at": last_process_at,
                "age_minutes": process_age,
            },
            "sync": {
                "status": sync_status,
                "last_graph_sync_at": last_graph_sync_at,
                "age_minutes": sync_age,
                "source": sync_source,
                "estimated": sync_estimated,
            },
        },
        "backlog": {
            "unprocessed_posts": _to_int(supa.get("unprocessed_posts"), 0),
            "unprocessed_comments": _to_int(supa.get("unprocessed_comments"), 0),
            "unsynced_posts": _to_int(supa.get("unsynced_posts"), 0),
            "unsynced_analysis": _to_int(supa.get("unsynced_analysis"), 0),
            "dead_letter_scopes": _to_int(supa.get("dead_letter_scopes"), 0),
            "retry_blocked_scopes": _to_int(supa.get("retry_blocked_scopes"), 0),
        },
        "drift": {
            "supabase_total_posts": _to_int(supa.get("total_posts"), 0),
            "neo4j_total_posts": _to_int(neo.get("post_count"), 0),
            "post_count_gap": _to_int(supa.get("total_posts"), 0) - _to_int(neo.get("post_count"), 0),
            "supabase_last_post_at": supa.get("last_post_at"),
            "neo4j_last_post_at": neo.get("last_post_at"),
            "supabase_last_post_age_minutes": supa_latest_post_age,
            "neo4j_last_post_age_minutes": neo_latest_post_age,
            "latest_post_delta_minutes": latest_post_delta_minutes,
            "neo4j_channel_count": _to_int(neo.get("channel_count"), 0),
            "neo4j_topic_count": _to_int(neo.get("topic_count"), 0),
        },
        "pulse": {
            "queue": {
                "ai_items": ai_queue,
                "graph_posts": graph_queue,
            },
            "processed": {
                "scraped_items_last_run": scraped_items_last_run,
                "ai_items_last_run": ai_items_last_run,
                "ai_failed_last_run": ai_failed_last_run,
                "ai_blocked_last_run": ai_blocked_last_run,
                "ai_deferred_last_run": ai_deferred_last_run,
                "neo4j_posts_last_run": neo4j_synced_last_run,
                "ai_rate_per_hour": ai_rate_per_hour,
                "ai_failure_rate_percent": ai_failure_rate,
                "neo4j_rate_per_hour": sync_rate_per_hour,
                "scrape_rate_per_hour": scrape_rate_per_hour,
            },
            "eta": {
                "ai_queue_minutes": eta_ai,
                "graph_queue_minutes": eta_graph,
                "total_minutes": eta_total,
                "confidence": _eta_confidence(recent_history, bool(scheduler.get("running_now", False))),
                "assumption": "ETA assumes similar throughput and no major burst of new incoming data.",
            },
        },
    }

    overall_status = _worst_status([scrape_status, process_status, sync_status])
    snapshot["health"] = {
        "status": overall_status,
        "score": _compute_health_score(snapshot),
        "notes": _build_notes(snapshot),
    }

    _CACHE = snapshot
    _CACHE_TS = now
    return snapshot
    
