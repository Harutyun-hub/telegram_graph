"""
server.py — FastAPI application serving the dashboard API.

Endpoints:
  GET  /api/dashboard     → Full AppData JSON (cached 5 min)
  GET  /api/topics        → Topics detail page (paginated)
  GET  /api/channels      → Channels detail page
  GET  /api/audience      → Audience detail page (paginated)
  POST /api/cache/clear   → Invalidate cache
  GET  /api/health        → Health check

Run:
  cd /Users/harutnahapetyan/Documents/Gemini/Telegram
  python -m uvicorn api.server:app --reload --port 8000
"""
from __future__ import annotations
import sys, os
import hashlib
import asyncio
import hmac
import threading
import time
from datetime import datetime, timedelta, timezone
import re
from typing import Any, Dict, List, Optional

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
config.validate()

from fastapi import FastAPI, Query, HTTPException, Depends, Header, Request
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger

from api.aggregator import (
    get_dashboard_data, get_dashboard_snapshot, get_topics_page, get_channels_page,
    get_audience_page, invalidate_cache
)
from api.dashboard_dates import build_dashboard_date_context
from api.queries import graph_dashboard
from api.freshness import get_freshness_snapshot
from api import insights
from api import behavioral_briefs
from api import opportunity_briefs
from api import question_briefs
from api import recommendation_briefs
from api.admin_runtime import (
    get_admin_config_runtime_warning,
    load_admin_config_raw,
    save_admin_config_raw,
)
from api import db
from buffer.supabase_writer import SupabaseWriter
from api.scraper_scheduler import ScraperSchedulerService
from processor import intent_extractor
from scraper.channel_metadata import get_full_channel_metadata
from utils.taxonomy import TAXONOMY_DOMAINS

# ── App setup ────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Radar Obshchiny API",
    description="Dashboard data API for the Radar Obshchiny community intelligence platform",
    version="1.0.0",
)

_cors_allow_origins = config.CORS_ALLOW_ORIGINS or ["*"]
_cors_allow_credentials = "*" not in _cors_allow_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_allow_origins,
    allow_credentials=_cors_allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AIQueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)


class ChannelSourceCreateRequest(BaseModel):
    channel_username: str = Field(..., min_length=1, max_length=256)
    channel_title: Optional[str] = Field(default=None, max_length=256)
    scrape_depth_days: int = Field(default=7, ge=1, le=3650)
    scrape_comments: bool = True


class ChannelSourceUpdateRequest(BaseModel):
    is_active: Optional[bool] = None
    scrape_depth_days: Optional[int] = Field(default=None, ge=1, le=3650)
    scrape_comments: Optional[bool] = None


class ScraperSchedulerUpdateRequest(BaseModel):
    interval_minutes: int = Field(..., ge=1, le=1440)


class AdminConfigPatchRequest(BaseModel):
    widgets: Optional[Dict[str, Dict[str, Any]]] = None
    prompts: Optional[Dict[str, Any]] = None
    runtime: Optional[Dict[str, Any]] = None

    class Config:
        extra = "forbid"


class GraphRequest(BaseModel):
    mode: Optional[str] = Field(default=None, max_length=64)
    timeframe: Optional[str] = Field(default="Last 7 Days", max_length=64)
    channels: Optional[List[str]] = None
    brandSource: Optional[List[str]] = None
    sentiment: Optional[List[str]] = None
    sentiments: Optional[List[str]] = None
    topics: Optional[List[str]] = None
    layers: Optional[List[str]] = None
    insightMode: Optional[str] = Field(default=None, max_length=64)
    sourceProfile: Optional[str] = Field(default=None, max_length=64)
    connectionStrength: Optional[int] = Field(default=None, ge=1, le=5)
    confidenceThreshold: Optional[int] = Field(default=None, ge=1, le=100)
    min_weight: Optional[int] = Field(default=None, ge=1, le=10000)
    max_nodes: Optional[int] = Field(default=None, ge=10, le=5000)
    max_edges: Optional[int] = Field(default=None, ge=10, le=10000)


class InsightCardsRequest(BaseModel):
    filters: dict = Field(default_factory=dict)
    audience: str = Field(default="analyst", max_length=32)


class FailureRetryRequest(BaseModel):
    scope_type: str = Field(..., min_length=1, max_length=32)
    scope_keys: List[str] = Field(default_factory=list)


class TopicProposalReviewRequest(BaseModel):
    topic_name: str = Field(..., min_length=1, max_length=200)
    decision: str = Field(..., min_length=1, max_length=16)
    canonical_topic: Optional[str] = Field(default=None, max_length=200)
    aliases: List[str] = Field(default_factory=list)
    notes: Optional[str] = Field(default=None, max_length=1000)
    reviewed_by: Optional[str] = Field(default=None, max_length=120)


supabase_writer = SupabaseWriter()
scraper_scheduler = ScraperSchedulerService(supabase_writer)
question_cards_scheduler: AsyncIOScheduler | None = None
behavioral_cards_scheduler: AsyncIOScheduler | None = None
opportunity_cards_scheduler: AsyncIOScheduler | None = None
USERNAME_RE = re.compile(r"^[a-z][a-z0-9_]{4,31}$")
_analytics_rate_limit_lock = threading.Lock()
_analytics_rate_limit_buckets: dict[tuple[str, str], list[float]] = {}
ADMIN_WIDGET_IDS = (
    "community_brief",
    "community_health_score",
    "trending_topics_feed",
    "topic_landscape",
    "conversation_trends",
    "question_cloud",
    "topic_lifecycle",
    "problem_tracker",
    "service_gap_detector",
    "satisfaction_by_area",
    "mood_over_time",
    "emotional_urgency_index",
    "top_channels",
    "key_voices",
    "recommendation_tracker",
    "information_velocity",
    "persona_gallery",
    "interest_radar",
    "community_growth_funnel",
    "retention_risk_gauge",
    "decision_stage_tracker",
    "emerging_interests",
    "new_vs_returning_voice",
    "business_opportunity_tracker",
    "job_market_pulse",
    "week_over_week_shifts",
    "sentiment_by_topic",
    "content_performance",
)
ADMIN_RUNTIME_STRING_KEYS = {
    "openaiModel",
    "questionBriefsModel",
    "behavioralBriefsModel",
    "opportunityBriefsModel",
    "questionBriefsPromptVersion",
    "behavioralBriefsPromptVersion",
    "opportunityBriefsPromptVersion",
    "aiPostPromptStyle",
}
ADMIN_RUNTIME_BOOL_KEYS = {
    "featureQuestionBriefsAi",
    "featureBehavioralBriefsAi",
    "featureOpportunityBriefsAi",
}


def _admin_prompt_defaults() -> dict[str, str]:
    defaults: dict[str, str] = {}
    for provider in (
        intent_extractor.get_admin_prompt_defaults,
        question_briefs.get_admin_prompt_defaults,
        behavioral_briefs.get_admin_prompt_defaults,
        opportunity_briefs.get_admin_prompt_defaults,
        recommendation_briefs.get_admin_prompt_defaults,
    ):
        defaults.update(provider())
    return defaults


def _default_admin_config() -> dict[str, Any]:
    prompt_defaults = _admin_prompt_defaults()
    return {
        "widgets": {widget_id: {"enabled": True} for widget_id in ADMIN_WIDGET_IDS},
        "prompts": dict(prompt_defaults),
        "promptDefaults": dict(prompt_defaults),
        "runtime": {
            "openaiModel": config.OPENAI_MODEL,
            "questionBriefsModel": config.QUESTION_BRIEFS_MODEL,
            "behavioralBriefsModel": config.BEHAVIORAL_BRIEFS_MODEL,
            "opportunityBriefsModel": config.OPPORTUNITY_BRIEFS_MODEL,
            "questionBriefsPromptVersion": config.QUESTION_BRIEFS_PROMPT_VERSION,
            "behavioralBriefsPromptVersion": config.BEHAVIORAL_BRIEFS_PROMPT_VERSION,
            "opportunityBriefsPromptVersion": config.OPPORTUNITY_BRIEFS_PROMPT_VERSION,
            "aiPostPromptStyle": config.AI_POST_PROMPT_STYLE,
            "featureQuestionBriefsAi": bool(config.FEATURE_QUESTION_BRIEFS_AI),
            "featureBehavioralBriefsAi": bool(config.FEATURE_BEHAVIORAL_BRIEFS_AI),
            "featureOpportunityBriefsAi": bool(config.FEATURE_OPPORTUNITY_BRIEFS_AI),
        },
    }


def _validate_admin_widgets(raw_widgets: Any) -> dict[str, dict[str, bool]]:
    if raw_widgets is None:
        return {}
    if not isinstance(raw_widgets, dict):
        raise HTTPException(status_code=400, detail="widgets must be an object")

    validated: dict[str, dict[str, bool]] = {}
    for widget_id, value in raw_widgets.items():
        if widget_id not in ADMIN_WIDGET_IDS:
            raise HTTPException(status_code=400, detail=f"Unknown widget id: {widget_id}")
        if not isinstance(value, dict):
            raise HTTPException(status_code=400, detail=f"Widget config for {widget_id} must be an object")
        enabled = value.get("enabled")
        if not isinstance(enabled, bool):
            raise HTTPException(status_code=400, detail=f"Widget {widget_id} requires boolean enabled")
        validated[widget_id] = {"enabled": enabled}
    return validated


def _validate_admin_prompts(raw_prompts: Any) -> dict[str, str]:
    if raw_prompts is None:
        return {}
    if not isinstance(raw_prompts, dict):
        raise HTTPException(status_code=400, detail="prompts must be an object")

    defaults = _admin_prompt_defaults()
    validated: dict[str, str] = {}
    for key, value in raw_prompts.items():
        if key not in defaults:
            raise HTTPException(status_code=400, detail=f"Unknown prompt key: {key}")
        if not isinstance(value, str):
            raise HTTPException(status_code=400, detail=f"Prompt {key} must be a string")
        text = value.strip()
        if not text:
            raise HTTPException(status_code=400, detail=f"Prompt {key} cannot be empty")
        validated[key] = text
    return validated


def _validate_admin_runtime(raw_runtime: Any) -> dict[str, Any]:
    if raw_runtime is None:
        return {}
    if not isinstance(raw_runtime, dict):
        raise HTTPException(status_code=400, detail="runtime must be an object")

    validated: dict[str, Any] = {}
    allowed_keys = ADMIN_RUNTIME_STRING_KEYS.union(ADMIN_RUNTIME_BOOL_KEYS)
    for key, value in raw_runtime.items():
        if key not in allowed_keys:
            raise HTTPException(status_code=400, detail=f"Unknown runtime key: {key}")
        if key in ADMIN_RUNTIME_BOOL_KEYS:
            if not isinstance(value, bool):
                raise HTTPException(status_code=400, detail=f"Runtime field {key} must be boolean")
            validated[key] = value
            continue
        if not isinstance(value, str):
            raise HTTPException(status_code=400, detail=f"Runtime field {key} must be a string")
        text = value.strip()
        if not text:
            raise HTTPException(status_code=400, detail=f"Runtime field {key} cannot be empty")
        if key == "aiPostPromptStyle" and text.lower() not in {"compact", "full"}:
            raise HTTPException(status_code=400, detail="aiPostPromptStyle must be compact or full")
        validated[key] = text.lower() if key == "aiPostPromptStyle" else text
    return validated


def _configured_analytics_tokens() -> list[str]:
    return [
        token
        for token in (
            config.ANALYTICS_API_KEY_FRONTEND,
            config.ANALYTICS_API_KEY_OPENCLAW,
        )
        if token
    ]


def _token_fingerprint(token: str | None) -> str:
    text = (token or "").strip()
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]


def _analytics_client_ip(request: Request) -> str:
    forwarded_for = (request.headers.get("x-forwarded-for") or "").strip()
    if config.ANALYTICS_RATE_LIMIT_TRUST_PROXY and forwarded_for:
        return forwarded_for.split(",")[0].strip() or "unknown"
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _enforce_analytics_rate_limit(request: Request) -> None:
    if not config.ANALYTICS_RATE_LIMIT_ENABLED:
        return

    now = time.monotonic()
    window_seconds = max(1, int(config.ANALYTICS_RATE_LIMIT_WINDOW_SECONDS))
    max_requests = max(1, int(config.ANALYTICS_RATE_LIMIT_MAX_REQUESTS))
    bucket_key = (_analytics_client_ip(request), request.url.path)

    with _analytics_rate_limit_lock:
        timestamps = _analytics_rate_limit_buckets.get(bucket_key, [])
        cutoff = now - window_seconds
        timestamps = [ts for ts in timestamps if ts >= cutoff]
        if len(timestamps) >= max_requests:
            _analytics_rate_limit_buckets[bucket_key] = timestamps
            logger.warning(
                "Analytics rate limit exceeded | endpoint={} client_ip={} count={}".format(
                    request.url.path,
                    bucket_key[0],
                    len(timestamps),
                )
            )
            raise HTTPException(status_code=429, detail="Rate limit exceeded for analytics API.")
        timestamps.append(now)
        _analytics_rate_limit_buckets[bucket_key] = timestamps


def require_analytics_access(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> None:
    _enforce_analytics_rate_limit(request)

    if not config.ANALYTICS_API_REQUIRE_AUTH:
        return

    valid_tokens = _configured_analytics_tokens()
    if not valid_tokens:
        logger.error(
            "Analytics auth enabled without configured tokens | endpoint={}".format(request.url.path)
        )
        raise HTTPException(status_code=503, detail="Analytics API auth is enabled but no server token is configured.")

    if not authorization:
        logger.warning(
            "Analytics auth failure | endpoint={} reason=missing_header client_ip={}".format(
                request.url.path,
                _analytics_client_ip(request),
            )
        )
        raise HTTPException(status_code=401, detail="Missing Authorization header.")

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        logger.warning(
            "Analytics auth failure | endpoint={} reason=bad_scheme client_ip={}".format(
                request.url.path,
                _analytics_client_ip(request),
            )
        )
        raise HTTPException(status_code=401, detail="Authorization header must use Bearer token.")

    clean_token = token.strip()
    if any(hmac.compare_digest(clean_token, valid) for valid in valid_tokens):
        return

    logger.warning(
        "Analytics auth failure | endpoint={} reason=invalid_token token_fp={} client_ip={}".format(
            request.url.path,
            _token_fingerprint(clean_token),
            _analytics_client_ip(request),
        )
    )
    raise HTTPException(status_code=401, detail="Invalid analytics API token.")


def _sanitize_admin_config(raw_config: Any) -> dict[str, Any]:
    defaults = _default_admin_config()
    payload = raw_config if isinstance(raw_config, dict) else {}

    widgets = payload.get("widgets") if isinstance(payload.get("widgets"), dict) else {}
    prompts = payload.get("prompts") if isinstance(payload.get("prompts"), dict) else {}
    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}

    defaults["widgets"].update(_validate_admin_widgets(widgets))
    defaults["prompts"].update(_validate_admin_prompts(prompts))
    defaults["runtime"].update(_validate_admin_runtime(runtime))
    return defaults


def _load_admin_config() -> dict[str, Any]:
    try:
        return _sanitize_admin_config(load_admin_config_raw())
    except HTTPException:
        logger.warning("Admin config contains invalid values; falling back to defaults")
        return _default_admin_config()


def _admin_config_response() -> dict[str, Any]:
    payload = _load_admin_config()
    warning = get_admin_config_runtime_warning()
    if warning:
        payload["warning"] = warning
    return payload


def _parse_snapshot_date(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _trusted_end_date_from_freshness(snapshot: dict) -> datetime.date:
    now = datetime.now(timezone.utc)
    sync = snapshot.get("pipeline", {}).get("sync", {})
    process = snapshot.get("pipeline", {}).get("process", {})
    drift = snapshot.get("drift", {})

    candidate = _parse_snapshot_date(
        sync.get("last_graph_sync_at")
        or drift.get("neo4j_last_post_at")
        or drift.get("supabase_last_post_at")
        or snapshot.get("generated_at")
    )
    trusted_end = (candidate or (now - timedelta(days=1))).date()

    sync_status = str(sync.get("status") or "unknown").lower()
    process_status = str(process.get("status") or "unknown").lower()
    sync_age_minutes = sync.get("age_minutes")
    process_age_minutes = process.get("age_minutes")

    if (
        trusted_end == now.date() and (
            sync_status != "healthy"
            or process_status in {"warning", "stale"}
            or (isinstance(sync_age_minutes, (int, float)) and sync_age_minutes > 180)
            or (isinstance(process_age_minutes, (int, float)) and process_age_minutes > 180)
        )
    ):
        trusted_end = trusted_end - timedelta(days=1)

    return trusted_end


def _dashboard_freshness_snapshot(force_refresh: bool = False) -> dict:
    return get_freshness_snapshot(
        supabase_writer,
        scheduler_status=scraper_scheduler.status(),
        force_refresh=force_refresh,
    )


def _default_dashboard_context(snapshot: Optional[dict] = None):
    freshness_snapshot = snapshot or _dashboard_freshness_snapshot(force_refresh=False)
    trusted_end = _trusted_end_date_from_freshness(freshness_snapshot)
    from_date = (trusted_end - timedelta(days=14)).isoformat()
    return build_dashboard_date_context(from_date, trusted_end.isoformat())


async def _warm_dashboard_cache() -> None:
    """Warm dashboard cache in background after startup."""
    try:
        loop = asyncio.get_running_loop()
        freshness_snapshot = _dashboard_freshness_snapshot(force_refresh=False)
        ctx = _default_dashboard_context(freshness_snapshot)
        await loop.run_in_executor(None, lambda: get_dashboard_data(ctx))
        logger.info("Dashboard cache warm-up completed")
    except Exception as e:
        logger.warning(f"Dashboard cache warm-up failed: {e}")


async def _warm_detail_caches() -> None:
    """Warm detail page caches in background after startup."""
    try:
        loop = asyncio.get_running_loop()
        await asyncio.gather(
            loop.run_in_executor(None, lambda: get_topics_page(0, 500)),
            loop.run_in_executor(None, get_channels_page),
            loop.run_in_executor(None, lambda: get_audience_page(0, 500)),
        )
        logger.info("Detail caches warm-up completed")
    except Exception as e:
        logger.warning(f"Detail caches warm-up failed: {e}")


async def _materialize_question_cards_once(force: bool = False) -> None:
    """Run question-card materialization off the request path."""
    try:
        loop = asyncio.get_running_loop()
        cards = await loop.run_in_executor(
            None,
            lambda: question_briefs.refresh_question_briefs(force=force),
        )
        logger.info(f"Question cards materialization completed | cards={len(cards)}")
    except Exception as e:
        logger.warning(f"Question cards materialization failed: {e}")


async def _materialize_behavioral_cards_once(force: bool = False) -> None:
    """Run behavioral-card materialization off the request path."""
    try:
        loop = asyncio.get_running_loop()
        payload = await loop.run_in_executor(
            None,
            lambda: behavioral_briefs.refresh_behavioral_briefs(force=force),
        )
        problems = len(payload.get("problemBriefs") or []) if isinstance(payload, dict) else 0
        services = len(payload.get("serviceGapBriefs") or []) if isinstance(payload, dict) else 0
        logger.info(f"Behavioral cards materialization completed | problem_cards={problems} service_cards={services}")
    except Exception as e:
        logger.warning(f"Behavioral cards materialization failed: {e}")


async def _materialize_opportunity_cards_once(force: bool = False) -> None:
    """Run opportunity-card materialization off the request path."""
    try:
        loop = asyncio.get_running_loop()
        cards = await loop.run_in_executor(
            None,
            lambda: opportunity_briefs.refresh_opportunity_briefs(force=force),
        )
        logger.info(f"Opportunity cards materialization completed | cards={len(cards)}")
    except Exception as e:
        logger.warning(f"Opportunity cards materialization failed: {e}")


def _start_question_cards_scheduler() -> None:
    """Start recurring question-card materialization scheduler."""
    global question_cards_scheduler

    interval = max(15, int(config.QUESTION_BRIEFS_REFRESH_MINUTES))
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        _materialize_question_cards_once,
        "interval",
        minutes=interval,
        id="question-cards-materializer",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    scheduler.start()
    question_cards_scheduler = scheduler
    logger.info(f"Question cards scheduler ready | interval={interval}m")


def _start_behavioral_cards_scheduler() -> None:
    """Start recurring W8/W9 card materialization scheduler."""
    global behavioral_cards_scheduler

    interval = max(15, int(config.BEHAVIORAL_BRIEFS_REFRESH_MINUTES))
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        _materialize_behavioral_cards_once,
        "interval",
        minutes=interval,
        id="behavioral-cards-materializer",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    scheduler.start()
    behavioral_cards_scheduler = scheduler
    logger.info(f"Behavioral cards scheduler ready | interval={interval}m")


def _start_opportunity_cards_scheduler() -> None:
    """Start recurring business-opportunity card materialization scheduler."""
    global opportunity_cards_scheduler

    interval = max(15, int(config.OPPORTUNITY_BRIEFS_REFRESH_MINUTES))
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        _materialize_opportunity_cards_once,
        "interval",
        minutes=interval,
        id="opportunity-cards-materializer",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    scheduler.start()
    opportunity_cards_scheduler = scheduler
    logger.info(f"Opportunity cards scheduler ready | interval={interval}m")


def _normalize_channel_username(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""

    value = re.sub(r"^https?://", "", value, flags=re.IGNORECASE)
    lowered = value.lower()
    if lowered.startswith("t.me/"):
        value = value[5:]
    elif lowered.startswith("telegram.me/"):
        value = value[12:]

    value = value.split("?", 1)[0].split("#", 1)[0]
    if value.startswith("@"):
        value = value[1:]
    if "/" in value:
        value = value.split("/", 1)[0]

    return value.strip().lower().lstrip("@")


def _canonical_channel_username(handle: str) -> str:
    normalized = (handle or "").strip().lower().lstrip("@")
    return f"@{normalized}" if normalized else ""


async def _try_enrich_channel_metadata(
    channel_uuid: str,
    canonical_username: str,
    fallback_title: Optional[str] = None,
) -> None:
    """
    Best-effort Telegram metadata enrichment for a channel source.

    This fills telegram_channel_id (and title where available) right after source
    creation/activation so users don't have to wait for the scraper cycle.
    """
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        import os
    except Exception as e:
        logger.warning(f"Telethon unavailable for metadata enrichment: {e}")
        return

    username = _canonical_channel_username(canonical_username)
    if not username:
        return

    # Check for session string from environment (Railway deployment)
    session_string = os.getenv("TELEGRAM_SESSION_STRING")

    if session_string:
        client = TelegramClient(
            StringSession(session_string),
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH,
        )
    else:
        # Use file-based session for local development
        client = TelegramClient(
            config.TELEGRAM_SESSION_NAME,
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH,
        )

    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.warning(f"Metadata enrichment skipped for {username}: Telegram session is not authorized")
            return

        metadata = await get_full_channel_metadata(client, username=username)
        if not metadata.get("channel_title") and fallback_title:
            metadata["channel_title"] = fallback_title

        supabase_writer.update_channel_metadata(channel_uuid, metadata)
    except Exception as e:
        logger.warning(f"Metadata enrichment failed for {username}: {e}")
    finally:
        client.disconnect()


def _validate_channel_username(username: str) -> None:
    if not USERNAME_RE.match(username):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid Telegram channel username. Use @name, t.me/name, or name; "
                "5-32 chars, letters/digits/underscore, starts with a letter."
            ),
        )


def _is_russian(text: str) -> bool:
    return bool(re.search(r"[\u0400-\u04FF]", text))


def _normalize_taxonomy_label(value: str) -> str:
    return " ".join(str(value or "").strip().lower().replace("&", "and").split())


def _taxonomy_quality_snapshot() -> dict:
    expected_domains = set(TAXONOMY_DOMAINS.keys())

    domain_rows = db.run_query("MATCH (d:TopicDomain) RETURN d.name AS name ORDER BY name")
    domain_names = [str(row.get("name") or "").strip() for row in domain_rows if row.get("name")]

    normalized_groups: dict[str, list[str]] = {}
    for domain_name in domain_names:
        key = _normalize_taxonomy_label(domain_name)
        normalized_groups.setdefault(key, []).append(domain_name)
    duplicate_domain_groups = [group for group in normalized_groups.values() if len(group) > 1]

    unknown_domains = [
        name for name in domain_names
        if name not in expected_domains and name != "General"
    ]

    general_stats = db.run_single(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(c:TopicCategory)-[:IN_DOMAIN]->(d:TopicDomain)
        WITH count(DISTINCT t) AS total,
             count(DISTINCT CASE WHEN c.name='General' OR d.name='General' THEN t END) AS general_topics
        RETURN total, general_topics,
               CASE WHEN total = 0 THEN 0.0 ELSE toFloat(general_topics) / toFloat(total) END AS ratio
        """
    ) or {}

    general_mention_stats = db.run_single(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(c:TopicCategory)-[:IN_DOMAIN]->(d:TopicDomain)
        OPTIONAL MATCH (n)-[:TAGGED]->(t)
        WITH t,c,d,count(n) AS mentions
        RETURN sum(mentions) AS total_mentions,
               sum(CASE WHEN c.name='General' OR d.name='General' THEN mentions ELSE 0 END) AS general_mentions,
               CASE
                 WHEN sum(mentions)=0 THEN 0.0
                 ELSE toFloat(sum(CASE WHEN c.name='General' OR d.name='General' THEN mentions ELSE 0 END)) / toFloat(sum(mentions))
               END AS ratio
        """
    ) or {}

    multi_rows = db.run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(c:TopicCategory)-[:IN_DOMAIN]->(d:TopicDomain)
        WITH t, collect(DISTINCT c.name) AS categories, collect(DISTINCT d.name) AS domains
        WHERE size(categories) > 1 OR size(domains) > 1
        RETURN t.name AS topic, categories, domains
        ORDER BY t.name
        LIMIT 200
        """
    )

    proposed_rows = db.run_single(
        """
        MATCH (t:Topic)
        RETURN count(t) AS total,
               sum(CASE WHEN coalesce(t.proposed,false) THEN 1 ELSE 0 END) AS proposed
        """
    ) or {}

    proposed_mention_stats = db.run_single(
        """
        MATCH (t:Topic)
        OPTIONAL MATCH (n)-[:TAGGED]->(t)
        WITH t, count(n) AS mentions
        RETURN sum(mentions) AS total_mentions,
               sum(CASE WHEN coalesce(t.proposed,false) THEN mentions ELSE 0 END) AS proposed_mentions,
               CASE
                 WHEN sum(mentions)=0 THEN 0.0
                 ELSE toFloat(sum(CASE WHEN coalesce(t.proposed,false) THEN mentions ELSE 0 END)) / toFloat(sum(mentions))
               END AS ratio
        """
    ) or {}

    pending_proposals = len(supabase_writer.list_topic_proposals(status="pending", limit=500))
    visible_emerging_proposals = len(supabase_writer.list_emerging_topic_candidates(status="pending", limit=500))

    total_topics = int(general_stats.get("total") or 0)
    general_topics = int(general_stats.get("general_topics") or 0)
    general_ratio = float(general_stats.get("ratio") or 0.0)
    general_mentions = int(general_mention_stats.get("general_mentions") or 0)
    total_mentions = int(general_mention_stats.get("total_mentions") or 0)
    general_ratio_mentions = float(general_mention_stats.get("ratio") or 0.0)

    proposed_total = int(proposed_rows.get("proposed") or 0)
    proposed_ratio = (proposed_total / total_topics) if total_topics else 0.0
    proposed_mentions = int(proposed_mention_stats.get("proposed_mentions") or 0)
    proposed_total_mentions = int(proposed_mention_stats.get("total_mentions") or 0)
    proposed_ratio_mentions = float(proposed_mention_stats.get("ratio") or 0.0)

    gates = {
        "domain_duplicates_zero": len(duplicate_domain_groups) == 0,
        "unknown_domains_zero": len(unknown_domains) == 0,
        "general_share_le_30pct": general_ratio_mentions <= 0.30,
        "multi_mapped_topics_le_5": len(multi_rows) <= 5,
        "proposed_share_le_20pct": proposed_ratio_mentions <= 0.20,
    }
    advisory = {
        "general_share_topics_le_35pct": general_ratio <= 0.35,
    }

    return {
        "summary": {
            "domains_total": len(domain_names),
            "topics_total": total_topics,
            "general_topics": general_topics,
            "general_ratio": round(general_ratio, 4),
            "general_mentions": general_mentions,
            "general_ratio_mentions": round(general_ratio_mentions, 4),
            "total_mentions": total_mentions,
            "proposed_topics": proposed_total,
            "proposed_ratio": round(proposed_ratio, 4),
            "proposed_mentions": proposed_mentions,
            "proposed_ratio_mentions": round(proposed_ratio_mentions, 4),
            "proposed_total_mentions": proposed_total_mentions,
            "multi_mapped_topics": len(multi_rows),
            "pending_topic_proposals": pending_proposals,
            "visible_emerging_proposals": visible_emerging_proposals,
        },
        "issues": {
            "duplicate_domain_groups": duplicate_domain_groups,
            "unknown_domains": unknown_domains,
            "multi_mapped_samples": multi_rows[:30],
        },
        "gates": gates,
        "advisory": advisory,
        "ready_for_phase2_signoff": all(gates.values()),
    }


def _build_ai_answer(query: str, dashboard: dict) -> str:
    q = query.lower()
    ru = _is_russian(query)

    trending = dashboard.get("trendingTopics") or []
    channels = dashboard.get("communityChannels") or []
    key_voices = dashboard.get("keyVoices") or []
    urgency = dashboard.get("urgencySignals") or []
    weekly = dashboard.get("weeklyShifts") or []
    community = dashboard.get("communityHealth") or {}

    top_topics = [t.get("name") for t in trending[:3] if isinstance(t, dict)]
    top_topics = [t for t in top_topics if t]
    top_channel = channels[0] if channels else {}
    top_voice = key_voices[0] if key_voices else {}
    week = weekly[0] if weekly else {}
    top_urgency = sorted(
        [u for u in urgency if isinstance(u, dict)],
        key=lambda u: int(u.get("urgentUsers", 0)),
        reverse=True,
    )[:3]

    def _weekly_delta(metric_key: str, *legacy_metric_names: str) -> str:
        for item in weekly:
            if not isinstance(item, dict):
                continue
            key = str(item.get("metricKey") or "").strip().lower()
            label = str(item.get("metric") or "").strip().lower()
            if key != metric_key and label not in legacy_metric_names:
                continue

            try:
                current = float(item.get("current") or 0)
                previous = float(item.get("previous") or 0)
            except Exception:
                return "N/A"

            if previous <= 0:
                return "new" if current > 0 else "0.0"
            return f"{round(100.0 * (current - previous) / previous, 1)}"

        legacy_value = week.get("postChange" if metric_key == "posts" else "commentChange")
        return str(legacy_value if legacy_value is not None else "N/A")

    def _format_delta(value: str) -> str:
        return value if value in {"N/A", "new"} else f"{value}%"

    post_delta = _weekly_delta("posts", "posts", "post")
    comment_delta = _weekly_delta("comments", "comments", "comment")

    if ru:
        if "жил" in q or "аренд" in q or "housing" in q:
            return (
                "**Сводка по жилью (живой срез дашборда)**\n\n"
                f"- Основные связанные темы: {', '.join(top_topics) if top_topics else 'недостаточно данных'}\n"
                f"- Самый активный канал: {top_channel.get('title', 'N/A')}\n"
                "- Рекомендация: закрепить FAQ по аренде, депозиту и проверке договоров"
            )
        if "голос" in q or "influenc" in q or "влият" in q:
            return (
                "**Ключевые голоса сообщества**\n\n"
                f"- Самый активный комментатор: user {top_voice.get('userId', 'N/A')}\n"
                f"- Роль: {top_voice.get('role', 'N/A')}\n"
                "- Рекомендация: вовлекать заметных комментаторов в модерацию и weekly digest"
            )
        if "сроч" in q or "urgent" in q or "криз" in q or "problem" in q:
            lines = [f"- {u.get('topic', 'N/A')}: {u.get('urgentUsers', 0)}" for u in top_urgency]
            lines_block = "\n".join(lines) if lines else "- Недостаточно сигналов срочности"
            return (
                "**Темы, требующие срочного внимания**\n\n"
                f"{lines_block}\n"
                "- Рекомендация: запустить быстрые модераторские ответы и закрепить FAQ по этим темам"
            )
        return (
            "**Краткий AI-обзор сообщества**\n\n"
            f"- Health score: {community.get('score', 'N/A')}\n"
            f"- Всего пользователей: {community.get('totalUsers', 'N/A')}\n"
            f"- Активные за 7 дней: {community.get('activeUsers', 'N/A')}\n"
            f"- Изменение постов за неделю: {_format_delta(post_delta)}\n"
            f"- Изменение комментариев за неделю: {_format_delta(comment_delta)}"
        )

    if "hous" in q or "rent" in q:
        return (
            "**Housing Snapshot (live dashboard window)**\n\n"
            f"- Leading related topics: {', '.join(top_topics) if top_topics else 'insufficient data'}\n"
            f"- Most active channel: {top_channel.get('title', 'N/A')}\n"
            "- Recommendation: pin a renter FAQ (contracts, deposits, neighborhood trade-offs)."
        )
    if "voice" in q or "influenc" in q:
        return (
            "**Key Community Voices**\n\n"
            f"- Most active commenter: user {top_voice.get('userId', 'N/A')}\n"
            f"- Role: {top_voice.get('role', 'N/A')}\n"
            "- Recommendation: involve visible commenters in moderation and weekly roundups."
        )
    if "urgent" in q or "problem" in q or "attention" in q:
        lines = [f"- {u.get('topic', 'N/A')}: {u.get('urgentUsers', 0)}" for u in top_urgency]
        lines_block = "\n".join(lines) if lines else "- Not enough urgency signals"
        return (
            "**Topics Requiring Urgent Attention**\n\n"
            f"{lines_block}\n"
            "- Recommendation: add rapid moderator response playbooks and pin practical guidance."
        )

    return (
        "**Community AI Brief**\n\n"
        f"- Health score: {community.get('score', 'N/A')}\n"
        f"- Total users: {community.get('totalUsers', 'N/A')}\n"
        f"- Active users (7d): {community.get('activeUsers', 'N/A')}\n"
        f"- Weekly post delta: {_format_delta(post_delta)}\n"
        f"- Weekly comment delta: {_format_delta(comment_delta)}"
    )


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    """Health check."""
    try:
        db.run_single("RETURN 1 AS ok")
        return {"status": "ok", "neo4j": "connected"}
    except Exception as e:
        return {"status": "degraded", "neo4j": str(e)}


@app.get("/api/dashboard", dependencies=[Depends(require_analytics_access)])
async def dashboard(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """
    Full dashboard data — matches the frontend's AppData interface.
    Cached for 5 minutes. Call POST /api/cache/clear to refresh.
    """
    try:
        freshness = _dashboard_freshness_snapshot(force_refresh=False)
        trusted_end = _trusted_end_date_from_freshness(freshness)
        if not from_date or not to_date:
            ctx = _default_dashboard_context(freshness)
        else:
            ctx = build_dashboard_date_context(from_date, to_date)
        loop = asyncio.get_running_loop()
        dashboard_data, dashboard_runtime_meta = await loop.run_in_executor(None, lambda: get_dashboard_snapshot(ctx))
        return {
            "data": dashboard_data,
            "meta": {
                "from": ctx.from_date.isoformat(),
                "to": ctx.to_date.isoformat(),
                "requestedFrom": from_date or ctx.from_date.isoformat(),
                "requestedTo": to_date or ctx.to_date.isoformat(),
                "days": ctx.days,
                "mode": "operational" if ctx.is_operational else "intelligence",
                "rangeLabel": ctx.range_label,
                "trustedEndDate": trusted_end.isoformat(),
                "degradedTiers": dashboard_runtime_meta.get("degradedTiers", []),
                "tierTimes": dashboard_runtime_meta.get("tierTimes", {}),
                "snapshotBuiltAt": dashboard_runtime_meta.get("snapshotBuiltAt"),
                "cacheStatus": dashboard_runtime_meta.get("cacheStatus"),
                "isStale": dashboard_runtime_meta.get("isStale", False),
                "buildElapsedSeconds": dashboard_runtime_meta.get("buildElapsedSeconds"),
                "buildMode": dashboard_runtime_meta.get("buildMode"),
                "freshness": {
                    "status": freshness.get("health", {}).get("status"),
                    "generatedAt": freshness.get("generated_at"),
                },
            },
        }
    except Exception as e:
        logger.error(f"Dashboard endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/ai/query", dependencies=[Depends(require_analytics_access)])
async def ai_query(request: AIQueryRequest):
    """Lightweight AI endpoint backed by the live dashboard snapshot."""
    try:
        freshness = _dashboard_freshness_snapshot(force_refresh=False)
        dashboard_data = get_dashboard_data(_default_dashboard_context(freshness))
        answer = _build_ai_answer(request.query, dashboard_data)
        return {
            "query": request.query,
            "answer": answer,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.error(f"AI query endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/topics", dependencies=[Depends(require_analytics_access)])
async def topics(
    page: int = Query(0, ge=0),
    size: int = Query(500, ge=1, le=1000),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Topics detail page — paginated."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: get_topics_page(page, size, ctx))
    except Exception as e:
        logger.error(f"Topics endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/channels", dependencies=[Depends(require_analytics_access)])
async def channels(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Channels detail page."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: get_channels_page(ctx))
    except Exception as e:
        logger.error(f"Channels endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/graph", dependencies=[Depends(require_analytics_access)])
async def graph_data(payload: GraphRequest):
    """Graph dataset for /graph page (server-side Neo4j)."""
    try:
        filters = payload.model_dump(exclude_none=True)
        graph = graph_dashboard.get_graph_data(filters)
        freshness = get_freshness_snapshot(
            supabase_writer,
            scheduler_status=scraper_scheduler.status(),
        )
        if not isinstance(graph, dict):
            return graph
        meta_existing = graph.get("meta")
        meta_dict = dict(meta_existing) if isinstance(meta_existing, dict) else {}
        meta_dict["freshness"] = {
            "status": freshness.get("health", {}).get("status"),
            "score": freshness.get("health", {}).get("score"),
            "generatedAt": freshness.get("generated_at"),
            "lastScrapeAt": freshness.get("pipeline", {}).get("scrape", {}).get("last_scrape_at"),
            "lastProcessAt": freshness.get("pipeline", {}).get("process", {}).get("last_process_at"),
            "lastGraphSyncAt": freshness.get("pipeline", {}).get("sync", {}).get("last_graph_sync_at"),
            "syncEstimated": freshness.get("pipeline", {}).get("sync", {}).get("estimated"),
            "unsyncedPosts": freshness.get("backlog", {}).get("unsynced_posts"),
            "analyticsWindowDays": freshness.get("drift", {}).get("analytics_window_days"),
            "latestPostDeltaMinutes": freshness.get("drift", {}).get("latest_post_delta_minutes"),
        }
        graph["meta"] = meta_dict
        return graph
    except Exception as e:
        logger.error(f"Graph data endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/node-details", dependencies=[Depends(require_analytics_access)])
async def node_details(
    nodeId: str = Query(...),
    nodeType: str = Query(...),
    timeframe: Optional[str] = Query(default="Last 7 Days"),
    channels: Optional[str] = Query(default=None),
):
    """Detailed panel data for a graph node."""
    try:
        channel_filters = [c.strip() for c in (channels or "").split(",") if c.strip()]
        details = graph_dashboard.get_node_details(
            nodeId,
            nodeType,
            timeframe=timeframe,
            channels=channel_filters,
        )
        if not details:
            raise HTTPException(status_code=404, detail="Node not found")
        return details
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Node details endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/search", dependencies=[Depends(require_analytics_access)])
async def search_graph(query: str = Query("", min_length=0, max_length=200), limit: int = Query(20, ge=1, le=100)):
    """Graph search across channels/topics/intents."""
    try:
        if not (query or "").strip():
            return []
        return graph_dashboard.search_graph(query, limit)
    except Exception as e:
        logger.error(f"Graph search endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trending-topics", dependencies=[Depends(require_analytics_access)])
async def trending_topics(limit: int = Query(10, ge=1, le=100), timeframe: str = Query("Last 7 Days")):
    """Top trending topics for graph filters."""
    try:
        return graph_dashboard.get_trending_topics(limit, timeframe)
    except Exception as e:
        logger.error(f"Trending topics endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/top-channels", dependencies=[Depends(require_analytics_access)])
async def top_channels(limit: int = Query(10, ge=1, le=100), timeframe: str = Query("Last 7 Days")):
    """Top channels by post activity (graph context)."""
    try:
        return graph_dashboard.get_top_channels(limit, timeframe)
    except Exception as e:
        logger.error(f"Top channels endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/all-channels", dependencies=[Depends(require_analytics_access)])
async def all_channels_graph():
    """All channels list for graph filters."""
    try:
        return graph_dashboard.get_all_channels()
    except Exception as e:
        logger.error(f"All channels endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/top-brands", dependencies=[Depends(require_analytics_access)])
async def top_brands_compat(limit: int = Query(10, ge=1, le=100), timeframe: str = Query("Last 7 Days")):
    """Compatibility endpoint: returns top channels in legacy shape."""
    try:
        return graph_dashboard.get_top_channels(limit, timeframe)
    except Exception as e:
        logger.error(f"Top brands compatibility endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/all-brands", dependencies=[Depends(require_analytics_access)])
async def all_brands_compat():
    """Compatibility endpoint: returns all channels in legacy shape."""
    try:
        return graph_dashboard.get_all_channels()
    except Exception as e:
        logger.error(f"All brands compatibility endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sentiment-distribution", dependencies=[Depends(require_analytics_access)])
async def sentiment_distribution(timeframe: str = Query("Last 7 Days")):
    """Sentiment distribution for graph side panels/filters."""
    try:
        return graph_dashboard.get_sentiment_distribution(timeframe)
    except Exception as e:
        logger.error(f"Sentiment distribution endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/graph-insights", dependencies=[Depends(require_analytics_access)])
async def graph_insights(timeframe: str = Query("Last 7 Days")):
    """Narrative summary for graph context."""
    try:
        return graph_dashboard.get_graph_insights(timeframe)
    except Exception as e:
        logger.error(f"Graph insights endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/insights/cards", dependencies=[Depends(require_analytics_access)])
async def insight_cards(payload: InsightCardsRequest):
    """Structured insight cards for analyst/executive surfaces."""
    try:
        audience = (payload.audience or "analyst").strip().lower()
        if audience not in {"analyst", "executive"}:
            audience = "analyst"
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: insights.get_insight_cards(payload.filters or {}, audience),
        )
    except Exception as e:
        logger.error(f"Insight cards endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/config")
async def get_admin_config():
    """Return merged Admin config with defaults and runtime overrides."""
    try:
        return _admin_config_response()
    except Exception as e:
        logger.error(f"Get admin config error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/admin/config")
async def update_admin_config(payload: AdminConfigPatchRequest):
    """Persist lightweight Admin page config in runtime storage."""
    try:
        current = _load_admin_config()
        widgets = _validate_admin_widgets(payload.widgets)
        prompts = _validate_admin_prompts(payload.prompts)
        runtime = _validate_admin_runtime(payload.runtime)

        if not widgets and not prompts and not runtime:
            raise HTTPException(status_code=400, detail="No update fields provided")

        next_config = {
            "widgets": {**current["widgets"], **widgets},
            "prompts": {**current["prompts"], **prompts},
            "runtime": {**current["runtime"], **runtime},
        }

        if not save_admin_config_raw(next_config):
            raise HTTPException(
                status_code=500,
                detail=get_admin_config_runtime_warning() or "Failed to persist admin config",
            )
        return _admin_config_response()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update admin config error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sources/channels")
async def list_channel_sources():
    """List configured Telegram channel sources from Supabase."""
    try:
        items = supabase_writer.list_channels()
        return {"count": len(items), "items": items}
    except Exception as e:
        logger.error(f"List source channels error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/sources/channels")
async def create_channel_source(payload: ChannelSourceCreateRequest):
    """Create or reactivate a Telegram channel source for scheduler pickup."""
    try:
        normalized_handle = _normalize_channel_username(payload.channel_username)
        _validate_channel_username(normalized_handle)
        canonical_username = _canonical_channel_username(normalized_handle)

        provided_title = (payload.channel_title or "").strip()
        channel_title = provided_title or normalized_handle

        existing = supabase_writer.get_channel_by_handle(normalized_handle)
        if existing:
            update_payload = {
                "channel_username": canonical_username,
                "scrape_depth_days": payload.scrape_depth_days,
                "scrape_comments": payload.scrape_comments,
            }
            existing_title = (existing.get("channel_title") or "").strip()
            if provided_title:
                update_payload["channel_title"] = provided_title
            elif (not existing_title) or (existing_title.lower() == canonical_username.lower()):
                update_payload["channel_title"] = normalized_handle
            action = "exists"
            if not existing.get("is_active", False):
                update_payload["is_active"] = True
                action = "reactivated"

            updated = supabase_writer.update_channel(existing["id"], update_payload)
            if updated:
                await _try_enrich_channel_metadata(
                    updated["id"],
                    updated.get("channel_username") or canonical_username,
                    updated.get("channel_title"),
                )
                updated = supabase_writer.get_channel_by_id(updated["id"]) or updated
            return {"action": action, "item": updated}

        created = supabase_writer.create_channel(
            {
                "channel_username": canonical_username,
                "channel_title": channel_title,
                "is_active": True,
                "scrape_depth_days": payload.scrape_depth_days,
                "scrape_comments": payload.scrape_comments,
            }
        )
        await _try_enrich_channel_metadata(
            created["id"],
            created.get("channel_username") or canonical_username,
            created.get("channel_title"),
        )
        created = supabase_writer.get_channel_by_id(created["id"]) or created
        return {"action": "created", "item": created}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create source channel error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/sources/channels/{channel_id}")
async def update_channel_source(channel_id: str, payload: ChannelSourceUpdateRequest):
    """Update source settings (active flag and scrape settings)."""
    try:
        existing = supabase_writer.get_channel_by_id(channel_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Channel source not found")

        update_payload: dict = {}
        if payload.is_active is not None:
            update_payload["is_active"] = payload.is_active
        if payload.scrape_depth_days is not None:
            update_payload["scrape_depth_days"] = payload.scrape_depth_days
        if payload.scrape_comments is not None:
            update_payload["scrape_comments"] = payload.scrape_comments

        if not update_payload:
            raise HTTPException(status_code=400, detail="No update fields provided")

        updated = supabase_writer.update_channel(channel_id, update_payload)
        if updated and updated.get("is_active"):
            await _try_enrich_channel_metadata(
                updated["id"],
                updated.get("channel_username") or "",
                updated.get("channel_title"),
            )
            updated = supabase_writer.get_channel_by_id(updated["id"]) or updated
        return {"item": updated}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update source channel error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/scraper/scheduler")
async def get_scraper_scheduler_status():
    """Current scraper scheduler runtime status."""
    return scraper_scheduler.status()


@app.get("/api/freshness", dependencies=[Depends(require_analytics_access)])
async def freshness_snapshot(force: bool = Query(False)):
    """Pipeline freshness/truth snapshot with backlog and Supabase↔Neo4j drift."""
    try:
        return get_freshness_snapshot(
            supabase_writer,
            scheduler_status=scraper_scheduler.status(),
            force_refresh=force,
        )
    except Exception as e:
        logger.error(f"Freshness endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/quality/taxonomy", dependencies=[Depends(require_analytics_access)])
async def taxonomy_quality_snapshot():
    """Enterprise taxonomy quality snapshot with sign-off gates."""
    try:
        loop = asyncio.get_running_loop()
        snapshot = await loop.run_in_executor(None, _taxonomy_quality_snapshot)
        snapshot["generated_at"] = datetime.now(timezone.utc).isoformat()
        return snapshot
    except Exception as e:
        logger.error(f"Taxonomy quality endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraper/scheduler/start")
async def start_scraper_scheduler():
    """Start recurring scraper schedule using persisted interval."""
    try:
        return await scraper_scheduler.start()
    except Exception as e:
        logger.error(f"Start scraper scheduler error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraper/scheduler/stop")
async def stop_scraper_scheduler():
    """Stop recurring scraper schedule."""
    try:
        return await scraper_scheduler.stop()
    except Exception as e:
        logger.error(f"Stop scraper scheduler error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/scraper/scheduler")
async def update_scraper_scheduler(payload: ScraperSchedulerUpdateRequest):
    """Update scraper scheduler interval in minutes."""
    try:
        return await scraper_scheduler.set_interval(payload.interval_minutes)
    except Exception as e:
        logger.error(f"Update scraper scheduler error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraper/scheduler/run-once")
async def run_scraper_once():
    """Trigger one immediate scrape cycle."""
    try:
        return await scraper_scheduler.run_once()
    except Exception as e:
        logger.error(f"Run-once scraper error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraper/scheduler/catchup-once")
async def run_scraper_catchup_once():
    """Trigger one immediate processing/sync-heavy catch-up cycle (no scraping)."""
    try:
        return await scraper_scheduler.run_catchup_once()
    except Exception as e:
        logger.error(f"Catchup-once scraper error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/ai/failures")
async def list_ai_failures(
    dead_letter_only: bool = Query(True),
    scope_type: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    """List AI processing failure scopes for operator triage."""
    try:
        items = supabase_writer.list_processing_failures(
            dead_letter_only=dead_letter_only,
            scope_type=scope_type,
            limit=limit,
        )
        return {
            "count": len(items),
            "items": items,
        }
    except Exception as e:
        logger.error(f"List AI failures endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/ai/failures/retry")
async def retry_ai_failures(payload: FailureRetryRequest):
    """Unlock selected AI failure scopes for immediate retry."""
    scope_type = (payload.scope_type or "").strip().lower()
    if scope_type not in {"comment_group", "post"}:
        raise HTTPException(status_code=400, detail="scope_type must be 'comment_group' or 'post'")

    try:
        retried = supabase_writer.retry_processing_failures(
            scope_type=scope_type,
            scope_keys=payload.scope_keys,
        )
        return {
            "retried": int(retried),
            "scope_type": scope_type,
        }
    except Exception as e:
        logger.error(f"Retry AI failures endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/taxonomy/proposals")
async def list_taxonomy_proposals(
    status: str = Query("pending"),
    visibility_state: Optional[str] = Query(None),
    visible_only: bool = Query(False),
    limit: int = Query(100, ge=1, le=500),
):
    """List proposed topics for review queue triage."""
    try:
        items = supabase_writer.list_topic_proposals(status=status, limit=limit)
        normalized_visibility = (visibility_state or "").strip().lower()

        if visible_only:
            filtered: list[dict] = []
            for item in items:
                state = str(item.get("visibility_state") or "").strip().lower()
                if bool(item.get("visibility_eligible")) or state == "emerging_visible" or int(item.get("proposed_count") or 0) >= 3:
                    filtered.append(item)
            items = filtered

        if normalized_visibility:
            items = [
                item for item in items
                if str(item.get("visibility_state") or "").strip().lower() == normalized_visibility
            ]

        return {
            "count": len(items),
            "status": status,
            "visibility_state": visibility_state,
            "visible_only": visible_only,
            "items": items,
        }
    except Exception as e:
        logger.error(f"List taxonomy proposals endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/taxonomy/trending-new")
async def list_taxonomy_trending_new(
    status: str = Query("pending"),
    limit: int = Query(30, ge=1, le=200),
):
    """List emerging proposed topics eligible for frontend visibility."""
    try:
        items = supabase_writer.list_emerging_topic_candidates(status=status, limit=limit)
        return {
            "count": len(items),
            "status": status,
            "items": items,
        }
    except Exception as e:
        logger.error(f"List taxonomy trending-new endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/taxonomy/proposals/review")
async def review_taxonomy_proposal(payload: TopicProposalReviewRequest):
    """Approve or reject a proposed topic, with optional alias promotions."""
    decision = (payload.decision or "").strip().lower()
    if decision not in {"approve", "reject"}:
        raise HTTPException(status_code=400, detail="decision must be 'approve' or 'reject'")

    try:
        item = supabase_writer.review_topic_proposal(
            topic_name=payload.topic_name,
            decision=decision,
            canonical_topic=payload.canonical_topic,
            aliases=payload.aliases,
            notes=payload.notes,
            reviewed_by=payload.reviewed_by,
        )
        if not item:
            raise HTTPException(status_code=404, detail="Topic proposal not found")
        return {
            "success": True,
            "item": item,
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Review taxonomy proposal endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/taxonomy/promotions")
async def list_taxonomy_promotions(
    active_only: bool = Query(True),
    limit: int = Query(200, ge=1, le=500),
):
    """List runtime topic promotion aliases."""
    try:
        items = supabase_writer.list_topic_promotions(limit=limit, active_only=active_only)
        return {
            "count": len(items),
            "active_only": active_only,
            "items": items,
        }
    except Exception as e:
        logger.error(f"List taxonomy promotions endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/taxonomy/promotions/reload")
async def reload_taxonomy_promotions():
    """Reload runtime alias map from approved promotions table."""
    try:
        loaded = supabase_writer.refresh_runtime_topic_aliases()
        return {
            "loaded_aliases": int(loaded),
        }
    except Exception as e:
        logger.error(f"Reload taxonomy promotions endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/audience", dependencies=[Depends(require_analytics_access)])
async def audience(
    page: int = Query(0, ge=0),
    size: int = Query(500, ge=1, le=1000),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Audience detail page — paginated."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: get_audience_page(page, size, ctx))
    except Exception as e:
        logger.error(f"Audience endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/cache/clear")
async def clear_cache():
    """Invalidate the in-memory dashboard cache."""
    invalidate_cache()
    question_briefs.invalidate_question_briefs_cache()
    behavioral_briefs.invalidate_behavioral_briefs_cache()
    opportunity_briefs.invalidate_opportunity_briefs_cache()
    return {"success": True, "message": "Cache cleared"}


@app.post("/api/question-briefs/debug/refresh")
async def debug_refresh_question_briefs():
    """Force-refresh question cards and return stage diagnostics."""
    try:
        loop = asyncio.get_running_loop()
        diagnostics = await loop.run_in_executor(
            None,
            lambda: question_briefs.refresh_question_briefs_with_diagnostics(force=True),
        )
        return {
            "success": True,
            "cardsProduced": diagnostics.get("cardsProduced", 0),
            "firstRejectionBucket": diagnostics.get("firstRejectionBucket", ""),
            "diagnostics": diagnostics,
        }
    except Exception as e:
        logger.error(f"Question briefs debug refresh endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/behavioral-briefs/debug/refresh")
async def debug_refresh_behavioral_briefs():
    """Force-refresh behavioral cards and return stage diagnostics."""
    try:
        loop = asyncio.get_running_loop()
        diagnostics = await loop.run_in_executor(
            None,
            lambda: behavioral_briefs.refresh_behavioral_briefs_with_diagnostics(force=True),
        )
        return {
            "success": True,
            "problemCardsProduced": diagnostics.get("stages", {}).get("problemCards", 0),
            "serviceCardsProduced": diagnostics.get("stages", {}).get("serviceCards", 0),
            "urgencyCardsProduced": diagnostics.get("stages", {}).get("urgencyCards", 0),
            "diagnostics": diagnostics,
        }
    except Exception as e:
        logger.error(f"Behavioral briefs debug refresh endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/opportunity-briefs/debug/refresh")
async def debug_refresh_opportunity_briefs():
    """Force-refresh opportunity cards and return stage diagnostics."""
    try:
        loop = asyncio.get_running_loop()
        diagnostics = await loop.run_in_executor(
            None,
            lambda: opportunity_briefs.refresh_opportunity_briefs_with_diagnostics(force=True),
        )
        return {
            "success": True,
            "cardsProduced": diagnostics.get("cardsProduced", 0),
            "firstRejectionBucket": diagnostics.get("firstRejectionBucket", ""),
            "diagnostics": diagnostics,
        }
    except Exception as e:
        logger.error(f"Opportunity briefs debug refresh endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Shutdown hook ────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    key_fp = hashlib.sha256(config.OPENAI_API_KEY.encode("utf-8")).hexdigest()[:12] if config.OPENAI_API_KEY else "missing"
    logger.info(f"AI runtime configured | model={config.OPENAI_MODEL} key_fp={key_fp}")
    await scraper_scheduler.startup()
    _start_question_cards_scheduler()
    _start_behavioral_cards_scheduler()
    _start_opportunity_cards_scheduler()
    asyncio.create_task(_warm_dashboard_cache())
    asyncio.create_task(_warm_detail_caches())
    if config.QUESTION_BRIEFS_REFRESH_ON_STARTUP:
        asyncio.create_task(_materialize_question_cards_once(force=False))
    if config.BEHAVIORAL_BRIEFS_REFRESH_ON_STARTUP:
        asyncio.create_task(_materialize_behavioral_cards_once(force=False))
    if config.OPPORTUNITY_BRIEFS_REFRESH_ON_STARTUP:
        asyncio.create_task(_materialize_opportunity_cards_once(force=False))

@app.on_event("shutdown")
async def shutdown():
    global question_cards_scheduler, behavioral_cards_scheduler, opportunity_cards_scheduler
    if question_cards_scheduler is not None:
        try:
            question_cards_scheduler.shutdown(wait=False)
        except Exception:
            pass
        question_cards_scheduler = None
    if behavioral_cards_scheduler is not None:
        try:
            behavioral_cards_scheduler.shutdown(wait=False)
        except Exception:
            pass
        behavioral_cards_scheduler = None
    if opportunity_cards_scheduler is not None:
        try:
            opportunity_cards_scheduler.shutdown(wait=False)
        except Exception:
            pass
        opportunity_cards_scheduler = None
    await scraper_scheduler.shutdown()
    db.close()
    logger.info("API server shut down — Neo4j driver closed")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api.server:app",
        host=config.APP_HOST,
        port=config.APP_PORT,
        reload=False,
    )
