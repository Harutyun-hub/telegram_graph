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
import asyncio
from datetime import datetime, timezone
import re
from typing import Optional, List

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
config.validate()

from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from api.aggregator import (
    get_dashboard_data, get_topics_page, get_channels_page,
    get_audience_page, invalidate_cache
)
from api.queries import graph_dashboard
from api.freshness import get_freshness_snapshot
from api import db
from buffer.supabase_writer import SupabaseWriter
from api.scraper_scheduler import ScraperSchedulerService
from scraper.channel_metadata import get_full_channel_metadata

# ── App setup ────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Radar Obshchiny API",
    description="Dashboard data API for the Radar Obshchiny community intelligence platform",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
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


supabase_writer = SupabaseWriter()
scraper_scheduler = ScraperSchedulerService(supabase_writer)
USERNAME_RE = re.compile(r"^[a-z][a-z0-9_]{4,31}$")


async def _warm_dashboard_cache() -> None:
    """Warm dashboard cache in background after startup."""
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, get_dashboard_data)
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
    except Exception as e:
        logger.warning(f"Telethon unavailable for metadata enrichment: {e}")
        return

    username = _canonical_channel_username(canonical_username)
    if not username:
        return

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

    if ru:
        if "жил" in q or "аренд" in q or "housing" in q:
            return (
                "**Сводка по жилью (синтетический датасет)**\n\n"
                f"- Основные связанные темы: {', '.join(top_topics) if top_topics else 'недостаточно данных'}\n"
                f"- Самый активный канал: {top_channel.get('title', 'N/A')}\n"
                "- Рекомендация: закрепить FAQ по аренде, депозиту и проверке договоров"
            )
        if "голос" in q or "influenc" in q or "влият" in q:
            return (
                "**Ключевые голоса сообщества**\n\n"
                f"- Топ-участник по влиянию: user {top_voice.get('userId', 'N/A')}\n"
                f"- Роль: {top_voice.get('role', 'N/A')}\n"
                "- Рекомендация: вовлекать ключевых участников в модерацию и weekly digest"
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
            f"- Изменение постов за неделю: {week.get('postChange', 'N/A')}%\n"
            f"- Изменение комментариев за неделю: {week.get('commentChange', 'N/A')}%"
        )

    if "hous" in q or "rent" in q:
        return (
            "**Housing Snapshot (synthetic dataset)**\n\n"
            f"- Leading related topics: {', '.join(top_topics) if top_topics else 'insufficient data'}\n"
            f"- Most active channel: {top_channel.get('title', 'N/A')}\n"
            "- Recommendation: pin a renter FAQ (contracts, deposits, neighborhood trade-offs)."
        )
    if "voice" in q or "influenc" in q:
        return (
            "**Key Community Voices**\n\n"
            f"- Top influence profile: user {top_voice.get('userId', 'N/A')}\n"
            f"- Role: {top_voice.get('role', 'N/A')}\n"
            "- Recommendation: involve top contributors in moderation and weekly roundups."
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
        f"- Weekly post delta: {week.get('postChange', 'N/A')}%\n"
        f"- Weekly comment delta: {week.get('commentChange', 'N/A')}%"
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


@app.get("/api/dashboard")
async def dashboard():
    """
    Full dashboard data — matches the frontend's AppData interface.
    Cached for 5 minutes. Call POST /api/cache/clear to refresh.
    """
    try:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, get_dashboard_data)
    except Exception as e:
        logger.error(f"Dashboard endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/ai/query")
async def ai_query(request: AIQueryRequest):
    """Lightweight AI endpoint backed by synthetic dashboard data."""
    try:
        dashboard_data = get_dashboard_data()
        answer = _build_ai_answer(request.query, dashboard_data)
        return {
            "query": request.query,
            "answer": answer,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.error(f"AI query endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/topics")
async def topics(page: int = Query(0, ge=0), size: int = Query(500, ge=1, le=1000)):
    """Topics detail page — paginated."""
    try:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: get_topics_page(page, size))
    except Exception as e:
        logger.error(f"Topics endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/channels")
async def channels():
    """Channels detail page."""
    try:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, get_channels_page)
    except Exception as e:
        logger.error(f"Channels endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/graph")
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
            "latestPostDeltaMinutes": freshness.get("drift", {}).get("latest_post_delta_minutes"),
        }
        graph["meta"] = meta_dict
        return graph
    except Exception as e:
        logger.error(f"Graph data endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/node-details")
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


@app.get("/api/search")
async def search_graph(query: str = Query("", min_length=0, max_length=200), limit: int = Query(20, ge=1, le=100)):
    """Graph search across channels/topics/intents."""
    try:
        if not (query or "").strip():
            return []
        return graph_dashboard.search_graph(query, limit)
    except Exception as e:
        logger.error(f"Graph search endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trending-topics")
async def trending_topics(limit: int = Query(10, ge=1, le=100), timeframe: str = Query("Last 7 Days")):
    """Top trending topics for graph filters."""
    try:
        return graph_dashboard.get_trending_topics(limit, timeframe)
    except Exception as e:
        logger.error(f"Trending topics endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/top-channels")
async def top_channels(limit: int = Query(10, ge=1, le=100), timeframe: str = Query("Last 7 Days")):
    """Top channels by post activity (graph context)."""
    try:
        return graph_dashboard.get_top_channels(limit, timeframe)
    except Exception as e:
        logger.error(f"Top channels endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/all-channels")
async def all_channels_graph():
    """All channels list for graph filters."""
    try:
        return graph_dashboard.get_all_channels()
    except Exception as e:
        logger.error(f"All channels endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/top-brands")
async def top_brands_compat(limit: int = Query(10, ge=1, le=100), timeframe: str = Query("Last 7 Days")):
    """Compatibility endpoint: returns top channels in legacy shape."""
    try:
        return graph_dashboard.get_top_channels(limit, timeframe)
    except Exception as e:
        logger.error(f"Top brands compatibility endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/all-brands")
async def all_brands_compat():
    """Compatibility endpoint: returns all channels in legacy shape."""
    try:
        return graph_dashboard.get_all_channels()
    except Exception as e:
        logger.error(f"All brands compatibility endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sentiment-distribution")
async def sentiment_distribution(timeframe: str = Query("Last 7 Days")):
    """Sentiment distribution for graph side panels/filters."""
    try:
        return graph_dashboard.get_sentiment_distribution(timeframe)
    except Exception as e:
        logger.error(f"Sentiment distribution endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/graph-insights")
async def graph_insights(timeframe: str = Query("Last 7 Days")):
    """Narrative summary for graph context."""
    try:
        return graph_dashboard.get_graph_insights(timeframe)
    except Exception as e:
        logger.error(f"Graph insights endpoint error: {e}")
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


@app.get("/api/freshness")
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


@app.get("/api/audience")
async def audience(page: int = Query(0, ge=0), size: int = Query(500, ge=1, le=1000)):
    """Audience detail page — paginated."""
    try:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: get_audience_page(page, size))
    except Exception as e:
        logger.error(f"Audience endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/cache/clear")
async def clear_cache():
    """Invalidate the in-memory dashboard cache."""
    invalidate_cache()
    return {"success": True, "message": "Cache cleared"}


# ── Shutdown hook ────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    await scraper_scheduler.startup()
    asyncio.create_task(_warm_dashboard_cache())
    asyncio.create_task(_warm_detail_caches())

@app.on_event("shutdown")
async def shutdown():
    await scraper_scheduler.shutdown()
    db.close()
    logger.info("API server shut down — Neo4j driver closed")
