"""Insight cards service (deterministic baseline + AI-ready data pack)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import config
from api import db


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _days_from_timeframe(value: str | None) -> int:
    text = (value or "").strip().lower()
    if "24" in text or "day" in text:
        return 1
    if "30" in text or "month" in text:
        return 30
    if "90" in text or "quarter" in text:
        return 90
    return 7


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _query_top_topics(days: int, channels: list[str] | None) -> list[dict]:
    cypher = """
    MATCH (p:Post)-[:TAGGED]->(t:Topic)
    OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
    WHERE p.posted_at >= datetime() - duration({days: $days})
      AND ($channels_empty OR coalesce(ch.title, ch.username, ch.uuid) IN $channels)
    RETURN t.name AS topic,
           count(DISTINCT p) AS posts,
           avg(coalesce(p.comment_count, 0)) AS avgComments
    ORDER BY posts DESC
    LIMIT 8
    """
    return db.run_query(
        cypher,
        {
            "days": max(1, int(days)),
            "channels": channels or [],
            "channels_empty": not bool(channels),
        },
    )


def _query_sentiment_breakdown(days: int, channels: list[str] | None) -> list[dict]:
    cypher = """
    MATCH (u:User)-[r:HAS_SENTIMENT]->(s:Sentiment)
    WHERE coalesce(r.last_seen, datetime()) >= datetime() - duration({days: $days})
    OPTIONAL MATCH (u)-[:WROTE]->(:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
    WHERE $channels_empty OR coalesce(ch.title, ch.username, ch.uuid) IN $channels
    RETURN s.label AS sentiment, sum(coalesce(r.count, 1)) AS mentions
    ORDER BY mentions DESC
    LIMIT 6
    """
    return db.run_query(
        cypher,
        {
            "days": max(1, int(days)),
            "channels": channels or [],
            "channels_empty": not bool(channels),
        },
    )


def _query_entity_mentions(days: int, channels: list[str] | None) -> list[dict]:
    cypher = """
    MATCH (c:Comment)-[:MENTIONS_ENTITY]->(e:Entity)
    MATCH (c)-[:REPLIES_TO]->(p:Post)
    OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
    WHERE c.posted_at >= datetime() - duration({days: $days})
      AND ($channels_empty OR coalesce(ch.title, ch.username, ch.uuid) IN $channels)
    RETURN e.name AS entity,
           coalesce(e.type, 'concept') AS type,
           count(c) AS mentions
    ORDER BY mentions DESC
    LIMIT 6
    """
    return db.run_query(
        cypher,
        {
            "days": max(1, int(days)),
            "channels": channels or [],
            "channels_empty": not bool(channels),
        },
    )


def _priority_from_count(value: int) -> str:
    if value >= 100:
        return "high"
    if value >= 30:
        return "medium"
    return "low"


def _build_fallback_cards(evidence_pack: dict, audience: str) -> list[dict]:
    cards: list[dict] = []
    now = _now_iso()

    top_topics = evidence_pack.get("top_topics") or []
    if top_topics:
        best = top_topics[0]
        posts = _safe_int(best.get("posts"), 0)
        cards.append(
            {
                "id": "topic-momentum",
                "title": f"Topic momentum: {best.get('topic', 'Unknown')}",
                "summary": "Highest post volume in selected window.",
                "why_it_matters": "High-volume topics indicate where narratives are concentrating.",
                "confidence": 78,
                "priority": _priority_from_count(posts),
                "audience": audience,
                "evidence": [
                    {
                        "query_id": "top_topics",
                        "metric": "posts",
                        "value": posts,
                        "note": f"avgComments={round(float(best.get('avgComments') or 0), 2)}",
                    }
                ],
                "generated_at": now,
            }
        )

    sentiments = evidence_pack.get("sentiment_breakdown") or []
    if sentiments:
        top_sentiment = sentiments[0]
        cards.append(
            {
                "id": "dominant-sentiment",
                "title": f"Dominant sentiment: {top_sentiment.get('sentiment', 'Unknown')}",
                "summary": "Most frequent sentiment signal among active users.",
                "why_it_matters": "Sentiment concentration helps identify communication posture and risk.",
                "confidence": 72,
                "priority": "medium",
                "audience": audience,
                "evidence": [
                    {
                        "query_id": "sentiment_breakdown",
                        "metric": "mentions",
                        "value": _safe_int(top_sentiment.get("mentions"), 0),
                    }
                ],
                "generated_at": now,
            }
        )

    entities = evidence_pack.get("entity_mentions") or []
    if entities:
        entity = entities[0]
        cards.append(
            {
                "id": "entity-focus",
                "title": f"Entity focus: {entity.get('entity', 'Unknown')}",
                "summary": "Most referenced entity in comment discourse.",
                "why_it_matters": "Entity concentration can indicate campaign focus, pressure points, or trust anchors.",
                "confidence": 70,
                "priority": "medium",
                "audience": audience,
                "evidence": [
                    {
                        "query_id": "entity_mentions",
                        "metric": "mentions",
                        "value": _safe_int(entity.get("mentions"), 0),
                        "note": f"type={entity.get('type', 'concept')}",
                    }
                ],
                "generated_at": now,
            }
        )

    return cards


def build_evidence_pack(filters: dict | None = None) -> dict:
    filters = filters or {}
    channels = filters.get("channels") or filters.get("brandSource") or []
    topics = filters.get("topics") or []
    days = _days_from_timeframe(filters.get("timeframe"))

    # Topics filter is currently reserved for AI layer post-processing.
    _ = topics

    return {
        "timeframe_days": days,
        "top_topics": _query_top_topics(days, channels),
        "sentiment_breakdown": _query_sentiment_breakdown(days, channels),
        "entity_mentions": _query_entity_mentions(days, channels),
    }


def get_insight_cards(filters: dict | None = None, audience: str = "analyst") -> dict:
    evidence_pack = build_evidence_pack(filters)

    # AI layer is introduced behind a feature flag. Deterministic fallback is always available.
    cards = _build_fallback_cards(evidence_pack, audience)
    source = "deterministic_fallback"

    if config.FEATURE_AI_ANALYST:
        # AI-enabled implementation will augment this path with schema-validated cards.
        source = "deterministic_fallback"

    return {
        "cards": cards,
        "source": source,
        "generated_at": _now_iso(),
        "evidence_pack": evidence_pack if audience == "analyst" else None,
    }
