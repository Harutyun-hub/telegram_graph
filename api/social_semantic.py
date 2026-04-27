from __future__ import annotations

import threading
import time
from collections import defaultdict
from datetime import datetime, time as datetime_time, timedelta, timezone
from typing import Any

from loguru import logger
from neo4j import GraphDatabase

import config


_CACHE_TTL_SECONDS = 300.0
_ORGANIC_SOURCE_KINDS = ("post",)
_NOISE_TOPIC_NAMES = {"", "null", "unknown", "none", "n/a", "na"}

_driver_lock = threading.Lock()
_driver: Any | None = None
_cache_lock = threading.Lock()
_cache: dict[tuple[Any, ...], tuple[float, Any]] = {}


def _social_neo4j_uri() -> str:
    uri = config.SOCIAL_NEO4J_URI
    if uri.startswith("neo4j+s://"):
        return uri.replace("neo4j+s://", "neo4j+ssc://")
    return uri


def _get_driver():
    global _driver
    if _driver is not None:
        return _driver
    with _driver_lock:
        if _driver is None:
            _driver = GraphDatabase.driver(
                _social_neo4j_uri(),
                auth=(config.SOCIAL_NEO4J_USERNAME, config.SOCIAL_NEO4J_PASSWORD),
                connection_timeout=10.0,
                connection_acquisition_timeout=10.0,
                max_connection_pool_size=4,
                max_transaction_retry_time=5.0,
            )
            _driver.verify_connectivity()
        return _driver


def _cache_get(key: tuple[Any, ...]):
    now = time.monotonic()
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (now - entry[0]) < _CACHE_TTL_SECONDS:
            return entry[1]
    return None


def _cache_set(key: tuple[Any, ...], value: Any) -> Any:
    with _cache_lock:
        if len(_cache) >= 128:
            _cache.pop(next(iter(_cache)), None)
        _cache[key] = (time.monotonic(), value)
    return value


def _cacheable_params(params: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    cache_parts: list[tuple[str, Any]] = []
    for key, value in sorted(params.items(), key=lambda item: item[0]):
        if isinstance(value, list):
            cache_parts.append((key, tuple(value)))
        else:
            cache_parts.append((key, value))
    return tuple(cache_parts)


def invalidate_social_semantic_cache() -> None:
    with _cache_lock:
        _cache.clear()


def _parse_date(value: str | None, *, end_of_day: bool) -> str:
    if not value:
        now = datetime.now(timezone.utc)
        if end_of_day:
            return now.isoformat()
        return (now - timedelta(days=90)).isoformat()
    text = str(value).strip()
    try:
        if len(text) == 10:
            parsed = datetime.combine(
                datetime.fromisoformat(text).date(),
                datetime_time.max if end_of_day else datetime_time.min,
                tzinfo=timezone.utc,
            )
        else:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        now = datetime.now(timezone.utc)
        return (now if end_of_day else now - timedelta(days=90)).isoformat()
    return parsed.astimezone(timezone.utc).isoformat()


def _range_bounds(from_date: str | None, to_date: str | None) -> tuple[str, str, str, str]:
    start = datetime.fromisoformat(_parse_date(from_date, end_of_day=False))
    end = datetime.fromisoformat(_parse_date(to_date, end_of_day=True))
    if end <= start:
        end = start + timedelta(days=1)
    duration = end - start
    previous_start = start - duration
    previous_end = start
    return start.isoformat(), end.isoformat(), previous_start.isoformat(), previous_end.isoformat()


def _sentiment_bucket(value: Any) -> str:
    label = str(value or "").strip().lower()
    if label in {"positive", "negative", "neutral", "mixed", "urgent", "sarcastic"}:
        return label
    return "neutral"


def _platform_filter(platform: str | None) -> str | None:
    clean = str(platform or "").strip().lower()
    return None if clean in {"", "all"} else clean


def _query_rows(cypher: str, params: dict[str, Any], *, op_name: str) -> list[dict[str, Any]]:
    started = time.perf_counter()
    with _get_driver().session(database=config.SOCIAL_NEO4J_DATABASE) as session:
        rows = [dict(record) for record in session.run(cypher, params)]
    elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
    logger.debug("Social Neo4j read complete | op={} rows={} elapsed_ms={}", op_name, len(rows), elapsed_ms)
    return rows


def get_topic_aggregates(
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    entity_id: str | None = None,
    platform: str | None = None,
    limit: int = 25,
) -> dict[str, Any]:
    start, end, previous_start, previous_end = _range_bounds(from_date, to_date)
    topic_limit = max(1, min(int(limit or 25), 100))
    params = {
        "start": start,
        "end": end,
        "previous_start": previous_start,
        "previous_end": previous_end,
        "entity_id": str(entity_id).strip() if entity_id else None,
        "platform": _platform_filter(platform),
        "source_kinds": list(_ORGANIC_SOURCE_KINDS),
        "noise": list(_NOISE_TOPIC_NAMES),
        "limit": topic_limit,
    }
    cache_key = ("topic_aggregates_v1", _cacheable_params(params))
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    cypher = """
    MATCH (a:SocialActivity)-[:COVERS]->(t:Topic)
    WHERE a.published_at >= datetime($start)
      AND a.published_at < datetime($end)
      AND coalesce(a.source_kind, '') IN $source_kinds
      AND ($platform IS NULL OR a.platform = $platform)
      AND coalesce(t.proposed, false) = false
      AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
      AND (
        $entity_id IS NULL
        OR EXISTS { MATCH (:TrackedEntity {id: $entity_id})-[:HAS_ACTIVITY]->(a) }
      )
    OPTIONAL MATCH (a)-[:HAS_SENTIMENT]->(s:Sentiment)
    WITH t, a, head(collect(DISTINCT toLower(coalesce(s.name, 'neutral')))) AS sentiment
    WITH
      t,
      count(DISTINCT a) AS currentMentions,
      avg(coalesce(a.sentiment_score, 0.0)) AS avgSentimentScore,
      collect(DISTINCT a.uid)[..30] AS activityUids,
      collect(DISTINCT a.platform)[..4] AS topPlatforms,
      sum(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive,
      sum(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative,
      sum(CASE WHEN sentiment = 'mixed' THEN 1 ELSE 0 END) AS mixed,
      sum(CASE WHEN sentiment = 'urgent' THEN 1 ELSE 0 END) AS urgent,
      sum(CASE WHEN sentiment = 'sarcastic' THEN 1 ELSE 0 END) AS sarcastic,
      sum(CASE WHEN sentiment = 'neutral' OR sentiment = '' THEN 1 ELSE 0 END) AS neutral
    ORDER BY currentMentions DESC, t.name ASC
    LIMIT $limit
    CALL (t) {
      MATCH (pa:SocialActivity)-[:COVERS]->(t)
      WHERE pa.published_at >= datetime($previous_start)
        AND pa.published_at < datetime($previous_end)
        AND coalesce(pa.source_kind, '') IN $source_kinds
        AND ($platform IS NULL OR pa.platform = $platform)
        AND (
          $entity_id IS NULL
          OR EXISTS { MATCH (:TrackedEntity {id: $entity_id})-[:HAS_ACTIVITY]->(pa) }
        )
      RETURN count(DISTINCT pa) AS previousMentions
    }
    CALL (t) {
      MATCH (entity:TrackedEntity)-[:HAS_ACTIVITY]->(ea:SocialActivity)-[:COVERS]->(t)
      WHERE ea.published_at >= datetime($start)
        AND ea.published_at < datetime($end)
        AND coalesce(ea.source_kind, '') IN $source_kinds
        AND ($platform IS NULL OR ea.platform = $platform)
        AND ($entity_id IS NULL OR entity.id = $entity_id)
      WITH entity.name AS name, count(DISTINCT ea) AS hits
      ORDER BY hits DESC, name ASC
      RETURN collect(name)[..5] AS topEntities
    }
    RETURN
      t.name AS topic,
      currentMentions,
      previousMentions,
      avgSentimentScore,
      positive,
      neutral,
      negative,
      mixed,
      urgent,
      sarcastic,
      topEntities,
      topPlatforms,
      activityUids
    ORDER BY currentMentions DESC, topic ASC
    """
    rows = _query_rows(cypher, params, op_name="social_topic_aggregates")
    items: list[dict[str, Any]] = []
    for row in rows:
        topic = str(row.get("topic") or "").strip()
        if not topic:
            continue
        current = int(row.get("currentMentions") or 0)
        previous = int(row.get("previousMentions") or 0)
        growth_support = current + previous
        growth_pct = None if growth_support < 3 else round(100.0 * (current - previous) / max(1, previous), 1)
        sentiments = {
            "positive": int(row.get("positive") or 0),
            "neutral": int(row.get("neutral") or 0),
            "negative": int(row.get("negative") or 0),
            "mixed": int(row.get("mixed") or 0),
            "urgent": int(row.get("urgent") or 0),
            "sarcastic": int(row.get("sarcastic") or 0),
        }
        dominant = max(sentiments.items(), key=lambda item: item[1])[0] if sentiments else "neutral"
        items.append(
            {
                "topic": topic,
                "count": current,
                "previousCount": previous,
                "deltaCount": current - previous,
                "growthPct": growth_pct if growth_pct is not None else (100.0 if current > 0 and previous == 0 else 0.0),
                "growthReliable": growth_pct is not None,
                "avgSentimentScore": round(float(row.get("avgSentimentScore") or 0.0), 4),
                "dominantSentiment": dominant,
                "sentimentCounts": sentiments,
                "topEntities": [str(value) for value in row.get("topEntities") or [] if value],
                "topPlatforms": [str(value) for value in row.get("topPlatforms") or [] if value],
                "activityUids": [str(value) for value in row.get("activityUids") or [] if value],
            }
        )
    return _cache_set(
        cache_key,
        {
            "items": items,
            "meta": {
                "source": "neo4j",
                "window": {
                    "from": start,
                    "to": end,
                    "previousFrom": previous_start,
                    "previousTo": previous_end,
                },
            },
        },
    )


def get_sentiment_trend(
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    entity_id: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    start, end, _previous_start, _previous_end = _range_bounds(from_date, to_date)
    params = {
        "start": start,
        "end": end,
        "entity_id": str(entity_id).strip() if entity_id else None,
        "platform": _platform_filter(platform),
        "source_kinds": list(_ORGANIC_SOURCE_KINDS),
    }
    cache_key = ("sentiment_trend_v2_day", _cacheable_params(params))
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    cypher = """
    MATCH (a:SocialActivity)-[:HAS_SENTIMENT]->(s:Sentiment)
    WHERE a.published_at >= datetime($start)
      AND a.published_at < datetime($end)
      AND coalesce(a.source_kind, '') IN $source_kinds
      AND ($platform IS NULL OR a.platform = $platform)
      AND (
        $entity_id IS NULL
        OR EXISTS { MATCH (:TrackedEntity {id: $entity_id})-[:HAS_ACTIVITY]->(a) }
      )
    WITH
      toString(date(a.published_at)) AS bucket,
      toLower(coalesce(s.name, 'neutral')) AS sentiment,
      count(DISTINCT a) AS hits
    RETURN bucket, sentiment, hits
    ORDER BY bucket ASC
    """
    rows = _query_rows(cypher, params, op_name="social_sentiment_trend")
    buckets: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "bucket": "",
            "total": 0,
            "positive": 0,
            "neutral": 0,
            "negative": 0,
            "mixed": 0,
            "urgent": 0,
            "sarcastic": 0,
        }
    )
    for row in rows:
        bucket = str(row.get("bucket") or "").strip()
        if not bucket:
            continue
        sentiment = _sentiment_bucket(row.get("sentiment"))
        hits = int(row.get("hits") or 0)
        item = buckets[bucket]
        item["bucket"] = bucket
        item[sentiment] += hits
        item["total"] += hits
    start_day = datetime.fromisoformat(start).date()
    end_day = datetime.fromisoformat(end).date()
    items: list[dict[str, Any]] = []
    total_days = (end_day - start_day).days + 1
    if 0 < total_days <= 366:
        for offset in range(total_days):
            key = (start_day + timedelta(days=offset)).isoformat()
            item = buckets[key]
            item["bucket"] = key
            items.append(item)
    else:
        items = [buckets[key] for key in sorted(buckets.keys())]

    return _cache_set(
        cache_key,
        {
            "items": items,
            "meta": {"source": "neo4j", "bucket": "day", "window": {"from": start, "to": end}},
        },
    )
