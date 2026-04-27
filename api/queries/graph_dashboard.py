"""
graph_dashboard.py — analyst graph projection for the /graph page.

This module intentionally exposes a lightweight conversation map:
channels provide source context, categories anchor the landscape, and topics
remain drill-down detail under categories. Evidence continues to come from the
dedicated topic/channel detail endpoints and graph node details.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import os
import threading
import time
from typing import Any

from api import topic_overviews
from api.queries import comparative
from api.dashboard_dates import DashboardDateContext, build_dashboard_date_context
from api.db import run_query
from api import topic_overviews
from api.queries import comparative
from utils.topic_normalizer import classify_topic

_NOISY_TOPIC_KEYS = {"", "null", "unknown", "none", "n/a", "na"}
_EXCLUDED_CATEGORY_KEYS = {"general"}
_NEGATIVE_SENTIMENT_LABELS = {"negative", "urgent", "sarcastic"}
_FEAR_TAGS = {
    "anxious",
    "frustrated",
    "angry",
    "exhausted",
    "grief",
    "distrustful",
    "confused",
}
_SUPPORT_INTENTS = ["Support / Help", "Information Seeking"]
_NEED_HINTS = [
    "need help",
    "looking for",
    "where can i",
    "where to find",
    "is there anyone",
    "can anyone recommend",
    "can someone recommend",
    "any recommendations",
    "how do i find",
    "how do i",
    "where do i",
    "подскажите",
    "помогите",
    "где найти",
    "ищу",
    "нужен",
    "нужна",
    "нужны",
    "нужно",
    "есть ли",
    "кто может",
    "можете порекомендовать",
]
_SOURCE_DETAIL_LIMITS: dict[str, tuple[int, int]] = {
    "minimal": (1, 8),
    "standard": (2, 12),
    "expanded": (3, 18),
}
GRAPH_CACHE_TTL_SECONDS = max(30, int(os.getenv("GRAPH_CACHE_TTL_SECONDS", "180")))
_graph_cache_lock = threading.Lock()
_graph_cache: dict[str, tuple[float, Any]] = {}


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _to_iso(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_name(value: Any, fallback: str) -> str:
    text = str(value or "").strip()
    return text or fallback


def _cache_key(prefix: str, payload: dict[str, Any]) -> str:
    return f"{prefix}:{json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True)}"


def _get_cached_graph_value(cache_key: str, builder, ttl_seconds: int = GRAPH_CACHE_TTL_SECONDS) -> Any:
    now = time.time()
    stale: Any = None
    with _graph_cache_lock:
        entry = _graph_cache.get(cache_key)
        if entry is not None:
            ts, data = entry
            stale = data
            if (now - ts) < ttl_seconds:
                return data

    try:
        fresh = builder()
        with _graph_cache_lock:
            _graph_cache[cache_key] = (time.time(), fresh)
        return fresh
    except Exception:
        if stale is not None:
            return stale
        raise


def invalidate_graph_cache() -> None:
    with _graph_cache_lock:
        _graph_cache.clear()


def _parse_timeframe_to_days(timeframe: str | None) -> int:
    tf = (timeframe or "").strip().lower()
    mapping = {
        "last 24h": 1,
        "24h": 1,
        "last 7 days": 7,
        "7d": 7,
        "last 15 days": 15,
        "15d": 15,
        "last month": 30,
        "30d": 30,
        "last 3 months": 90,
        "90d": 90,
    }
    return mapping.get(tf, 7)


def _default_context_for_days(days: int) -> DashboardDateContext:
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=max(0, days - 1))
    return build_dashboard_date_context(start.isoformat(), today.isoformat())


def _resolve_context(filters: dict | None = None, *, timeframe: str | None = None) -> DashboardDateContext:
    filters = filters or {}
    from_date = str(filters.get("from_date") or filters.get("from") or "").strip()
    to_date = str(filters.get("to_date") or filters.get("to") or "").strip()
    if from_date and to_date:
        return build_dashboard_date_context(from_date, to_date)
    return _default_context_for_days(_parse_timeframe_to_days(filters.get("timeframe") or timeframe))


def _normalize_channels(values: list[str] | None) -> list[str]:
    if not values:
        return []
    seen: set[str] = set()
    output: list[str] = []
    for raw in values:
        text = str(raw or "").strip()
        if not text:
            continue
        key = text.lower().lstrip("@")
        if key in seen:
            continue
        seen.add(key)
        output.append(key)
    return output


def _normalize_sentiments(values: list[str] | None) -> list[str]:
    if not values:
        return []
    seen: set[str] = set()
    output: list[str] = []
    for raw in values:
        label = str(raw or "").strip().title()
        if label not in {"Positive", "Neutral", "Negative", "Urgent"}:
            continue
        if label in seen:
            continue
        seen.add(label)
        output.append(label)
    return output


def _normalize_topics(values: list[str] | None) -> list[str]:
    if not values:
        return []
    seen: set[str] = set()
    output: list[str] = []
    for raw in values:
        text = str(raw or "").strip()
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        output.append(text)
    return output


def _normalize_category(value: Any) -> str:
    return str(value or "").strip()


def _normalize_signal_focus(value: Any) -> str:
    normalized = str(value or "all").strip().lower()
    if normalized in {"asks", "needs", "fear"}:
        return normalized
    return "all"


def _normalize_source_detail(value: Any) -> str:
    normalized = str(value or "standard").strip().lower()
    if normalized in _SOURCE_DETAIL_LIMITS:
        return normalized
    return "standard"


def _normalize_ranking_mode(value: Any) -> str:
    normalized = str(value or "volume").strip().lower()
    if normalized in {"volume", "momentum", "spread"}:
        return normalized
    return "volume"


def _source_detail_limits(source_detail: str) -> tuple[int, int]:
    return _SOURCE_DETAIL_LIMITS.get(source_detail, _SOURCE_DETAIL_LIMITS["standard"])


def _channel_predicate(alias: str) -> str:
    return (
        f"$channel_count = 0 OR "
        f"toLower(coalesce({alias}.username, '')) IN $channels OR "
        f"toLower(coalesce({alias}.title, '')) IN $channels OR "
        f"coalesce({alias}.uuid, '') IN $channels OR "
        f"('channel:' + coalesce({alias}.uuid, '')) IN $channels"
    )


def _resolve_filters(raw_filters: dict | None = None) -> dict[str, Any]:
    filters = raw_filters or {}
    max_nodes = max(12, min(_to_int(filters.get("max_nodes"), 20), 60))
    min_mentions = max(1, min(_to_int(filters.get("minMentions"), 2), 50))
    return {
        "channels": _normalize_channels(filters.get("channels") or filters.get("brandSource")),
        "sentiments": _normalize_sentiments(filters.get("sentiment") or filters.get("sentiments")),
        "topics": _normalize_topics(filters.get("topics")),
        "category": _normalize_category(filters.get("category")),
        "signal_focus": _normalize_signal_focus(filters.get("signalFocus")),
        "source_detail": _normalize_source_detail(filters.get("sourceDetail")),
        "ranking_mode": _normalize_ranking_mode(filters.get("rankingMode")),
        "min_mentions": min_mentions,
        "max_nodes": max_nodes,
        "raw_limit": max(max_nodes * 3, 84),
    }


def _row_matches_sentiments(row: dict[str, Any], sentiments: list[str]) -> bool:
    if not sentiments:
        return True
    dominant = _dominant_sentiment(
        {
            "sentimentPositive": row.get("sentimentPositive", row.get("positiveScore")),
            "sentimentNeutral": row.get("sentimentNeutral", row.get("neutralScore")),
            "sentimentNegative": row.get("sentimentNegative", row.get("negativeScore")),
            "urgentSignals": row.get("urgentSignals", row.get("urgentScore")),
        }
    )
    return dominant in sentiments


def _row_matches_signal_focus(row: dict[str, Any], signal_focus: str) -> bool:
    if signal_focus == "asks":
        return _to_int(row.get("askSignalCount")) > 0
    if signal_focus == "needs":
        return _to_int(row.get("needSignalCount")) > 0
    if signal_focus == "fear":
        return _to_int(row.get("fearSignalCount")) > 0
    return True


def _dominant_sentiment(row: dict[str, Any]) -> str:
    positive = _to_int(row.get("sentimentPositive"))
    neutral = _to_int(row.get("sentimentNeutral"))
    negative = _to_int(row.get("sentimentNegative"))
    urgent_signals = _to_int(row.get("urgentSignals"))

    if urgent_signals > 0 and urgent_signals >= max(positive, neutral, max(1, negative // 2)):
        return "Urgent"
    if positive >= neutral and positive >= negative:
        return "Positive"
    if negative >= neutral:
        return "Negative"
    return "Neutral"


def _topic_sort_key(row: dict[str, Any], ranking_mode: str) -> tuple[Any, ...]:
    mention_count = _to_int(row.get("mentionCount"))
    growth = _to_float(row.get("trendPct"))
    distinct_channels = _to_int(row.get("distinctChannels"))
    evidence_count = _to_int(row.get("evidenceCount"))
    if ranking_mode == "momentum":
        return (-growth, -mention_count, -distinct_channels, -evidence_count, str(row.get("name") or ""))
    if ranking_mode == "spread":
        return (-distinct_channels, -mention_count, -growth, -evidence_count, str(row.get("name") or ""))
    return (-mention_count, -growth, -distinct_channels, -evidence_count, str(row.get("name") or ""))


def _topic_value(row: dict[str, Any]) -> float:
    mention_count = _to_int(row.get("mentionCount"))
    growth = max(0.0, _to_float(row.get("trendPct")))
    distinct_channels = _to_int(row.get("distinctChannels"))
    return round(mention_count + (growth * 0.15) + (distinct_channels * 1.5), 2)


def _topic_label_id(name: str) -> str:
    return f"topic:{name}"


def _category_label_id(name: str) -> str:
    return f"category:{name}"


def _merge_top_channels(*channel_sets: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for channels in channel_sets:
        for channel in channels or []:
            channel_id = str(channel.get("id") or "").strip()
            channel_name = _safe_name(channel.get("name"), channel_id.replace("channel:", ""))
            mentions = max(0, _to_int(channel.get("mentions"), 0))
            if not channel_id and not channel_name:
                continue
            key = channel_id or f"channel:{channel_name}"
            entry = merged.setdefault(
                key,
                {
                    "id": key,
                    "name": channel_name,
                    "mentions": 0,
                },
            )
            entry["mentions"] = _to_int(entry.get("mentions")) + mentions
            if channel_name and not str(entry.get("name") or "").strip():
                entry["name"] = channel_name

    return sorted(
        merged.values(),
        key=lambda item: (-_to_int(item.get("mentions")), str(item.get("name") or "")),
    )[:12]


def _canonicalize_topic_rows(topic_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    canonical_rows: dict[tuple[str, str], dict[str, Any]] = {}
    existing_topic_names = {
        _safe_name(row.get("name"), "").strip().lower()
        for row in topic_rows
        if _safe_name(row.get("name"), "").strip()
    }

    for row in topic_rows:
        raw_name = _safe_name(row.get("name"), "")
        raw_category = _safe_name(row.get("category"), "General")
        classification = classify_topic(raw_name)
        canonical_candidate = _safe_name((classification or {}).get("taxonomy_topic") or raw_name, raw_name)
        canonical_exists = canonical_candidate.strip().lower() in existing_topic_names
        use_canonical_name = canonical_exists or canonical_candidate.strip().lower() == raw_name.strip().lower()
        canonical_name = canonical_candidate if use_canonical_name else raw_name
        canonical_category = _safe_name(
            ((classification or {}).get("closest_category") if use_canonical_name else raw_category) or raw_category,
            raw_category,
        )
        if not canonical_name:
            continue

        key = (canonical_name.lower(), canonical_category.lower())
        if key not in canonical_rows:
            canonical_rows[key] = {
                **row,
                "id": _topic_label_id(canonical_name),
                "name": canonical_name,
                "category": canonical_category,
                "topChannels": list(row.get("topChannels") or []),
            }
            continue

        entry = canonical_rows[key]
        entry["mentionCount"] = _to_int(entry.get("mentionCount")) + _to_int(row.get("mentionCount"))
        entry["postCount"] = _to_int(entry.get("postCount")) + _to_int(row.get("postCount"))
        entry["commentCount"] = _to_int(entry.get("commentCount")) + _to_int(row.get("commentCount"))
        entry["evidenceCount"] = _to_int(entry.get("evidenceCount")) + _to_int(row.get("evidenceCount"))
        entry["distinctUsers"] = max(_to_int(entry.get("distinctUsers")), _to_int(row.get("distinctUsers")))
        entry["askSignalCount"] = _to_int(entry.get("askSignalCount")) + _to_int(row.get("askSignalCount"))
        entry["needSignalCount"] = _to_int(entry.get("needSignalCount")) + _to_int(row.get("needSignalCount"))
        entry["fearSignalCount"] = _to_int(entry.get("fearSignalCount")) + _to_int(row.get("fearSignalCount"))
        entry["urgentSignals"] = _to_int(entry.get("urgentSignals")) + _to_int(row.get("urgentSignals"))
        entry["_positive_raw"] = _to_int(entry.get("_positive_raw")) + _to_int(row.get("_positive_raw"))
        entry["_neutral_raw"] = _to_int(entry.get("_neutral_raw")) + _to_int(row.get("_neutral_raw"))
        entry["_negative_raw"] = _to_int(entry.get("_negative_raw")) + _to_int(row.get("_negative_raw"))
        entry["lastSeen"] = max(
            str(entry.get("lastSeen") or ""),
            str(row.get("lastSeen") or ""),
        ) or entry.get("lastSeen") or row.get("lastSeen")
        entry["sampleEvidenceId"] = entry.get("sampleEvidenceId") or row.get("sampleEvidenceId")
        entry["topChannels"] = _merge_top_channels(entry.get("topChannels") or [], row.get("topChannels") or [])

    output: list[dict[str, Any]] = []
    for entry in canonical_rows.values():
        positive_raw = _to_int(entry.get("_positive_raw"))
        neutral_raw = _to_int(entry.get("_neutral_raw"))
        negative_raw = _to_int(entry.get("_negative_raw"))
        sentiment_total = positive_raw + neutral_raw + negative_raw
        distinct_channels = len({str(channel.get("name") or "").strip().lower() for channel in entry.get("topChannels") or [] if str(channel.get("name") or "").strip()})
        mention_count = _to_int(entry.get("mentionCount"))

        entry["distinctChannels"] = max(distinct_channels, _to_int(entry.get("distinctChannels")))
        entry["sentimentPositive"] = _to_int(round(100.0 * positive_raw / sentiment_total)) if sentiment_total > 0 else 0
        entry["sentimentNeutral"] = _to_int(round(100.0 * neutral_raw / sentiment_total)) if sentiment_total > 0 else 0
        entry["sentimentNegative"] = _to_int(round(100.0 * negative_raw / sentiment_total)) if sentiment_total > 0 else 0
        entry["dominantSentiment"] = _dominant_sentiment(entry)
        entry["val"] = _topic_value(
            {
                "mentionCount": mention_count,
                "trendPct": entry.get("trendPct"),
                "distinctChannels": entry.get("distinctChannels"),
            }
        )
        output.append(entry)

    output.sort(key=lambda row: _topic_sort_key(row, "volume"))
    return output


def _load_topic_rows(ctx: DashboardDateContext, filters: dict[str, Any]) -> list[dict[str, Any]]:
    query = f"""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
          AND NOT toLower(trim(coalesce(cat.name, 'General'))) IN $excluded_categories
          AND ($topic_count = 0 OR t.name IN $topics)

        CALL {{
            WITH t
            CALL {{
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                WITH p, ch
                WHERE {_channel_predicate('ch')}
                OPTIONAL MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
                OPTIONAL MATCH (p)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
                WITH p, ch,
                     max(toLower(coalesce(s.label, ''))) AS sentimentLabel,
                     collect(DISTINCT toLower(coalesce(tag.name, ''))) AS tagNames,
                     toLower(trim(coalesce(p.text, ''))) AS textLower
                RETURN {{
                    id: coalesce(p.uuid, 'post:' + elementId(p)),
                    contentType: 'post',
                    occurredAt: p.posted_at,
                    timestamp: toString(p.posted_at),
                    channel: coalesce(ch.title, ch.username, 'unknown'),
                    channelUuid: coalesce(ch.uuid, ''),
                    actorKey: coalesce(ch.username, ch.title, 'unknown'),
                    sentimentLabel: coalesce(sentimentLabel, ''),
                    hasText: CASE WHEN trim(coalesce(p.text, '')) <> '' THEN 1 ELSE 0 END,
                    askLike: CASE
                        WHEN p.text IS NOT NULL AND trim(p.text) <> '' AND p.text CONTAINS '?' THEN 1
                        ELSE 0
                    END,
                    needLike: CASE
                        WHEN (p.text IS NOT NULL AND trim(p.text) <> '' AND p.text CONTAINS '?')
                          OR any(h IN $need_hints WHERE textLower CONTAINS h)
                        THEN 1
                        ELSE 0
                    END,
                    fearLike: CASE
                        WHEN coalesce(sentimentLabel, '') IN $fear_labels
                          OR any(tag IN tagNames WHERE tag IN $fear_tags)
                        THEN 1
                        ELSE 0
                    END
                }} AS row

                UNION ALL

                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                OPTIONAL MATCH (u:User)-[:WROTE]->(c)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                WITH c, u, ch
                WHERE {_channel_predicate('ch')}
                OPTIONAL MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
                OPTIONAL MATCH (c)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
                OPTIONAL MATCH (u)-[:EXHIBITS]->(intent:Intent)
                WITH c, u, ch,
                     max(toLower(coalesce(s.label, ''))) AS sentimentLabel,
                     collect(DISTINCT toLower(coalesce(tag.name, ''))) AS tagNames,
                     max(CASE WHEN intent.name IN $support_intents THEN 1 ELSE 0 END) AS supportIntent,
                     toLower(trim(coalesce(c.text, ''))) AS textLower
                RETURN {{
                    id: coalesce(c.uuid, 'comment:' + elementId(c)),
                    contentType: 'comment',
                    occurredAt: c.posted_at,
                    timestamp: toString(c.posted_at),
                    channel: coalesce(ch.title, ch.username, 'unknown'),
                    channelUuid: coalesce(ch.uuid, ''),
                    actorKey: coalesce(toString(u.telegram_user_id), coalesce(ch.username, ch.title, 'anonymous')),
                    sentimentLabel: coalesce(sentimentLabel, ''),
                    hasText: CASE WHEN trim(coalesce(c.text, '')) <> '' THEN 1 ELSE 0 END,
                    askLike: CASE
                        WHEN c.text IS NOT NULL AND trim(c.text) <> '' AND c.text CONTAINS '?' THEN 1
                        ELSE 0
                    END,
                    needLike: CASE
                        WHEN (c.text IS NOT NULL AND trim(c.text) <> '' AND c.text CONTAINS '?')
                          OR supportIntent = 1
                          OR any(h IN $need_hints WHERE textLower CONTAINS h)
                        THEN 1
                        ELSE 0
                    END,
                    fearLike: CASE
                        WHEN coalesce(sentimentLabel, '') IN $fear_labels
                          OR any(tag IN tagNames WHERE tag IN $fear_tags)
                        THEN 1
                        ELSE 0
                    END
                }} AS row
            }}
            WITH row
            ORDER BY row.occurredAt DESC, row.id DESC
            WITH collect(row) AS rows
            WITH rows,
                 size(rows) AS mentionCount,
                 size([row IN rows WHERE row.hasText = 1 | 1]) AS evidenceCount,
                 size([row IN rows WHERE row.contentType = 'post' | 1]) AS postCount,
                 size([row IN rows WHERE row.contentType = 'comment' | 1]) AS commentCount,
                 size(reduce(acc = [], row IN rows |
                    CASE
                        WHEN row.actorKey <> '' AND NOT (row.actorKey IN acc) THEN acc + row.actorKey
                        ELSE acc
                    END
                 )) AS distinctUsers,
                 size(reduce(acc = [], row IN rows |
                    CASE
                        WHEN row.channel <> '' AND row.channel <> 'unknown' AND NOT (row.channel IN acc) THEN acc + row.channel
                        ELSE acc
                    END
                 )) AS distinctChannels,
                 size([row IN rows WHERE row.sentimentLabel = 'positive' | 1]) AS positiveScore,
                 size([row IN rows WHERE row.sentimentLabel = 'neutral' | 1]) AS neutralScore,
                 size([row IN rows WHERE row.sentimentLabel IN $fear_labels | 1]) AS negativeScore,
                 size([row IN rows WHERE row.sentimentLabel = 'urgent' | 1]) AS urgentScore,
                 size([row IN rows WHERE row.askLike = 1 | 1]) AS askSignalCount,
                 size([row IN rows WHERE row.needLike = 1 | 1]) AS needSignalCount,
                 size([row IN rows WHERE row.fearLike = 1 | 1]) AS fearSignalCount,
                 head([row IN rows WHERE row.hasText = 1 | row.id]) AS sampleEvidenceId,
                 head([row IN rows | row.timestamp]) AS latestAt
            CALL {{
                WITH rows
                UNWIND rows AS row
                WITH row.channel AS channel, row.channelUuid AS channelUuid, count(*) AS mentions
                WHERE channel <> '' AND channel <> 'unknown'
                ORDER BY mentions DESC, channel ASC
                RETURN collect({{
                    id: CASE
                        WHEN channelUuid <> '' THEN 'channel:' + channelUuid
                        ELSE 'channel:' + channel
                    END,
                    name: channel,
                    mentions: mentions
                }})[..12] AS topChannels
            }}
            RETURN mentionCount,
                   evidenceCount,
                   postCount,
                   commentCount,
                   distinctUsers,
                   distinctChannels,
                   positiveScore,
                   neutralScore,
                   negativeScore,
                   urgentScore,
                   askSignalCount,
                   needSignalCount,
                   fearSignalCount,
                   sampleEvidenceId,
                   latestAt,
                   topChannels
        }}

        CALL {{
            WITH t
            CALL {{
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($previous_start)
                  AND p.posted_at < datetime($previous_end)
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                WITH p, ch
                WHERE {_channel_predicate('ch')}
                RETURN 1 AS hit

                UNION ALL

                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($previous_start)
                  AND c.posted_at < datetime($previous_end)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                WITH c, ch
                WHERE {_channel_predicate('ch')}
                RETURN 1 AS hit
            }}
            RETURN count(hit) AS prevMentions
        }}

        WITH t, cat, mentionCount, evidenceCount, postCount, commentCount, distinctUsers, distinctChannels,
             positiveScore, neutralScore, negativeScore, urgentScore, askSignalCount, needSignalCount,
             fearSignalCount, sampleEvidenceId, latestAt, topChannels, prevMentions
        WHERE mentionCount >= $min_mentions
          AND evidenceCount > 0
        RETURN t.name AS name,
               cat.name AS category,
               mentionCount,
               postCount,
               commentCount,
               evidenceCount,
               distinctUsers,
               distinctChannels,
               positiveScore,
               neutralScore,
               negativeScore,
               urgentScore,
               askSignalCount,
               needSignalCount,
               fearSignalCount,
               sampleEvidenceId,
               latestAt,
               topChannels,
               prevMentions,
               CASE
                   WHEN prevMentions > 0 THEN round(100.0 * (mentionCount - prevMentions) / prevMentions, 1)
                   WHEN mentionCount > 0 THEN 100.0
                   ELSE 0.0
               END AS trendPct
        ORDER BY mentionCount DESC, distinctChannels DESC, evidenceCount DESC, t.name ASC
        LIMIT $raw_limit
    """
    rows = run_query(
        query,
        {
            "start": ctx.start_at.isoformat(),
            "end": ctx.end_at.isoformat(),
            "previous_start": ctx.previous_start_at.isoformat(),
            "previous_end": ctx.previous_end_at.isoformat(),
            "channels": filters["channels"],
            "channel_count": len(filters["channels"]),
            "topics": filters["topics"],
            "topic_count": len(filters["topics"]),
            "min_mentions": filters["min_mentions"],
            "raw_limit": filters["raw_limit"],
            "noise": sorted(_NOISY_TOPIC_KEYS),
            "excluded_categories": sorted(_EXCLUDED_CATEGORY_KEYS),
            "fear_labels": sorted(_NEGATIVE_SENTIMENT_LABELS),
            "fear_tags": sorted(_FEAR_TAGS),
            "need_hints": _NEED_HINTS,
            "support_intents": _SUPPORT_INTENTS,
        },
    )

    output: list[dict[str, Any]] = []
    for row in rows:
        topic_name = _safe_name(row.get("name"), "")
        category = _safe_name(row.get("category"), "General")
        if not topic_name or topic_name.lower() in _NOISY_TOPIC_KEYS:
            continue
        if category.strip().lower() in _EXCLUDED_CATEGORY_KEYS:
            continue
        total_sentiment = (
            _to_int(row.get("positiveScore"))
            + _to_int(row.get("neutralScore"))
            + _to_int(row.get("negativeScore"))
        )
        positive_pct = _to_int(round(100.0 * _to_int(row.get("positiveScore")) / total_sentiment)) if total_sentiment > 0 else 0
        neutral_pct = _to_int(round(100.0 * _to_int(row.get("neutralScore")) / total_sentiment)) if total_sentiment > 0 else 0
        negative_pct = _to_int(round(100.0 * _to_int(row.get("negativeScore")) / total_sentiment)) if total_sentiment > 0 else 0
        topic_row = {
            "id": _topic_label_id(topic_name),
            "name": topic_name,
            "type": "topic",
            "category": category,
            "mentionCount": _to_int(row.get("mentionCount")),
            "postCount": _to_int(row.get("postCount")),
            "commentCount": _to_int(row.get("commentCount")),
            "evidenceCount": _to_int(row.get("evidenceCount")),
            "distinctUsers": _to_int(row.get("distinctUsers")),
            "distinctChannels": _to_int(row.get("distinctChannels")),
            "trendPct": _to_float(row.get("trendPct")),
            "sentimentPositive": positive_pct,
            "sentimentNeutral": neutral_pct,
            "sentimentNegative": negative_pct,
            "urgentSignals": _to_int(row.get("urgentScore")),
            "askSignalCount": _to_int(row.get("askSignalCount")),
            "needSignalCount": _to_int(row.get("needSignalCount")),
            "fearSignalCount": _to_int(row.get("fearSignalCount")),
            "sampleEvidenceId": _to_iso(row.get("sampleEvidenceId")),
            "lastSeen": _to_iso(row.get("latestAt")),
            "topChannels": list(row.get("topChannels") or []),
            "_positive_raw": _to_int(row.get("positiveScore")),
            "_neutral_raw": _to_int(row.get("neutralScore")),
            "_negative_raw": _to_int(row.get("negativeScore")),
        }
        topic_row["dominantSentiment"] = _dominant_sentiment(topic_row)
        topic_row["val"] = _topic_value(topic_row)
        output.append(topic_row)
    return _canonicalize_topic_rows(output)


def _filter_topic_rows(topic_rows: list[dict[str, Any]], filters: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str], int]:
    available_categories = sorted({str(row.get("category") or "") for row in topic_rows if str(row.get("category") or "").strip()})
    filtered: list[dict[str, Any]] = []
    selected_category = filters["category"]
    for row in topic_rows:
        if selected_category and str(row.get("category") or "") != selected_category:
            continue
        if not _row_matches_sentiments(row, filters["sentiments"]):
            continue
        if not _row_matches_signal_focus(row, filters["signal_focus"]):
            continue
        filtered.append(row)
    filtered.sort(key=lambda row: _topic_sort_key(row, filters["ranking_mode"]))
    total_eligible = len(filtered)
    return filtered[: filters["max_nodes"]], available_categories, total_eligible


def _build_category_nodes(visible_topics: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    categories: dict[str, dict[str, Any]] = {}
    category_topic_links: dict[tuple[str, str], dict[str, Any]] = {}
    for topic in visible_topics:
        category_name = _safe_name(topic.get("category"), "General")
        category_node = categories.setdefault(
            category_name,
            {
                "id": _category_label_id(category_name),
                "name": category_name,
                "type": "category",
                "val": 0.0,
                "mentionCount": 0,
                "topicCount": 0,
                "_positive_raw": 0,
                "_neutral_raw": 0,
                "_negative_raw": 0,
                "_urgent_raw": 0,
                "askSignalCount": 0,
                "needSignalCount": 0,
                "fearSignalCount": 0,
                "trendWeightedSum": 0.0,
                "distinctChannels": 0,
                "_topic_ids": set(),
            },
        )
        category_node["val"] = round(_to_float(category_node.get("val")) + _to_float(topic.get("val")), 2)
        category_node["mentionCount"] = _to_int(category_node.get("mentionCount")) + _to_int(topic.get("mentionCount"))
        category_node["_positive_raw"] += _to_int(topic.get("_positive_raw"))
        category_node["_neutral_raw"] += _to_int(topic.get("_neutral_raw"))
        category_node["_negative_raw"] += _to_int(topic.get("_negative_raw"))
        category_node["_urgent_raw"] += _to_int(topic.get("urgentSignals"))
        category_node["askSignalCount"] += _to_int(topic.get("askSignalCount"))
        category_node["needSignalCount"] += _to_int(topic.get("needSignalCount"))
        category_node["fearSignalCount"] += _to_int(topic.get("fearSignalCount"))
        category_node["trendWeightedSum"] += _to_float(topic.get("trendPct")) * max(1, _to_int(topic.get("mentionCount")))
        category_node["distinctChannels"] = max(
            _to_int(category_node.get("distinctChannels")),
            _to_int(topic.get("distinctChannels")),
        )
        category_node["_topic_ids"].add(topic["id"])
        category_topic_links[(category_node["id"], topic["id"])] = {
            "source": category_node["id"],
            "target": topic["id"],
            "value": max(1, _to_int(topic.get("mentionCount"))),
            "type": "category-topic",
        }
    for category_node in categories.values():
        topic_count = len(category_node.pop("_topic_ids"))
        mention_count = max(1, _to_int(category_node.get("mentionCount")))
        category_node["topicCount"] = topic_count
        category_node["trendPct"] = round(_to_float(category_node.pop("trendWeightedSum", 0.0)) / mention_count, 1)
        positive_raw = _to_int(category_node.pop("_positive_raw", 0))
        neutral_raw = _to_int(category_node.pop("_neutral_raw", 0))
        negative_raw = _to_int(category_node.pop("_negative_raw", 0))
        urgent_raw = _to_int(category_node.pop("_urgent_raw", 0))
        sentiment_total = positive_raw + neutral_raw + negative_raw
        category_node["sentimentPositive"] = _to_int(round(100.0 * positive_raw / sentiment_total)) if sentiment_total > 0 else 0
        category_node["sentimentNeutral"] = _to_int(round(100.0 * neutral_raw / sentiment_total)) if sentiment_total > 0 else 0
        category_node["sentimentNegative"] = _to_int(round(100.0 * negative_raw / sentiment_total)) if sentiment_total > 0 else 0
        category_node["urgentSignals"] = urgent_raw
        category_node["dominantSentiment"] = _dominant_sentiment(category_node)
        category_node["val"] = round(max(12.0, _to_float(category_node.get("val")) * 0.72 + topic_count * 5), 2)
    return categories, category_topic_links


def _build_channel_nodes(
    visible_topics: list[dict[str, Any]],
    source_detail: str,
) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    per_category_limit, global_limit = _source_detail_limits(source_detail)
    channel_scores: dict[str, dict[str, Any]] = {}
    category_channel_scores: dict[tuple[str, str], dict[str, Any]] = {}

    for topic in visible_topics:
        category_name = _safe_name(topic.get("category"), "General")
        for idx, channel in enumerate(topic.get("topChannels") or []):
            if idx >= max(per_category_limit * 2, 3):
                break
            channel_id = str(channel.get("id") or "").strip()
            channel_name = _safe_name(channel.get("name"), channel_id.replace("channel:", ""))
            mentions = max(1, _to_int(channel.get("mentions"), 1))
            entry = channel_scores.setdefault(
                channel_id,
                {"id": channel_id, "name": channel_name, "mentions": 0, "topicCount": 0, "_categories": set()},
            )
            entry["mentions"] += mentions
            entry["topicCount"] += 1
            entry["_categories"].add(category_name)
            pair = category_channel_scores.setdefault(
                (channel_id, category_name),
                {"id": channel_id, "name": channel_name, "category": category_name, "mentions": 0},
            )
            pair["mentions"] += mentions
    allowed_channel_ids = {
        row["id"]
        for row in sorted(
            channel_scores.values(),
            key=lambda item: (
                -_to_int(item.get("mentions")),
                -len(item.get("_categories") or set()),
                -_to_int(item.get("topicCount")),
                str(item.get("name") or ""),
            ),
        )[:global_limit]
    }

    channel_nodes: dict[str, dict[str, Any]] = {}
    links: dict[tuple[str, str], dict[str, Any]] = {}
    category_buckets: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for pair in category_channel_scores.values():
        if pair["id"] in allowed_channel_ids:
            category_buckets[str(pair.get("category") or "General")].append(pair)

    for category_name, rows in category_buckets.items():
        for pair in sorted(rows, key=lambda item: (-_to_int(item.get("mentions")), str(item.get("name") or "")))[:per_category_limit]:
            channel_id = str(pair.get("id") or "").strip()
            channel_name = _safe_name(pair.get("name"), channel_id.replace("channel:", ""))
            mentions = max(1, _to_int(pair.get("mentions"), 1))
            node = channel_nodes.setdefault(
                channel_id,
                {
                    "id": channel_id,
                    "name": channel_name,
                    "type": "channel",
                    "val": 0.0,
                    "mentionCount": 0,
                    "topicCount": 0,
                    "categoryCount": 0,
                    "_categories": set(),
                },
            )
            node["val"] = round(_to_float(node.get("val")) + mentions * 2.2, 2)
            node["mentionCount"] = _to_int(node.get("mentionCount")) + mentions
            node["topicCount"] = _to_int(node.get("topicCount")) + 1
            node["_categories"].add(category_name)
            links[(channel_id, _category_label_id(category_name))] = {
                "source": channel_id,
                "target": _category_label_id(category_name),
                "value": mentions,
                "type": "channel-category",
            }

    for node in channel_nodes.values():
        categories = node.pop("_categories", set())
        node["categoryCount"] = len(categories)

    return channel_nodes, links


def _load_evidence_for_topic_names(
    topic_names: list[str],
    ctx: DashboardDateContext,
    channels: list[str] | None = None,
    sentiments: list[str] | None = None,
    *,
    limit: int = 6,
    questions_only: bool = False,
) -> list[dict[str, Any]]:
    cleaned_topics = [str(name or "").strip() for name in topic_names if str(name or "").strip()]
    if not cleaned_topics:
        return []
    normalized_channels = _normalize_channels(channels)
    normalized_sentiments = _normalize_sentiments(sentiments)

    rows = run_query(
        f"""
        UNWIND $topics AS topic_name
        MATCH (t:Topic {{name: topic_name}})
        CALL {{
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            OPTIONAL MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WITH p, ch, t, max(toLower(coalesce(s.label, ''))) AS sentimentLabel
            WHERE {_channel_predicate('ch')}
              AND ($sentiment_count = 0 OR
                   CASE
                       WHEN sentimentLabel = 'positive' THEN 'Positive'
                       WHEN sentimentLabel = 'neutral' THEN 'Neutral'
                       WHEN sentimentLabel IN $fear_labels THEN 'Negative'
                       ELSE ''
                   END IN $sentiments
                   OR ('Urgent' IN $sentiments AND sentimentLabel = 'urgent'))
              AND ($questions_only = false OR p.text CONTAINS '?')
            RETURN {{
                id: coalesce(p.uuid, 'post:' + elementId(p)),
                channel: coalesce(ch.title, ch.username, 'Community message'),
                author: coalesce(ch.title, ch.username, 'Community message'),
                text: trim(coalesce(p.text, '')),
                timestamp: toString(p.posted_at),
                reactions: coalesce(p.views, 0),
                replies: coalesce(p.comment_count, 0),
                topic: t.name
            }} AS evidence

            UNION ALL

            WITH t
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
            OPTIONAL MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            WITH c, u, ch, t, max(toLower(coalesce(s.label, ''))) AS sentimentLabel
            WHERE {_channel_predicate('ch')}
              AND ($sentiment_count = 0 OR
                   CASE
                       WHEN sentimentLabel = 'positive' THEN 'Positive'
                       WHEN sentimentLabel = 'neutral' THEN 'Neutral'
                       WHEN sentimentLabel IN $fear_labels THEN 'Negative'
                       ELSE ''
                   END IN $sentiments
                   OR ('Urgent' IN $sentiments AND sentimentLabel = 'urgent'))
              AND ($questions_only = false OR c.text CONTAINS '?')
            RETURN {{
                id: coalesce(c.uuid, 'comment:' + elementId(c)),
                channel: coalesce(ch.title, ch.username, 'Community message'),
                author: coalesce(toString(u.telegram_user_id), coalesce(ch.title, ch.username, 'Community member')),
                text: trim(coalesce(c.text, '')),
                timestamp: toString(c.posted_at),
                reactions: 0,
                replies: 0,
                topic: t.name
            }} AS evidence
        }}
        WITH evidence
        WHERE evidence.text <> ''
        RETURN evidence
        ORDER BY evidence.timestamp DESC, evidence.id DESC
        LIMIT $limit
        """,
        {
            "topics": cleaned_topics,
            "start": ctx.start_at.isoformat(),
            "end": ctx.end_at.isoformat(),
            "channels": normalized_channels,
            "channel_count": len(normalized_channels),
            "sentiments": normalized_sentiments,
            "sentiment_count": len(normalized_sentiments),
            "fear_labels": sorted(_NEGATIVE_SENTIMENT_LABELS),
            "questions_only": bool(questions_only),
            "limit": max(1, min(int(limit), 24)),
        },
    )
    return [dict(row.get("evidence") or {}) for row in rows if isinstance(row.get("evidence"), dict)]


def get_graph_data(filters: dict | None = None) -> dict:
    resolved_filters = _resolve_filters(filters)
    ctx = _resolve_context(filters)
    cache_key = _cache_key(
        "graph:data:v2",
        {
            "scope": ctx.cache_key,
            "channels": resolved_filters["channels"],
            "sentiments": resolved_filters["sentiments"],
            "topics": resolved_filters["topics"],
            "category": resolved_filters["category"],
            "signal_focus": resolved_filters["signal_focus"],
            "source_detail": resolved_filters["source_detail"],
            "ranking_mode": resolved_filters["ranking_mode"],
            "min_mentions": resolved_filters["min_mentions"],
            "max_nodes": resolved_filters["max_nodes"],
        },
    )

    def build_graph() -> dict[str, Any]:
        topic_rows = _load_topic_rows(ctx, resolved_filters)
        visible_topics, available_categories, total_eligible_topics = _filter_topic_rows(topic_rows, resolved_filters)

        if not visible_topics:
            return {
                "nodes": [],
                "links": [],
                "meta": {
                    "from": ctx.from_date.isoformat(),
                    "to": ctx.to_date.isoformat(),
                    "days": ctx.days,
                    "selectedChannels": resolved_filters["channels"],
                    "selectedSentiments": resolved_filters["sentiments"],
                    "selectedCategory": resolved_filters["category"] or None,
                    "signalFocus": resolved_filters["signal_focus"],
                    "sourceDetail": resolved_filters["source_detail"],
                    "rankingMode": resolved_filters["ranking_mode"],
                    "minMentions": resolved_filters["min_mentions"],
                    "availableCategories": available_categories,
                    "visibleTopicCount": 0,
                    "totalEligibleTopicCount": total_eligible_topics,
                    "topicLimit": resolved_filters["max_nodes"],
                    "isCurated": False,
                    "visibleCategoryCount": 0,
                    "visibleChannelCount": 0,
                    "totalMentions": 0,
                    "generatedAt": datetime.now(timezone.utc).isoformat(),
                },
            }

        category_nodes, category_links = _build_category_nodes(visible_topics)
        channel_nodes, channel_links = _build_channel_nodes(visible_topics, resolved_filters["source_detail"])

        nodes = list(category_nodes.values()) + visible_topics + list(channel_nodes.values())
        links = list(category_links.values()) + list(channel_links.values())

        return {
            "nodes": nodes,
            "links": links,
            "meta": {
                "from": ctx.from_date.isoformat(),
                "to": ctx.to_date.isoformat(),
                "days": ctx.days,
                "selectedChannels": resolved_filters["channels"],
                "selectedSentiments": resolved_filters["sentiments"],
                "selectedCategory": resolved_filters["category"] or None,
                "signalFocus": resolved_filters["signal_focus"],
                "sourceDetail": resolved_filters["source_detail"],
                "rankingMode": resolved_filters["ranking_mode"],
                "minMentions": resolved_filters["min_mentions"],
                "availableCategories": available_categories,
                "visibleTopicCount": len(visible_topics),
                "totalEligibleTopicCount": total_eligible_topics,
                "topicLimit": resolved_filters["max_nodes"],
                "isCurated": total_eligible_topics > len(visible_topics),
                "visibleCategoryCount": len(category_nodes),
                "visibleChannelCount": len(channel_nodes),
                "totalMentions": sum(_to_int(node.get("mentionCount")) for node in visible_topics),
                "generatedAt": datetime.now(timezone.utc).isoformat(),
            },
        }

    return _get_cached_graph_value(cache_key, build_graph)


def _graph_scope_topic_rows(
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    timeframe: str | None = None,
    channels: list[str] | None = None,
    sentiments: list[str] | None = None,
    category: str | None = None,
    topics: list[str] | None = None,
    signal_focus: str | None = None,
    min_mentions: int = 2,
    max_nodes: int = 36,
) -> tuple[list[dict[str, Any]], DashboardDateContext]:
    raw_filters = {
        "from_date": from_date,
        "to_date": to_date,
        "timeframe": timeframe,
        "channels": channels or [],
        "sentiments": sentiments or [],
        "category": category,
        "topics": topics or [],
        "signalFocus": signal_focus,
        "minMentions": min_mentions,
        "max_nodes": max_nodes,
    }
    resolved_filters = _resolve_filters(raw_filters)
    ctx = _resolve_context(raw_filters, timeframe=timeframe)
    topic_rows = _load_topic_rows(ctx, resolved_filters)
    visible_topics, _, _ = _filter_topic_rows(topic_rows, resolved_filters)
    return visible_topics, ctx


def get_node_details(
    node_id: str,
    node_type: str,
    *,
    timeframe: str | None = None,
    channels: list[str] | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    sentiments: list[str] | None = None,
    category: str | None = None,
    signal_focus: str | None = None,
) -> dict | None:
    node_type_l = str(node_type or "").strip().lower()
    ctx = _resolve_context(
        {
            "from_date": from_date,
            "to_date": to_date,
            "timeframe": timeframe,
        },
        timeframe=timeframe,
    )
    cache_key = _cache_key(
        "graph:node_details:v3",
        {
            "scope": ctx.cache_key,
            "node_id": str(node_id or "").strip(),
            "node_type": node_type_l,
            "channels": _normalize_channels(channels),
            "sentiments": _normalize_sentiments(sentiments),
            "category": _normalize_category(category),
            "signal_focus": _normalize_signal_focus(signal_focus),
        },
    )

    def build_details() -> dict[str, Any] | None:
        topic_rows, scoped_ctx = _graph_scope_topic_rows(
            from_date=from_date,
            to_date=to_date,
            timeframe=timeframe,
            channels=channels,
            sentiments=sentiments,
            category=category,
            signal_focus=signal_focus,
            max_nodes=60,
        )

        if node_type_l == "category":
            category_name = node_id.split(":", 1)[1] if node_id.startswith("category:") else node_id
            scoped_topics = [row for row in topic_rows if str(row.get("category") or "") == category_name]
            if not scoped_topics:
                return None
            evidence = _load_evidence_for_topic_names(
                [str(row.get("name") or "") for row in scoped_topics[:12]],
                scoped_ctx,
                channels=channels,
                sentiments=sentiments,
                limit=8,
            )
            topic_payload = [
                {
                    "name": row["name"],
                    "mentions": _to_int(row.get("mentionCount")),
                    "growth": _to_float(row.get("trendPct")),
                    "dominantSentiment": row.get("dominantSentiment"),
                }
                for row in scoped_topics[:8]
            ]
            channel_scores: defaultdict[str, int] = defaultdict(int)
            for row in scoped_topics:
                for channel in row.get("topChannels") or []:
                    channel_scores[_safe_name(channel.get("name"), "")] += _to_int(channel.get("mentions"))
            top_channels = [
                {"name": name, "mentions": mentions}
                for name, mentions in sorted(channel_scores.items(), key=lambda item: (-item[1], item[0]))[:8]
            ]
            mention_total = sum(_to_int(row.get("mentionCount")) for row in scoped_topics)
            weighted_growth = sum(_to_float(row.get("trendPct")) * max(1, _to_int(row.get("mentionCount"))) for row in scoped_topics)
            overview = topic_overviews.get_category_overview(
                category_name,
                [
                    {
                        "name": str(row.get("name") or ""),
                        "mentions": _to_int(row.get("mentionCount")),
                        "dominantSentiment": row.get("dominantSentiment"),
                    }
                    for row in scoped_topics[:8]
                ],
            )
            return {
                "id": _category_label_id(category_name),
                "name": category_name,
                "type": "category",
                "topicCount": len(scoped_topics),
                "mentionCount": mention_total,
                "trendPct": round(weighted_growth / max(1, mention_total), 1),
                "dominantSentiment": _dominant_sentiment(
                    {
                        "sentimentPositive": sum(_to_int(row.get("sentimentPositive")) for row in scoped_topics),
                        "sentimentNeutral": sum(_to_int(row.get("sentimentNeutral")) for row in scoped_topics),
                        "sentimentNegative": sum(_to_int(row.get("sentimentNegative")) for row in scoped_topics),
                        "urgentSignals": sum(_to_int(row.get("urgentSignals")) for row in scoped_topics),
                    }
                ),
                "askSignalCount": sum(_to_int(row.get("askSignalCount")) for row in scoped_topics),
                "needSignalCount": sum(_to_int(row.get("needSignalCount")) for row in scoped_topics),
                "fearSignalCount": sum(_to_int(row.get("fearSignalCount")) for row in scoped_topics),
                "topTopics": topic_payload,
                "topChannels": top_channels,
                "overview": overview,
                "evidence": evidence,
                "from": scoped_ctx.from_date.isoformat(),
                "to": scoped_ctx.to_date.isoformat(),
            }
        if node_type_l == "topic":
            topic_name = node_id.split(":", 1)[1] if node_id.startswith("topic:") else node_id
            row = next((item for item in topic_rows if str(item.get("name") or "") == topic_name), None)
            if not row:
                return None
            topic_detail = comparative.get_topic_detail(
                topic_name,
                str(row.get("category") or ""),
                scoped_ctx,
            ) or {}
            topic_evidence = comparative.get_topic_evidence_page(
                topic_name,
                str(row.get("category") or ""),
                "all",
                0,
                20,
                None,
                scoped_ctx,
            ) or {}
            topic_questions = comparative.get_topic_evidence_page(
                topic_name,
                str(row.get("category") or ""),
                "questions",
                0,
                20,
                None,
                scoped_ctx,
            ) or {}
            overview = topic_overviews.get_topic_overview(
                str(topic_detail.get("sourceTopic") or row.get("name") or topic_name),
                str(topic_detail.get("category") or row.get("category") or ""),
            )
            if overview is None:
                overview = topic_overviews.build_fallback_topic_overview(topic_detail or row, scoped_ctx)
            has_scoped_filters = bool(
                _normalize_channels(channels)
                or _normalize_sentiments(sentiments)
                or _normalize_signal_focus(signal_focus) != "all"
            )
            if has_scoped_filters:
                evidence_rows = _load_evidence_for_topic_names(
                    [str(row.get("name") or topic_name)],
                    scoped_ctx,
                    channels=channels,
                    sentiments=sentiments,
                    limit=20,
                )
                question_rows = _load_evidence_for_topic_names(
                    [str(row.get("name") or topic_name)],
                    scoped_ctx,
                    channels=channels,
                    sentiments=sentiments,
                    limit=20,
                    questions_only=True,
                )
            else:
                evidence_rows = list(topic_evidence.get("items") or [])
                question_rows = list(topic_questions.get("items") or [])
            related_topics = run_query(
                """
                MATCH (t:Topic {name: $topic})-[r:CO_OCCURS_WITH]-(other:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
                WHERE coalesce(other.proposed, false) = false
                  AND NOT toLower(trim(coalesce(other.name, ''))) IN $noise
                  AND NOT toLower(trim(coalesce(cat.name, 'General'))) IN $excluded_categories
                RETURN other.name AS name,
                       cat.name AS category,
                       coalesce(r.count, 0) AS mentions
                ORDER BY mentions DESC, name ASC
                LIMIT 8
                """,
                {
                    "topic": topic_name,
                    "noise": sorted(_NOISY_TOPIC_KEYS),
                    "excluded_categories": sorted(_EXCLUDED_CATEGORY_KEYS),
                },
            )
            return {
                "id": row["id"],
                "name": row["name"],
                "type": "topic",
                "category": row.get("category"),
                "mentionCount": _to_int(row.get("mentionCount")),
                "evidenceCount": _to_int(row.get("evidenceCount")),
                "distinctChannels": _to_int(row.get("distinctChannels")),
                "trendPct": _to_float(row.get("trendPct")),
                "dominantSentiment": row.get("dominantSentiment"),
                "askSignalCount": _to_int(row.get("askSignalCount")),
                "needSignalCount": _to_int(row.get("needSignalCount")),
                "fearSignalCount": _to_int(row.get("fearSignalCount")),
                "topChannels": row.get("topChannels") or [],
                "relatedTopics": related_topics,
                "overview": overview,
                "evidence": evidence_rows,
                "questionEvidence": question_rows,
                "dailyRows": list(topic_detail.get("dailyRows") or []),
                "weeklyRows": list(topic_detail.get("weeklyRows") or []),
                "sampleEvidence": topic_detail.get("sampleEvidence"),
                "sampleQuote": topic_detail.get("sampleQuote"),
                "sourceTopic": topic_detail.get("sourceTopic") or row.get("name"),
                "lastSeen": topic_detail.get("latestAt") or row.get("lastSeen"),
                "from": scoped_ctx.from_date.isoformat(),
                "to": scoped_ctx.to_date.isoformat(),
            }

        if node_type_l == "channel":
            channel_id = node_id.split(":", 1)[1] if node_id.startswith("channel:") else node_id
            rows = run_query(
                f"""
                MATCH (ch:Channel)
                WHERE (
                       ch.uuid = $channel_id
                    OR toLower(coalesce(ch.title, '')) = $channel_key
                    OR toLower(coalesce(ch.username, '')) = $channel_key
                )
                  AND coalesce(ch.source_type, 'channel') = 'channel'
                CALL {{
                    WITH ch
                    OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
                    WHERE p.posted_at >= datetime($start)
                      AND p.posted_at < datetime($end)
                      AND coalesce(p.entry_kind, 'broadcast_post') = 'broadcast_post'
                      AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
                      AND NOT toLower(trim(coalesce(cat.name, 'General'))) IN $excluded_categories
                    WITH p, t, cat
                    ORDER BY coalesce(p.posted_at, datetime($start)) DESC
                    RETURN count(DISTINCT p) AS postCount,
                           collect(DISTINCT {{
                               name: t.name,
                               category: cat.name
                           }})[..24] AS rawTopics
                }}
                RETURN coalesce(ch.title, ch.username) AS name,
                       ch.username AS username,
                       postCount,
                       rawTopics
                LIMIT 1
                """,
                {
                    "channel_id": channel_id,
                    "channel_key": channel_id.lower(),
                    "start": scoped_ctx.start_at.isoformat(),
                    "end": scoped_ctx.end_at.isoformat(),
                    "noise": sorted(_NOISY_TOPIC_KEYS),
                    "excluded_categories": sorted(_EXCLUDED_CATEGORY_KEYS),
                },
            )
            if not rows:
                return None
            row = rows[0]
            canonical_topics: dict[tuple[str, str], dict[str, Any]] = {}
            for topic in row.get("rawTopics") or []:
                raw_topic_name = _safe_name(topic.get("name"), "")
                raw_category = _safe_name(topic.get("category"), "General")
                if not raw_topic_name:
                    continue
                classification = classify_topic(raw_topic_name)
                canonical_name = _safe_name((classification or {}).get("taxonomy_topic") or raw_topic_name, raw_topic_name)
                canonical_category = _safe_name((classification or {}).get("closest_category") or raw_category, raw_category)
                key = (canonical_name.lower(), canonical_category.lower())
                canonical_topics[key] = {
                    "name": canonical_name,
                    "category": canonical_category,
                }
            category_counts: defaultdict[str, int] = defaultdict(int)
            for topic in canonical_topics.values():
                category_counts[_safe_name(topic.get("category"), "General")] += 1
            return {
                "id": node_id if node_id.startswith("channel:") else f"channel:{channel_id}",
                "name": _safe_name(row.get("name"), channel_id),
                "type": "channel",
                "username": row.get("username"),
                "postCount": _to_int(row.get("postCount")),
                "topics": list(canonical_topics.values())[:12],
                "categories": [
                    {"name": name, "topicCount": topic_count}
                    for name, topic_count in sorted(category_counts.items(), key=lambda item: (-item[1], item[0]))
                ],
                "from": scoped_ctx.from_date.isoformat(),
                "to": scoped_ctx.to_date.isoformat(),
            }

        return None

    return _get_cached_graph_value(cache_key, build_details)


def search_graph(query: str, limit: int = 20) -> list[dict]:
    q = str(query or "").strip().lower()
    if not q:
        return []
    lim = max(1, min(int(limit), 100))
    cache_key = _cache_key("graph:search:v1", {"q": q, "limit": lim})

    def build_search() -> list[dict[str, Any]]:
        return run_query(
            """
            CALL {
                MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
                WHERE coalesce(t.proposed, false) = false
                  AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
                  AND NOT toLower(trim(coalesce(cat.name, 'General'))) IN $excluded_categories
                  AND toLower(t.name) CONTAINS $q
                RETURN 'topic' AS type,
                       'topic:' + t.name AS id,
                       t.name AS name,
                       cat.name AS text,
                       4 AS rank

                UNION ALL

                MATCH (cat:TopicCategory)
                WHERE NOT toLower(trim(coalesce(cat.name, 'General'))) IN $excluded_categories
                  AND toLower(cat.name) CONTAINS $q
                RETURN 'category' AS type,
                       'category:' + cat.name AS id,
                       cat.name AS name,
                       'Category' AS text,
                       3 AS rank

                UNION ALL

                MATCH (ch:Channel)
                WHERE toLower(coalesce(ch.title, '')) CONTAINS $q
                   OR toLower(coalesce(ch.username, '')) CONTAINS $q
                RETURN 'channel' AS type,
                       'channel:' + ch.uuid AS id,
                       coalesce(ch.title, ch.username) AS name,
                       ch.username AS text,
                       2 AS rank
            }
            RETURN type, id, name, text
            ORDER BY rank DESC, name ASC
            LIMIT $limit
            """,
            {
                "q": q,
                "limit": lim,
                "noise": sorted(_NOISY_TOPIC_KEYS),
                "excluded_categories": sorted(_EXCLUDED_CATEGORY_KEYS),
            },
        )

    return _get_cached_graph_value(cache_key, build_search)


def get_trending_topics(limit: int = 10, timeframe: str | None = None) -> list[dict]:
    ctx = _resolve_context({"timeframe": timeframe})
    lim = max(1, min(limit, 100))
    cache_key = _cache_key("graph:trending:v1", {"scope": ctx.cache_key, "limit": lim})

    def build_trending() -> list[dict[str, Any]]:
        return run_query(
            """
            MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
            WHERE coalesce(t.proposed, false) = false
              AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
              AND NOT toLower(trim(coalesce(cat.name, 'General'))) IN $excluded_categories
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                RETURN count(p) AS postCount
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                RETURN count(c) AS postCount
            }
            WITH t, sum(postCount) AS mentionCount
            WHERE mentionCount > 0
            RETURN t.name AS name,
                   'topic:' + t.name AS id,
                   mentionCount AS adCount
            ORDER BY adCount DESC, name ASC
            LIMIT $limit
            """,
            {
                "start": ctx.start_at.isoformat(),
                "end": ctx.end_at.isoformat(),
                "limit": lim,
                "noise": sorted(_NOISY_TOPIC_KEYS),
                "excluded_categories": sorted(_EXCLUDED_CATEGORY_KEYS),
            },
        )

    return _get_cached_graph_value(cache_key, build_trending)


def get_top_channels(limit: int = 10, timeframe: str | None = None) -> list[dict]:
    ctx = _resolve_context({"timeframe": timeframe})
    lim = max(1, min(limit, 100))
    cache_key = _cache_key("graph:top_channels:v1", {"scope": ctx.cache_key, "limit": lim})

    def build_top_channels() -> list[dict[str, Any]]:
        return run_query(
            """
            MATCH (ch:Channel)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND coalesce(ch.source_type, 'channel') = 'channel'
              AND coalesce(p.entry_kind, 'broadcast_post') = 'broadcast_post'
            RETURN 'channel:' + ch.uuid AS id,
                   coalesce(ch.title, ch.username) AS name,
                   count(p) AS adCount
            ORDER BY adCount DESC, name ASC
            LIMIT $limit
            """,
            {"start": ctx.start_at.isoformat(), "end": ctx.end_at.isoformat(), "limit": lim},
        )

    return _get_cached_graph_value(cache_key, build_top_channels)


def get_all_channels() -> list[dict]:
    rows = run_query(
        """
        MATCH (ch:Channel)
        WHERE coalesce(ch.source_type, 'channel') = 'channel'
        OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
        WHERE coalesce(p.entry_kind, 'broadcast_post') = 'broadcast_post'
        WITH ch, count(p) AS postCount
        RETURN 'channel:' + ch.uuid AS id,
               coalesce(ch.title, ch.username) AS name,
               postCount AS adCount
        ORDER BY adCount DESC, name ASC
        """
    )
    return rows


def get_sentiment_distribution(timeframe: str | None = None) -> list[dict]:
    ctx = _resolve_context({"timeframe": timeframe})
    cache_key = _cache_key("graph:sentiment_distribution:v1", {"scope": ctx.cache_key})

    def build_distribution() -> list[dict[str, Any]]:
        return run_query(
            """
            CALL {
                MATCH (p:Post)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                RETURN s.label AS label, count(*) AS count

                UNION ALL

                MATCH (c:Comment)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                RETURN s.label AS label, count(*) AS count
            }
            RETURN label, sum(count) AS count
            ORDER BY count DESC, label ASC
            """,
            {"start": ctx.start_at.isoformat(), "end": ctx.end_at.isoformat()},
        )

    return _get_cached_graph_value(cache_key, build_distribution)


def get_graph_insights(timeframe: str | None = None) -> dict:
    ctx = _resolve_context({"timeframe": timeframe})
    cache_key = _cache_key("graph:insights:v1", {"scope": ctx.cache_key})

    def build_insights() -> dict[str, Any]:
        graph = get_graph_data(
            {
                "from_date": ctx.from_date.isoformat(),
                "to_date": ctx.to_date.isoformat(),
                "max_nodes": 12,
                "sourceDetail": "minimal",
            }
        )
        topic_names = [node["name"] for node in graph.get("nodes", []) if node.get("type") == "topic"][:3]
        category_names = [node["name"] for node in graph.get("nodes", []) if node.get("type") == "category"][:3]
        insight = (
            f"Conversation map for {ctx.range_label}: "
            f"{graph.get('meta', {}).get('visibleTopicCount', 0)} topics across "
            f"{graph.get('meta', {}).get('visibleCategoryCount', 0)} categories. "
            f"Top categories: {', '.join(category_names) or 'n/a'}. "
            f"Leading topics: {', '.join(topic_names) or 'n/a'}."
        )
        return {"insight": insight, "timestamp": datetime.now(timezone.utc).isoformat()}

    return _get_cached_graph_value(cache_key, build_insights)
