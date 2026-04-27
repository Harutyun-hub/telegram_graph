from __future__ import annotations

import logging
import re
from typing import Any

from client import AnalyticsAPIError
from formatters import build_success
from models import (
    AddSourceRequest,
    AskInsightsRequest,
    CompareChannelsRequest,
    CompareTopicsRequest,
    GetFreshnessStatusRequest,
    GetActiveAlertsRequest,
    GetDecliningTopicsRequest,
    GetGraphSnapshotRequest,
    GetNodeContextRequest,
    GetProblemSpikesRequest,
    GetQuestionClustersRequest,
    GetSentimentOverviewRequest,
    GetTopicDetailRequest,
    GetTopicEvidenceRequest,
    GetTopTopicsRequest,
    InvestigateChannelRequest,
    InvestigateQuestionRequest,
    InvestigateTopicRequest,
    SearchEntitiesRequest,
    ValidationError,
)


STOPWORDS = {
    "a", "an", "and", "are", "about", "at", "be", "by", "for", "from", "how", "in",
    "is", "it", "main", "of", "on", "or", "the", "to", "what", "which", "who", "why",
    "with", "driving", "drive", "issue", "issues", "problem", "problems", "trend", "trends",
    "now", "right", "week", "month", "today", "current", "currently",
}
TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")
QUOTED_PHRASE_RE = re.compile(r"['\"]([^'\"]{2,80})['\"]")
TITLECASE_PHRASE_RE = re.compile(r"\b(?:[A-Z][A-Za-z0-9_-]+(?:\s+[A-Z][A-Za-z0-9_-]+)+)\b")
SEVERITY_RANK = {"critical": 4, "high": 3, "urgent": 3, "medium": 2, "low": 1}
SOURCE_PRIORITY = {
    "question_brief": 4,
    "problem_brief": 3,
    "urgency_signal": 3,
    "trending_topic": 2,
    "insight_card": 1,
}
QUESTION_ALIASES = {
    "politics": ["Political protests", "Politics"],
    "political": ["Political protests", "Politics"],
    "permit": ["Residency permits", "permit"],
    "permits": ["Residency permits", "permits"],
    "residence permit": ["Residency permits", "residence permit"],
    "residence permits": ["Residency permits", "residence permits"],
    "residency": ["Residency permits", "residency"],
    "visa": ["Residency permits", "Visa appointments", "visa"],
    "visas": ["Residency permits", "Visa appointments", "visas"],
    "appointment": ["Residency permits", "Visa appointments", "appointment"],
    "appointments": ["Residency permits", "Visa appointments", "appointments"],
    "paperwork": ["Residency permits", "paperwork"],
    "document": ["Residency permits", "Documents", "document"],
    "documents": ["Residency permits", "Documents", "documents"],
    "housing": ["Rental costs", "Housing"],
    "rent": ["Rental costs", "rent"],
    "rental": ["Rental costs", "rental"],
    "school": ["School admissions", "schools"],
    "schools": ["School admissions", "schools"],
    "admission": ["School admissions", "admissions"],
}
GRAPH_HINT_TOKENS = {
    "channels", "community", "discussion", "ecosystem", "graph", "hidden", "landscape",
    "map", "network", "networks", "pattern", "patterns", "shaping", "conversation",
}
CHANNEL_HINT_TOKENS = {"channel", "channels", "chat", "group", "groups", "inside", "in"}
RESOLUTION_PROBE_LIMIT = 3
TELEGRAM_SOURCE_RE = re.compile(r"^(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/", re.IGNORECASE)
FACEBOOK_SOURCE_RE = re.compile(r"^(?:https?://)?(?:(?:www|m)\.)?(?:facebook\.com|fb\.com)/", re.IGNORECASE)
INSTAGRAM_SOURCE_RE = re.compile(r"^(?:https?://)?(?:(?:www|m)\.)?instagram\.com/", re.IGNORECASE)
WEB_DOMAIN_RE = re.compile(r"^(?:https?://)?(?:www\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)+(?::\d+)?(?:[/?#].*)?$", re.IGNORECASE)
logger = logging.getLogger(__name__)


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _trim_text(value: Any, limit: int = 180) -> str:
    text = _clean_text(value)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", _clean_text(value).lower()).strip()


def _tokenize(value: Any) -> list[str]:
    return [token for token in TOKEN_RE.findall(_normalize_text(value)) if token and token not in STOPWORDS]


def _topic_matches(candidate_topic: Any, requested_topic: str | None) -> bool:
    if not requested_topic:
        return True
    candidate = _normalize_text(candidate_topic)
    requested = _normalize_text(requested_topic)
    if not candidate or not requested:
        return False
    if requested in candidate or candidate in requested:
        return True
    requested_tokens = set(_tokenize(requested))
    candidate_tokens = set(_tokenize(candidate))
    return bool(requested_tokens and candidate_tokens and requested_tokens.issubset(candidate_tokens))


def _dashboard_data(payload: dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload


def _detect_source_type(value: str, requested_type: str) -> str:
    source_type = _normalize_text(requested_type) or "auto"
    if source_type != "auto":
        return source_type

    text = _clean_text(value)
    if not text:
        raise ValidationError("value is required")

    if text.startswith("@") or TELEGRAM_SOURCE_RE.match(text):
        return "telegram"
    if FACEBOOK_SOURCE_RE.match(text):
        return "facebook_page"
    if INSTAGRAM_SOURCE_RE.match(text):
        return "instagram_profile"
    if WEB_DOMAIN_RE.match(text):
        return "google_domain"

    raise ValidationError("Source is ambiguous. Provide a full URL or @handle.")


def _source_item_label(item: dict[str, Any]) -> str:
    for key in ("channel_title", "channel_username", "display_url", "company_name", "value"):
        text = _clean_text(item.get(key))
        if text:
            return text
    return "Source"


def _source_action_summary(action: str, label: str) -> str:
    if action == "created":
        return f"{label} was added to the tracking list."
    if action == "reactivated":
        return f"{label} was already known and has been reactivated for tracking."
    return f"{label} is already in the tracking list."


def _source_action_bullets(action: str, item: dict[str, Any]) -> list[str]:
    bullets = []
    status = _clean_text(item.get("resolution_status"))
    if action == "created":
        bullets.append("Status: source added successfully.")
    elif action == "reactivated":
        bullets.append("Status: existing source restored to active tracking.")
    else:
        bullets.append("Status: source already tracked.")
    if item.get("platform"):
        bullets.append(f"Platform: {item['platform']}.")
    if item.get("source_type"):
        bullets.append(f"Source type: {item['source_type']}.")
    if status:
        bullets.append(f"Resolution status: {status}.")
    return _limit_bullets(bullets)


def _build_source_item(action: str, resolved_type: str, request_value: str, backend_item: dict[str, Any]) -> dict[str, Any]:
    platform = "telegram" if resolved_type == "telegram" else _clean_text(backend_item.get("platform")) or "social"
    source_type = "telegram" if resolved_type == "telegram" else _clean_text(backend_item.get("source_kind")) or resolved_type
    value = (
        _clean_text(backend_item.get("channel_username"))
        or _clean_text(backend_item.get("display_url"))
        or _clean_text(request_value)
    )
    return {
        "platform": platform,
        "source_type": source_type,
        "value": value,
        "status": action,
        "is_active": bool(backend_item.get("is_active", True)),
        "resolution_status": backend_item.get("resolution_status"),
        "title": _clean_text(backend_item.get("channel_title") or backend_item.get("company_name")),
    }


def _severity_sort_key(value: Any) -> int:
    return SEVERITY_RANK.get(_normalize_text(value), 0)


def _limit_bullets(bullets: list[str], limit: int = 4) -> list[str]:
    return bullets[:limit]


def _compact_evidence_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "type": row.get("type"),
        "author": row.get("author"),
        "channel": row.get("channel"),
        "text": _trim_text(row.get("text"), 180),
        "timestamp": row.get("timestamp"),
        "reactions": int(row.get("reactions") or 0),
        "replies": int(row.get("replies") or 0),
    }


def _compact_sample_evidence(row: Any) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    return {
        "id": row.get("id"),
        "type": row.get("type"),
        "author": row.get("author"),
        "channel": row.get("channel"),
        "text": _trim_text(row.get("text"), 160),
        "timestamp": row.get("timestamp"),
    }


def _topic_detail_item(detail: dict[str, Any]) -> dict[str, Any]:
    return {
        "topic": detail.get("name"),
        "category": detail.get("category"),
        "mention_count": int(detail.get("mentionCount") or 0),
        "growth_7d_pct": float(detail.get("growth7dPct") or 0.0),
        "user_count": int(detail.get("userCount") or 0),
        "distinct_channels": int(detail.get("distinctChannels") or 0),
        "top_channels": list(detail.get("topChannels") or [])[:3],
        "sentiment_positive": int(detail.get("sentimentPositive") or 0),
        "sentiment_neutral": int(detail.get("sentimentNeutral") or 0),
        "sentiment_negative": int(detail.get("sentimentNegative") or 0),
        "sample_evidence": _compact_sample_evidence(detail.get("sampleEvidence")),
    }


def _freshness_item(snapshot: dict[str, Any]) -> dict[str, Any]:
    health = snapshot.get("health") if isinstance(snapshot.get("health"), dict) else {}
    backlog = snapshot.get("backlog") if isinstance(snapshot.get("backlog"), dict) else {}
    drift = snapshot.get("drift") if isinstance(snapshot.get("drift"), dict) else {}
    pipeline = snapshot.get("pipeline") if isinstance(snapshot.get("pipeline"), dict) else {}
    scrape = pipeline.get("scrape") if isinstance(pipeline.get("scrape"), dict) else {}
    process = pipeline.get("process") if isinstance(pipeline.get("process"), dict) else {}
    sync = pipeline.get("sync") if isinstance(pipeline.get("sync"), dict) else {}
    return {
        "health_status": health.get("status") or "unknown",
        "health_score": int(health.get("score") or 0),
        "unsynced_posts": int(backlog.get("unsynced_posts") or 0),
        "unprocessed_posts": int(backlog.get("unprocessed_posts") or 0),
        "unprocessed_comments": int(backlog.get("unprocessed_comments") or 0),
        "latest_post_delta_minutes": drift.get("latest_post_delta_minutes"),
        "scrape_age_minutes": scrape.get("age_minutes"),
        "process_age_minutes": process.get("age_minutes"),
        "sync_age_minutes": sync.get("age_minutes"),
    }


def _freshness_caveat(snapshot: dict[str, Any]) -> str | None:
    item = _freshness_item(snapshot)
    status = _normalize_text(item.get("health_status"))
    if status not in {"warning", "stale"}:
        return None
    return (
        f"Data freshness is {status}. Unsynced posts: {item['unsynced_posts']}, "
        f"latest post delta: {item.get('latest_post_delta_minutes')} minutes."
    )


def _extract_primary_topic(payload: dict[str, Any]) -> str | None:
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        return None
    first = items[0] if isinstance(items[0], dict) else {}
    topic = first.get("topic")
    text = _clean_text(topic)
    return text or None


def _dedupe_texts(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        text = _clean_text(value)
        normalized = _normalize_text(text)
        if not text or not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(text)
    return deduped


def _extract_quoted_phrases(value: str) -> list[str]:
    return _dedupe_texts([match.group(1) for match in QUOTED_PHRASE_RE.finditer(value or "")])


def _extract_titlecase_phrases(value: str) -> list[str]:
    phrases: list[str] = []
    for match in TITLECASE_PHRASE_RE.finditer(value or ""):
        phrase = _clean_text(match.group(0))
        if len(_tokenize(phrase)) < 1:
            continue
        phrases.append(phrase)
    return _dedupe_texts(phrases)


def _alias_terms_for_text(value: Any) -> list[str]:
    normalized = _normalize_text(value)
    if not normalized:
        return []
    matches: list[str] = []
    for alias, expansions in QUESTION_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", normalized):
            matches.extend(expansions)
    return _dedupe_texts(matches)


def _keyword_bigrams(value: Any) -> list[str]:
    tokens = [token for token in _tokenize(value) if len(token) >= 4]
    return _dedupe_texts([" ".join(tokens[idx: idx + 2]) for idx in range(max(0, len(tokens) - 1))][:3])


def _alias_hint_candidates(value: Any, *, limit: int = 2) -> list[dict[str, Any]]:
    normalized_value = _normalize_text(value)
    hint_terms = [
        term for term in _dedupe_texts(_alias_terms_for_text(value) + _extract_titlecase_phrases(_clean_text(value)))
        if _normalize_text(term) and _normalize_text(term) != normalized_value
    ]
    multiword_terms = [term for term in hint_terms if len(_tokenize(term)) >= 2]
    if multiword_terms:
        hint_terms = multiword_terms
    hint_terms = hint_terms[:limit]
    candidates: list[dict[str, Any]] = []
    for term in hint_terms:
        if len(_tokenize(term)) < 1:
            continue
        candidates.append(
            {
                "source": "alias_hint",
                "topic": term,
                "summary": "Closest local topic interpretation from the question wording.",
                "detail": "No exact backend entity matched yet for this wording in the selected window.",
                "evidence_count": 0,
            }
        )
    return candidates


def _alias_hint_search_items(value: Any, *, limit: int = 2) -> list[dict[str, Any]]:
    return [
        {
            "type": "topic_hint",
            "id": None,
            "name": candidate.get("topic"),
            "text": _trim_text(candidate.get("detail"), 120),
        }
        for candidate in _alias_hint_candidates(value, limit=limit)
        if candidate.get("topic")
    ]


def _is_graph_wide_question(value: str) -> bool:
    tokens = set(_tokenize(value))
    return bool(tokens.intersection(GRAPH_HINT_TOKENS))


def _is_channel_question(value: str) -> bool:
    tokens = set(_tokenize(value))
    if tokens.intersection(CHANNEL_HINT_TOKENS):
        return True
    return bool(_extract_titlecase_phrases(value))


def _resolution_terms(base_payload: dict[str, Any], question: str, *, attempted_topic: str | None = None) -> list[str]:
    terms: list[str] = []
    items = base_payload.get("items")
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            topic = _clean_text(item.get("topic"))
            if topic:
                terms.append(topic)
            name = _clean_text(item.get("name"))
            if name:
                terms.append(name)

    if attempted_topic:
        terms.append(attempted_topic)
    terms.extend(_extract_quoted_phrases(question))
    terms.extend(_extract_titlecase_phrases(question))
    terms.extend(_alias_terms_for_text(question))
    terms.extend(_alias_terms_for_text(attempted_topic))
    terms.extend(_keyword_bigrams(question))
    keyword_terms = [token for token in _tokenize(question) if len(token) >= 4]
    terms.extend(keyword_terms)
    return _dedupe_texts(terms)


def _prioritized_resolution_terms(
    base_payload: dict[str, Any],
    question: str,
    *,
    attempted_topic: str | None = None,
    limit: int = RESOLUTION_PROBE_LIMIT,
) -> list[str]:
    base_items = base_payload.get("items") if isinstance(base_payload.get("items"), list) else []
    base_topics: list[str] = []
    base_names: list[str] = []
    for item in base_items:
        if not isinstance(item, dict):
            continue
        topic = _clean_text(item.get("topic"))
        if topic:
            base_topics.append(topic)
        name = _clean_text(item.get("name"))
        if name:
            base_names.append(name)

    alias_terms = _alias_terms_for_text(question)
    alias_multiword = [term for term in alias_terms if len(_tokenize(term)) >= 2]
    phrase_terms = _extract_quoted_phrases(question) + _extract_titlecase_phrases(question)
    bigram_terms = _keyword_bigrams(question)
    keyword_terms = [token for token in _tokenize(question) if len(token) >= 4]

    ordered = _dedupe_texts(
        ([attempted_topic] if attempted_topic else [])
        + alias_multiword
        + phrase_terms
        + base_topics
        + base_names
        + alias_terms
        + bigram_terms
        + keyword_terms
    )
    return ordered[:limit]


def _merge_search_items(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged = list(existing)
    seen = {
        (
            _normalize_text(item.get("type")),
            _normalize_text(item.get("id")),
            _normalize_text(item.get("name")),
        )
        for item in existing
        if isinstance(item, dict)
    }
    for item in incoming:
        if not isinstance(item, dict):
            continue
        key = (
            _normalize_text(item.get("type")),
            _normalize_text(item.get("id")),
            _normalize_text(item.get("name")),
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)
    return merged


def _log_resolution_outcome(
    question: str,
    *,
    tried_terms: list[str],
    outcome: str,
    resolved_name: str | None = None,
) -> None:
    logger.info(
        "question_resolution outcome=%s resolved=%s tried_terms=%s question=%s",
        outcome,
        resolved_name or "",
        tried_terms,
        _trim_text(question, 160),
    )


def _candidate_items(payload: dict[str, Any], *, limit: int = 3) -> list[dict[str, Any]]:
    items = payload.get("items")
    if not isinstance(items, list):
        return []
    return [item for item in items[:limit] if isinstance(item, dict)]


def _candidate_bullets(items: list[dict[str, Any]]) -> list[str]:
    return _limit_bullets([
        f"{item.get('source') or item.get('type') or 'signal'}: {item.get('topic') or item.get('name')}"
        for item in items
        if item.get("topic") or item.get("name")
    ])


def _relabel_action(payload: dict[str, Any], action: str) -> dict[str, Any]:
    adjusted = dict(payload)
    adjusted["action"] = action
    return adjusted


def _channel_detail_item(detail: dict[str, Any]) -> dict[str, Any]:
    return {
        "channel": detail.get("title") or detail.get("username"),
        "username": detail.get("username"),
        "member_count": int(detail.get("memberCount") or 0),
        "post_count": int(detail.get("postCount") or 0),
        "avg_views": int(detail.get("avgViews") or 0),
        "daily_messages": int(detail.get("dailyMessages") or 0),
        "growth_7d_pct": float(detail.get("growth7dPct") or 0.0),
        "top_topics": list(detail.get("topTopics") or [])[:3],
        "sentiment_positive": int(detail.get("sentimentPositive") or 0),
        "sentiment_neutral": int(detail.get("sentimentNeutral") or 0),
        "sentiment_negative": int(detail.get("sentimentNegative") or 0),
    }


def _node_context_item(detail: dict[str, Any]) -> dict[str, Any]:
    node_type = _normalize_text(detail.get("type"))
    if node_type == "topic":
        return {
            "type": "topic",
            "name": detail.get("name"),
            "category": detail.get("category"),
            "mention_count": int(detail.get("mentionCount") or 0),
            "evidence_count": int(detail.get("evidenceCount") or 0),
            "distinct_channels": int(detail.get("distinctChannels") or 0),
            "trend_pct": float(detail.get("trendPct") or 0.0),
            "dominant_sentiment": detail.get("dominantSentiment"),
            "top_channels": list(detail.get("topChannels") or [])[:3],
            "related_topics": list(detail.get("relatedTopics") or [])[:3],
        }
    if node_type == "category":
        return {
            "type": "category",
            "name": detail.get("name"),
            "topic_count": int(detail.get("topicCount") or 0),
            "mention_count": int(detail.get("mentionCount") or 0),
            "trend_pct": float(detail.get("trendPct") or 0.0),
            "dominant_sentiment": detail.get("dominantSentiment"),
            "top_topics": list(detail.get("topTopics") or [])[:3],
            "top_channels": list(detail.get("topChannels") or [])[:3],
        }
    return {
        "type": "channel",
        "name": detail.get("name"),
        "username": detail.get("username"),
        "post_count": int(detail.get("postCount") or 0),
        "topics": list(detail.get("topics") or [])[:4],
        "categories": list(detail.get("categories") or [])[:4],
    }


def _graph_snapshot_item(
    graph: dict[str, Any],
    insights: dict[str, Any],
    top_channels: list[dict[str, Any]],
    trending_topics: list[dict[str, Any]],
) -> dict[str, Any]:
    meta = graph.get("meta") if isinstance(graph.get("meta"), dict) else {}
    nodes = list(graph.get("nodes") or [])
    categories = sorted(
        [node for node in nodes if _normalize_text(node.get("type")) == "category"],
        key=lambda row: (int(row.get("mentionCount") or 0), int(row.get("topicCount") or 0)),
        reverse=True,
    )[:3]
    topics = sorted(
        [node for node in nodes if _normalize_text(node.get("type")) == "topic"],
        key=lambda row: int(row.get("mentionCount") or 0),
        reverse=True,
    )[:3]
    return {
        "topic_count": int(meta.get("visibleTopicCount") or 0),
        "category_count": int(meta.get("visibleCategoryCount") or 0),
        "channel_count": int(meta.get("visibleChannelCount") or 0),
        "total_mentions": int(meta.get("totalMentions") or 0),
        "top_categories": [
            {
                "name": row.get("name"),
                "mention_count": int(row.get("mentionCount") or 0),
                "topic_count": int(row.get("topicCount") or 0),
            }
            for row in categories
        ],
        "leading_topics": [
            {"name": row.get("name"), "mention_count": int(row.get("mentionCount") or 0)}
            for row in topics
        ] or [
            {"name": row.get("name"), "mention_count": int(row.get("adCount") or 0)}
            for row in trending_topics[:3]
        ],
        "leading_channels": [
            {"name": row.get("name"), "mention_count": int(row.get("adCount") or 0)}
            for row in top_channels[:3]
        ],
        "insight": _trim_text(insights.get("insight"), 180),
    }


def get_top_topics(client, request: GetTopTopicsRequest) -> dict[str, Any]:
    dashboard = _dashboard_data(client.get_dashboard(request.window))
    rows = sorted(
        list(dashboard.get("trendingTopics") or []),
        key=lambda item: int(item.get("mentions") or 0),
        reverse=True,
    )[: request.limit]

    items = [
        {
            "topic": row.get("name") or row.get("topic"),
            "category": row.get("category") or "General",
            "mentions": int(row.get("mentions") or 0),
            "trend_pct": float(row.get("trendPct") or 0.0),
            "sample_quote": _trim_text(row.get("sampleQuote"), 140),
        }
        for row in rows
    ]

    bullets = [
        f"{item['topic']}: {item['mentions']} mentions, trend {item['trend_pct']:+.1f}%."
        for item in items
    ]
    summary = f"Top {len(items)} topics in the last {request.window}."
    return build_success(
        action="get_top_topics",
        window=request.window,
        summary=summary,
        confidence="high" if items else "low_confidence",
        bullets=bullets or ["No topic activity was returned for this window."],
        items=items,
        source_endpoints=["/api/dashboard"],
        caveat=None if items else "The backend returned no trending topics for the selected window.",
    )


def add_source(client, request: AddSourceRequest) -> dict[str, Any]:
    resolved_type = _detect_source_type(request.value, request.source_type)
    if resolved_type == "telegram":
        payload = client.add_telegram_source(request.value, channel_title=request.title)
        source_endpoints = ["/api/agent/sources/telegram"]
    else:
        payload = client.add_social_source(resolved_type, request.value)
        source_endpoints = ["/api/agent/sources/social"]

    action = _normalize_text(payload.get("action")) or "created"
    backend_item = payload.get("item") if isinstance(payload.get("item"), dict) else {}
    item = _build_source_item(action, resolved_type, request.value, backend_item)
    label = _source_item_label({**backend_item, **item})
    return build_success(
        action="add_source",
        window=None,
        summary=_source_action_summary(action, label),
        confidence="high",
        bullets=_source_action_bullets(action, item),
        items=[item],
        source_endpoints=source_endpoints,
    )


def search_entities(client, request: SearchEntitiesRequest) -> dict[str, Any]:
    rows = list(client.search_entities(request.query, request.limit) or [])[: request.limit]
    alias_hint_items: list[dict[str, Any]] = []
    if not rows:
        alias_hint_items = _alias_hint_search_items(request.query, limit=min(request.limit, 2))
    items = [
        {
            "type": row.get("type"),
            "id": row.get("id"),
            "name": row.get("name"),
            "text": _trim_text(row.get("text"), 120),
        }
        for row in rows
    ] or alias_hint_items
    bullets = _limit_bullets([
        f"{item['type']}: {item['name']}" for item in items if item.get("name")
    ])
    confidence = "low_confidence"
    if items:
        item_type = _normalize_text(items[0].get("type"))
        if item_type == "topic":
            confidence = "high"
        elif item_type == "topic_hint":
            confidence = "low_confidence"
        else:
            confidence = "medium"
    summary = f'Found {len(rows)} matching entities for "{request.query}".'
    caveat = None
    if not rows and alias_hint_items:
        summary = f'No exact backend entities matched "{request.query}". Closest interpreted topics are shown below.'
        caveat = "These are local alias hints, not confirmed backend entity matches."
    return build_success(
        action="search_entities",
        window=None,
        summary=summary,
        confidence=confidence,
        bullets=bullets or ["No matching entities were found."],
        items=items,
        source_endpoints=["/api/search"],
        caveat=caveat or (None if items else "Try a narrower topic or keyword."),
    )


def get_topic_detail(client, request: GetTopicDetailRequest) -> dict[str, Any]:
    detail = client.get_topic_detail(request.topic, request.category, request.window)
    item = _topic_detail_item(detail)
    topic = item["topic"] or request.topic
    sample = item.get("sample_evidence") if isinstance(item.get("sample_evidence"), dict) else {}
    bullets = _limit_bullets([
        f"Top channels: {', '.join(item['top_channels'])}." if item["top_channels"] else "Top channels are limited in this window.",
        (
            f"Sentiment split: +{item['sentiment_positive']} / "
            f"~{item['sentiment_neutral']} / -{item['sentiment_negative']}."
        ),
        (
            f"Evidence sample from {sample.get('channel') or 'the topic feed'}: "
            f"{sample.get('text') or 'No direct sample available.'}"
        ),
    ])
    summary = (
        f"{topic} has {item['mention_count']} mentions in the last {request.window} "
        f"with growth {item['growth_7d_pct']:+.1f}%."
    )
    return build_success(
        action="get_topic_detail",
        window=request.window,
        summary=summary,
        confidence="high" if item["mention_count"] > 0 else "medium",
        bullets=bullets,
        items=[item],
        source_endpoints=["/api/topics/detail"],
        caveat=None if item["mention_count"] > 0 else "Topic detail exists, but current evidence is thin in this window.",
    )


def get_topic_evidence(client, request: GetTopicEvidenceRequest) -> dict[str, Any]:
    payload = client.get_topic_evidence(
        request.topic,
        request.category,
        request.view,
        page=0,
        size=request.limit,
        focus_id=request.focus_id,
        window=request.window,
    )
    evidence_rows = list(payload.get("items") or [])
    evidence_items = [_compact_evidence_item(row) for row in evidence_rows[: request.limit]]
    response_items: list[dict[str, Any]] = list(evidence_items)
    meta_item = {
        "total": int(payload.get("total") or 0),
        "has_more": bool(payload.get("hasMore")),
    }
    focused = payload.get("focusedItem")
    if isinstance(focused, dict) and focused:
        meta_item["focused_item"] = _compact_evidence_item(focused)
    if meta_item["total"] or meta_item.get("focused_item") or meta_item["has_more"]:
        response_items.append(meta_item)
    bullets = _limit_bullets([
        f"{item['channel'] or 'Unknown channel'}: {item['text']}" for item in evidence_items[:3]
    ])
    label = "question evidence" if request.view == "questions" else "evidence items"
    summary = f"Showing {len(evidence_items)} {label} for {request.topic}."
    return build_success(
        action="get_topic_evidence",
        window=request.window,
        summary=summary,
        confidence="high" if evidence_items else "low_confidence",
        bullets=bullets or ["No evidence items were returned for this topic."],
        items=response_items,
        source_endpoints=["/api/topics/evidence"],
        caveat=None if evidence_items else "Try a different window or remove filters to find more direct evidence.",
    )


def get_freshness_status(client, request: GetFreshnessStatusRequest) -> dict[str, Any]:
    snapshot = client.get_freshness_status(force=request.force)
    item = _freshness_item(snapshot)
    status = item["health_status"]
    bullets = _limit_bullets([
        (
            f"Backlog: {item['unprocessed_posts']} unprocessed posts, "
            f"{item['unprocessed_comments']} unprocessed comments, "
            f"{item['unsynced_posts']} unsynced posts."
        ),
        f"Pipeline ages: scrape {item.get('scrape_age_minutes')}m, process {item.get('process_age_minutes')}m, sync {item.get('sync_age_minutes')}m.",
        f"Supabase-to-Neo4j latest post delta: {item.get('latest_post_delta_minutes')} minutes.",
    ])
    return build_success(
        action="get_freshness_status",
        window=None,
        summary=f"Data freshness is {status}.",
        confidence="high",
        bullets=bullets,
        items=[item],
        source_endpoints=["/api/freshness"],
    )


def get_declining_topics(client, request: GetDecliningTopicsRequest) -> dict[str, Any]:
    dashboard = _dashboard_data(client.get_dashboard(request.window))
    rows = list(dashboard.get("topicBubbles") or [])
    declining = [
        row for row in rows
        if float(row.get("growth7dPct") if row.get("growth7dPct") is not None else row.get("growth") or 0.0) < 0
    ]
    declining.sort(key=lambda item: float(item.get("growth7dPct") if item.get("growth7dPct") is not None else item.get("growth") or 0.0))
    declining = declining[: request.limit]

    items = [
        {
            "topic": row.get("name") or row.get("topic"),
            "category": row.get("category") or "General",
            "mention_count": int(row.get("mentionCount") or row.get("mentions7d") or 0),
            "growth_7d_pct": float(row.get("growth7dPct") if row.get("growth7dPct") is not None else row.get("growth") or 0.0),
            "growth_support": int(row.get("growthSupport") or 0),
        }
        for row in declining
    ]

    bullets = [
        f"{item['topic']}: {item['growth_7d_pct']:+.1f}% with support {item['growth_support']}."
        for item in items
    ]
    return build_success(
        action="get_declining_topics",
        window=request.window,
        summary=f"Found {len(items)} declining topics in the last {request.window}.",
        confidence="high" if items else "medium",
        bullets=bullets or ["No meaningful declines were detected in the selected window."],
        items=items,
        source_endpoints=["/api/dashboard"],
    )


def get_problem_spikes(client, request: GetProblemSpikesRequest) -> dict[str, Any]:
    dashboard = _dashboard_data(client.get_dashboard(request.window))
    problem_briefs = list(dashboard.get("problemBriefs") or [])
    raw_problems = list(dashboard.get("problems") or [])

    items: list[dict[str, Any]] = []
    if problem_briefs:
        ranked = sorted(
            problem_briefs,
            key=lambda row: (
                _severity_sort_key(row.get("severity")),
                int((row.get("demandSignals") or {}).get("messages") or 0),
                float((row.get("demandSignals") or {}).get("trend7dPct") or 0.0),
            ),
            reverse=True,
        )[:5]
        for row in ranked:
            items.append(
                {
                    "topic": row.get("topic"),
                    "category": row.get("category"),
                    "severity": row.get("severity"),
                    "problem": row.get("problemEn") or row.get("problem"),
                    "summary": row.get("summaryEn") or row.get("summary"),
                    "messages": int((row.get("demandSignals") or {}).get("messages") or 0),
                    "trend_7d_pct": float((row.get("demandSignals") or {}).get("trend7dPct") or 0.0),
                }
            )
    else:
        ranked = sorted(
            raw_problems,
            key=lambda row: (
                _severity_sort_key(row.get("severity")),
                int(row.get("affectedUsers") or 0),
                float(row.get("trendPct") or 0.0),
            ),
            reverse=True,
        )[:5]
        for row in ranked:
            items.append(
                {
                    "topic": row.get("topic") or row.get("name"),
                    "category": row.get("category"),
                    "severity": row.get("severity"),
                    "problem": row.get("sampleText") or "Problem spike detected",
                    "summary": row.get("sampleText") or "",
                    "messages": int(row.get("affectedUsers") or 0),
                    "trend_7d_pct": float(row.get("trendPct") or 0.0),
                }
            )

    bullets = [
        f"{item['topic']}: {item['severity']} severity, {item['messages']} signals, trend {item['trend_7d_pct']:+.1f}%."
        for item in items
    ]
    return build_success(
        action="get_problem_spikes",
        window=request.window,
        summary=f"Top problem spikes for the last {request.window}.",
        confidence="high" if items else "medium",
        bullets=bullets or ["No problem spikes were returned for this window."],
        items=items,
        source_endpoints=["/api/dashboard"],
    )


def get_question_clusters(client, request: GetQuestionClustersRequest) -> dict[str, Any]:
    dashboard = _dashboard_data(client.get_dashboard(request.window))
    rows = [
        row for row in list(dashboard.get("questionBriefs") or [])
        if _topic_matches(row.get("topic"), request.topic)
    ][:6]

    items = [
        {
            "topic": row.get("topic"),
            "category": row.get("category"),
            "question": row.get("canonicalQuestionEn") or row.get("questionEn") or row.get("question"),
            "summary": row.get("summaryEn") or row.get("summary"),
            "messages": int((row.get("demandSignals") or {}).get("messages") or 0),
            "unique_users": int((row.get("demandSignals") or {}).get("uniqueUsers") or 0),
            "channels": int((row.get("demandSignals") or {}).get("channels") or 0),
            "trend_7d_pct": float((row.get("demandSignals") or {}).get("trend7dPct") or 0.0),
            "evidence": [
                {
                    "quote": _trim_text(ev.get("quote"), 120),
                    "channel": ev.get("channel"),
                }
                for ev in list(row.get("evidence") or [])[:2]
            ],
        }
        for row in rows
    ]

    topic_suffix = f" for {request.topic}" if request.topic else ""
    bullets = [
        f"{item['topic']}: {item['question']} ({item['messages']} messages, trend {item['trend_7d_pct']:+.1f}%)."
        for item in items
    ]
    caveat = None if items else "No matching question clusters were found for the selected filters."
    return build_success(
        action="get_question_clusters",
        window=request.window,
        summary=f"Question clusters{topic_suffix} in the last {request.window}.",
        confidence="high" if items else "medium",
        bullets=bullets or ["No question clusters were returned for this query."],
        items=items,
        source_endpoints=["/api/dashboard"],
        caveat=caveat,
    )


def get_sentiment_overview(client, request: GetSentimentOverviewRequest) -> dict[str, Any]:
    dashboard = _dashboard_data(client.get_dashboard(request.window))
    health = dashboard.get("communityHealth") or {}
    health_score = int(health.get("score") or 0)
    health_trend = health.get("trend") or "flat"
    try:
        sentiments = list(client.get_sentiment_distribution(request.window) or [])
    except AnalyticsAPIError as exc:
        bullets = _limit_bullets([
            f"Sentiment distribution is temporarily unavailable for the last {request.window}.",
            f"Community health score is {health_score} with a {health_trend} trend.",
            f"{len(list(dashboard.get('urgencySignals') or []))} urgency signals remain visible in dashboard data.",
        ])
        return build_success(
            action="get_sentiment_overview",
            window=request.window,
            summary=f"Sentiment distribution is temporarily unavailable for the last {request.window}.",
            confidence="low_confidence",
            bullets=bullets,
            items=[
                {
                    "label": "Unavailable",
                    "count": 0,
                    "share_pct": 0.0,
                    "community_health_score": health_score,
                    "community_health_trend": health_trend,
                }
            ],
            source_endpoints=["/api/sentiment-distribution", "/api/dashboard"],
            caveat=f"Sentiment endpoint /api/sentiment-distribution is unavailable right now: {exc.message}",
        )

    total = sum(int(row.get("count") or 0) for row in sentiments)
    counts = {str(row.get("label") or "Unknown"): int(row.get("count") or 0) for row in sentiments}
    dominant_label = max(counts, key=counts.get) if counts else "Unknown"
    dominant_count = counts.get(dominant_label, 0)

    items = [
        {
            "label": label,
            "count": count,
            "share_pct": round((count / total) * 100.0, 1) if total else 0.0,
        }
        for label, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)
    ]
    bullets = []
    if total:
        bullets.append(f"Dominant sentiment is {dominant_label} at {round((dominant_count / total) * 100.0, 1)}% of classified messages.")
    bullets.append(f"Community health score is {health_score} with a {health_trend} trend.")
    if counts.get("Negative", 0) or counts.get("Urgent", 0):
        negative_total = counts.get("Negative", 0) + counts.get("Urgent", 0) + counts.get("Sarcastic", 0)
        bullets.append(f"Negative-pressure signals account for {round((negative_total / total) * 100.0, 1) if total else 0.0}% of classified messages.")

    return build_success(
        action="get_sentiment_overview",
        window=request.window,
        summary=f"Sentiment overview for the last {request.window}.",
        confidence="high" if total else "medium",
        bullets=bullets,
        items=items,
        source_endpoints=["/api/sentiment-distribution", "/api/dashboard"],
        caveat=None if total else "No sentiment distribution data was returned for the selected window.",
    )


def get_active_alerts(client, request: GetActiveAlertsRequest) -> dict[str, Any]:
    dashboard = _dashboard_data(client.get_dashboard())
    alerts = list(dashboard.get("urgencySignals") or [])
    items = [
        {
            "topic": row.get("topicEn") or row.get("topic") or "Unknown",
            "message": row.get("messageEn") or row.get("message") or "",
            "action": row.get("actionEn") or row.get("action") or "",
            "urgency": row.get("urgency") or "high",
        }
        for row in alerts
    ]
    items.sort(key=lambda row: _severity_sort_key(row.get("urgency")), reverse=True)

    bullets = [
        f"{item['urgency'].upper()}: {item['topic']} - {_trim_text(item['message'], 120)}"
        for item in items[:6]
    ]
    return build_success(
        action="get_active_alerts",
        window=None,
        summary="Current active alerts." if items else "No active alerts.",
        confidence="high" if items else "medium",
        bullets=bullets or ["No urgent community alerts are active right now."],
        items=items,
        source_endpoints=["/api/dashboard"],
    )


def _search_match(items: list[dict[str, Any]], *preferred_types: str) -> dict[str, Any] | None:
    for preferred_type in preferred_types:
        for item in items:
            if _normalize_text(item.get("type")) == preferred_type and _clean_text(item.get("name")):
                return item
    return None


def _confirmed_search_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        item for item in items
        if _normalize_text(item.get("type")) not in {"topic_hint", "channel_hint", "category_hint"}
    ]


def _search_hint_candidates(items: list[dict[str, Any]], *, limit: int = 2) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for item in items[:limit]:
        if _normalize_text(item.get("type")) != "topic_hint":
            continue
        topic = _clean_text(item.get("name"))
        if not topic:
            continue
        candidates.append(
            {
                "source": "alias_hint",
                "topic": topic,
                "summary": "Closest local topic interpretation from the question wording.",
                "detail": _clean_text(item.get("text")) or "No exact backend entity matched yet for this wording in the selected window.",
                "evidence_count": 0,
            }
        )
    return candidates


def get_graph_snapshot(client, request: GetGraphSnapshotRequest) -> dict[str, Any]:
    graph = client.get_graph_data(
        request.window,
        category=request.category,
        signal_focus=request.signal_focus,
        max_nodes=request.max_nodes,
    )
    insights = client.get_graph_insights(request.window)
    top_channels = list(client.get_top_channels(limit=5, window=request.window) or [])
    trending_topics = list(client.get_trending_topics(limit=5, window=request.window) or [])
    freshness = client.get_freshness_status(False)
    item = _graph_snapshot_item(graph, insights, top_channels, trending_topics)
    bullets = _limit_bullets([
        (
            "Top categories: " + ", ".join(
                f"{row.get('name')} ({int(row.get('mention_count') or 0)})" for row in item.get("top_categories") or []
            ) + "."
            if item.get("top_categories")
            else "No graph categories were returned for this window."
        ),
        (
            "Leading topics: " + ", ".join(
                f"{row.get('name')} ({int(row.get('mention_count') or 0)})" for row in item.get("leading_topics") or []
            ) + "."
            if item.get("leading_topics")
            else "No leading topics were returned for this window."
        ),
        (
            "Leading channels: " + ", ".join(
                f"{row.get('name')} ({int(row.get('mention_count') or 0)})" for row in item.get("leading_channels") or []
            ) + "."
            if item.get("leading_channels")
            else "No leading channels were returned for this window."
        ),
    ])
    summary = item.get("insight") or (
        f"The graph currently shows {item['topic_count']} topics across {item['category_count']} categories "
        f"and {item['channel_count']} visible channels."
    )
    return build_success(
        action="get_graph_snapshot",
        window=request.window,
        summary=summary,
        confidence="high" if item["topic_count"] > 0 else "medium",
        bullets=bullets,
        items=[item],
        source_endpoints=[
            "/api/graph",
            "/api/graph-insights",
            "/api/top-channels",
            "/api/trending-topics",
            "/api/freshness",
        ],
        caveat=_freshness_caveat(freshness),
    )


def get_node_context(client, request: GetNodeContextRequest) -> dict[str, Any]:
    source_endpoints: list[str] = []
    candidate_name = request.entity
    candidate_id: str | None = None
    candidate_type = request.type

    if request.type == "auto" or request.type == "channel":
        search_payload = search_entities(client, SearchEntitiesRequest(query=request.entity, limit=5))
        source_endpoints.append("/api/search")
        candidates = list(search_payload.get("items") or [])
        chosen = _search_match(candidates, request.type if request.type != "auto" else "topic", "category", "channel")
        if request.type == "auto":
            chosen = _search_match(candidates, "topic", "category", "channel")
        if request.type == "channel":
            chosen = _search_match(candidates, "channel")
        if chosen:
            candidate_name = _clean_text(chosen.get("name")) or request.entity
            candidate_id = _clean_text(chosen.get("id"))
            candidate_type = _normalize_text(chosen.get("type")) or request.type
        elif request.type in {"auto", "channel"}:
            return build_success(
                action="get_node_context",
                window=request.window,
                summary=f'No graph node matched "{request.entity}" clearly enough.',
                confidence="low_confidence",
                bullets=["No matching graph node was found."],
                items=candidates[:3],
                source_endpoints=source_endpoints,
                caveat="Try the exact topic, category, or channel title.",
            )

    if request.type in {"topic", "category"}:
        candidate_type = request.type
        candidate_id = f"{request.type}:{request.entity}"

    if not candidate_id:
        candidate_id = f"{candidate_type}:{candidate_name}" if candidate_type in {"topic", "category", "channel"} else request.entity

    detail = client.get_node_details(candidate_id, candidate_type, request.window)
    item = _node_context_item(detail)
    evidence_rows = list(detail.get("evidence") or detail.get("questionEvidence") or [])[:2]
    evidence_items = [_compact_evidence_item(row) for row in evidence_rows]

    if _normalize_text(detail.get("type")) == "category":
        bullets = _limit_bullets([
            f"{detail.get('name')} includes {int(detail.get('topicCount') or 0)} scoped topics and {int(detail.get('mentionCount') or 0)} mentions.",
            (
                "Top topics: " + ", ".join(
                    f"{row.get('name')} ({int(row.get('mentions') or 0)})" for row in list(detail.get("topTopics") or [])[:3]
                ) + "."
                if detail.get("topTopics")
                else "Top topics are limited in this window."
            ),
            (
                "Top channels: " + ", ".join(
                    f"{row.get('name')} ({int(row.get('mentions') or 0)})" for row in list(detail.get("topChannels") or [])[:3]
                ) + "."
                if detail.get("topChannels")
                else "Top channels are limited in this window."
            ),
        ])
        summary = f"{detail.get('name')} is a category-level graph context with {int(detail.get('topicCount') or 0)} active topics."
    elif _normalize_text(detail.get("type")) == "channel":
        bullets = _limit_bullets([
            f"{detail.get('name')} has {int(detail.get('postCount') or 0)} broadcast posts in this scoped graph window.",
            (
                "Key topics: " + ", ".join(
                    row.get("name") for row in list(detail.get("topics") or [])[:4] if row.get("name")
                ) + "."
                if detail.get("topics")
                else "No topic list was returned for this channel node."
            ),
            (
                "Top categories: " + ", ".join(
                    row.get("name") for row in list(detail.get("categories") or [])[:4] if row.get("name")
                ) + "."
                if detail.get("categories")
                else "No category breakdown was returned for this channel node."
            ),
        ])
        summary = f"{detail.get('name')} is active in the graph with {int(detail.get('postCount') or 0)} scoped posts."
    else:
        overview = detail.get("overview") if isinstance(detail.get("overview"), dict) else {}
        bullets = _limit_bullets([
            f"{detail.get('name')} has {int(detail.get('mentionCount') or 0)} mentions and trend {float(detail.get('trendPct') or 0.0):+.1f}%.",
            (
                "Top channels: " + ", ".join(
                    row.get("name") for row in list(detail.get("topChannels") or [])[:3] if row.get("name")
                ) + "."
                if detail.get("topChannels")
                else "Top channels are limited in this window."
            ),
            (
                _trim_text(overview.get("summaryEn"), 140)
                if overview.get("summaryEn")
                else (
                    f"Evidence sample: {evidence_items[0]['channel'] or 'Unknown source'} - {evidence_items[0]['text']}"
                    if evidence_items
                    else "No direct evidence was returned for this node."
                )
            ),
        ])
        summary = f"{detail.get('name')} is the closest graph node for this request."

    return build_success(
        action="get_node_context",
        window=request.window,
        summary=summary,
        confidence="high" if item else "medium",
        bullets=bullets,
        items=[item] + evidence_items,
        source_endpoints=source_endpoints + ["/api/node-details"],
        caveat=(
            "Channel graph context is structural; use investigate_channel for recent post evidence."
            if _normalize_text(detail.get("type")) == "channel"
            else None
        ),
    )


def investigate_channel(client, request: InvestigateChannelRequest) -> dict[str, Any]:
    detail = client.get_channel_detail(request.channel, request.window)
    posts_payload = client.get_channel_posts(request.channel, limit=3, page=0, window=request.window)
    freshness = client.get_freshness_status(False)
    channel_item = _channel_detail_item(detail)
    recent_posts = [
        _compact_evidence_item({**row, "type": "post"})
        for row in list(posts_payload.get("items") or [])[:3]
    ]
    top_topics = [row.get("name") for row in channel_item.get("top_topics") or [] if row.get("name")]
    bullets = _limit_bullets([
        (
            f"Top topics: {', '.join(top_topics)}."
            if top_topics
            else "Top topics are limited in this window."
        ),
        (
            f"Recent post: {recent_posts[0]['channel'] or channel_item['channel']} - {recent_posts[0]['text']}"
            if recent_posts
            else "Recent post evidence is thin for this channel."
        ),
        (
            f"Sentiment: +{channel_item['sentiment_positive']} / "
            f"~{channel_item['sentiment_neutral']} / -{channel_item['sentiment_negative']}."
        ),
    ])
    caveat = _freshness_caveat(freshness)
    if not recent_posts:
        caveat = (
            f"{caveat} Recent posts are thin for this channel."
            if caveat
            else "Recent posts are thin for this channel."
        )
    return build_success(
        action="investigate_channel",
        window=request.window,
        summary=(
            f"{channel_item['channel']} has {channel_item['post_count']} posts in the last {request.window}, "
            f"averaging {channel_item['daily_messages']} messages per day with growth {channel_item['growth_7d_pct']:+.1f}%."
        ),
        confidence="high" if channel_item["post_count"] > 0 and recent_posts else "medium",
        bullets=bullets,
        items=[channel_item] + recent_posts,
        source_endpoints=["/api/channels/detail", "/api/channels/posts", "/api/freshness"],
        caveat=caveat,
    )


def compare_topics(client, request: CompareTopicsRequest) -> dict[str, Any]:
    detail_a = _topic_detail_item(client.get_topic_detail(request.topic_a, None, request.window))
    detail_b = _topic_detail_item(client.get_topic_detail(request.topic_b, None, request.window))
    larger = detail_a if detail_a["mention_count"] >= detail_b["mention_count"] else detail_b
    faster = detail_a if detail_a["growth_7d_pct"] >= detail_b["growth_7d_pct"] else detail_b
    more_negative = detail_a if detail_a["sentiment_negative"] >= detail_b["sentiment_negative"] else detail_b
    comparison = {
        "topic_a": detail_a["topic"],
        "topic_b": detail_b["topic"],
        "larger_by_mentions": larger["topic"],
        "faster_growth": faster["topic"],
        "more_negative_sentiment": more_negative["topic"],
        "mention_gap": abs(detail_a["mention_count"] - detail_b["mention_count"]),
        "growth_gap_pct": round(abs(detail_a["growth_7d_pct"] - detail_b["growth_7d_pct"]), 1),
    }
    bullets = _limit_bullets([
        f"{detail_a['topic']}: {detail_a['mention_count']} mentions, growth {detail_a['growth_7d_pct']:+.1f}%, top channels {', '.join(detail_a['top_channels']) or 'n/a'}.",
        f"{detail_b['topic']}: {detail_b['mention_count']} mentions, growth {detail_b['growth_7d_pct']:+.1f}%, top channels {', '.join(detail_b['top_channels']) or 'n/a'}.",
        f"{more_negative['topic']} carries the heavier negative-pressure mix right now.",
    ])
    return build_success(
        action="compare_topics",
        window=request.window,
        summary=(
            f"{larger['topic']} is larger by volume right now, while {faster['topic']} is moving faster "
            f"over the last {request.window}."
        ),
        confidence="high",
        bullets=bullets,
        items=[comparison, detail_a, detail_b],
        source_endpoints=["/api/topics/detail"],
    )


def compare_channels(client, request: CompareChannelsRequest) -> dict[str, Any]:
    detail_a = _channel_detail_item(client.get_channel_detail(request.channel_a, request.window))
    detail_b = _channel_detail_item(client.get_channel_detail(request.channel_b, request.window))
    larger = detail_a if detail_a["post_count"] >= detail_b["post_count"] else detail_b
    stronger = detail_a if detail_a["avg_views"] >= detail_b["avg_views"] else detail_b
    faster = detail_a if detail_a["growth_7d_pct"] >= detail_b["growth_7d_pct"] else detail_b
    comparison = {
        "channel_a": detail_a["channel"],
        "channel_b": detail_b["channel"],
        "higher_volume": larger["channel"],
        "higher_engagement_proxy": stronger["channel"],
        "faster_growth": faster["channel"],
        "post_gap": abs(detail_a["post_count"] - detail_b["post_count"]),
        "view_gap": abs(detail_a["avg_views"] - detail_b["avg_views"]),
    }
    topics_a = [row.get("name") for row in detail_a["top_topics"] if row.get("name")]
    topics_b = [row.get("name") for row in detail_b["top_topics"] if row.get("name")]
    bullets = _limit_bullets([
        f"{detail_a['channel']}: {detail_a['post_count']} posts, avg views {detail_a['avg_views']}, top topics {', '.join(topics_a) or 'n/a'}.",
        f"{detail_b['channel']}: {detail_b['post_count']} posts, avg views {detail_b['avg_views']}, top topics {', '.join(topics_b) or 'n/a'}.",
        f"{stronger['channel']} has the stronger engagement proxy, while {faster['channel']} is growing faster.",
    ])
    return build_success(
        action="compare_channels",
        window=request.window,
        summary=(
            f"{larger['channel']} is larger by recent post volume, while {stronger['channel']} has the stronger view proxy."
        ),
        confidence="high",
        bullets=bullets,
        items=[comparison, detail_a, detail_b],
        source_endpoints=["/api/channels/detail"],
    )


def ask_insights(client, request: AskInsightsRequest) -> dict[str, Any]:
    dashboard = _dashboard_data(client.get_dashboard(request.window))
    insight_cards = client.get_insight_cards(request.window)
    question_tokens = _tokenize(request.question)

    candidates = _build_insight_candidates(dashboard, insight_cards)
    ranked = _rank_candidates(question_tokens, request.question, candidates)

    if not ranked:
        return _low_confidence_response(
            request,
            items=[],
            bullets=["Top current signals are still included below for context."],
        )

    if _has_conflicting_support(ranked):
        return _low_confidence_response(
            request,
            items=[_candidate_item(entry["candidate"]) for entry in ranked[:3]],
            bullets=[_candidate_to_bullet(entry["candidate"]) for entry in ranked[:2]],
            caveat="Relevant signals point in different directions, so the skill did not force a single answer.",
        )

    if not _has_sufficient_support(question_tokens, ranked):
        return _low_confidence_response(
            request,
            items=[_candidate_item(entry["candidate"]) for entry in ranked[:3]],
            bullets=[_candidate_to_bullet(entry["candidate"]) for entry in ranked[:2]],
        )

    top_entries = ranked[:4]
    items = [_candidate_item(entry["candidate"]) for entry in top_entries]
    bullets = [_candidate_to_bullet(entry["candidate"]) for entry in top_entries[:3]]
    summary = _build_summary(request.question, top_entries)
    confidence = "high" if ranked[0]["score"] >= 5 and sum(entry["score"] for entry in top_entries[:3]) >= 10 else "medium"
    caveat = None
    if len({entry["candidate"]["source"] for entry in top_entries[:3]}) < 2:
        caveat = "This answer is grounded in a narrow slice of evidence and should be monitored against fresh data."

    return build_success(
        action="ask_insights",
        window=request.window,
        summary=summary,
        confidence=confidence,
        bullets=bullets,
        items=items,
        source_endpoints=["/api/dashboard", "/api/insights/cards"],
        caveat=caveat,
    )


def investigate_topic(client, request: InvestigateTopicRequest) -> dict[str, Any]:
    detail = client.get_topic_detail(request.topic, request.category, request.window)
    evidence_payload = client.get_topic_evidence(
        request.topic,
        request.category,
        "all",
        page=0,
        size=3,
        window=request.window,
    )
    freshness = client.get_freshness_status(False)

    topic_item = _topic_detail_item(detail)
    evidence_items = [_compact_evidence_item(row) for row in list(evidence_payload.get("items") or [])[:3]]
    topic = topic_item["topic"] or request.topic
    summary = (
        f"{topic} is active in the last {request.window} with {topic_item['mention_count']} mentions "
        f"and growth {topic_item['growth_7d_pct']:+.1f}%."
    )
    bullets = _limit_bullets([
        (
            f"Top channels: {', '.join(topic_item['top_channels'])}."
            if topic_item["top_channels"]
            else "Top channels are limited in this window."
        ),
        (
            f"Evidence: {evidence_items[0]['channel'] or 'Unknown channel'} - {evidence_items[0]['text']}"
            if evidence_items
            else "No direct evidence rows were returned for this topic."
        ),
        (
            f"Sentiment: +{topic_item['sentiment_positive']} / "
            f"~{topic_item['sentiment_neutral']} / -{topic_item['sentiment_negative']}."
        ),
    ])
    caveat = _freshness_caveat(freshness)
    if not evidence_items:
        caveat = (
            f"{caveat} Evidence rows are currently thin for this topic."
            if caveat
            else "Evidence rows are currently thin for this topic."
        )
    confidence = "high" if topic_item["mention_count"] > 0 and evidence_items else "medium"
    if topic_item["mention_count"] <= 0:
        confidence = "low_confidence"
    return build_success(
        action="investigate_topic",
        window=request.window,
        summary=summary,
        confidence=confidence,
        bullets=bullets,
        items=[topic_item] + evidence_items,
        source_endpoints=["/api/topics/detail", "/api/topics/evidence", "/api/freshness"],
        caveat=caveat,
    )


def investigate_question(client, request: InvestigateQuestionRequest) -> dict[str, Any]:
    if _is_graph_wide_question(request.question):
        try:
            payload = _relabel_action(
                get_graph_snapshot(client, GetGraphSnapshotRequest(window=request.window)),
                "investigate_question",
            )
            _log_resolution_outcome(request.question, tried_terms=[], outcome="graph_snapshot")
            return payload
        except AnalyticsAPIError:
            pass

    if _is_channel_question(request.question):
        tried_channel_terms: list[str] = []
        for channel_search_term in _prioritized_resolution_terms({"items": []}, request.question):
            tried_channel_terms.append(channel_search_term)
            search_payload = search_entities(client, SearchEntitiesRequest(query=channel_search_term, limit=5))
            channel_candidates = _confirmed_search_items(list(search_payload.get("items") or []))
            channel_candidate = _search_match(channel_candidates, "channel")
            if not channel_candidate:
                continue
            try:
                payload = _relabel_action(
                    investigate_channel(
                        client,
                        InvestigateChannelRequest(window=request.window, channel=str(channel_candidate.get("name"))),
                    ),
                    "investigate_question",
                )
                _log_resolution_outcome(
                    request.question,
                    tried_terms=tried_channel_terms,
                    outcome="channel_search_match",
                    resolved_name=_clean_text(channel_candidate.get("name")),
                )
                return payload
            except AnalyticsAPIError as exc:
                if exc.error_type != "not_found":
                    raise

    base = ask_insights(client, AskInsightsRequest(window=request.window, question=request.question))
    base_confidence = _normalize_text(base.get("confidence"))
    topic = _extract_primary_topic(base)

    if topic and base_confidence in {"high", "medium"}:
        try:
            detail = client.get_topic_detail(topic, None, request.window)
            freshness = client.get_freshness_status(False)
            detail_item = _topic_detail_item(detail)
            sample = detail_item.get("sample_evidence") if isinstance(detail_item.get("sample_evidence"), dict) else {}
            bullets = _limit_bullets([
                base.get("summary") or "",
                (
                    f"Top channels: {', '.join(detail_item['top_channels'])}."
                    if detail_item["top_channels"]
                    else "Top channels are limited in this window."
                ),
                (
                    f"Evidence sample: {sample.get('channel') or 'Unknown source'} - "
                    f"{sample.get('text') or 'No direct sample available.'}"
                ),
            ])
            caveat = _freshness_caveat(freshness)
            payload = build_success(
                action="investigate_question",
                window=request.window,
                summary=base.get("summary") or f"{topic} appears most relevant to this question.",
                confidence=base.get("confidence") or "medium",
                bullets=bullets,
                items=[detail_item],
                source_endpoints=["/api/dashboard", "/api/insights/cards", "/api/topics/detail", "/api/freshness"],
                caveat=caveat,
            )
            _log_resolution_outcome(
                request.question,
                tried_terms=[topic],
                outcome="direct_insight_match",
                resolved_name=detail_item["topic"],
            )
            return payload
        except AnalyticsAPIError as exc:
            if exc.error_type != "not_found":
                raise

    search_used = False
    tried_search_terms: list[str] = []
    candidates: list[dict[str, Any]] = []

    for search_term in _prioritized_resolution_terms(base, request.question, attempted_topic=topic):
        search_used = True
        tried_search_terms.append(search_term)
        search_payload = search_entities(client, SearchEntitiesRequest(query=search_term, limit=5))
        search_candidates = list(search_payload.get("items") or [])
        candidates = _merge_search_items(candidates, search_candidates)
        confirmed_candidates = _confirmed_search_items(search_candidates)

        topic_candidate = _search_match(confirmed_candidates, "topic")
        if topic_candidate:
            try:
                detail = client.get_topic_detail(str(topic_candidate.get("name")), None, request.window)
                detail_item = _topic_detail_item(detail)
                sample = detail_item.get("sample_evidence") if isinstance(detail_item.get("sample_evidence"), dict) else {}
                bullets = _limit_bullets([
                    f"Topic match: {detail_item['topic']}.",
                    (
                        f"Top channels: {', '.join(detail_item['top_channels'])}."
                        if detail_item["top_channels"]
                        else "Top channels are limited in this window."
                    ),
                    (
                        f"Evidence sample: {sample.get('channel') or 'Unknown source'} - "
                        f"{sample.get('text') or 'No direct sample available.'}"
                    ),
                ])
                payload = build_success(
                    action="investigate_question",
                    window=request.window,
                    summary=f"{detail_item['topic']} looks like the closest evidence-backed match to this question.",
                    confidence="medium" if detail_item["mention_count"] > 0 else "low_confidence",
                    bullets=bullets,
                    items=[detail_item],
                    source_endpoints=["/api/dashboard", "/api/insights/cards", "/api/search", "/api/topics/detail"],
                )
                _log_resolution_outcome(
                    request.question,
                    tried_terms=tried_search_terms,
                    outcome="search_topic_match",
                    resolved_name=detail_item["topic"],
                )
                return payload
            except AnalyticsAPIError as exc:
                if exc.error_type != "not_found":
                    raise

        channel_candidate = _search_match(confirmed_candidates, "channel")
        if channel_candidate:
            try:
                payload = _relabel_action(
                    investigate_channel(
                        client,
                        InvestigateChannelRequest(window=request.window, channel=str(channel_candidate.get("name"))),
                    ),
                    "investigate_question",
                )
                _log_resolution_outcome(
                    request.question,
                    tried_terms=tried_search_terms,
                    outcome="search_channel_match",
                    resolved_name=_clean_text(channel_candidate.get("name")),
                )
                return payload
            except AnalyticsAPIError as exc:
                if exc.error_type != "not_found":
                    raise

        category_candidate = _search_match(confirmed_candidates, "category")
        if category_candidate:
            try:
                payload = _relabel_action(
                    get_node_context(
                        client,
                        GetNodeContextRequest(
                            window=request.window,
                            entity=str(category_candidate.get("name")),
                            type="category",
                        ),
                    ),
                    "investigate_question",
                )
                _log_resolution_outcome(
                    request.question,
                    tried_terms=tried_search_terms,
                    outcome="search_category_match",
                    resolved_name=_clean_text(category_candidate.get("name")),
                )
                return payload
            except AnalyticsAPIError as exc:
                if exc.error_type != "not_found":
                    raise

    confirmed_candidates = _confirmed_search_items(candidates)

    if _is_graph_wide_question(request.question):
        try:
            payload = _relabel_action(
                get_graph_snapshot(client, GetGraphSnapshotRequest(window=request.window)),
                "investigate_question",
            )
            _log_resolution_outcome(
                request.question,
                tried_terms=tried_search_terms,
                outcome="graph_snapshot_fallback",
            )
            return payload
        except AnalyticsAPIError:
            pass

    fallback_items = confirmed_candidates[:3] or _candidate_items(base)
    if not fallback_items:
        fallback_items = [_candidate_item(candidate) for candidate in _alias_hint_candidates(request.question, limit=2)]
    if not fallback_items:
        fallback_items = [_candidate_item(candidate) for candidate in _search_hint_candidates(candidates, limit=2)]
    fallback_bullets = (
        _limit_bullets([f"{item.get('type')}: {item.get('name')}" for item in confirmed_candidates[:3] if item.get("name")])
        if confirmed_candidates
        else _candidate_bullets(fallback_items)
    )
    fallback_summary = base.get("summary")
    outcome = "candidate_fallback"
    if not fallback_summary:
        if tried_search_terms:
            searched_using = ", ".join(tried_search_terms[:2])
            fallback_summary = (
                f'The question did not resolve to a single exact topic. Closest matches were searched using "{searched_using}".'
            )
        else:
            fallback_summary = "The question did not resolve to a single exact topic right now."
    if not confirmed_candidates and fallback_items:
        hinted_topic = _clean_text(fallback_items[0].get("topic"))
        if hinted_topic:
            fallback_summary = (
                f"{hinted_topic} is the closest local interpretation of this question, "
                "but current backend evidence is too thin to answer confidently."
            )
            outcome = "alias_fallback"
    payload = build_success(
        action="investigate_question",
        window=request.window,
        summary=fallback_summary,
        confidence="low_confidence",
        bullets=fallback_bullets or ["No clear topic candidate was found for this question."],
        items=fallback_items,
        source_endpoints=["/api/dashboard", "/api/insights/cards"] + (["/api/search"] if search_used else []),
        caveat="The question did not resolve cleanly to a single exact topic, so the skill returned the closest candidate signals instead of forcing a deeper lookup.",
        suggested_follow_up="Try naming a topic directly, such as residency permits, rental costs, or school admissions.",
    )
    _log_resolution_outcome(
        request.question,
        tried_terms=tried_search_terms,
        outcome=outcome,
        resolved_name=_clean_text(fallback_items[0].get("topic") or fallback_items[0].get("name")) if fallback_items else None,
    )
    return payload


def _low_confidence_response(
    request: AskInsightsRequest,
    *,
    items: list[dict[str, Any]],
    bullets: list[str],
    caveat: str | None = None,
) -> dict[str, Any]:
    return build_success(
        action="ask_insights",
        window=request.window,
        summary="Evidence is too limited to answer that confidently right now.",
        confidence="low_confidence",
        bullets=bullets,
        items=items,
        source_endpoints=["/api/dashboard", "/api/insights/cards"],
        caveat=caveat or "The current backend evidence does not strongly match the question, so the skill did not infer an answer.",
        suggested_follow_up="Try a narrower question with a topic, timeframe, or audience segment.",
    )


def _build_insight_candidates(dashboard: dict[str, Any], insight_cards: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    for row in list(insight_cards.get("cards") or []):
        candidates.append(
            {
                "source": "insight_card",
                "topic": row.get("title") or "Insight",
                "summary": row.get("summary") or row.get("why_it_matters") or "",
                "detail": row.get("why_it_matters") or "",
                "evidence_count": len(row.get("evidence") or []),
            }
        )

    for row in list(dashboard.get("questionBriefs") or []):
        candidates.append(
            {
                "source": "question_brief",
                "topic": row.get("topic"),
                "summary": row.get("canonicalQuestionEn") or row.get("questionEn") or "",
                "detail": row.get("summaryEn") or "",
                "messages": int((row.get("demandSignals") or {}).get("messages") or 0),
                "trend": float((row.get("demandSignals") or {}).get("trend7dPct") or 0.0),
                "evidence_count": len(row.get("evidence") or []),
            }
        )

    for row in list(dashboard.get("problemBriefs") or []):
        candidates.append(
            {
                "source": "problem_brief",
                "topic": row.get("topic"),
                "summary": row.get("problemEn") or row.get("problem"),
                "detail": row.get("summaryEn") or "",
                "severity": row.get("severity") or "medium",
                "messages": int((row.get("demandSignals") or {}).get("messages") or 0),
                "trend": float((row.get("demandSignals") or {}).get("trend7dPct") or 0.0),
                "evidence_count": len(row.get("evidence") or []),
            }
        )

    for row in list(dashboard.get("urgencySignals") or []):
        candidates.append(
            {
                "source": "urgency_signal",
                "topic": row.get("topicEn") or row.get("topic") or "Urgent issue",
                "summary": row.get("messageEn") or row.get("message") or "",
                "detail": row.get("actionEn") or row.get("action") or "",
                "severity": row.get("urgency") or "high",
                "evidence_count": 2,
            }
        )

    for row in list(dashboard.get("trendingTopics") or []):
        candidates.append(
            {
                "source": "trending_topic",
                "topic": row.get("name") or row.get("topic"),
                "summary": f"{int(row.get('mentions') or 0)} mentions",
                "detail": row.get("sampleQuote") or "",
                "trend": float(row.get("trendPct") or 0.0),
                "evidence_count": 1,
            }
        )

    return candidates


def _rank_candidates(question_tokens: list[str], question: str, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_question = _normalize_text(question)
    alias_terms = {_normalize_text(term) for term in _alias_terms_for_text(question)}
    ranked: list[dict[str, Any]] = []
    for candidate in candidates:
        topic_norm = _normalize_text(candidate.get("topic"))
        corpus = " ".join(
            part for part in [
                candidate.get("topic"),
                candidate.get("summary"),
                candidate.get("detail"),
            ] if part
        )
        corpus_norm = _normalize_text(corpus)
        corpus_tokens = set(_tokenize(corpus))
        overlap = len(corpus_tokens.intersection(question_tokens))
        score = overlap
        if normalized_question and normalized_question in corpus_norm:
            score += 4
        topic_tokens = set(_tokenize(candidate.get("topic")))
        topic_overlap = len(topic_tokens.intersection(question_tokens))
        if topic_overlap:
            score += 2 + min(topic_overlap, 2)
        exact_topic_match = bool(topic_norm and topic_norm in normalized_question)
        if exact_topic_match:
            score += 4
        alias_match = bool(topic_norm and any(alias and (alias == topic_norm or alias in topic_norm or topic_norm in alias) for alias in alias_terms))
        if alias_match:
            score += 2
        if not (normalized_question and normalized_question in corpus_norm) and not exact_topic_match and not topic_overlap and not alias_match and overlap < 2:
            continue
        if int(candidate.get("evidence_count") or 0) >= 2:
            score += 1
        if int(candidate.get("messages") or 0) >= 10:
            score += 1
        if score <= 0:
            continue
        score += SOURCE_PRIORITY.get(str(candidate.get("source") or ""), 0)
        ranked.append({"candidate": candidate, "score": score})
    ranked.sort(
        key=lambda item: (
            item["score"],
            int(item["candidate"].get("evidence_count") or 0),
            _severity_sort_key(item["candidate"].get("severity")),
            _normalize_text(item["candidate"].get("topic")),
        ),
        reverse=True,
    )
    return ranked


def _has_sufficient_support(question_tokens: list[str], ranked: list[dict[str, Any]]) -> bool:
    if len(question_tokens) < 1 or not ranked:
        return False
    top_score = ranked[0]["score"]
    total_score = sum(item["score"] for item in ranked[:3])
    evidence = sum(int(item["candidate"].get("evidence_count") or 0) for item in ranked[:2])
    return top_score >= 5 and total_score >= 8 and evidence >= 2


def _has_conflicting_support(ranked: list[dict[str, Any]]) -> bool:
    if len(ranked) < 2:
        return False
    first = ranked[0]
    second = ranked[1]
    first_topic = _normalize_text(first["candidate"].get("topic"))
    second_topic = _normalize_text(second["candidate"].get("topic"))
    if not first_topic or not second_topic or first_topic == second_topic:
        return False
    return abs(first["score"] - second["score"]) <= 1 and len(ranked) >= 3


def _candidate_item(candidate: dict[str, Any]) -> dict[str, Any]:
    item = {
        "source": candidate.get("source"),
        "topic": candidate.get("topic"),
        "summary": _trim_text(candidate.get("summary"), 180),
        "detail": _trim_text(candidate.get("detail"), 180),
        "evidence_count": int(candidate.get("evidence_count") or 0),
    }
    if candidate.get("messages") is not None:
        item["messages"] = int(candidate.get("messages") or 0)
    if candidate.get("trend") is not None:
        item["trend"] = float(candidate.get("trend") or 0.0)
    if candidate.get("severity") is not None:
        item["severity"] = candidate.get("severity")
    return item


def _candidate_to_bullet(candidate: dict[str, Any]) -> str:
    source = candidate.get("source")
    topic = candidate.get("topic") or "Signal"
    if source == "question_brief":
        return f"{topic}: repeated question cluster with {_trim_text(candidate.get('summary'), 110)}."
    if source == "problem_brief":
        return f"{topic}: {_trim_text(candidate.get('summary'), 110)}."
    if source == "urgency_signal":
        return f"{topic}: urgent signal - {_trim_text(candidate.get('summary'), 110)}."
    if source == "trending_topic":
        trend = float(candidate.get("trend") or 0.0)
        return f"{topic}: high discussion volume with trend {trend:+.1f}%."
    return f"{topic}: {_trim_text(candidate.get('summary'), 110)}."


def _build_summary(question: str, top_entries: list[dict[str, Any]]) -> str:
    lead = top_entries[0]["candidate"]
    topic = lead.get("topic") or "current signals"
    summary = _trim_text(lead.get("summary"), 140)
    if summary:
        return f"{topic} appears most relevant to '{question}' based on the strongest matching evidence: {summary}"
    return f"{topic} appears most relevant to '{question}' based on the strongest matching evidence."
