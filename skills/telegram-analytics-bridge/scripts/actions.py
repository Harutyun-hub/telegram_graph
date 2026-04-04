from __future__ import annotations

import re
from typing import Any

from client import AnalyticsAPIError
from formatters import build_success
from models import (
    AskInsightsRequest,
    GetFreshnessStatusRequest,
    GetActiveAlertsRequest,
    GetDecliningTopicsRequest,
    GetProblemSpikesRequest,
    GetQuestionClustersRequest,
    GetSentimentOverviewRequest,
    GetTopicDetailRequest,
    GetTopicEvidenceRequest,
    GetTopTopicsRequest,
    InvestigateQuestionRequest,
    InvestigateTopicRequest,
    SearchEntitiesRequest,
)


STOPWORDS = {
    "a", "an", "and", "are", "about", "at", "be", "by", "for", "from", "how", "in",
    "is", "it", "main", "of", "on", "or", "the", "to", "what", "which", "who", "why",
    "with", "driving", "drive", "issue", "issues", "problem", "problems", "trend", "trends",
}
TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")
SEVERITY_RANK = {"critical": 4, "high": 3, "urgent": 3, "medium": 2, "low": 1}


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

    keyword_terms = [token for token in _tokenize(question) if len(token) >= 4]
    terms.extend(keyword_terms)
    return _dedupe_texts(terms)


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


def search_entities(client, request: SearchEntitiesRequest) -> dict[str, Any]:
    rows = list(client.search_entities(request.query, request.limit) or [])[: request.limit]
    items = [
        {
            "type": row.get("type"),
            "id": row.get("id"),
            "name": row.get("name"),
            "text": _trim_text(row.get("text"), 120),
        }
        for row in rows
    ]
    bullets = _limit_bullets([
        f"{item['type']}: {item['name']}" for item in items if item.get("name")
    ])
    confidence = "low_confidence"
    if items:
        confidence = "high" if _normalize_text(items[0].get("type")) == "topic" else "medium"
    return build_success(
        action="search_entities",
        window=None,
        summary=f'Found {len(items)} matching entities for "{request.query}".',
        confidence=confidence,
        bullets=bullets or ["No matching entities were found."],
        items=items,
        source_endpoints=["/api/search"],
        caveat=None if items else "Try a narrower topic or keyword.",
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
    sentiments = list(client.get_sentiment_distribution(request.window) or [])
    dashboard = _dashboard_data(client.get_dashboard(request.window))
    total = sum(int(row.get("count") or 0) for row in sentiments)
    counts = {str(row.get("label") or "Unknown"): int(row.get("count") or 0) for row in sentiments}
    dominant_label = max(counts, key=counts.get) if counts else "Unknown"
    dominant_count = counts.get(dominant_label, 0)
    health = dashboard.get("communityHealth") or {}
    health_score = int(health.get("score") or 0)
    health_trend = health.get("trend") or "flat"

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
            return build_success(
                action="investigate_question",
                window=request.window,
                summary=base.get("summary") or f"{topic} appears most relevant to this question.",
                confidence=base.get("confidence") or "medium",
                bullets=bullets,
                items=[detail_item],
                source_endpoints=["/api/dashboard", "/api/insights/cards", "/api/topics/detail", "/api/freshness"],
                caveat=caveat,
            )
        except AnalyticsAPIError as exc:
            if exc.error_type != "not_found":
                raise

    search_term = next(iter(_resolution_terms(base, request.question, attempted_topic=topic)), None)
    search_used = bool(search_term)
    search_payload = (
        search_entities(client, SearchEntitiesRequest(query=search_term, limit=5))
        if search_term
        else build_success(
            action="search_entities",
            window=None,
            summary="No search candidates were derived from this question.",
            confidence="low_confidence",
            bullets=["No matching entities were found."],
            items=[],
            source_endpoints=["/api/search"],
            caveat="Try a narrower topic or keyword.",
        )
    )
    candidates = list(search_payload.get("items") or [])
    topic_candidate = next(
        (item for item in candidates if _normalize_text(item.get("type")) == "topic" and _clean_text(item.get("name"))),
        None,
    )
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
            return build_success(
                action="investigate_question",
                window=request.window,
                summary=f"{detail_item['topic']} looks like the closest evidence-backed match to this question.",
                confidence="medium" if detail_item["mention_count"] > 0 else "low_confidence",
                bullets=bullets,
                items=[detail_item],
                source_endpoints=["/api/dashboard", "/api/insights/cards", "/api/search", "/api/topics/detail"],
            )
        except AnalyticsAPIError as exc:
            if exc.error_type != "not_found":
                raise

    fallback_items = candidates[:3] or _candidate_items(base)
    fallback_bullets = (
        _limit_bullets([f"{item.get('type')}: {item.get('name')}" for item in candidates[:3] if item.get("name")])
        if candidates
        else _candidate_bullets(fallback_items)
    )
    fallback_summary = base.get("summary")
    if not fallback_summary:
        if search_term:
            fallback_summary = (
                f'The question did not resolve to a single exact topic. Closest matches were searched using "{search_term}".'
            )
        else:
            fallback_summary = "The question did not resolve to a single exact topic right now."
    return build_success(
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
    ranked: list[dict[str, Any]] = []
    for candidate in candidates:
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
            score += 3
        topic_tokens = set(_tokenize(candidate.get("topic")))
        if topic_tokens and topic_tokens.intersection(question_tokens):
            score += 2
        if int(candidate.get("evidence_count") or 0) >= 2:
            score += 1
        if score <= 0:
            continue
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
    return top_score >= 3 and total_score >= 5 and evidence >= 2


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
