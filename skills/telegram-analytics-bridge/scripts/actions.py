from __future__ import annotations

import re
from typing import Any

from formatters import build_success
from models import (
    AskInsightsRequest,
    GetActiveAlertsRequest,
    GetDecliningTopicsRequest,
    GetProblemSpikesRequest,
    GetQuestionClustersRequest,
    GetSentimentOverviewRequest,
    GetTopTopicsRequest,
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
