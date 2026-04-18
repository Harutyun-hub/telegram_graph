from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable


NON_LLM_REQUEST_TIME_SECONDARY_WIDGET_IDS = (
    "question_cloud",
    "problem_tracker",
    "service_gap_detector",
    "emotional_urgency_index",
    "recommendation_tracker",
    "persona_gallery",
    "business_opportunity_tracker",
    "topic_overviews_v2",
)


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_str(value: Any, default: str = "") -> str:
    text = str(value).strip() if value is not None else ""
    return text or default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _top_topics(snapshot: dict[str, Any], limit: int = 6) -> list[dict[str, Any]]:
    trending = [item for item in _as_list(snapshot.get("trendingTopics")) if isinstance(item, dict)]
    bubbles = [item for item in _as_list(snapshot.get("topicBubbles")) if isinstance(item, dict)]
    seen: set[str] = set()
    results: list[dict[str, Any]] = []
    for item in trending + bubbles:
        key = _as_str(item.get("topic") or item.get("name") or item.get("sourceTopic"))
        if not key or key in seen:
            continue
        seen.add(key)
        results.append(item)
        if len(results) >= limit:
            break
    return results


def _build_question_cloud(snapshot: dict[str, Any]) -> dict[str, Any]:
    categories: dict[str, list[dict[str, Any]]] = {}
    for item in _top_topics(snapshot, limit=8):
        category = _as_str(item.get("category"), "General")
        topic = _as_str(item.get("topic") or item.get("name") or item.get("sourceTopic"), "Community Topic")
        categories.setdefault(category, []).append(
            {
                "q": f"What is driving the discussion around {topic}?",
                "topic": topic,
                "count": max(1, _as_int(item.get("mentions"), 1)),
                "answered": False,
                "coveragePct": 0,
            }
        )
    question_categories = [
        {"category": category, "color": item_color, "questions": rows[:4]}
        for item_color, (category, rows) in zip(
            ["#3b82f6", "#10b981", "#f59e0b", "#8b5cf6", "#ef4444"],
            categories.items(),
        )
    ]
    total_questions = sum(len(item.get("questions") or []) for item in question_categories)
    return {
        "questionCategories": question_categories,
        "questionBriefs": [],
        "qaGap": {"totalQuestions": total_questions, "answered": 0},
    }


def _build_problem_cards(snapshot: dict[str, Any]) -> dict[str, Any]:
    cards: list[dict[str, Any]] = []
    for item in _as_list(snapshot.get("satisfactionAreas"))[:6]:
        if not isinstance(item, dict):
            continue
        area = _as_str(item.get("area"), "Community area")
        score = _as_int(item.get("satisfaction"), 0)
        if score >= 65:
            continue
        cards.append(
            {
                "id": f"problem:{area.lower().replace(' ', '_')}",
                "topic": area,
                "category": _as_str(item.get("category"), "Community"),
                "problem": f"Low satisfaction around {area}",
                "summary": f"{area} is below the desired satisfaction baseline for this window.",
                "severity": "high" if score < 40 else "medium",
                "confidence": "medium",
                "confidenceScore": 0.55,
                "demandSignals": {
                    "messages": max(1, _as_int(item.get("mentions"), 1)),
                    "uniqueUsers": max(1, _as_int(item.get("users"), 1)),
                    "channels": max(1, _as_int(item.get("channels"), 1)),
                    "trend7dPct": _as_int(item.get("trend"), 0),
                },
                "evidence": [],
                "latestAt": _utc_now_iso(),
            }
        )
    return {"problemBriefs": cards, "problems": []}


def _build_service_gap_cards(snapshot: dict[str, Any]) -> dict[str, Any]:
    cards: list[dict[str, Any]] = []
    for item in _as_list(snapshot.get("satisfactionAreas"))[:6]:
        if not isinstance(item, dict):
            continue
        area = _as_str(item.get("area"), "Community service")
        score = _as_int(item.get("satisfaction"), 0)
        if score >= 75:
            continue
        cards.append(
            {
                "id": f"service:{area.lower().replace(' ', '_')}",
                "topic": area,
                "category": _as_str(item.get("category"), "Community"),
                "serviceNeed": area,
                "unmetReason": f"Coverage remains thin for {area} in the selected window.",
                "urgency": "high" if score < 40 else "medium",
                "unmetPct": max(5, 100 - score),
                "confidence": "medium",
                "confidenceScore": 0.5,
                "demandSignals": {
                    "messages": max(1, _as_int(item.get("mentions"), 1)),
                    "uniqueUsers": max(1, _as_int(item.get("users"), 1)),
                    "channels": max(1, _as_int(item.get("channels"), 1)),
                    "trend7dPct": _as_int(item.get("trend"), 0),
                },
                "evidence": [],
                "latestAt": _utc_now_iso(),
            }
        )
    return {"serviceGapBriefs": cards, "serviceGaps": []}


def _build_urgency_cards(snapshot: dict[str, Any]) -> dict[str, Any]:
    signals: list[dict[str, Any]] = []
    for item in _as_list(snapshot.get("moodData"))[:8]:
        if not isinstance(item, dict):
            continue
        sentiment = _as_str(item.get("sentiment") or item.get("label"), "mixed")
        magnitude = max(1, _as_int(item.get("count", item.get("value")), 1))
        signals.append(
            {
                "id": f"urgency:{sentiment.lower().replace(' ', '_')}",
                "topic": sentiment,
                "summary": f"{sentiment} sentiment is active in the selected window.",
                "severity": "high" if sentiment.lower() in {"urgent", "negative"} else "medium",
                "confidence": "medium",
                "confidenceScore": 0.45,
                "mentions": magnitude,
                "latestAt": _utc_now_iso(),
                "evidence": [],
            }
        )
    return {"urgencySignals": signals}


def _build_recommendations(snapshot: dict[str, Any]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for item in _top_topics(snapshot, limit=6):
        topic = _as_str(item.get("topic") or item.get("name"), "Community Topic")
        rows.append(
            {
                "item": f"Create a targeted content or service response for {topic}",
                "category": _as_str(item.get("category"), "General"),
                "mentions": max(1, _as_int(item.get("mentions"), 1)),
                "rating": 4,
                "sentiment": "positive",
            }
        )
    return {"recommendations": rows}


def _build_personas(snapshot: dict[str, Any]) -> dict[str, Any]:
    personas: list[dict[str, Any]] = []
    interests = _as_list(snapshot.get("interests"))
    for index, item in enumerate(interests[:4], start=1):
        if not isinstance(item, dict):
            continue
        topic = _as_str(item.get("topic") or item.get("name"), f"Persona {index}")
        personas.append(
            {
                "name": topic,
                "size": max(1, _as_int(item.get("value", item.get("mentions")), 1)),
                "count": max(1, _as_int(item.get("value", item.get("mentions")), 1)),
                "color": ["#3b82f6", "#10b981", "#f59e0b", "#8b5cf6"][((index - 1) % 4)],
                "desc": f"Members clustered around {topic} interests.",
                "profile": f"People most engaged with {topic}.",
                "needs": "Clear guidance and follow-up support.",
                "interests": topic,
                "pain": "Signal is present but still broad.",
            }
        )
    return {
        "personas": personas,
        "origins": [],
        "integrationData": [],
        "integrationLevels": [],
        "integrationSeriesConfig": [],
    }


def _build_business_opportunities(snapshot: dict[str, Any]) -> dict[str, Any]:
    cards: list[dict[str, Any]] = []
    for item in _as_list(snapshot.get("jobTrends"))[:6]:
        if not isinstance(item, dict):
            continue
        topic = _as_str(item.get("topic") or item.get("role") or item.get("title"), "Opportunity")
        cards.append(
            {
                "id": f"opp:{topic.lower().replace(' ', '_')}",
                "title": topic,
                "summary": f"Demand signal for {topic} appears in the selected window.",
                "confidence": "medium",
                "confidenceScore": 0.5,
                "demandSignals": {
                    "messages": max(1, _as_int(item.get("mentions"), 1)),
                    "uniqueUsers": max(1, _as_int(item.get("users"), 1)),
                    "channels": max(1, _as_int(item.get("channels"), 1)),
                    "trend7dPct": _as_int(item.get("trend"), 0),
                },
                "evidence": [],
            }
        )
    return {"businessOpportunityBriefs": cards, "businessOpportunities": []}


def _build_topic_overviews(snapshot: dict[str, Any]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for item in _top_topics(snapshot, limit=6):
        topic = _as_str(item.get("topic") or item.get("name"), "Community Topic")
        items.append(
            {
                "topic": topic,
                "overview": f"{topic} remains active in the selected range and is tracked by Dashboard V2.",
                "confidence": "deterministic",
            }
        )
    return {"topicOverviews": items}


_SECONDARY_FACT_BUILDERS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "question_cloud": _build_question_cloud,
    "problem_tracker": _build_problem_cards,
    "service_gap_detector": _build_service_gap_cards,
    "emotional_urgency_index": _build_urgency_cards,
    "recommendation_tracker": _build_recommendations,
    "persona_gallery": _build_personas,
    "business_opportunity_tracker": _build_business_opportunities,
    "topic_overviews_v2": _build_topic_overviews,
}


def build_request_time_secondary_snapshot(widget_id: str, snapshot: dict[str, Any]) -> dict[str, Any]:
    builder = _SECONDARY_FACT_BUILDERS[widget_id]
    return builder(snapshot)
