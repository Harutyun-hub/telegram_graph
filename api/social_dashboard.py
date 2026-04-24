from __future__ import annotations

import time
import uuid
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, time as datetime_time, timezone
from typing import Any

from loguru import logger


SNAPSHOT_TTL_SECONDS = 300
ACTIVITY_SCAN_LIMIT = 2500
EVIDENCE_LIMIT = 24


_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def _trimmed(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _parse_dt(value: Any) -> datetime | None:
    text = _trimmed(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.fromisoformat(f"{text}T00:00:00+00:00")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _range_bounds(from_date: str | None, to_date: str | None) -> tuple[datetime | None, datetime | None]:
    start = _parse_dt(from_date)
    end = _parse_dt(to_date)
    if start:
        start = datetime.combine(start.date(), datetime_time.min, tzinfo=timezone.utc)
    if end:
        end = datetime.combine(end.date(), datetime_time.max, tzinfo=timezone.utc)
    return start, end


def _effective_dt(row: dict[str, Any]) -> datetime | None:
    return _parse_dt(row.get("published_at")) or _parse_dt(row.get("last_seen_at")) or _parse_dt(row.get("created_at"))


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    return _as_dict(_as_dict(row.get("analysis")).get("analysis_payload"))


def _analysis_text(row: dict[str, Any], key: str) -> str:
    return _trimmed(_payload(row).get(key))


def _analysis_list(row: dict[str, Any], key: str) -> list[str]:
    values: list[str] = []
    for item in _as_list(_payload(row).get(key)):
        if isinstance(item, str):
            text = _trimmed(item)
        elif isinstance(item, dict):
            text = _trimmed(item.get("name") or item.get("claim") or item.get("label") or item.get("value"))
        else:
            text = ""
        if text:
            values.append(text[:180])
    return values


def _sentiment_score(row: dict[str, Any]) -> float:
    analysis = _as_dict(row.get("analysis"))
    payload = _payload(row)
    for value in (analysis.get("sentiment_score"), payload.get("sentiment_score")):
        try:
            return max(-1.0, min(1.0, float(value)))
        except Exception:
            continue
    return 0.0


def _sentiment(row: dict[str, Any]) -> str:
    label = _trimmed(_as_dict(row.get("analysis")).get("sentiment") or _payload(row).get("sentiment")).lower()
    if label in {"positive", "negative", "neutral"}:
        return label
    score = _sentiment_score(row)
    if score >= 0.2:
        return "positive"
    if score <= -0.2:
        return "negative"
    return "neutral"


def _metric_number(metrics: dict[str, Any], *keys: str) -> int:
    for key in keys:
        value = metrics.get(key)
        if isinstance(value, (int, float)):
            return int(value)
        try:
            if value not in (None, ""):
                return int(float(value))
        except Exception:
            continue
    return 0


def _engagement_parts(row: dict[str, Any]) -> dict[str, int]:
    metrics = _as_dict(row.get("engagement_metrics"))
    return {
        "likes": _metric_number(metrics, "likes", "like_count", "reaction_count", "reactionCount"),
        "comments": _metric_number(metrics, "comments", "comment_count", "commentCount", "reply_count"),
        "shares": _metric_number(metrics, "shares", "share_count"),
        "views": _metric_number(metrics, "views", "view_count", "videoViewCount", "play_count", "impression_count"),
        "clicks": _metric_number(metrics, "clicks", "click_count"),
    }


def _engagement_total(row: dict[str, Any]) -> int:
    return sum(_engagement_parts(row).values())


def _entity_name(row: dict[str, Any]) -> str:
    return _trimmed(_as_dict(row.get("entity")).get("name")) or "Unknown"


def _entity_id(row: dict[str, Any]) -> str:
    return _trimmed(_as_dict(row.get("entity")).get("id") or row.get("entity_id"))


def _is_ad(row: dict[str, Any]) -> bool:
    source_kind = _trimmed(row.get("source_kind")).lower()
    return source_kind in {"ad", "meta_ads", "google_ads"} or _trimmed(row.get("platform")).lower() == "google"


def _is_organic(row: dict[str, Any]) -> bool:
    return not _is_ad(row)


def _date_bucket(row: dict[str, Any]) -> str:
    dt = _effective_dt(row)
    return dt.date().isoformat() if dt else "unknown"


def _week_label(index: int) -> str:
    return f"W{index + 1}"


def _series_key(label: str) -> str:
    cleaned = "".join(char.lower() if char.isalnum() else "_" for char in label).strip("_")
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned or "entity"


def _safe_pct(part: int | float, whole: int | float) -> float:
    return round((float(part) / float(whole)) * 100.0, 1) if whole else 0.0


def _evidence(row: dict[str, Any]) -> dict[str, Any]:
    parts = _engagement_parts(row)
    return {
        "id": row.get("id"),
        "activity_uid": row.get("activity_uid"),
        "entity": _entity_name(row),
        "entity_id": _entity_id(row),
        "platform": row.get("platform"),
        "source_kind": row.get("source_kind"),
        "source_url": row.get("source_url"),
        "text": row.get("text_content"),
        "summary": _analysis_text(row, "summary") or _trimmed(_as_dict(row.get("analysis")).get("summary")),
        "published_at": row.get("published_at") or row.get("last_seen_at") or row.get("created_at"),
        "author_handle": row.get("author_handle"),
        "sentiment": _sentiment(row),
        "sentiment_score": _sentiment_score(row),
        "topics": _analysis_list(row, "topics"),
        "pain_points": _analysis_list(row, "pain_points"),
        "engagement": {**parts, "total": sum(parts.values())},
        "assets": _as_list(row.get("assets")),
    }


def _empty_snapshot(filters: dict[str, Any], request_id: str, started_at: float) -> dict[str, Any]:
    return {
        "meta": {
            "requestId": request_id,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "cacheStatus": "miss",
            "rowCap": ACTIVITY_SCAN_LIMIT,
            "rowCapReached": False,
            "scannedActivities": 0,
            "usedActivities": 0,
            "analyzedActivities": 0,
            "missingAnalysis": 0,
            "missingText": 0,
            "missingPublishedDate": 0,
            "degradedSections": [],
            "emptyReasons": {"snapshot": "No social activities matched the selected filters."},
            "timingsMs": {"total": round((time.perf_counter() - started_at) * 1000, 2)},
        },
        "filters": {
            "selected": filters,
            "entities": [],
            "platforms": [],
            "sourceKinds": [],
        },
        "deepAnalysis": {
            "topicBubbles": [],
            "topicMomentum": [],
            "sentimentTrend": [],
            "intentSignals": [],
            "signalTrend": [],
            "topQuestions": [],
            "painPoints": [],
            "evidence": [],
        },
        "adIntelligence": {
            "items": [],
            "summary": {"topMarketingIntent": None, "topCtaType": None, "topProduct": None},
        },
        "strictMetrics": {
            "sentimentByEntity": [],
            "engagementRadar": [],
            "visibilityData": [],
            "visibilityTrend": [],
            "positiveImpact": [],
            "negativeImpact": [],
            "weeklyShifts": [],
            "scorecard": [],
            "shareOfVoice": [],
        },
    }


def _topic_bubbles(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        for topic in _analysis_list(row, "topics"):
            counts[topic][_sentiment(row)] += 1
    items = []
    for index, (topic, counter) in enumerate(sorted(counts.items(), key=lambda item: -sum(item[1].values()))[:12]):
        total = sum(counter.values())
        dominant = counter.most_common(1)[0][0] if counter else "neutral"
        items.append({
            "topic": topic,
            "count": total,
            "sentiment": dominant,
            "x": 70 + (index % 4) * 120,
            "y": 70 + (index // 4) * 95,
            "r": max(24, min(58, 20 + total * 4)),
        })
    return items


def _sentiment_trend(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        bucket = _date_bucket(row)
        if bucket == "unknown":
            continue
        buckets[bucket][_sentiment(row)] += 1
    return [
        {
            "week": bucket,
            "bucket": bucket,
            "positive": counts.get("positive", 0),
            "neutral": counts.get("neutral", 0),
            "negative": counts.get("negative", 0),
        }
        for bucket, counts in sorted(buckets.items())[-12:]
    ]


def _topic_momentum(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets = sorted({_date_bucket(row) for row in rows if _date_bucket(row) != "unknown"})[-5:]
    topic_counts: dict[str, Counter] = defaultdict(Counter)
    topic_sentiment: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        bucket = _date_bucket(row)
        for topic in _analysis_list(row, "topics"):
            if bucket in buckets:
                topic_counts[topic][bucket] += 1
            topic_sentiment[topic][_sentiment(row)] += 1
    output = []
    for topic, counter in sorted(topic_counts.items(), key=lambda item: -sum(item[1].values()))[:10]:
        values = [counter.get(bucket, 0) for bucket in buckets]
        first = values[0] if values else 0
        last = values[-1] if values else 0
        velocity = round(((last - first) / first) * 100.0, 1) if first else (100.0 if last else 0.0)
        item = {
            "topic": topic,
            "velocity": velocity,
            "sentiment": topic_sentiment[topic].most_common(1)[0][0] if topic_sentiment[topic] else "neutral",
        }
        for index in range(5):
            item[f"w{index + 1}"] = values[index] if index < len(values) else 0
        output.append(item)
    return output


def _intent_signals(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter = Counter()
    examples: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        text = _trimmed(row.get("text_content"))
        payload = _payload(row)
        candidates = [
            _trimmed(payload.get("customer_intent")),
            _trimmed(payload.get("marketing_intent")),
        ]
        if "?" in text:
            candidates.append("Questions")
        if _sentiment(row) == "negative":
            candidates.append("Complaints")
        if _sentiment(row) == "positive":
            candidates.append("Praise")
        for candidate in candidates:
            if not candidate:
                continue
            counts[candidate] += 1
            if text and len(examples[candidate]) < 3:
                examples[candidate].append(text[:160])
    total = sum(counts.values())
    return [
        {
            "intent": intent,
            "count": count,
            "pct": _safe_pct(count, total),
            "delta": 0,
            "examples": examples.get(intent, []),
        }
        for intent, count in counts.most_common(8)
    ]


def _top_questions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    questions: Counter = Counter()
    entity_by_question: dict[str, str] = {}
    for row in rows:
        text = _trimmed(row.get("text_content"))
        if "?" not in text:
            continue
        sentence = next((part.strip() for part in text.replace("\n", " ").split("?") if part.strip()), "")
        if not sentence:
            continue
        question = f"{sentence[:180]}?"
        questions[question] += 1
        entity_by_question.setdefault(question, _entity_name(row))
    return [
        {
            "question": question,
            "count": count,
            "trend": "stable",
            "entity": entity_by_question.get(question, "Unknown"),
            "category": "Social",
            "answered": False,
        }
        for question, count in questions.most_common(12)
    ]


def _pain_points(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        for label in _analysis_list(row, "pain_points"):
            counts[label][_entity_name(row)] += 1
    return [
        {
            "text": label,
            "count": sum(counter.values()),
            "entities": [name for name, _ in counter.most_common(3)],
            "severity": "high" if sum(counter.values()) >= 3 else "medium",
        }
        for label, counter in sorted(counts.items(), key=lambda item: -sum(item[1].values()))[:12]
    ]


def _ad_items(rows: list[dict[str, Any]]) -> dict[str, Any]:
    items = []
    intent_counter: Counter = Counter()
    cta_counter: Counter = Counter()
    product_counter: Counter = Counter()
    for row in sorted(rows, key=lambda item: _trimmed(item.get("published_at") or item.get("last_seen_at")), reverse=True)[:50]:
        intent = _analysis_text(row, "marketing_intent")
        cta = _trimmed(row.get("cta_type"))
        products = _analysis_list(row, "products")
        if intent:
            intent_counter[intent] += 1
        if cta:
            cta_counter[cta] += 1
        for product in products:
            product_counter[product] += 1
        parts = _engagement_parts(row)
        items.append({
            "id": row.get("id"),
            "entity": _entity_name(row),
            "source": "meta" if row.get("platform") == "facebook" else row.get("platform"),
            "platform": row.get("platform"),
            "source_kind": row.get("source_kind"),
            "copy": row.get("text_content") or _analysis_text(row, "summary"),
            "cta": cta,
            "format": row.get("content_format"),
            "intent": intent,
            "valueProps": _analysis_list(row, "value_propositions"),
            "products": products,
            "urgency": bool(_analysis_list(row, "urgency_indicators")),
            "date": row.get("published_at") or row.get("last_seen_at"),
            "impressions": parts["views"],
            "engagement": sum(parts.values()),
            "clicks": parts["clicks"],
            "source_url": row.get("source_url"),
            "evidence": _evidence(row),
        })
    return {
        "items": items,
        "summary": {
            "topMarketingIntent": intent_counter.most_common(1)[0][0] if intent_counter else None,
            "topCtaType": cta_counter.most_common(1)[0][0] if cta_counter else None,
            "topProduct": product_counter.most_common(1)[0][0] if product_counter else None,
        },
    }


def _strict_metrics(rows: list[dict[str, Any]], previous_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_entity: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_entity[_entity_name(row)].append(row)
    total_mentions = len(rows)
    sentiment_by_entity = []
    visibility_data = []
    scorecard = []
    engagement_subjects = ["likes", "comments", "shares", "views", "clicks"]
    engagement_totals: dict[str, Counter] = defaultdict(Counter)
    for entity, entity_rows in by_entity.items():
        sentiment_counts = Counter(_sentiment(row) for row in entity_rows)
        engagement = sum(_engagement_total(row) for row in entity_rows)
        score_total = sum(_sentiment_score(row) for row in entity_rows)
        topics = Counter(topic for row in entity_rows for topic in _analysis_list(row, "topics"))
        products = Counter(product for row in entity_rows for product in _analysis_list(row, "products"))
        intents = Counter(_analysis_text(row, "marketing_intent") for row in entity_rows if _analysis_text(row, "marketing_intent"))
        ads = sum(1 for row in entity_rows if _is_ad(row))
        for row in entity_rows:
            parts = _engagement_parts(row)
            for subject in engagement_subjects:
                engagement_totals[entity][subject] += parts[subject]
        sentiment_by_entity.append({
            "entity": entity,
            "pos": _safe_pct(sentiment_counts.get("positive", 0), len(entity_rows)),
            "neu": _safe_pct(sentiment_counts.get("neutral", 0), len(entity_rows)),
            "neg": _safe_pct(sentiment_counts.get("negative", 0), len(entity_rows)),
            "total": len(entity_rows),
        })
        visibility_data.append({
            "entity": entity,
            "visibility": _safe_pct(len(entity_rows), total_mentions),
            "delta": 0,
            "reach": engagement,
            "deltaReach": 0,
            "engagement": round(engagement / max(1, len(entity_rows)), 1),
            "deltaEngage": 0,
            "sov": _safe_pct(len(entity_rows), total_mentions),
            "deltaSov": 0,
        })
        scorecard.append({
            "id": entity.lower().replace(" ", "-"),
            "name": entity,
            "posts": len(entity_rows),
            "ads": ads,
            "sentiment": round(((score_total / max(1, len(entity_rows))) + 1.0) * 50.0, 1),
            "intent": intents.most_common(1)[0][0] if intents else None,
            "topics": [name for name, _ in topics.most_common(3)],
            "products": [name for name, _ in products.most_common(3)],
        })
    max_subject = {
        subject: max((counter.get(subject, 0) for counter in engagement_totals.values()), default=0)
        for subject in engagement_subjects
    }
    engagement_radar = [
        {
            "subject": subject.capitalize(),
            **{
                _series_key(entity): _safe_pct(counter.get(subject, 0), max_subject[subject])
                for entity, counter in engagement_totals.items()
            },
            "fullMark": 100,
        }
        for subject in engagement_subjects
    ]
    current_counter = Counter(topic for row in rows for topic in _analysis_list(row, "topics"))
    previous_counter = Counter(topic for row in previous_rows for topic in _analysis_list(row, "topics"))
    weekly_shifts = [
        {
            "metric": "Total Mentions",
            "current": len(rows),
            "previous": len(previous_rows),
            "unit": "",
            "goodIfUp": True,
        },
        {
            "metric": "Positive Sentiment",
            "current": _safe_pct(sum(1 for row in rows if _sentiment(row) == "positive"), len(rows)),
            "previous": _safe_pct(sum(1 for row in previous_rows if _sentiment(row) == "positive"), len(previous_rows)),
            "unit": "%",
            "goodIfUp": True,
        },
        {
            "metric": "Questions Asked",
            "current": sum(1 for row in rows if "?" in _trimmed(row.get("text_content"))),
            "previous": sum(1 for row in previous_rows if "?" in _trimmed(row.get("text_content"))),
            "unit": "",
            "goodIfUp": False,
        },
        {
            "metric": "Ads Running",
            "current": sum(1 for row in rows if _is_ad(row)),
            "previous": sum(1 for row in previous_rows if _is_ad(row)),
            "unit": "",
            "goodIfUp": True,
        },
    ]
    positive_impact = [
        {"topic": topic, "gain": f"+{count - previous_counter.get(topic, 0)}", "mentions": count}
        for topic, count in current_counter.most_common(8)
        if count >= previous_counter.get(topic, 0)
    ]
    negative_impact = [
        {"topic": topic, "loss": f"-{previous_counter.get(topic, 0) - count}", "mentions": count}
        for topic, count in current_counter.most_common(8)
        if count < previous_counter.get(topic, 0)
    ]
    return {
        "sentimentByEntity": sorted(sentiment_by_entity, key=lambda item: -item["total"]),
        "engagementRadar": engagement_radar,
        "visibilityData": sorted(visibility_data, key=lambda item: -item["visibility"]),
        "visibilityTrend": [],
        "positiveImpact": positive_impact,
        "negativeImpact": negative_impact,
        "weeklyShifts": weekly_shifts,
        "scorecard": sorted(scorecard, key=lambda item: -item["posts"]),
        "shareOfVoice": [
            {"name": item["entity"], "value": item["sov"]}
            for item in sorted(visibility_data, key=lambda value: -value["sov"])
        ],
    }


def _cache_key(filters: dict[str, Any]) -> str:
    parts = [
        filters.get("from") or "",
        filters.get("to") or "",
        filters.get("entity_id") or "",
        filters.get("platform") or "",
        filters.get("source_kind") or "",
    ]
    return "|".join(parts)


def _fetch_rows(store: Any, filters: dict[str, Any], timings: dict[str, float]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    started = time.perf_counter()
    activity_filters: list[tuple[str, str, Any]] = []
    if filters.get("entity_id"):
        activity_filters.append(("eq", "entity_id", filters["entity_id"]))
    if filters.get("platform") and filters.get("platform") != "all":
        activity_filters.append(("eq", "platform", filters["platform"]))
    if filters.get("source_kind") and filters.get("source_kind") != "all":
        activity_filters.append(("eq", "source_kind", filters["source_kind"]))
    activities = store._select_rows(
        "social_activities",
        columns=(
            "id,entity_id,account_id,activity_uid,platform,source_kind,provider_item_id,source_url,"
            "text_content,published_at,author_handle,cta_type,content_format,region_name,"
            "engagement_metrics,assets,provider_payload,ingest_status,analysis_status,graph_status,"
            "last_seen_at,created_at"
        ),
        filters=activity_filters,
        order_by="last_seen_at",
        desc=True,
        limit=ACTIVITY_SCAN_LIMIT,
    )
    timings["activitiesFetchMs"] = round((time.perf_counter() - started) * 1000, 2)

    started = time.perf_counter()
    activity_ids = [row["id"] for row in activities if row.get("id")]
    analyses = {}
    if activity_ids:
        analyses = {
            row["activity_id"]: row
            for row in store._select_rows(
                "social_activity_analysis",
                columns=(
                    "activity_id,summary,marketing_intent,sentiment,sentiment_score,"
                    "analysis_payload,raw_model_output,analyzed_at"
                ),
                filters=(("in", "activity_id", activity_ids),),
            )
        }
    timings["analysisFetchMs"] = round((time.perf_counter() - started) * 1000, 2)

    started = time.perf_counter()
    entity_ids = list({row.get("entity_id") for row in activities if row.get("entity_id")})
    entities = {}
    if entity_ids:
        entities = {
            row["id"]: row
            for row in store._select_rows(
                "social_entities",
                columns="id,name,industry,website,logo_url,is_active",
                filters=(("in", "id", entity_ids),),
            )
        }
    accounts = {}
    account_ids = list({row.get("account_id") for row in activities if row.get("account_id")})
    if account_ids:
        accounts = {
            row["id"]: row
            for row in store._select_rows(
                "social_entity_accounts",
                columns="id,entity_id,platform,source_kind,account_handle,account_external_id,domain,metadata,is_active,health_status",
                filters=(("in", "id", account_ids),),
            )
        }
    timings["dimensionFetchMs"] = round((time.perf_counter() - started) * 1000, 2)
    return activities, list(analyses.values()), entities, accounts


def build_social_dashboard_snapshot(
    store: Any,
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    entity_id: str | None = None,
    platform: str | None = None,
    source_kind: str | None = None,
    use_cache: bool = True,
) -> dict[str, Any]:
    filters = {
        "from": from_date,
        "to": to_date,
        "entity_id": entity_id,
        "platform": platform,
        "source_kind": source_kind,
    }
    key = _cache_key(filters)
    now = time.time()
    if use_cache and key in _CACHE:
        cached_at, cached = _CACHE[key]
        if now - cached_at < SNAPSHOT_TTL_SECONDS:
            payload = deepcopy(cached)
            payload["meta"]["cacheStatus"] = "hit"
            payload["meta"]["servedFromCacheAt"] = datetime.now(timezone.utc).isoformat()
            return payload

    request_id = uuid.uuid4().hex[:12]
    started_at = time.perf_counter()
    timings: dict[str, float] = {}
    try:
        activities, analyses, entities, accounts = _fetch_rows(store, filters, timings)
    except Exception as exc:
        if use_cache and key in _CACHE:
            payload = deepcopy(_CACHE[key][1])
            payload["meta"]["cacheStatus"] = "stale"
            payload["meta"]["degradedSections"] = sorted(set(payload["meta"].get("degradedSections", []) + ["fetch"]))
            payload["meta"]["lastError"] = str(exc)
            logger.warning("Social dashboard served stale snapshot | request_id={} error={}", request_id, exc)
            return payload
        raise

    analysis_by_activity = {row.get("activity_id"): row for row in analyses}
    start_bound, end_bound = _range_bounds(from_date, to_date)
    enriched: list[dict[str, Any]] = []
    for row in activities:
        effective = _effective_dt(row)
        if start_bound and effective and effective < start_bound:
            continue
        if end_bound and effective and effective > end_bound:
            continue
        enriched.append({
            **row,
            "entity": entities.get(row.get("entity_id")),
            "account": accounts.get(row.get("account_id")),
            "analysis": analysis_by_activity.get(row.get("id")),
        })

    previous_rows: list[dict[str, Any]] = []
    # Keep v1 simple: previous-window comparisons use the scanned rows outside the
    # selected window when available. This avoids a second expensive query.
    if start_bound:
        previous_rows = [
            {
                **row,
                "entity": entities.get(row.get("entity_id")),
                "account": accounts.get(row.get("account_id")),
                "analysis": analysis_by_activity.get(row.get("id")),
            }
            for row in activities
            if (_effective_dt(row) and _effective_dt(row) < start_bound)
        ]

    if not enriched:
        snapshot = _empty_snapshot(filters, request_id, started_at)
    else:
        organic = [row for row in enriched if _is_organic(row)]
        ads = [row for row in enriched if _is_ad(row)]
        section_started = time.perf_counter()
        deep = {
            "topicBubbles": _topic_bubbles(organic),
            "topicMomentum": _topic_momentum(organic),
            "sentimentTrend": _sentiment_trend(organic),
            "intentSignals": _intent_signals(organic),
            "topQuestions": _top_questions(organic),
            "painPoints": _pain_points(organic),
            "evidence": [_evidence(row) for row in organic[:EVIDENCE_LIMIT]],
        }
        timings["deepAnalysisBuildMs"] = round((time.perf_counter() - section_started) * 1000, 2)

        section_started = time.perf_counter()
        ad_intelligence = _ad_items(ads)
        timings["adIntelligenceBuildMs"] = round((time.perf_counter() - section_started) * 1000, 2)

        section_started = time.perf_counter()
        strict = _strict_metrics(enriched, previous_rows)
        timings["strictMetricsBuildMs"] = round((time.perf_counter() - section_started) * 1000, 2)

        empty_reasons = {}
        if not organic:
            empty_reasons["deepAnalysis"] = "No organic Facebook Page or Instagram discussion rows matched the selected filters."
        if not ads:
            empty_reasons["adIntelligence"] = "No Meta/Google ad rows matched the selected filters."
        if not any(row.get("analysis") for row in enriched):
            empty_reasons["analysis"] = "Matched activities exist, but none have social AI analysis yet."

        entities_filter = sorted(
            [{"id": row["id"], "name": row.get("name") or "Unknown"} for row in entities.values()],
            key=lambda item: item["name"],
        )
        snapshot = {
            "meta": {
                "requestId": request_id,
                "generatedAt": datetime.now(timezone.utc).isoformat(),
                "cacheStatus": "miss",
                "rowCap": ACTIVITY_SCAN_LIMIT,
                "rowCapReached": len(activities) >= ACTIVITY_SCAN_LIMIT,
                "scannedActivities": len(activities),
                "usedActivities": len(enriched),
                "analyzedActivities": sum(1 for row in enriched if row.get("analysis")),
                "missingAnalysis": sum(1 for row in enriched if not row.get("analysis")),
                "missingText": sum(1 for row in enriched if not _trimmed(row.get("text_content"))),
                "missingPublishedDate": sum(1 for row in enriched if not row.get("published_at")),
                "degradedSections": [],
                "emptyReasons": empty_reasons,
                "timingsMs": timings,
            },
            "filters": {
                "selected": filters,
                "entities": entities_filter,
                "platforms": sorted({row.get("platform") for row in enriched if row.get("platform")}),
                "sourceKinds": sorted({row.get("source_kind") for row in enriched if row.get("source_kind")}),
            },
            "deepAnalysis": deep,
            "adIntelligence": ad_intelligence,
            "strictMetrics": strict,
        }

    snapshot["meta"]["timingsMs"]["total"] = round((time.perf_counter() - started_at) * 1000, 2)
    _CACHE[key] = (time.time(), deepcopy(snapshot))
    return snapshot
