from __future__ import annotations

import json
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger
from openai import OpenAI

import config
from api import social_semantic
from utils.ai_usage import log_openai_usage


SNAPSHOT_SETTING_KEY = "ai_brief_snapshot"
SIGNAL_HISTORY_SETTING_KEY = "ai_brief_signal_history"
PROMPT_VERSION = "social-ai-briefs-v1"
WINDOW_DAYS = 15
REFRESH_MIN_HOURS = 24
MIN_NEW_PARENT_THREADS = 50
MAX_CANDIDATE_CLUSTERS = 24
MAX_EVIDENCE_PER_CLUSTER = 4
MAX_COMPLETION_TOKENS = 7000
MIN_CONFIDENCE = 0.60
SIGNAL_HISTORY_LIMIT = 30
ALLOWED_FAMILIES = {
    "support": "Support",
    "concern": "Concern",
    "questions": "Questions",
    "complaints": "Complaints",
    "trust / distrust": "Trust / Distrust",
    "trust/distrust": "Trust / Distrust",
    "trust": "Trust / Distrust",
    "distrust": "Trust / Distrust",
}


SYSTEM_PROMPT = """
You create concise social-media intelligence brief cards from evidence clusters.
The domain can be politics, public figures, business, services, civic issues, or community discussion.

Return STRICT JSON only. No markdown.

Use only the evidence provided. Do not invent facts, causes, claims, actors, or numbers.
Write English and Russian text yourself. Keep Russian natural and concise.

Create one shared snapshot with:
- intentCards: evidence-backed overview cards about what people support, question, complain about, worry about, trust, or distrust.
- topSignals: short reusable signal cards for future widgets.
- topQuestions: questions or information needs supported by evidence.

Allowed intent card families:
Support, Concern, Questions, Complaints, Trust / Distrust

Rules:
1. Produce as many cards as the evidence supports, but only medium/high confidence cards.
2. Each card must cite real evidence_ids from the input.
3. Do not create Mobilization or Policy Expectation cards.
4. Prefer clear analyst wording over raw labels.
5. Keep summaries grounded and specific.
6. Confidence must be 0.0 to 1.0.
7. Keep the total JSON compact enough to return completely in one response.

Return shape:
{
  "intentCards": [
    {
      "family": "Support|Concern|Questions|Complaints|Trust / Distrust",
      "title_en": "...",
      "title_ru": "...",
      "summary_en": "...",
      "summary_ru": "...",
      "main_topic": "...",
      "sentiment": "positive|neutral|negative|mixed|urgent|sarcastic",
      "signal_count": 3,
      "trend_pct": 0,
      "confidence": 0.75,
      "evidence_ids": ["activity_uid"],
      "evidence_quotes": ["original short quote"]
    }
  ],
  "topSignals": [
    {
      "family": "Support|Concern|Questions|Complaints|Trust / Distrust",
      "title_en": "...",
      "title_ru": "...",
      "summary_en": "...",
      "summary_ru": "...",
      "main_topic": "...",
      "sentiment": "positive|neutral|negative|mixed|urgent|sarcastic",
      "signal_count": 3,
      "trend_pct": 0,
      "confidence": 0.75,
      "evidence_ids": ["activity_uid"],
      "evidence_quotes": ["original short quote"]
    }
  ],
  "topQuestions": [
    {
      "question_en": "...",
      "question_ru": "...",
      "topic": "...",
      "count": 2,
      "confidence": 0.75,
      "evidence_ids": ["activity_uid"]
    }
  ]
}
""".strip()


def _trimmed(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _parse_dt(value: Any) -> datetime | None:
    text = _trimmed(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _window_bounds(now: datetime | None = None) -> tuple[str, str]:
    end = (now or _utc_now()).astimezone(timezone.utc)
    start = end - timedelta(days=WINDOW_DAYS)
    return _iso(start), _iso(end)


def _text_preview(value: Any, limit: int = 260) -> str:
    text = re.sub(r"\s+", " ", _trimmed(value))
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 1)].rstrip()}..."


def _payload_topics(payload: dict[str, Any]) -> list[str]:
    topics = []
    for item in _as_list(payload.get("topics")):
        if isinstance(item, str):
            text = _trimmed(item)
        elif isinstance(item, dict):
            text = _trimmed(item.get("name") or item.get("topic") or item.get("label"))
        else:
            text = ""
        if text:
            topics.append(text)
    return topics


def _payload_list(payload: dict[str, Any], key: str) -> list[str]:
    output = []
    for item in _as_list(payload.get(key)):
        if isinstance(item, str):
            text = _trimmed(item)
        elif isinstance(item, dict):
            text = _trimmed(item.get("name") or item.get("claim") or item.get("label") or item.get("value"))
        else:
            text = ""
        if text:
            output.append(text)
    return output


def _sentiment(payload: dict[str, Any], analysis: dict[str, Any]) -> str:
    label = _trimmed(payload.get("sentiment") or analysis.get("sentiment")).lower()
    if label in {"positive", "negative", "neutral", "mixed", "urgent", "sarcastic"}:
        return label
    try:
        score = float(payload.get("sentiment_score", analysis.get("sentiment_score", 0.0)))
    except Exception:
        score = 0.0
    if score >= 0.2:
        return "positive"
    if score <= -0.2:
        return "negative"
    return "neutral"


def _load_snapshot(store: Any) -> dict[str, Any]:
    snapshot = store.get_runtime_setting(SNAPSHOT_SETTING_KEY, {})
    return snapshot if isinstance(snapshot, dict) else {}


def get_social_ai_brief_snapshot(store: Any) -> dict[str, Any]:
    """Request-time read path for the Social dashboard. Never calls AI."""
    snapshot = _load_snapshot(store)
    if not snapshot:
        return {
            "status": "missing",
            "intentCards": [],
            "topSignals": [],
            "topQuestions": [],
            "metadata": {"reason": "No Social AI brief snapshot has been generated yet."},
        }
    return snapshot


def _save_snapshot(store: Any, snapshot: dict[str, Any]) -> dict[str, Any]:
    store.save_runtime_setting(SNAPSHOT_SETTING_KEY, snapshot)
    return snapshot


def get_social_ai_brief_signal_trend(store: Any) -> list[dict[str, Any]]:
    """Request-time read path for the Social dashboard. Never calls AI."""
    history = store.get_runtime_setting(SIGNAL_HISTORY_SETTING_KEY, [])
    return history if isinstance(history, list) else []


def _signal_family_key(family: str) -> str:
    return family.lower().replace(" / ", "_").replace("/", "_").replace(" ", "_")


def _signal_history_point(snapshot: dict[str, Any]) -> dict[str, Any]:
    cards = _as_list(snapshot.get("topSignals")) or _as_list(snapshot.get("intentCards"))
    counts: Counter[str] = Counter()
    for card in cards:
        if not isinstance(card, dict):
            continue
        family = _normalize_family(card.get("family") or card.get("intent"))
        if not family:
            continue
        try:
            count = max(1, int(card.get("signal_count") or card.get("count") or 1))
        except Exception:
            count = 1
        counts[family] += count
    generated_at = _trimmed(snapshot.get("generatedAt")) or _trimmed(_as_dict(snapshot.get("metadata")).get("generatedAt"))
    bucket = generated_at[:10] if generated_at else _utc_now().date().isoformat()
    point = {"bucket": bucket, "generatedAt": generated_at, "total": sum(counts.values())}
    for family, count in counts.items():
        point[_signal_family_key(family)] = count
    return point


def _append_signal_history(store: Any, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    point = _signal_history_point(snapshot)
    history = get_social_ai_brief_signal_trend(store)
    next_history = [
        item for item in history
        if isinstance(item, dict) and item.get("bucket") != point.get("bucket")
    ]
    next_history.append(point)
    next_history = next_history[-SIGNAL_HISTORY_LIMIT:]
    store.save_runtime_setting(SIGNAL_HISTORY_SETTING_KEY, next_history)
    return next_history


def _activity_filters(start_iso: str, end_iso: str) -> list[tuple[str, str, Any]]:
    return [
        ("eq", "source_kind", "post"),
        ("eq", "analysis_status", "analyzed"),
        ("eq", "graph_status", "synced"),
        ("gte", "published_at", start_iso),
        ("lte", "published_at", end_iso),
    ]


def _load_parent_threads(store: Any, *, start_iso: str, end_iso: str, limit: int = 5000) -> list[dict[str, Any]]:
    activities = store._select_rows(
        "social_activities",
        columns=(
            "id,entity_id,account_id,activity_uid,platform,source_kind,source_url,text_content,"
            "published_at,author_handle,engagement_metrics,assets,analysis_status,graph_status"
        ),
        filters=_activity_filters(start_iso, end_iso),
        order_by="published_at",
        desc=True,
        limit=max(1, min(int(limit or 5000), 10000)),
    )
    if not activities:
        return []

    activity_ids = [row["id"] for row in activities if row.get("id")]
    analyses = {
        row["activity_id"]: row
        for row in store._select_rows(
            "social_activity_analysis",
            columns="activity_id,summary,sentiment,sentiment_score,analysis_payload,raw_model_output,analyzed_at",
            filters=(("in", "activity_id", activity_ids),),
        )
    }
    entity_ids = list({row.get("entity_id") for row in activities if row.get("entity_id")})
    entities = {
        row["id"]: row
        for row in store._select_rows(
            "social_entities",
            columns="id,name,industry,website,logo_url,is_active",
            filters=(("in", "id", entity_ids),),
        )
    } if entity_ids else {}

    enriched = []
    for row in activities:
        analysis = analyses.get(row.get("id"))
        if not analysis:
            continue
        enriched.append({**row, "analysis": analysis, "entity": entities.get(row.get("entity_id"))})
    return enriched


def _count_new_parent_threads(rows: list[dict[str, Any]], snapshot: dict[str, Any]) -> int:
    included = set(_as_list(_as_dict(snapshot.get("metadata")).get("includedActivityUids")))
    return sum(1 for row in rows if _trimmed(row.get("activity_uid")) and _trimmed(row.get("activity_uid")) not in included)


def should_refresh_social_ai_briefs(store: Any, *, force: bool = False, now: datetime | None = None) -> dict[str, Any]:
    start_iso, end_iso = _window_bounds(now)
    rows = _load_parent_threads(store, start_iso=start_iso, end_iso=end_iso)
    snapshot = _load_snapshot(store)
    if force:
        return {
            "eligible": True,
            "reason": "manual_refresh",
            "newProcessedParentThreads": len(rows),
            "window": {"from": start_iso, "to": end_iso},
        }

    metadata = _as_dict(snapshot.get("metadata"))
    generated_at = _parse_dt(metadata.get("generatedAt") or snapshot.get("generatedAt"))
    age_hours = 999999.0 if not generated_at else ((_utc_now() if now is None else now) - generated_at).total_seconds() / 3600.0
    new_count = _count_new_parent_threads(rows, snapshot)
    eligible = age_hours >= REFRESH_MIN_HOURS and new_count >= MIN_NEW_PARENT_THREADS
    reason = "eligible" if eligible else "not_enough_new_data"
    if age_hours < REFRESH_MIN_HOURS:
        reason = "too_recent"
    return {
        "eligible": eligible,
        "reason": reason,
        "ageHours": round(age_hours, 2),
        "newProcessedParentThreads": new_count,
        "minimumNewParentThreads": MIN_NEW_PARENT_THREADS,
        "minimumAgeHours": REFRESH_MIN_HOURS,
        "window": {"from": start_iso, "to": end_iso},
    }


def _evidence_by_uid(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    evidence: dict[str, dict[str, Any]] = {}
    for row in rows:
        uid = _trimmed(row.get("activity_uid"))
        if not uid:
            continue
        analysis = _as_dict(row.get("analysis"))
        payload = _as_dict(analysis.get("analysis_payload"))
        entity = _as_dict(row.get("entity"))
        evidence[uid] = {
            "evidence_id": uid,
            "activity_id": row.get("id"),
            "entity": entity.get("name") or "Unknown",
            "platform": row.get("platform"),
            "published_at": row.get("published_at"),
            "source_url": row.get("source_url"),
            "quote": _text_preview(row.get("text_content"), 240),
            "summary": _text_preview(payload.get("summary") or analysis.get("summary"), 220),
            "topics": _payload_topics(payload)[:6],
            "sentiment": _sentiment(payload, analysis),
            "customer_intent": _trimmed(payload.get("customer_intent")),
            "pain_points": _payload_list(payload, "pain_points")[:5],
        }
    return evidence


def build_social_ai_brief_candidates(store: Any, *, now: datetime | None = None) -> dict[str, Any]:
    start_iso, end_iso = _window_bounds(now)
    rows = _load_parent_threads(store, start_iso=start_iso, end_iso=end_iso)
    evidence = _evidence_by_uid(rows)
    graph = social_semantic.get_topic_aggregates(
        from_date=start_iso,
        to_date=end_iso,
        limit=MAX_CANDIDATE_CLUSTERS,
    )
    candidates = []
    for item in _as_list(graph.get("items"))[:MAX_CANDIDATE_CLUSTERS]:
        topic = _trimmed(item.get("topic"))
        activity_uids = [uid for uid in _as_list(item.get("activityUids")) if _trimmed(uid) in evidence]
        topic_key = topic.lower()
        if len(activity_uids) < MAX_EVIDENCE_PER_CLUSTER:
            for uid, ev in evidence.items():
                if uid in activity_uids:
                    continue
                if topic_key and topic_key in {value.lower() for value in _as_list(ev.get("topics"))}:
                    activity_uids.append(uid)
                if len(activity_uids) >= MAX_EVIDENCE_PER_CLUSTER:
                    break
        selected_evidence = [evidence[uid] for uid in activity_uids[:MAX_EVIDENCE_PER_CLUSTER]]
        if not topic or not selected_evidence:
            continue
        candidates.append(
            {
                "topic": topic,
                "count": int(item.get("count") or 0),
                "growth_pct": item.get("growthPct"),
                "dominant_sentiment": item.get("dominantSentiment") or "neutral",
                "sentiment_counts": item.get("sentimentCounts") or {},
                "top_entities": item.get("topEntities") or [],
                "evidence": selected_evidence,
            }
        )
    return {
        "window": {"from": start_iso, "to": end_iso},
        "parentThreads": rows,
        "candidateClusters": candidates,
        "evidenceByUid": evidence,
    }


def _request_ai_synthesis(candidates: dict[str, Any]) -> dict[str, Any]:
    if not config.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured")
    client = OpenAI(api_key=config.OPENAI_API_KEY)
    payload = {
        "prompt_version": PROMPT_VERSION,
        "window": candidates.get("window"),
        "candidate_clusters": candidates.get("candidateClusters") or [],
    }
    started_at = time.perf_counter()
    response = client.chat.completions.create(
        model=config.SOCIAL_ANALYSIS_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
        max_completion_tokens=MAX_COMPLETION_TOKENS,
        timeout=config.AI_REQUEST_TIMEOUT_SECONDS,
    )
    log_openai_usage(
        feature="social_ai_briefs",
        model=config.SOCIAL_ANALYSIS_MODEL,
        response=response,
        started_at=started_at,
        extra={"candidate_clusters": len(payload["candidate_clusters"])},
    )
    raw = _trimmed(response.choices[0].message.content)
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        raw = raw[start : end + 1]
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Social AI brief response was not a JSON object")
    return parsed


def _normalize_family(value: Any) -> str | None:
    clean = _trimmed(value)
    return ALLOWED_FAMILIES.get(clean.lower())


def _confidence(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except Exception:
        return 0.0


def _valid_evidence_ids(value: Any, evidence_ids: set[str]) -> list[str]:
    output = []
    for item in _as_list(value):
        clean = _trimmed(item)
        if clean and clean in evidence_ids and clean not in output:
            output.append(clean)
    return output


def _normalize_sentiment(value: Any) -> str:
    clean = _trimmed(value).lower()
    if clean in {"positive", "negative", "neutral", "mixed", "urgent", "sarcastic"}:
        return clean
    return "neutral"


def validate_social_ai_brief_output(raw: dict[str, Any], *, evidence_by_uid: dict[str, Any]) -> dict[str, Any]:
    evidence_ids = set(evidence_by_uid.keys())
    seen_cards: set[str] = set()
    intent_cards = []
    rejected = Counter()
    for item in _as_list(raw.get("intentCards")):
        if not isinstance(item, dict):
            rejected["invalid_shape"] += 1
            continue
        family = _normalize_family(item.get("family"))
        confidence = _confidence(item.get("confidence"))
        ids = _valid_evidence_ids(item.get("evidence_ids"), evidence_ids)
        title_en = _trimmed(item.get("title_en"))
        title_ru = _trimmed(item.get("title_ru"))
        summary_en = _trimmed(item.get("summary_en"))
        summary_ru = _trimmed(item.get("summary_ru"))
        if not family:
            rejected["invalid_family"] += 1
            continue
        if confidence < MIN_CONFIDENCE:
            rejected["low_confidence"] += 1
            continue
        if not ids:
            rejected["missing_evidence"] += 1
            continue
        if not (title_en and title_ru and summary_en and summary_ru):
            rejected["missing_bilingual_text"] += 1
            continue
        dedupe_key = f"{family}:{title_en.lower()}"
        if dedupe_key in seen_cards:
            rejected["duplicate"] += 1
            continue
        seen_cards.add(dedupe_key)
        quotes = [_trimmed(value) for value in _as_list(item.get("evidence_quotes")) if _trimmed(value)]
        if not quotes:
            quotes = [_trimmed(evidence_by_uid[uid].get("quote")) for uid in ids[:2] if _trimmed(evidence_by_uid[uid].get("quote"))]
        intent_cards.append(
            {
                "family": family,
                "intent": family,
                "title_en": title_en,
                "title_ru": title_ru,
                "summary_en": summary_en,
                "summary_ru": summary_ru,
                "main_topic": _trimmed(item.get("main_topic")) or "General Discussion",
                "sentiment": _normalize_sentiment(item.get("sentiment")),
                "signal_count": max(1, int(item.get("signal_count") or len(ids))),
                "count": max(1, int(item.get("signal_count") or len(ids))),
                "trend_pct": float(item.get("trend_pct") or 0),
                "delta": float(item.get("trend_pct") or 0),
                "confidence": confidence,
                "evidence_ids": ids,
                "evidence_quotes": quotes[:3],
                "examples": quotes[:3],
            }
        )

    def _quotes_for_item(item: dict[str, Any], ids: list[str]) -> list[str]:
        quotes = [_trimmed(value) for value in _as_list(item.get("evidence_quotes")) if _trimmed(value)]
        if not quotes:
            quotes = [
                _trimmed(evidence_by_uid[uid].get("quote"))
                for uid in ids[:3]
                if _trimmed(evidence_by_uid[uid].get("quote"))
            ]
        return quotes[:3]

    def _validate_light(items: Any, *, question: bool = False) -> list[dict[str, Any]]:
        output = []
        seen: set[str] = set()
        for item in _as_list(items):
            if not isinstance(item, dict):
                continue
            confidence = _confidence(item.get("confidence"))
            ids = _valid_evidence_ids(item.get("evidence_ids"), evidence_ids)
            if confidence < MIN_CONFIDENCE or not ids:
                continue
            title_en = _trimmed(item.get("question_en" if question else "title_en"))
            title_ru = _trimmed(item.get("question_ru" if question else "title_ru"))
            if not title_en or not title_ru:
                continue
            key = title_en.lower()
            if key in seen:
                continue
            seen.add(key)
            if question:
                output.append(
                    {
                        "question": title_en,
                        "question_en": title_en,
                        "question_ru": title_ru,
                        "topic": _trimmed(item.get("topic")),
                        "count": max(1, int(item.get("count") or len(ids))),
                        "confidence": confidence,
                        "evidence_ids": ids,
                    }
                )
            else:
                family = _normalize_family(item.get("family") or item.get("intent"))
                if not family:
                    continue
                quotes = _quotes_for_item(item, ids)
                if not quotes:
                    continue
                try:
                    signal_count = max(1, int(item.get("signal_count") or len(ids)))
                except Exception:
                    signal_count = len(ids)
                output.append(
                    {
                        "family": family,
                        "intent": family,
                        "title_en": title_en,
                        "title_ru": title_ru,
                        "summary_en": _trimmed(item.get("summary_en")),
                        "summary_ru": _trimmed(item.get("summary_ru")),
                        "main_topic": _trimmed(item.get("main_topic")) or "General Discussion",
                        "sentiment": _normalize_sentiment(item.get("sentiment")),
                        "signal_count": signal_count,
                        "count": signal_count,
                        "trend_pct": float(item.get("trend_pct") or 0),
                        "delta": float(item.get("trend_pct") or 0),
                        "confidence": confidence,
                        "evidence_ids": ids,
                        "evidence_quotes": quotes,
                        "examples": quotes,
                    }
                )
        return output

    return {
        "intentCards": intent_cards,
        "topSignals": _validate_light(raw.get("topSignals")),
        "topQuestions": _validate_light(raw.get("topQuestions"), question=True),
        "diagnostics": {"rejected": dict(rejected), "publishedIntentCards": len(intent_cards)},
    }


def refresh_social_ai_briefs(store: Any, *, force: bool = False, now: datetime | None = None) -> dict[str, Any]:
    eligibility = should_refresh_social_ai_briefs(store, force=force, now=now)
    if not eligibility.get("eligible"):
        return {"status": "skipped", **eligibility}

    candidates = build_social_ai_brief_candidates(store, now=now)
    if not candidates["candidateClusters"]:
        return {"status": "skipped", "reason": "no_candidate_clusters", **eligibility}

    raw = _request_ai_synthesis(candidates)
    validated = validate_social_ai_brief_output(raw, evidence_by_uid=candidates["evidenceByUid"])
    included_uids = sorted(candidates["evidenceByUid"].keys())
    snapshot = {
        "status": "ready",
        "version": 1,
        "generatedAt": _iso(now or _utc_now()),
        "intentCards": validated["intentCards"],
        "topSignals": validated["topSignals"],
        "topQuestions": validated["topQuestions"],
        "metadata": {
            "generatedAt": _iso(now or _utc_now()),
            "window": candidates["window"],
            "includedActivityUids": included_uids,
            "newProcessedParentThreads": int(eligibility.get("newProcessedParentThreads") or len(included_uids)),
            "model": config.SOCIAL_ANALYSIS_MODEL,
            "promptVersion": PROMPT_VERSION,
            "candidateClusters": len(candidates["candidateClusters"]),
            "diagnostics": {
                **validated["diagnostics"],
                "eligibility": eligibility,
            },
        },
    }
    _save_snapshot(store, snapshot)
    signal_history = _append_signal_history(store, snapshot)
    snapshot["metadata"]["signalHistoryPoints"] = len(signal_history)
    _save_snapshot(store, snapshot)
    return {"status": "refreshed", "snapshot": snapshot}
