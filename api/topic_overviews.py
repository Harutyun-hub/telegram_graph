"""Prompt-driven topic overview generation with cached runtime persistence."""

from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
from datetime import datetime, timezone
from typing import Any

from loguru import logger

import config
from api.admin_runtime import get_admin_prompt, get_admin_runtime_value
from api.dashboard_dates import DashboardDateContext
from buffer.supabase_writer import SupabaseWriter

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore[assignment]


TOPIC_OVERVIEWS_SYNTHESIS_PROMPT = """
You generate a concise, evidence-grounded, insight-rich overview for one community topic.

The user already sees the chart, mentions, growth, sentiment, and top channels on the page.
Your job is not to repeat those stats. Your job is to explain what the conversation is actually about, what patterns are emerging, and what matters in the discussion.

Rules:
1) Use only the provided evidence, question-style evidence, and metrics. Do not invent causes, actors, numbers, or conclusions not supported by the evidence.
2) Do not restate raw numbers, percentages, trend lines, or channel names unless they are essential to explain a meaningful development that is not otherwise obvious.
3) Prioritize insight over recap. Focus on the dominant narrative, repeated complaints or concerns, visible tensions, expectations, unresolved questions, and what these signals suggest about the topic.
4) Synthesize across posts and comments. Highlight recurring patterns, not isolated claims from a single message.
5) Keep the tone analytical, professional, and useful to a decision-maker.
6) summaryEn and summaryRu should each be one short paragraph, maximum 2 sentences. They should tell the reader what this topic is really about right now and why it matters.
7) signalsEn and signalsRu must each contain exactly 3 short bullets. Every bullet must contain a distinct insight, not a reformatted statistic.
8) Avoid generic bullets such as "discussion remains active", "negative sentiment dominates", "mentions fell", or other observations the user can already see in the UI.
9) If the evidence is mixed, contradictory, or weak, describe the tension or uncertainty instead of forcing a strong conclusion.
10) Russian output must be natural and professional, not a literal translation.

Return JSON only:
{
  "overview": {
    "summaryEn": "string",
    "summaryRu": "string",
    "signalsEn": ["string", "string", "string"],
    "signalsRu": ["string", "string", "string"]
  }
}
""".strip()

ADMIN_PROMPT_DEFAULTS = {
    "topic_overviews.synthesis_prompt": TOPIC_OVERVIEWS_SYNTHESIS_PROMPT,
}

_runtime_store_lock = threading.Lock()
_runtime_store: SupabaseWriter | None = None
_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, dict]] = {}
_client = OpenAI(api_key=config.OPENAI_API_KEY) if (OpenAI and config.OPENAI_API_KEY) else None


def _as_str(value: Any, default: str = "") -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return default
    return str(value)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _trim_text(value: Any, limit: int) -> str:
    text = _as_str(value, "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _window_payload(ctx: DashboardDateContext) -> dict[str, Any]:
    return {
        "from": ctx.from_date.isoformat(),
        "to": ctx.to_date.isoformat(),
        "days": int(ctx.days),
    }


def _cache_key(topic: str, category: str, ctx: DashboardDateContext) -> str:
    return f"{ctx.from_date.isoformat()}:{ctx.to_date.isoformat()}:{topic.strip().lower()}:{category.strip().lower()}"


def _storage_path(topic: str, category: str, ctx: DashboardDateContext) -> str:
    digest = hashlib.sha256(_cache_key(topic, category, ctx).encode("utf-8")).hexdigest()[:24]
    return f"topic_overviews/generated/{digest}.json"


def _get_runtime_store() -> SupabaseWriter | None:
    global _runtime_store
    with _runtime_store_lock:
        if _runtime_store is not None:
            return _runtime_store
        try:
            _runtime_store = SupabaseWriter()
        except Exception as exc:
            logger.warning(f"Topic overview runtime store unavailable: {exc}")
            _runtime_store = None
    return _runtime_store


def _runtime_prompt() -> str:
    return get_admin_prompt("topic_overviews.synthesis_prompt", TOPIC_OVERVIEWS_SYNTHESIS_PROMPT)


def _runtime_model() -> str:
    value = get_admin_runtime_value("topicOverviewsModel", config.TOPIC_OVERVIEWS_MODEL)
    text = _as_str(value, config.TOPIC_OVERVIEWS_MODEL).strip()
    return text or config.TOPIC_OVERVIEWS_MODEL


def _runtime_prompt_version() -> str:
    value = get_admin_runtime_value("topicOverviewsPromptVersion", config.TOPIC_OVERVIEWS_PROMPT_VERSION)
    text = _as_str(value, config.TOPIC_OVERVIEWS_PROMPT_VERSION).strip()
    return text or config.TOPIC_OVERVIEWS_PROMPT_VERSION


def _runtime_feature_enabled() -> bool:
    value = get_admin_runtime_value("featureTopicOverviewsAi", config.FEATURE_TOPIC_OVERVIEWS_AI)
    if isinstance(value, bool):
        return value
    return bool(config.FEATURE_TOPIC_OVERVIEWS_AI)


def get_topic_overviews_refresh_minutes() -> int:
    value = get_admin_runtime_value("topicOverviewsRefreshMinutes", str(config.TOPIC_OVERVIEWS_REFRESH_MINUTES))
    try:
        return max(15, int(str(value).strip()))
    except Exception:
        return max(15, int(config.TOPIC_OVERVIEWS_REFRESH_MINUTES))


def get_admin_prompt_defaults() -> dict[str, str]:
    return dict(ADMIN_PROMPT_DEFAULTS)


def invalidate_topic_overviews_cache() -> None:
    with _cache_lock:
        _cache.clear()


def _cache_valid(saved_at: float) -> bool:
    return (time.time() - saved_at) < max(60, int(config.TOPIC_OVERVIEWS_CACHE_TTL_SECONDS))


def _load_cached_item(cache_key: str) -> dict | None:
    with _cache_lock:
        row = _cache.get(cache_key)
        if row is None:
            return None
        saved_at, payload = row
        if not _cache_valid(saved_at):
            _cache.pop(cache_key, None)
            return None
        return copy.deepcopy(payload)


def _save_cached_item(cache_key: str, payload: dict) -> dict:
    with _cache_lock:
        _cache[cache_key] = (time.time(), copy.deepcopy(payload))
    return copy.deepcopy(payload)


def _load_persisted_item(path: str) -> dict | None:
    store = _get_runtime_store()
    if not store:
        return None
    payload = store.get_runtime_json(path, default={})
    if not isinstance(payload, dict):
        return None
    if not isinstance(payload.get("overview"), dict):
        return None
    return payload["overview"]


def _save_persisted_item(path: str, overview: dict) -> None:
    store = _get_runtime_store()
    if not store:
        return
    payload = {
        "generatedAt": _now_iso(),
        "overview": overview,
    }
    if not store.save_runtime_json(path, payload):
        logger.warning("Topic overview cache persistence failed | path={}", path)


def _support_gate(detail_payload: dict) -> bool:
    evidence_count = len(detail_payload.get("evidence") or []) + len(detail_payload.get("questionEvidence") or [])
    distinct_users = _as_int(detail_payload.get("distinctUsers") or detail_payload.get("userCount") or 0)
    distinct_channels = _as_int(detail_payload.get("distinctChannels") or 0)
    return (
        evidence_count >= int(config.TOPIC_OVERVIEWS_MIN_EVIDENCE)
        and distinct_users >= int(config.TOPIC_OVERVIEWS_MIN_USERS)
        and distinct_channels >= int(config.TOPIC_OVERVIEWS_MIN_CHANNELS)
    )


def _build_evidence_ids(detail_payload: dict) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for section in ("evidence", "questionEvidence"):
        for row in detail_payload.get(section) or []:
            if not isinstance(row, dict):
                continue
            evidence_id = _as_str(row.get("id")).strip()
            if evidence_id and evidence_id not in seen:
                seen.add(evidence_id)
                output.append(evidence_id)
    return output[:12]


def _insufficient_item(topic: str, category: str, detail_payload: dict, ctx: DashboardDateContext) -> dict:
    return {
        "topic": topic,
        "category": category,
        "status": "insufficient_evidence",
        "summaryEn": "",
        "summaryRu": "",
        "signalsEn": [],
        "signalsRu": [],
        "generatedAt": _now_iso(),
        "windowStart": ctx.from_date.isoformat(),
        "windowEnd": ctx.to_date.isoformat(),
        "windowDays": int(ctx.days),
        "evidenceIds": _build_evidence_ids(detail_payload),
    }


def _fallback_item(topic: str, category: str, detail_payload: dict, ctx: DashboardDateContext) -> dict:
    existing = detail_payload.get("overview") if isinstance(detail_payload.get("overview"), dict) else detail_payload
    return {
        "topic": topic,
        "category": category,
        "status": "fallback",
        "summaryEn": _trim_text(existing.get("summaryEn"), 320),
        "summaryRu": _trim_text(existing.get("summaryRu"), 360),
        "signalsEn": [_trim_text(item, 140) for item in (existing.get("signalsEn") or [])[:3]],
        "signalsRu": [_trim_text(item, 160) for item in (existing.get("signalsRu") or [])[:3]],
        "generatedAt": _now_iso(),
        "windowStart": ctx.from_date.isoformat(),
        "windowEnd": ctx.to_date.isoformat(),
        "windowDays": int(ctx.days),
        "evidenceIds": _build_evidence_ids(detail_payload),
    }


def _normalize_ai_item(parsed: dict, topic: str, category: str, detail_payload: dict, ctx: DashboardDateContext) -> dict | None:
    if not isinstance(parsed, dict):
        return None
    overview = parsed.get("overview")
    if not isinstance(overview, dict):
        return None
    summary_en = _trim_text(overview.get("summaryEn"), 320)
    summary_ru = _trim_text(overview.get("summaryRu"), 360)
    signals_en = [_trim_text(item, 140) for item in (overview.get("signalsEn") or []) if _trim_text(item, 140)]
    signals_ru = [_trim_text(item, 160) for item in (overview.get("signalsRu") or []) if _trim_text(item, 160)]
    if not summary_en or not summary_ru or len(signals_en) < 3 or len(signals_ru) < 3:
        return None
    return {
        "topic": topic,
        "category": category,
        "status": "ready",
        "summaryEn": summary_en,
        "summaryRu": summary_ru,
        "signalsEn": signals_en[:3],
        "signalsRu": signals_ru[:3],
        "generatedAt": _now_iso(),
        "windowStart": ctx.from_date.isoformat(),
        "windowEnd": ctx.to_date.isoformat(),
        "windowDays": int(ctx.days),
        "evidenceIds": _build_evidence_ids(detail_payload),
    }


def _chat_json(*, system_prompt: str, user_payload: dict) -> dict:
    if not _client:
        return {}
    response = _client.chat.completions.create(
        model=_runtime_model(),
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
        response_format={"type": "json_object"},
        max_completion_tokens=int(config.TOPIC_OVERVIEWS_MAX_TOKENS),
        timeout=config.AI_REQUEST_TIMEOUT_SECONDS,
    )
    raw = _as_str(response.choices[0].message.content)
    return json.loads(raw) if raw else {}


def get_topic_overview(
    topic_name: str,
    category: str | None = None,
    *,
    detail_payload: dict | None = None,
    ctx: DashboardDateContext | None = None,
) -> dict | None:
    topic = _as_str(topic_name).strip()
    resolved_category = _as_str(category or "General").strip() or "General"
    if not topic:
        return None
    if ctx is None:
        return None

    cache_key = _cache_key(topic, resolved_category, ctx)
    cached = _load_cached_item(cache_key)
    if cached is not None:
        return cached

    path = _storage_path(topic, resolved_category, ctx)
    persisted = _load_persisted_item(path)
    if persisted is not None:
        return _save_cached_item(cache_key, persisted)

    if not isinstance(detail_payload, dict):
        return None

    if not _support_gate(detail_payload):
        overview = _insufficient_item(topic, resolved_category, detail_payload, ctx)
        _save_persisted_item(path, overview)
        return _save_cached_item(cache_key, overview)

    if not _client or not _runtime_feature_enabled():
        overview = _fallback_item(topic, resolved_category, detail_payload, ctx)
        _save_persisted_item(path, overview)
        return _save_cached_item(cache_key, overview)

    payload = {
        "window": _window_payload(ctx),
        "topic": {
            "name": topic,
            "category": resolved_category,
            "mentions": _as_int(detail_payload.get("mentionCount") or detail_payload.get("mentions") or 0),
            "previousMentions": _as_int(detail_payload.get("prev7Mentions") or detail_payload.get("previousMentions") or 0),
            "growthPct": _as_int(detail_payload.get("growth7dPct") or 0),
            "distinctUsers": _as_int(detail_payload.get("distinctUsers") or detail_payload.get("userCount") or 0),
            "distinctChannels": _as_int(detail_payload.get("distinctChannels") or 0),
            "sentiment": {
                "positive": _as_int(detail_payload.get("sentimentPositive") or 0),
                "neutral": _as_int(detail_payload.get("sentimentNeutral") or 0),
                "negative": _as_int(detail_payload.get("sentimentNegative") or 0),
            },
            "topChannels": [_as_str(ch) for ch in (detail_payload.get("topChannels") or [])[:3]],
            "latestAt": _as_str((detail_payload.get("sampleEvidence") or {}).get("timestamp")),
        },
        "evidence": [
            {
                "id": _as_str(ev.get("id"), ""),
                "text": _trim_text(ev.get("text"), 220),
                "channel": _as_str(ev.get("channel"), "unknown"),
                "timestamp": _as_str(ev.get("timestamp"), ""),
            }
            for ev in (detail_payload.get("evidence") or [])[: int(config.TOPIC_OVERVIEWS_EVIDENCE_PER_TOPIC)]
            if isinstance(ev, dict)
        ],
        "questionEvidence": [
            {
                "id": _as_str(ev.get("id"), ""),
                "text": _trim_text(ev.get("text"), 180),
                "channel": _as_str(ev.get("channel"), "unknown"),
                "timestamp": _as_str(ev.get("timestamp"), ""),
            }
            for ev in (detail_payload.get("questionEvidence") or [])[: int(config.TOPIC_OVERVIEWS_QUESTION_LIMIT)]
            if isinstance(ev, dict)
        ],
    }

    try:
        parsed = _chat_json(system_prompt=_runtime_prompt(), user_payload=payload)
        overview = _normalize_ai_item(parsed, topic, resolved_category, detail_payload, ctx)
        if overview is None:
            overview = _fallback_item(topic, resolved_category, detail_payload, ctx)
    except Exception as exc:
        logger.warning("Topic overview generation failed | topic={} category={} error={}", topic, resolved_category, exc)
        overview = _fallback_item(topic, resolved_category, detail_payload, ctx)

    _save_persisted_item(path, overview)
    return _save_cached_item(cache_key, overview)
