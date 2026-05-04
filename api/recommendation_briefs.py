"""AI-generated Recommendation Cards with evidence grounding."""

from __future__ import annotations

import json
import hashlib
import re
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any

from loguru import logger

import config
from api.analysis_lenses import active_analysis_lens_payload, build_lens_system_prompt
from api.admin_runtime import get_admin_prompt, get_admin_runtime_value
from api.queries import network
from buffer.supabase_writer import SupabaseWriter
from utils.ai_usage import log_openai_usage

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore[assignment]


_cache_lock = threading.Lock()
_cached_recommendations: list[dict] = []
_cache_ts: float = 0.0

_runtime_store_lock = threading.Lock()
_runtime_store: SupabaseWriter | None = None

_SNAPSHOT_FOLDER = "recommendation_cards/snapshots"
_STATE_FOLDER = "recommendation_cards/state"
_SCHEMA_VERSION = 1

_client = OpenAI(api_key=config.OPENAI_API_KEY) if (OpenAI and config.OPENAI_API_KEY) else None

RECOMMENDATION_EXTRACTION_PROMPT = """
You extract product/service recommendations from community messages.

Rules:
1) Extract only explicit recommendations (not just mentions)
2) Identify WHAT is being recommended
3) Include WHY it's recommended if mentioned
4) Use only provided evidence text and IDs
5) Categorize appropriately
6) Set confidence based on clarity

Return JSON only:
{
  "recommendations": [
    {
      "item": "Service/product being recommended",
      "reason": "Why it's recommended (if mentioned)",
      "category": "Category (Housing/Work/Services/etc)",
      "confidence": "high|medium|low",
      "evidenceId": "message_id"
    }
  ]
}
""".strip()

ADMIN_PROMPT_DEFAULTS = {
    "recommendation_briefs.extraction_prompt": RECOMMENDATION_EXTRACTION_PROMPT,
}

# Patterns that indicate a recommendation
_RECOMMENDATION_PATTERNS = (
    "recommend",
    "suggest",
    "try",
    "should use",
    "best option",
    "go with",
    "советую",
    "рекомендую",
    "попробуй",
    "лучше",
    "стоит",
    "используй",
    "обратись",
)

# Noise markers to filter out
_NOISE_MARKERS = (
    "не советую",
    "не рекомендую",
    "don't recommend",
    "avoid",
    "stay away",
    "worst",
    "terrible",
    "scam",
)


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


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _clamp(num: float, low: float, high: float) -> float:
    return max(low, min(high, num))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_runtime_store() -> SupabaseWriter | None:
    global _runtime_store
    with _runtime_store_lock:
        if _runtime_store is not None:
            return _runtime_store
        try:
            _runtime_store = SupabaseWriter()
        except Exception as e:
            logger.warning(f"Recommendation cards runtime store unavailable: {e}")
            _runtime_store = None
    return _runtime_store


def _load_runtime_json(path: str, default: dict | None = None) -> dict:
    fallback = default if isinstance(default, dict) else {}
    store = _get_runtime_store()
    if not store:
        return dict(fallback)
    return store.get_runtime_json(path, default=fallback)


def _save_runtime_json(path: str, payload: dict) -> bool:
    store = _get_runtime_store()
    if not store:
        return False
    return store.save_runtime_json(path, payload)


def get_admin_prompt_defaults() -> dict[str, str]:
    return dict(ADMIN_PROMPT_DEFAULTS)


def _runtime_prompt(key: str, default: str) -> str:
    return get_admin_prompt(key, default)


def _runtime_recommendation_model() -> str:
    value = get_admin_runtime_value("openaiModel", config.OPENAI_MODEL)
    text = _as_str(value, "").strip()
    return text or config.OPENAI_MODEL


def _slugify(value: str) -> str:
    text = _as_str(value, "").lower()
    out = []
    last_dash = False
    for ch in text:
        if ch.isalnum():
            out.append(ch)
            last_dash = False
        elif not last_dash:
            out.append("-")
            last_dash = True
    return "".join(out).strip("-")[:60]


def _trim_text(text: Any, max_chars: int) -> str:
    value = _as_str(text, "").strip()
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 3] + "..."


def _has_recommendation_pattern(text: str) -> bool:
    """Check if text contains recommendation patterns."""
    text_lower = text.lower()

    # Check for noise markers (negative recommendations)
    for noise in _NOISE_MARKERS:
        if noise in text_lower:
            return False

    # Check for positive recommendation patterns
    for pattern in _RECOMMENDATION_PATTERNS:
        if pattern in text_lower:
            return True

    return False


def _cache_valid(now: float) -> bool:
    ttl = max(60, int(getattr(config, "RECOMMENDATION_BRIEFS_CACHE_TTL_SECONDS", 300)))
    return (now - _cache_ts) < ttl


def _fetch_recommendation_candidates() -> list[dict]:
    """Query posts and comments that might contain recommendations."""
    from api.db import run_query

    # Get posts and comments with Support/Help intent or recommendation patterns
    results = run_query("""
        // Find comments with recommendation patterns
        MATCH (c:Comment)
        WHERE c.text IS NOT NULL
          AND c.posted_at > datetime() - duration('P30D')
          AND (
              c.text =~ '(?i).*(recommend|suggest|try|советую|рекомендую).*'
              OR EXISTS((c)<-[:WROTE]-(:User)-[:EXHIBITS]->(:Intent {name: 'Support / Help'}))
          )
        OPTIONAL MATCH (c)<-[:WROTE]-(u:User)
        OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)-[:IN_CHANNEL]->(ch:Channel)
        OPTIONAL MATCH (c)-[:TAGGED]->(t:Topic)

        RETURN 'comment' AS kind,
               c.uuid AS id,
               c.text AS message,
               c.posted_at AS timestamp,
               ch.title AS channel,
               u.telegram_user_id AS userId,
               u.username AS username,
               collect(DISTINCT t.name)[..3] AS topics
        ORDER BY c.posted_at DESC
        LIMIT 200

        UNION ALL

        // Find posts with recommendation patterns
        MATCH (p:Post)
        WHERE p.text IS NOT NULL
          AND p.posted_at > datetime() - duration('P30D')
          AND p.text =~ '(?i).*(recommend|suggest|try|советую|рекомендую).*'
        OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
        OPTIONAL MATCH (p)-[:TAGGED]->(t:Topic)

        RETURN 'post' AS kind,
               p.uuid AS id,
               p.text AS message,
               p.posted_at AS timestamp,
               ch.title AS channel,
               null AS userId,
               null AS username,
               collect(DISTINCT t.name)[..3] AS topics
        ORDER BY p.posted_at DESC
        LIMIT 200
    """)

    # Filter for actual recommendations
    candidates = []
    for row in results:
        message = _as_str(row.get("message"), "")
        if _has_recommendation_pattern(message):
            candidates.append({
                "kind": _as_str(row.get("kind"), "message"),
                "id": _as_str(row.get("id"), ""),
                "message": message,
                "timestamp": _as_str(row.get("timestamp"), ""),
                "channel": _as_str(row.get("channel"), "unknown"),
                "userId": _as_str(row.get("userId"), ""),
                "username": _as_str(row.get("username"), ""),
                "topics": row.get("topics", []),
            })

    return candidates


def _extract_recommendations_ai(candidates: list[dict]) -> list[dict]:
    """Use AI to extract actual recommendations from candidates."""
    if not _client or not config.FEATURE_QUESTION_BRIEFS_AI:
        # Fallback: use simple pattern matching
        recommendations = []
        for candidate in candidates[:20]:
            if _has_recommendation_pattern(candidate["message"]):
                recommendations.append({
                    "id": f"rec-{candidate['id'][:8]}",
                    "item": f"Recommendation from {candidate.get('channel', 'community')}",
                    "category": candidate.get("topics", ["General"])[0] if candidate.get("topics") else "General",
                    "evidenceId": candidate["id"],
                    "evidenceText": _trim_text(candidate["message"], 200),
                    "channel": candidate.get("channel", "unknown"),
                    "timestamp": candidate.get("timestamp", ""),
                    "confidence": "medium",
                })
        return recommendations

    system_prompt = build_lens_system_prompt(
        _runtime_prompt("recommendation_briefs.extraction_prompt", RECOMMENDATION_EXTRACTION_PROMPT),
        include_directive=True,
    )

    recommendations = []

    # Process in batches of 10
    for i in range(0, min(50, len(candidates)), 10):
        batch = candidates[i:i+10]
        payload = {
            **active_analysis_lens_payload(include_lenses=False),
            "messages": [
                {
                    "id": c["id"],
                    "text": _trim_text(c["message"], 300),
                    "channel": c["channel"],
                    "topics": c.get("topics", [])
                }
                for c in batch
            ]
        }

        try:
            request_started_at = time.perf_counter()
            response = _client.chat.completions.create(
                model=_runtime_recommendation_model(),
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                response_format={"type": "json_object"},
                max_completion_tokens=1000,
                timeout=config.AI_REQUEST_TIMEOUT_SECONDS,
            )
            log_openai_usage(
                feature="recommendation_briefs",
                model=_runtime_recommendation_model(),
                response=response,
                started_at=request_started_at,
                extra={"max_completion_tokens": 1000},
            )

            raw = _as_str(response.choices[0].message.content)
            parsed = json.loads(raw) if raw else {}

            for rec in parsed.get("recommendations", []):
                if not isinstance(rec, dict):
                    continue

                # Find the original candidate for this recommendation
                evidence_id = _as_str(rec.get("evidenceId"), "")
                original = next((c for c in batch if c["id"] == evidence_id), None)
                if not original:
                    continue

                recommendations.append({
                    "id": f"rec-{hashlib.md5(evidence_id.encode()).hexdigest()[:8]}",
                    "item": _as_str(rec.get("item"), "Unknown recommendation"),
                    "reason": _as_str(rec.get("reason"), ""),
                    "category": _as_str(rec.get("category"), "General"),
                    "evidenceId": evidence_id,
                    "evidenceText": _trim_text(original["message"], 200),
                    "channel": original.get("channel", "unknown"),
                    "timestamp": original.get("timestamp", ""),
                    "username": original.get("username", ""),
                    "confidence": _as_str(rec.get("confidence"), "medium"),
                })

        except Exception as e:
            logger.warning(f"AI extraction failed for batch {i}: {e}")

    return recommendations


def _aggregate_recommendations(recommendations: list[dict]) -> list[dict]:
    """Aggregate similar recommendations and count mentions."""
    # Group by similar items
    aggregated = {}

    for rec in recommendations:
        item_key = _slugify(rec["item"])[:30]  # Simplified key for grouping

        if item_key not in aggregated:
            aggregated[item_key] = {
                "item": rec["item"],
                "category": rec["category"],
                "mentions": 0,
                "evidence": [],
                "channels": set(),
                "latestAt": rec["timestamp"],
            }

        aggregated[item_key]["mentions"] += 1
        aggregated[item_key]["evidence"].append({
            "id": rec["evidenceId"],
            "text": rec["evidenceText"],
            "channel": rec["channel"],
            "timestamp": rec["timestamp"],
        })
        aggregated[item_key]["channels"].add(rec["channel"])

        # Update latest timestamp
        if rec["timestamp"] > aggregated[item_key]["latestAt"]:
            aggregated[item_key]["latestAt"] = rec["timestamp"]

    # Convert to list format
    result = []
    for key, data in aggregated.items():
        result.append({
            "item": data["item"],
            "category": data["category"],
            "mentions": data["mentions"],
            "channelCount": len(data["channels"]),
            "evidence": data["evidence"][:3],  # Keep top 3 evidence
            "latestAt": data["latestAt"],
            "rating": min(5, 3 + data["mentions"] // 3),  # Calculate rating
            "sentiment": "positive",  # Recommendations are generally positive
        })

    # Sort by mentions
    result.sort(key=lambda x: x["mentions"], reverse=True)

    return result[:20]  # Return top 20


def refresh_recommendation_briefs(*, force: bool = False) -> list[dict]:
    """Materialize recommendation cards."""
    global _cached_recommendations, _cache_ts

    logger.info("Refreshing recommendation briefs...")

    try:
        # Fetch candidates
        candidates = _fetch_recommendation_candidates()
        logger.info(f"Found {len(candidates)} recommendation candidates")

        if not candidates:
            _cached_recommendations = []
            _cache_ts = time.time()
            return []

        # Extract recommendations using AI
        recommendations = _extract_recommendations_ai(candidates)
        logger.info(f"Extracted {len(recommendations)} recommendations")

        # Aggregate similar recommendations
        aggregated = _aggregate_recommendations(recommendations)
        logger.info(f"Aggregated to {len(aggregated)} unique recommendations")

        # Cache results
        with _cache_lock:
            _cached_recommendations = aggregated
            _cache_ts = time.time()

        # Save snapshot
        snapshot_path = f"{_SNAPSHOT_FOLDER}/latest.json"
        _save_runtime_json(snapshot_path, {
            "version": _SCHEMA_VERSION,
            "timestamp": _now_iso(),
            "recommendations": aggregated,
        })

        return aggregated

    except Exception as e:
        logger.error(f"Failed to refresh recommendation briefs: {e}")
        return []


def get_recommendation_briefs(*, force_refresh: bool = False) -> list[dict]:
    """Get recommendation cards (cached or refreshed)."""
    global _cached_recommendations, _cache_ts

    if force_refresh:
        return refresh_recommendation_briefs(force=True)

    now = time.time()
    with _cache_lock:
        if _cached_recommendations and _cache_valid(now):
            return list(_cached_recommendations)

    # Try to load from snapshot
    snapshot_path = f"{_SNAPSHOT_FOLDER}/latest.json"
    snapshot = _load_runtime_json(snapshot_path)

    if snapshot and isinstance(snapshot.get("recommendations"), list):
        with _cache_lock:
            _cached_recommendations = snapshot["recommendations"]
            _cache_ts = now
        return list(_cached_recommendations)

    # Refresh if no cache
    return refresh_recommendation_briefs()


def invalidate_recommendation_briefs_cache() -> None:
    """Clear in-process cache."""
    global _cached_recommendations, _cache_ts
    with _cache_lock:
        _cached_recommendations = []
        _cache_ts = 0.0
