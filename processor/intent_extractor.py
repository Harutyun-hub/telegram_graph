"""
intent_extractor.py — Expert-grade behavioral intelligence extraction via the configured OpenAI model.

Expert Panel:
  1. Behavioral Intelligence Analyst     — psychological profile, desires, hidden signals
  2. Graph Database Architect (Neo4j)    — canonical English labels, dedup, clean graph nodes
  3. CIS/Caucasus Social Scientist       — sarcasm detection, collective memory, geopolitical alignment

Strategy:
  - Groups comments by (user_id, channel_id, post_id) for strict post isolation
  - Processes channel posts in strict micro-batches keyed by post_id
  - Returns 13-dimension structured JSON per user batch
  - Full output stored in raw_llm_response JSONB for flexibility
  - Standard columns (primary_intent, sentiment_score, topics, language) kept for Neo4j compat
"""
from __future__ import annotations
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import openai
from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam
from loguru import logger
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import time
import config
from api.admin_runtime import get_admin_prompt, get_admin_runtime_value
from api.runtime_coordinator import get_runtime_coordinator
from utils.ai_usage import log_openai_usage
from utils.taxonomy import TAXONOMY_VERSION, compact_taxonomy_prompt
from utils.topic_normalizer import normalize_model_topics

client = OpenAI(api_key=config.OPENAI_API_KEY)

_OPENAI_CIRCUIT_STATE_KEY = "openai:circuit:v1"
_OPENAI_CIRCUIT_HALF_OPEN_LOCK = "openai-circuit-half-open"
_OPENAI_CIRCUIT_REASON_QUOTA = "insufficient_quota"
_OPENAI_CIRCUIT_REASON_RATE_LIMIT = "rate_limit"
_OPENAI_CIRCUIT_REASON_PROVIDER_ERROR = "provider_error"


class OpenAICircuitOpenError(RuntimeError):
    def __init__(self, *, reason: str, open_until: str | None = None, phase: str = "open") -> None:
        self.reason = str(reason or _OPENAI_CIRCUIT_REASON_PROVIDER_ERROR)
        self.open_until = str(open_until or "").strip() or None
        self.phase = str(phase or "open")
        detail = f"OpenAI circuit is {self.phase}"
        if self.reason:
            detail = f"{detail} ({self.reason})"
        if self.open_until:
            detail = f"{detail} until {self.open_until}"
        super().__init__(detail)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).astimezone(timezone.utc)
    except ValueError:
        return None


def _openai_circuit_enabled() -> bool:
    return bool(getattr(config, "OPENAI_CIRCUIT_BREAKER_ENABLED", True))


def _load_openai_circuit_state() -> dict[str, object] | None:
    raw = get_runtime_coordinator().get_json(_OPENAI_CIRCUIT_STATE_KEY)
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="ignore")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("OpenAI circuit state is not valid JSON; clearing stale value")
            get_runtime_coordinator().delete_json(_OPENAI_CIRCUIT_STATE_KEY)
            return None
        return parsed if isinstance(parsed, dict) else None
    return raw if isinstance(raw, dict) else None


def _base_openai_circuit_ttl_seconds(reason: str) -> int:
    if reason == _OPENAI_CIRCUIT_REASON_QUOTA:
        return max(60, int(config.OPENAI_CIRCUIT_QUOTA_OPEN_SECONDS))
    if reason == _OPENAI_CIRCUIT_REASON_RATE_LIMIT:
        return max(30, int(config.OPENAI_CIRCUIT_RATE_LIMIT_OPEN_SECONDS))
    return max(30, int(config.OPENAI_CIRCUIT_PROVIDER_ERROR_OPEN_SECONDS))


def _next_openai_circuit_ttl_seconds(reason: str, prior_state: dict[str, object] | None = None) -> int:
    base_seconds = _base_openai_circuit_ttl_seconds(reason)
    if not prior_state:
        return base_seconds

    phase = str(prior_state.get("state") or "").strip().lower()
    if phase != "half_open":
        return base_seconds

    previous = int(prior_state.get("open_seconds") or base_seconds)
    multiplied = int(max(base_seconds, round(previous * float(config.OPENAI_CIRCUIT_REOPEN_MULTIPLIER))))
    return min(max(base_seconds, multiplied), int(config.OPENAI_CIRCUIT_MAX_OPEN_SECONDS))


def _persist_openai_circuit_state(
    *,
    state: str,
    reason: str,
    open_seconds: int,
    failure_count: int,
    last_error_code: str | None = None,
    last_error_message: str | None = None,
) -> dict[str, object]:
    now = _utc_now()
    open_until = now + timedelta(seconds=max(1, int(open_seconds)))
    payload: dict[str, object] = {
        "state": state,
        "reason": reason,
        "opened_at": _serialize_timestamp(now),
        "open_until": _serialize_timestamp(open_until),
        "open_seconds": int(open_seconds),
        "failure_count": max(1, int(failure_count)),
        "last_error_code": str(last_error_code or "").strip() or None,
        "last_error_message": str(last_error_message or "").strip() or None,
    }
    ttl_seconds = min(
        int(config.OPENAI_CIRCUIT_MAX_OPEN_SECONDS) + int(config.OPENAI_CIRCUIT_HALF_OPEN_TTL_SECONDS) + 60,
        max(int(open_seconds) + int(config.OPENAI_CIRCUIT_HALF_OPEN_TTL_SECONDS) + 60, 120),
    )
    get_runtime_coordinator().set_json(_OPENAI_CIRCUIT_STATE_KEY, json.dumps(payload), ttl_seconds)
    return payload


def _close_openai_circuit() -> None:
    get_runtime_coordinator().delete_json(_OPENAI_CIRCUIT_STATE_KEY)


def _extract_openai_error_code(error: Exception) -> str | None:
    body = getattr(error, "body", None)
    if isinstance(body, dict):
        err = body.get("error")
        if isinstance(err, dict):
            code = str(err.get("code") or err.get("type") or "").strip()
            if code:
                return code
        code = str(body.get("code") or body.get("type") or "").strip()
        if code:
            return code
    code = str(getattr(error, "code", "") or "").strip()
    return code or None


def _extract_openai_error_message(error: Exception) -> str:
    body = getattr(error, "body", None)
    if isinstance(body, dict):
        err = body.get("error")
        if isinstance(err, dict) and err.get("message"):
            return str(err["message"])
        if body.get("message"):
            return str(body["message"])
    return str(error)


def _classify_openai_provider_failure(error: Exception) -> str | None:
    code = (_extract_openai_error_code(error) or "").lower()
    message = _extract_openai_error_message(error).lower()
    status_code = getattr(error, "status_code", None)
    response = getattr(error, "response", None)
    if status_code is None and response is not None:
        status_code = getattr(response, "status_code", None)

    if code == _OPENAI_CIRCUIT_REASON_QUOTA or "insufficient_quota" in message:
        return _OPENAI_CIRCUIT_REASON_QUOTA

    if isinstance(error, openai.RateLimitError) or status_code == 429:
        return _OPENAI_CIRCUIT_REASON_RATE_LIMIT

    if "rate limit" in message or "too many requests" in message:
        return _OPENAI_CIRCUIT_REASON_RATE_LIMIT

    if isinstance(error, (openai.APIConnectionError, openai.APITimeoutError, openai.InternalServerError)):
        return _OPENAI_CIRCUIT_REASON_PROVIDER_ERROR

    if status_code in {500, 502, 503, 504}:
        return _OPENAI_CIRCUIT_REASON_PROVIDER_ERROR

    if any(token in message for token in ("service unavailable", "server error", "bad gateway", "gateway timeout")):
        return _OPENAI_CIRCUIT_REASON_PROVIDER_ERROR

    return None


def _openai_circuit_counter_name(reason: str) -> tuple[str, int, int] | None:
    if reason == _OPENAI_CIRCUIT_REASON_RATE_LIMIT:
        return (
            "openai:rate_limit",
            int(config.OPENAI_CIRCUIT_RATE_LIMIT_THRESHOLD),
            int(config.OPENAI_CIRCUIT_RATE_LIMIT_WINDOW_SECONDS),
        )
    if reason == _OPENAI_CIRCUIT_REASON_PROVIDER_ERROR:
        return (
            "openai:provider_error",
            int(config.OPENAI_CIRCUIT_PROVIDER_ERROR_THRESHOLD),
            int(config.OPENAI_CIRCUIT_PROVIDER_ERROR_WINDOW_SECONDS),
        )
    return None


def _trip_openai_circuit(reason: str, error: Exception, *, prior_state: dict[str, object] | None = None) -> dict[str, object] | None:
    code = _extract_openai_error_code(error)
    message = _extract_openai_error_message(error)

    if reason == _OPENAI_CIRCUIT_REASON_QUOTA:
        state = _persist_openai_circuit_state(
            state="open",
            reason=reason,
            open_seconds=_next_openai_circuit_ttl_seconds(reason, prior_state),
            failure_count=max(1, int((prior_state or {}).get("failure_count") or 0) + 1),
            last_error_code=code,
            last_error_message=message,
        )
        logger.warning(f"OpenAI circuit opened for quota exhaustion until {state.get('open_until')}")
        return state

    counter_spec = _openai_circuit_counter_name(reason)
    if counter_spec is None:
        return None

    counter_name, threshold, window_seconds = counter_spec
    failure_count = get_runtime_coordinator().increment_window_counter(counter_name, window_seconds)
    if failure_count < threshold:
        return None

    state = _persist_openai_circuit_state(
        state="open",
        reason=reason,
        open_seconds=_next_openai_circuit_ttl_seconds(reason, prior_state),
        failure_count=failure_count,
        last_error_code=code,
        last_error_message=message,
    )
    logger.warning(
        f"OpenAI circuit opened for {reason} after {failure_count} failures in {window_seconds}s "
        f"until {state.get('open_until')}"
    )
    return state


def _prepare_openai_circuit_probe(request_label: str) -> str | None:
    if not _openai_circuit_enabled():
        return None

    coordinator = get_runtime_coordinator()
    state = _load_openai_circuit_state()
    if not state:
        return None

    reason = str(state.get("reason") or _OPENAI_CIRCUIT_REASON_PROVIDER_ERROR)
    now = _utc_now()
    open_until = _parse_timestamp(state.get("open_until"))
    phase = str(state.get("state") or "open").strip().lower() or "open"

    if phase == "half_open":
        probe_until = _parse_timestamp(state.get("probe_until"))
        if probe_until is not None and probe_until > now:
            raise OpenAICircuitOpenError(
                reason=reason,
                open_until=_serialize_timestamp(probe_until),
                phase="half_open",
            )
        phase = "open"

    if open_until is not None and open_until > now:
        raise OpenAICircuitOpenError(
            reason=reason,
            open_until=_serialize_timestamp(open_until),
            phase=phase,
        )

    probe_token = coordinator.acquire_lock(
        _OPENAI_CIRCUIT_HALF_OPEN_LOCK,
        int(config.OPENAI_CIRCUIT_HALF_OPEN_TTL_SECONDS),
    )
    if not probe_token:
        raise OpenAICircuitOpenError(
            reason=reason,
            open_until=_serialize_timestamp(open_until or now),
            phase="half_open",
        )

    probe_until = now + timedelta(seconds=int(config.OPENAI_CIRCUIT_HALF_OPEN_TTL_SECONDS))
    probe_state = dict(state)
    probe_state.update({
        "state": "half_open",
        "probe_started_at": _serialize_timestamp(now),
        "probe_until": _serialize_timestamp(probe_until),
    })
    ttl_seconds = max(int(config.OPENAI_CIRCUIT_HALF_OPEN_TTL_SECONDS) + 60, 120)
    coordinator.set_json(_OPENAI_CIRCUIT_STATE_KEY, json.dumps(probe_state), ttl_seconds)
    logger.warning(f"{request_label}: OpenAI circuit entering half-open probe mode until {probe_state['probe_until']}")
    return probe_token


def _close_openai_circuit_probe(probe_token: str | None, request_label: str) -> None:
    if not probe_token:
        return
    _close_openai_circuit()
    get_runtime_coordinator().release_lock(_OPENAI_CIRCUIT_HALF_OPEN_LOCK, probe_token)
    logger.info(f"{request_label}: OpenAI circuit closed after successful half-open probe")


def _reopen_openai_circuit_from_probe(probe_token: str | None, error: Exception, request_label: str) -> dict[str, object] | None:
    prior_state = _load_openai_circuit_state()
    reason = _classify_openai_provider_failure(error) or _OPENAI_CIRCUIT_REASON_PROVIDER_ERROR
    state = _persist_openai_circuit_state(
        state="open",
        reason=reason,
        open_seconds=_next_openai_circuit_ttl_seconds(reason, prior_state),
        failure_count=max(1, int((prior_state or {}).get("failure_count") or 0) + 1),
        last_error_code=_extract_openai_error_code(error),
        last_error_message=_extract_openai_error_message(error),
    )
    if probe_token:
        get_runtime_coordinator().release_lock(_OPENAI_CIRCUIT_HALF_OPEN_LOCK, probe_token)
    logger.warning(f"{request_label}: OpenAI half-open probe failed; circuit reopened until {state.get('open_until')}")
    return state

_SOCIAL_SENTIMENT_TAGS = {
    "Anxious",
    "Frustrated",
    "Angry",
    "Confused",
    "Hopeful",
    "Trusting",
    "Distrustful",
    "Solidarity",
    "Exhausted",
    "Grief",
}

_DEFAULT_TAGS_BY_SENTIMENT = {
    "positive": ["Hopeful"],
    "negative": ["Frustrated"],
    "urgent": ["Anxious"],
    "sarcastic": ["Distrustful"],
}

_TONE_TO_TAGS = [
    ("anx", "Anxious"),
    ("worr", "Anxious"),
    ("fear", "Anxious"),
    ("frustr", "Frustrated"),
    ("ang", "Angry"),
    ("indignan", "Angry"),
    ("confus", "Confused"),
    ("uncertain", "Confused"),
    ("hope", "Hopeful"),
    ("optim", "Hopeful"),
    ("trust", "Trusting"),
    ("distrust", "Distrustful"),
    ("skeptic", "Distrustful"),
    ("solidar", "Solidarity"),
    ("exhaust", "Exhausted"),
    ("fatigue", "Exhausted"),
    ("grief", "Grief"),
    ("mour", "Grief"),
]

_MULTI_SPACE = re.compile(r"\s+")

# ── System Prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Analyze Telegram user messages and return one strict JSON object.

Purpose:
- extract behavioral intelligence grounded in the text
- keep topics and entities canonical for graph storage
- capture CIS/Caucasus context for Russian and Armenian discourse

Core interpretation rules:
1. All labels, topics, entities, and descriptions must be in English.
2. Keep evidence_quotes in the original language exactly as written.
3. Treat Russian sarcasm or ironic praise as negative unless evidence strongly says otherwise.
4. Treat Armenian understatement as potentially stronger than it sounds.
5. Use the supplied user profile as a strong signal for language, gender, age, and social context.
6. Do not invent facts. If a signal is absent, use null, [] or "unknown".
7. Topics must be canonical English, title case, specific, max 4 words, singular where natural, and deduplicated.
8. Entity names must be canonical English forms.
9. Keep output grounded in the provided messages only.

Allowed values:
- primary_intent: Information Seeking | Opinion Sharing | Emotional Venting | Celebration | Debate / Argumentation | Coordination | Promotion / Spam | Support / Help | Humor / Sarcasm | Observation / Monitoring
- sentiment: Positive | Negative | Neutral | Mixed | Urgent | Sarcastic
- social_sentiment_tags: Anxious | Frustrated | Angry | Confused | Hopeful | Trusting | Distrustful | Solidarity | Exhausted | Grief
- behavioral_pattern.community_role: Leader | Influencer | Engaged_Participant | Passive_Observer | Agitator | Helper | Troll | Lurker | Newcomer | Informant
- behavioral_pattern.communication_style: Formal | Informal | Aggressive | Passive | Analytical | Emotional | Persuasive | Ironic
- social_signals.geopolitical_alignment: Pro_Russia | Pro_West | Pro_Armenia | Pro_Azerbaijan | Nationalist | Anti_Government | Neutral | Ambiguous
- social_signals.migration_intent: Yes | No | Implied
- social_signals.diaspora_signals: Yes | No
- social_signals.authority_attitude: Deferential | Critical | Dismissive | Fearful | Admiring | Humorous
- demographics.language: ru | hy | en | mixed | unknown
- demographics.inferred_gender: male | female | unknown
- demographics.inferred_age_bracket: 13-17 | 18-24 | 25-34 | 35-44 | 45-54 | 55+ | unknown
- business_opportunity.opportunity_type: Business_Idea | Investment_Interest | Job_Seeking | Hiring | Partnership_Request | Market_Gap_Observed | Service_Demand | Product_Demand | Real_Estate | Import_Export | none
- psychographic.locus_of_control: internal | external | mixed
- psychographic.coping_style: action_oriented | resigned | dark_humor | denial | seeking_support
- psychographic.security_vs_freedom: security | freedom | balanced
- trust_landscape.trust_*: low | medium | high | hostile | unknown
- linguistic_intelligence.code_switching: high | medium | low | none
- linguistic_intelligence.certainty_level: dogmatic | confident | uncertain | questioning
- linguistic_intelligence.rhetorical_strategy: emotional | logical | anecdotal | authoritative | humorous | mixed
- linguistic_intelligence.pronoun_pattern: individual | collective | mixed
- financial_signals.financial_distress_level: none | mild | moderate | severe
- financial_signals.price_sensitivity: high | medium | low | unknown

Output schema:
{
  "primary_intent": "<intent>",
  "evidence_quotes": ["<original language verbatim>", "<second quote if available>"],
  "sentiment": "Positive|Negative|Neutral|Mixed|Urgent|Sarcastic",
  "sentiment_score": <-1.0 to 1.0>,
  "emotional_tone": "<precise emotion label>",
  "social_sentiment_tags": ["Anxious|Frustrated|Angry|Confused|Hopeful|Trusting|Distrustful|Solidarity|Exhausted|Grief"],
  "topics": [
    {"name": "<Canonical English Topic>", "importance": "primary|secondary|tertiary", "evidence": "<quote or grounded observation>"}
  ],
  "message_topics": [
    {
      "message_ref": "MSG 1",
      "comment_id": "<comment UUID if provided in input, otherwise null>",
      "topics": [
        {"name": "<Canonical English Topic>", "importance": "primary|secondary|tertiary", "evidence": "<quote or grounded observation>"}
      ]
    }
  ],
  "message_sentiments": [
    {
      "message_ref": "MSG 1",
      "comment_id": "<comment UUID if provided in input, otherwise null>",
      "sentiment": "Positive|Negative|Neutral|Mixed|Urgent|Sarcastic",
      "sentiment_score": <-1.0 to 1.0>
    }
  ],
  "entities": [
    {"name": "<Canonical English Name>", "type": "person|group|organization|place|concept|media", "sentiment_toward": "positive|negative|neutral|ambiguous|fearful|admiring|mocking"}
  ],
  "behavioral_pattern": {
    "community_role": "<role>",
    "communication_style": "<style>"
  },
  "social_signals": {
    "geopolitical_alignment": "<alignment>",
    "collective_memory": "<historical reference or null>",
    "migration_intent": "Yes|No|Implied",
    "diaspora_signals": "Yes|No",
    "authority_attitude": "<attitude>"
  },
  "demographics": {
    "language": "<ISO 639-1>",
    "inferred_gender": "male|female|unknown",
    "inferred_age_bracket": "<bracket>"
  },
  "daily_life": {
    "life_stage_signal": "<life stage inferred or null>"
  },
  "business_opportunity": {
    "opportunity_type": "Business_Idea|Investment_Interest|Job_Seeking|Hiring|Partnership_Request|Market_Gap_Observed|Service_Demand|Product_Demand|Real_Estate|Import_Export|none",
    "description": "<what opportunity or economic signal is present, or null>"
  },
  "psychographic": {
    "soviet_nostalgia": <0.0-1.0>,
    "locus_of_control": "internal|external|mixed",
    "coping_style": "action_oriented|resigned|dark_humor|denial|seeking_support",
    "security_vs_freedom": "security|freedom|balanced"
  },
  "trust_landscape": {
    "trust_government": "low|medium|high|hostile|unknown",
    "trust_media": "low|medium|high|hostile|unknown",
    "trust_peers": "low|medium|high|hostile|unknown",
    "trust_foreign": "low|medium|high|hostile|unknown"
  },
  "linguistic_intelligence": {
    "code_switching": "high|medium|low|none",
    "certainty_level": "dogmatic|confident|uncertain|questioning",
    "rhetorical_strategy": "emotional|logical|anecdotal|authoritative|humorous|mixed",
    "pronoun_pattern": "individual|collective|mixed"
  },
  "financial_signals": {
    "financial_distress_level": "none|mild|moderate|severe",
    "price_sensitivity": "high|medium|low|unknown"
  }
}

Return only strict JSON. No markdown. No explanation."""

STRICT_TAXONOMY_PROMPT = f"""### STRICT TAXONOMY CONTRACT (VERSION {TAXONOMY_VERSION})
You MUST prioritize canonical taxonomy topics. For each topic object:
- Use `taxonomy_topic` when a taxonomy match exists (preferred path)
- Use `proposed_topic` only when no good taxonomy match exists
- Prefer taxonomy topics over proposed topics whenever possible
- You may propose at most ONE topic per analyzed item
- Set `proposed=true` only for non-taxonomy topics
- Always provide `closest_category` and `domain`

Taxonomy reference:
{compact_taxonomy_prompt(max_topics_per_category=4)}

Required topics object shape:
{{
  "name": "<Canonical Or Proposed Name>",
  "taxonomy_topic": "<Canonical topic or null>",
  "proposed_topic": "<Proposed topic or null>",
  "proposed": false,
  "closest_category": "<taxonomy category>",
  "domain": "<taxonomy domain>",
  "importance": "primary|secondary|tertiary",
  "evidence": "<quote or observation>"
}}
"""


def _safe_json_object(raw: str | None) -> dict:
    raw_text = raw.strip() if isinstance(raw, str) else ""
    if not raw_text:
        raise json.JSONDecodeError("empty response", "", 0)
    parsed = json.loads(raw_text)
    if not isinstance(parsed, dict):
        raise ValueError("Model response is not a JSON object")
    return parsed


def _clamp_score(value, default: float = 0.0) -> float:
    try:
        score = float(value)
    except Exception:
        score = default
    return max(-1.0, min(1.0, score))


def _normalize_social_sentiment_tags(parsed: dict) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()

    def _add(tag: str | None) -> None:
        if not tag:
            return
        norm = str(tag).strip().title()
        if norm not in _SOCIAL_SENTIMENT_TAGS or norm in seen:
            return
        seen.add(norm)
        tags.append(norm)

    raw_tags = parsed.get("social_sentiment_tags")
    if isinstance(raw_tags, list):
        for item in raw_tags:
            if isinstance(item, str):
                _add(item)

    tone = str(parsed.get("emotional_tone") or "").strip().lower()
    if tone:
        for needle, tag in _TONE_TO_TAGS:
            if needle in tone:
                _add(tag)

    sentiment = str(parsed.get("sentiment") or "").strip().lower()
    for tag in _DEFAULT_TAGS_BY_SENTIMENT.get(sentiment, []):
        _add(tag)

    return tags


def _trim_text(value: str | None, limit: int) -> str:
    if not value:
        return ""
    text = str(value).strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _normalize_comment_text_key(value: object) -> str:
    text = _MULTI_SPACE.sub(" ", str(value or "").strip()).casefold()
    return text


def _filter_comment_group_comments(comments: list[dict]) -> tuple[list[dict], dict[str, int]]:
    min_length = max(0, int(getattr(config, "AI_MIN_COMMENT_LENGTH", 0)))
    dedupe_enabled = bool(getattr(config, "AI_FILTER_DUPLICATE_COMMENTS", True))
    kept: list[dict] = []
    filtered_stats = {
        "short_comments": 0,
        "duplicate_comments": 0,
    }
    seen_text_keys: set[str] = set()

    for comment in comments:
        text = str(comment.get("text") or "").strip()
        if min_length and len(text) < min_length:
            filtered_stats["short_comments"] += 1
            continue

        if dedupe_enabled:
            key = _normalize_comment_text_key(text)
            if key and key in seen_text_keys:
                filtered_stats["duplicate_comments"] += 1
                continue
            if key:
                seen_text_keys.add(key)

        kept.append(comment)

    return kept, filtered_stats


def _chunked(items: list[dict], size: int) -> list[list[dict]]:
    step = max(1, int(size))
    return [items[i:i + step] for i in range(0, len(items), step)]


def _normalize_payload(parsed: dict) -> dict:
    normalized = dict(parsed)
    normalized_topics = normalize_model_topics(parsed.get("topics") or [])
    normalized["topics"] = normalized_topics

    evidence_quotes = []
    for quote in parsed.get("evidence_quotes") or []:
        if isinstance(quote, str) and quote.strip():
            evidence_quotes.append(quote.strip()[:300])
        if len(evidence_quotes) >= 3:
            break
    normalized["evidence_quotes"] = evidence_quotes

    normalized["sentiment_score"] = _clamp_score(parsed.get("sentiment_score"), 0.0)
    normalized["social_sentiment_tags"] = _normalize_social_sentiment_tags(parsed)

    demographics = parsed.get("demographics")
    if not isinstance(demographics, dict):
        demographics = {}
    demographics.setdefault("language", "unknown")
    demographics.setdefault("inferred_gender", "unknown")
    demographics.setdefault("inferred_age_bracket", "unknown")
    normalized["demographics"] = demographics

    if config.FEATURE_EXTRACTION_V2:
        canonical_count = sum(1 for item in normalized_topics if item.get("taxonomy_topic"))
        proposed_count = sum(1 for item in normalized_topics if item.get("proposed"))
        normalized["extraction_contract"] = {
            "mode": "strict_taxonomy_primary",
            "taxonomy_version": TAXONOMY_VERSION,
            "canonical_topics": canonical_count,
            "proposed_topics": proposed_count,
        }

    message_topics: list[dict] = []
    aggregate_by_name: dict[str, dict] = {str(item.get("name")): dict(item) for item in normalized_topics if item.get("name")}
    for item in parsed.get("message_topics") or []:
        if not isinstance(item, dict):
            continue
        comment_id = str(item.get("comment_id") or "").strip()
        message_ref = str(item.get("message_ref") or "").strip()
        item_topics = normalize_model_topics(item.get("topics") or [])
        if not comment_id and not message_ref:
            continue
        message_topics.append({
            "comment_id": comment_id,
            "message_ref": message_ref,
            "topics": item_topics,
        })
        for topic in item_topics:
            name = str(topic.get("name") or "").strip()
            if name and name not in aggregate_by_name:
                aggregate_by_name[name] = dict(topic)

    if message_topics:
        normalized["message_topics"] = message_topics
        normalized["topics"] = list(aggregate_by_name.values())[:6]

    message_sentiments: list[dict] = []
    seen_message_sentiments: set[tuple[str, str]] = set()
    for item in parsed.get("message_sentiments") or []:
        if not isinstance(item, dict):
            continue
        comment_id = str(item.get("comment_id") or "").strip()
        message_ref = str(item.get("message_ref") or "").strip()
        if not comment_id and not message_ref:
            continue
        key = (comment_id, message_ref)
        if key in seen_message_sentiments:
            continue
        seen_message_sentiments.add(key)
        message_sentiments.append({
            "comment_id": comment_id,
            "message_ref": message_ref,
            "sentiment": _normalize_enum(item.get("sentiment")),
            "sentiment_score": _clamp_score(item.get("sentiment_score"), normalized["sentiment_score"]),
        })

    if message_sentiments:
        normalized["message_sentiments"] = message_sentiments

    return normalized


def _extract_topic_names(parsed: dict) -> list[str]:
    names: list[str] = []
    for item in parsed.get("topics") or []:
        if isinstance(item, dict) and item.get("name"):
            names.append(str(item["name"]))
        elif isinstance(item, str):
            names.append(item)
    return names


def _request_json(*, system_prompt: str, user_context: str, max_tokens: int, request_label: str) -> dict:
    retry_limit = max(0, int(config.AI_REQUEST_MAX_RETRIES))
    retry_backoff_seconds = max(0.0, float(getattr(config, "AI_REQUEST_RETRY_BACKOFF_SECONDS", 0.0)))
    model_name = _runtime_openai_model()
    messages: list[ChatCompletionMessageParam] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_context},
    ]

    for attempt in range(retry_limit + 1):
        probe_token: str | None = None
        try:
            probe_token = _prepare_openai_circuit_probe(request_label)
            attempt_max_tokens = max_tokens + (attempt * 400)
            request_started_at = time.perf_counter()
            response = client.chat.completions.create(
                model=model_name,
                messages=messages,  # pyright: ignore[reportArgumentType]
                response_format={"type": "json_object"},
                max_completion_tokens=attempt_max_tokens,
                timeout=config.AI_REQUEST_TIMEOUT_SECONDS,
            )
            log_openai_usage(
                feature="intent_extractor",
                model=model_name,
                response=response,
                started_at=request_started_at,
                extra={
                    "attempt": attempt + 1,
                    "max_completion_tokens": attempt_max_tokens,
                },
            )
            logger.debug(
                f"{request_label}: AI response received id={getattr(response, 'id', 'unknown')} model={model_name}"
            )
            raw = response.choices[0].message.content
            _close_openai_circuit_probe(probe_token, request_label)
            return _safe_json_object(raw)
        except OpenAICircuitOpenError:
            raise
        except Exception as exc:
            provider_reason = _classify_openai_provider_failure(exc)
            if probe_token:
                if provider_reason:
                    reopened_state = _reopen_openai_circuit_from_probe(probe_token, exc, request_label)
                    raise OpenAICircuitOpenError(
                        reason=str((reopened_state or {}).get("reason") or provider_reason),
                        open_until=str((reopened_state or {}).get("open_until") or ""),
                        phase="open",
                    ) from exc
                _close_openai_circuit_probe(probe_token, request_label)

            if _openai_circuit_enabled() and provider_reason:
                opened_state = _trip_openai_circuit(provider_reason, exc, prior_state=_load_openai_circuit_state())
                if opened_state:
                    raise OpenAICircuitOpenError(
                        reason=str(opened_state.get("reason") or provider_reason),
                        open_until=str(opened_state.get("open_until") or ""),
                        phase="open",
                    ) from exc

            if attempt >= retry_limit:
                raise
            logger.warning(
                f"{request_label}: AI request failed on attempt {attempt + 1} ({type(exc).__name__}) — retrying ({exc})"
            )
            messages.append(
                {
                    "role": "user",
                    "content": (
                        "Return ONLY a strict JSON object matching the schema. "
                        "Do not include markdown, prose, or trailing text."
                    ),
                }
            )
            if retry_backoff_seconds > 0:
                time.sleep(retry_backoff_seconds * (attempt + 1))

    raise RuntimeError(f"{request_label}: AI request retries exhausted")
    return {}


def _comment_scope_key(telegram_user_id, channel_id, post_id) -> str:
    uid = str(telegram_user_id if telegram_user_id is not None else "anonymous")
    cid = str(channel_id or "unknown")
    pid = str(post_id or "unknown")
    return f"{uid}:{cid}:{pid}"


def _record_failure_scope(
    supabase_writer,
    *,
    scope_type: str,
    scope_key: str,
    channel_id: str | None,
    post_id: str | None,
    telegram_user_id,
    error: Exception | str,
) -> None:
    if not hasattr(supabase_writer, "record_processing_failure"):
        return
    try:
        user_id = int(telegram_user_id) if isinstance(telegram_user_id, int) else None
        supabase_writer.record_processing_failure(
            scope_type=scope_type,
            scope_key=scope_key,
            channel_id=channel_id,
            post_id=post_id,
            telegram_user_id=user_id,
            error=str(error),
        )
    except Exception:
        pass


def _clear_failure_scope(supabase_writer, *, scope_type: str, scope_key: str) -> None:
    if not hasattr(supabase_writer, "clear_processing_failure"):
        return
    try:
        supabase_writer.clear_processing_failure(scope_type, scope_key)
    except Exception:
        pass


def _blocked_scope_keys(supabase_writer, *, scope_type: str, scope_keys: list[str]) -> set[str]:
    if not hasattr(supabase_writer, "get_blocked_scopes"):
        return set()
    try:
        return set(supabase_writer.get_blocked_scopes(scope_type, scope_keys) or set())
    except Exception:
        return set()


def _analyze_comment_group_payload(payload: dict) -> dict:
    telegram_user_id = payload.get("telegram_user_id")
    post_id = payload.get("post_id")
    user_context = payload.get("user_context") or ""
    system_prompt = _runtime_prompt("extraction.system_prompt", SYSTEM_PROMPT)
    strict_taxonomy_prompt = _runtime_prompt("extraction.strict_taxonomy_prompt", STRICT_TAXONOMY_PROMPT)

    prompt_candidates = [system_prompt]
    if config.FEATURE_EXTRACTION_V2:
        prompt_candidates = [
            f"{system_prompt}\n\n{strict_taxonomy_prompt}",
            system_prompt,
        ]

    parsed = None
    last_error = None
    for prompt_index, system_prompt in enumerate(prompt_candidates, start=1):
        try:
            parsed = _normalize_payload(
                _request_json(
                    system_prompt=system_prompt,
                    user_context=user_context,
                    max_tokens=max(300, int(config.AI_COMMENT_MAX_TOKENS)),
                    request_label=(
                        f"user {telegram_user_id} post {post_id or 'unknown'} "
                        f"prompt#{prompt_index}"
                    ),
                )
            )
            break
        except Exception as exc:
            last_error = exc
            if prompt_index < len(prompt_candidates):
                logger.warning(
                    f"Comment analysis fallback for user={telegram_user_id} post={post_id}: "
                    f"strict-taxonomy prompt failed ({exc}); retrying with compact base prompt"
                )
            else:
                raise

    if parsed is None:
        raise RuntimeError(f"Comment parsing failed: {last_error}")
    return parsed


# ── Comment Batch Analysis ────────────────────────────────────────────────────

def extract_intents(
    comments: list[dict],
    supabase_writer,
    deadline_epoch: float | None = None,
    *,
    include_stats: bool = False,
) -> int | dict:
    """
    Process unprocessed comments through the configured OpenAI model.
    Groups by (telegram_user_id, channel_id, post_id) — one API call per user per post.

    Returns: number of analysis records saved (default) or detailed stage stats.
    """
    started_at = time.monotonic()
    stats: dict[str, int | float] = {
        "workers": max(1, int(getattr(config, "AI_COMMENT_WORKERS", 1))),
        "inflight_limit": max(1, int(getattr(config, "AI_MAX_INFLIGHT_REQUESTS", 1))),
        "attempted_groups": 0,
        "blocked_groups": 0,
        "filtered_groups": 0,
        "filtered_bot_groups": 0,
        "filtered_short_comments": 0,
        "filtered_duplicate_comments": 0,
        "deferred_groups": 0,
        "succeeded_groups": 0,
        "failed_groups": 0,
        "saved": 0,
    }
    if not comments:
        stats["duration_seconds"] = round(max(0.0, time.monotonic() - started_at), 2)
        return stats if include_stats else 0

    # Prefetch parent post context for better per-post grounding.
    post_ids = [str(comment.get("post_id")) for comment in comments if comment.get("post_id")]
    post_map: dict[str, dict] = {}
    if post_ids:
        try:
            post_map = supabase_writer.get_posts_by_ids(post_ids)
        except Exception as e:
            logger.warning(f"Post context prefetch failed: {e}")

    # Group by (user, channel, post)
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for comment in comments:
        uid = comment.get("telegram_user_id") or "anonymous"
        cid = comment.get("channel_id", "unknown")
        pid = comment.get("post_id")
        groups[(uid, cid, pid)].append(comment)

    profile_cache: dict[int, dict | None] = {}

    def _load_user_profile(telegram_user_id) -> dict | None:
        if telegram_user_id == "anonymous":
            return None
        try:
            user_id = int(telegram_user_id)
        except Exception:
            return None
        if user_id in profile_cache:
            return profile_cache[user_id]
        try:
            profile_cache[user_id] = supabase_writer.get_user_by_telegram_id(user_id)
        except Exception:
            profile_cache[user_id] = None
        return profile_cache[user_id]

    group_payloads: list[dict] = []
    for (telegram_user_id, channel_id, post_id), user_comments in groups.items():
        profile = _load_user_profile(telegram_user_id)
        if bool(getattr(config, "AI_SKIP_BOT_COMMENTS", True)) and profile and bool(profile.get("is_bot")):
            for comment in user_comments:
                supabase_writer.mark_comment_processed(comment["id"])
            stats["filtered_groups"] = int(stats["filtered_groups"]) + 1
            stats["filtered_bot_groups"] = int(stats["filtered_bot_groups"]) + 1
            continue

        filtered_comments, filtered_stats = _filter_comment_group_comments(user_comments)
        stats["filtered_short_comments"] = int(stats["filtered_short_comments"]) + int(filtered_stats["short_comments"])
        stats["filtered_duplicate_comments"] = int(stats["filtered_duplicate_comments"]) + int(filtered_stats["duplicate_comments"])
        filtered_comment_ids = {str(comment.get("id")) for comment in filtered_comments if comment.get("id")}
        for comment in user_comments:
            if str(comment.get("id")) not in filtered_comment_ids:
                supabase_writer.mark_comment_processed(comment["id"])

        if not filtered_comments:
            stats["filtered_groups"] = int(stats["filtered_groups"]) + 1
            continue

        analysis_comments = filtered_comments[:config.AI_BATCH_SIZE]

        # Build numbered temporal message block
        message_char_limit = max(120, int(config.AI_MESSAGE_CHAR_LIMIT))
        messages_text = "\n\n".join([
            (
                f"[MSG {i+1} | COMMENT_ID {c.get('id')} | {c.get('posted_at', '')[:16]}]\n"
                f"{_trim_text(c.get('text', ''), message_char_limit)}"
            )
            for i, c in enumerate(analysis_comments)
        ])

        post_context_section = ""
        if post_id:
            post_context = post_map.get(str(post_id), {})
            post_excerpt = _trim_text(post_context.get("text", ""), max(180, message_char_limit))
            if str(post_context.get("entry_kind") or "").strip().lower() == "thread_anchor":
                post_context_section = (
                    f"\nTHREAD CONTEXT:\n"
                    f"  Thread Anchor ID   : {post_id}\n"
                    f"  Telegram Top ID    : {post_context.get('telegram_message_id')}\n"
                    f"  Root Posted At     : {post_context.get('posted_at')}\n"
                    f"  Thread Messages    : {post_context.get('thread_message_count')}\n"
                    f"  Thread Participants: {post_context.get('thread_participant_count')}\n"
                    f"  Root Excerpt       : {post_excerpt}\n"
                )
            else:
                post_context_section = (
                    f"\nPOST CONTEXT:\n"
                    f"  Post ID            : {post_id}\n"
                    f"  Telegram Message ID: {post_context.get('telegram_message_id')}\n"
                    f"  Posted At          : {post_context.get('posted_at')}\n"
                    f"  Parent Post Excerpt: {post_excerpt}\n"
                )

        # Fetch user profile to enrich AI context
        profile_section = ""
        if profile:
            name_parts = [p for p in [profile.get("first_name"), profile.get("last_name")] if p]
            full_name = " ".join(name_parts) or "Unknown"
            username = profile.get("username") or "no username"
            bio = profile.get("bio") or "none"
            profile_section = (
                f"\nUSER PROFILE (use for precise demographic inference):\n"
                f"  Full Name : {full_name}\n"
                f"  Username  : @{username}\n"
                f"  Bio       : {bio}\n"
                f"  Is Bot    : {profile.get('is_bot', False)}\n"
            )

        scope_key = _comment_scope_key(telegram_user_id, channel_id, post_id)
        source_label = (
            "Telegram public supergroup"
            if str(post_map.get(str(post_id), {}).get("entry_kind") or "").strip().lower() == "thread_anchor"
            else "Telegram public channel"
        )
        user_context = (
            f"Channel: {source_label}\n"
            f"Post ID: {post_id or 'unknown'}\n"
            f"Messages analyzed: {len(analysis_comments)}\n"
            f"User ID: {telegram_user_id}\n"
            f"IMPORTANT: Return message_topics with one entry per message using the COMMENT_ID from each [MSG ...] header. "
            f"Only assign a topic to a message when that specific message clearly mentions it. "
            f"Also return message_sentiments with one entry per message using the same COMMENT_ID values."
            f"{profile_section}"
            f"{post_context_section}\n"
            f"--- MESSAGES ---\n{messages_text}"
        )

        group_payloads.append(
            {
                "telegram_user_id": telegram_user_id,
                "channel_id": channel_id,
                "post_id": post_id,
                "user_comments": user_comments,
                "analysis_comments": analysis_comments,
                "scope_key": scope_key,
                "user_context": user_context,
            }
        )

    blocked = _blocked_scope_keys(
        supabase_writer,
        scope_type="comment_group",
        scope_keys=[payload["scope_key"] for payload in group_payloads],
    )

    runnable_payloads: list[dict] = []
    for payload in group_payloads:
        if payload["scope_key"] in blocked:
            stats["blocked_groups"] = int(stats["blocked_groups"]) + 1
        else:
            runnable_payloads.append(payload)

    stats["attempted_groups"] = len(runnable_payloads)

    def _handle_success(payload: dict, parsed: dict):
        telegram_user_id = payload.get("telegram_user_id")
        channel_id = payload.get("channel_id")
        post_id = payload.get("post_id")
        user_comments = payload.get("user_comments") or []
        scope_key = payload.get("scope_key") or ""

        demographics = parsed.get("demographics") or {}
        content_id = str(post_id) if post_id else None

        if not post_id:
            logger.warning(
                f"Comment analysis fell back to legacy channel scope for user={telegram_user_id} "
                f"channel={channel_id} because post_id is missing"
            )

        analysis = {
            "channel_id": channel_id,
            "telegram_user_id": telegram_user_id if telegram_user_id != "anonymous" else None,
            "content_type": "batch",
            "content_id": content_id,
            "primary_intent": parsed.get("primary_intent"),
            "sentiment_score": parsed.get("sentiment_score"),
            "topics": _extract_topic_names(parsed),
            "language": demographics.get("language"),
            "inferred_gender": demographics.get("inferred_gender", "unknown"),
            "inferred_age_bracket": demographics.get("inferred_age_bracket", "unknown"),
            "raw_llm_response": parsed,
            "neo4j_synced": False,
        }

        supabase_writer.save_analysis(analysis)
        for c in user_comments:
            supabase_writer.mark_comment_processed(c["id"])

        _clear_failure_scope(supabase_writer, scope_type="comment_group", scope_key=scope_key)

        psycho = parsed.get("psychographic", {})
        trust = parsed.get("trust_landscape", {})
        fin = parsed.get("financial_signals", {})
        social = parsed.get("social_signals", {})
        logger.debug(
            f"User {telegram_user_id} | post={post_id or 'unknown'} | "
            f"intent={parsed.get('primary_intent')} | "
            f"sentiment={_clamp_score(parsed.get('sentiment_score'), 0.0):.2f} | "
            f"nostalgia={psycho.get('soviet_nostalgia', '?')} | "
            f"locus={psycho.get('locus_of_control', '?')} | "
            f"trust_gov={trust.get('trust_government', '?')} | "
            f"distress={fin.get('financial_distress_level', '?')} | "
            f"geo={social.get('geopolitical_alignment', '?')}"
        )

        stats["saved"] = int(stats["saved"]) + 1
        stats["succeeded_groups"] = int(stats["succeeded_groups"]) + 1

    def _handle_failure(payload: dict, error: Exception):
        telegram_user_id = payload.get("telegram_user_id")
        channel_id = payload.get("channel_id")
        post_id = payload.get("post_id")
        scope_key = payload.get("scope_key") or ""

        if isinstance(error, OpenAICircuitOpenError):
            logger.warning(
                f"Comment analysis deferred for user {telegram_user_id} post {post_id}: provider circuit {error}"
            )
            stats["blocked_groups"] = int(stats["blocked_groups"]) + 1
            return

        logger.error(f"AI processing error for user {telegram_user_id} post {post_id}: {error}")
        _record_failure_scope(
            supabase_writer,
            scope_type="comment_group",
            scope_key=scope_key,
            channel_id=str(channel_id) if channel_id else None,
            post_id=str(post_id) if post_id else None,
            telegram_user_id=telegram_user_id,
            error=error,
        )
        stats["failed_groups"] = int(stats["failed_groups"]) + 1

    workers = max(1, int(getattr(config, "AI_COMMENT_WORKERS", 1)))
    inflight_limit = max(1, int(getattr(config, "AI_MAX_INFLIGHT_REQUESTS", 1)))
    max_inflight = max(1, min(workers, inflight_limit))

    if workers <= 1:
        for index, payload in enumerate(runnable_payloads):
            if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                stats["deferred_groups"] = int(stats["deferred_groups"]) + (len(runnable_payloads) - index)
                logger.warning("Comment extraction stage deadline reached; deferring remaining users to next cycle")
                break
            try:
                parsed = _analyze_comment_group_payload(payload)
                _handle_success(payload, parsed)
            except Exception as e:
                _handle_failure(payload, e)
    else:
        next_index = 0
        deadline_reached = False

        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending: dict = {}

            def _submit_available():
                nonlocal next_index, deadline_reached
                while next_index < len(runnable_payloads) and len(pending) < max_inflight:
                    if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                        deadline_reached = True
                        return
                    payload = runnable_payloads[next_index]
                    next_index += 1
                    future = executor.submit(_analyze_comment_group_payload, payload)
                    pending[future] = payload

            _submit_available()

            while pending:
                done, _ = wait(list(pending.keys()), timeout=1.0, return_when=FIRST_COMPLETED)
                if not done:
                    if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                        deadline_reached = True
                    continue

                for future in done:
                    payload = pending.pop(future)
                    try:
                        parsed = future.result()
                        _handle_success(payload, parsed)
                    except Exception as e:
                        _handle_failure(payload, e)

                if not deadline_reached:
                    _submit_available()

        if deadline_reached and next_index < len(runnable_payloads):
            remaining = len(runnable_payloads) - next_index
            stats["deferred_groups"] = int(stats["deferred_groups"]) + remaining
            logger.warning("Comment extraction stage deadline reached; deferring remaining users to next cycle")

    stats["duration_seconds"] = round(max(0.0, time.monotonic() - started_at), 2)
    logger.success(
        "AI analysis complete — "
        f"saved={int(stats['saved'])} "
        f"succeeded_groups={int(stats['succeeded_groups'])} "
        f"failed_groups={int(stats['failed_groups'])} "
        f"blocked_groups={int(stats['blocked_groups'])} "
        f"deferred_groups={int(stats['deferred_groups'])}"
    )
    return stats if include_stats else int(stats["saved"])


# ── Single Post Analysis ──────────────────────────────────────────────────────

POST_SYSTEM_PROMPT = """### ROLE
You are the same expert panel (Behavioral Analyst + Graph Architect + CIS Social Scientist).
Analyze this single Telegram CHANNEL POST as published content — not a user comment.
Focus on what the AUTHOR communicates, implies, and signals to their audience.

Apply the same language rules:
- All taxonomy labels → ENGLISH
- Topic names → canonical English, title case, singular
- evidence_quotes → preserve original language
- Descriptions → precise English

Return ONLY the JSON schema below, no preamble.

{
  "primary_intent": "<intent>",
  "evidence_quotes": ["<original language>"],
  "sentiment": "Positive|Negative|Neutral|Mixed|Urgent|Sarcastic",
  "sentiment_score": <-1.0 to 1.0>,
  "emotional_tone": "<emotion>",
  "social_sentiment_tags": ["Anxious|Frustrated|Angry|Confused|Hopeful|Trusting|Distrustful|Solidarity|Exhausted|Grief"],
  "topics": [{"name": "<Canonical English>", "importance": "primary|secondary|tertiary", "evidence": "<>"}],
  "entities": [{"name": "<Canonical English>", "type": "person|group|organization|place|concept|media", "sentiment_toward": "positive|negative|neutral|ambiguous|fearful|admiring|mocking"}],
  "social_signals": {
    "geopolitical_alignment": "<>",
    "collective_memory": "<or null>",
    "migration_intent": "Yes|No|Implied",
    "diaspora_signals": "Yes|No",
    "authority_attitude": "<>"
  },
  "demographics": {
    "language": "<ISO 639-1>",
    "inferred_gender": "male|female|unknown",
    "inferred_age_bracket": "<bracket>"
  }
}"""


POST_SYSTEM_PROMPT_COMPACT = """You analyze one Telegram channel post.
Return STRICT JSON only (no markdown).

Schema:
{
  "primary_intent": "<intent>",
  "evidence_quotes": ["<original language>"],
  "sentiment": "Positive|Negative|Neutral|Mixed|Urgent|Sarcastic",
  "sentiment_score": <-1.0 to 1.0>,
  "emotional_tone": "<emotion>",
  "social_sentiment_tags": ["Anxious|Frustrated|Angry|Confused|Hopeful|Trusting|Distrustful|Solidarity|Exhausted|Grief"],
  "topics": [
    {"name": "<Canonical English>", "importance": "primary|secondary|tertiary", "evidence": "<short evidence>"}
  ],
  "entities": [
    {"name": "<Canonical English>", "type": "person|group|organization|place|concept|media", "sentiment_toward": "positive|negative|neutral|ambiguous|fearful|admiring|mocking"}
  ],
  "social_signals": {
    "geopolitical_alignment": "Pro_Russia|Pro_West|Pro_Armenia|Pro_Azerbaijan|Nationalist|Anti_Government|Neutral|Ambiguous|unknown",
    "collective_memory": "<or null>",
    "migration_intent": "Yes|No|Implied",
    "diaspora_signals": "Yes|No",
    "authority_attitude": "Deferential|Critical|Dismissive|Fearful|Admiring|Humorous|unknown"
  },
  "demographics": {
    "language": "<ISO 639-1>",
    "inferred_gender": "male|female|unknown",
    "inferred_age_bracket": "13-17|18-24|25-34|35-44|45-54|55+|unknown"
  }
}
"""


THREAD_POST_SYSTEM_PROMPT_COMPACT = """You analyze one Telegram public supergroup thread.
Treat it as a user conversation thread, not as an official channel post.
Return STRICT JSON only (no markdown).

Schema:
{
  "primary_intent": "<thread-level dominant intent>",
  "evidence_quotes": ["<original language>"],
  "sentiment": "Positive|Negative|Neutral|Mixed|Urgent|Sarcastic",
  "sentiment_score": <-1.0 to 1.0>,
  "emotional_tone": "<dominant thread mood>",
  "social_sentiment_tags": ["Anxious|Frustrated|Angry|Confused|Hopeful|Trusting|Distrustful|Solidarity|Exhausted|Grief"],
  "topics": [
    {"name": "<Canonical English>", "importance": "primary|secondary|tertiary", "evidence": "<short evidence>"}
  ],
  "entities": [
    {"name": "<Canonical English>", "type": "person|group|organization|place|concept|media", "sentiment_toward": "positive|negative|neutral|ambiguous|fearful|admiring|mocking"}
  ],
  "social_signals": {
    "geopolitical_alignment": "Pro_Russia|Pro_West|Pro_Armenia|Pro_Azerbaijan|Nationalist|Anti_Government|Neutral|Ambiguous|unknown",
    "collective_memory": "<or null>",
    "migration_intent": "Yes|No|Implied",
    "diaspora_signals": "Yes|No",
    "authority_attitude": "Deferential|Critical|Dismissive|Fearful|Admiring|Humorous|unknown"
  },
  "demographics": {
    "language": "<ISO 639-1>",
    "inferred_gender": "male|female|unknown",
    "inferred_age_bracket": "13-17|18-24|25-34|35-44|45-54|55+|unknown"
  }
}
"""


POST_BATCH_SYSTEM_PROMPT_COMPACT = """You analyze MULTIPLE Telegram channel posts.
Each post MUST be analyzed independently.
Never merge or transfer evidence between posts.

Return STRICT JSON only with this schema:
{
  "items": [
    {
      "post_id": "<post UUID from input>",
      "primary_intent": "<intent>",
      "evidence_quotes": ["<original language>"],
      "sentiment": "Positive|Negative|Neutral|Mixed|Urgent|Sarcastic",
      "sentiment_score": <-1.0 to 1.0>,
      "emotional_tone": "<emotion>",
      "social_sentiment_tags": ["Anxious|Frustrated|Angry|Confused|Hopeful|Trusting|Distrustful|Solidarity|Exhausted|Grief"],
      "topics": [
        {"name": "<Canonical English>", "importance": "primary|secondary|tertiary", "evidence": "<short evidence>"}
      ],
      "entities": [
        {"name": "<Canonical English>", "type": "person|group|organization|place|concept|media", "sentiment_toward": "positive|negative|neutral|ambiguous|fearful|admiring|mocking"}
      ],
      "social_signals": {
        "geopolitical_alignment": "Pro_Russia|Pro_West|Pro_Armenia|Pro_Azerbaijan|Nationalist|Anti_Government|Neutral|Ambiguous|unknown",
        "collective_memory": "<or null>",
        "migration_intent": "Yes|No|Implied",
        "diaspora_signals": "Yes|No",
        "authority_attitude": "Deferential|Critical|Dismissive|Fearful|Admiring|Humorous|unknown"
      },
      "demographics": {
        "language": "<ISO 639-1>",
        "inferred_gender": "male|female|unknown",
        "inferred_age_bracket": "13-17|18-24|25-34|35-44|45-54|55+|unknown"
      }
    }
  ]
}

Rules:
1) Return exactly one item for each provided post_id.
2) Do not include extra or missing post_ids.
3) Keep each item scoped to its own post text only.
"""

ADMIN_PROMPT_DEFAULTS = {
    "extraction.system_prompt": SYSTEM_PROMPT,
    "extraction.strict_taxonomy_prompt": STRICT_TAXONOMY_PROMPT,
    "extraction.post_system_prompt": POST_SYSTEM_PROMPT,
    "extraction.post_system_prompt_compact": POST_SYSTEM_PROMPT_COMPACT,
    "extraction.thread_post_system_prompt_compact": THREAD_POST_SYSTEM_PROMPT_COMPACT,
    "extraction.post_batch_system_prompt_compact": POST_BATCH_SYSTEM_PROMPT_COMPACT,
}


def get_admin_prompt_defaults() -> dict[str, str]:
    return dict(ADMIN_PROMPT_DEFAULTS)


def _runtime_prompt(key: str, default: str) -> str:
    return get_admin_prompt(key, default)


def _runtime_openai_model() -> str:
    value = get_admin_runtime_value("openaiModel", config.OPENAI_MODEL)
    text = str(value).strip() if value is not None else ""
    return text or config.OPENAI_MODEL


def _runtime_ai_post_prompt_style() -> str:
    value = get_admin_runtime_value("aiPostPromptStyle", config.AI_POST_PROMPT_STYLE)
    style = str(value).strip().lower() if value is not None else ""
    return style if style in {"compact", "full"} else str(config.AI_POST_PROMPT_STYLE or "compact").strip().lower()


def _post_entry_kind(post: dict) -> str:
    value = str(post.get("entry_kind") or "broadcast_post").strip().lower()
    return value or "broadcast_post"


def _is_thread_anchor(post: dict) -> bool:
    return _post_entry_kind(post) == "thread_anchor"


def _should_skip_post_analysis(post: dict) -> bool:
    text = str(post.get("text") or "").strip()
    if _is_thread_anchor(post):
        thread_message_count = int(post.get("thread_message_count") or 0)
        return len(text) < 20 and thread_message_count <= 1
    return (not text) or len(text) < 20


def _thread_post_user_context(post: dict, supabase_writer) -> str:
    context_limit = max(4, int(getattr(config, "AI_THREAD_SUMMARY_CONTEXT_MESSAGES", 12)))
    message_char_limit = max(120, int(config.AI_MESSAGE_CHAR_LIMIT))
    comments = supabase_writer.get_comments_for_post(post["id"], limit=context_limit)
    sections: list[str] = []
    for index, comment in enumerate(comments, start=1):
        label = "ROOT" if comment.get("is_thread_root") else f"MSG {index}"
        user_id = comment.get("telegram_user_id") or "anonymous"
        sections.append(
            f"[{label} | USER {user_id} | {str(comment.get('posted_at') or '')[:16]}]\n"
            f"{_trim_text(comment.get('text', ''), message_char_limit)}"
        )

    thread_messages = "\n\n".join(sections) if sections else _trim_text(post.get("text", ""), message_char_limit)
    return (
        "Analyze this Telegram public supergroup thread as a conversation summary.\n\n"
        f"Thread Anchor ID: {post.get('id')}\n"
        f"Telegram Top Message ID: {post.get('telegram_message_id')}\n"
        f"Thread Message Count: {int(post.get('thread_message_count') or 0)}\n"
        f"Thread Participant Count: {int(post.get('thread_participant_count') or 0)}\n"
        f"Last Activity At: {post.get('last_activity_at') or post.get('posted_at')}\n\n"
        f"THREAD CONTEXT:\n{thread_messages}"
    )


def _build_post_analysis_row(post: dict, parsed: dict) -> dict:
    demographics = parsed.get("demographics") or {}
    return {
        "channel_id":           post["channel_id"],
        "telegram_user_id":     None,
        "content_type":         "post",
        "content_id":           post["id"],
        "primary_intent":       parsed.get("primary_intent"),
        "sentiment_score":      parsed.get("sentiment_score"),
        "topics":               _extract_topic_names(parsed),
        "language":             demographics.get("language"),
        "inferred_gender":      "unknown",
        "inferred_age_bracket": "unknown",
        "raw_llm_response":     parsed,
        "neo4j_synced":         False,
    }


def _persist_post_analysis(post: dict, parsed: dict, supabase_writer) -> None:
    analysis = _build_post_analysis_row(post, parsed)
    supabase_writer.save_analysis(analysis)
    supabase_writer.mark_post_processed(post["id"])
    logger.debug(
        f"Post analyzed {post.get('id')} | intent={parsed.get('primary_intent')} | "
        f"tone={parsed.get('emotional_tone')} | "
        f"geo={parsed.get('social_signals', {}).get('geopolitical_alignment', '?')}"
    )


def _analyze_single_post_payload(post: dict, supabase_writer) -> dict:
    is_thread_anchor = _is_thread_anchor(post)
    prompt_style = _runtime_ai_post_prompt_style()
    compact_prompt = _runtime_prompt("extraction.post_system_prompt_compact", POST_SYSTEM_PROMPT_COMPACT)
    full_prompt = _runtime_prompt("extraction.post_system_prompt", POST_SYSTEM_PROMPT)
    thread_prompt = _runtime_prompt("extraction.thread_post_system_prompt_compact", THREAD_POST_SYSTEM_PROMPT_COMPACT)
    strict_taxonomy_prompt = _runtime_prompt("extraction.strict_taxonomy_prompt", STRICT_TAXONOMY_PROMPT)
    base_prompt = thread_prompt if is_thread_anchor else (compact_prompt if prompt_style == "compact" else full_prompt)
    prompt_candidates = [base_prompt]
    if (not is_thread_anchor) and base_prompt != compact_prompt:
        prompt_candidates.append(compact_prompt)

    text = post.get("text", "")
    post_text = _trim_text(text, max(200, int(config.AI_MESSAGE_CHAR_LIMIT) * 3))
    user_context = _thread_post_user_context(post, supabase_writer) if is_thread_anchor else f"Analyze this channel post:\n\n{post_text}"
    parsed = None
    last_error = None

    for index, candidate_prompt in enumerate(prompt_candidates, start=1):
        system_prompt = candidate_prompt
        if config.FEATURE_EXTRACTION_V2:
            system_prompt = f"{candidate_prompt}\n\n{strict_taxonomy_prompt}"

        try:
            parsed = _normalize_payload(
                _request_json(
                    system_prompt=system_prompt,
                    user_context=user_context,
                    max_tokens=max(250, int(config.AI_POST_MAX_TOKENS)),
                    request_label=f"post {post.get('id')} prompt#{index}",
                )
            )
            break
        except Exception as exc:
            last_error = exc
            if index < len(prompt_candidates):
                logger.warning(
                    f"Post {post.get('id')}: primary prompt failed ({exc}); retrying with compact fallback"
                )
            else:
                raise

    if parsed is None:
        raise RuntimeError(f"Post parsing failed: {last_error}")
    return parsed


def _validate_post_batch_payload(parsed: dict, posts: list[dict]) -> dict[str, dict]:
    items = parsed.get("items")
    if not isinstance(items, list):
        raise ValueError("Batch post response missing 'items' list")

    expected_ids = [str(post.get("id") or "") for post in posts if post.get("id")]
    expected_set = set(expected_ids)
    if len(expected_ids) != len(posts) or len(expected_set) != len(expected_ids):
        raise ValueError("Invalid input posts for batch validation")

    output: dict[str, dict] = {}
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("Batch post item is not an object")

        post_id = str(item.get("post_id") or "").strip()
        if not post_id:
            raise ValueError("Batch post item missing post_id")
        if post_id not in expected_set:
            raise ValueError(f"Batch post item has unknown post_id={post_id}")
        if post_id in output:
            raise ValueError(f"Batch post response has duplicate post_id={post_id}")

        payload = {k: v for k, v in item.items() if k != "post_id"}
        output[post_id] = _normalize_payload(payload)

    missing = expected_set - set(output.keys())
    if missing:
        raise ValueError(f"Batch post response missing ids: {sorted(missing)}")

    return output


def _analyze_post_batch_payload(posts: list[dict]) -> dict[str, dict]:
    if not posts:
        return {}

    item_char_limit = max(220, int(config.AI_MESSAGE_CHAR_LIMIT) * 2)
    sections: list[str] = []
    for index, post in enumerate(posts, start=1):
        post_id = str(post.get("id") or "")
        if not post_id:
            raise ValueError("Post batch payload contains item without id")
        sections.append(
            f"[POST {index}]\n"
            f"post_id: {post_id}\n"
            f"channel_id: {post.get('channel_id')}\n"
            f"telegram_message_id: {post.get('telegram_message_id')}\n"
            f"posted_at: {post.get('posted_at')}\n"
            f"text:\n{_trim_text(post.get('text', ''), item_char_limit)}"
        )

    batch_prompt = _runtime_prompt("extraction.post_batch_system_prompt_compact", POST_BATCH_SYSTEM_PROMPT_COMPACT)
    strict_taxonomy_prompt = _runtime_prompt("extraction.strict_taxonomy_prompt", STRICT_TAXONOMY_PROMPT)
    system_prompt = batch_prompt
    if config.FEATURE_EXTRACTION_V2:
        system_prompt = f"{batch_prompt}\n\n{strict_taxonomy_prompt}"

    parsed = _request_json(
        system_prompt=system_prompt,
        user_context=(
            "Analyze each post independently and return EXACTLY one item per post_id.\n\n"
            f"Posts count: {len(posts)}\n\n"
            + "\n\n---\n\n".join(sections)
        ),
        max_tokens=max(700, int(config.AI_POST_BATCH_MAX_TOKENS)),
        request_label=f"post-batch size={len(posts)}",
    )
    return _validate_post_batch_payload(parsed, posts)


def _process_single_post(post: dict, supabase_writer) -> str:
    if _should_skip_post_analysis(post):
        supabase_writer.mark_post_processed(post["id"])
        return "skipped"

    try:
        parsed = _analyze_single_post_payload(post, supabase_writer)
        _persist_post_analysis(post, parsed, supabase_writer)
        _clear_failure_scope(supabase_writer, scope_type="post", scope_key=str(post["id"]))
        return "saved"
    except OpenAICircuitOpenError as error:
        logger.warning(f"Post intent extraction deferred for post {post['id']}: provider circuit {error}")
        return "blocked"
    except Exception as e:
        logger.error(f"Post intent extraction failed for post {post['id']}: {e}")
        _record_failure_scope(
            supabase_writer,
            scope_type="post",
            scope_key=str(post["id"]),
            channel_id=str(post.get("channel_id")) if post.get("channel_id") else None,
            post_id=str(post.get("id")) if post.get("id") else None,
            telegram_user_id=None,
            error=e,
        )
        # Do NOT mark as processed — retry on the next cycle
        return "failed"


def extract_post_intents(
    posts: list[dict],
    supabase_writer,
    *,
    deadline_epoch: float | None = None,
    include_stats: bool = False,
) -> int | dict:
    """Analyze posts with strict micro-batching, bounded concurrency, and safe fallback."""
    started_at = time.monotonic()
    stats: dict[str, int | float] = {
        "workers": max(1, int(getattr(config, "AI_POST_WORKERS", 1))),
        "inflight_limit": max(1, int(getattr(config, "AI_MAX_INFLIGHT_REQUESTS", 1))),
        "attempted_posts": 0,
        "blocked_posts": 0,
        "deferred_posts": 0,
        "succeeded_posts": 0,
        "failed_posts": 0,
        "batch_failures": 0,
        "saved": 0,
    }

    if not posts:
        stats["duration_seconds"] = round(max(0.0, time.monotonic() - started_at), 2)
        return stats if include_stats else 0

    processable_posts: list[dict] = []
    for post in posts:
        if _should_skip_post_analysis(post):
            supabase_writer.mark_post_processed(post["id"])
            continue
        processable_posts.append(post)

    if not processable_posts:
        stats["duration_seconds"] = round(max(0.0, time.monotonic() - started_at), 2)
        return stats if include_stats else 0

    blocked = _blocked_scope_keys(
        supabase_writer,
        scope_type="post",
        scope_keys=[str(post.get("id")) for post in processable_posts if post.get("id")],
    )
    runnable_posts = []
    for post in processable_posts:
        key = str(post.get("id") or "")
        if key and key in blocked:
            stats["blocked_posts"] = int(stats["blocked_posts"]) + 1
            continue
        runnable_posts.append(post)

    stats["attempted_posts"] = len(runnable_posts)
    if not runnable_posts:
        stats["duration_seconds"] = round(max(0.0, time.monotonic() - started_at), 2)
        return stats if include_stats else 0

    batch_size = max(1, int(config.AI_POST_BATCH_SIZE))
    chunks = _chunked(runnable_posts, batch_size)
    workers = max(1, int(getattr(config, "AI_POST_WORKERS", 1)))
    inflight_limit = max(1, int(getattr(config, "AI_MAX_INFLIGHT_REQUESTS", 1)))
    max_inflight = max(1, min(workers, inflight_limit))

    def _handle_chunk_success(chunk: list[dict], parsed_by_post_id: dict[str, dict]):
        for post in chunk:
            post_id = str(post.get("id") or "")
            parsed = parsed_by_post_id.get(post_id)
            if not parsed:
                stats["failed_posts"] = int(stats["failed_posts"]) + 1
                _record_failure_scope(
                    supabase_writer,
                    scope_type="post",
                    scope_key=post_id,
                    channel_id=str(post.get("channel_id")) if post.get("channel_id") else None,
                    post_id=post_id or None,
                    telegram_user_id=None,
                    error="missing parsed payload for post id",
                )
                continue
            _persist_post_analysis(post, parsed, supabase_writer)
            _clear_failure_scope(supabase_writer, scope_type="post", scope_key=post_id)
            stats["saved"] = int(stats["saved"]) + 1
            stats["succeeded_posts"] = int(stats["succeeded_posts"]) + 1

    def _fallback_chunk_to_single(chunk: list[dict]):
        for index, post in enumerate(chunk):
            if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                stats["deferred_posts"] = int(stats["deferred_posts"]) + (len(chunk) - index)
                logger.warning("Post extraction deadline reached during fallback; deferring remaining posts")
                return
            result = _process_single_post(post, supabase_writer)
            if result == "saved":
                stats["saved"] = int(stats["saved"]) + 1
                stats["succeeded_posts"] = int(stats["succeeded_posts"]) + 1
            elif result == "blocked":
                stats["blocked_posts"] = int(stats["blocked_posts"]) + 1
            elif result == "failed":
                stats["failed_posts"] = int(stats["failed_posts"]) + 1

    def _mark_chunk_blocked(chunk: list[dict], error: OpenAICircuitOpenError) -> None:
        stats["blocked_posts"] = int(stats["blocked_posts"]) + len(chunk)
        logger.warning(
            f"Post analysis deferred for {len(chunk)} posts: provider circuit {error}"
        )

    if workers <= 1:
        for chunk_index, chunk in enumerate(chunks):
            if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                remaining_posts = sum(len(item) for item in chunks[chunk_index:])
                stats["deferred_posts"] = int(stats["deferred_posts"]) + remaining_posts
                logger.warning("Post extraction stage deadline reached; deferring remaining posts to next cycle")
                break

            if len(chunk) == 1:
                result = _process_single_post(chunk[0], supabase_writer)
                if result == "saved":
                    stats["saved"] = int(stats["saved"]) + 1
                    stats["succeeded_posts"] = int(stats["succeeded_posts"]) + 1
                elif result == "blocked":
                    stats["blocked_posts"] = int(stats["blocked_posts"]) + 1
                elif result == "failed":
                    stats["failed_posts"] = int(stats["failed_posts"]) + 1
                continue

            if any(_is_thread_anchor(post) for post in chunk):
                _fallback_chunk_to_single(chunk)
                continue

            try:
                parsed_by_post_id = _analyze_post_batch_payload(chunk)
                _handle_chunk_success(chunk, parsed_by_post_id)
            except OpenAICircuitOpenError as circuit_error:
                _mark_chunk_blocked(chunk, circuit_error)
            except Exception as batch_error:
                stats["batch_failures"] = int(stats["batch_failures"]) + 1
                logger.warning(
                    f"Post batch analysis failed for {len(chunk)} posts ({batch_error}); falling back to single-post mode"
                )
                _fallback_chunk_to_single(chunk)
    else:
        next_chunk = 0
        deadline_reached = False

        def _analyze_chunk(chunk: list[dict]) -> dict[str, dict]:
            if any(_is_thread_anchor(post) for post in chunk):
                return {
                    str(post.get("id")): _analyze_single_post_payload(post, supabase_writer)
                    for post in chunk
                }
            if len(chunk) == 1:
                post = chunk[0]
                return {str(post.get("id")): _analyze_single_post_payload(post, supabase_writer)}
            return _analyze_post_batch_payload(chunk)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending: dict = {}

            def _submit_available():
                nonlocal next_chunk, deadline_reached
                while next_chunk < len(chunks) and len(pending) < max_inflight:
                    if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                        deadline_reached = True
                        return
                    chunk = chunks[next_chunk]
                    next_chunk += 1
                    future = executor.submit(_analyze_chunk, chunk)
                    pending[future] = chunk

            _submit_available()

            while pending:
                done, _ = wait(list(pending.keys()), timeout=1.0, return_when=FIRST_COMPLETED)
                if not done:
                    if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                        deadline_reached = True
                    continue

                for future in done:
                    chunk = pending.pop(future)
                    try:
                        parsed_by_post_id = future.result()
                        _handle_chunk_success(chunk, parsed_by_post_id)
                    except OpenAICircuitOpenError as circuit_error:
                        _mark_chunk_blocked(chunk, circuit_error)
                    except Exception as batch_error:
                        stats["batch_failures"] = int(stats["batch_failures"]) + 1
                        logger.warning(
                            f"Post batch analysis failed for {len(chunk)} posts ({batch_error}); "
                            "falling back to single-post mode"
                        )
                        _fallback_chunk_to_single(chunk)

                if not deadline_reached:
                    _submit_available()

        if deadline_reached and next_chunk < len(chunks):
            remaining_posts = sum(len(item) for item in chunks[next_chunk:])
            stats["deferred_posts"] = int(stats["deferred_posts"]) + remaining_posts
            logger.warning("Post extraction stage deadline reached; deferring remaining posts to next cycle")

    stats["duration_seconds"] = round(max(0.0, time.monotonic() - started_at), 2)
    logger.success(
        "Post AI analysis complete — "
        f"saved={int(stats['saved'])} "
        f"succeeded_posts={int(stats['succeeded_posts'])} "
        f"failed_posts={int(stats['failed_posts'])} "
        f"blocked_posts={int(stats['blocked_posts'])} "
        f"deferred_posts={int(stats['deferred_posts'])} "
        f"batch_failures={int(stats['batch_failures'])}"
    )
    return stats if include_stats else int(stats["saved"])


def extract_post_intent(post: dict, supabase_writer) -> bool:
    """Analyze a single channel post with full behavioral intelligence framework."""
    return _process_single_post(post, supabase_writer) == "saved"
