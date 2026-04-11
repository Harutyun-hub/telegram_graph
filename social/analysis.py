from __future__ import annotations

import json
import time
from typing import Any

from loguru import logger
from openai import OpenAI

import config
from utils.ai_usage import log_openai_usage


def _trimmed(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


SYSTEM_PROMPT = """
You are a competitive intelligence analyst extracting enterprise-grade signals
from social media evidence about brands, businesses, competitors, and public figures.

Return structured JSON only. Never include markdown or prose outside JSON.

For each activity, extract:
- summary
- marketing_intent
- products
- audience_segments
- pain_points
- value_propositions
- competitive_signals
- customer_intent
- urgency_indicators
- topics
- sentiment
- sentiment_score
- marketing_tactic

Quality rules:
- Ground every insight in the provided evidence.
- Prefer concrete offers, claims, and positioning over vague abstractions.
- Keep topics canonical and short.
- If the signal is missing, return empty arrays or null-like strings instead of inventing facts.
""".strip()


class SocialActivityAnalyzer:
    def __init__(self) -> None:
        if not config.OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY is not configured")
        self.client = OpenAI(api_key=config.OPENAI_API_KEY)

    def analyze_batch(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not items:
            return []
        if len(items) > max(1, int(config.SOCIAL_ANALYSIS_BATCH_SIZE)):
            raise ValueError("Batch exceeds configured social analysis batch size")
        raw = self._request(self._build_batch_payload(items))
        parsed = self._parse_batch_response(raw)
        if len(parsed) != len(items):
            raise ValueError(f"Batch response length mismatch: expected {len(items)}, got {len(parsed)}")

        normalized: list[dict[str, Any]] = []
        for index, item in enumerate(items):
            result = parsed[index]
            batch_index = result.get("batch_index")
            activity_uid = _trimmed(result.get("activity_uid"))
            if batch_index != index or activity_uid != item["activity_uid"]:
                raise ValueError("Batch response did not preserve item ordering and identity")
            normalized.append(
                {
                    "activity_id": item["id"],
                    "entity_id": item["entity_id"],
                    "platform": item["platform"],
                    "activity_uid": item["activity_uid"],
                    "analysis_payload": self._normalize_result(result),
                    "raw_model_output": result,
                    "model": config.SOCIAL_ANALYSIS_MODEL,
                    "prompt_version": config.SOCIAL_ANALYSIS_PROMPT_VERSION,
                    "analysis_version": config.SOCIAL_ANALYSIS_PROMPT_VERSION,
                }
            )
        return normalized

    def analyze_one(self, item: dict[str, Any]) -> dict[str, Any]:
        return self.analyze_batch([item])[0]

    def _request(self, payload: dict[str, Any]) -> str:
        retry_limit = max(0, int(config.AI_REQUEST_MAX_RETRIES))
        backoff = max(0.0, float(config.AI_REQUEST_RETRY_BACKOFF_SECONDS))
        last_error: Exception | None = None
        for attempt in range(retry_limit + 1):
            try:
                request_started_at = time.perf_counter()
                response = self.client.chat.completions.create(
                    model=config.SOCIAL_ANALYSIS_MODEL,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                    ],
                    max_completion_tokens=max(1200, 800 * max(1, len(payload.get("items") or []))),
                    timeout=config.AI_REQUEST_TIMEOUT_SECONDS,
                )
                log_openai_usage(
                    feature="social_analysis",
                    model=config.SOCIAL_ANALYSIS_MODEL,
                    response=response,
                    started_at=request_started_at,
                    extra={
                        "attempt": attempt + 1,
                        "items": len(payload.get("items") or []),
                    },
                )
                return _trimmed(response.choices[0].message.content)
            except Exception as exc:  # pragma: no cover - network/provider failures
                last_error = exc
                if attempt >= retry_limit:
                    break
                logger.warning("Social AI request failed on attempt {}: {}", attempt + 1, exc)
                time.sleep(backoff * (attempt + 1))
        raise RuntimeError(f"Social AI request failed: {last_error}")

    def _build_batch_payload(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        platform = items[0].get("platform")
        entity = items[0].get("entity") or {}
        return {
            "contract": {
                "type": "ordered_json_array",
                "required_length": len(items),
                "prompt_version": config.SOCIAL_ANALYSIS_PROMPT_VERSION,
            },
            "entity": {
                "id": entity.get("id"),
                "name": entity.get("name"),
                "industry": entity.get("industry"),
            },
            "platform": platform,
            "items": [
                {
                    "batch_index": index,
                    "activity_uid": item["activity_uid"],
                    "source_kind": item.get("source_kind"),
                    "published_at": item.get("published_at"),
                    "source_url": item.get("source_url"),
                    "author_handle": item.get("author_handle"),
                    "cta_type": item.get("cta_type"),
                    "content_format": item.get("content_format"),
                    "region_name": item.get("region_name"),
                    "engagement_metrics": item.get("engagement_metrics") or {},
                    "text_content": item.get("text_content"),
                }
                for index, item in enumerate(items)
            ],
            "response_instructions": {
                "root_type": "json_array",
                "same_order_as_input": True,
                "include_batch_index_and_activity_uid": True,
            },
        }

    @staticmethod
    def _parse_batch_response(raw: str) -> list[dict[str, Any]]:
        if not raw:
            raise ValueError("Empty AI response")
        text = raw.strip()
        start = text.find("[")
        end = text.rfind("]")
        parsed: Any
        if start != -1 and end != -1 and end > start:
            parsed = json.loads(text[start:end + 1])
        else:
            candidate = json.loads(text)
            if isinstance(candidate, dict) and isinstance(candidate.get("items"), list):
                parsed = candidate["items"]
            else:
                parsed = candidate
        if not isinstance(parsed, list):
            raise ValueError("AI batch response was not a JSON array")
        return [item for item in parsed if isinstance(item, dict)]

    @staticmethod
    def _normalize_result(result: dict[str, Any]) -> dict[str, Any]:
        def _as_list(key: str) -> list[Any]:
            value = result.get(key)
            return value if isinstance(value, list) else []

        normalized = {
            "batch_index": result.get("batch_index"),
            "activity_uid": result.get("activity_uid"),
            "summary": _trimmed(result.get("summary")),
            "marketing_intent": _trimmed(result.get("marketing_intent")),
            "products": _as_list("products"),
            "audience_segments": _as_list("audience_segments"),
            "pain_points": _as_list("pain_points"),
            "value_propositions": _as_list("value_propositions"),
            "competitive_signals": _as_list("competitive_signals"),
            "customer_intent": _trimmed(result.get("customer_intent")),
            "urgency_indicators": _as_list("urgency_indicators"),
            "topics": _as_list("topics"),
            "sentiment": _trimmed(result.get("sentiment")) or "Neutral",
            "sentiment_score": SocialActivityAnalyzer._clamp_score(result.get("sentiment_score")),
            "marketing_tactic": _trimmed(result.get("marketing_tactic")),
        }
        return normalized

    @staticmethod
    def _clamp_score(value: Any) -> float:
        try:
            score = float(value)
        except Exception:
            score = 0.0
        return max(-1.0, min(1.0, score))
